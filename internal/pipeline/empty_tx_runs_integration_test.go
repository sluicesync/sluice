//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Bug 300 against real servers. The regression cycle measured it on MySQL
// 8.4: 1,500 autocommit INSERTs into another database on the same server
// plus 2 rows of the backup's own database, and `backup incremental`
// recorded 1,501 empty tx_begin/tx_commit pairs; the broker then spent 18 s
// applying 2 rows, one position write per foreign transaction.
//
// THE SUITE, per source: two incrementals over the same traffic shape —
// three runs of foreignTxs foreign transactions with a two-row transaction of
// the backup's own table between each — the first written by the PRE-FIX
// writer (the recordEveryEmptyTx seam), the second by this one. Graded:
//
//   - the writer: the pre-fix incremental carries a pair per foreign
//     transaction (the anti-vacuity check — on a source that emits them) and
//     the new one at most one per run; both end exactly on their recorded
//     tail;
//   - chain restore (serial apply) reaches the SOURCE's rows;
//   - the broker, killed partway through the PRE-FIX incremental with
//     --apply-concurrency 1, has persisted the frontier of the last REAL
//     transaction's commit (the run after it was withheld, never applied),
//     and its re-run to the tail converges to the SOURCE in a bounded number
//     of position writes, counted by a trigger on the target's control table
//     — independent of sluice — rather than by time.
//
// Postgres is the analogue the filing could not measure. Ground truth, run
// here: pgoutput on PG 15+ skips a transaction that touched no published
// table, so neither incremental records a pair for the foreign traffic there
// (keepalive boundaries aside); PG 14 decodes each one as an empty
// BEGIN/COMMIT, so it has the MySQL shape.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

const (
	foreignTxs        = 300
	emptyTxRunsChunk  = 100
	emptyTxRunsWindow = 8 * time.Second
)

// emptyTxEnv is one source under the suite.
type emptyTxEnv struct {
	*sevEnv
	// foreignPairs: the source decodes a transaction that touched nothing in
	// scope as an empty pair (MySQL, PG 14) — so the pre-fix incremental must
	// carry one per foreign transaction.
	foreignPairs bool
}

// foreign commits n transactions outside the backup's scope, each its own.
func (e *emptyTxEnv) foreign(n int) {
	e.t.Helper()
	stmt := "INSERT INTO etx_noise.n () VALUES ()"
	if e.driver == "pgx" {
		stmt = "INSERT INTO etx_noise DEFAULT VALUES"
	}
	db, err := sql.Open(e.driver, e.src)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for i := 0; i < n; i++ {
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			e.t.Fatalf("foreign transaction %d: %v", i, err)
		}
	}
}

// real commits one two-row transaction into the backup's own table.
func (e *emptyTxEnv) real(k int) {
	e.t.Helper()
	b := "BEGIN;"
	if e.driver == "mysql" {
		b = "START TRANSACTION;"
	}
	e.exec(fmt.Sprintf("%s INSERT INTO kd VALUES (%d,'a'),(%d,'b'); COMMIT;", b, 10*k+1, 10*k+2))
}

// traffic is the suite's window shape: run, real, run, real, run.
func (e *emptyTxEnv) traffic(round int) {
	e.foreign(foreignTxs)
	e.real(2 * round)
	e.foreign(foreignTxs)
	e.real(2*round + 1)
	e.foreign(foreignTxs)
}

func (e *emptyTxEnv) incremental(c *sevChain, legacy bool) *irbackup.Manifest {
	e.t.Helper()
	eng, _ := engines.Get(e.engine)
	b := &IncrementalBackup{
		Source: eng, SourceDSN: e.src, Store: c.store, Window: emptyTxRunsWindow,
		ChunkChanges: emptyTxRunsChunk, SluiceVersion: "test", recordEveryEmptyTx: legacy,
	}
	if err := b.Run(context.Background()); err != nil {
		e.t.Fatalf("backup incremental (pre-fix shape %v): %v", legacy, err)
	}
	return tailIncremental(e.t, c)
}

// tailIncremental returns the chain's newest incremental link's manifest.
func tailIncremental(t *testing.T, c *sevChain) *irbackup.Manifest {
	t.Helper()
	chain, err := (&SyncFromBackup{Store: c.store, ChainURL: "x", StreamID: "x"}).brokerChain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return chain[len(chain)-1].Manifest
}

// emptyPairs counts the empty transactions in a decoded stream (a TxBegin
// immediately followed by a TxCommit) and its row changes.
func emptyPairs(events []ir.Change) (pairs, rows int) {
	for i, c := range events {
		if _, isRow := rowChangeTable(c); isRow {
			rows++
		}
		if _, b := c.(ir.TxBegin); b && i+1 < len(events) {
			if _, cm := events[i+1].(ir.TxCommit); cm {
				pairs++
			}
		}
	}
	return pairs, rows
}

func (e *emptyTxEnv) link(c *sevChain, m *irbackup.Manifest) lineage.SegmentRecord {
	e.t.Helper()
	chain, err := (&SyncFromBackup{Store: c.store, ChainURL: "x", StreamID: "x"}).brokerChain(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	for _, l := range chain {
		if lineage.ManifestBackupID(l.Manifest) == lineage.ManifestBackupID(m) {
			return l
		}
	}
	e.t.Fatal("incremental not in the chain")
	return lineage.SegmentRecord{}
}

// countPositionWrites installs a trigger on dsn's control table that counts
// every write that MOVES a stream's source_position, and returns a reader of
// the count. Independent of sluice: it is the target's own bookkeeping.
func (e *emptyTxEnv) countPositionWrites(dsn string) func() int {
	t := e.t
	t.Helper()
	if e.driver == "pgx" {
		applyDDL(t, dsn, `
			CREATE SCHEMA etx;
			CREATE TABLE etx.pos_writes (n BIGSERIAL PRIMARY KEY);
			CREATE FUNCTION etx.count_write() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN
				IF TG_OP = 'INSERT' OR OLD.source_position IS DISTINCT FROM NEW.source_position THEN
					INSERT INTO etx.pos_writes DEFAULT VALUES;
				END IF;
				RETURN NULL;
			END $$;
			CREATE TRIGGER etx_count AFTER INSERT OR UPDATE ON sluice_cdc_state
				FOR EACH ROW EXECUTE FUNCTION etx.count_write();`)
		return func() int {
			n, _ := strconv.Atoi(sevQuery(t, "pgx", dsn, "SELECT COUNT(*) FROM etx.pos_writes"))
			return n
		}
	}
	applyDDLMySQL(t, dsn, `
		CREATE DATABASE IF NOT EXISTS etx_audit;
		CREATE TABLE IF NOT EXISTS etx_audit.pos_writes (n BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);
		CREATE TRIGGER etx_ins AFTER INSERT ON sluice_cdc_state FOR EACH ROW INSERT INTO etx_audit.pos_writes () VALUES ();
		CREATE TRIGGER etx_upd AFTER UPDATE ON sluice_cdc_state FOR EACH ROW
			BEGIN IF NOT (OLD.source_position <=> NEW.source_position) THEN INSERT INTO etx_audit.pos_writes () VALUES (); END IF; END;`)
	return func() int {
		n, _ := strconv.Atoi(sevQuery(t, "mysql", dsn, "SELECT COUNT(*) FROM etx_audit.pos_writes"))
		return n
	}
}

func (e *emptyTxEnv) kdState(dsn string) string {
	e.t.Helper()
	q := "SELECT COALESCE(GROUP_CONCAT(CONCAT(id,':',note) ORDER BY id),'') FROM kd"
	if e.driver == "pgx" {
		q = "SELECT COALESCE(string_agg(id||':'||note, ',' ORDER BY id),'') FROM kd"
	}
	return sevQuery(e.t, e.driver, dsn, q)
}

func runEmptyTxRunsSuite(t *testing.T, e *emptyTxEnv) {
	ctx := context.Background()
	if e.driver == "pgx" {
		// The foreign traffic is a table in the same database that is not
		// published — the only shape pgoutput can decode at all (logical
		// decoding is per database).
		applyDDL(t, e.src, `
			CREATE TABLE kd (id INT PRIMARY KEY, note TEXT);
			CREATE TABLE etx_noise (id BIGSERIAL PRIMARY KEY);
			CREATE PUBLICATION sluice_pub FOR TABLE kd;
			INSERT INTO kd VALUES (0,'seed');`)
	} else {
		e.exec(`CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, note VARCHAR(16)) ENGINE=InnoDB;
			CREATE DATABASE etx_noise; CREATE TABLE etx_noise.n (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB;
			CREATE DATABASE etx_audit;
			INSERT INTO kd VALUES (0,'seed');`)
	}
	c := e.newChain(nil, false)

	// ---- The writer ----
	e.traffic(0)
	legacy := e.incremental(c, true)
	e.traffic(1)
	fixed := e.incremental(c, false)
	legacyLink, fixedLink := e.link(c, legacy), e.link(c, fixed)
	legacyEvents := decodeIncrementalEvents(t, c.store, &legacyLink)
	fixedEvents := decodeIncrementalEvents(t, c.store, &fixedLink)
	lp, lr := emptyPairs(legacyEvents)
	fp, fr := emptyPairs(fixedEvents)
	t.Logf("pre-fix incremental: %d events, %d empty transactions, %d rows; fixed: %d events, %d empty transactions, %d rows",
		len(legacyEvents), lp, lr, len(fixedEvents), fp, fr)
	if lr != 4 || fr != 4 {
		t.Fatalf("each incremental must carry the window's 4 rows; got %d and %d", lr, fr)
	}
	if e.foreignPairs && lp < 3*foreignTxs {
		t.Fatalf("the pre-fix incremental carries %d empty transactions for %d foreign ones: the fixture does not exercise Bug 300", lp, 3*foreignTxs)
	}
	// One per run at most: before each real transaction and at the tail.
	if fp > 3 {
		t.Errorf("the fixed writer recorded %d empty transactions for three runs; want at most one per run", fp)
	}
	for name, ev := range map[string][]ir.Change{"pre-fix": legacyEvents, "fixed": fixedEvents} {
		m := legacy
		if name == "fixed" {
			m = fixed
		}
		if last := ev[len(ev)-1].Pos(); last != m.EndPosition {
			t.Errorf("%s incremental: EndPosition %+v is not its last recorded position %+v", name, m.EndPosition, last)
		}
		if n := manifestChangeRecordCount(m); n != int64(len(ev)) {
			t.Errorf("%s incremental: the chunks' RowCount sums to %d but they decode to %d records", name, n, len(ev))
		}
	}
	want := e.kdState(e.src)

	// ---- Chain restore, serial ----
	eng, _ := engines.Get(e.engine)
	restoreDSN := e.database("etx_restore")
	if err := (&backup.ChainRestore{Target: eng, TargetDSN: restoreDSN, Store: c.store, ApplyConcurrency: 1, ApplyBatchSize: 100}).Run(ctx); err != nil {
		t.Fatalf("chain restore: %v", err)
	}
	if got := e.kdState(restoreDSN); got != want {
		t.Errorf("chain restore DIVERGES from the source: target {%s}, source {%s}", got, want)
	}
	if !e.foreignPairs {
		// Postgres 15+: pgoutput skips the foreign transactions outright, so
		// neither writer recorded a pair for them — the ground truth this
		// engine's row of the matrix exists to pin. Nothing for the broker
		// to collapse, so its leg would grade nothing.
		if lp > 3 || fp > 3 {
			t.Errorf("Postgres 15+ recorded %d (pre-fix) and %d (fixed) empty transactions for %d foreign ones; pgoutput should skip them", lp, fp, 3*foreignTxs)
		}
		return
	}

	// ---- The broker, killed inside the pre-fix incremental ----
	brokerDSN := e.database("etx_broker")
	if err := (&backup.Restore{Target: eng, TargetDSN: brokerDSN, Store: c.store, SkipChainDispatch: true}).Run(ctx); err != nil {
		t.Fatalf("restore the full into the broker target: %v", err)
	}
	bc := &brokerCrashChain{e: e.sevEnv, store: c.store, fullID: c.fullID, incr: legacyLink, events: legacyEvents}
	// Kill before a chunk in the middle of the second foreign run: the first
	// real transaction is applied, the run after it is not.
	firstRealCommit := -1
	for i, ev := range legacyEvents {
		if _, isCommit := ev.(ir.TxCommit); isCommit && i > 0 {
			if _, prevRow := rowChangeTable(legacyEvents[i-1]); prevRow {
				firstRealCommit = i
				break
			}
		}
	}
	if firstRealCommit < 0 {
		t.Fatal("the pre-fix incremental has no real transaction")
	}
	j := (firstRealCommit + foreignTxs) / emptyTxRunsChunk // a chunk inside the run after it
	if j*emptyTxRunsChunk <= firstRealCommit+1 || j >= len(legacy.ChangeChunks) {
		t.Fatalf("fixture: no chunk boundary inside the run after the first real transaction (commit %d, %d chunks)", firstRealCommit, len(legacy.ChangeChunks))
	}
	const stream = "etx-broker"
	b := bc.broker(brokerDSN, stream, 1, c.fullID)
	b.ApplyBatchSize = 100
	killCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	err := bc.killBefore(b, j).Run(killCtx)
	cancel()
	if !errors.Is(err, errBrokerCrashKill) {
		t.Fatalf("the broker did not die at the kill before chunk %d: %v", j, err)
	}
	pos, _ := bc.persisted(t, brokerDSN, stream)
	tok, derr := decodeBrokerPosition(ir.Position{Token: pos})
	if derr != nil || tok.LastAppliedBackupID == "" {
		t.Fatalf("at the kill the position is %q (%v)", pos, derr)
	}
	// The broker's position names the parent of the pre-fix incremental and
	// stands INSIDE it — at the first real transaction's commit. Every
	// empty transaction of the run after it was withheld: on the pre-fix
	// apply path this was the commit of the last empty transaction below
	// chunk j.
	if tok.InProgress == nil || tok.InProgress.Through != int64(firstRealCommit) {
		got := "none"
		if tok.InProgress != nil {
			got = fmt.Sprint(tok.InProgress.Through)
		}
		t.Errorf("at the kill the frontier is %s; want %d, the first real transaction's commit (the empty run after it is never applied on its own)", got, firstRealCommit)
	}

	// The re-run, counted.
	writes := e.countPositionWrites(brokerDSN)
	bc.runToTail(t, brokerDSN, stream, 1)
	n := writes()
	remainingPairs, _ := emptyPairs(legacyEvents[j*emptyTxRunsChunk:])
	t.Logf("broker re-run: %d position writes for %d remaining pre-fix empty transactions, %d fixed ones and 4 real transactions", n, remainingPairs, fp)
	if n == 0 {
		t.Fatal("the counter saw no position write at all: it graded nothing")
	}
	// Per run at most one, per real transaction one, per incremental a
	// finalising write; a generous ceiling far below one per pair.
	if limit := 20; n > limit {
		t.Errorf("the broker's re-run made %d position writes; want at most %d (the chain still carries %d empty transactions: one write per empty transaction is Bug 300)", n, limit, remainingPairs)
	}
	if got := e.kdState(brokerDSN); got != want {
		t.Errorf("the broker DIVERGES from the source: target {%s}, source {%s}", got, want)
	}
}

func TestEmptyTxRuns_MySQLGTID(t *testing.T) {
	src, dst, cleanup := startMySQLGTID(t)
	t.Cleanup(cleanup)
	runEmptyTxRunsSuite(t, &emptyTxEnv{sevEnv: &sevEnv{t: t, engine: "mysql", driver: "mysql", gtid: true, src: src, dst: dst}, foreignPairs: true})
}

func TestEmptyTxRuns_MySQLFilePos(t *testing.T) {
	src, dst, cleanup := startMySQLBinlog(t)
	t.Cleanup(cleanup)
	runEmptyTxRunsSuite(t, &emptyTxEnv{sevEnv: &sevEnv{t: t, engine: "mysql", driver: "mysql", src: src, dst: dst}, foreignPairs: true})
}

func TestEmptyTxRuns_Postgres(t *testing.T) {
	src, dst, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)
	runEmptyTxRunsSuite(t, &emptyTxEnv{sevEnv: &sevEnv{t: t, engine: "postgres", driver: "pgx", src: src, dst: dst}})
}

// TestEmptyTxRuns_Postgres14 is the Postgres version that DOES decode a
// transaction touching no published table, as an empty BEGIN/COMMIT.
func TestEmptyTxRuns_Postgres14(t *testing.T) {
	src, dst, cleanup := startPostgresLogicalImage(t, "postgres:14", 8)
	t.Cleanup(cleanup)
	runEmptyTxRunsSuite(t, &emptyTxEnv{sevEnv: &sevEnv{t: t, engine: "postgres", driver: "pgx", src: src, dst: dst}, foreignPairs: true})
}

// TestEmptyTxRuns_LiveSyncCatchUp is the live stream's half, MySQL GTID with
// --apply-concurrency 1: a sync that is behind by foreignTxs*3 foreign
// transactions catches up in far fewer position writes than that, and lands
// the real rows exactly. The stage collapses only what is ALREADY QUEUED, so
// the catch-up is the shape it acts on; the ceiling is generous because how
// much of the backlog is queued at each step is the scheduler's business —
// before the fix it was one write per foreign transaction, with no
// scheduling at all involved.
//
// Counted up to the moment the real rows land, not after: source and target
// share a server here, so each position write is itself a transaction the
// stream reads back as out-of-scope — an echo that keeps an idle same-server
// stream writing at its own commit rate, independent of this fix.
func TestEmptyTxRuns_LiveSyncCatchUp(t *testing.T) {
	src, dst, cleanup := startMySQLGTID(t)
	t.Cleanup(cleanup)
	e := &emptyTxEnv{sevEnv: &sevEnv{t: t, engine: "mysql", driver: "mysql", gtid: true, src: src, dst: dst}, foreignPairs: true}
	e.exec(`CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, note VARCHAR(16)) ENGINE=InnoDB;
		CREATE DATABASE etx_noise; CREATE TABLE etx_noise.n (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB;
		CREATE DATABASE etx_audit;
		INSERT INTO kd VALUES (0,'seed');`)
	eng, _ := engines.Get("mysql")
	const stream = "etx-live"
	streamer := func() *Streamer {
		return &Streamer{Source: eng, Target: eng, SourceDSN: src, TargetDSN: dst, StreamID: stream, ApplyConcurrency: 1, ApplyBatchSize: 100}
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	run1 := startStream(ctx1, streamer())
	// Stop only after the anchor is persisted, for the reason liveHeadRun.run
	// gives: a stop inside the copy makes the second run a refused cold start.
	run1.await(t, "the cold start copying the seed row and persisting a position", 60*time.Second, 200*time.Millisecond,
		func() bool { return pollRowCountMySQL(dst, "kd") >= 1 && e.persisted(dst, stream) != "" },
		func() string { return "" })
	cancel1()
	run1.join()

	writes := e.countPositionWrites(dst)
	e.foreign(3 * foreignTxs)
	e.real(1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	run2 := startStream(ctx2, streamer())
	run2.await(t, "the warm resume applying the real transaction", 2*time.Minute, 200*time.Millisecond,
		func() bool { return pollRowCountMySQL(dst, "kd") >= 3 },
		func() string { return "" })
	n := writes()
	// Ground truth for the echo described above, logged rather than graded:
	// how many position writes an idle same-server stream makes per second.
	time.Sleep(2 * time.Second)
	t.Logf("idle echo: %d position writes in 2 s after the catch-up", writes()-n)
	cancel2()
	run2.join()
	t.Logf("live catch-up: %d position writes for %d foreign transactions", n, 3*foreignTxs)
	if n == 0 {
		t.Fatal("the counter saw no position write: it graded nothing")
	}
	if limit := foreignTxs; n > limit {
		t.Errorf("the catch-up made %d position writes for %d foreign transactions; want at most %d (one per foreign transaction is Bug 300)", n, 3*foreignTxs, limit)
	}
	if got, want := e.kdState(dst), e.kdState(src); got != want {
		t.Errorf("the sync DIVERGES from the source: target {%s}, source {%s}", got, want)
	}
}

// liveApplyMode is one apply path the live stage feeds.
type liveApplyMode struct {
	name        string
	concurrency int
	batchSize   int
}

var liveApplyModes = []liveApplyMode{
	{"serial-batched", 1, 100},
	{"per-change", 1, 1},
	{"lanes", 2, 100},
}

// liveHeadRun is one live leg of the source-head pin: a stream into its own
// target database, stopped, the source given foreign-only traffic around one
// real transaction, then resumed.
type liveHeadRun struct {
	e        *emptyTxEnv
	target   string // the target database's DSN
	stream   string
	mode     liveApplyMode
	filter   migcore.TableFilter
	headOK   func(persisted string) bool // persisted position reaches the source head
	readHead func()                      // snapshot the head, after the traffic
}

func (r *liveHeadRun) streamer() *Streamer {
	eng, _ := engines.Get(r.e.engine)
	return &Streamer{
		Source: eng, Target: eng, SourceDSN: r.e.src, TargetDSN: r.target, StreamID: r.stream,
		ApplyConcurrency: r.mode.concurrency, ApplyBatchSize: r.mode.batchSize, Filter: r.filter,
	}
}

// run is the leg. Graded: the position-write count during the catch-up is
// bounded (Bug 300), the target equals the source, and — the independent
// expected value — once the stream is quiet, the position it PERSISTED
// reaches the source's own head read straight from the source server
// (`@@GLOBAL.gtid_executed` / `pg_current_wal_lsn()`): the trailing run of
// foreign transactions was held, and must still have been released.
func (r *liveHeadRun) run(t *testing.T, k int) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1 := startStream(ctx1, r.streamer())
	want := r.e.kdState(r.e.src)
	// Stop only once the stream has handed off to CDC and persisted a
	// position: a stop inside the copy (the rows can be visible before the
	// copy phase ends) leaves STOPPED-SLOT-KEPT and a re-run cold start
	// that refuses on the populated target, which is not the path under
	// test.
	run1.await(t, r.mode.name+": the cold start copying kd and persisting a position", 90*time.Second, 250*time.Millisecond,
		func() bool { return r.e.kdStateOrEmpty(r.target) == want && r.e.persisted(r.target, r.stream) != "" },
		func() string { return "" })
	cancel1()
	run1.join()

	writes := r.e.countPositionWrites(r.target)
	before := writes()
	r.e.foreign(foreignTxs)
	r.e.real(k)
	r.e.foreign(2 * foreignTxs) // the stream ends on a foreign-only run
	r.readHead()
	want = r.e.kdState(r.e.src)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	run2 := startStream(ctx2, r.streamer())
	run2.await(t, r.mode.name+": the warm resume applying the real transaction", 2*time.Minute, 100*time.Millisecond,
		func() bool { return r.e.kdStateOrEmpty(r.target) == want },
		func() string {
			return fmt.Sprintf("target sessions: %s; source sessions: %s", r.e.sessions(r.target), r.e.sessions(r.e.src))
		})
	caughtUp := writes() - before
	var pos string
	run2.await(t, r.mode.name+": the persisted position reaching the source head after a foreign-only tail", 60*time.Second, 200*time.Millisecond,
		func() bool {
			pos = r.e.persisted(r.target, r.stream)
			return pos != "" && r.headOK(pos)
		},
		func() string { return "persisted " + pos })
	cancel2()
	run2.join()
	t.Logf("%s: %d position writes up to the real transaction for %d foreign transactions; position reached the source head", r.mode.name, caughtUp, 3*foreignTxs)
	if caughtUp > foreignTxs {
		t.Errorf("%s: %d position writes for %d foreign transactions; want at most %d (one per foreign transaction is Bug 300)", r.mode.name, caughtUp, 3*foreignTxs, foreignTxs)
	}
}

// runningStream is a Streamer.Run in flight. Its await is the only way this
// suite polls the target while a stream runs, because a poll that does not
// also watch the stream reads an early return as a stall: a re-run cold
// start that REFUSED in under two seconds, loudly, was recorded as a
// two-minute hang "right after snapshot captured" with idle sessions on
// both servers (COLDSTART-RERUN-STALL, 2026-10-10 — the refusal was sitting
// unread in the done channel the whole time).
type runningStream struct{ done chan error }

func startStream(ctx context.Context, s *Streamer) *runningStream {
	r := &runningStream{done: make(chan error, 1)}
	go func() { r.done <- s.Run(ctx) }()
	return r
}

// await polls cond until it holds. It fails at once, with the stream's own
// error, if the stream returns first — nothing under test returns before it
// is stopped — and with diag() if timeout passes with the stream running.
func (r *runningStream) await(t *testing.T, what string, timeout, every time.Duration, cond func() bool, diag func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		select {
		case err := <-r.done:
			t.Fatalf("waiting for %s: the stream RETURNED instead: %v", what, err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s: still not there after %s with the stream running; %s", what, timeout, diag())
		}
		time.Sleep(every)
	}
}

// join waits for a stopped stream to return.
func (r *runningStream) join() { <-r.done }

// kdStateOrEmpty is kdState, reading "" while the table does not exist yet.
func (e *emptyTxEnv) kdStateOrEmpty(dsn string) string {
	db, err := sql.Open(e.driver, dsn)
	if err != nil {
		return ""
	}
	defer func() { _ = db.Close() }()
	q := "SELECT COALESCE(GROUP_CONCAT(CONCAT(id,':',note) ORDER BY id),'') FROM kd"
	if e.driver == "pgx" {
		q = "SELECT COALESCE(string_agg(id||':'||note, ',' ORDER BY id),'') FROM kd"
	}
	var s sql.NullString
	if err := db.QueryRowContext(context.Background(), q).Scan(&s); err != nil {
		return ""
	}
	return s.String
}

// sessions renders the target server's live sessions, for a failure message.
func (e *emptyTxEnv) sessions(dsn string) string {
	q := "SELECT COALESCE(GROUP_CONCAT(CONCAT(id,' ',command,' ',time,'s ',COALESCE(state,''),' ',COALESCE(LEFT(info,80),'')) SEPARATOR ' | '),'') FROM information_schema.processlist WHERE command <> 'Daemon'"
	if e.driver == "pgx" {
		q = "SELECT COALESCE(string_agg(pid||' '||state||' '||COALESCE(wait_event,'')||' '||left(query,80), ' | '),'') FROM pg_stat_activity WHERE backend_type = 'client backend'"
	}
	db, err := sql.Open(e.driver, dsn)
	if err != nil {
		return err.Error()
	}
	defer func() { _ = db.Close() }()
	var s string
	if err := db.QueryRowContext(context.Background(), q).Scan(&s); err != nil {
		return err.Error()
	}
	return s
}

// persisted reads a stream's persisted source position, "" when absent.
func (e *emptyTxEnv) persisted(dsn, stream string) string {
	db, err := sql.Open(e.driver, dsn)
	if err != nil {
		return ""
	}
	defer func() { _ = db.Close() }()
	q := "SELECT source_position FROM sluice_cdc_state WHERE stream_id = ?"
	if e.driver == "pgx" {
		q = "SELECT source_position FROM sluice_cdc_state WHERE stream_id = $1"
	}
	var s string
	if err := db.QueryRowContext(context.Background(), q, stream).Scan(&s); err != nil {
		return ""
	}
	return s
}

// TestEmptyTxRuns_LiveSyncReachesSourceHead_MySQLGTID runs the live leg on
// every apply path the stage feeds — serial batched, per-change, the lanes —
// with the target on a SEPARATE server, so nothing but the test writes the
// source and its head stands still once the traffic stops.
func TestEmptyTxRuns_LiveSyncReachesSourceHead_MySQLGTID(t *testing.T) {
	src, _, cleanupSrc := startMySQLGTID(t)
	t.Cleanup(cleanupSrc)
	_, tgtServer, cleanupTgt := startMySQLGTID(t)
	t.Cleanup(cleanupTgt)
	e := &emptyTxEnv{sevEnv: &sevEnv{t: t, engine: "mysql", driver: "mysql", gtid: true, src: src}, foreignPairs: true}
	e.exec(`CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, note VARCHAR(16)) ENGINE=InnoDB;
		CREATE DATABASE etx_noise; CREATE TABLE etx_noise.n (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB;
		INSERT INTO kd VALUES (0,'seed');`)
	for i, mode := range liveApplyModes {
		t.Run(mode.name, func(t *testing.T) {
			e.t = t
			db := fmt.Sprintf("etx_live_%d", i)
			applyDDLMySQL(t, tgtServer, "CREATE DATABASE "+db)
			target, err := buildMySQLDSN(tgtServer, db)
			if err != nil {
				t.Fatal(err)
			}
			var head string
			r := &liveHeadRun{e: e, target: target, stream: "etx-head-" + mode.name, mode: mode}
			r.readHead = func() { head = sevQuery(t, "mysql", src, "SELECT @@GLOBAL.gtid_executed") }
			r.headOK = func(pos string) bool {
				set := decodePersistedMySQLToken(t, pos).GTIDSet
				// Equal sets: each a subset of the other, judged by the server.
				return sevQuery(t, "mysql", src, fmt.Sprintf("SELECT GTID_SUBSET('%s','%s') AND GTID_SUBSET('%s','%s')", head, set, set, head)) == "1"
			}
			r.run(t, 10+i)
		})
	}
}

// TestEmptyTxRuns_LiveSyncReachesSourceHead_Postgres14 is the Postgres
// version that decodes a transaction touching no published table as an
// empty pair. The target database shares the server, which logical decoding
// does not see (it is per database), but its writes do advance the server's
// WAL, so the pin is persisted >= the head read right after the traffic.
func TestEmptyTxRuns_LiveSyncReachesSourceHead_Postgres14(t *testing.T) {
	src, dst, cleanup := startPostgresLogicalImage(t, "postgres:14", 8)
	t.Cleanup(cleanup)
	e := &emptyTxEnv{sevEnv: &sevEnv{t: t, engine: "postgres", driver: "pgx", src: src, dst: dst}, foreignPairs: true}
	applyDDL(t, src, `
		CREATE TABLE kd (id INT PRIMARY KEY, note TEXT);
		CREATE TABLE etx_noise (id BIGSERIAL PRIMARY KEY);
		INSERT INTO kd VALUES (0,'seed');`)
	var head string
	r := &liveHeadRun{
		e: e, target: dst, stream: "etx-head-pg14", mode: liveApplyModes[0],
		filter: migcore.TableFilter{Include: []string{"kd"}},
	}
	r.readHead = func() { head = sevQuery(t, "pgx", src, "SELECT pg_current_wal_lsn()::text") }
	r.headOK = func(pos string) bool {
		var tok struct {
			LSN string `json:"lsn"`
		}
		if err := json.Unmarshal([]byte(pos), &tok); err != nil || tok.LSN == "" {
			t.Fatalf("persisted position %q carries no lsn: %v", pos, err)
		}
		return sevQuery(t, "pgx", src, fmt.Sprintf("SELECT ('%s'::pg_lsn >= '%s'::pg_lsn)::text", tok.LSN, head)) == "true"
	}
	r.run(t, 20)
}
