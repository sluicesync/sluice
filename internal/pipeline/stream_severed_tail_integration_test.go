//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// F-E1-SEVERED-TAIL-REPLAY, the real-engine matrix. Ported from the
// reproduction that measured the defect (all at exit 0, before the fix):
//
//   - Postgres and MySQL GTID: a `backup stream` stop / stop file / cancel
//     landing mid-transaction committed an incremental ending with T open;
//     the resumed incremental re-delivered ALL of T; chain restore into an
//     empty target and the broker both applied T's head twice — a keyless
//     table at 6 rows against the source's 3, and a key-reusing T
//     (`UPDATE 1→2, DELETE 2, UPDATE 3→1`) losing the row `1:three`.
//   - MySQL file/pos: the cut landed mid-ROWS-event and the resume started
//     AFTER the event (every row of it shares the event's end LogPos): a
//     filler of 40,000 rows restored as 39,996.
//
// The matrix is {Postgres, MySQL GTID, MySQL file/pos} × {in-process stop,
// stop file, ctx cancel, stop whose drain budget runs out} × one source
// transaction carrying all three shapes at once: a keyless insert, the
// key-reuse sequence, and a filler of sevFillerRows rows that spans many
// chunk flushes (ChunkChanges=sevChunkChanges) — on MySQL written as
// statements of sevStmtRows rows, each binlogged as ONE multi-row ROWS
// event, and on file/pos (where every row of an event shares the event's
// end position) the matrix asserts that at least one cut landed between
// two rows of one event. Every shape is graded against the SOURCE, the independent expected value: per-table multisets read back
// from the restored target and the broker target, never counts the chain
// records about itself.
//
// The cut is DETERMINISTIC: the stream's onWindowChange seam fires the
// trial's stop, stop file or cancel when the window has read row sevCutRow
// of the trial's own filler — inside the transaction by construction — and
// then slows the window a little so the exit is observed before the
// transaction's remaining rows drain. The stop-file trial shortens the
// stop poll through the stopPollInterval seam. That is what lets a
// transaction of a few thousand rows straddle every exit (the first port
// needed 50,000–250,000 rows to outrun a 1 s poll from outside, ~19 min of
// CI). A trial whose exit still did not land inside the transaction FAILS
// as vacuous — judged from what the window observed, independent of the
// fix — and each exit is also held to its mechanism: a stop commits the
// window at the transaction's commit, a cancel or a spent drain budget
// commits nothing.
//
// One container per test function: chain A (keyless + key-reuse + filler,
// all four exits, chain restore) and, on Postgres and MySQL GTID, chain B
// on the same server (key-reuse + filler only — the broker refuses keyless
// tables since v0.156.11 — two exits, chain restore AND the broker).
//
// The source-channel-close exit cannot be driven on a real engine without
// racing the cancel arm (the pump closes its channel on the same cancel);
// it is pinned deterministically in stream_severed_tail_test.go.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

const (
	sevFillerRows   = 2000
	sevStmtRows     = 50  // MySQL rows per filler statement (one ROWS event each)
	sevCutRow       = 225 // not a multiple of sevStmtRows: inside an event
	sevChunkChanges = 50
	sevStopPoll     = 20 * time.Millisecond
)

type sevStop string

const (
	sevStopInProcess sevStop = "stop"
	sevStopFile      sevStop = "stopfile"
	sevStopCancel    sevStop = "cancel"
	sevStopBudget    sevStop = "budget"
)

// sevTrial is one severed transaction: its tables carry the suffix name.
type sevTrial struct {
	name string
	mode sevStop
}

var (
	sevChainATrials = []sevTrial{{"stop", sevStopInProcess}, {"stopfile", sevStopFile}, {"cancel", sevStopCancel}, {"budget", sevStopBudget}}
	sevChainBTrials = []sevTrial{{"bstop", sevStopInProcess}, {"bcancel", sevStopCancel}}
)

// sevEnv is one engine's server and its source database.
type sevEnv struct {
	t      *testing.T
	engine string // registry name
	driver string // database/sql driver
	gtid   bool   // MySQL position mode
	src    string
	dst    string // empty target database on the same server
}

// sevChain is one backup chain on the env's source.
type sevChain struct {
	e        *sevEnv
	trials   []sevTrial
	keyless  bool
	store    *blobcodec.LocalStore
	fullID   string
	restore  string // chain-restore target (empty)
	brk      string // broker target, seeded from the full; "" = no broker leg
	streamed int

	// midEventCuts counts trials whose cut row and the row after it share a
	// position — on MySQL file/pos, a cut inside one multi-row ROWS event.
	midEventCuts int
}

func sevQuery(t *testing.T, driver, dsn, q string) string {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var s sql.NullString
	if err := db.QueryRowContext(context.Background(), q).Scan(&s); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return s.String
}

func (e *sevEnv) exec(q string) {
	e.t.Helper()
	dsn := e.src
	if e.driver == "mysql" && !strings.Contains(dsn, "multiStatements") {
		dsn += "&multiStatements=true"
	}
	db, err := sql.Open(e.driver, dsn)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, q); err != nil {
		e.t.Fatalf("exec: %v", err)
	}
}

// seed creates every trial's tables (and, for keyless, the keyless table).
func (e *sevEnv) seed(trials []sevTrial, keyless bool) {
	var b strings.Builder
	for _, s := range trials {
		if e.driver == "pgx" {
			fmt.Fprintf(&b, "CREATE TABLE k_%[1]s (id INT PRIMARY KEY, note TEXT); CREATE TABLE f_%[1]s (id INT PRIMARY KEY, pad TEXT); INSERT INTO k_%[1]s VALUES (1,'one'),(3,'three');\n", s.name)
			if keyless {
				fmt.Fprintf(&b, "CREATE TABLE kl_%[1]s (v INT NOT NULL, note TEXT); ALTER TABLE kl_%[1]s REPLICA IDENTITY FULL;\n", s.name)
			}
			continue
		}
		fmt.Fprintf(&b, "CREATE TABLE k_%[1]s (id INT NOT NULL PRIMARY KEY, note VARCHAR(64)) ENGINE=InnoDB; CREATE TABLE f_%[1]s (id INT NOT NULL PRIMARY KEY, pad VARCHAR(200)) ENGINE=InnoDB; INSERT INTO k_%[1]s VALUES (1,'one'),(3,'three');\n", s.name)
		if keyless {
			fmt.Fprintf(&b, "CREATE TABLE kl_%[1]s (v INT NOT NULL, note VARCHAR(64)) ENGINE=InnoDB;\n", s.name)
		}
	}
	e.exec(b.String())
}

// txSQL is the one source transaction a trial severs: the keyless insert,
// the key-reuse sequence, then the filler that spans many chunk flushes.
func (c *sevChain) txSQL(s sevTrial) string {
	kl := ""
	if c.keyless {
		kl = fmt.Sprintf("INSERT INTO kl_%s VALUES (1,'a'),(2,'b'),(3,'c');", s.name)
	}
	if c.e.driver == "pgx" {
		return fmt.Sprintf(`BEGIN; %[2]s
UPDATE k_%[1]s SET id = 2 WHERE id = 1; DELETE FROM k_%[1]s WHERE id = 2; UPDATE k_%[1]s SET id = 1 WHERE id = 3;
INSERT INTO f_%[1]s SELECT g, repeat('x', 100) FROM generate_series(1, %[3]d) g;
COMMIT;`, s.name, kl, sevFillerRows)
	}
	// MySQL: the filler is sevFillerRows/sevStmtRows statements of
	// sevStmtRows rows each. Each statement is one TABLE_MAP plus ONE
	// multi-row ROWS event (sevStmtRows rows of ~110 bytes stays under the
	// 8 KiB binlog_row_event_max_size), so a file/pos cut inside an event
	// resumes on the NEXT statement's TABLE_MAP — the silent-loss shape the
	// reproduction measured. (One INSERT … SELECT of thousands of rows
	// spans several ROWS events under ONE table map, and a resume between
	// them wedges loudly on `rows event for unknown table_id` instead.)
	var fill strings.Builder
	for lo := 1; lo <= sevFillerRows; lo += sevStmtRows {
		fmt.Fprintf(&fill, "INSERT INTO f_%s (id, pad) WITH RECURSIVE s(n) AS (SELECT %d UNION ALL SELECT n + 1 FROM s WHERE n < %d) SELECT n, REPEAT('x', 100) FROM s;\n",
			s.name, lo, lo+sevStmtRows-1)
	}
	return fmt.Sprintf(`START TRANSACTION; %[2]s
UPDATE k_%[1]s SET id = 2 WHERE id = 1; DELETE FROM k_%[1]s WHERE id = 2; UPDATE k_%[1]s SET id = 1 WHERE id = 3;
%[3]sCOMMIT;`, s.name, kl, fill.String())
}

func sevTableIs(name, want string) bool {
	return name == want || strings.HasSuffix(name, "."+want)
}

func (c *sevChain) newStream(window time.Duration) *BackupStream {
	eng, _ := engines.Get(c.e.engine)
	s := &BackupStream{
		Source: eng, SourceDSN: c.e.src, Store: c.store,
		RolloverWindow: window, RolloverMaxChanges: 1 << 30, RolloverMaxBytes: 1 << 40,
		ChunkChanges: sevChunkChanges, SluiceVersion: "test",
		stopPollInterval: sevStopPoll,
	}
	if c.streamed == 0 {
		s.ParentRef = c.fullID
	}
	c.streamed++
	return s
}

func (c *sevChain) writeStopFile() {
	t := c.e.t
	st, err := readStreamState(context.Background(), c.store, DefaultStreamStateFilename)
	if err != nil || st == nil {
		t.Errorf("read stream state: %v", err)
		return
	}
	now := time.Now().UTC()
	st.StopRequestedAt = &now
	if err := writeStreamState(context.Background(), c.store, DefaultStreamStateFilename, st); err != nil {
		t.Error(err)
	}
}

// sever runs one trial: a stream is up, the trial's transaction commits at
// the source, and the trial's exit fires at row sevCutRow of its filler.
func (c *sevChain) sever(s sevTrial) {
	t := c.e.t
	t.Helper()
	stream := c.newStream(30 * time.Minute)
	if s.mode == sevStopBudget {
		stream.StopTransactionDrainTimeout = time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	filler := "f_" + s.name
	var (
		rows           int
		fired          bool // the exit fired, inside the transaction by construction
		seenInside     bool // the window had latched the stop with the transaction still open
		commitAfterCut bool // the window read the transaction's TxCommit after the exit fired
		cutPos         ir.Position
		nextChecked    bool
	)
	stream.onWindowChange = func(ch ir.Change, inTx bool, out *captureOutcome) {
		if fired {
			if ins, ok := ch.(ir.Insert); ok && !nextChecked && sevTableIs(ins.Table, filler) {
				nextChecked = true
				if ins.Position == cutPos {
					c.midEventCuts++
				}
			}
			if _, ok := ch.(ir.TxCommit); ok && !inTx {
				commitAfterCut = true
			}
			if inTx && out.StopRequested {
				seenInside = true
			}
			// Give the exit a turn before the transaction drains: the stop
			// poll ticks every sevStopPoll, the budget timer at 1 ms, and
			// select picks among ready cases at random.
			if inTx && (!out.StopRequested || s.mode == sevStopBudget) || s.mode == sevStopCancel {
				time.Sleep(2 * time.Millisecond)
			}
			return
		}
		if ins, ok := ch.(ir.Insert); !ok || !inTx || !sevTableIs(ins.Table, filler) {
			return
		}
		if rows++; rows < sevCutRow {
			return
		}
		fired, cutPos = true, ch.Pos()
		switch s.mode {
		case sevStopInProcess, sevStopBudget:
			if !notifyStreamStop(c.store) {
				t.Errorf("[%s] no stream registered for the in-process stop", s.name)
			}
		case sevStopFile:
			c.writeStopFile()
		case sevStopCancel:
			cancel()
		}
	}
	before := c.incrementalCount()
	done := make(chan error, 1)
	go func() { done <- stream.Run(ctx) }()
	c.e.exec(c.txSQL(s))
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("[%s] stream.Run: %v", s.name, err)
		}
	case <-time.After(3 * time.Minute):
		t.Fatalf("[%s] stream.Run did not return (the cut never fired?)", s.name)
	}

	// The precondition, independent of the behaviour under test: the exit
	// took effect INSIDE the transaction — the window latched the stop while
	// the transaction was open, or stopped reading before its commit.
	// Without it the trial proves nothing (an exit after the commit was
	// always safe).
	if !fired || (!seenInside && commitAfterCut) {
		t.Fatalf("[%s] VACUOUS TRIAL: the exit did not land inside the transaction (fired=%v, latched-inside=%v, commit read after the cut=%v)",
			s.name, fired, seenInside, commitAfterCut)
	}
	// The verdict per exit. A stop drains to the commit and commits the
	// window (one new incremental, carrying the whole transaction); a cancel
	// or a drain that runs out commits NOTHING (the window is re-read by the
	// next run). The data grading at the end of the chain is the harm check;
	// this is the mechanism check.
	committed := c.incrementalCount() - before
	switch s.mode {
	case sevStopInProcess, sevStopFile:
		if committed != 1 || !commitAfterCut {
			t.Fatalf("[%s] a stop inside a transaction must drain to its commit and commit the window: %d incremental(s) committed, commit read=%v",
				s.name, committed, commitAfterCut)
		}
	case sevStopCancel, sevStopBudget:
		if committed != 0 {
			t.Fatalf("[%s] an exit that cannot reach the commit must ABANDON the window, but %d incremental(s) were committed",
				s.name, committed)
		}
	}
}

func (c *sevChain) incrementalCount() int {
	c.e.t.Helper()
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), c.store)
	if err != nil {
		c.e.t.Fatal(err)
	}
	n := 0
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			n++
		}
	}
	return n
}

// finish runs a last stream that re-reads anything abandoned plus a
// sentinel, and stops at the boundary right after the sentinel commits —
// so everything the source holds is in a committed incremental.
func (c *sevChain) finish() {
	t := c.e.t
	t.Helper()
	sentinel := "k_" + c.trials[0].name
	c.e.exec(`INSERT INTO ` + sentinel + ` VALUES (100, 'sentinel')`)
	stream := c.newStream(30 * time.Minute)
	seen := false
	stream.onWindowChange = func(ch ir.Change, inTx bool, _ *captureOutcome) {
		if ins, ok := ch.(ir.Insert); ok && sevTableIs(ins.Table, sentinel) {
			seen = true
		}
		if seen && !inTx {
			seen = false
			notifyStreamStop(c.store)
		}
	}
	done := make(chan error, 1)
	go func() { done <- stream.Run(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("final stream.Run: %v", err)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("final stream.Run never reached the sentinel")
	}
}

// compare grades every trial's tables on dsn against the source.
func (c *sevChain) compare(dsn, label string) {
	t := c.e.t
	t.Helper()
	for _, s := range c.trials {
		kq := fmt.Sprintf("SELECT COALESCE(GROUP_CONCAT(CONCAT(id,':',note) ORDER BY id),'') FROM k_%s", s.name)
		klq := fmt.Sprintf("SELECT COALESCE(GROUP_CONCAT(CONCAT(v,':',note) ORDER BY v, note),'') FROM kl_%s", s.name)
		fq := fmt.Sprintf("SELECT CONCAT(COUNT(*), '/', COALESCE(SUM(id),0)) FROM f_%s", s.name)
		if c.e.driver == "pgx" {
			kq = fmt.Sprintf("SELECT COALESCE(string_agg(id||':'||note, ',' ORDER BY id),'') FROM k_%s", s.name)
			klq = fmt.Sprintf("SELECT COALESCE(string_agg(v||':'||note, ',' ORDER BY v, note),'') FROM kl_%s", s.name)
			fq = fmt.Sprintf("SELECT COUNT(*)||'/'||COALESCE(SUM(id),0) FROM f_%s", s.name)
		}
		queries := map[string]string{"key-reuse k_" + s.name: kq, "multi-row filler f_" + s.name: fq}
		if c.keyless {
			queries["keyless kl_"+s.name] = klq
		}
		for what, q := range queries {
			src, got := sevQuery(t, c.e.driver, c.e.src, q), sevQuery(t, c.e.driver, dsn, q)
			if src != got {
				t.Errorf("[%s after %s] %s DIVERGES: source {%s}, target {%s}", label, s.name, what, src, got)
			}
		}
	}
}

func (c *sevChain) chainRestore() {
	t := c.e.t
	t.Helper()
	eng, _ := engines.Get(c.e.engine)
	if err := (&backup.ChainRestore{Target: eng, TargetDSN: c.restore, Store: c.store, ApplyConcurrency: 1}).Run(context.Background()); err != nil {
		t.Fatalf("chain restore into an empty target: %v", err)
	}
	c.compare(c.restore, "chain restore")
}

// chainTailID is the id of the chain's last link in CHAIN order.
func (c *sevChain) chainTailID() string {
	c.e.t.Helper()
	chain, err := (&SyncFromBackup{Store: c.store, ChainURL: "x", StreamID: "x"}).brokerChain(context.Background())
	if err != nil {
		c.e.t.Fatal(err)
	}
	return lineage.ManifestBackupID(chain[len(chain)-1].Manifest)
}

func (c *sevChain) broker() {
	t := c.e.t
	t.Helper()
	if c.brk == "" {
		return
	}
	eng, _ := engines.Get(c.e.engine)
	b := &SyncFromBackup{
		Target: eng, TargetDSN: c.brk, Store: c.store, ChainURL: "test://sev",
		StreamID: "sev", PollInterval: 500 * time.Millisecond, ApplyBatchSize: 100,
		ApplyConcurrency: 1, AtChainID: c.fullID, SluiceVersion: "test",
		brokerStatePath: "manifests/broker_state_sev.json",
	}
	tail := c.chainTailID()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	posQ := "SELECT COALESCE(string_agg(source_position, ','), '') FROM sluice_cdc_state WHERE stream_id = 'sev'"
	if c.e.driver == "mysql" {
		posQ = "SELECT COALESCE(GROUP_CONCAT(source_position), '') FROM sluice_cdc_state WHERE stream_id = 'sev'"
	}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		select {
		case err := <-done:
			t.Fatalf("broker exited early: %v", err)
		case <-time.After(250 * time.Millisecond):
		}
		pos := ""
		func() {
			defer func() { _ = recover() }()
			pos = sevQuery(t, c.e.driver, c.brk, posQ)
		}()
		if strings.Contains(pos, tail) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker never reached the chain tail %s (position %q)", tail, pos)
		}
	}
	cancel()
	<-done
	c.compare(c.brk, "broker")
}

// run drives one chain through its trials and grades it.
func (c *sevChain) run() {
	for _, s := range c.trials {
		c.sever(s)
	}
	c.finish()
	// The writer-side rule, read back from the chunks: no incremental this
	// binary wrote ends inside a transaction, so the door passes.
	chain, err := (&SyncFromBackup{Store: c.store, ChainURL: "x", StreamID: "x"}).brokerChain(context.Background())
	if err != nil {
		c.e.t.Fatal(err)
	}
	eng, _ := engines.Get(c.e.engine)
	cmp, _ := eng.(ir.PositionMonotonicChecker)
	if err := backup.NewSeveredTransactionDoor(c.store, cmp, nil).Check(context.Background(), chain); err != nil {
		c.e.t.Fatalf("this binary wrote a chain the severed-transaction door refuses: %v", err)
	}
	c.chainRestore()
	c.broker()
}

// newChain takes a full backup of the env's source into a fresh store.
// Postgres: the slot is (re)created first and the full anchored at it.
func (e *sevEnv) newChain(trials []sevTrial, keyless bool) *sevChain {
	t := e.t
	t.Helper()
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng, _ := engines.Get(e.engine)
	var anchor ir.Position
	if e.driver == "pgx" {
		dropPGLogicalSlot(t, e.src, "sluice_slot")
		lsn, err := createPGLogicalSlotReturningLSN(t, e.src, "sluice_slot")
		if err != nil {
			t.Fatalf("create slot: %v", err)
		}
		t.Cleanup(func() { dropPGLogicalSlot(t, e.src, "sluice_slot") })
		anchor = ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"slot":"sluice_slot","lsn":%q}`, lsn)}
	}
	if err := (&backup.Backup{Source: eng, SourceDSN: e.src, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case e.driver == "pgx":
		full.EndPosition = anchor
	case full.EndPosition.Token == "" && e.gtid:
		set := strings.ReplaceAll(sevQuery(t, "mysql", e.src, "SELECT @@GLOBAL.gtid_executed"), "\n", "")
		full.EndPosition = ir.Position{Engine: "mysql", Token: fmt.Sprintf(`{"mode":"gtid","gtid_set":%q}`, set)}
	case full.EndPosition.Token == "":
		file, pos := readMySQLBinlogPos(t, e.src)
		full.EndPosition = ir.Position{Engine: "mysql", Token: fmt.Sprintf(`{"mode":"file_pos","file":%q,"pos":%d}`, file, pos)}
	}
	full.Kind = irbackup.BackupKindFull
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatal(err)
	}
	return &sevChain{e: e, trials: trials, keyless: keyless, store: store, fullID: full.BackupID}
}

// database creates an empty database on the env's server and returns its DSN.
func (e *sevEnv) database(name string) string {
	t := e.t
	t.Helper()
	if e.driver == "pgx" {
		applyDDL(t, e.dst, "CREATE DATABASE "+name)
		dsn, err := buildPGDSN(e.dst, name)
		if err != nil {
			t.Fatal(err)
		}
		return dsn
	}
	applyDDLMySQL(t, e.src, "CREATE DATABASE "+name)
	dsn, err := buildMySQLDSN(e.src, name)
	if err != nil {
		t.Fatal(err)
	}
	return dsn
}

// matrix runs chain A (keyless, all four exits, chain restore) and, when
// withBroker, chain B (keyed, two exits, chain restore and the broker) on
// one server.
func (e *sevEnv) matrix(withBroker bool) {
	t := e.t
	e.seed(sevChainATrials, true)
	if withBroker {
		e.seed(sevChainBTrials, false)
	}
	if e.driver == "pgx" {
		applyDDL(t, e.src, `CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
	}

	a := e.newChain(sevChainATrials, true)
	a.restore = e.dst
	a.run()
	if e.driver == "mysql" && !e.gtid && a.midEventCuts == 0 {
		t.Fatal("VACUOUS FILE/POS CELL: no trial's cut landed between two rows of one ROWS event (the cut row and the next never shared a position), so the mid-event shape was not exercised")
	}
	if !withBroker {
		return
	}

	// Chain B's full must not record a keyless table, or the broker refuses
	// the whole chain (audit F-E1): drop chain A's keyless tables first.
	var drop strings.Builder
	for _, s := range sevChainATrials {
		fmt.Fprintf(&drop, "DROP TABLE kl_%s;", s.name)
	}
	e.exec(drop.String())
	b := e.newChain(sevChainBTrials, false)
	b.restore = e.database("sev_restore_b")
	b.brk = e.database("sev_brk")
	eng, _ := engines.Get(e.engine)
	if err := (&backup.Restore{Target: eng, TargetDSN: b.brk, Store: b.store}).Run(context.Background()); err != nil {
		t.Fatalf("seed broker target: %v", err)
	}
	b.run()
}

func TestSeveredTail_PG(t *testing.T) {
	src, dst, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)
	(&sevEnv{t: t, engine: "postgres", driver: "pgx", src: src, dst: dst}).matrix(true)
}

func TestSeveredTail_MySQLGTID(t *testing.T) {
	src, dst, cleanup := startMySQLGTID(t)
	t.Cleanup(cleanup)
	(&sevEnv{t: t, engine: "mysql", driver: "mysql", gtid: true, src: src, dst: dst}).matrix(true)
}

func TestSeveredTail_MySQLFilePos(t *testing.T) {
	src, dst, cleanup := startMySQLBinlog(t)
	t.Cleanup(cleanup)
	(&sevEnv{t: t, engine: "mysql", driver: "mysql", src: src, dst: dst}).matrix(false)
}
