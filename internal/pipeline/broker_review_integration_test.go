//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Pins ported from the independent review of ADR-0191 (2026-10-07):
//
//   - TestBroker_ResetOverAPosition_Postgres — the review's MEDIUM:
//     --reset-target-data over a target that already holds a broker
//     position used to warm-resume instead, so it re-hit the refusal it is
//     the prescribed remedy for;
//   - TestBroker_CrashMidIncremental_MariaDB — the crash suite on the one
//     source family §13 R2 named as an unverified premise for the keyless lift;
//   - TestBackupChain_NoTransactionInTwoIncrementals_* — §13 R2's premise
//     itself, measured: across many resumed windows (stream runs and one-shot
//     incrementals) no source transaction's identity appears in two
//     incrementals, and the broker converges;
//   - TestBroker_CancelStorm_* — repeated abrupt cancels (not the failpoint)
//     of a broker replaying one large identity incremental, serial and lanes,
//     each re-run converging to the source (§4 C3: on the lanes, data can be
//     ahead of the persisted frontier).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestBroker_ResetOverAPosition_Postgres kills a broker inside an incremental
// (a v2 in_progress position), forges the position's chunk digest as smart
// compaction would have moved it, and grades both directions: a plain warm
// resume refuses BROKER-INCREMENTAL-REWRITTEN, and --reset-target-data — that
// refusal's remedy — rebuilds the target and converges to the SOURCE (the
// independent expected value). Before the fix the reset warm-resumed and
// refused again. It then plants broker-owned rows that do not decode (Bug
// 299) and grades the same two directions over each.
func TestBroker_ResetOverAPosition_Postgres(t *testing.T) {
	src, dst, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)
	e := &sevEnv{t: t, engine: "postgres", driver: "pgx", src: src, dst: dst}
	bc := newBrokerCrashChain(t, e)
	want := map[string]string{}
	for _, tbl := range brokerCrashTables {
		want[tbl] = bc.tableState(t, e.src, tbl)
	}
	dsn := bc.target(t, "rv_reset")
	j := len(bc.events) / 2
	for {
		if _, isRow := rowChangeTable(bc.events[j-1]); isRow {
			break
		}
		j++
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	err := bc.killBefore(bc.broker(dsn, "rv", 1, bc.fullID), j).Run(ctx)
	cancel()
	if !errorsIsKill(err) {
		t.Fatalf("no kill: %v", err)
	}
	pos, _ := bc.persisted(t, dsn, "rv")
	tok, derr := decodeBrokerPosition(ir.Position{Token: pos})
	if derr != nil || tok.InProgress == nil {
		t.Fatalf("expected an in_progress frontier at the kill, got %s (%v)", pos, derr)
	}
	forged := strings.Replace(pos, tok.InProgress.Chunks, strings.Repeat("0", 64), 1)
	applyDDL(t, dsn, "UPDATE sluice_cdc_state SET source_position = '"+forged+"' WHERE stream_id = 'rv'")

	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	err = bc.broker(dsn, "rv", 1, "").Run(ctx)
	cancel()
	if err == nil || !strings.Contains(err.Error(), string(BrokerIncrementalRewrittenMarker)) {
		t.Fatalf("warm resume over the forged digest: Run = %v; want %s", err, BrokerIncrementalRewrittenMarker)
	}

	b := bc.broker(dsn, "rv", 1, "")
	b.ResetTargetData = true
	bc.runToTailWith(t, dsn, b)
	for _, tbl := range brokerCrashTables {
		if got := bc.tableState(t, dsn, tbl); got != want[tbl] {
			t.Errorf("after --reset-target-data %s DIVERGES from the source: target {%s}, source {%s}", tbl, got, want[tbl])
		}
	}
	bc.gradeCorruptBrokerRows(t, dsn, "rv", want)
}

// TestBroker_CorruptBrokerRow_MySQLGTID is the MySQL-target cell of Bug 299:
// a broker at the chain's tail on a real MySQL target, then
// gradeCorruptBrokerRows over its real `sluice_cdc_state` (a LONGTEXT column,
// so invalid JSON is storable there as on Postgres).
func TestBroker_CorruptBrokerRow_MySQLGTID(t *testing.T) {
	src, dst, cleanup := startMySQLGTID(t)
	t.Cleanup(cleanup)
	e := &sevEnv{t: t, engine: "mysql", driver: "mysql", gtid: true, src: src, dst: dst}
	bc := newBrokerCrashChain(t, e)
	want := map[string]string{}
	for _, tbl := range brokerCrashTables {
		want[tbl] = bc.tableState(t, e.src, tbl)
	}
	dsn := bc.target(t, "rv_corrupt")
	b := bc.broker(dsn, "rvm", 1, "")
	b.ResetTargetData = true
	bc.runToTailWith(t, dsn, b)
	bc.gradeCorruptBrokerRows(t, dsn, "rvm", want)
}

// gradeCorruptBrokerRows is Bug 299 on a real control table: for every way a
// broker-owned row can fail to decode × both token generations, planted over
// the stream's row, a plain run refuses BROKER-POSITION-CORRUPT — not "owned by
// a non-broker writer" — and --reset-target-data is honoured over it and
// converges to the SOURCE (want, the independent expected value).
func (bc *brokerCrashChain) gradeCorruptBrokerRows(t *testing.T, dsn, stream string, want map[string]string) {
	t.Helper()
	plant := applyDDL
	if bc.e.driver == "mysql" {
		plant = applyDDLMySQL
	}
	for _, sentinel := range []string{BackupBrokerPositionEngine, BackupBrokerPositionEngineV2} {
		for name, bad := range corruptBrokerTokens(sentinel) {
			cell := sentinel + "/" + name
			plant(t, dsn, "UPDATE sluice_cdc_state SET source_position = '"+bad+"' WHERE stream_id = '"+stream+"'")
			if got := positionOrEmpty(bc.e.driver, dsn, stream); got != bad {
				t.Fatalf("%s: the planted row reads back as %q; want %q", cell, got, bad)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := bc.broker(dsn, stream, 1, "").Run(ctx)
			cancel()
			if err == nil || !strings.Contains(err.Error(), BrokerPositionCorruptMarker) || strings.Contains(err.Error(), "non-broker writer") {
				t.Fatalf("%s: plain run = %v; want %s", cell, err, BrokerPositionCorruptMarker)
			}
			b := bc.broker(dsn, stream, 1, "")
			b.ResetTargetData = true
			bc.runToTailWith(t, dsn, b)
			for _, tbl := range brokerCrashTables {
				if got := bc.tableState(t, dsn, tbl); got != want[tbl] {
					t.Errorf("%s: after --reset-target-data %s DIVERGES from the source: target {%s}, source {%s}", cell, tbl, got, want[tbl])
				}
			}
		}
	}
}

// errorsIsKill reports whether err is the crash suite's simulated kill.
func errorsIsKill(err error) bool {
	return err != nil && strings.Contains(err.Error(), errBrokerCrashKill.Error())
}

// runToTailWith runs b until its persisted position names the chain's tail,
// then cancels it.
func (bc *brokerCrashChain) runToTailWith(t *testing.T, dsn string, b *SyncFromBackup) {
	t.Helper()
	if err := bc.runToTailWithin(dsn, b, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
}

// runToTailWithin is runToTailWith returning its failure, for a caller on a
// goroutine other than the test's.
func (bc *brokerCrashChain) runToTailWithin(dsn string, b *SyncFromBackup, limit time.Duration) error {
	chain, err := (&SyncFromBackup{Store: bc.store, ChainURL: "x", StreamID: "x"}).brokerChain(context.Background())
	if err != nil {
		return err
	}
	tail := lineage.ManifestBackupID(chain[len(chain)-1].Manifest)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	deadline := time.Now().Add(limit)
	for {
		select {
		case err := <-done:
			return fmt.Errorf("the run exited before reaching the tail: %w", err)
		case <-time.After(200 * time.Millisecond):
		}
		if pos := positionOrEmpty(bc.e.driver, dsn, b.StreamID); strings.Contains(pos, `"last_applied_backup_id":"`+tail+`"`) {
			cancel()
			<-done
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the run never reached the chain's tail within %s", limit)
		}
	}
}

// positionOrEmpty reads stream's persisted broker position, or "" while the
// broker has not created its control tables yet.
func positionOrEmpty(driver, dsn, stream string) string {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return ""
	}
	defer func() { _ = db.Close() }()
	var s sql.NullString
	if err := db.QueryRowContext(context.Background(),
		"SELECT source_position FROM sluice_cdc_state WHERE stream_id = '"+stream+"'").Scan(&s); err != nil {
		return ""
	}
	return s.String
}

func TestBroker_CrashMidIncremental_MariaDB(t *testing.T) {
	src, cleanup := startMariaDBBinlog(t)
	t.Cleanup(cleanup)
	runBrokerCrashSuite(t, &sevEnv{t: t, engine: "mariadb", driver: "mysql", gtid: true, src: src, dst: src})
}

// noTxInTwoIncrementals builds a chain over many RESUMED windows (new stream
// runs and one-shot incrementals) and asserts §13 R2's premise: no apply
// identity's transaction appears in two incrementals, every row change
// carries an identity, the severed-transaction door passes, and the broker
// replays the chain to the source's state. domains > 1 spreads the
// transactions across MariaDB gtid_domain_id values.
func noTxInTwoIncrementals(t *testing.T, e *sevEnv, domains int) {
	t.Helper()
	ctx := context.Background()
	e.exec(`CREATE TABLE kl (v INT NOT NULL, note VARCHAR(16)) ENGINE=InnoDB;
		CREATE TABLE k (id INT NOT NULL PRIMARY KEY, note VARCHAR(16)) ENGINE=InnoDB;
		CREATE TABLE p2s (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
		INSERT INTO k VALUES (1,'one'),(3,'three');`)
	c := e.newChain(nil, false)
	eng, _ := engines.Get(e.engine)
	domain := func(i int) string {
		if domains <= 1 {
			return ""
		}
		return fmt.Sprintf("SET SESSION gtid_domain_id=%d; ", i%domains)
	}
	txn := func(i int) string {
		return fmt.Sprintf(`%sSTART TRANSACTION; INSERT INTO kl VALUES (%[2]d,'a'),(%[2]d,'b'); UPDATE k SET note='n%[2]d' WHERE id=1; INSERT INTO k VALUES (%[3]d,'x'); COMMIT;`, domain(i), i, 100+i)
	}
	n, sentinel := 0, 0
	for run := 0; run < 7; run++ {
		if run%2 == 1 {
			for j := 0; j < 3; j++ {
				n++
				e.exec(txn(n))
			}
			if err := (&IncrementalBackup{
				Source: eng, SourceDSN: e.src, Store: c.store, Window: 20 * time.Second,
				MaxChanges: 12, ChunkChanges: 3, SluiceVersion: "test",
			}).Run(ctx); err != nil {
				t.Fatalf("IncrementalBackup.Run: %v", err)
			}
			continue
		}
		stream := c.newStream(30 * time.Minute)
		stream.RolloverMaxChanges = 7
		sentinel++
		sv := sentinel
		seen := false
		stream.onWindowChange = func(ch ir.Change, inTx bool, _ *captureOutcome) {
			if ins, ok := ch.(ir.Insert); ok && sevTableIs(ins.Table, "p2s") && fmt.Sprint(ins.Row["id"]) == fmt.Sprint(sv) {
				seen = true
			}
			if seen && !inTx {
				seen = false
				notifyStreamStop(c.store)
			}
		}
		done := make(chan error, 1)
		go func() { done <- stream.Run(ctx) }()
		time.Sleep(500 * time.Millisecond)
		for j := 0; j < 5; j++ {
			n++
			e.exec(txn(n))
		}
		e.exec(fmt.Sprintf(`%sINSERT INTO p2s VALUES (%d)`, domain(sv), sv))
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("stream.Run: %v", err)
			}
		case <-time.After(3 * time.Minute):
			t.Fatal("the stream never reached its sentinel")
		}
	}

	chain, err := (&SyncFromBackup{Store: c.store, ChainURL: "x", StreamID: "x"}).brokerChain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seenIn := map[string]string{}
	incrs := 0
	for i := range chain {
		link := &chain[i]
		if lineage.CanonicalKind(link.Manifest.Kind) != irbackup.BackupKindIncremental {
			continue
		}
		incrs++
		id := lineage.ManifestBackupID(link.Manifest)
		local := map[string]bool{}
		for _, ev := range decodeIncrementalEvents(t, c.store, link) {
			if _, ok := rowChangeTable(ev); !ok {
				continue
			}
			a := ir.ApplyIDOf(ev)
			if a.IsZero() {
				t.Errorf("incremental %s: a row change %T carries no identity", id, ev)
				continue
			}
			local[a.TxID] = true
		}
		for tx := range local {
			if prev, ok := seenIn[tx]; ok {
				t.Errorf("§13 R2 PREMISE BROKEN: transaction %s appears in incremental %s and in %s", tx, prev, id)
			}
			seenIn[tx] = id
		}
	}
	t.Logf("%d incrementals, %d distinct transactions, %d source transactions", incrs, len(seenIn), n)
	if incrs < 6 || len(seenIn) < n {
		t.Fatalf("vacuous: %d incrementals carrying %d of %d source transactions", incrs, len(seenIn), n)
	}
	cmp, _ := eng.(ir.PositionMonotonicChecker)
	if err := backup.NewSeveredTransactionDoor(c.store, cmp, nil).Check(ctx, chain); err != nil {
		t.Errorf("the severed-transaction door refused the chain: %v", err)
	}
	dsn := e.database("r2_brk")
	if err := (&backup.Restore{Target: eng, TargetDSN: dsn, Store: c.store, SkipChainDispatch: true}).Run(ctx); err != nil {
		t.Fatalf("seed the broker target from the full: %v", err)
	}
	bc := &brokerCrashChain{e: e, store: c.store, fullID: c.fullID}
	b := &SyncFromBackup{
		Target: eng, TargetDSN: dsn, Store: c.store, ChainURL: "test://r2",
		StreamID: "r2", PollInterval: 300 * time.Millisecond, ApplyBatchSize: 100,
		AtChainID: c.fullID, SluiceVersion: "test", brokerStatePath: "manifests/broker_state_r2.json",
	}
	bc.runToTailWith(t, dsn, b)
	for _, q := range []string{
		"SELECT COALESCE(GROUP_CONCAT(CONCAT(v,':',note) ORDER BY v, note),'') FROM kl",
		"SELECT COALESCE(GROUP_CONCAT(CONCAT(id,':',note) ORDER BY id),'') FROM k",
	} {
		if s, g := sevQuery(t, "mysql", e.src, q), sevQuery(t, "mysql", dsn, q); s != g {
			t.Errorf("the broker DIVERGES on %q: source {%s} target {%s}", q, s, g)
		}
	}
}

func TestBackupChain_NoTransactionInTwoIncrementals_MariaDB(t *testing.T) {
	src, cleanup := startMariaDBBinlog(t)
	t.Cleanup(cleanup)
	noTxInTwoIncrementals(t, &sevEnv{t: t, engine: "mariadb", driver: "mysql", gtid: true, src: src, dst: src}, 1)
}

func TestBackupChain_NoTransactionInTwoIncrementals_MariaDBMultiDomain(t *testing.T) {
	src, cleanup := startMariaDBBinlog(t)
	t.Cleanup(cleanup)
	noTxInTwoIncrementals(t, &sevEnv{t: t, engine: "mariadb", driver: "mysql", gtid: true, src: src, dst: src}, 3)
}

func TestBackupChain_NoTransactionInTwoIncrementals_MySQLGTID(t *testing.T) {
	src, dst, cleanup := startMySQLGTID(t)
	t.Cleanup(cleanup)
	noTxInTwoIncrementals(t, &sevEnv{t: t, engine: "mysql", driver: "mysql", gtid: true, src: src, dst: dst}, 1)
}

// runCancelStorm captures one ntx-transaction identity incremental (a keyless
// table, a primary-key rotation through a temporary, two secondary-unique
// updates and a padding insert per transaction) and replays it into fresh
// targets — serial, lanes, and eight lanes, reps times each, in parallel — with
// the broker cancelled at a random moment on every run (a context deadline,
// which aborts the lane orchestrator without its final checkpoint), until a
// run reaches the tail. Every table must then equal the SOURCE's.
func runCancelStorm(t *testing.T, e *sevEnv, ntx, reps, maxRuns int) {
	pg := e.driver == "pgx"
	if pg {
		e.exec(`CREATE TABLE kl (v INT NOT NULL, note TEXT); ALTER TABLE kl REPLICA IDENTITY FULL;
			CREATE TABLE kr (id INT PRIMARY KEY, note TEXT);
			CREATE TABLE su (id INT PRIMARY KEY, u TEXT UNIQUE, v TEXT);
			CREATE TABLE pd (id INT PRIMARY KEY);
			INSERT INTO kr VALUES (1,'one'),(2,'two'); INSERT INTO su VALUES (1,'u0','x'),(2,'w0','y');
			CREATE PUBLICATION sluice_pub FOR ALL TABLES;`)
	} else {
		e.exec(`CREATE TABLE kl (v INT NOT NULL, note VARCHAR(16)) ENGINE=InnoDB;
			CREATE TABLE kr (id INT NOT NULL PRIMARY KEY, note VARCHAR(16)) ENGINE=InnoDB;
			CREATE TABLE su (id INT NOT NULL PRIMARY KEY, u VARCHAR(16) NULL UNIQUE, v VARCHAR(16)) ENGINE=InnoDB;
			CREATE TABLE pd (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
			INSERT INTO kr VALUES (1,'one'),(2,'two'); INSERT INTO su VALUES (1,'u0','x'),(2,'w0','y');`)
	}
	c := e.newChain(nil, false)
	eng, _ := engines.Get(e.engine)
	stream := &BackupStream{
		Source: eng, SourceDSN: e.src, Store: c.store, ParentRef: c.fullID,
		RolloverWindow: 30 * time.Minute, RolloverMaxChanges: 1 << 30, RolloverMaxBytes: 1 << 40,
		ChunkChanges: 7, SluiceVersion: "test", stopPollInterval: sevStopPoll,
	}
	seen := false
	stream.onWindowChange = func(ch ir.Change, inTx bool, _ *captureOutcome) {
		if ins, ok := ch.(ir.Insert); ok && sevTableIs(ins.Table, "pd") && fmt.Sprint(ins.Row["id"]) == "999999" {
			seen = true
		}
		if seen && !inTx {
			seen = false
			notifyStreamStop(c.store)
		}
	}
	done := make(chan error, 1)
	go func() { done <- stream.Run(context.Background()) }()
	time.Sleep(time.Second)
	begin := "BEGIN;"
	if !pg {
		begin = "START TRANSACTION;"
	}
	var all strings.Builder
	for i := 1; i <= ntx; i++ {
		fmt.Fprintf(&all, `%s INSERT INTO kl VALUES (%[2]d,'a'); INSERT INTO kl VALUES (%[2]d,'a'); DELETE FROM kl WHERE v = %[3]d;
UPDATE kr SET id = -1 WHERE id = 1; UPDATE kr SET id = 1 WHERE id = 2; UPDATE kr SET id = 2, note='n%[2]d' WHERE id = -1;
UPDATE su SET u = 'u%[2]d' WHERE id = 1; UPDATE su SET u = 'w%[2]d' WHERE id = 2; INSERT INTO pd VALUES (%[2]d); COMMIT;
`, begin, i, i-7)
		if i%50 == 0 {
			e.exec(all.String())
			all.Reset()
		}
	}
	e.exec(`INSERT INTO pd VALUES (999999)`)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stream.Run: %v", err)
		}
	case <-time.After(5 * time.Minute):
		t.Fatal("the capture never reached its sentinel")
	}
	chain, err := (&SyncFromBackup{Store: c.store, ChainURL: "x", StreamID: "x"}).brokerChain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tail := lineage.ManifestBackupID(chain[len(chain)-1].Manifest)
	bc := &brokerCrashChain{e: e, store: c.store, fullID: c.fullID}

	q := map[string]string{
		"kl": "SELECT COALESCE(GROUP_CONCAT(CONCAT(v,':',note) ORDER BY v, note),'') FROM kl",
		"kr": "SELECT COALESCE(GROUP_CONCAT(CONCAT(id,':',note) ORDER BY id),'') FROM kr",
		"su": "SELECT COALESCE(GROUP_CONCAT(CONCAT(id,':',u,':',v) ORDER BY id),'') FROM su",
		"pd": "SELECT CONCAT(COUNT(*),'/',COALESCE(SUM(id),0)) FROM pd",
	}
	if pg {
		q = map[string]string{
			"kl": "SELECT COALESCE(string_agg(v||':'||note, ',' ORDER BY v, note),'') FROM kl",
			"kr": "SELECT COALESCE(string_agg(id||':'||note, ',' ORDER BY id),'') FROM kr",
			"su": "SELECT COALESCE(string_agg(id||':'||u||':'||v, ',' ORDER BY id),'') FROM su",
			"pd": "SELECT COUNT(*)||'/'||COALESCE(SUM(id),0) FROM pd",
		}
	}
	want := map[string]string{}
	for k, s := range q {
		want[k] = sevQuery(t, e.driver, e.src, s)
	}
	atTail := func(dsn, stream string) bool {
		pos := positionOrEmpty(e.driver, dsn, stream)
		return strings.Contains(pos, `"last_applied_backup_id":"`+tail+`"`)
	}

	var wg sync.WaitGroup
	var partialsTotal sync.Map
	for _, mode := range []struct {
		name string
		conc int
	}{{"serial", 1}, {"lanes", 0}, {"lanes8", 8}} {
		for rep := 0; rep < reps; rep++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				name := fmt.Sprintf("cs_%s_%d", mode.name, rep)
				dsn := bc.target(t, name)
				r := rand.New(rand.NewSource(int64(rep*31 + mode.conc))) //nolint:gosec // a test's reproducible schedule
				base := 300
				if !pg {
					base = 1500
				}
				runs, partials := 0, 0
				for attempt := 0; attempt < maxRuns && !atTail(dsn, name); attempt++ {
					at := ""
					if positionOrEmpty(e.driver, dsn, name) == "" {
						at = c.fullID
					}
					b := bc.broker(dsn, name, mode.conc, at)
					b.ApplyBatchSize = 1 + r.Intn(4)
					b.PollInterval = 50 * time.Millisecond
					ctx, cancel := context.WithTimeout(context.Background(), time.Duration(base+r.Intn(base*3))*time.Millisecond)
					err := b.Run(ctx)
					cancel()
					runs++
					if err != nil {
						// A stop is graded by errors.Is, the contract every caller
						// uses — not by the message: a DNS lookup that hits the
						// run's deadline unwraps to context.DeadlineExceeded
						// (net.DNSError) while its text says only "i/o timeout".
						stopped := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
						if !stopped && !strings.Contains(err.Error(), BrokerIncrementalPartialMarker) {
							t.Errorf("[%s] run %d: unexpected error: %v", name, runs, err)
							return
						}
						partials++
					}
				}
				partialsTotal.Store(name, partials)
				if !atTail(dsn, name) {
					// The storm's runs are short; finish uninterrupted, batched.
					// With the same --at-chain-id rule as the storm: under load
					// every short run can be cancelled before its cold start
					// records a position (the keyless door decodes the whole
					// incremental first), and a finish without the assertion
					// then refuses as a cold start with no override.
					finishAt := ""
					if positionOrEmpty(e.driver, dsn, name) == "" {
						finishAt = c.fullID
					}
					fb := bc.broker(dsn, name, mode.conc, finishAt)
					fb.ApplyBatchSize = 200
					fb.PollInterval = 100 * time.Millisecond
					if err := bc.runToTailWithin(dsn, fb, 10*time.Minute); err != nil {
						t.Errorf("[%s] after %d interrupted run(s): %v", name, partials, err)
						return
					}
				}
				for k, s := range q {
					if got := sevQuery(t, e.driver, dsn, s); got != want[k] {
						t.Errorf("[%s] %s DIVERGES after %d interrupted run(s): target {%.300s} source {%.300s}", name, k, partials, got, want[k])
					}
				}
			}()
		}
	}
	wg.Wait()
	interrupted := 0
	partialsTotal.Range(func(_, v any) bool { interrupted += v.(int); return true })
	if interrupted == 0 {
		t.Fatal("no run was interrupted: the storm graded nothing")
	}
}

func TestBroker_CancelStorm_Postgres(t *testing.T) {
	src, dst, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)
	runCancelStorm(t, &sevEnv{t: t, engine: "postgres", driver: "pgx", src: src, dst: dst}, 300, 2, 40)
}

func TestBroker_CancelStorm_MySQLGTID(t *testing.T) {
	src, dst, cleanup := startMySQLGTID(t)
	t.Cleanup(cleanup)
	// Sized down from the review's 300 transactions x 2 reps x 40 runs (380 s
	// locally; 26 s at this size) for the CI shard's -race budget: each mode
	// is still interrupted up to 12 times before it finishes uninterrupted.
	runCancelStorm(t, &sevEnv{t: t, engine: "mysql", driver: "mysql", gtid: true, src: src, dst: dst}, 120, 1, 12)
}
