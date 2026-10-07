//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0191 §9 P6 and P7 against real servers, through `sync from-backup`:
// a broker interrupted at EVERY statement of an incremental resumes and
// converges to the SOURCE, on Postgres, MySQL GTID and MySQL file/pos, serial
// and lanes, with a keyless table, a secondary-unique table, and the two key-
// reuse shapes audit F-E1-KEY-REUSE-REPLAY measured — each as one source
// transaction and split into one transaction per statement.
//
// THE KILL (§13 R12). The incremental is captured one change per chunk, and a
// cell arms its broker's replayChunkFailpoint at chunk j: the broker streams events
// 0..j-1, the applier commits what it received when its channel closes —
// mid-transaction, without a position, exactly as a crash leaves a run that
// committed each row on its own (--apply-batch-size 1) — and the run dies
// before chunk j. A store-side chunk fault cannot be the kill: the keyless
// door reads every chunk of the incremental before anything is applied, so it
// refuses there. The workload is padded by a transaction at each end.
//
// THE GRADING. At the kill: the persisted position names the full as last
// applied and, when it stands inside the incremental, an event that is a
// source-transaction boundary below j (serial: the LAST such event; lanes: one
// at or below it, never ahead); every apply mark names the transaction j-1
// belongs to, never another. After the re-run: every table equals the
// SOURCE's — the independent expected value — row for row (a multiset on the
// keyless table).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// brokerCrashTables are the tables the crash suite grades.
var brokerCrashTables = []string{"kl", "su", "ka", "kb", "kc", "kd", "pd"}

// brokerCrashSeed creates the tables: kl keyless, su with a secondary UNIQUE,
// ka..kd the key-reuse tables (kc and kd seeded {1, 3} for the second shape),
// pd the padding.
func brokerCrashSeed(driver string) string {
	if driver == "pgx" {
		return `
			CREATE TABLE kl (v INT NOT NULL, note TEXT); ALTER TABLE kl REPLICA IDENTITY FULL;
			CREATE TABLE su (id INT PRIMARY KEY, u TEXT UNIQUE, v TEXT);
			CREATE TABLE ka (id INT PRIMARY KEY, note TEXT); CREATE TABLE kb (id INT PRIMARY KEY, note TEXT);
			CREATE TABLE kc (id INT PRIMARY KEY, note TEXT); CREATE TABLE kd (id INT PRIMARY KEY, note TEXT);
			CREATE TABLE pd (id INT PRIMARY KEY);
			INSERT INTO kc VALUES (1,'one'),(3,'three'); INSERT INTO kd VALUES (1,'one'),(3,'three');
			INSERT INTO kl VALUES (0,'seed'); INSERT INTO pd VALUES (0);`
	}
	return `
		CREATE TABLE kl (v INT NOT NULL, note VARCHAR(16)) ENGINE=InnoDB;
		CREATE TABLE su (id INT NOT NULL PRIMARY KEY, u VARCHAR(16) NULL UNIQUE, v VARCHAR(16)) ENGINE=InnoDB;
		CREATE TABLE ka (id INT NOT NULL PRIMARY KEY, note VARCHAR(16)) ENGINE=InnoDB; CREATE TABLE kb (id INT NOT NULL PRIMARY KEY, note VARCHAR(16)) ENGINE=InnoDB;
		CREATE TABLE kc (id INT NOT NULL PRIMARY KEY, note VARCHAR(16)) ENGINE=InnoDB; CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, note VARCHAR(16)) ENGINE=InnoDB;
		CREATE TABLE pd (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
		INSERT INTO kc VALUES (1,'one'),(3,'three'); INSERT INTO kd VALUES (1,'one'),(3,'three');
		INSERT INTO kl VALUES (0,'seed'); INSERT INTO pd VALUES (0);`
}

// brokerCrashTraffic is the captured workload, one string per source
// transaction: padding, a keyless + secondary-unique transaction, the
// [INSERT 1, UPDATE 1→2] shape as one transaction (ka) and as two (kb), the
// [UPDATE 1→2, DELETE 2, UPDATE 3→1] shape as one (kc) and as three (kd), a
// second keyless + secondary-unique transaction, padding.
func brokerCrashTraffic(driver string) []string {
	b := "BEGIN;"
	if driver == "mysql" {
		b = "START TRANSACTION;"
	}
	tx := func(stmts string) string { return b + " " + stmts + " COMMIT;" }
	return []string{
		tx(`INSERT INTO pd VALUES (1);`),
		tx(`INSERT INTO kl VALUES (1,'a'); INSERT INTO kl VALUES (1,'a'); INSERT INTO su VALUES (1,'u1','x'); UPDATE su SET v = 'y' WHERE id = 1;`),
		tx(`INSERT INTO ka VALUES (1,'r'); UPDATE ka SET id = 2 WHERE id = 1;`),
		tx(`INSERT INTO kb VALUES (1,'r');`),
		tx(`UPDATE kb SET id = 2 WHERE id = 1;`),
		tx(`UPDATE kc SET id = 2 WHERE id = 1; DELETE FROM kc WHERE id = 2; UPDATE kc SET id = 1 WHERE id = 3;`),
		tx(`UPDATE kd SET id = 2 WHERE id = 1;`),
		tx(`DELETE FROM kd WHERE id = 2;`),
		tx(`UPDATE kd SET id = 1 WHERE id = 3;`),
		tx(`INSERT INTO kl VALUES (2,'b'); INSERT INTO kl VALUES (1,'a'); UPDATE su SET u = 'u2' WHERE id = 1;`),
		tx(`INSERT INTO pd VALUES (2);`),
	}
}

// brokerCrashChain is a captured chain: its data incremental (one change per
// chunk) and the decoded events.
type brokerCrashChain struct {
	e      *sevEnv
	store  *blobcodec.LocalStore
	fullID string
	incr   lineage.SegmentRecord
	events []ir.Change
}

// newBrokerCrashChain seeds the source, takes the anchored full, and captures
// the traffic (and a sentinel) with `backup stream` at one change per chunk.
func newBrokerCrashChain(t *testing.T, e *sevEnv) *brokerCrashChain {
	t.Helper()
	e.exec(brokerCrashSeed(e.driver))
	if e.driver == "pgx" {
		applyDDL(t, e.src, `CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
	}
	c := e.newChain(nil, false)
	eng, _ := engines.Get(e.engine)
	stream := &BackupStream{
		Source: eng, SourceDSN: e.src, Store: c.store, ParentRef: c.fullID,
		RolloverWindow: 30 * time.Minute, RolloverMaxChanges: 1 << 30, RolloverMaxBytes: 1 << 40,
		ChunkChanges: 1, SluiceVersion: "test", stopPollInterval: sevStopPoll,
	}
	seen := false
	stream.onWindowChange = func(ch ir.Change, inTx bool, _ *captureOutcome) {
		if ins, ok := ch.(ir.Insert); ok && sevTableIs(ins.Table, "pd") {
			if v, _ := ins.Row["id"].(int64); v == 99 || fmt.Sprint(ins.Row["id"]) == "99" {
				seen = true
			}
		}
		if seen && !inTx {
			seen = false
			notifyStreamStop(c.store)
		}
	}
	done := make(chan error, 1)
	go func() { done <- stream.Run(context.Background()) }()
	for _, tx := range brokerCrashTraffic(e.driver) {
		e.exec(tx)
	}
	e.exec(`INSERT INTO pd VALUES (99)`)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stream.Run: %v", err)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("the capture never reached the sentinel")
	}
	bc := &brokerCrashChain{e: e, store: c.store, fullID: c.fullID}
	chain, err := (&SyncFromBackup{Store: c.store, ChainURL: "x", StreamID: "x"}).brokerChain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := range chain {
		if lineage.CanonicalKind(chain[i].Manifest.Kind) == irbackup.BackupKindIncremental &&
			(bc.incr.Manifest == nil || len(chain[i].Manifest.ChangeChunks) > len(bc.incr.Manifest.ChangeChunks)) {
			bc.incr = chain[i]
		}
	}
	if bc.incr.Manifest == nil || !bc.incr.Manifest.ApplyIdentity {
		t.Fatal("the capture wrote no identity-bearing incremental")
	}
	bc.events = decodeIncrementalEvents(t, c.store, &bc.incr)
	if len(bc.events) != len(bc.incr.Manifest.ChangeChunks) {
		t.Fatalf("%d events in %d chunks; the suite needs one change per chunk", len(bc.events), len(bc.incr.Manifest.ChangeChunks))
	}
	return bc
}

// decodeIncrementalEvents decodes every event of link in order.
func decodeIncrementalEvents(t *testing.T, store irbackup.Store, link *lineage.SegmentRecord) []ir.Change {
	t.Helper()
	ctx := context.Background()
	var out []ir.Change
	for idx, chunk := range link.Manifest.ChangeChunks {
		src, err := blobcodec.FetchChunkVerified(ctx, link.Segment.Store(store), chunk.File, chunk.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		cr, err := blobcodec.NewChangeChunkReader(src, chunk.SHA256, nil, link.Segment.CodecOrDefault(), irbackup.ChangeChunkAADFor(link.Manifest, chunk, idx))
		if err != nil {
			t.Fatal(err)
		}
		for {
			ch, err := cr.ReadChange()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, ch)
		}
		_ = cr.Close()
	}
	return out
}

// lastBoundaryBelow is the ordinal of the last boundary (a TxCommit, or a row
// change outside a transaction) among events[0..j-1], or -1.
func lastBoundaryBelow(events []ir.Change, j int) int64 {
	last, open := int64(-1), false
	for i := 0; i < j; i++ {
		switch events[i].(type) {
		case ir.TxBegin:
			open = true
		case ir.TxCommit:
			open = false
			last = int64(i)
		default:
			if !open {
				last = int64(i)
			}
		}
	}
	return last
}

// txOfOrdinal is the identity TxID of the open transaction event i belongs
// to, or "" when i is a TxCommit, lies outside every transaction, or its
// transaction's changes carry no identity.
func txOfOrdinal(events []ir.Change, i int) string {
	if _, ok := events[i].(ir.TxCommit); ok {
		return ""
	}
	for k := i; k >= 0; k-- {
		if _, ok := events[k].(ir.TxCommit); ok && k < i {
			return "" // a commit closes every transaction before i
		}
		if _, ok := events[k].(ir.TxBegin); !ok {
			continue
		}
		for m := k + 1; m < len(events); m++ {
			if _, ok := events[m].(ir.TxCommit); ok {
				return ""
			}
			if id := ir.ApplyIDOf(events[m]); !id.IsZero() {
				return id.TxID
			}
		}
		return ""
	}
	return ""
}

// tableState renders a table's rows for comparison, ordered (a multiset for
// the keyless one).
func (bc *brokerCrashChain) tableState(t *testing.T, dsn, table string) string {
	t.Helper()
	q := map[string]string{
		"kl": "SELECT COALESCE(GROUP_CONCAT(CONCAT(v,':',note) ORDER BY v, note),'') FROM kl",
		"su": "SELECT COALESCE(GROUP_CONCAT(CONCAT(id,':',u,':',v) ORDER BY id),'') FROM su",
		"pd": "SELECT COALESCE(GROUP_CONCAT(id ORDER BY id),'') FROM pd",
	}
	pg := map[string]string{
		"kl": "SELECT COALESCE(string_agg(v||':'||note, ',' ORDER BY v, note),'') FROM kl",
		"su": "SELECT COALESCE(string_agg(id||':'||u||':'||v, ',' ORDER BY id),'') FROM su",
		"pd": "SELECT COALESCE(string_agg(id::text, ',' ORDER BY id),'') FROM pd",
	}
	sqlText, ok := q[table]
	if bc.e.driver == "pgx" {
		sqlText, ok = pg[table]
	}
	if !ok {
		sqlText = fmt.Sprintf("SELECT COALESCE(GROUP_CONCAT(CONCAT(id,':',note) ORDER BY id),'') FROM %s", table)
		if bc.e.driver == "pgx" {
			sqlText = fmt.Sprintf("SELECT COALESCE(string_agg(id||':'||note, ',' ORDER BY id),'') FROM %s", table)
		}
	}
	return sevQuery(t, bc.e.driver, dsn, sqlText)
}

// target creates an empty database and restores the full into it.
func (bc *brokerCrashChain) target(t *testing.T, name string) string {
	t.Helper()
	// e.database, with the CELL's t (cells run in parallel).
	var dsn string
	var err error
	if bc.e.driver == "pgx" {
		applyDDL(t, bc.e.dst, "CREATE DATABASE "+name)
		dsn, err = buildPGDSN(bc.e.dst, name)
	} else {
		applyDDLMySQL(t, bc.e.src, "CREATE DATABASE "+name)
		dsn, err = buildMySQLDSN(bc.e.src, name)
	}
	if err != nil {
		t.Fatal(err)
	}
	eng, _ := engines.Get(bc.e.engine)
	if err := (&backup.Restore{Target: eng, TargetDSN: dsn, Store: bc.store, SkipChainDispatch: true}).Run(context.Background()); err != nil {
		t.Fatalf("restore the full into %s: %v", name, err)
	}
	return dsn
}

func (bc *brokerCrashChain) broker(dsn, streamID string, conc int, atChain string) *SyncFromBackup {
	eng, _ := engines.Get(bc.e.engine)
	return &SyncFromBackup{
		Target: eng, TargetDSN: dsn, Store: bc.store, ChainURL: "test://" + streamID,
		StreamID: streamID, PollInterval: 500 * time.Millisecond, ApplyBatchSize: 1,
		ApplyConcurrency: conc, AtChainID: atChain, SluiceVersion: "test",
		brokerStatePath: "manifests/broker_state_" + streamID + ".json",
	}
}

// errBrokerCrashKill is the simulated kill.
var errBrokerCrashKill = errors.New("simulated kill")

// killBefore arms b's replay failpoint to kill it just before chunk j of the
// suite's incremental is read (ADR-0191 §14), and returns b.
func (bc *brokerCrashChain) killBefore(b *SyncFromBackup, j int) *SyncFromBackup {
	id := lineage.ManifestBackupID(bc.incr.Manifest)
	b.replayChunkFailpoint = func(backupID string, chunkIdx int) error {
		if backupID == id && chunkIdx == j {
			return errBrokerCrashKill
		}
		return nil
	}
	return b
}

// positionQ and marksQ read the broker's control rows.
func (bc *brokerCrashChain) persisted(t *testing.T, dsn, streamID string) (pos string, marks []string) {
	t.Helper()
	posQ := "SELECT COALESCE(string_agg(source_position, ','), '') FROM sluice_cdc_state WHERE stream_id = '" + streamID + "'"
	marksQ := "SELECT COALESCE(string_agg(DISTINCT tx_id, ','), '') FROM sluice_cdc_apply_marks WHERE stream_id = '" + streamID + "'"
	if bc.e.driver == "mysql" {
		posQ = "SELECT COALESCE(GROUP_CONCAT(source_position), '') FROM sluice_cdc_state WHERE stream_id = '" + streamID + "'"
		marksQ = "SELECT COALESCE(GROUP_CONCAT(DISTINCT tx_id), '') FROM sluice_cdc_apply_marks WHERE stream_id = '" + streamID + "'"
	}
	pos = sevQuery(t, bc.e.driver, dsn, posQ)
	if m := sevQuery(t, bc.e.driver, dsn, marksQ); m != "" {
		marks = strings.Split(m, ",")
	}
	return pos, marks
}

// runToTail runs a warm broker until its position names the chain's tail.
func (bc *brokerCrashChain) runToTail(t *testing.T, dsn, streamID string, conc int) {
	t.Helper()
	tail := ""
	chain, err := (&SyncFromBackup{Store: bc.store, ChainURL: "x", StreamID: "x"}).brokerChain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tail = lineage.ManifestBackupID(chain[len(chain)-1].Manifest)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- bc.broker(dsn, streamID, conc, "").Run(ctx) }()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		select {
		case err := <-done:
			t.Fatalf("the re-run exited before reaching the tail: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
		if pos, _ := bc.persisted(t, dsn, streamID); strings.Contains(pos, `"last_applied_backup_id":"`+tail+`"`) {
			cancel()
			<-done
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the re-run never reached the chain's tail")
		}
	}
}

// runBrokerCrashSuite is the matrix for one engine.
func runBrokerCrashSuite(t *testing.T, e *sevEnv) {
	bc := newBrokerCrashChain(t, e)
	want := map[string]string{}
	for _, tbl := range brokerCrashTables {
		want[tbl] = bc.tableState(t, e.src, tbl)
	}
	n := len(bc.events)
	var keylessCommitted atomic.Int64
	// The cells are independent (a database and a stream id each, the kill
	// armed on their own broker), so they run in parallel; the group returns
	// once every cell has, before the anti-vacuity floor reads the count.
	t.Run("cells", func(t *testing.T) {
		for _, mode := range []struct {
			name string
			conc int
		}{{"serial", 1}, {"lanes", 0}} {
			for j := 2; j <= n-4; j++ {
				// Only kills that land after a statement: the event before j is a row change.
				if _, isRow := rowChangeTable(bc.events[j-1]); !isRow {
					continue
				}
				t.Run(fmt.Sprintf("%s/kill_after_event_%d", mode.name, j-1), func(t *testing.T) {
					t.Parallel()
					bc.crashCell(t, mode.name, mode.conc, j, want, &keylessCommitted)
				})
			}
		}
	})
	// Anti-vacuity (§9 P6): kills landed after keyless rows had committed.
	if keylessCommitted.Load() == 0 {
		t.Fatal("no kill landed with a keyless row committed: the keyless half of the suite graded nothing")
	}
}

// crashCell is one kill point: kill before chunk j, grade the persisted state,
// re-run to the tail, compare every table with the source.
func (bc *brokerCrashChain) crashCell(t *testing.T, modeName string, conc, j int, want map[string]string, keylessCommitted *atomic.Int64) {
	dsn := bc.target(t, fmt.Sprintf("bk_%s_%d", modeName, j))
	stream := "bk-" + modeName + fmt.Sprint(j)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	err := bc.killBefore(bc.broker(dsn, stream, conc, bc.fullID), j).Run(ctx)
	cancel()
	if !errors.Is(err, errBrokerCrashKill) {
		t.Fatalf("the broker did not die at the kill before chunk %d: %v", j, err)
	}

	// At the kill.
	pos, marks := bc.persisted(t, dsn, stream)
	tok, derr := decodeBrokerPosition(ir.Position{Token: pos})
	if derr != nil || tok.LastAppliedBackupID != bc.fullID {
		t.Fatalf("at the kill the position is %q (%v); want the full %s as last applied", pos, derr, bc.fullID)
	}
	wantThrough := lastBoundaryBelow(bc.events, j)
	got := int64(-1)
	if tok.InProgress != nil {
		got = tok.InProgress.Through
	}
	if (conc == 1 && got != wantThrough) || got > wantThrough {
		t.Errorf("at the kill the frontier is %d; want %d (the last source-transaction boundary below the kill)", got, wantThrough)
	}
	if got >= 0 {
		if _, isCommit := bc.events[got].(ir.TxCommit); !isCommit && txOfOrdinal(bc.events, int(got)) != "" {
			t.Errorf("the persisted frontier names event %d, inside a transaction", got)
		}
	}
	inFlight := txOfOrdinal(bc.events, j-1)
	for _, m := range marks {
		if m != inFlight {
			t.Errorf("at the kill an apply mark names %s; marks may name only the in-flight transaction %q", m, inFlight)
		}
	}
	if kl := sevQuery(t, bc.e.driver, dsn, "SELECT COUNT(*) FROM kl"); kl != "0" && kl != "1" {
		keylessCommitted.Add(1)
	}

	// The re-run.
	bc.runToTail(t, dsn, stream, conc)
	for _, tbl := range brokerCrashTables {
		if got := bc.tableState(t, dsn, tbl); got != want[tbl] {
			t.Errorf("after the re-run %s DIVERGES from the source: target {%s}, source {%s}", tbl, got, want[tbl])
		}
	}
}

func TestBroker_CrashMidIncremental_Postgres(t *testing.T) {
	src, dst, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)
	runBrokerCrashSuite(t, &sevEnv{t: t, engine: "postgres", driver: "pgx", src: src, dst: dst})
}

func TestBroker_CrashMidIncremental_MySQLGTID(t *testing.T) {
	src, dst, cleanup := startMySQLGTID(t)
	t.Cleanup(cleanup)
	runBrokerCrashSuite(t, &sevEnv{t: t, engine: "mysql", driver: "mysql", gtid: true, src: src, dst: dst})
}

func TestBroker_CrashMidIncremental_MySQLFilePos(t *testing.T) {
	src, dst, cleanup := startMySQLBinlog(t)
	t.Cleanup(cleanup)
	runBrokerCrashSuite(t, &sevEnv{t: t, engine: "mysql", driver: "mysql", src: src, dst: dst})
}
