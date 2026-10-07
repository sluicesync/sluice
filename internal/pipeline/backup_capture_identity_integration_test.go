//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// capturedRow is one row change as the identity binding compares it.
type capturedRow struct {
	kind, table, key string
	id               ir.ApplyID
}

func (r capturedRow) String() string {
	return fmt.Sprintf("%s %s[%s] %s#%d", r.kind, r.table, r.key, r.id.TxID, r.id.Seq)
}

// captureRowOf reduces a row change to what the binding compares: its kind,
// its bare table, its key column `id`, and its identity. A non-row change
// answers ok=false.
func captureRowOf(c ir.Change) (capturedRow, bool) {
	var (
		kind, table string
		row         ir.Row
	)
	switch v := c.(type) {
	case ir.Insert:
		kind, table, row = "I", v.Table, v.Row
	case ir.Update:
		kind, table, row = "U", v.Table, v.After
	case ir.Delete:
		kind, table, row = "D", v.Table, v.Before
	default:
		return capturedRow{}, false
	}
	if i := strings.LastIndex(table, "."); i >= 0 {
		table = table[i+1:]
	}
	return capturedRow{kind: kind, table: table, key: fmt.Sprint(row["id"]), id: ir.ApplyIDOf(c)}, true
}

// p2Tables are the tables the binding writes: two, so the per-TABLE ordinal
// (ADR-0190 §1) is exercised — a capture that counted per transaction instead
// would number the second table's rows from where the first stopped.
var p2Tables = []string{"p2a", "p2b"}

// p2Traffic is three source transactions over both tables: inserts, an
// update, a key change and a delete, interleaved across the tables so the
// two per-table counters advance in step with each other.
func p2Traffic(driver string) []string {
	begin := "BEGIN;"
	if driver == "mysql" {
		begin = "START TRANSACTION;"
	}
	return []string{
		begin + ` INSERT INTO p2a VALUES (1,'a'); INSERT INTO p2b VALUES (1,'x'); INSERT INTO p2a VALUES (2,'b'); INSERT INTO p2b VALUES (2,'y'); COMMIT;`,
		begin + ` UPDATE p2a SET v = 'a2' WHERE id = 1; UPDATE p2b SET id = 3 WHERE id = 1; DELETE FROM p2a WHERE id = 2; INSERT INTO p2a VALUES (4,'d'); COMMIT;`,
		begin + ` DELETE FROM p2b WHERE id = 2; INSERT INTO p2b VALUES (5,'z'); UPDATE p2a SET v = 'a3' WHERE id = 1; COMMIT;`,
	}
}

// TestBackupCapture_RecordsTheReaderIdentity is ADR-0191 §9 P2, the binding of
// §3.1's one NEW premise: the identity the backup capture records in a change
// chunk is the identity the same reader stamps when it re-delivers that change
// from the same boundary. The overlap and crash arguments both rest on it, and
// nothing bound the two before: the capture path opens the reader through
// openCDCReaderWithSlot and the re-delivery through whatever resumes the
// stream, and a capture that renumbered, filtered or rewrote a change before
// writing it would break every mark built on the recorded identity.
//
// Each engine cell: a full anchored before the traffic, a `backup stream`
// capturing three transactions over two tables, then the engine's own CDC
// reader opened from the incremental's StartPosition — the same boundary a
// resumed stream re-delivers from — reading the same transactions back. The
// independent expected value is that second, fresh delivery from the source,
// compared change by change, identity by identity, with the decoded chunks.
// Anti-vacuity: every captured row change carries a non-zero identity, the
// manifest is flagged ApplyIdentity, and the two tables' ordinals both reach
// above 1 (or, on a trigger source, every change carries its own).
//
// Reach, stated: Postgres, MySQL GTID and file/pos through `backup stream`;
// postgres-trigger and sqlite-trigger through `backup incremental`. MariaDB
// and VStream are NOT bound here (each needs its own server image); their
// readers' stability across re-delivery is ADR-0190's own pins
// (TestCDCReader_ApplyIdentity_MariaDB_StableAcrossRedelivery,
// TestVStream_ApplyIdentity_StableAcrossMidStreamResume), and the capture
// path they share with these engines is one function, processChange →
// WriteChange, with no per-engine branch.
func TestBackupCapture_RecordsTheReaderIdentity(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		src, dst, cleanup := startPostgresLogical(t)
		t.Cleanup(cleanup)
		runCaptureIdentityBinding(t, &sevEnv{t: t, engine: "postgres", driver: "pgx", src: src, dst: dst})
	})
	t.Run("mysql_gtid", func(t *testing.T) {
		src, dst, cleanup := startMySQLGTID(t)
		t.Cleanup(cleanup)
		runCaptureIdentityBinding(t, &sevEnv{t: t, engine: "mysql", driver: "mysql", gtid: true, src: src, dst: dst})
	})
	t.Run("mysql_filepos", func(t *testing.T) {
		src, dst, cleanup := startMySQLBinlog(t)
		t.Cleanup(cleanup)
		runCaptureIdentityBinding(t, &sevEnv{t: t, engine: "mysql", driver: "mysql", src: src, dst: dst})
	})
	// The trigger sources go through the OTHER capture lane, one-shot
	// `backup incremental` (IncrementalBackup.captureWindow), so both writers
	// of change chunks are bound.
	t.Run("postgres_trigger", func(t *testing.T) {
		src, _, cleanup := startPostgres(t)
		t.Cleanup(cleanup)
		applyDDL(t, src, `CREATE TABLE p2a (id INT PRIMARY KEY, v TEXT); CREATE TABLE p2b (id INT PRIMARY KEY, v TEXT);`)
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		if _, err := pgtrigger.Setup(ctx, src, pgtrigger.SetupOptions{Tables: p2Tables, Schema: "public"}); err != nil {
			t.Fatalf("pgtrigger.Setup: %v", err)
		}
		eng, _ := engines.Get(pgtrigger.EngineName)
		runTriggerCaptureIdentityBinding(ctx, t, eng, src, pgtrigger.EngineName, pgtrigger.AppliedLastID, func(stmts []string) {
			for _, s := range stmts {
				applyDDL(t, src, s)
			}
		})
	})
	t.Run("sqlite_trigger", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "p2.db")
		for _, s := range []string{
			`PRAGMA journal_mode=WAL`,
			`CREATE TABLE p2a (id INTEGER PRIMARY KEY, v TEXT)`,
			`CREATE TABLE p2b (id INTEGER PRIMARY KEY, v TEXT)`,
		} {
			sqliteExec(t, path, s)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		if _, err := sqlitetrigger.Setup(ctx, path, sqlitetrigger.SetupOptions{Tables: p2Tables}); err != nil {
			t.Fatalf("sqlitetrigger.Setup: %v", err)
		}
		eng, _ := engines.Get(sqlitetrigger.EngineName)
		runTriggerCaptureIdentityBinding(ctx, t, eng, path, sqlitetrigger.EngineName, sqlitetrigger.AppliedLastID, func(stmts []string) {
			for _, s := range stmts {
				// Each statement its own transaction (the change log names
				// every change as one anyway).
				for _, one := range strings.Split(strings.TrimSuffix(strings.TrimSpace(s), ";"), ";") {
					if one = strings.TrimSpace(one); one != "" && one != "BEGIN" && one != "COMMIT" {
						sqliteExec(t, path, one)
					}
				}
			}
		})
	})
}

// runTriggerCaptureIdentityBinding is the trigger-source cell: a full that
// records the change log's anchor, the traffic, one `backup incremental`
// closing on its row count, then the engine's reader re-delivering from the
// incremental's StartPosition.
func runTriggerCaptureIdentityBinding(ctx context.Context, t *testing.T, eng ir.Engine, dsn, engineName string, decode func(string) (int64, error), exec func([]string)) {
	t.Helper()
	store := runTriggerChainFull(ctx, t, eng, dsn, engineName, decode)
	exec(p2Traffic("pgx"))
	const rowChanges = 11 // p2Traffic's three transactions
	if err := (&IncrementalBackup{
		Source: eng, SourceDSN: dsn, Store: store, Window: 45 * time.Second,
		MaxChanges: rowChanges, ChunkChanges: 4, SluiceVersion: "test",
	}).Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	captured, first := decodeCapturedRows(t, store)
	requireCapturedIdentities(t, captured, true)
	if len(captured) != rowChanges {
		t.Fatalf("the incremental captured %d row changes; the traffic made %d", len(captured), rowChanges)
	}
	reader, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("open the re-delivery reader: %v", err)
	}
	assertRedeliveryMatchesCapture(t, engineName, captured, reader, first.StartPosition)
}

func runCaptureIdentityBinding(t *testing.T, e *sevEnv) {
	t.Helper()
	ctx := context.Background()
	if e.driver == "pgx" {
		e.exec(`CREATE TABLE p2a (id INT PRIMARY KEY, v TEXT); CREATE TABLE p2b (id INT PRIMARY KEY, v TEXT);
			CREATE TABLE p2s (id INT PRIMARY KEY); CREATE PUBLICATION sluice_pub FOR ALL TABLES;`)
	} else {
		e.exec(`CREATE TABLE p2a (id INT NOT NULL PRIMARY KEY, v VARCHAR(16)) ENGINE=InnoDB;
			CREATE TABLE p2b (id INT NOT NULL PRIMARY KEY, v VARCHAR(16)) ENGINE=InnoDB;
			CREATE TABLE p2s (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB;`)
	}
	// Postgres: a second slot created BEFORE the chain's own (so its
	// confirmed_flush sits at or behind the full's anchor), so the re-delivery
	// can read from the anchor after the capture's slot has acknowledged past
	// it.
	redeliverySlot := "sluice_p2_redelivery"
	if e.driver == "pgx" {
		if _, err := createPGLogicalSlotReturningLSN(t, e.src, redeliverySlot); err != nil {
			t.Fatalf("create the re-delivery slot: %v", err)
		}
		t.Cleanup(func() { dropPGLogicalSlot(t, e.src, redeliverySlot) })
	}
	c := e.newChain(nil, false)

	// Capture: one stream, the traffic, a sentinel that closes the window.
	stream := c.newStream(30 * time.Minute)
	sentinelSeen := false
	stream.onWindowChange = func(ch ir.Change, inTx bool, _ *captureOutcome) {
		if ins, ok := ch.(ir.Insert); ok && sevTableIs(ins.Table, "p2s") {
			sentinelSeen = true
		}
		if sentinelSeen && !inTx {
			sentinelSeen = false
			notifyStreamStop(c.store)
		}
	}
	done := make(chan error, 1)
	go func() { done <- stream.Run(ctx) }()
	for _, tx := range p2Traffic(e.driver) {
		e.exec(tx)
	}
	e.exec(`INSERT INTO p2s VALUES (1)`)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stream.Run: %v", err)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("the capture never reached the sentinel")
	}

	captured, first := decodeCapturedRows(t, c.store)
	requireCapturedIdentities(t, captured, false)

	// Re-delivery: the engine's own reader, from the boundary the capture
	// started at, reading the same transactions back from the source.
	eng, _ := engines.Get(e.engine)
	from := first.StartPosition
	var reader ir.CDCReader
	var err error
	if e.driver == "pgx" {
		from = withPGSlot(t, from, redeliverySlot)
		reader, err = eng.(ir.CDCReaderWithSlotOpener).OpenCDCReaderWithSlot(ctx, e.src, redeliverySlot)
	} else {
		reader, err = eng.OpenCDCReader(ctx, e.src)
	}
	if err != nil {
		t.Fatalf("open the re-delivery reader: %v", err)
	}
	assertRedeliveryMatchesCapture(t, e.engine, captured, reader, from)
}

// assertRedeliveryMatchesCapture reads len(captured) row changes from reader,
// started at from, and requires each to be the captured change with the same
// identity.
func assertRedeliveryMatchesCapture(t *testing.T, engine string, captured []capturedRow, reader ir.CDCReader, from ir.Position) {
	t.Helper()
	if cl, ok := reader.(io.Closer); ok {
		defer func() { _ = cl.Close() }()
	}
	rctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ch, err := reader.StreamChanges(rctx, from)
	if err != nil {
		t.Fatalf("StreamChanges from %+v: %v", from, err)
	}
	var redelivered []capturedRow
	for len(redelivered) < len(captured) {
		select {
		case c, ok := <-ch:
			if !ok {
				t.Fatalf("the re-delivery stream closed after %d of %d row changes", len(redelivered), len(captured))
			}
			if r, ok := captureRowOf(c); ok && r.table != "p2s" {
				redelivered = append(redelivered, r)
			}
		case <-rctx.Done():
			t.Fatalf("the re-delivery produced %d of %d row changes before the deadline", len(redelivered), len(captured))
		}
	}
	for i := range captured {
		if captured[i] != redelivered[i] {
			t.Errorf("change %d: the chunk recorded %s, the reader re-delivered %s", i, captured[i], redelivered[i])
		}
	}
	t.Logf("%s: %d row changes, chunk identities equal the re-delivery's (last %s)", engine, len(captured), captured[len(captured)-1])
}

// requireCapturedIdentities is the anti-vacuity half: every captured row
// change carries an identity, and — on a source that frames transactions —
// both tables' ordinals pass 1. A trigger source names every change as its
// own transaction (Seq 1, ADR-0190 phase 5), so there every change must carry
// a DISTINCT transaction id instead.
func requireCapturedIdentities(t *testing.T, captured []capturedRow, perChangeTransactions bool) {
	t.Helper()
	if len(captured) == 0 {
		t.Fatal("the capture recorded no row change: the cell grades nothing")
	}
	maxSeq := map[string]uint64{}
	txs := map[string]bool{}
	for _, r := range captured {
		if r.id.IsZero() {
			t.Fatalf("the capture recorded %s with no identity, from a reader that stamps one", r)
		}
		maxSeq[r.table] = max(maxSeq[r.table], r.id.Seq)
		txs[r.id.TxID] = true
	}
	if perChangeTransactions {
		if len(txs) != len(captured) {
			t.Fatalf("%d row changes carry only %d transaction ids; a trigger source names each change its own", len(captured), len(txs))
		}
		return
	}
	for _, tbl := range p2Tables {
		if maxSeq[tbl] < 2 {
			t.Fatalf("table %s never reached ordinal 2 (%v): the per-table ordinal was not exercised", tbl, maxSeq)
		}
	}
}

// decodeCapturedRows decodes every incremental of the chain in chain order and
// returns its row changes on the binding's tables, plus the first incremental
// that carried any.
func decodeCapturedRows(t *testing.T, store irbackup.Store) ([]capturedRow, *irbackup.Manifest) {
	t.Helper()
	ctx := context.Background()
	chain, err := (&SyncFromBackup{Store: store, ChainURL: "x", StreamID: "x"}).brokerChain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var (
		out   []capturedRow
		first *irbackup.Manifest
	)
	for _, link := range chain {
		if lineage.CanonicalKind(link.Manifest.Kind) != irbackup.BackupKindIncremental {
			continue
		}
		before := len(out)
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
				if r, ok := captureRowOf(ch); ok && r.table != "p2s" {
					out = append(out, r)
				}
			}
			if err := cr.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if len(out) > before {
			if !link.Manifest.ApplyIdentity {
				t.Errorf("incremental %s carries row changes but is not flagged ApplyIdentity, from a reader that declares ir.ApplyIdentityProvider",
					lineage.ManifestBackupID(link.Manifest))
			}
			if first == nil {
				first = link.Manifest
			}
		}
	}
	return out, first
}

// withPGSlot rewrites a Postgres position token's slot.
func withPGSlot(t *testing.T, p ir.Position, slot string) ir.Position {
	t.Helper()
	var tok map[string]any
	if err := json.Unmarshal([]byte(p.Token), &tok); err != nil {
		t.Fatalf("decode position %q: %v", p.Token, err)
	}
	tok["slot"] = slot
	b, _ := json.Marshal(tok)
	return ir.Position{Engine: p.Engine, Token: string(b)}
}
