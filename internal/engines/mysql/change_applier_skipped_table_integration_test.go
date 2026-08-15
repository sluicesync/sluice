//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the audit-C-11 unknown-target-table skip
// semantics on a REAL MySQL target — the half of the fix that CHANGES
// behaviour: pre-C-11 this applier HALTED loudly on a table the target
// lacks (colTypesFor's "has no columns" error failed the stream),
// which inverted the blast radius (a halted stream lags every table
// and can outlive binlog retention; the skipped table's rows are still
// on the source and recoverable with add-table). A running apply
// stream that receives changes for a genuinely-absent table now
//
//   - keeps applying other tables' changes (never halts),
//   - WARNs once per table (not once per event),
//   - counts EVERY skipped event durably in sluice_cdc_skipped_tables
//     with the first/last source position tokens stored VERBATIM
//     (hostile-ish table names + tokens round-trip byte-exact), and
//   - does not perturb the CDCPOS-2 deferred-position contract.
//
// Paths covered here: the serial per-change Apply loop and ApplyBatch's
// ADR-0139/0140 coalescing path (whose per-kind dispatchers funnel a
// missing table through applySerial → dispatch; the ADR-0104 concurrent
// lanes drive the same coalescing dispatcher per lane).

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

const skippedWarnMarker = "target lacks this table"

func captureSkipWarns(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// Hostile-ish fixtures: a backtick + non-ASCII table name and position
// tokens carrying quotes and backslashes — the ledger stores both
// verbatim (byte-exact round-trip; utf8mb4_bin keying).
const (
	skipTableName    = "Sk`ip précis"
	skipTokFirst     = `{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5","note":"O'Reilly \\ first"}`
	skipTokMid       = `{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-6"}`
	skipTokLast      = `{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-7","note":"last \"quoted\""}`
	skipTokCommitTok = `{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-8"}`
)

func listSkips(t *testing.T, ctx context.Context, applier ir.ChangeApplier) []ir.SkippedTableRecord {
	t.Helper()
	lister, ok := applier.(ir.SkippedTableLister)
	if !ok {
		t.Fatalf("applier does not implement ir.SkippedTableLister")
	}
	records, err := lister.ListSkippedTables(ctx)
	if err != nil {
		t.Fatalf("ListSkippedTables: %v", err)
	}
	return records
}

func assertSkipRecord(t *testing.T, records []ir.SkippedTableRecord, wantTable string, wantCount int64, wantFirst, wantLast string) {
	t.Helper()
	if len(records) != 1 {
		t.Fatalf("skip records = %+v; want exactly 1", records)
	}
	rec := records[0]
	if rec.StreamID != testStreamID {
		t.Errorf("StreamID = %q; want %q", rec.StreamID, testStreamID)
	}
	if rec.Table != wantTable {
		t.Errorf("Table = %q; want %q (byte-exact round-trip)", rec.Table, wantTable)
	}
	if rec.SkipCount != wantCount {
		t.Errorf("SkipCount = %d; want %d", rec.SkipCount, wantCount)
	}
	if rec.FirstPosition != wantFirst {
		t.Errorf("FirstPosition = %q; want %q (verbatim token)", rec.FirstPosition, wantFirst)
	}
	if rec.LastPosition != wantLast {
		t.Errorf("LastPosition = %q; want %q (verbatim token)", rec.LastPosition, wantLast)
	}
	if rec.FirstSkippedAt.IsZero() || rec.LastSkippedAt.IsZero() {
		t.Errorf("skip timestamps not populated: %+v", rec)
	}
}

func countKnownRows(t *testing.T, ctx context.Context, dsn string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM known").Scan(&n); err != nil {
		t.Fatalf("count known: %v", err)
	}
	return n
}

// TestChangeApplier_SkippedTable_MariaDBValuesSpelling closes the C-11
// value-fidelity residual (audit backlog 2026-08-12, item 1): the skip
// ledger's upsert is flavor-dispatched — MySQL 8 takes the row-alias
// spelling, MariaDB takes `VALUES(col)` — and every prior real-server
// pin ran on MySQL 8, so the MariaDB arm's SQL had never executed
// against the server that requires it (a failure would be a loud SQL
// error on the first skip increment, mid-incident). This drives the
// FULL applier on a real MariaDB — flavor selection included, per the
// Bug 180 pin-through-the-layer lesson — through a first sighting
// (INSERT arm) and a second skip (the ON DUPLICATE KEY UPDATE arm,
// which is where the VALUES() spelling actually lives). One LTS image
// suffices: the spelling is a grammar fact, not a per-line rendering
// (the per-line matrices cover rendering-shape drift).
func TestChangeApplier_SkippedTable_MariaDBValuesSpelling(t *testing.T) {
	dsn := newMariaDB(t, mariadb114Image, "skip_mdb")

	applyMySQLApplier(t, dsn, `
		CREATE TABLE known (
			id   BIGINT PRIMARY KEY,
			body TEXT   NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	_ = captureSkipWarns(t)

	eng := Engine{Flavor: FlavorMariaDB}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	pos := func(tok string) ir.Position { return ir.Position{Engine: "mysql", Token: tok} }
	events := []ir.Change{
		ir.TxBegin{Position: pos(`{"gtid":"begin"}`)},
		ir.Insert{Schema: "skip_mdb", Table: "known", Row: ir.Row{"id": int64(1), "body": "kept"}, Position: pos(`{"gtid":"k1"}`)},
		ir.Insert{Schema: "skip_mdb", Table: skipTableName, Row: ir.Row{"id": int64(1)}, Position: pos(skipTokFirst)},
		ir.Delete{Schema: "skip_mdb", Table: skipTableName, Before: ir.Row{"id": int64(1)}, Position: pos(skipTokLast)},
		ir.TxCommit{Position: pos(skipTokCommitTok)},
	}
	pumpChanges(t, ctx, applier, events)

	// The known table's row landed (the data-path upsert spelling ran
	// too) and the ledger carries insert-then-increment with verbatim
	// tokens — the VALUES() arm executed against the server that
	// requires it.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM known").Scan(&n); err != nil {
		t.Fatalf("count known: %v", err)
	}
	if n != 1 {
		t.Fatalf("known rows = %d; want 1", n)
	}
	assertSkipRecord(t, listSkips(t, ctx, applier), "skip_mdb."+skipTableName, 2, skipTokFirst, skipTokLast)
}

// TestChangeApplier_SkippedTable_PositionTokenBeyond64KB closes the
// C-11 value-fidelity residual item 2: first_position/last_position are
// LONGTEXT by the item-65a precedent (a VGTID set can exceed TEXT's
// 64 KB), but no pin had ever pushed a token past that boundary — a
// silent truncation (or a strict-mode refusal) at 64 KB would have
// looked exactly like coverage. A VGTID-shaped ~96 KB token rides one
// skipped event through the full applier and must round-trip
// byte-exact through the ledger.
func TestChangeApplier_SkippedTable_PositionTokenBeyond64KB(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	_ = captureSkipWarns(t)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// A VGTID-shaped token: many shard entries, ~96 KB total — well past
	// TEXT's 64 KB cap, well under LONGTEXT's.
	var b strings.Builder
	b.WriteString(`{"shard_gtids":[`)
	for i := 0; b.Len() < 96*1024; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"keyspace":"commerce","shard":"%04d-%04d","gtid":"MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-%d"}`, i, i+1, 1000000+i)
	}
	b.WriteString(`]}`)
	giant := b.String()

	pos := func(tok string) ir.Position { return ir.Position{Engine: "mysql", Token: tok} }
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.TxBegin{Position: pos(`{"gtid":"begin"}`)},
		ir.Insert{Schema: "target_db", Table: skipTableName, Row: ir.Row{"id": int64(1)}, Position: pos(giant)},
		ir.TxCommit{Position: pos(skipTokCommitTok)},
	})

	assertSkipRecord(t, listSkips(t, ctx, applier), "target_db."+skipTableName, 1, giant, giant)
}

// TestChangeApplier_SkippedTable_PerChangeApply drives the serial
// per-change Apply loop — the path that halted pre-C-11.
func TestChangeApplier_SkippedTable_PerChangeApply(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE known (
			id   BIGINT PRIMARY KEY,
			body TEXT   NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	warns := captureSkipWarns(t)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	pos := func(tok string) ir.Position { return ir.Position{Engine: "mysql", Token: tok} }

	// One source transaction interleaving a known table with a table the
	// target lacks: insert, update, delete, and truncate arms all skip.
	events := []ir.Change{
		ir.TxBegin{Position: pos(`{"gtid":"begin"}`)},
		ir.Insert{Schema: "target_db", Table: "known", Row: ir.Row{"id": int64(1), "body": "kept"}, Position: pos(`{"gtid":"k1"}`)},
		ir.Insert{Schema: "target_db", Table: skipTableName, Row: ir.Row{"id": int64(1)}, Position: pos(skipTokFirst)},
		ir.Update{
			Schema: "target_db", Table: skipTableName,
			Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2)},
			Position: pos(skipTokMid),
		},
		ir.Truncate{Schema: "target_db", Table: skipTableName, Position: pos(skipTokMid)},
		ir.Delete{Schema: "target_db", Table: skipTableName, Before: ir.Row{"id": int64(2)}, Position: pos(skipTokLast)},
		ir.Insert{Schema: "target_db", Table: "known", Row: ir.Row{"id": int64(2), "body": "also kept"}, Position: pos(`{"gtid":"k2"}`)},
		ir.TxCommit{Position: pos(skipTokCommitTok)},
	}
	pumpChanges(t, ctx, applier, events)

	// The stream kept applying the known table's changes — the halt is
	// gone.
	if n := countKnownRows(t, ctx, dsn); n != 2 {
		t.Fatalf("known rows = %d; want 2 (the stream must keep applying other tables)", n)
	}

	assertSkipRecord(t, listSkips(t, ctx, applier), "target_db."+skipTableName, 4, skipTokFirst, skipTokLast)

	// CDCPOS-2: the persisted position is the TxCommit token.
	gotPos, ok, err := applier.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("ReadPosition: ok=%v err=%v", ok, err)
	}
	if gotPos.Token != skipTokCommitTok {
		t.Fatalf("persisted position = %q; want the TxCommit token %q (CDCPOS-2)", gotPos.Token, skipTokCommitTok)
	}

	if got := strings.Count(warns.String(), skippedWarnMarker); got != 1 {
		t.Fatalf("skip WARN fired %d times; want exactly 1:\n%s", got, warns.String())
	}

	// A later skipped event OUTSIDE a source tx accumulates onto the
	// same ledger row and advances the stream position past itself.
	later := `{"gtid":"outside-tx"}`
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Schema: "target_db", Table: skipTableName, Row: ir.Row{"id": int64(9)}, Position: pos(later)},
	})
	assertSkipRecord(t, listSkips(t, ctx, applier), "target_db."+skipTableName, 5, skipTokFirst, later)
	gotPos, ok, err = applier.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("ReadPosition after outside-tx skip: ok=%v err=%v", ok, err)
	}
	if gotPos.Token != later {
		t.Fatalf("persisted position = %q; want %q (the stream advances past a skipped event)", gotPos.Token, later)
	}
	if got := strings.Count(warns.String(), skippedWarnMarker); got != 1 {
		t.Fatalf("skip WARN re-fired on later events (%d total); want once per applier lifetime", got)
	}
}

// TestChangeApplier_SkippedTable_BatchApply drives ApplyBatch — the
// ADR-0139/0140 coalescing path the ADR-0104 concurrent lanes share.
func TestChangeApplier_SkippedTable_BatchApply(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE known (
			id   BIGINT PRIMARY KEY,
			body TEXT   NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	warns := captureSkipWarns(t)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	batched, ok := applier.(ir.BatchedChangeApplier)
	if !ok {
		t.Fatalf("applier does not implement BatchedChangeApplier")
	}
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	pos := func(tok string) ir.Position { return ir.Position{Engine: "mysql", Token: tok} }
	events := []ir.Change{
		ir.Insert{Schema: "target_db", Table: "known", Row: ir.Row{"id": int64(1), "body": "kept"}, Position: pos(`{"gtid":"k1"}`)},
		ir.Insert{Schema: "target_db", Table: skipTableName, Row: ir.Row{"id": int64(1)}, Position: pos(skipTokFirst)},
		ir.Insert{Schema: "target_db", Table: skipTableName, Row: ir.Row{"id": int64(2)}, Position: pos(skipTokMid)},
		ir.Delete{Schema: "target_db", Table: skipTableName, Before: ir.Row{"id": int64(1)}, Position: pos(skipTokLast)},
		ir.Insert{Schema: "target_db", Table: "known", Row: ir.Row{"id": int64(2), "body": "also kept"}, Position: pos(`{"gtid":"k2"}`)},
	}
	ch := make(chan ir.Change, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	if err := batched.ApplyBatch(ctx, testStreamID, ch, 100); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	if n := countKnownRows(t, ctx, dsn); n != 2 {
		t.Fatalf("known rows = %d; want 2 (the batched stream must keep applying other tables)", n)
	}

	assertSkipRecord(t, listSkips(t, ctx, applier), "target_db."+skipTableName, 3, skipTokFirst, skipTokLast)

	if got := strings.Count(warns.String(), skippedWarnMarker); got != 1 {
		t.Fatalf("skip WARN fired %d times on the batched path; want exactly 1:\n%s", got, warns.String())
	}
}

// TestChangeApplier_MissingDatabase_HaltsNotSilentlySkips is the M-2
// pin (audit 2026-08-14): an information_schema miss is ambiguous — a
// missing TABLE (the recoverable C-11 skip) vs a missing DATABASE (a
// routing fault). Pre-fix both classified as unknown-table, so a
// misrouted multi-database stream skipped EVERY event and exited 0 with
// positions advanced — the loud halt C-11 replaced, turned silent. The
// fix probes information_schema.SCHEMATA: a present schema keeps the
// skip; a missing one HALTS. This drives a real event routed to a
// non-existent target database and asserts the apply refuses loudly and
// writes NO skip-ledger row for it.
func TestChangeApplier_MissingDatabase_HaltsNotSilentlySkips(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	applyMySQLApplier(t, dsn, `CREATE TABLE known (id BIGINT PRIMARY KEY) ENGINE=InnoDB`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	// Enable ADR-0074 multi-database routing with a namespace rename that
	// maps the source database to a target database that does NOT exist —
	// the exact misrouted-fan-out shape M-2 is about.
	router, ok := applier.(ir.MultiDatabaseRouter)
	if !ok {
		t.Fatal("applier does not implement ir.MultiDatabaseRouter")
	}
	router.SetMultiDatabaseRouting(true, func(string) string { return "ghost_db_does_not_exist" })

	// An event whose source database routes to the non-existent target.
	ch := make(chan ir.Change, 1)
	ch <- ir.Insert{
		Schema: "app_eu", Table: "orders",
		Row:      ir.Row{"id": int64(1)},
		Position: ir.Position{Engine: "mysql", Token: `{"gtid":"g1"}`},
	}
	close(ch)
	applyErr := applier.Apply(ctx, testStreamID, ch)
	if applyErr == nil {
		t.Fatal("apply of an event routed to a NON-EXISTENT database returned nil — the stream silently skipped it (M-2 silent-loss: a misrouted multi-database sync would drop every table at exit 0)")
	}
	if !strings.Contains(applyErr.Error(), "ghost_db_does_not_exist") ||
		!strings.Contains(applyErr.Error(), "does not exist") {
		t.Fatalf("halt error should name the missing database and say it does not exist; got: %v", applyErr)
	}

	// The routing fault must NOT be recorded as a recoverable skip — it
	// is not a table an operator can add later.
	skips := listSkips(t, ctx, applier)
	for _, r := range skips {
		if strings.Contains(r.Table, "orders") {
			t.Fatalf("the missing-database event was recorded as a recoverable skip (%q) — it is a routing fault, not an add-table-able skip", r.Table)
		}
	}
}
