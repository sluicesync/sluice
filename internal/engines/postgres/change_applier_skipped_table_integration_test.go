//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the audit-C-11 unknown-target-table skip
// semantics on a REAL Postgres target: a running apply stream that
// receives changes for a table the target genuinely lacks
//
//   - keeps applying other tables' changes (never halts),
//   - WARNs once per table (not once per event),
//   - counts EVERY skipped event in the durable
//     sluice_cdc_skipped_tables ledger with the first/last source
//     position tokens stored VERBATIM (the new-surface codec
//     checklist: hostile-ish table names + tokens round-trip
//     byte-exact), and
//   - does not perturb the CDCPOS-2 deferred-position contract (the
//     persisted position after a source tx is the TxCommit token,
//     never a skipped event's own).
//
// Paths covered here: the serial per-change Apply loop (which the
// batched serial fallback and the ADR-0105 lanes' serial fallback
// share via dispatch) and ApplyBatch's ADR-0092 pipelined path (which
// the ADR-0105 concurrent lanes share via dispatchPipelined). The
// Truncate arm's resolution-time skip is exercised in the per-change
// stream.

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// skippedWarnMarker is the load-bearing fragment of the once-per-table
// WARN; the tests count its occurrences in a captured slog buffer.
const skippedWarnMarker = "target lacks this table"

// captureSkipWarns installs a WARN-level JSON slog handler for the
// test's duration (same pattern as the MySQL keyless-warn pins).
func captureSkipWarns(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// Hostile-ish fixtures: a mixed-case, quoted, non-ASCII table name and
// position tokens carrying quotes, backslashes, and non-ASCII — the
// ledger must round-trip both byte-exact (stored verbatim, never
// parsed or folded).
const (
	skipTableName = `Sk"ip précis`
	skipTokFirst  = `{"lsn":"0/16B2C50","note":"O'Reilly \\ first ☃"}`
	skipTokMid    = `{"lsn":"0/16B2D00","note":"mid"}`
	skipTokLast   = `{"lsn":"0/16B2E10","note":"last \"quoted\" ✓"}`
	skipTokCommit = `{"lsn":"0/16B2F00","note":"commit"}`
)

// countKnownRows counts the rows of the `known` table on a fresh
// connection.
func countKnownRows(t *testing.T, ctx context.Context, dsn string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
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

// TestChangeApplier_SkippedTable_PerChangeApply drives the serial
// per-change Apply loop.
func TestChangeApplier_SkippedTable_PerChangeApply(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE known (
			id   BIGINT PRIMARY KEY,
			body TEXT   NOT NULL
		);
	`)

	warns := captureSkipWarns(t)

	eng := Engine{}
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

	pos := func(tok string) ir.Position { return ir.Position{Engine: "postgres", Token: tok} }

	// One source transaction interleaving a known table with a table the
	// target lacks: insert, update, delete, and truncate arms all skip.
	events := []ir.Change{
		ir.TxBegin{Position: pos(`{"lsn":"0/16B2C00"}`)},
		ir.Insert{Schema: "public", Table: "known", Row: ir.Row{"id": int64(1), "body": "kept"}, Position: pos(`{"lsn":"0/16B2C10"}`)},
		ir.Insert{Schema: "public", Table: skipTableName, Row: ir.Row{"id": int64(1)}, Position: pos(skipTokFirst)},
		ir.Update{
			Schema: "public", Table: skipTableName,
			Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2)},
			Position: pos(skipTokMid),
		},
		ir.Truncate{Schema: "public", Table: skipTableName, Position: pos(skipTokMid)},
		ir.Delete{Schema: "public", Table: skipTableName, Before: ir.Row{"id": int64(2)}, Position: pos(skipTokLast)},
		ir.Insert{Schema: "public", Table: "known", Row: ir.Row{"id": int64(2), "body": "also kept"}, Position: pos(`{"lsn":"0/16B2E20"}`)},
		ir.TxCommit{Position: pos(skipTokCommit)},
	}
	pumpChanges(t, ctx, applier, events)

	// The stream kept applying the known table's changes.
	if n := countKnownRows(t, ctx, dsn); n != 2 {
		t.Fatalf("known rows = %d; want 2 (the stream must keep applying other tables)", n)
	}

	// The durable ledger counted all four skipped events with verbatim
	// first/last tokens.
	assertSkipRecord(t, listSkips(t, ctx, applier), "public."+skipTableName, 4, skipTokFirst, skipTokLast)

	// CDCPOS-2: the persisted position is the TxCommit token — deferred
	// through the source transaction, never a skipped event's own.
	gotPos, ok, err := applier.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("ReadPosition: ok=%v err=%v", ok, err)
	}
	if gotPos.Token != skipTokCommit {
		t.Fatalf("persisted position = %q; want the TxCommit token %q (CDCPOS-2)", gotPos.Token, skipTokCommit)
	}

	// WARN fired once per table, not once per event.
	if got := strings.Count(warns.String(), skippedWarnMarker); got != 1 {
		t.Fatalf("skip WARN fired %d times; want exactly 1:\n%s", got, warns.String())
	}

	// A later skipped event OUTSIDE a source tx accumulates onto the
	// same ledger row and advances the stream position past itself.
	later := `{"lsn":"0/16B3000","note":"outside-tx"}`
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Schema: "public", Table: skipTableName, Row: ir.Row{"id": int64(9)}, Position: pos(later)},
	})
	assertSkipRecord(t, listSkips(t, ctx, applier), "public."+skipTableName, 5, skipTokFirst, later)
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
// ADR-0092 pipelined path (dispatchPipelined), which the ADR-0105
// concurrent lanes share.
func TestChangeApplier_SkippedTable_BatchApply(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE known (
			id   BIGINT PRIMARY KEY,
			body TEXT   NOT NULL
		);
	`)

	warns := captureSkipWarns(t)

	eng := Engine{}
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

	pos := func(tok string) ir.Position { return ir.Position{Engine: "postgres", Token: tok} }
	events := []ir.Change{
		ir.Insert{Schema: "public", Table: "known", Row: ir.Row{"id": int64(1), "body": "kept"}, Position: pos(`{"lsn":"0/1"}`)},
		ir.Insert{Schema: "public", Table: skipTableName, Row: ir.Row{"id": int64(1)}, Position: pos(skipTokFirst)},
		ir.Insert{Schema: "public", Table: skipTableName, Row: ir.Row{"id": int64(2)}, Position: pos(skipTokMid)},
		ir.Delete{Schema: "public", Table: skipTableName, Before: ir.Row{"id": int64(1)}, Position: pos(skipTokLast)},
		ir.Insert{Schema: "public", Table: "known", Row: ir.Row{"id": int64(2), "body": "also kept"}, Position: pos(`{"lsn":"0/2"}`)},
	}
	pumpBatchedChanges(t, ctx, applier, events, 100)

	if n := countKnownRows(t, ctx, dsn); n != 2 {
		t.Fatalf("known rows = %d; want 2 (the batched stream must keep applying other tables)", n)
	}

	assertSkipRecord(t, listSkips(t, ctx, applier), "public."+skipTableName, 3, skipTokFirst, skipTokLast)

	if got := strings.Count(warns.String(), skippedWarnMarker); got != 1 {
		t.Fatalf("skip WARN fired %d times on the batched path; want exactly 1:\n%s", got, warns.String())
	}
}

// TestChangeApplier_MissingSchema_HaltsNotSilentlySkips is the M-2 pin
// (audit 2026-08-14), the PostgreSQL sibling of the MySQL missing-
// database halt: an empty information_schema result is ambiguous — a
// missing TABLE (the recoverable C-11 skip) vs a missing SCHEMA (a
// routing fault). Pre-fix both classified as unknown-table, so a
// misrouted namespace mapping skipped EVERY event and exited 0 with
// positions advanced. The fix probes information_schema.schemata: a
// present schema keeps the skip; a missing one HALTS. This routes a real
// event to a non-existent target schema and asserts the apply refuses
// loudly and records NO skip-ledger row for it.
func TestChangeApplier_MissingSchema_HaltsNotSilentlySkips(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	applyPGApplier(t, dsn, `CREATE TABLE known (id BIGINT PRIMARY KEY);`)

	eng := Engine{}
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

	// Enable multi-database routing with a namespace rename mapping the
	// source schema to a target schema that does NOT exist — the misrouted
	// namespace-mapping shape M-2 is about.
	router, ok := applier.(ir.MultiDatabaseRouter)
	if !ok {
		t.Fatal("applier does not implement ir.MultiDatabaseRouter")
	}
	router.SetMultiDatabaseRouting(true, func(string) string { return "ghost_schema_does_not_exist" })

	ch := make(chan ir.Change, 1)
	ch <- ir.Insert{
		Schema: "app_eu", Table: "orders",
		Row:      ir.Row{"id": int64(1)},
		Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/16B2C00"}`},
	}
	close(ch)
	applyErr := applier.Apply(ctx, testStreamID, ch)
	if applyErr == nil {
		t.Fatal("apply of an event routed to a NON-EXISTENT schema returned nil — the stream silently skipped it (M-2 silent-loss: a misrouted namespace mapping would drop every table at exit 0)")
	}
	if !strings.Contains(applyErr.Error(), "ghost_schema_does_not_exist") ||
		!strings.Contains(applyErr.Error(), "does not exist") {
		t.Fatalf("halt error should name the missing schema and say it does not exist; got: %v", applyErr)
	}

	for _, r := range listSkips(t, ctx, applier) {
		if strings.Contains(r.Table, "orders") {
			t.Fatalf("the missing-schema event was recorded as a recoverable skip (%q) — it is a routing fault, not add-table-able", r.Table)
		}
	}
}
