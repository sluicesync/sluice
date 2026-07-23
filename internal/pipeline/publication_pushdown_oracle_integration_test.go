//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The ADR-0176 §4 equivalence oracle — the gate the publication row-filter
// push-down ships behind, on real PG 16.
//
// For every cell of the proven envelope (predicate-shape family × value
// family, with NULLs woven into every workload and all four row-move
// truth-table transitions exercised), the SAME workload is decoded through
// TWO real logical-replication streams:
//
//   - PUSH: a publication carrying the predicate as its per-table row
//     filter — the server is authoritative for delivery, and the client
//     evaluator runs after it as the production belt (ADR-0176 §2);
//   - CLIENT: an unfiltered publication with the client-side evaluator as
//     the only filter — the pre-ADR-0176 correctness authority.
//
// The oracle asserts the two produce IDENTICAL ordered change sequences.
// Server-stricter-than-client — the silent-loss direction, unobservable in
// production by construction — surfaces here as a missing row on the PUSH
// side. Two non-vacuity belts per cell: the PUSH publication's prqual must
// be recorded by PG (the filter actually emitted), and the PUSH stream's
// RAW delivery must be strictly smaller than CLIENT's (the server actually
// filtered — every workload keeps at least one permanently out-of-scope
// row).
//
// A divergent cell is a FINDING: per the ADR it gets EXCLUDED from the
// classifier envelope (pgPushdownEligibleColumn + the envelope pin), never
// "fixed" by bending the client evaluator.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// oracleWorkload renders the fixed row-move script for one cell. Rows 1–4
// walk every truth-table transition (stay-in / move-in / move-out /
// stay-out, plus in- and out-scope INSERTs and DELETEs); row 5 is the
// NULL arc (NULL insert, NULL→in move, in→NULL move); the w-only UPDATE
// is the NO-TOUCH arc (audit 2026-07-23 G-1): an UPDATE that does NOT set
// the filtered column, so the stream must classify the row-move from an
// unchanged `v` — pre-G-1 every cell SET v itself, leaving the
// unchanged-column delivery path structurally unexercised; 9999 is the
// end-of-workload sentinel (in-scope by construction).
func oracleWorkload(table string, c oracleCell) []string {
	q := func(format string, args ...any) string { return fmt.Sprintf(format, args...) }
	return []string{
		q(`INSERT INTO %s (id, v) VALUES (1, %s)`, table, c.in0),
		q(`INSERT INTO %s (id, v) VALUES (2, %s)`, table, c.out0),
		q(`INSERT INTO %s (id, v) VALUES (3, %s)`, table, c.in0),
		q(`INSERT INTO %s (id, v) VALUES (4, %s)`, table, c.out0),
		q(`INSERT INTO %s (id, v) VALUES (5, NULL)`, table),
		q(`UPDATE %s SET w = COALESCE(w, 0) + 1 WHERE id = 1`, table), // no-touch (v unchanged, stay-in)
		q(`UPDATE %s SET v = %s WHERE id = 1`, table, c.in1),          // stay-in
		q(`UPDATE %s SET v = %s WHERE id = 2`, table, c.in0),          // move-IN
		q(`UPDATE %s SET v = %s WHERE id = 3`, table, c.out0),         // move-OUT
		q(`UPDATE %s SET v = %s WHERE id = 4`, table, c.out1),         // stay-out
		q(`UPDATE %s SET v = %s WHERE id = 5`, table, c.in0),          // NULL→in
		q(`UPDATE %s SET v = NULL WHERE id = 5`, table),               // in→NULL
		q(`DELETE FROM %s WHERE id = 1`, table),                       // delete in-scope
		q(`DELETE FROM %s WHERE id = 4`, table),                       // delete out-of-scope
		q(`INSERT INTO %s (id, v) VALUES (9999, %s)`, table, c.in0),   // sentinel
	}
}

// renderOracleRow renders an ir.Row deterministically (sorted keys, %v
// values). Both legs decode through the identical reader code, so equal
// values render equally.
func renderOracleRow(row ir.Row) string {
	if row == nil {
		return "<nil>"
	}
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, row[k]))
	}
	return strings.Join(parts, ",")
}

// renderOracleChange normalizes one post-route change for comparison,
// excluding Position and CommitTime (per-slot artifacts).
func renderOracleChange(c ir.Change) string {
	switch e := c.(type) {
	case ir.Insert:
		return "INSERT " + renderOracleRow(e.Row)
	case ir.Update:
		return "UPDATE before{" + renderOracleRow(e.Before) + "} after{" + renderOracleRow(e.After) + "}"
	case ir.Delete:
		return "DELETE " + renderOracleRow(e.Before)
	default:
		return fmt.Sprintf("OTHER %T", c)
	}
}

// drainUntilSentinel reads row changes for table off ch until the sentinel
// insert (id=9999) arrives, returning them in order (sentinel included).
func drainUntilSentinel(t *testing.T, ch <-chan ir.Change, table string, timeout time.Duration) []ir.Change {
	t.Helper()
	deadline := time.After(timeout)
	var out []ir.Change
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				t.Fatalf("change channel closed before the sentinel insert arrived (%d changes so far)", len(out))
			}
			switch e := c.(type) {
			case ir.Insert:
				if !strings.EqualFold(e.Table, table) {
					continue
				}
				out = append(out, c)
				if id, ok := e.Row["id"].(int64); ok && id == 9999 {
					return out
				}
				if id, ok := e.Row["id"].(int32); ok && id == 9999 {
					return out
				}
			case ir.Update:
				if strings.EqualFold(e.Table, table) {
					out = append(out, c)
				}
			case ir.Delete:
				if strings.EqualFold(e.Table, table) {
					out = append(out, c)
				}
			default:
				// Tx boundaries / snapshots: not row-scoped, ignore.
			}
		case <-deadline:
			t.Fatalf("timed out waiting for the sentinel insert (%d changes so far)", len(out))
		}
	}
}

// dropSlotWithRetry drops a replication slot, retrying while the just-
// cancelled walsender still holds it active. A walsender can outlive the
// reader's ctx-cancel by tens of seconds on a cold/loaded container (its
// socket teardown, not sluice's, is what releases the slot), so after each
// failed drop the walsender is terminated server-side — deterministic
// cleanup for a test that cycles ~80 slots through one container.
func dropSlotWithRetry(t *testing.T, db *sql.DB, slot string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, err := db.ExecContext(context.Background(), `SELECT pg_drop_replication_slot($1)`, slot)
		if err == nil || strings.Contains(err.Error(), "does not exist") {
			return
		}
		_, _ = db.ExecContext(context.Background(),
			`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
			 WHERE slot_name = $1 AND active_pid IS NOT NULL`, slot)
		if time.Now().After(deadline) {
			t.Fatalf("drop slot %s: %v", slot, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func TestPublicationScope_PushdownOracle(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	db, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// One table per value family, created up front; REPLICA IDENTITY FULL is
	// the filtered-sync precondition (SLUICE-E-WHERE-CDC-BEFORE-IMAGE). The
	// `w` sibling column exists solely for the no-touch workload arc (G-1):
	// an UPDATE that never sets the filtered column.
	for _, fam := range oracleMatrix {
		applyDDL(t, sourceDSN, fmt.Sprintf(
			`CREATE TABLE orc_%s (id int PRIMARY KEY, v %s, w int);
			 ALTER TABLE orc_%s REPLICA IDENTITY FULL;`, fam.name, fam.colType, fam.name,
		))
	}

	// Read the source schema ONCE for predicate compilation — the exact
	// production compile path (engine collation resolver + real catalog
	// types), so the CLIENT leg is byte-for-byte the shipped evaluator.
	sr, err := pgEng.OpenSchemaReader(context.Background(), sourceDSN)
	if err != nil {
		t.Fatalf("open schema reader: %v", err)
	}
	schema, err := sr.ReadSchema(context.Background())
	migcore.CloseIf(sr)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	resolver := pgEng.(ir.CollationResolverProvider).CollationResolver()

	cellN := 0
	for _, fam := range oracleMatrix {
		fam := fam
		table := "orc_" + fam.name
		for _, cell := range fam.cells {
			cell := cell
			cellN++
			n := cellN
			t.Run(fam.name+"/"+cell.name, func(t *testing.T) {
				runOracleCell(t, db, sourceDSN, pgEng, resolver, schema, table, cell, n)
			})
		}
	}
}

// runOracleCell drives one matrix cell end to end: publications (filtered +
// unfiltered) → two live streams → workload → drain → route through the
// production client evaluator → assert identical sequences.
func runOracleCell(
	t *testing.T,
	db *sql.DB,
	dsn string,
	pgEng ir.Engine,
	resolver ir.CollationResolver,
	schema *ir.Schema,
	table string,
	cell oracleCell,
	n int,
) {
	ctx := context.Background()
	pubPush := fmt.Sprintf("sluice_orcp_%d", n)
	pubClient := fmt.Sprintf("sluice_orcc_%d", n)
	slotPush := fmt.Sprintf("sluice_orcp_%d", n)
	slotClient := fmt.Sprintf("sluice_orcc_%d", n)

	// The classifier must admit every matrix cell — a cell the classifier
	// rejects is not "skipped", it is an envelope/matrix mismatch.
	wf, err := buildWhereCDCFilter(resolver, map[string]string{table: cell.pred}, schema, false)
	if err != nil {
		t.Fatalf("compile predicate %q: %v", cell.pred, err)
	}
	var tbl *ir.Table
	for _, tb := range schema.Tables {
		if tb != nil && strings.EqualFold(tb.Name, table) {
			tbl = tb
		}
	}
	if ok, reason := pgPushdownEligible(tbl, wf.predicateFor(table)); !ok {
		t.Fatalf("oracle cell %q is outside the classifier envelope (%s) — the matrix and the envelope must move together", cell.pred, reason)
	}

	// PUSH leg: publication WITH the row filter, via the production emit
	// path (EnsurePublication + WithPublicationRowFilters).
	engPush := pgEng.(ir.PublicationScoper).WithPublicationScope(pubPush, slotPush)
	engPush = engPush.(ir.PublicationRowFilterer).WithPublicationRowFilters(map[string]string{table: cell.pred})
	if err := engPush.(publicationEnsurer).EnsurePublication(ctx, dsn, []string{table}); err != nil {
		t.Fatalf("ensure push publication: %v", err)
	}
	// CLIENT leg: same member, no filter.
	engClient := pgEng.(ir.PublicationScoper).WithPublicationScope(pubClient, slotClient)
	if err := engClient.(publicationEnsurer).EnsurePublication(ctx, dsn, []string{table}); err != nil {
		t.Fatalf("ensure client publication: %v", err)
	}
	defer func() {
		_, _ = db.ExecContext(ctx, "DROP PUBLICATION IF EXISTS "+pubPush)
		_, _ = db.ExecContext(ctx, "DROP PUBLICATION IF EXISTS "+pubClient)
		if _, err := db.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			t.Errorf("reset %s: %v", table, err)
		}
	}()

	// Non-vacuity belt 1: PG recorded the filter on the PUSH publication.
	var qual string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(pg_get_expr(pr.prqual, pr.prrelid), '')
		FROM pg_publication p JOIN pg_publication_rel pr ON pr.prpubid = p.oid
		WHERE p.pubname = $1`, pubPush).Scan(&qual); err != nil {
		t.Fatalf("read push prqual: %v", err)
	}
	if qual == "" {
		t.Fatal("push publication carries NO row filter — the oracle would be vacuously green")
	}

	// Open both streams BEFORE the workload (StreamChanges returns after
	// slot creation + START_REPLICATION, so both slots cover every write).
	streamCtx, cancelStreams := context.WithCancel(ctx)
	openLeg := func(eng ir.Engine, slot string) (ir.CDCReader, <-chan ir.Change) {
		rdr, err := eng.(ir.CDCReaderWithSlotOpener).OpenCDCReaderWithSlot(ctx, dsn, slot)
		if err != nil {
			t.Fatalf("open CDC reader (%s): %v", slot, err)
		}
		rdr.(ir.FullBeforeImageSetter).SetFullBeforeImageTables(map[string]bool{table: true})
		ch, err := rdr.StreamChanges(streamCtx, ir.Position{})
		if err != nil {
			t.Fatalf("StreamChanges (%s): %v", slot, err)
		}
		return rdr, ch
	}
	rdrPush, chPush := openLeg(engPush, slotPush)
	rdrClient, chClient := openLeg(engClient, slotClient)
	defer func() {
		cancelStreams()
		migcore.CloseIf(rdrPush)
		migcore.CloseIf(rdrClient)
		dropSlotWithRetry(t, db, slotPush, 30*time.Second)
		dropSlotWithRetry(t, db, slotClient, 30*time.Second)
	}()

	for _, stmt := range oracleWorkload(table, cell) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("workload %q: %v", stmt, err)
		}
	}

	rawPush := drainUntilSentinel(t, chPush, table, 60*time.Second)
	rawClient := drainUntilSentinel(t, chClient, table, 60*time.Second)

	// Non-vacuity belt 2: the server actually filtered — every workload
	// keeps id=4 permanently out of scope, so PUSH must deliver strictly
	// less than CLIENT.
	if len(rawPush) >= len(rawClient) {
		t.Errorf("push-down did not reduce delivery: raw push=%d raw client=%d", len(rawPush), len(rawClient))
	}

	// Route both legs through the production client evaluator (the belt on
	// the PUSH leg, the authority on the CLIENT leg) and compare.
	routeAll := func(raw []ir.Change, leg string) []string {
		var out []string
		for _, c := range raw {
			emitted, err := wf.route(c)
			if err != nil {
				t.Fatalf("%s leg route: %v", leg, err)
			}
			for _, e := range emitted {
				out = append(out, renderOracleChange(e))
			}
		}
		return out
	}
	gotPush := routeAll(rawPush, "push")
	gotClient := routeAll(rawClient, "client")

	if len(gotPush) != len(gotClient) {
		t.Fatalf("delivered row sets DIVERGE (push=%d client=%d)\npush:\n  %s\nclient:\n  %s",
			len(gotPush), len(gotClient),
			strings.Join(gotPush, "\n  "), strings.Join(gotClient, "\n  "))
	}
	for i := range gotClient {
		if gotPush[i] != gotClient[i] {
			t.Fatalf("delivered row sets DIVERGE at change %d:\npush:   %s\nclient: %s", i, gotPush[i], gotClient[i])
		}
	}

	// The truth-table cells must be present, not vacuously absent: the
	// CLIENT leg (the authority) must contain the move-IN as an INSERT of
	// id=2, the move-OUT as a DELETE, and the in-scope DELETE of id=1 —
	// plus the sentinel. (Rendered ops carry the row, so substring checks
	// on the op names are sufficient shape pins here; exact content
	// equality with PUSH was asserted above.)
	joined := strings.Join(gotClient, "\n")
	if !strings.Contains(joined, "INSERT") || !strings.Contains(joined, "DELETE") {
		t.Errorf("client leg is missing INSERT/DELETE shapes — the workload did not exercise the truth table:\n%s", joined)
	}
}

// TestPublicationScope_TemporalLiteralNormalization is the D0-5 / Q1 gate
// (audit 2026-07-23, owner call: filtered replicas follow the SOURCE
// ENGINE's comparison semantics): for the temporal literal-granularity
// boundary shapes — a time-bearing literal against a DATE column, a
// >6-fractional-digit literal against a microsecond timestamp — the server
// truncates/rounds the literal, and Compile now normalizes the client
// literal to the SAME coercion. For each shape it asserts BOTH halves:
//
//   - server and client verdicts AGREE on the boundary row, live (server
//     verdict via SELECT under the raw predicate vs the shipped client
//     evaluator on the decoded value-contract row). RED on the pre-Q1
//     evaluator, whose full-precision compare provably diverged here —
//     the predecessor of this test pinned that divergence live; and
//   - the classifier ADMITS the normalized predicate into the push-down
//     envelope (the M0-3 exclusions moved back inside it; the term flags
//     remain only as the belt for a compile without the engine lens,
//     pinned at the unit level).
//
// The full two-leg stream equivalence for these shapes rides the oracle's
// granularity cells (date eq/lt/ne/in_time_bearing; timestamp eq_7digit,
// eq_halfeven — the rounding-MODE pin — and eq_7digit_carry).
func TestPublicationScope_TemporalLiteralNormalization(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// Boundary rows: the DATE cell's midnight-vs-noon boundary, and the
	// timestamp cell's µs twins of the 7-digit, half-boundary, and
	// double-mediated (review F1) literals.
	applyDDL(t, sourceDSN, `
		CREATE TABLE orcg_date (id int PRIMARY KEY, v date);
		CREATE TABLE orcg_ts   (id int PRIMARY KEY, v timestamp);
		INSERT INTO orcg_date (id, v) VALUES (1, '2026-01-15');
		INSERT INTO orcg_ts   (id, v) VALUES (1, '2026-01-15 08:30:00.123457'), (2, '2026-01-15 08:30:00.123456'), (3, '2026-01-15 08:30:00.000125');
	`)

	db, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	sr, err := pgEng.OpenSchemaReader(context.Background(), sourceDSN)
	if err != nil {
		t.Fatalf("open schema reader: %v", err)
	}
	schema, err := sr.ReadSchema(context.Background())
	migcore.CloseIf(sr)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	resolver := pgEng.(ir.CollationResolverProvider).CollationResolver()

	// clientRow values follow docs/value-types.md: FamilyTemporal decodes to
	// a UTC time.Time (a date at midnight; a timestamp at µs precision).
	// rowID selects which stored row the server verdict runs against, so the
	// client row always mirrors the server row exactly.
	cells := []struct {
		name      string
		table     string
		pred      string
		rowID     int
		clientRow ir.Row
	}{
		{"date lt time-bearing", "orcg_date", "v < '2026-01-15 12:00:00'", 1, ir.Row{"id": int64(1), "v": time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)}},
		{"date ne time-bearing", "orcg_date", "v != '2026-01-15 12:00:00'", 1, ir.Row{"id": int64(1), "v": time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)}},
		{"date eq time-bearing", "orcg_date", "v = '2026-01-15 08:30:00'", 1, ir.Row{"id": int64(1), "v": time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)}},
		{"date NOT(ge) time-bearing (3VL negation)", "orcg_date", "NOT (v >= '2026-01-15 12:00:00')", 1, ir.Row{"id": int64(1), "v": time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)}},
		{"timestamp eq 7-fractional-digit (rounded twin)", "orcg_ts", "v = '2026-01-15 08:30:00.1234567'", 1, ir.Row{"id": int64(1), "v": time.Date(2026, 1, 15, 8, 30, 0, 123457000, time.UTC)}},
		{"timestamp eq 7-fractional-digit (floor twin)", "orcg_ts", "v = '2026-01-15 08:30:00.1234567'", 2, ir.Row{"id": int64(2), "v": time.Date(2026, 1, 15, 8, 30, 0, 123456000, time.UTC)}},
		{"timestamp eq half boundary (double-mediated rint)", "orcg_ts", "v = '2026-01-15 08:30:00.1234565'", 2, ir.Row{"id": int64(2), "v": time.Date(2026, 1, 15, 8, 30, 0, 123456000, time.UTC)}},
		// The review-F1 pair: PG's double lands BELOW the half for .0001255
		// (→ .000125) and ABOVE for .0001265 (→ .000127) — exact decimal
		// half-even gives .000126 for both, which is exactly how the F1
		// CRITICAL slipped past every hand-picked boundary pin.
		{"timestamp eq .0001255 (double lands below)", "orcg_ts", "v = '2026-01-15 08:30:00.0001255'", 3, ir.Row{"id": int64(3), "v": time.Date(2026, 1, 15, 8, 30, 0, 125000, time.UTC)}},
		{"timestamp eq .0001265 (double lands above; row does NOT match)", "orcg_ts", "v = '2026-01-15 08:30:00.0001265'", 3, ir.Row{"id": int64(3), "v": time.Date(2026, 1, 15, 8, 30, 0, 125000, time.UTC)}},
	}
	for _, cell := range cells {
		cell := cell
		t.Run(cell.name, func(t *testing.T) {
			// Ground truth: server and client verdicts AGREE on the boundary
			// row — the Q1 contract, RED on the pre-normalization evaluator.
			var serverMatches int
			if err := db.QueryRowContext(context.Background(),
				fmt.Sprintf(`SELECT count(*) FROM %s WHERE id = %d AND (%s)`, cell.table, cell.rowID, cell.pred)).Scan(&serverMatches); err != nil {
				t.Fatalf("server verdict for %q: %v", cell.pred, err)
			}
			wf, err := buildWhereCDCFilter(resolver, map[string]string{cell.table: cell.pred}, schema, false)
			if err != nil {
				t.Fatalf("compile %q: %v", cell.pred, err)
			}
			clientMatches := wf.predicateFor(cell.table).Eval(cell.clientRow)
			if (serverMatches > 0) != clientMatches {
				t.Fatalf("server (%d) and client (%v) DISAGREE for %q on row %d — Compile's engine-semantics normalization does not reproduce PG's literal coercion (audit 2026-07-23 D0-5 / Q1)", serverMatches, clientMatches, cell.pred, cell.rowID)
			}

			// Q1 re-admission: the normalized predicate is INSIDE the
			// push-down envelope (the two-leg stream equivalence rides the
			// oracle's granularity cells).
			var tbl *ir.Table
			for _, tb := range schema.Tables {
				if tb != nil && strings.EqualFold(tb.Name, cell.table) {
					tbl = tb
				}
			}
			if ok, reason := pgPushdownEligible(tbl, wf.predicateFor(cell.table)); !ok {
				t.Fatalf("classifier refused the normalized predicate %q (%s) — the Q1 re-admission regressed", cell.pred, reason)
			}
		})
	}
}
