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

// oracleCell is one matrix cell: a predicate plus the literals that define
// in-scope (in0/in1) and out-of-scope (out0/out1) values for it. Literals
// are SQL fragments ("NULL" allowed) interpolated into the fixed workload
// script; in0 doubles as the sentinel value, so it must genuinely satisfy
// the predicate.
type oracleCell struct {
	name string
	pred string
	in0  string
	in1  string
	out0 string
	out1 string
}

// oracleFamily is one value family: the column type of `v` plus its cells.
type oracleFamily struct {
	name    string
	colType string
	cells   []oracleCell
}

// oracleMatrix is the ADR-0176 §4 family matrix. The value-family axis
// mirrors the classifier envelope EXACTLY (int, numeric, bool, text
// default-collation, text COLLATE "C", date, timestamp-naive); the
// predicate-shape axis covers =, !=/<>, all four ordering ops (full set on
// int, one representative elsewhere), IN / NOT IN, IS NULL / IS NOT NULL,
// and AND/OR/NOT compositions incl. the NOT(… OR …) three-valued case.
// Every workload carries NULL rows (the fixed script's id=5 arc), so
// NULL-in-every-position rides every cell.
var oracleMatrix = []oracleFamily{
	{
		name: "int", colType: "int",
		cells: []oracleCell{
			{name: "eq", pred: "v = 5", in0: "5", in1: "5", out0: "6", out1: "4"},
			{name: "ne", pred: "v != 5", in0: "6", in1: "4", out0: "5", out1: "5"},
			{name: "lt", pred: "v < 5", in0: "4", in1: "-3", out0: "5", out1: "6"},
			{name: "le", pred: "v <= 5", in0: "5", in1: "4", out0: "6", out1: "100"},
			{name: "gt", pred: "v > 5", in0: "6", in1: "100", out0: "5", out1: "4"},
			{name: "ge", pred: "v >= 5", in0: "5", in1: "6", out0: "4", out1: "-3"},
			{name: "in", pred: "v IN (5, 7)", in0: "5", in1: "7", out0: "6", out1: "8"},
			{name: "notin", pred: "v NOT IN (5, 7)", in0: "6", in1: "8", out0: "5", out1: "7"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "5", out1: "6"},
			{name: "isnotnull", pred: "v IS NOT NULL", in0: "5", in1: "6", out0: "NULL", out1: "NULL"},
			{name: "and", pred: "v >= 5 AND v < 10", in0: "5", in1: "9", out0: "10", out1: "4"},
			{name: "or", pred: "v = 5 OR v = 7", in0: "5", in1: "7", out0: "6", out1: "8"},
			{name: "not_or_3vl", pred: "NOT (v = 5 OR v > 10)", in0: "6", in1: "10", out0: "5", out1: "11"},
		},
	},
	{
		name: "numeric", colType: "numeric(12,4)",
		cells: []oracleCell{
			// 10.5000 == 10.5 numerically: scale must not break equality on
			// either side.
			{name: "eq", pred: "v = 10.5", in0: "10.5", in1: "10.5000", out0: "10.5001", out1: "10"},
			{name: "ne", pred: "v <> 10.5", in0: "10.5001", in1: "10", out0: "10.5", out1: "10.5000"},
			{name: "gt", pred: "v > 10.5", in0: "10.5001", in1: "99999999.9999", out0: "10.5", out1: "0.0001"},
			{name: "in", pred: "v IN (10.5, 20.25)", in0: "10.5", in1: "20.25", out0: "10.4999", out1: "20"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "10.5", out1: "0"},
			{name: "not_or_3vl", pred: "NOT (v = 10.5 OR v > 100)", in0: "99.9999", in1: "0", out0: "10.5", out1: "100.0001"},
		},
	},
	{
		name: "bool", colType: "boolean",
		cells: []oracleCell{
			{name: "eq", pred: "v = TRUE", in0: "TRUE", in1: "TRUE", out0: "FALSE", out1: "FALSE"},
			{name: "ne", pred: "v != FALSE", in0: "TRUE", in1: "TRUE", out0: "FALSE", out1: "FALSE"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "TRUE", out1: "FALSE"},
			{name: "isnotnull", pred: "v IS NOT NULL", in0: "TRUE", in1: "FALSE", out0: "NULL", out1: "NULL"},
		},
	},
	{
		name: "text", colType: "text",
		cells: []oracleCell{
			// Case variant + shared prefix out-values: byte-exact equality on
			// both sides, no case folding, no prefix confusion.
			{name: "eq", pred: "v = 'alpha'", in0: "'alpha'", in1: "'alpha'", out0: "'Alpha'", out1: "'alphax'"},
			// Trailing space: text is NO-PAD on both sides ('alpha ' ≠ 'alpha').
			{name: "eq_trailing_space", pred: "v = 'alpha '", in0: "'alpha '", in1: "'alpha '", out0: "'alpha'", out1: "'alpha  '"},
			{name: "ne", pred: "v != 'alpha'", in0: "'Alpha'", in1: "'beta'", out0: "'alpha'", out1: "'alpha'"},
			// Embedded quote in an IN member: the doubled-quote escape must
			// survive the single-sourced rendering into the publication DDL.
			{name: "in_quote", pred: "v IN ('alpha', 'ga''mma')", in0: "'alpha'", in1: "'ga''mma'", out0: "'gamma'", out1: "'beta'"},
			// Empty string is in-scope and distinct from NULL.
			{name: "notin", pred: "v NOT IN ('alpha', 'beta')", in0: "'x'", in1: "''", out0: "'alpha'", out1: "'beta'"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "''", out1: "'alpha'"},
			{name: "not_or_3vl", pred: "NOT (v = 'alpha' OR v = 'zeta')", in0: "'beta'", in1: "''", out0: "'alpha'", out1: "'zeta'"},
		},
	},
	{
		name: "text_collate_c", colType: `text COLLATE "C"`,
		cells: []oracleCell{
			{name: "eq", pred: "v = 'alpha'", in0: "'alpha'", in1: "'alpha'", out0: "'Alpha'", out1: "'alphax'"},
			{name: "ne", pred: "v != 'alpha'", in0: "'Alpha'", in1: "'beta'", out0: "'alpha'", out1: "'alpha'"},
			{name: "in", pred: "v IN ('alpha', 'beta')", in0: "'alpha'", in1: "'beta'", out0: "'ALPHA'", out1: "'gamma'"},
		},
	},
	{
		name: "date", colType: "date",
		cells: []oracleCell{
			{name: "eq", pred: "v = '2026-01-15'", in0: "'2026-01-15'", in1: "'2026-01-15'", out0: "'2026-01-14'", out1: "'2026-01-16'"},
			{name: "ge", pred: "v >= '2026-01-15'", in0: "'2026-01-15'", in1: "'2027-03-01'", out0: "'2026-01-14'", out1: "'1999-12-31'"},
			{name: "in", pred: "v IN ('2026-01-15', '2026-02-01')", in0: "'2026-01-15'", in1: "'2026-02-01'", out0: "'2026-01-16'", out1: "'2026-02-02'"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "'2026-01-15'", out1: "'2026-01-16'"},
		},
	},
	{
		name: "timestamp", colType: "timestamp",
		cells: []oracleCell{
			{name: "eq", pred: "v = '2026-01-15 08:30:00'", in0: "'2026-01-15 08:30:00'", in1: "'2026-01-15 08:30:00'", out0: "'2026-01-15 08:30:01'", out1: "'2026-01-15 08:29:59.999999'"},
			{name: "lt", pred: "v < '2026-01-15 08:30:00'", in0: "'2026-01-15 08:29:59.999999'", in1: "'1999-01-01 00:00:00'", out0: "'2026-01-15 08:30:00'", out1: "'2026-01-15 08:30:00.000001'"},
			{name: "ne", pred: "v != '2026-01-15 08:30:00'", in0: "'2026-01-15 08:30:00.000001'", in1: "'2020-05-05 05:05:05'", out0: "'2026-01-15 08:30:00'", out1: "'2026-01-15 08:30:00'"},
			{name: "isnull", pred: "v IS NULL", in0: "NULL", in1: "NULL", out0: "'2026-01-15 08:30:00'", out1: "'2026-01-16 00:00:00'"},
		},
	},
}

// oracleWorkload renders the fixed row-move script for one cell. Rows 1–4
// walk every truth-table transition (stay-in / move-in / move-out /
// stay-out, plus in- and out-scope INSERTs and DELETEs); row 5 is the
// NULL arc (NULL insert, NULL→in move, in→NULL move); 9999 is the
// end-of-workload sentinel (in-scope by construction).
func oracleWorkload(table string, c oracleCell) []string {
	q := func(format string, args ...any) string { return fmt.Sprintf(format, args...) }
	return []string{
		q(`INSERT INTO %s (id, v) VALUES (1, %s)`, table, c.in0),
		q(`INSERT INTO %s (id, v) VALUES (2, %s)`, table, c.out0),
		q(`INSERT INTO %s (id, v) VALUES (3, %s)`, table, c.in0),
		q(`INSERT INTO %s (id, v) VALUES (4, %s)`, table, c.out0),
		q(`INSERT INTO %s (id, v) VALUES (5, NULL)`, table),
		q(`UPDATE %s SET v = %s WHERE id = 1`, table, c.in1),        // stay-in
		q(`UPDATE %s SET v = %s WHERE id = 2`, table, c.in0),        // move-IN
		q(`UPDATE %s SET v = %s WHERE id = 3`, table, c.out0),       // move-OUT
		q(`UPDATE %s SET v = %s WHERE id = 4`, table, c.out1),       // stay-out
		q(`UPDATE %s SET v = %s WHERE id = 5`, table, c.in0),        // NULL→in
		q(`UPDATE %s SET v = NULL WHERE id = 5`, table),             // in→NULL
		q(`DELETE FROM %s WHERE id = 1`, table),                     // delete in-scope
		q(`DELETE FROM %s WHERE id = 4`, table),                     // delete out-of-scope
		q(`INSERT INTO %s (id, v) VALUES (9999, %s)`, table, c.in0), // sentinel
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
	// the filtered-sync precondition (SLUICE-E-WHERE-CDC-BEFORE-IMAGE).
	for _, fam := range oracleMatrix {
		applyDDL(t, sourceDSN, fmt.Sprintf(
			`CREATE TABLE orc_%s (id int PRIMARY KEY, v %s);
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
