//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-07-23 D0-6 / G-6, owner call Q4 — the real-PG non-finite float
// gate: the snapshot leg (a server-evaluated WHERE) and the client CDC leg
// (the rowpredicate evaluator over DECODED changes) must classify NaN and
// ±Inf rows IDENTICALLY under a float ordering predicate. Postgres orders
// float with a TOTAL order (NaN greater than everything, NaN = NaN —
// `'NaN'::float8 > 0.1` is TRUE), and it is the only supported source that
// can deliver a float NaN (MySQL/MariaDB cannot store one; SQLite stores NaN
// as NULL). Pre-Q4 the client mapped NaN to UNKNOWN→drop: the snapshot
// copied a NaN row and the CDC leg then dropped its every UPDATE (stale
// target row) and swallowed its DELETE (orphan forever), at exit 0. This
// test drives real logical decoding so the NaN/±Inf-through-pgoutput leg —
// derived-not-verified in the audit — is ground-truthed, not assumed.
//
// Float stays OUTSIDE the push-down envelope (TestPGPushdownEligible_
// EnvelopePin): Q4 fixes client-leg vs snapshot consistency, not push-down
// eligibility, and this test pins that refusal too.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestWhereCDC_PGFloatNonFinite(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const table = "orcf"
	applyDDL(t, sourceDSN, `
		CREATE TABLE orcf (id int PRIMARY KEY, v float8, w int);
		ALTER TABLE orcf REPLICA IDENTITY FULL;
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

	// One live logical stream over an UNFILTERED publication — the client
	// evaluator is the only filter, exactly the production shape for a
	// float predicate (float is outside the push-down envelope).
	ctx := context.Background()
	const pub, slot = "sluice_orcf", "sluice_orcf"
	eng := pgEng.(ir.PublicationScoper).WithPublicationScope(pub, slot)
	if err := eng.(publicationEnsurer).EnsurePublication(ctx, sourceDSN, []string{table}); err != nil {
		t.Fatalf("ensure publication: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, "DROP PUBLICATION IF EXISTS "+pub) }()

	streamCtx, cancelStream := context.WithCancel(ctx)
	rdr, err := eng.(ir.CDCReaderWithSlotOpener).OpenCDCReaderWithSlot(ctx, sourceDSN, slot)
	if err != nil {
		t.Fatalf("open CDC reader: %v", err)
	}
	rdr.(ir.FullBeforeImageSetter).SetFullBeforeImageTables(map[string]bool{table: true})
	ch, err := rdr.StreamChanges(streamCtx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	defer func() {
		cancelStream()
		migcore.CloseIf(rdr)
		dropSlotWithRetry(t, db, slot, 30*time.Second)
	}()

	// The non-finite workload: NaN, ±Inf, two finite controls; then an
	// UPDATE and a DELETE on the NaN row — the exact change shapes the
	// pre-Q4 evaluator silently dropped (stale row / orphan).
	for _, stmt := range []string{
		`INSERT INTO orcf (id, v, w) VALUES (1, 'NaN'::float8, 0)`,
		`INSERT INTO orcf (id, v, w) VALUES (2, 'Infinity'::float8, 0)`,
		`INSERT INTO orcf (id, v, w) VALUES (3, '-Infinity'::float8, 0)`,
		`INSERT INTO orcf (id, v, w) VALUES (4, 0.5, 0)`,
		`INSERT INTO orcf (id, v, w) VALUES (5, 0.01, 0)`,
		`UPDATE orcf SET w = 1 WHERE id = 1`,
		`DELETE FROM orcf WHERE id = 1`,
		`INSERT INTO orcf (id, v, w) VALUES (9999, 0.5, 0)`, // sentinel
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("workload %q: %v", stmt, err)
		}
	}
	raw := drainUntilSentinel(t, ch, table, 60*time.Second)

	// Decode-leg ground truth: pgoutput must deliver 'NaN'/'±Infinity' as
	// the value-contract float64s (the audit's derived-not-verified link).
	decoded := map[int64]ir.Row{}
	var nanUpdate, nanDelete ir.Change
	for _, c := range raw {
		switch e := c.(type) {
		case ir.Insert:
			if id, ok := e.Row["id"].(int64); ok {
				decoded[id] = e.Row
			} else if id32, ok := e.Row["id"].(int32); ok {
				decoded[int64(id32)] = e.Row
			}
		case ir.Update:
			nanUpdate = e
		case ir.Delete:
			nanDelete = e
		}
	}
	nanV, ok := decoded[1]["v"].(float64)
	if !ok || !math.IsNaN(nanV) {
		t.Fatalf("decoded NaN row v = %#v; want float64 NaN (the pgoutput decode leg)", decoded[1]["v"])
	}
	if inf, ok := decoded[2]["v"].(float64); !ok || !math.IsInf(inf, 1) {
		t.Fatalf("decoded +Inf row v = %#v; want float64 +Inf", decoded[2]["v"])
	}
	if inf, ok := decoded[3]["v"].(float64); !ok || !math.IsInf(inf, -1) {
		t.Fatalf("decoded -Inf row v = %#v; want float64 -Inf", decoded[3]["v"])
	}

	// Snapshot-leg vs CDC-leg agreement, per (ordering op × row): the
	// server's own WHERE verdict is the oracle for the client Eval on the
	// DECODED row. NaN rows are the RED cells on the pre-Q4 evaluator.
	// (The rows were captured from the stream; ids 1 and 9999 were
	// deleted/added after, so verdicts run on VALUES-projected literals
	// mirroring each decoded row rather than the live table.)
	rowLits := map[int64]string{1: "'NaN'::float8", 2: "'Infinity'::float8", 3: "'-Infinity'::float8", 4: "0.5::float8", 5: "0.01::float8"}
	for _, pred := range []string{"v > 0.1", "v >= 0.1", "v < 0.1", "v <= 0.1"} {
		wf, err := buildWhereCDCFilter(resolver, map[string]string{table: pred}, schema, false)
		if err != nil {
			t.Fatalf("compile %q: %v", pred, err)
		}
		for id, lit := range rowLits {
			var serverMatches int
			if err := db.QueryRowContext(ctx,
				fmt.Sprintf(`SELECT count(*) FROM (VALUES (%s)) AS r(v) WHERE %s`, lit, pred)).Scan(&serverMatches); err != nil {
				t.Fatalf("server verdict for %q on %s: %v", pred, lit, err)
			}
			client := wf.predicateFor(table).Eval(decoded[id])
			if (serverMatches > 0) != client {
				t.Errorf("row %d (%s) under %q: server=%v client=%v — snapshot and CDC legs disagree (audit 2026-07-23 D0-6)",
					id, lit, pred, serverMatches > 0, client)
			}
		}

		// Float stays OUTSIDE the push-down envelope: Q4 is a client-leg
		// consistency fix, not a push-down admission.
		var tbl *ir.Table
		for _, tb := range schema.Tables {
			if tb != nil && strings.EqualFold(tb.Name, table) {
				tbl = tb
			}
		}
		if ok, _ := pgPushdownEligible(tbl, wf.predicateFor(table)); ok {
			t.Fatalf("float ordering predicate %q must stay outside the push-down envelope", pred)
		}
	}

	// The D0-6 consequence shapes, routed through the production filter
	// under `v > 0.1` (NaN is in scope server-side): the NaN row's UPDATE
	// must apply as an UPDATE and its DELETE as a DELETE — pre-Q4 both were
	// silently dropped (stale target row, then a permanent orphan).
	wf, err := buildWhereCDCFilter(resolver, map[string]string{table: "v > 0.1"}, schema, false)
	if err != nil {
		t.Fatalf("compile route filter: %v", err)
	}
	if nanUpdate == nil {
		t.Fatal("workload UPDATE on the NaN row never arrived")
	}
	emitted, err := wf.route(nanUpdate)
	if err != nil {
		t.Fatalf("route NaN UPDATE: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("route NaN UPDATE emitted %d changes; want 1 (the UPDATE)", len(emitted))
	}
	if _, ok := emitted[0].(ir.Update); !ok {
		t.Fatalf("route NaN UPDATE emitted %T; want ir.Update (an in-scope row must stay in scope)", emitted[0])
	}
	if nanDelete == nil {
		t.Fatal("workload DELETE on the NaN row never arrived")
	}
	emitted, err = wf.route(nanDelete)
	if err != nil {
		t.Fatalf("route NaN DELETE: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("route NaN DELETE emitted %d changes; want 1 (the DELETE)", len(emitted))
	}
	if _, ok := emitted[0].(ir.Delete); !ok {
		t.Fatalf("route NaN DELETE emitted %T; want ir.Delete (dropping it orphans the target row)", emitted[0])
	}
}
