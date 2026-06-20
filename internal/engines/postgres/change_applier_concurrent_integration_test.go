//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the ADR-0105 (item 26) concurrent key-hash CDC apply
// on the Postgres target. PG joins the GA MySQL target on the shared
// internal/laneapply core: the merged change stream is fanned across W
// in-order lanes by primary-key hash (same key → same lane → in-order), each
// committing concurrently on a dedicated backend, with the resume position
// advanced only to a fully-durable source boundary (the seq-frontier). These
// pins are the load-bearing safety net (mirroring the MySQL ADR-0104 matrix):
//
//   - EXACTLY-ONCE / DEPENDENT-ROW ORDERING UNDER CONCURRENCY: a stream of
//     Insert→Update→Delete on the same PK split across batches yields the
//     correct final state at depth=W (all ops for a key land on one lane in
//     source order); cross-key work runs concurrently. Idempotent replay is a
//     no-op (ADR-0010 UPSERT).
//   - VALUE-FIDELITY DIFFERENTIAL (pin the class, Bug-74 corollary): the SAME
//     ordered stream applied serially (W=1) and concurrently (W=4) produces
//     byte-identical target state, exercised across the full VALUE-TYPE
//     FAMILY MATRIX (numeric/decimal extremes, text/varchar/uuid, json/jsonb,
//     timestamp µs, bool, bytea, AND arrays int/text/numeric — the Bug-74
//     family) so a missing lane-pool codec is caught. Geometry is covered by
//     the postgis-gated sibling pin (the standard rig has no PostGIS).
//   - POSITION (seq-frontier): a boundary-less stream (every change a distinct
//     position) still persists the last fully-durable position.
//   - WARM-RESUME UNDER THE KNOB: interrupt mid-CDC, restart with W=4, no full
//     re-copy, finishes exactly-once with no position regression.
//   - W=1 ≡ unset ≡ serial byte-identical.
//
// NOTE: this is a CONCURRENCY chunk — the -race Integration job on CI is the
// authoritative gate (this box is CGO=0 so -race can't run locally).

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/ir"
)

const concurrentLanesW = 4

// openConcurrentApplier opens a ChangeApplier against dsn and wires the
// ADR-0105 key-hash apply lane count when lanes > 1. It asserts pipelineCfg
// is set (so applyConcurrency > 1 actually engages the lane path rather than
// silently falling back to the serial batch loop).
func openConcurrentApplier(t *testing.T, ctx context.Context, dsn string, lanes int) *ChangeApplier {
	t.Helper()
	a, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	ca := a.(*ChangeApplier)
	if lanes > 1 {
		if ca.pipelineCfg == nil {
			t.Fatal("pipelineCfg is nil — concurrent lane path would silently fall back to serial (ADR-0105)")
		}
		ca.SetApplyConcurrency(lanes)
	}
	return ca
}

// pumpConcurrentChanges feeds events through ApplyBatch under an explicit
// streamID (so the differential's serial and concurrent passes can run under
// distinct stream rows against the same target).
func pumpConcurrentChanges(t *testing.T, ctx context.Context, applier ir.ChangeApplier, streamID string, events []ir.Change, batchSize int) {
	t.Helper()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	batched, ok := applier.(ir.BatchedChangeApplier)
	if !ok {
		t.Fatalf("applier does not implement BatchedChangeApplier")
	}
	ch := make(chan ir.Change, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	if err := batched.ApplyBatch(ctx, streamID, ch, batchSize); err != nil {
		t.Fatalf("ApplyBatch (stream %s): %v", streamID, err)
	}
}

func cpos(tok string) ir.Position {
	return ir.Position{Engine: engineNamePostgres, Token: tok}
}

// TestConcurrentApply_DependentOrderingAndExactlyOnce pins the core
// correctness claim: across W lanes, every operation on a given key is
// applied in source order (Insert→Update→Delete cannot reorder), so a stream
// that inserts then deletes the first half leaves only the second half, each
// with its updated value. Many keys exercise real cross-lane concurrency;
// same-key sequences exercise the in-lane ordering guarantee. Idempotent
// replay leaves the state unchanged.
func TestConcurrentApply_DependentOrderingAndExactlyOnce(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `CREATE TABLE conc_dep (id BIGINT PRIMARY KEY, v TEXT NOT NULL);`)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer func() { _ = a.Close() }()

	const keys = 200
	var events []ir.Change
	seq := 0
	tok := func() string { seq++; return fmt.Sprintf("t-%06d", seq) }

	for i := int64(1); i <= keys; i++ {
		events = append(events, ir.Insert{Position: cpos(tok()), Schema: "public", Table: "conc_dep", Row: ir.Row{"id": i, "v": "init"}})
	}
	for i := int64(1); i <= keys; i++ {
		events = append(events, ir.Update{Position: cpos(tok()), Schema: "public", Table: "conc_dep", Before: ir.Row{"id": i, "v": "init"}, After: ir.Row{"id": i, "v": "final"}})
	}
	for i := int64(1); i <= keys/2; i++ {
		events = append(events, ir.Delete{Position: cpos(tok()), Schema: "public", Table: "conc_dep", Before: ir.Row{"id": i, "v": "final"}})
	}

	pumpConcurrentChanges(t, ctx, a, testStreamID, events, 7)

	if got := countAllRows(t, dsn, "conc_dep"); got != keys/2 {
		t.Fatalf("rows = %d; want %d (first half deleted)", got, keys/2)
	}
	if got := queryScalarString(t, dsn, "SELECT v FROM conc_dep WHERE id = 200"); got != "final" {
		t.Errorf("id=200 v=%q; want final (Update must land after Insert on its lane)", got)
	}
	if n := countAllRows(t, dsn, "conc_dep WHERE id = 1"); n != 0 {
		t.Errorf("id=1 should have been deleted (Delete must land after Insert/Update on its lane)")
	}

	// Idempotent replay: re-apply the whole stream; final state unchanged.
	pumpConcurrentChanges(t, ctx, a, testStreamID, events, 7)
	if got := countAllRows(t, dsn, "conc_dep"); got != keys/2 {
		t.Errorf("after replay: rows = %d; want %d (idempotency violated)", got, keys/2)
	}
}

// TestConcurrentApply_SerialDifferential is the Bug-74-corollary pin: the
// SAME ordered multi-table stream applied serially (W=1) and concurrently
// (W=4) must produce byte-identical target state across the full VALUE-TYPE
// FAMILY MATRIX. A missing/wrong codec on the dedicated lane pool (e.g. the
// geometry codec — or any per-OID array codec) would surface here as a
// divergence. Two table sets (serial-suffixed vs concurrent-suffixed) on one
// database, identical input, compared with EXCEPT in both directions.
func TestConcurrentApply_SerialDifferential(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// The family-matrix table: one column per value-type family the PG
	// applier's prepareValue / per-OID codecs dispatch on. The differential
	// proves the lane pool encodes every family byte-identically to serial.
	ddl := func(suffix string) string {
		return fmt.Sprintf(`CREATE TABLE fam_%s (
			id      BIGINT PRIMARY KEY,
			n_int   INTEGER,
			n_big   BIGINT,
			n_num   NUMERIC(38,10),
			t_text  TEXT,
			t_vc    VARCHAR(64),
			u_uuid  UUID,
			j_json  JSON,
			j_jsonb JSONB,
			ts_us   TIMESTAMP(6),
			b_bool  BOOLEAN,
			y_bytea BYTEA,
			a_int   INTEGER[],
			a_text  TEXT[],
			a_num   NUMERIC(20,4)[],
			a_int2d INTEGER[][],
			a_num2d NUMERIC(20,4)[][]
		);`, suffix)
	}

	mkStream := func(table string) []ir.Change {
		var ev []ir.Change
		seq := 0
		tok := func() string { seq++; return fmt.Sprintf("d-%s-%06d", table, seq) }
		mkRow := func(i int64) ir.Row {
			return ir.Row{
				"id":      i,
				"n_int":   -2147483648 + i,
				"n_big":   9223372036854775807 - i,
				"n_num":   fmt.Sprintf("%d.1234567890", i*1000000),
				"t_text":  fmt.Sprintf("row-%d-ünïcödé-\t-tab", i),
				"t_vc":    fmt.Sprintf("vc%d", i),
				"u_uuid":  fmt.Sprintf("00000000-0000-0000-0000-%012d", i),
				"j_json":  fmt.Sprintf(`{"k":%d,"a":[1,2,3]}`, i),
				"j_jsonb": fmt.Sprintf(`{"z":%d,"nested":{"q":"v"}}`, i),
				"ts_us":   "2024-01-02 03:04:05.123456",
				"b_bool":  i%2 == 0,
				"y_bytea": []byte{0x00, 0x01, byte(i % 256), 0xff},
				// Array families × shape (Bug-74 matrix): native int (1-D + 2-D),
				// string-leaf text (1-D, with NULL element), decimal numeric
				// (1-D + 2-D — the family that silently flattened in Bug 74).
				"a_int":   []any{i, i + 1, nil},
				"a_text":  []any{fmt.Sprintf("e%d", i), nil, "z"},
				"a_num":   []any{"1.5", "2.25", fmt.Sprintf("%d.0001", i)},
				"a_int2d": []any{[]any{i, int64(2)}, []any{int64(3), i + 4}},
				"a_num2d": []any{[]any{"1.1", "2.2"}, []any{"3.3", fmt.Sprintf("%d.4", i)}},
			}
		}
		for i := int64(1); i <= 80; i++ {
			ev = append(ev, ir.Insert{Position: cpos(tok()), Schema: "public", Table: table, Row: mkRow(i)})
		}
		// Updates on odd ids (change a value in every family).
		for i := int64(1); i <= 80; i += 2 {
			after := mkRow(i)
			after["t_text"] = "UPD"
			after["n_num"] = "0.0000000001"
			after["a_num2d"] = []any{[]any{"9.9", "8.8"}}
			ev = append(ev, ir.Update{Position: cpos(tok()), Schema: "public", Table: table, Before: mkRow(i), After: after})
		}
		// Deletes on ids divisible by 4.
		for i := int64(4); i <= 80; i += 4 {
			ev = append(ev, ir.Delete{Position: cpos(tok()), Schema: "public", Table: table, Before: mkRow(i)})
		}
		return ev
	}

	applyPGApplier(t, dsn, ddl("serial")+ddl("conc"))

	// Serial pass (W=1, the ADR-0092 batch loop).
	aSerial := openConcurrentApplier(t, ctx, dsn, 1)
	defer func() { _ = aSerial.Close() }()
	pumpConcurrentChanges(t, ctx, aSerial, "stream-serial", mkStream("fam_serial"), 9)

	// Concurrent pass (W=4) WITH per-lane AIMD controllers wired, so the
	// differential proves byte-identity HOLDS with controllers engaged (value
	// encoding is independent of WHEN/whether a batch is sized or retried).
	aConc := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer func() { _ = aConc.Close() }()
	ctrls := make([]ir.BatchSizeController, concurrentLanesW)
	for i := range ctrls {
		c, err := appliercontrol.New(appliercontrol.Config{
			StreamID: "stream-conc", Floor: 1, Ceiling: 9, InitialSize: 9, TargetLatency: 10 * time.Second,
		})
		if err != nil {
			t.Fatalf("controller %d: %v", i, err)
		}
		ctrls[i] = c
	}
	aConc.SetLaneAIMDControllers(ctrls)
	pumpConcurrentChanges(t, ctx, aConc, "stream-conc", mkStream("fam_conc"), 9)

	// Byte-identical differential across EVERY family column (cast arrays /
	// json / bytea / numeric to ::text so EXCEPT compares the canonical
	// rendered value, catching a per-family codec divergence such as a 2-D
	// array silently flattened — Bug 74).
	const cols = `id, n_int, n_big, n_num::text, t_text, t_vc, u_uuid::text,
		j_json::text, j_jsonb::text, ts_us::text, b_bool, encode(y_bytea,'hex'),
		a_int::text, a_text::text, a_num::text, a_int2d::text, a_num2d::text`
	assertNoDifference(t, ctx, dsn, "fam_serial", "fam_conc", cols)
}

// TestConcurrentApply_BoundarylessStreamPersistsPosition pins the
// position-change boundary detection: a stream with NO Tx boundaries (every
// change a distinct position) must still persist a resume position — the last
// change's token once all lanes are durable — not stall at none.
func TestConcurrentApply_BoundarylessStreamPersistsPosition(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `CREATE TABLE conc_pos (id BIGINT PRIMARY KEY, v TEXT NOT NULL);`)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer func() { _ = a.Close() }()

	const n = 100
	lastTok := fmt.Sprintf("p-%06d", n)
	var ev []ir.Change
	for i := int64(1); i <= n; i++ {
		ev = append(ev, ir.Insert{Position: cpos(fmt.Sprintf("p-%06d", i)), Schema: "public", Table: "conc_pos", Row: ir.Row{"id": i, "v": "x"}})
	}
	pumpConcurrentChanges(t, ctx, a, testStreamID, ev, 8)

	if got := countAllRows(t, dsn, "conc_pos"); got != n {
		t.Fatalf("rows = %d; want %d", got, n)
	}
	pos, ok, err := a.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("ReadPosition: ok=%v err=%v", ok, err)
	}
	if pos.Token != lastTok {
		t.Errorf("persisted position = %q; want %q (seq-frontier must reach the last durable change)", pos.Token, lastTok)
	}
}

// TestConcurrentApply_WarmResumeUnderKnob pins warm-resume under the lane
// knob: apply a prefix at W=4 (persisting a position), then open a FRESH
// applier at W=4 and apply the FULL stream again (the warm-resume shape — the
// streamer re-streams from the persisted position; here we re-feed the whole
// stream and rely on ADR-0010 idempotency). The final state is exactly-once
// (no dup), the position advances to the last change, and there is no
// regression. This is the crash-resume contract under concurrency.
func TestConcurrentApply_WarmResumeUnderKnob(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `CREATE TABLE conc_resume (id BIGINT PRIMARY KEY, v TEXT NOT NULL);`)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const keys = 150
	mk := func(lo, hi int64) []ir.Change {
		var ev []ir.Change
		for i := lo; i <= hi; i++ {
			ev = append(ev, ir.Insert{Position: cpos(fmt.Sprintf("r-%06d", i)), Schema: "public", Table: "conc_resume", Row: ir.Row{"id": i, "v": "x"}})
		}
		return ev
	}

	// Phase 1: apply the first 60 keys at W=4 (a partial prefix), persisting a
	// position somewhere in that prefix.
	a1 := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	pumpConcurrentChanges(t, ctx, a1, testStreamID, mk(1, 60), 8)
	pos1, ok, err := a1.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("after prefix ReadPosition: ok=%v err=%v", ok, err)
	}
	_ = a1.Close()

	// Phase 2: a fresh applier (the restart) re-applies the FULL stream at
	// W=4. Idempotent UPSERT makes the re-applied prefix a no-op; the tail
	// (61..150) lands new. Exactly-once: every key present exactly once.
	a2 := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer func() { _ = a2.Close() }()
	pumpConcurrentChanges(t, ctx, a2, testStreamID, mk(1, keys), 8)

	if got := countAllRows(t, dsn, "conc_resume"); got != keys {
		t.Fatalf("rows = %d; want %d (exactly-once across warm-resume)", got, keys)
	}
	pos2, ok, err := a2.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("after resume ReadPosition: ok=%v err=%v", ok, err)
	}
	if pos2.Token != fmt.Sprintf("r-%06d", keys) {
		t.Errorf("resumed position = %q; want last change r-%06d", pos2.Token, keys)
	}
	if pos2.Token < pos1.Token {
		t.Errorf("position regressed: prefix=%q resume=%q", pos1.Token, pos2.Token)
	}
}

// TestConcurrentApply_W1EquivalentToSerial pins the zero-value/W=1 contract:
// W=1 (explicitly set) and unset (serial batch path) produce byte-identical
// target state for the same stream — concurrency engages ONLY for W>1.
func TestConcurrentApply_W1EquivalentToSerial(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyPGApplier(t, dsn, `
		CREATE TABLE w1_unset (id BIGINT PRIMARY KEY, n INT, s TEXT, arr INT[]);
		CREATE TABLE w1_one   (id BIGINT PRIMARY KEY, n INT, s TEXT, arr INT[]);`)

	mkStream := func(table string) []ir.Change {
		var ev []ir.Change
		for i := int64(1); i <= 40; i++ {
			ev = append(ev, ir.Insert{Position: cpos(fmt.Sprintf("w-%s-%d", table, i)), Schema: "public", Table: table, Row: ir.Row{"id": i, "n": i * 3, "s": fmt.Sprintf("s%d", i), "arr": []any{i, nil}}})
		}
		return ev
	}

	// Unset (zero-value applyConcurrency → serial batch loop).
	aUnset := openConcurrentApplier(t, ctx, dsn, 1) // lanes==1 → SetApplyConcurrency NOT called; field stays 0
	defer func() { _ = aUnset.Close() }()
	pumpConcurrentChanges(t, ctx, aUnset, "w1-unset", mkStream("w1_unset"), 7)

	// Explicit W=1: SetApplyConcurrency(1) → still serial (W>1 gate).
	aOne := openConcurrentApplier(t, ctx, dsn, 1)
	aOne.SetApplyConcurrency(1)
	defer func() { _ = aOne.Close() }()
	pumpConcurrentChanges(t, ctx, aOne, "w1-one", mkStream("w1_one"), 7)

	assertNoDifference(t, ctx, dsn, "w1_unset", "w1_one", "id, n, s, arr::text")
}

// TestConcurrentApply_FirstBoundarySchemaSnapshot_Bug158 is the Bug 158
// regression pin: a CDC stream whose FIRST event is a SchemaSnapshot barrier
// (the first-post-cold-start boundary — pgoutput's first-touch RelationMessage,
// which carries WAL position 0/0) followed by the full value-family row stream
// must, at W=4, apply EVERY row byte-identically to serial AND persist a REAL
// resume position (never the SchemaSnapshot's metadata-anchored 0/0 token).
//
// The CRITICAL silent-loss this pins (the data half): on v0.99.82 the
// concurrent orchestrator UNCONDITIONALLY invalidated the table's metadata
// caches on every SchemaSnapshot barrier (even the first-touch baseline),
// marking the table schema-dirty. That forced every subsequent lane DML onto
// pgx's QueryExecModeExec text-encode path, where json/jsonb (and other typed)
// values fail to encode ("invalid input syntax for type json", SQLSTATE 22P02)
// → the lane aborted → the whole run wedged at position 0/0 → ALL post-baseline
// changes silently lost. The serial path never hit it because its boundary
// invalidation (cacheActiveSchemaAfterCommit) is GUARDED to fire only on a real
// signature-changing boundary, not the baseline. The fix defers all barrier
// invalidation to ApplyBarrierChange (which runs that same guarded path), so
// the concurrent baseline boundary no longer over-invalidates.
//
// The position half: ApplyBarrierChange used to write the SchemaSnapshot's own
// 0/0 token as the resume position; the fix makes the concurrent barrier apply
// position-free (the frontier checkpoint owns the resume position), and the
// orchestrator excludes a SchemaSnapshot from boundary tracking, so the
// persisted position advances to the surrounding rows' real LSN — warm-resume
// works instead of cold-starting from 0/0 forever.
//
// This is the Bug-74 reviewer-corollary realized: the differential exercises
// EVERY value family (numeric extremes, text/varchar/uuid, json/jsonb,
// timestamp µs, bool, bytea, AND arrays int/text/numeric incl. 2-D + NULL) —
// the json/jsonb family is the one that silently dropped; the rest guard the
// class.
func TestConcurrentApply_FirstBoundarySchemaSnapshot_Bug158(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	ddl := func(suffix string) string {
		return fmt.Sprintf(`CREATE TABLE b158_%s (
			id      BIGINT PRIMARY KEY,
			n_int   INTEGER,
			n_num   NUMERIC(38,10),
			t_text  TEXT,
			u_uuid  UUID,
			j_json  JSON,
			j_jsonb JSONB,
			ts_us   TIMESTAMP(6),
			b_bool  BOOLEAN,
			y_bytea BYTEA,
			a_int   INTEGER[],
			a_text  TEXT[],
			a_num2d NUMERIC(20,4)[][]
		);`, suffix)
	}

	// tableIR mirrors the DDL so the SchemaSnapshot's IR signature matches the
	// table — the FIRST-TOUCH BASELINE case (no prior version on this stream →
	// cacheActiveSchemaAfterCommit's guard must NOT invalidate). This is exactly
	// the phantom-skip shape: a SchemaSnapshot that does not change the shape.
	tableIR := func(table string) *ir.Table {
		col := func(name string, typ ir.Type) *ir.Column { return &ir.Column{Name: name, Type: typ} }
		return &ir.Table{
			Schema: "public", Name: table,
			Columns: []*ir.Column{
				col("id", ir.Integer{Width: 64}),
				col("n_int", ir.Integer{Width: 32}),
				col("n_num", ir.Decimal{Precision: 38, Scale: 10}),
				col("t_text", ir.Text{}),
				col("u_uuid", ir.UUID{}),
				col("j_json", ir.JSON{Binary: false}),
				col("j_jsonb", ir.JSON{Binary: true}),
				col("ts_us", ir.Timestamp{Precision: 6}),
				col("b_bool", ir.Boolean{}),
				col("y_bytea", ir.Blob{Size: ir.BlobLong}),
				col("a_int", ir.Array{Element: ir.Integer{Width: 32}}),
				col("a_text", ir.Array{Element: ir.Text{}}),
				col("a_num2d", ir.Array{Element: ir.Array{Element: ir.Decimal{Precision: 20, Scale: 4}}}),
			},
			PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
		}
	}

	// CRITICAL value-shape fidelity (Bug 158 + Bug 74): the PG CDC reader
	// decodes json/jsonb to []byte (value_decode.go: ir.JSON → decodeBytes),
	// NOT a Go string. The bug only fires for the []byte shape: under the
	// over-invalidation's QueryExecModeExec text-encode path, pgx rejects a
	// []byte bound to a json column ("invalid input syntax for type json",
	// 22P02). A Go string would have round-tripped and HIDDEN the bug — so the
	// pin MUST use the same []byte shape the CDC decode produces.
	mkRow := func(i int64) ir.Row {
		return ir.Row{
			"id":      i,
			"n_int":   -2147483648 + i,
			"n_num":   fmt.Sprintf("%d.1234567890", i*1000000),
			"t_text":  fmt.Sprintf("row-%d-ünïcödé-世界", i),
			"u_uuid":  fmt.Sprintf("00000000-0000-0000-0000-%012d", i),
			"j_json":  []byte(fmt.Sprintf(`{"k":%d,"u":"世界"}`, i)),
			"j_jsonb": []byte(fmt.Sprintf(`{"z":%d,"nested":{"q":[1,2,3]}}`, i)),
			"ts_us":   "2024-01-02 03:04:05.123456",
			"b_bool":  i%2 == 0,
			"y_bytea": []byte{0x00, 0x01, byte(i % 256), 0xff},
			"a_int":   []any{i, i + 1, nil},
			"a_text":  []any{fmt.Sprintf("e%d", i), nil, "z"},
			"a_num2d": []any{[]any{"1.1", "2.2"}, []any{"3.3", fmt.Sprintf("%d.4", i)}},
		}
	}

	// The Bug-158 stream shape: a first-touch SchemaSnapshot BARRIER carrying
	// the 0/0 metadata-anchored token, THEN the value-family row stream carrying
	// real positions. Under the bug, the barrier marked the table schema-dirty
	// → every row's json/jsonb encode failed → nothing applied.
	const snapTok = `{"slot":"sluice_slot","lsn":"0/0"}`
	lastRowTok := func(table string) string { return fmt.Sprintf(`{"slot":"sluice_slot","lsn":"row-%s-final"}`, table) }
	mkStream := func(table string) []ir.Change {
		ev := []ir.Change{
			ir.SchemaSnapshot{Position: cpos(snapTok), Schema: "public", Table: table, IR: tableIR(table)},
		}
		seq := 0
		tok := func() string { seq++; return fmt.Sprintf(`{"slot":"sluice_slot","lsn":"row-%s-%06d"}`, table, seq) }
		for i := int64(1); i <= 40; i++ {
			ev = append(ev, ir.Insert{Position: cpos(tok()), Schema: "public", Table: table, Row: mkRow(i)})
		}
		// Dependent same-key sequences (INSERT→UPDATE→DELETE) exercising the
		// value families through the lane on a re-touched key.
		for i := int64(1); i <= 40; i += 2 {
			after := mkRow(i)
			after["t_text"] = "UPD-世界"
			after["j_jsonb"] = []byte(`{"upd":true}`)
			after["a_num2d"] = []any{[]any{"9.9", "8.8"}}
			ev = append(ev, ir.Update{Position: cpos(tok()), Schema: "public", Table: table, Before: mkRow(i), After: after})
		}
		for i := int64(4); i <= 40; i += 4 {
			ev = append(ev, ir.Delete{Position: cpos(tok()), Schema: "public", Table: table, Before: mkRow(i)})
		}
		// A final distinct-token insert so the last persisted position is
		// deterministic for the position assertion.
		ev = append(ev, ir.Insert{Position: cpos(lastRowTok(table)), Schema: "public", Table: table, Row: mkRow(9999)})
		return ev
	}

	applyPGApplier(t, dsn, ddl("serial")+ddl("conc"))

	aSerial := openConcurrentApplier(t, ctx, dsn, 1)
	defer func() { _ = aSerial.Close() }()
	pumpConcurrentChanges(t, ctx, aSerial, "b158-serial", mkStream("b158_serial"), 9)

	aConc := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer func() { _ = aConc.Close() }()
	pumpConcurrentChanges(t, ctx, aConc, "b158-conc", mkStream("b158_conc"), 9)

	// (1) Data half: byte-identical across EVERY value family (the json/jsonb
	// columns are the ones that silently dropped under the bug).
	const cols = `id, n_int, n_num::text, t_text, u_uuid::text,
		j_json::text, j_jsonb::text, ts_us::text, b_bool, encode(y_bytea,'hex'),
		a_int::text, a_text::text, a_num2d::text`
	assertNoDifference(t, ctx, dsn, "b158_serial", "b158_conc", cols)

	// (2) Row count must be the full applied set (not frozen at 0) — a direct
	// guard against the silent-total-loss symptom (dst stayed at the seed).
	if got := countAllRows(t, dsn, "b158_conc"); got == 0 {
		t.Fatal("concurrent target empty — the first-boundary SchemaSnapshot swallowed every row (Bug 158)")
	}

	// (3) Position half: the persisted resume position must be the last REAL
	// row token, NEVER the SchemaSnapshot's 0/0 metadata anchor (which pinned
	// the stream at 0/0 and broke warm-resume).
	pos, ok, err := aConc.ReadPosition(ctx, "b158-conc")
	if err != nil || !ok {
		t.Fatalf("ReadPosition(concurrent): ok=%v err=%v", ok, err)
	}
	if pos.Token == snapTok {
		t.Fatalf("persisted position pinned at the SchemaSnapshot's 0/0 token %q — "+
			"the barrier must not persist a metadata-anchored position (Bug 158, position half)", pos.Token)
	}
	if pos.Token != lastRowTok("b158_conc") {
		t.Errorf("persisted position = %q; want the last real row token %q "+
			"(the frontier checkpoint must advance past the 0/0 baseline)", pos.Token, lastRowTok("b158_conc"))
	}
}

// assertNoDifference fails the test unless tableA and tableB hold the exact
// same rows over the given projected columns (EXCEPT in both directions).
func assertNoDifference(t *testing.T, ctx context.Context, dsn, tableA, tableB, cols string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	check := func(left, right string) {
		q := fmt.Sprintf(`SELECT COUNT(*) FROM (
			SELECT %[1]s FROM %[2]s
			EXCEPT
			SELECT %[1]s FROM %[3]s) d`, cols, left, right)
		var diff int
		if err := db.QueryRowContext(ctx, q).Scan(&diff); err != nil {
			t.Fatalf("differential %s EXCEPT %s: %v", left, right, err)
		}
		if diff != 0 {
			t.Errorf("%s has %d row(s) absent from %s — concurrent apply diverged from serial (a per-family codec or ordering bug)", left, diff, right)
		}
	}
	check(tableA, tableB)
	check(tableB, tableA)
}
