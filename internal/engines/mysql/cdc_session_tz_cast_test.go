// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
)

// requireSessionTZRefusal asserts the loud shape: the refusal names the
// column, the pair, the MECHANISM (the executing session's time_zone —
// which is the part an operator needs in order to know their exposure) and
// the drained-model remedy.
func requireSessionTZRefusal(t *testing.T, err error, col string) {
	t.Helper()
	if err == nil {
		t.Fatal("TIMESTAMP⇄DATETIME MODIFY was forwarded; want the session-time_zone cast refusal (a forwarded MODIFY re-casts every pre-existing target row against a different session zone)")
	}
	for _, want := range []string{
		"cannot be forwarded", `column "` + col + `"`,
		"TIMESTAMP and DATETIME", "time_zone", "drained model", "sync stop --wait",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("session-time_zone refusal missing %q; got: %v", want, err)
		}
	}
}

// TestSessionTZSwapPair_EveryZoneSiblingPair derives the pair universe
// from the PROJECTIONS — every MySQL temporal `data_type` translateType
// resolves, classified by (family, carries-a-zone) — and asserts
// sessionTZSwapPair matches exactly the pairs that disagree on the zone.
// The derivation is independent of the predicate under test, so a pair the
// predicate forgot still shows up here; deriving from the predicate would
// make the gate self-referential.
//
// Scope: the MySQL lane's declaration only. The PG lane's equivalent is
// postgres.TestSessionTZSwapGate_EveryZoneSiblingPair; the cross-lane
// roster that fails a lane with no refusal path at all is
// docsync.TestSessionGUCCastRoster_EveryCDCLane.
func TestSessionTZSwapPair_EveryZoneSiblingPair(t *testing.T) {
	prec := func(p int64) *int64 { return &p }

	// MySQL's temporal universe, listed from the manual's Date and Time
	// Types chapter rather than derived from translateType, so a type that
	// stopped resolving fails here instead of silently shrinking the
	// universe. YEAR is included deliberately: it is temporal and it is NOT
	// part of this class, which is a fact the roster should assert rather
	// than assume.
	universe := map[string]columnMeta{
		"date":        {DataType: "date", ColumnType: "date"},
		"time":        {DataType: "time", ColumnType: "time(3)", DTPrec: prec(3)},
		"datetime":    {DataType: "datetime", ColumnType: "datetime(3)", DTPrec: prec(3)},
		"timestamp":   {DataType: "timestamp", ColumnType: "timestamp(3)", DTPrec: prec(3)},
		"datetime(6)": {DataType: "datetime", ColumnType: "datetime(6)", DTPrec: prec(6)},
		"year":        {DataType: "year", ColumnType: "year"},
	}
	if len(universe) < 6 {
		t.Fatalf("temporal universe holds %d entries; floor 6 — the derivation went vacuous", len(universe))
	}

	type zoneClass struct {
		family string
		zoned  bool
		ok     bool
	}
	classify := func(t *testing.T, name string, meta columnMeta) (ir.Type, zoneClass) {
		t.Helper()
		typ, err := translateType(meta)
		if err != nil {
			t.Fatalf("translateType(%s): %v", name, err)
		}
		switch v := typ.(type) {
		case ir.Timestamp:
			return typ, zoneClass{family: "timestamp", zoned: v.WithTimeZone, ok: true}
		case ir.DateTime:
			return typ, zoneClass{family: "timestamp", ok: true}
		case ir.Date:
			return typ, zoneClass{family: "date", ok: true}
		case ir.Time:
			return typ, zoneClass{family: "time-of-day", zoned: v.WithTimeZone, ok: true}
		}
		// YEAR projects outside the four temporal Go types; it carries no
		// zone-sibling and therefore no session-cast hazard.
		return typ, zoneClass{}
	}

	types := map[string]ir.Type{}
	classes := map[string]zoneClass{}
	for name, meta := range universe {
		typ, class := classify(t, name, meta)
		types[name] = typ
		classes[name] = class
	}

	wantSwap := map[[2]string]bool{}
	for a := range universe {
		for b := range universe {
			if a == b {
				continue
			}
			ca, cb := classes[a], classes[b]
			if ca.ok && cb.ok && ca.family == cb.family && ca.zoned != cb.zoned {
				wantSwap[[2]string{a, b}] = true
			}
		}
	}
	// Anti-vacuity floor: timestamp⇄datetime and timestamp⇄datetime(6),
	// each direction. If MySQL ever grows a second zone-aware temporal this
	// count changes and the declaration must be revisited in the same edit.
	if len(wantSwap) != 4 {
		t.Fatalf("derived %d ordered zone-sibling pairs; want 4 (timestamp⇄datetime at two precisions)", len(wantSwap))
	}

	for a := range universe {
		for b := range universe {
			if a == b {
				continue
			}
			pair, matched := sessionTZSwapPair(types[a], types[b])
			want := wantSwap[[2]string{a, b}]
			switch {
			case want && !matched:
				t.Errorf("%s→%s is a zone-sibling swap but sessionTZSwapPair does not match it — MySQL resolves that MODIFY against the EXECUTING session's time_zone, so a forwarded ALTER re-casts every pre-existing target row", a, b)
			case !want && matched:
				t.Errorf("%s→%s is NOT a zone-sibling swap but sessionTZSwapPair matched it as %q — the predicate is broader than the class and would false-refuse a forwardable MODIFY", a, b, pair)
			case want && pair == "":
				t.Errorf("%s→%s matched with an empty pair name; the refusal text names the pair", a, b)
			}
		}
	}
}

// TestSessionTZSwapPair_PrecisionOnlyIsNotASwap is the no-over-refusal
// floor named explicitly (it is also covered by the roster above, but this
// is the cell an operator would be hurt by): DATETIME(3)→DATETIME(6) and
// TIMESTAMP(3)→TIMESTAMP(6) carry no zone conversion and must keep
// forwarding, in both widening and narrowing directions.
func TestSessionTZSwapPair_PrecisionOnlyIsNotASwap(t *testing.T) {
	for _, tc := range []struct {
		name     string
		from, to ir.Type
	}{
		{"datetime(3)→datetime(6)", ir.DateTime{Precision: 3}, ir.DateTime{Precision: 6}},
		{"datetime(6)→datetime(3)", ir.DateTime{Precision: 6}, ir.DateTime{Precision: 3}},
		{"timestamp(3)→timestamp(6)", ir.Timestamp{Precision: 3, WithTimeZone: true}, ir.Timestamp{Precision: 6, WithTimeZone: true}},
		{"timestamp(6)→timestamp(3)", ir.Timestamp{Precision: 6, WithTimeZone: true}, ir.Timestamp{Precision: 3, WithTimeZone: true}},
	} {
		if pair, matched := sessionTZSwapPair(tc.from, tc.to); matched {
			t.Errorf("%s matched as %q; a precision-only MODIFY must keep forwarding", tc.name, pair)
		}
	}
}

// tsCol / dtCol build the two sides of the swap as the binlog reader's
// information_schema projection produces them.
func tsCol(name string, precision int) *ir.Column {
	return &ir.Column{Name: name, Type: ir.Timestamp{Precision: precision, WithTimeZone: true}}
}

func dtCol(name string, precision int) *ir.Column {
	return &ir.Column{Name: name, Type: ir.DateTime{Precision: precision}}
}

// TestB1_MaybeSnapshot_SessionTZCastRefuses is the behavioural half on the
// BINLOG lane: with a forward path live, a TIMESTAMP⇄DATETIME MODIFY
// refuses at the reader — before the boundary is emitted, so neither the
// ADR-0091 intercept nor Shape A's boundary router can act on it.
func TestB1_MaybeSnapshot_SessionTZCastRefuses(t *testing.T) {
	anchor := ir.Position{Engine: engineNameMySQL, Token: "ddl-anchor"}
	newReader := func(armed bool) *CDCReader {
		return &CDCReader{
			schema:                     "app",
			snapshotSig:                map[string]ir.SchemaSignature{},
			pendingDDLActive:           true,
			pendingDDLAnchor:           anchor,
			schemaDeltaAppliesToTarget: armed,
		}
	}
	events := func(t *testing.T, r *CDCReader, v1, v2 *tableSchema) error {
		t.Helper()
		out := make(chan ir.Change, 8)
		if err := r.maybeSnapshotSchemaB1(context.Background(), "app.events", v1, out); err != nil {
			t.Fatalf("prime boundary: %v", err)
		}
		return r.maybeSnapshotSchemaB1(context.Background(), "app.events", v2, out)
	}

	for _, tc := range []struct {
		name   string
		v1, v2 []*ir.Column
	}{
		{"TIMESTAMP→DATETIME", []*ir.Column{tsCol("created_at", 0)}, []*ir.Column{dtCol("created_at", 0)}},
		{"DATETIME→TIMESTAMP", []*ir.Column{dtCol("created_at", 0)}, []*ir.Column{tsCol("created_at", 0)}},
		{"TIMESTAMP(3)→DATETIME(6)", []*ir.Column{tsCol("created_at", 3)}, []*ir.Column{dtCol("created_at", 6)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v1 := &tableSchema{Schema: "app", Name: "events", Columns: tc.v1}
			v2 := &tableSchema{Schema: "app", Name: "events", Columns: tc.v2}
			requireSessionTZRefusal(t, events(t, newReader(true), v1, v2), "created_at")

			// Not armed (--schema-changes=refuse, no Shape A): nothing
			// re-applies the delta, so there is nothing to diverge and the
			// boundary must still emit for the ADR-0049 history write.
			if err := events(t, newReader(false), v1, v2); err != nil {
				t.Errorf("un-armed reader must not refuse (no forward path re-applies the delta); got: %v", err)
			}
		})
	}

	t.Run("precision-only MODIFY still forwards", func(t *testing.T) {
		v1 := &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{dtCol("created_at", 3)}}
		v2 := &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{dtCol("created_at", 6)}}
		if err := events(t, newReader(true), v1, v2); err != nil {
			t.Errorf("DATETIME(3)→DATETIME(6) must forward; got: %v", err)
		}
	})

	t.Run("identical re-send emits nothing and refuses nothing", func(t *testing.T) {
		r := newReader(true)
		v := &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{tsCol("created_at", 0)}}
		out := make(chan ir.Change, 8)
		for i := range 3 {
			if err := r.maybeSnapshotSchemaB1(context.Background(), "app.events", v, out); err != nil {
				t.Fatalf("re-send %d: %v", i, err)
			}
		}
		close(out)
		n := 0
		for c := range out {
			if _, ok := c.(ir.SchemaSnapshot); ok {
				n++
			}
		}
		if n != 1 {
			t.Errorf("identical re-sends produced %d boundaries, want 1 (the initial)", n)
		}
	})

	// SLM-1 (audit 2026-09-01) — the FIRST boundary. The pre-SLM-1 cell
	// here pinned "the first boundary cannot refuse (no prior type)" on the
	// premise that nothing forwards from it; Shape A's router forwards
	// exactly that boundary (its cache is seeded from the cold-start
	// handoff), and it was observed forwarding a 9 h re-cast at exit 0.
	// The expectation is FLIPPED: a table the streamer seeded, or one the
	// reader decoded before the DDL, refuses at its first boundary.
	first := func(t *testing.T, r *CDCReader, v *tableSchema) error {
		t.Helper()
		out := make(chan ir.Change, 8)
		return r.maybeSnapshotSchemaB1(context.Background(), "app.events", v, out)
	}
	seedTable := func(cols ...*ir.Column) *ir.Table {
		return &ir.Table{Name: "events", Columns: cols}
	}

	for _, tc := range []struct {
		name string
		seed []*ir.Column
		v1   []*ir.Column
	}{
		{"TIMESTAMP seed → DATETIME first boundary", []*ir.Column{tsCol("created_at", 0)}, []*ir.Column{dtCol("created_at", 0)}},
		{"DATETIME seed → TIMESTAMP first boundary", []*ir.Column{dtCol("created_at", 0)}, []*ir.Column{tsCol("created_at", 0)}},
		{"TIMESTAMP(3) seed → DATETIME(6) first boundary", []*ir.Column{tsCol("created_at", 3)}, []*ir.Column{dtCol("created_at", 6)}},
	} {
		t.Run("the FIRST boundary for a SEEDED table refuses: "+tc.name, func(t *testing.T) {
			r := newReader(true)
			// The seed carries no Schema, as the MySQL SchemaReader's IR
			// does; the reader keys it under its own bound database.
			r.SetSchemaSeed([]*ir.Table{seedTable(tc.seed...)})
			v := &tableSchema{Schema: "app", Name: "events", Columns: tc.v1}
			requireSessionTZRefusal(t, first(t, r, v), "created_at")

			// Un-armed: the seed informs a refusal that is not armed, so
			// nothing changes for --schema-changes=refuse without Shape A.
			r = newReader(false)
			r.SetSchemaSeed([]*ir.Table{seedTable(tc.seed...)})
			if err := first(t, r, v); err != nil {
				t.Errorf("un-armed seeded reader must not refuse; got: %v", err)
			}
		})
	}

	t.Run("a table DECODED before the DDL refuses at its first boundary without a seed", func(t *testing.T) {
		// The decode cache held the pre-DDL shape; the generic-DDL arm
		// retains it (retainPriorShapes) before the blanket clear.
		r := newReader(true)
		r.schemaCache = map[string]*tableSchema{
			"app.events": {Schema: "app", Name: "events", Columns: []*ir.Column{tsCol("created_at", 0)}},
		}
		r.retainPriorShapes()
		clear(r.schemaCache)
		v := &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{dtCol("created_at", 0)}}
		requireSessionTZRefusal(t, first(t, r, v), "created_at")
	})

	t.Run("the seed does not suppress the first history boundary", func(t *testing.T) {
		// The seed is the refusal's prev, NOT the ADR-0049 true-delta memo:
		// a first boundary whose shape equals the seed still emits, so the
		// schema-history contract is byte-for-byte what it was.
		r := newReader(true)
		r.SetSchemaSeed([]*ir.Table{seedTable(tsCol("created_at", 0))})
		v := &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{tsCol("created_at", 0)}}
		out := make(chan ir.Change, 8)
		if err := r.maybeSnapshotSchemaB1(context.Background(), "app.events", v, out); err != nil {
			t.Fatalf("seed-equal first boundary refused: %v", err)
		}
		close(out)
		n := 0
		for c := range out {
			if _, ok := c.(ir.SchemaSnapshot); ok {
				n++
			}
		}
		if n != 1 {
			t.Errorf("seed-equal first boundary emitted %d snapshots; want 1 — the seed must not be mistaken for an emitted version", n)
		}
	})

	t.Run("a table with NO prior at all still cannot refuse — the stated residual", func(t *testing.T) {
		// Never seeded (not in the cold-start scope, no retained version),
		// never decoded before the DDL: there is no prev type to compare
		// against, and inventing one would be a guess. This is the honest
		// remaining window, and the pipeline intercepts treat such a
		// boundary as a cache prime rather than an ALTER.
		r := newReader(true)
		v := &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{dtCol("created_at", 0)}}
		if err := first(t, r, v); err != nil {
			t.Errorf("first boundary with no prior must not refuse; got: %v", err)
		}
	})
}

// TestSetSchemaSeed_KeysLikeTheReaderCache pins the key alignment the
// seed depends on: a seed table without a Schema lands under the
// reader's bound database, exactly where the TABLE_MAP-derived cache key
// will look for it; a qualified one keeps its database.
func TestSetSchemaSeed_KeysLikeTheReaderCache(t *testing.T) {
	r := &CDCReader{schema: "app"}
	r.SetSchemaSeed([]*ir.Table{
		{Name: "bare", Columns: []*ir.Column{tsCol("c", 0)}},
		{Schema: "other", Name: "qualified", Columns: []*ir.Column{dtCol("c", 0)}},
		nil,
	})
	for _, want := range []string{"app.bare", "other.qualified"} {
		if _, ok := r.priorSig[want]; !ok {
			t.Errorf("seed key %q missing; keys = %v", want, r.priorSig)
		}
	}
	if len(r.priorSig) != 2 {
		t.Errorf("seeded %d keys; want 2 (nil entries skipped)", len(r.priorSig))
	}
}

// tzFieldEvent builds a FIELD event for the shared "users" table with one
// temporal column, in the ColumnType form VStream delivers.
func tzFieldEvent(columnType string) *binlogdata.VEvent {
	protoType := query.Type_DATETIME
	if strings.HasPrefix(columnType, "timestamp") {
		protoType = query.Type_TIMESTAMP
	}
	return fieldEvent([]*query.Field{
		{Name: "id", Type: query.Type_INT64, ColumnType: "bigint"},
		{Name: "created_at", Type: protoType, ColumnType: columnType},
	})
}

// TestVStreamSchemaHistory_SessionTZCastRefuses is the SIBLING pin on the
// VStream standalone lane. The MySQL engine has THREE ir.SchemaSnapshot
// emitters and a refusal wired into one of them is the sibling-miss shape
// this project keeps paying for, so each is pinned separately rather than
// by a shared helper the production code does not share.
func TestVStreamSchemaHistory_SessionTZCastRefuses(t *testing.T) {
	run := func(t *testing.T, armed bool, from, to string) error {
		t.Helper()
		r := newVStreamTestReader()
		r.schemaDeltaAppliesToTarget = armed
		out := make(chan ir.Change, 16)
		ctx := context.Background()
		for _, ev := range []*binlogdata.VEvent{vgtidEvent("gtid-1"), tzFieldEvent(from), vgtidEvent("gtid-2")} {
			if err := r.dispatch(ctx, ev, out); err != nil {
				t.Fatalf("prime: %v", err)
			}
		}
		return r.dispatch(ctx, tzFieldEvent(to), out)
	}

	for _, tc := range []struct{ name, from, to string }{
		{"timestamp→datetime", "timestamp", "datetime"},
		{"datetime→timestamp", "datetime", "timestamp"},
		{"timestamp(3)→datetime(6)", "timestamp(3)", "datetime(6)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireSessionTZRefusal(t, run(t, true, tc.from, tc.to), "created_at")
			if err := run(t, false, tc.from, tc.to); err != nil {
				t.Errorf("un-armed VStream reader must not refuse; got: %v", err)
			}
		})
	}

	t.Run("precision-only MODIFY still forwards", func(t *testing.T) {
		if err := run(t, true, "datetime(3)", "datetime(6)"); err != nil {
			t.Errorf("datetime(3)→datetime(6) must forward on the VStream lane; got: %v", err)
		}
	})

	// SLM-1: the FIRST FIELD of this process, with the prior coming from
	// the streamer's seed rather than an earlier FIELD.
	seeded := func(t *testing.T, seedType, first string) (error, int) {
		t.Helper()
		r := newVStreamTestReader()
		r.schemaDeltaAppliesToTarget = true
		r.SetSchemaSeed([]*ir.Table{{Name: "users", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "created_at", Type: ir.Timestamp{WithTimeZone: seedType == "timestamp"}},
		}}})
		out := make(chan ir.Change, 16)
		ctx := context.Background()
		if err := r.dispatch(ctx, vgtidEvent("gtid-1"), out); err != nil {
			t.Fatalf("vgtid: %v", err)
		}
		err := r.dispatch(ctx, tzFieldEvent(first), out)
		close(out)
		n := 0
		for c := range out {
			if _, ok := c.(ir.SchemaSnapshot); ok {
				n++
			}
		}
		return err, n
	}
	t.Run("SEEDED: the first FIELD refuses a zone-sibling swap", func(t *testing.T) {
		err, _ := seeded(t, "timestamp", "datetime")
		requireSessionTZRefusal(t, err, "created_at")
	})
	t.Run("SEEDED: a seed-equal first FIELD still emits its history version", func(t *testing.T) {
		err, n := seeded(t, "timestamp", "timestamp")
		if err != nil {
			t.Fatalf("seed-equal first FIELD refused: %v", err)
		}
		if n != 1 {
			t.Errorf("seed-equal first FIELD emitted %d snapshots; want 1 — the seed must not stand in for an emitted version", n)
		}
	})
}

// TestVStreamSnapshotCDC_SessionTZCastRefuses is the SIBLING pin on the
// cold-start snapshot-stream's CDC phase — the third emitter, and the one
// the F7c boundary fix had to be retrofitted into, which is exactly why it
// gets its own cell rather than riding the standalone reader's.
func TestVStreamSnapshotCDC_SessionTZCastRefuses(t *testing.T) {
	run := func(t *testing.T, armed bool, from, to string) error {
		t.Helper()
		s := newVStreamSnapshotTestStream()
		s.schemaDeltaAppliesToTarget = armed
		out := make(chan ir.Change, 16)
		ctx := context.Background()
		for _, ev := range []*binlogdata.VEvent{vgtidEvent("gtid-pre"), tzFieldEvent(from), vgtidEvent("gtid-post")} {
			if err := s.dispatchCDCEvent(ctx, ev, out); err != nil {
				t.Fatalf("prime: %v", err)
			}
		}
		return s.dispatchCDCEvent(ctx, tzFieldEvent(to), out)
	}

	for _, tc := range []struct{ name, from, to string }{
		{"timestamp→datetime", "timestamp", "datetime"},
		{"datetime→timestamp", "datetime", "timestamp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireSessionTZRefusal(t, run(t, true, tc.from, tc.to), "created_at")
			if err := run(t, false, tc.from, tc.to); err != nil {
				t.Errorf("un-armed snapshot stream must not refuse; got: %v", err)
			}
		})
	}

	t.Run("precision-only MODIFY still forwards", func(t *testing.T) {
		if err := run(t, true, "datetime(3)", "datetime(6)"); err != nil {
			t.Errorf("datetime(3)→datetime(6) must forward on the snapshot-stream lane; got: %v", err)
		}
	})

	// SLM-1: the first post-COPY FIELD of this process, prior from the
	// streamer's seed (handed through the snapshot stream's CDC half).
	seeded := func(t *testing.T, seedType, first string) (error, int) {
		t.Helper()
		s := newVStreamSnapshotTestStream()
		s.schemaDeltaAppliesToTarget = true
		(&vstreamSnapshotChanges{snap: s}).SetSchemaSeed([]*ir.Table{{Name: "users", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "created_at", Type: ir.Timestamp{WithTimeZone: seedType == "timestamp"}},
		}}})
		out := make(chan ir.Change, 16)
		ctx := context.Background()
		if err := s.dispatchCDCEvent(ctx, vgtidEvent("gtid-pre"), out); err != nil {
			t.Fatalf("vgtid: %v", err)
		}
		err := s.dispatchCDCEvent(ctx, tzFieldEvent(first), out)
		close(out)
		n := 0
		for c := range out {
			if _, ok := c.(ir.SchemaSnapshot); ok {
				n++
			}
		}
		return err, n
	}
	t.Run("SEEDED: the first FIELD refuses a zone-sibling swap", func(t *testing.T) {
		err, _ := seeded(t, "timestamp", "datetime")
		requireSessionTZRefusal(t, err, "created_at")
	})
	t.Run("SEEDED: a seed-equal first FIELD still emits its history version", func(t *testing.T) {
		err, n := seeded(t, "timestamp", "timestamp")
		if err != nil {
			t.Fatalf("seed-equal first FIELD refused: %v", err)
		}
		if n != 1 {
			t.Errorf("seed-equal first FIELD emitted %d snapshots; want 1", n)
		}
	})
}
