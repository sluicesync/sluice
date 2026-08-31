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

	t.Run("the FIRST boundary for a table cannot refuse (no prior type)", func(t *testing.T) {
		// Honest scope note, pinned: with no prior snapshot there is no
		// prev type to have swapped away from. That is also the boundary
		// the pipeline intercept treats as a cache prime (warm resume) or
		// a seed-guarded no-op (cold start), so nothing forwards from it.
		r := newReader(true)
		v := &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{dtCol("created_at", 0)}}
		out := make(chan ir.Change, 8)
		if err := r.maybeSnapshotSchemaB1(context.Background(), "app.events", v, out); err != nil {
			t.Errorf("first boundary must not refuse; got: %v", err)
		}
	})
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
}
