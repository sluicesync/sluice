// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTargetEmitShape_MySQLTemporalMatrix walks the temporal family ×
// shape matrix the Bug 74 lesson asks for rather than one representative:
// every temporal IR type a source can produce, × {bare, explicit-0,
// explicit-mid, explicit-6} × {zoned, unzoned} where the type carries a
// zone flag. The two axes are independent — a `TIME(3) WITH TIME ZONE`
// needs no precision fix and still has to lose the flag — so a matrix
// that varied only one would have passed while the other stayed broken.
//
// `want == nil` means "no rewrite": the expected side already names what
// the MySQL catalog reads back.
func TestTargetEmitShape_MySQLTemporalMatrix(t *testing.T) {
	rule := targetEmitShapeRuleFor("mysql")
	if rule == nil {
		t.Fatal("no target-emit-shape rule for a mysql target")
	}
	for _, tc := range []struct {
		name string
		in   ir.Type
		want ir.Type
	}{
		// ir.Time — PG `time`, the filed Bug 237(a) witness.
		{"time bare", ir.Time{PrecisionUnspecified: true}, ir.Time{Precision: 6}},
		{"time(0)", ir.Time{Precision: 0}, nil},
		{"time(3)", ir.Time{Precision: 3}, nil},
		{"time(6)", ir.Time{Precision: 6}, nil},
		// ir.Time + zone — PG `timetz`. MySQL has no timetz at all, so
		// the flag must go at EVERY precision, not only the bare one.
		{"timetz bare", ir.Time{PrecisionUnspecified: true, WithTimeZone: true}, ir.Time{Precision: 6}},
		{"timetz(0)", ir.Time{Precision: 0, WithTimeZone: true}, ir.Time{Precision: 0}},
		{"timetz(3)", ir.Time{Precision: 3, WithTimeZone: true}, ir.Time{Precision: 3}},
		{"timetz(6)", ir.Time{Precision: 6, WithTimeZone: true}, ir.Time{Precision: 6}},

		// ir.DateTime — PG `timestamp without time zone`.
		{"datetime bare", ir.DateTime{PrecisionUnspecified: true}, ir.DateTime{Precision: 6}},
		{"datetime(0)", ir.DateTime{Precision: 0}, nil},
		{"datetime(3)", ir.DateTime{Precision: 3}, nil},
		{"datetime(6)", ir.DateTime{Precision: 6}, nil},

		// ir.Timestamp — PG `timestamptz`. The zone flag SURVIVES here
		// (MySQL's TIMESTAMP converts to UTC on store and back on
		// retrieval, and its reader reports the column as zoned), which
		// is the opposite of the ir.Time arm and the reason both are
		// enumerated rather than sharing a case.
		{"timestamptz bare", ir.Timestamp{PrecisionUnspecified: true, WithTimeZone: true}, ir.Timestamp{Precision: 6, WithTimeZone: true}},
		{"timestamptz(3)", ir.Timestamp{Precision: 3, WithTimeZone: true}, nil},
		{"timestamp bare unzoned", ir.Timestamp{PrecisionUnspecified: true}, ir.Timestamp{Precision: 6}},

		// Non-temporal and precision-free temporal controls: a rule that
		// fired here would rewrite columns it has no business touching.
		{"date", ir.Date{}, nil},
		{"integer", ir.Integer{Width: 32}, nil},
		{"text", ir.Text{Size: ir.TextLong}, nil},
		{"interval", ir.Interval{}, nil},

		// A DOMAIN over a bare temporal is graded on its STORAGE, the
		// same reading MySQL's emitColumnType takes (its ir.Domain arm
		// recurses into the base type).
		{
			"domain over bare timestamp",
			ir.Domain{Name: "d_ts", BaseType: ir.DateTime{PrecisionUnspecified: true}},
			ir.DateTime{Precision: 6},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := rule(&ir.Column{Name: "c", Type: tc.in})
			switch {
			case tc.want == nil && got != nil:
				t.Fatalf("rewrote %s to %s; want NO rewrite — the expected side already names what a MySQL "+
					"catalog reads back, so a rewrite here can only invent drift", tc.in, got)
			case tc.want != nil && got == nil:
				t.Fatalf("no rewrite for %s; want %s. Un-rewritten, `schema diff` reports phantom drift on this "+
					"column of every target migrate created AND the ADR-0166 pre-create gate REFUSES a re-run",
					tc.in, tc.want)
			case tc.want != nil && got.String() != tc.want.String():
				t.Fatalf("rewrote %s to %s; want %s", tc.in, got, tc.want)
			}
		})
	}
}

// TestTargetEmitShape_PostgresGeneratedEnum pins the arm that CANNOT be
// written as a type rule: the same ir.Enum rewrites or not depending on
// whether the COLUMN is generated.
func TestTargetEmitShape_PostgresGeneratedEnum(t *testing.T) {
	rule := targetEmitShapeRuleFor("postgres")
	if rule == nil {
		t.Fatal("no target-emit-shape rule for a postgres target")
	}
	enum := ir.Enum{TypeName: "mood", Values: []string{"happy", "sad"}}
	for _, tc := range []struct {
		name string
		col  *ir.Column
		want ir.Type
	}{
		{
			"generated enum lands as TEXT (Bug 25)",
			&ir.Column{Name: "g", Type: enum, GeneratedExpr: "mood", GeneratedStored: true},
			ir.Text{Size: ir.TextLong},
		},
		{
			"plain enum keeps the native type",
			&ir.Column{Name: "m", Type: enum},
			nil,
		},
		{
			"generated non-enum is untouched",
			&ir.Column{Name: "n", Type: ir.Integer{Width: 32}, GeneratedExpr: "id + 1", GeneratedStored: true},
			nil,
		},
		{
			// The declared-type mirror, registered in
			// translateDomainDispatchExemptions: the PG emitter's enum
			// branch reads c.Type, so a domain-wrapped enum does not take
			// it and must not be predicted as TEXT.
			"generated DOMAIN over an enum keeps the domain",
			&ir.Column{
				Name:            "d",
				Type:            ir.Domain{Name: "d_mood", BaseType: enum},
				GeneratedExpr:   "mood",
				GeneratedStored: true,
			},
			nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := rule(tc.col)
			switch {
			case tc.want == nil && got != nil:
				t.Fatalf("rewrote to %s; want NO rewrite", got)
			case tc.want != nil && got == nil:
				t.Fatalf("no rewrite; want %s", tc.want)
			case tc.want != nil && got.String() != tc.want.String():
				t.Fatalf("rewrote to %s; want %s", got, tc.want)
			}
		})
	}
}

// TestTargetEmitShapeRuleFor_TargetRoster states which targets have an
// emitter-effect rule and which deliberately do not, so a reader cannot
// mistake "sqlite is absent" for "sqlite was forgotten".
func TestTargetEmitShapeRuleFor_TargetRoster(t *testing.T) {
	for _, tc := range []struct {
		engine string
		want   bool
	}{
		{"mysql", true},
		{"planetscale", true},
		{"vitess", true},
		{"mariadb", true},
		{"postgres", true},
		{"postgres-trigger", true},
		// SQLite stores temporals as TEXT and declares no precision, so a
		// precision-unspecified source reads back unspecified and there
		// is nothing to materialize; it has no enum type to degrade
		// either. Absent ON PURPOSE.
		{"sqlite", false},
		{"d1", false},
		{"some-future-engine", false},
	} {
		if got := targetEmitShapeRuleFor(tc.engine) != nil; got != tc.want {
			t.Errorf("targetEmitShapeRuleFor(%q) present = %v; want %v", tc.engine, got, tc.want)
		}
	}
}

// TestTargetEmitShape_IsCompareLaneOnly is the half that keeps this pass
// from leaking, and it is asserted at the ENTRY POINTS rather than by
// reading the code: [RetargetForEngine] hands its result to a
// SchemaWriter and to RowWriters that consult
// [ir.Column.SourceColumnType], so a rewrite landing there would stamp a
// provenance on every bare temporal and every generated enum column.
//
// Both directions: the emit lane must NOT rewrite, and the compare lane
// MUST — a test that only asserted the first would pass with the feature
// deleted.
func TestTargetEmitShape_IsCompareLaneOnly(t *testing.T) {
	schema := func() *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{
			Name: "t",
			Columns: []*ir.Column{
				{Name: "ts", Type: ir.DateTime{PrecisionUnspecified: true}},
			},
		}}}
	}

	emit := RetargetForEngine(schema(), "postgres", "mysql")
	emitCol := emit.Tables[0].Columns[0]
	if got := emitCol.Type.String(); got != "DateTime(unspecified)" {
		t.Errorf("EMIT lane rewrote the bare temporal to %s. The MySQL emitter already materializes 6 from the "+
			"unspecified form, so this buys nothing — and it newly stamps SourceColumnType on every bare "+
			"temporal column for restore/chain-restore's RowWriters", got)
	}
	if emitCol.SourceColumnType != nil {
		t.Errorf("EMIT lane stamped SourceColumnType = %s on a column it should not have touched",
			emitCol.SourceColumnType)
	}

	cmp := RetargetForShapeCompare(schema(), "postgres", "mysql")
	if got := cmp.Tables[0].Columns[0].Type.String(); got != "DateTime(6)" {
		t.Errorf("COMPARE lane left the bare temporal as %s; want DateTime(6) — this is the Bug 237(a) fix and "+
			"without it `schema diff` reports phantom drift and `migrate` refuses a re-run", got)
	}
}

// TestTargetEmitShape_PostgresToPostgresReachesTheGeneratedEnum is the
// reason this pass is keyed on the TARGET rather than on the pair: a
// same-engine postgres→postgres comparison has NO rule table (and never
// will — the pair is identity by construction), and the Bug-25 rewrite
// still fires on it, because the PG emitter applies it to whatever IR it
// is handed.
func TestTargetEmitShape_PostgresToPostgresReachesTheGeneratedEnum(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{{
			Name:            "g",
			Type:            ir.Enum{TypeName: "mood", Values: []string{"happy", "sad"}},
			GeneratedExpr:   "mood",
			GeneratedStored: true,
		}},
	}}}
	got := RetargetForShapeCompare(s, "postgres", "postgres").Tables[0].Columns[0].Type.String()
	if want := "Text[long]"; got != want {
		t.Fatalf("postgres→postgres generated enum compares as %s; want %s. An arm in a pair rule table could "+
			"not have reached this pair at all", got, want)
	}
}
