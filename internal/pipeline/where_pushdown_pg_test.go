// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/rowpredicate"
)

// TestPGPushdownEligible_EnvelopePin is the ADR-0176 change-detector: it
// pins the classifier's proven-envelope FAMILY LIST over every ir value
// type, so widening the envelope (adding a type or collation to the
// eligible set) FAILS here and forces the widening to land together with
// its real-PG oracle cells (TestPublicationScope_PushdownOracle) — the
// Bug-74 "pin the class" discipline applied to the classifier itself.
// Narrowing fails too: a silently-shrunk envelope would quietly demote
// proven push-down cells back to client-side-only.
func TestPGPushdownEligible_EnvelopePin(t *testing.T) {
	cases := []struct {
		name string
		typ  ir.Type
		want bool
	}{
		// ---- The proven envelope (each family has oracle cells) ----
		{"integer", ir.Integer{Width: 64}, true},
		{"decimal", ir.Decimal{Precision: 12, Scale: 4}, true},
		{"boolean", ir.Boolean{}, true},
		{"varchar default collation", ir.Varchar{Length: 32}, true},
		{"text default collation", ir.Text{}, true},
		{"varchar COLLATE C", ir.Varchar{Length: 32, Collation: "C"}, true},
		{"text COLLATE C", ir.Text{Collation: "C"}, true},
		{"date", ir.Date{}, true},
		{"datetime (PG's `timestamp without time zone` — the oracle's timestamp cells)", ir.DateTime{}, true},
		{"timestamp without tz (cross-engine spelling of the same naive family)", ir.Timestamp{}, true},

		// ---- Outside the envelope: falls back to client-side-only ----
		{"timestamp WITH tz (fail closed; Compile refuses anyway)", ir.Timestamp{WithTimeZone: true}, false},
		{"text POSIX collation (byte-identical to C but not oracle-proven)", ir.Text{Collation: "POSIX"}, false},
		{"text named deterministic collation", ir.Text{Collation: "en_US.utf8"}, false},
		{"char/bpchar (PAD SPACE equality)", ir.Char{Length: 4}, false},
		{"enum", ir.Enum{Values: []string{"a", "b"}}, false},
		{"uuid", ir.UUID{}, false},
		{"inet", ir.Inet{}, false},
		{"cidr", ir.Cidr{}, false},
		{"macaddr", ir.Macaddr{}, false},
		{"time-of-day", ir.Time{}, false},
		{"float (IEEE-754 literal-coercion class)", ir.Float{}, false},
		{"binary", ir.Binary{Length: 4}, false},
		{"varbinary", ir.Varbinary{Length: 4}, false},
		{"blob (bytea escape-literal divergence class)", ir.Blob{}, false},
		{"json", ir.JSON{}, false},
		{"domain", ir.Domain{Name: "email"}, false},
		{"set", ir.Set{Values: []string{"a"}}, false},
		{"array", ir.Array{Element: ir.Integer{Width: 32}}, false},
		{"geometry", ir.Geometry{}, false},
		{"extension type", ir.ExtensionType{Name: "citext"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := pgPushdownEligibleColumn(&ir.Column{Name: "v", Type: tc.typ})
			if got != tc.want {
				t.Errorf("pgPushdownEligibleColumn(%T) = %v, want %v (reason %q)", tc.typ, got, tc.want, reason)
			}
			if !tc.want && reason == "" {
				t.Errorf("ineligible %T must carry a fallback-log reason", tc.typ)
			}
		})
	}
}

// TestPGPushdownEligible_Predicate pins the term-level exclusions and the
// whole-predicate composition: one ineligible term poisons the table (the
// classifier fails CLOSED), and the bool-vs-0/1 idiom — valid in the
// client grammar, invalid Postgres SQL — never reaches the publication.
func TestPGPushdownEligible_Predicate(t *testing.T) {
	tbl := &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "flag", Type: ir.Boolean{}},
			{Name: "tag", Type: ir.UUID{}},
			{Name: "d", Type: ir.Date{}},
			{Name: "ts", Type: ir.DateTime{}},
		},
	}
	infos := map[string]rowpredicate.ColumnInfo{
		"id":   {Family: rowpredicate.FamilyNumeric},
		"flag": {Family: rowpredicate.FamilyBool},
		"tag":  {Family: rowpredicate.FamilyString, Faithful: true},
		"d":    {Family: rowpredicate.FamilyTemporal},
		"ts":   {Family: rowpredicate.FamilyTemporal},
	}
	compile := func(pred string) *rowpredicate.Predicate {
		p, err := rowpredicate.Compile("orders", pred, infos)
		if err != nil {
			t.Fatalf("Compile(%q): %v", pred, err)
		}
		return p
	}

	if ok, _ := pgPushdownEligible(tbl, compile("id < 100 AND flag = TRUE")); !ok {
		t.Error("all-eligible predicate classified ineligible")
	}
	if ok, reason := pgPushdownEligible(tbl, compile("flag = 1")); ok {
		t.Error("bool = 1 must be ineligible (boolean = integer is not valid PG SQL)")
	} else if !strings.Contains(reason, "flag") {
		t.Errorf("reason must name the column; got %q", reason)
	}
	if ok, _ := pgPushdownEligible(tbl, compile("id < 100 AND tag = 'x'")); ok {
		t.Error("one ineligible term (uuid) must poison the whole predicate — the classifier fails closed")
	}

	// ---- Temporal literal granularity (audit 2026-07-23 D0-5) ----
	// A DATE column with a time-bearing literal, and any temporal column
	// with a >6-fractional-digit literal, classify OUT: PG truncates/rounds
	// the literal while the client compares at full precision, so pushing
	// the predicate would make server and client provably disagree.
	if ok, _ := pgPushdownEligible(tbl, compile("d = '2026-01-15'")); !ok {
		t.Error("date column with a pure-date literal must stay eligible")
	}
	if ok, reason := pgPushdownEligible(tbl, compile("d < '2026-01-15 12:00:00'")); ok {
		t.Error("date column with a time-bearing literal must be ineligible (PG truncates to the date)")
	} else if !strings.Contains(reason, "d") || !strings.Contains(reason, "time-bearing") {
		t.Errorf("granularity reason must name the column and the truncation; got %q", reason)
	}
	if ok, _ := pgPushdownEligible(tbl, compile("NOT (d >= '2026-01-15 12:00:00')")); ok {
		t.Error("date granularity exclusion must survive NOT composition (the 3VL negation shape)")
	}
	if ok, _ := pgPushdownEligible(tbl, compile("d IN ('2026-01-15', '2026-01-16 08:30')")); ok {
		t.Error("one time-bearing IN member must poison the date term")
	}
	if ok, _ := pgPushdownEligible(tbl, compile("ts = '2026-01-15 08:30:00.123456'")); !ok {
		t.Error("timestamp column with a ≤6-fractional-digit literal must stay eligible")
	}
	if ok, reason := pgPushdownEligible(tbl, compile("ts = '2026-01-15 08:30:00.1234567'")); ok {
		t.Error("timestamp column with a 7-fractional-digit literal must be ineligible (PG rounds to µs, the client keeps ns)")
	} else if !strings.Contains(reason, "ts") || !strings.Contains(reason, "fractional") {
		t.Errorf("sub-microsecond reason must name the column and the rounding; got %q", reason)
	}
	if ok, _ := pgPushdownEligible(tbl, compile("ts < '2026-01-15 12:00:00'")); !ok {
		t.Error("timestamp column with a time-bearing (µs-representable) literal must stay eligible — only DATE truncates it")
	}
	if ok, _ := pgPushdownEligible(tbl, compile("tag IS NULL")); ok {
		t.Error("IS NULL on an outside-envelope column type is still outside the envelope")
	}
	if ok, _ := pgPushdownEligible(tbl, nil); ok {
		t.Error("nil predicate must classify ineligible (fail closed)")
	}
	if ok, _ := pgPushdownEligible(nil, compile("id = 1")); ok {
		t.Error("nil table must classify ineligible (fail closed)")
	}
}
