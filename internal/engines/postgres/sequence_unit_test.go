// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"database/sql"
	"math"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestParseNextvalSequence pins the default-text parser across the
// rendering families PG produces in column_default: bare, schema-
// qualified, double-quoted, and the combinations — plus the shapes
// that must NOT parse (so the caller falls back to the verbatim-
// expression carry, never a silent collapse).
func TestParseNextvalSequence(t *testing.T) {
	cases := []struct {
		in         string
		wantSchema string
		wantName   string
		wantOK     bool
	}{
		{`nextval('order_number_seq'::regclass)`, "", "order_number_seq", true},
		{`nextval('public.order_number_seq'::regclass)`, "public", "order_number_seq", true},
		{`nextval('"Mixed Case"'::regclass)`, "", "Mixed Case", true},
		{`nextval('public."Mixed.Name"'::regclass)`, "public", "Mixed.Name", true},
		{`nextval('"My Schema"."My Seq"'::regclass)`, "My Schema", "My Seq", true},
		{`nextval('"quo""ted"'::regclass)`, "", `quo"ted`, true},
		{`nextval('it''s_a_seq'::regclass)`, "", "it's_a_seq", true},
		// Non-nextval shapes: plain literals, other functions, junk.
		{`'0'::bigint`, "", "", false},
		{`now()`, "", "", false},
		{`nextval(`, "", "", false},
		{`nextval('unterminated`, "", "", false},
		{`NEXTVAL('shouty'::regclass)`, "", "", false}, // PG renders lowercase; anything else is not the catalog shape
	}
	for _, c := range cases {
		gotSchema, gotName, ok := parseNextvalSequence(c.in)
		if ok != c.wantOK || gotSchema != c.wantSchema || gotName != c.wantName {
			t.Errorf("parseNextvalSequence(%q) = (%q, %q, %v); want (%q, %q, %v)",
				c.in, gotSchema, gotName, ok, c.wantSchema, c.wantName, c.wantOK)
		}
	}
}

// TestSerialDefaultOptions pins the collapse gate across the full
// serial data-type family (small/integer/big — the Bug-74 discipline:
// each family has its own MAXVALUE ceiling) and each option that must
// individually disqualify the collapse.
func TestSerialDefaultOptions(t *testing.T) {
	base := func(dataType string, maxValue int64) *ir.Sequence {
		return &ir.Sequence{
			DataType: dataType,
			Start:    1, Increment: 1, MinValue: 1, MaxValue: maxValue, Cache: 1,
		}
	}
	// Every serial family collapses on its factory shape.
	for _, c := range []struct {
		dataType string
		maxValue int64
	}{
		{"smallint", math.MaxInt16},
		{"integer", math.MaxInt32},
		{"bigint", math.MaxInt64},
	} {
		if !serialDefaultOptions(base(c.dataType, c.maxValue)) {
			t.Errorf("serialDefaultOptions(%s factory shape) = false; want true", c.dataType)
		}
		// A family/ceiling mismatch must NOT collapse (e.g. an
		// integer-typed sequence with a hand-set bigint ceiling).
		if serialDefaultOptions(base(c.dataType, c.maxValue-1)) {
			t.Errorf("serialDefaultOptions(%s, custom MAXVALUE) = true; want false", c.dataType)
		}
	}
	// Each non-factory option individually disqualifies.
	mods := map[string]func(*ir.Sequence){
		"start":     func(s *ir.Sequence) { s.Start = 1000 },
		"increment": func(s *ir.Sequence) { s.Increment = 5 },
		"minvalue":  func(s *ir.Sequence) { s.MinValue = 0 },
		"cache":     func(s *ir.Sequence) { s.Cache = 10 },
		"cycle":     func(s *ir.Sequence) { s.Cycle = true },
	}
	for name, mod := range mods {
		s := base("bigint", math.MaxInt64)
		mod(s)
		if serialDefaultOptions(s) {
			t.Errorf("serialDefaultOptions with custom %s = true; want false (must carry standalone)", name)
		}
	}
	// Unknown data type never matches the factory shape (safe side:
	// standalone carry).
	if serialDefaultOptions(base("money", math.MaxInt64)) {
		t.Error("serialDefaultOptions(unknown data type) = true; want false")
	}
}

// TestClassifyAutoIncrement pins the ReadSchema-side classification:
// identity always wins; a nextval() default collapses ONLY for the
// serial-collapsible sequence owned by exactly this column.
func TestClassifyAutoIncrement(t *testing.T) {
	r := &SchemaReader{schema: "public"}
	serial := map[string]pgSequenceOwner{
		"users_id_seq": {table: "users", column: "id"},
	}
	nv := func(s string) sql.NullString { return sql.NullString{Valid: true, String: s} }

	cases := []struct {
		name          string
		isIdentity    string
		columnDefault sql.NullString
		table, column string
		want          bool
	}{
		{"identity column", "YES", sql.NullString{}, "users", "id", true},
		{"serial owner match", "NO", nv(`nextval('users_id_seq'::regclass)`), "users", "id", true},
		{"schema-qualified serial owner match", "NO", nv(`nextval('public.users_id_seq'::regclass)`), "users", "id", true},
		{"standalone sequence (not in serial map)", "NO", nv(`nextval('order_number_seq'::regclass)`), "orders", "order_number", false},
		{"shared: another column borrowing the serial sequence", "NO", nv(`nextval('users_id_seq'::regclass)`), "audit", "user_ref", false},
		{"cross-schema nextval never collapses", "NO", nv(`nextval('other.users_id_seq'::regclass)`), "users", "id", false},
		{"plain literal default", "NO", nv(`'0'::bigint`), "users", "n", false},
		{"no default", "NO", sql.NullString{}, "users", "n", false},
	}
	for _, c := range cases {
		if got := r.classifyAutoIncrement(c.isIdentity, c.columnDefault, c.table, c.column, serial); got != c.want {
			t.Errorf("%s: classifyAutoIncrement = %v; want %v", c.name, got, c.want)
		}
	}
}

// TestEmitCreateSequence pins the DDL rendering: every option
// explicit, `AS <type>` only for non-bigint, CYCLE/NO CYCLE both
// spelled out, descending values passed through untouched.
func TestEmitCreateSequence(t *testing.T) {
	cases := []struct {
		name string
		seq  *ir.Sequence
		want string
	}{
		{
			"ascending bigint (no AS clause)",
			&ir.Sequence{Name: "order_number_seq", DataType: "bigint", Start: 1000, Increment: 5, MinValue: 1, MaxValue: math.MaxInt64, Cache: 1},
			`CREATE SEQUENCE "public"."order_number_seq" START WITH 1000 INCREMENT BY 5 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 NO CYCLE;`,
		},
		{
			"integer type with cycle + cache",
			&ir.Sequence{Name: "ticket_seq", DataType: "integer", Start: 1, Increment: 1, MinValue: 1, MaxValue: 50, Cache: 10, Cycle: true},
			`CREATE SEQUENCE "public"."ticket_seq" AS integer START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 50 CACHE 10 CYCLE;`,
		},
		{
			"descending",
			&ir.Sequence{Name: "countdown_seq", DataType: "bigint", Start: -10, Increment: -3, MinValue: math.MinInt64, MaxValue: -1, Cache: 1},
			`CREATE SEQUENCE "public"."countdown_seq" START WITH -10 INCREMENT BY -3 MINVALUE -9223372036854775808 MAXVALUE -1 CACHE 1 NO CYCLE;`,
		},
	}
	for _, c := range cases {
		if got := emitCreateSequence("public", c.seq); got != c.want {
			t.Errorf("%s:\n got  %s\n want %s", c.name, got, c.want)
		}
	}
}

func TestEmitAlterSequenceOwnedBy(t *testing.T) {
	seq := &ir.Sequence{Name: "invoice_seq", OwnedByTable: "invoices", OwnedByColumn: "number"}
	want := `ALTER SEQUENCE "public"."invoice_seq" OWNED BY "public"."invoices"."number";`
	if got := emitAlterSequenceOwnedBy("public", seq); got != want {
		t.Errorf("emitAlterSequenceOwnedBy = %s; want %s", got, want)
	}
}

// TestSequencePositionBehind pins the forward-only comparison every
// re-prime path gates on (delta review finding #1), across both
// directions and the is_called tie-break: a true result must mean
// "setval to b cannot rewind a".
func TestSequencePositionBehind(t *testing.T) {
	cases := []struct {
		name      string
		increment int64
		aLV       int64
		aCalled   bool
		bLV       int64
		bCalled   bool
		want      bool
	}{
		{"ascending: target below source", 5, 1000, false, 1005, true, true},
		{"ascending: target equal, source issued it", 5, 1005, false, 1005, true, true},
		{"ascending: equal positions", 5, 1005, true, 1005, true, false},
		{"ascending: target ahead", 5, 1010, true, 1005, true, false},
		{"ascending: target ahead but uncalled", 5, 1010, false, 1005, true, false},
		{"descending: target above source (behind)", -3, -10, false, -13, true, true},
		{"descending: target equal, source issued it", -3, -13, false, -13, true, true},
		{"descending: target further down (ahead)", -3, -16, true, -13, true, false},
		{"never-called source never pulls a called target back", 5, 1000, true, 1000, false, false},
	}
	for _, c := range cases {
		if got := sequencePositionBehind(c.increment, c.aLV, c.aCalled, c.bLV, c.bCalled); got != c.want {
			t.Errorf("%s: sequencePositionBehind(%d, %d/%v, %d/%v) = %v; want %v",
				c.name, c.increment, c.aLV, c.aCalled, c.bLV, c.bCalled, got, c.want)
		}
	}
}
