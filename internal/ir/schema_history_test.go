// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// codecTypeMatrix is the every-family list the backup tagged-union
// codec supports — deliberately the SAME matrix TestMarshalType_RoundTrip
// in backup_test.go exercises. Bug-74 lesson: a Table-level round-trip
// must cover every Type FAMILY the codec dispatches on, not one
// representative, because Column.MarshalJSON delegates per-column to
// MarshalType and a family the codec mishandles only surfaces when
// that family rides through a real Table.
//
// NOTE (lead-review item): ir.Bit and ir.ExtensionType are NOT in this
// matrix because the backup codec (MarshalType/UnmarshalType in
// backup.go) has no branch for them — they hit its `default` error.
// ADR-0049 locked decision #1 is "reuse the backup codec verbatim", so
// schema-history inherits exactly the codec's supported set; extending
// the codec to Bit/ExtensionType is out of Chunk-A scope and a
// separate decision. The matrix below is the codec's true coverage.
func codecTypeMatrix() []struct {
	name string
	typ  Type
} {
	return []struct {
		name string
		typ  Type
	}{
		{"int", Integer{Width: 64, AutoIncrement: true}},
		{"int unsigned", Integer{Width: 32, Unsigned: true}},
		{"float single", Float{Precision: FloatSingle}},
		{"float double", Float{Precision: FloatDouble}},
		{"decimal", Decimal{Precision: 19, Scale: 4}},
		{"decimal unconstrained", Decimal{Unconstrained: true}},
		{"boolean", Boolean{}},
		{"text", Text{Size: TextLong, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}},
		{"char", Char{Length: 36, Charset: "utf8mb4", Collation: "utf8mb4_bin"}},
		{"varchar", Varchar{Length: 255}},
		{"date", Date{}},
		{"time", Time{Precision: 6, WithTimeZone: true}},
		{"datetime", DateTime{Precision: 3}},
		{"timestamp tz", Timestamp{Precision: 6, WithTimeZone: true}},
		{"json", JSON{Binary: true}},
		{"uuid", UUID{}},
		{"inet", Inet{}},
		{"cidr", Cidr{}},
		{"macaddr", Macaddr{}},
		{"binary", Binary{Length: 16}},
		{"varbinary", Varbinary{Length: 64}},
		{"blob", Blob{Size: BlobMedium}},
		{"geometry", Geometry{Subtype: GeometryPoint, SRID: 4326, IsGeography: true, HasZ: true, HasM: true}},
		{"enum", Enum{Values: []string{"a", "b", "c"}}},
		{"set", Set{Values: []string{"r", "w", "x"}}},
		{"array of int", Array{Element: Integer{Width: 32}}},
		{"array of uuid", Array{Element: UUID{}}},
		{"verbatim", VerbatimType{Definition: "ltree"}},
	}
}

// TestMarshalTable_RoundTrip_AllTypeFamilies builds ONE table whose
// columns span every codec-supported Type family plus a NOT NULL
// column, a column with a literal default, a column with an expression
// default, and a STORED generated column — then asserts MarshalTable →
// UnmarshalTable is deep-equal. This pins the class (Bug-74 lesson):
// the schema-history payload must survive every family, not a
// representative.
func TestMarshalTable_RoundTrip_AllTypeFamilies(t *testing.T) {
	fixed := []*Column{
		{Name: "id", Type: Integer{Width: 64, AutoIncrement: true}, Nullable: false},
		{Name: "with_literal_default", Type: Integer{Width: 32}, Nullable: true, Default: DefaultLiteral{Value: "0"}},
		{
			Name:    "with_expr_default",
			Type:    Timestamp{Precision: 6, WithTimeZone: true},
			Default: DefaultExpression{Expr: "CURRENT_TIMESTAMP", Dialect: "postgres"},
		},
		{
			Name:                 "generated_stored",
			Type:                 Integer{Width: 64},
			GeneratedExpr:        "id * 2",
			GeneratedStored:      true,
			GeneratedExprDialect: "postgres",
		},
		{Name: "not_null_text", Type: Text{Size: TextLong}, Nullable: false, Comment: "a comment"},
	}
	matrix := codecTypeMatrix()
	cols := make([]*Column, 0, len(fixed)+len(matrix))
	cols = append(cols, fixed...)
	for _, c := range matrix {
		cols = append(cols, &Column{Name: "c_" + c.name, Type: c.typ, Nullable: true})
	}

	original := &Table{
		Schema:  "app",
		Name:    "everything",
		Columns: cols,
		PrimaryKey: &Index{
			Name:    "pk_everything",
			Columns: []IndexColumn{{Column: "id"}},
			Unique:  true,
		},
		Indexes: []*Index{
			{Name: "idx_text", Columns: []IndexColumn{{Column: "not_null_text"}}},
		},
		Comment: "table-level comment",
	}

	b, err := MarshalTable(original)
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	got, err := UnmarshalTable(b)
	if err != nil {
		t.Fatalf("UnmarshalTable: %v", err)
	}
	if got == nil {
		t.Fatal("UnmarshalTable returned nil table")
	}

	// Column.UnmarshalJSON normalises an absent default to DefaultNone{};
	// mirror that on the original so reflect.DeepEqual is apples-to-apples.
	for _, c := range original.Columns {
		if c.Default == nil {
			c.Default = DefaultNone{}
		}
	}

	if !reflect.DeepEqual(original, got) {
		// Narrow the failure to the offending column for a usable diff.
		if len(original.Columns) == len(got.Columns) {
			for i := range original.Columns {
				if !reflect.DeepEqual(original.Columns[i], got.Columns[i]) {
					t.Errorf("column %q mismatch:\n orig=%#v\n got =%#v",
						original.Columns[i].Name, original.Columns[i], got.Columns[i])
				}
			}
		}
		t.Fatalf("table round-trip not deep-equal\n orig=%#v\n got =%#v", original, got)
	}
}

func TestMarshalTable_NilAndNull(t *testing.T) {
	b, err := MarshalTable(nil)
	if err != nil {
		t.Fatalf("MarshalTable(nil): %v", err)
	}
	if string(b) != "null" {
		t.Errorf("MarshalTable(nil) = %q; want null", b)
	}
	for _, in := range [][]byte{nil, {}, []byte("null")} {
		got, err := UnmarshalTable(in)
		if err != nil {
			t.Fatalf("UnmarshalTable(%q): %v", in, err)
		}
		if got != nil {
			t.Errorf("UnmarshalTable(%q) = %#v; want nil", in, got)
		}
	}
}

// ---- ResolveSchemaVersion ----

// fakeOrderer models a TOTAL order over an int rank parsed from the
// position token ("rN"). It is intentionally total (the resolve
// algorithm is exercised against the partial-order edge separately via
// disjointOrderer). PositionAtOrAfter(p, anchor) == p.rank >= anchor.rank.
type fakeOrderer struct{}

func rankOf(p Position) (int, error) {
	var n int
	if _, err := fmt.Sscanf(p.Token, "r%d", &n); err != nil {
		return 0, fmt.Errorf("fakeOrderer: malformed token %q: %w", p.Token, err)
	}
	return n, nil
}

func (fakeOrderer) PositionAtOrAfter(p, anchor Position) (bool, error) {
	pr, err := rankOf(p)
	if err != nil {
		return false, err
	}
	ar, err := rankOf(anchor)
	if err != nil {
		return false, err
	}
	return pr >= ar, nil
}

func anchorVersion(t *testing.T, token, tableName string) RetainedSchemaVersion {
	t.Helper()
	b, err := MarshalTable(&Table{Name: tableName, Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}}})
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	return RetainedSchemaVersion{Anchor: Position{Engine: "test", Token: token}, TableJSON: b}
}

func TestResolveSchemaVersion_Ordering(t *testing.T) {
	// Anchors at ranks 10, 20, 30 carrying distinguishable table names.
	versions := []RetainedSchemaVersion{
		anchorVersion(t, "r10", "v10"),
		anchorVersion(t, "r30", "v30"),
		anchorVersion(t, "r20", "v20"),
	}

	cases := []struct {
		name      string
		posToken  string
		wantTable string
		wantErrIs error
	}{
		{"before first → loud floor", "r5", "", ErrPositionInvalid},
		{"exactly at first", "r10", "v10", nil},
		{"between first and second", "r15", "v10", nil},
		{"exactly at middle", "r20", "v20", nil},
		{"between middle and last", "r25", "v20", nil},
		{"exactly at last", "r30", "v30", nil},
		{"after last", "r99", "v30", nil},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := ResolveSchemaVersion(fakeOrderer{}, versions, Position{Engine: "test", Token: c.posToken})
			if c.wantErrIs != nil {
				if !errors.Is(err, c.wantErrIs) {
					t.Fatalf("err = %v; want errors.Is %v", err, c.wantErrIs)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got == nil || got.Name != c.wantTable {
				t.Fatalf("got table %v; want %q", got, c.wantTable)
			}
		})
	}
}

func TestResolveSchemaVersion_NilOrderer_IsLoudNotErrPositionInvalid(t *testing.T) {
	versions := []RetainedSchemaVersion{anchorVersion(t, "r10", "v10")}
	_, err := ResolveSchemaVersion(nil, versions, Position{Engine: "test", Token: "r20"})
	if err == nil {
		t.Fatal("want loud error for nil orderer, got nil")
	}
	if errors.Is(err, ErrPositionInvalid) {
		t.Fatalf("nil-orderer error must NOT be ErrPositionInvalid (it is a config bug, not a cold-start "+
			"trigger); got %v", err)
	}
}

func TestResolveSchemaVersion_NoVersions_IsErrPositionInvalid(t *testing.T) {
	_, err := ResolveSchemaVersion(fakeOrderer{}, nil, Position{Engine: "test", Token: "r1"})
	if !errors.Is(err, ErrPositionInvalid) {
		t.Fatalf("empty history must be ErrPositionInvalid (→ ADR-0022 cold-start); got %v", err)
	}
}

// disjointOrderer models the MySQL GTID PARTIAL order: tokens are
// comma-separated set members; PositionAtOrAfter(p, anchor) == p ⊇
// anchor. Two disjoint sets are neither at-or-after the other — the
// Bug-74-class case a -1/0/1 comparator would mis-handle.
type disjointOrderer struct{}

func tokenSet(p Position) map[string]struct{} {
	s := map[string]struct{}{}
	for _, e := range splitComma(p.Token) {
		if e != "" {
			s[e] = struct{}{}
		}
	}
	return s
}

func splitComma(s string) []string {
	out := []string{}
	cur := ""
	for _, r := range s {
		if r == ',' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	return append(out, cur)
}

func (disjointOrderer) PositionAtOrAfter(p, anchor Position) (bool, error) {
	ps := tokenSet(p)
	for k := range tokenSet(anchor) {
		if _, ok := ps[k]; !ok {
			return false, nil
		}
	}
	return true, nil
}

// Two retained anchors that are mutually incomparable (disjoint GTID
// sets), and an event position that is a superset of BOTH. The resolve
// MUST refuse loudly with ErrPositionInvalid rather than silently pick
// one — there is no single in-effect schema.
func TestResolveSchemaVersion_PartialOrder_IncomparableCandidates_IsLoud(t *testing.T) {
	versions := []RetainedSchemaVersion{
		anchorVersion(t, "A", "vA"),
		anchorVersion(t, "B", "vB"),
	}
	// p ⊇ {A} and p ⊇ {B}; A and B are disjoint → both satisfy, neither dominates.
	_, err := ResolveSchemaVersion(disjointOrderer{}, versions, Position{Token: "A,B"})
	if !errors.Is(err, ErrPositionInvalid) {
		t.Fatalf("incomparable candidates must be a loud ErrPositionInvalid refuse; got %v", err)
	}
}

// When a single anchor is the dominating one under the partial order,
// resolve picks it cleanly (sanity that the partial-order path isn't
// over-refusing).
func TestResolveSchemaVersion_PartialOrder_DominatingAnchor(t *testing.T) {
	versions := []RetainedSchemaVersion{
		anchorVersion(t, "A", "vA"),
		anchorVersion(t, "A,B", "vAB"),
	}
	got, err := ResolveSchemaVersion(disjointOrderer{}, versions, Position{Token: "A,B,C"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got == nil || got.Name != "vAB" {
		t.Fatalf("want vAB (the greater anchor); got %v", got)
	}
}

func TestResolveSchemaVersion_MalformedPosition_IsLoudError(t *testing.T) {
	versions := []RetainedSchemaVersion{anchorVersion(t, "r10", "v10")}
	_, err := ResolveSchemaVersion(fakeOrderer{}, versions, Position{Token: "not-a-rank"})
	if err == nil {
		t.Fatal("want loud error for malformed position, got nil")
	}
	if errors.Is(err, ErrPositionInvalid) {
		t.Fatalf("a malformed position is a bug, not a cold-start trigger; must not be ErrPositionInvalid; got %v", err)
	}
}

// TestSchemaVersionKey pins the surrogate-PK contract that replaced the
// natural composite key after the CI 1071 regression (db212c8): the key
// must be deterministic, fixed 64-hex-char width, and — the correctness
// half, not just the InnoDB-3072-byte size half — DISTINCT for two
// anchors that share a long common prefix. The original
// anchor_position(255) prefix index would have collided these and
// silently overwritten one version with the other's schema (a
// silent-loss class). Pin the class, not one representative.
func TestSchemaVersionKey(t *testing.T) {
	// Deterministic + fixed width.
	k1 := SchemaVersionKey("s", "sch", "tbl", "gtid:1-100")
	if k1 != SchemaVersionKey("s", "sch", "tbl", "gtid:1-100") {
		t.Fatal("SchemaVersionKey must be deterministic")
	}
	if len(k1) != 64 {
		t.Fatalf("want 64 hex chars (CHAR(64) PK), got %d (%q)", len(k1), k1)
	}

	// Prefix-collision class: two distinct anchors sharing a 300-char
	// prefix (longer than the old 255 prefix index) must NOT collide.
	common := ""
	for i := 0; i < 300; i++ {
		common += "a"
	}
	kA := SchemaVersionKey("s", "sch", "tbl", common+"X")
	kB := SchemaVersionKey("s", "sch", "tbl", common+"Y")
	if kA == kB {
		t.Fatal("distinct long anchors sharing a prefix must yield distinct keys (the silent-overwrite class)")
	}

	// NUL-delimited: component regrouping must not alias
	// (a||b vs a'||b' where a+b == a'+b' concatenated).
	if SchemaVersionKey("ab", "c", "t", "p") == SchemaVersionKey("a", "bc", "t", "p") {
		t.Fatal("component boundaries must be unambiguous (NUL-delimited)")
	}
}
