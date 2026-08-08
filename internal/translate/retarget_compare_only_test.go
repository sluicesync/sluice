// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// compareOnly runs the real shape-compare pass over a single mysql
// column and returns the type it lands on, so no test below hand-spells
// the rule's output.
func compareOnly(src ir.Type) ir.Type {
	out := RetargetForShapeCompare(&ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "c", Type: src}},
	}}}, "mysql", "postgres")
	return out.Tables[0].Columns[0].Type
}

// mysqlToPGShapeCase is one row of the item-158 family matrix.
//
// `want` is the type a Postgres CATALOG reads back for the column the PG
// writer creates from `src`. Every row's `want` was MEASURED — the
// fixture is the same one TestSchemaDiffAfterMigrate_MySQLToPostgres_
// TypeFamilyMatrix migrates onto a real PostgreSQL — never derived by
// mirroring [retargetPGtoMySQL], which answers a different question.
type mysqlToPGShapeCase struct {
	name string
	src  ir.Type
	want ir.Type
}

// mysqlToPGShapeMatrix covers EVERY family internal/engines/mysql's
// translateType can produce, not one representative per family (the Bug
// 74 lesson): the rewrite dispatches on the family AND on a shape field
// inside it (Integer.Width × Unsigned, Text.Size, Blob.Size), and a green
// row for one shape says nothing about its siblings.
//
// Rows whose `want` equals `src` are the identity half and are load-
// bearing in the other direction: they are what fails if a rule starts
// firing where the catalog round-trips faithfully, which would make
// `schema diff` stop reporting real drift.
var mysqlToPGShapeMatrix = []mysqlToPGShapeCase{
	// ---- Integer: PG has no unsigned and only three widths ----
	{"tinyint", ir.Integer{Width: 8}, ir.Integer{Width: 16}},
	{"tinyint unsigned", ir.Integer{Width: 8, Unsigned: true}, ir.Integer{Width: 16}},
	{"smallint", ir.Integer{Width: 16}, ir.Integer{Width: 16}},
	{"smallint unsigned", ir.Integer{Width: 16, Unsigned: true}, ir.Integer{Width: 32}},
	{"mediumint", ir.Integer{Width: 24}, ir.Integer{Width: 32}},
	{"mediumint unsigned", ir.Integer{Width: 24, Unsigned: true}, ir.Integer{Width: 32}},
	{"int", ir.Integer{Width: 32}, ir.Integer{Width: 32}},
	{"int unsigned", ir.Integer{Width: 32, Unsigned: true}, ir.Integer{Width: 64}},
	{"bigint", ir.Integer{Width: 64}, ir.Integer{Width: 64}},
	{"bigint unsigned", ir.Integer{Width: 64, Unsigned: true}, ir.Integer{Width: 64}},
	// AUTO_INCREMENT rides through PG's GENERATED … AS IDENTITY and reads
	// back set. A rule that dropped it would report phantom drift on every
	// synthetic primary key, which is most of them.
	{
		"int auto_increment",
		ir.Integer{Width: 32, AutoIncrement: true},
		ir.Integer{Width: 32, AutoIncrement: true},
	},
	{
		"bigint unsigned auto_increment",
		ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true},
		ir.Integer{Width: 64, AutoIncrement: true},
	},
	{
		"tinyint unsigned auto_increment",
		ir.Integer{Width: 8, Unsigned: true, AutoIncrement: true},
		ir.Integer{Width: 16, AutoIncrement: true},
	},

	// ---- Text: PG `text` is unbounded and tier-free ----
	{
		"tinytext",
		ir.Text{Size: ir.TextTiny, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
		ir.Text{Size: ir.TextLong, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
	},
	{"text", ir.Text{Size: ir.TextRegular}, ir.Text{Size: ir.TextLong}},
	{"mediumtext", ir.Text{Size: ir.TextMedium}, ir.Text{Size: ir.TextLong}},
	{"longtext", ir.Text{Size: ir.TextLong}, ir.Text{Size: ir.TextLong}},

	// ---- Binary: PG has exactly one, BYTEA ----
	{"binary(16)", ir.Binary{Length: 16}, ir.Blob{Size: ir.BlobLong}},
	{"varbinary(64)", ir.Varbinary{Length: 64}, ir.Blob{Size: ir.BlobLong}},
	{"tinyblob", ir.Blob{Size: ir.BlobTiny}, ir.Blob{Size: ir.BlobLong}},
	{"blob", ir.Blob{Size: ir.BlobRegular}, ir.Blob{Size: ir.BlobLong}},
	{"mediumblob", ir.Blob{Size: ir.BlobMedium}, ir.Blob{Size: ir.BlobLong}},
	{"longblob", ir.Blob{Size: ir.BlobLong}, ir.Blob{Size: ir.BlobLong}},

	// ---- SET degrades to TEXT[] (+ a CHECK the diff still reports) ----
	{
		"set",
		ir.Set{Values: []string{"x", "y"}},
		ir.Array{Element: ir.Text{Size: ir.TextLong}},
	},

	// ---- Identity families, measured rather than assumed ----
	{"boolean", ir.Boolean{}, ir.Boolean{}},
	{"decimal(10,2)", ir.Decimal{Precision: 10, Scale: 2}, ir.Decimal{Precision: 10, Scale: 2}},
	{"float", ir.Float{Precision: ir.FloatSingle}, ir.Float{Precision: ir.FloatSingle}},
	{"double", ir.Float{Precision: ir.FloatDouble}, ir.Float{Precision: ir.FloatDouble}},
	{"bit(8)", ir.Bit{Length: 8}, ir.Bit{Length: 8}},
	{"char(10)", ir.Char{Length: 10}, ir.Char{Length: 10}},
	{"varchar(50)", ir.Varchar{Length: 50}, ir.Varchar{Length: 50}},
	{"date", ir.Date{}, ir.Date{}},
	{"time(3)", ir.Time{Precision: 3}, ir.Time{Precision: 3}},
	{"datetime(6)", ir.DateTime{Precision: 6}, ir.DateTime{Precision: 6}},
	{
		"timestamp(0)",
		ir.Timestamp{Precision: 0, WithTimeZone: true},
		ir.Timestamp{Precision: 0, WithTimeZone: true},
	},
	{"enum", ir.Enum{Values: []string{"red", "green"}}, ir.Enum{Values: []string{"red", "green"}}},
	{"json", ir.JSON{Binary: true}, ir.JSON{Binary: true}},
	// MariaDB natives. ir.Inet.String() carries no family discriminant, so
	// INET4 and INET6 are indistinguishable from PG's `inet` read-back;
	// that is a property of the comparison, and the identity row is what
	// pins it rather than leaving it to be rediscovered.
	{"uuid (mariadb)", ir.UUID{}, ir.UUID{}},
	{"inet6 (mariadb)", ir.Inet{Family: ir.InetFamilyIPv6}, ir.Inet{Family: ir.InetFamilyIPv6}},
}

// TestRetargetMySQLtoPGShapeCompare_FamilyMatrix is the primary pin: for
// every family × shape, the expected side names what the target holds.
func TestRetargetMySQLtoPGShapeCompare_FamilyMatrix(t *testing.T) {
	var rewritten, identity int
	for _, tc := range mysqlToPGShapeMatrix {
		t.Run(tc.name, func(t *testing.T) {
			got := compareOnly(tc.src)
			if got.String() != tc.want.String() {
				t.Errorf("compare-lane retarget of %s = %s; a Postgres catalog reads that column back as %s.\n"+
					"An expected side that names the wrong shape reports PHANTOM type drift on a target "+
					"`sluice migrate` itself created", tc.src, got, tc.want)
			}
		})
		if tc.src.String() == tc.want.String() {
			identity++
		} else {
			rewritten++
		}
	}

	// Anti-vacuity on BOTH axes. A matrix that was all-identity would pass
	// against a rule table that never fires; an all-rewritten one could not
	// tell a correct rule from one that fires on everything.
	if rewritten < 16 {
		t.Errorf("only %d matrix rows expect a rewrite; the measured mysql→postgres divergence is 16 columns "+
			"across 4 families and this matrix has stopped covering them", rewritten)
	}
	if identity < 15 {
		t.Errorf("only %d matrix rows expect identity; the identity half is what catches a rule that fires "+
			"where the catalog round-trips faithfully", identity)
	}
}

// TestRetargetMySQLtoPGShapeCompare_GenuineDriftIsStillReported is the
// over-refusal half, and it is the assertion that matters most.
//
// Every rule added to the compare lane makes `schema diff` say LESS. The
// failure mode to fear is not a leftover phantom line, it is a diff that
// answers "in sync" about a target whose column type genuinely changed —
// so each rewritten family gets a paired row here: a real, plausible
// drift in that family must still come out DIFFERENT after the rewrite.
func TestRetargetMySQLtoPGShapeCompare_GenuineDriftIsStillReported(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  ir.Type
		// actual is what the target really holds after someone ALTERed it.
		actual ir.Type
	}{
		// Integer: the whole family collapses widths, so the drift that
		// must survive is a NARROWING the collapse could have hidden.
		{"tinyint altered to bigint", ir.Integer{Width: 8}, ir.Integer{Width: 64}},
		{"int unsigned altered to integer", ir.Integer{Width: 32, Unsigned: true}, ir.Integer{Width: 32}},
		{"bigint altered to integer", ir.Integer{Width: 64}, ir.Integer{Width: 32}},
		{
			"auto_increment dropped on the target",
			ir.Integer{Width: 32, AutoIncrement: true},
			ir.Integer{Width: 32},
		},
		// Text: the tier collapse must not swallow a text→varchar change,
		// which is a real length constraint appearing on the target.
		{"text altered to varchar(20)", ir.Text{Size: ir.TextRegular}, ir.Varchar{Length: 20}},
		{"longtext altered to varchar(20)", ir.Text{Size: ir.TextLong}, ir.Varchar{Length: 20}},
		// Binary: the collapse to Blob[long] must not swallow a binary
		// column that became text on the target — the classic encoding bug.
		{"varbinary altered to text", ir.Varbinary{Length: 64}, ir.Text{Size: ir.TextLong}},
		{"blob altered to jsonb", ir.Blob{Size: ir.BlobRegular}, ir.JSON{Binary: true}},
		// SET: TEXT[] is what sluice lands, so a target holding a scalar
		// text column (someone dropped the array-ness) is real drift.
		{"set altered to text", ir.Set{Values: []string{"x", "y"}}, ir.Text{Size: ir.TextLong}},
		{
			"set element family changed",
			ir.Set{Values: []string{"x", "y"}},
			ir.Array{Element: ir.Integer{Width: 32}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expected := compareOnly(tc.src)
			if expected.String() == tc.actual.String() {
				t.Errorf("the compare-lane retarget of %s produced %s, which EQUALS the drifted target shape %s.\n\n"+
					"`sluice schema diff` would report this target as in sync. A rule that silences a phantom "+
					"line by also silencing real drift is strictly worse than the phantom: the operator is told "+
					"a target matches when its column type has genuinely changed",
					tc.src, expected, tc.actual)
			}
		})
	}
}

// TestShapeCompareRuleFor_NoPairHasBothTables pins the exclusivity
// [shapeCompareRuleFor] relies on. A pair holding an emit table AND a
// compare-only table would silently run only the first, and the second
// would look present while grading nothing.
func TestShapeCompareRuleFor_NoPairHasBothTables(t *testing.T) {
	engineNames := []string{"mysql", "mariadb", "planetscale", "vitess", "postgres", "postgres-trigger", "sqlite", "d1"}
	pairs := 0
	for _, src := range engineNames {
		for _, tgt := range engineNames {
			if retargetRuleFor(src, tgt) != nil && compareOnlyRuleFor(src, tgt) != nil {
				t.Errorf("engine pair %s→%s has BOTH an emit-lane rule table and a compare-only one; "+
					"shapeCompareRuleFor runs only the emit table, so the compare-only arms would be dead. "+
					"An incomplete emit table is a defect to fix there", src, tgt)
			}
			pairs++
		}
	}
	if pairs < 64 {
		t.Fatalf("only %d pairs probed; the roster has shrunk", pairs)
	}
}

// TestHasStorageShapeMapping_WithholdsTheCompareOnlyPair pins the
// deliberate divergence documented on [HasStorageShapeMapping]: the
// mysql→postgres pair HAS a shape-compare mapping and is still withheld
// from the REFUSING consumer, because ir.Geometry is unmeasured on that
// lane. Both directions are asserted so neither half can quietly change:
// dropping the withholding arms `migrate`, and dropping the compare rule
// re-opens the phantom drift.
func TestHasStorageShapeMapping_WithholdsTheCompareOnlyPair(t *testing.T) {
	if shapeCompareRuleFor("mysql", "postgres") == nil {
		t.Fatal("mysql→postgres has no shape-compare rule; item 158's whole rule table is gone")
	}
	if HasStorageShapeMapping("mysql", "postgres") {
		t.Error("HasStorageShapeMapping is now true for mysql→postgres, which ARMS the migrate pre-create " +
			"shape gate to REFUSE on that pair. That is a deliberate future step, not a side effect: it needs " +
			"a PostGIS-tagged family matrix first (see the doc on HasStorageShapeMapping). If that evidence " +
			"now exists, update the doc and this test together")
	}
	// The control: a pair that legitimately reports true, so the assertion
	// above cannot pass against a predicate that returns false for
	// everything.
	if !HasStorageShapeMapping("postgres", "mysql") {
		t.Error("HasStorageShapeMapping is false for postgres→mysql, which has an emit-lane rule table; " +
			"this control is what keeps the assertion above from being vacuous")
	}
}

// TestPGStorageIntegerWidth pins the arithmetic the Postgres emitter and
// the compare-lane retarget now SHARE. It is deliberately a table over
// every (width, unsigned) pair a reader can produce rather than over the
// interesting ones — the two-stage widen-then-collapse is exactly the
// shape where a representative passes and a sibling drifts.
func TestPGStorageIntegerWidth(t *testing.T) {
	for _, tc := range []struct {
		width    int8
		unsigned bool
		want     int8
	}{
		{8, false, 16},
		{8, true, 16},
		{16, false, 16},
		{16, true, 32},
		{24, false, 32},
		{24, true, 32},
		{32, false, 32},
		{32, true, 64},
		{64, false, 64},
		{64, true, 64},
	} {
		got := PGStorageIntegerWidth(ir.Integer{Width: tc.width, Unsigned: tc.unsigned})
		if got != tc.want {
			t.Errorf("PGStorageIntegerWidth(width=%d unsigned=%v) = %d; want %d",
				tc.width, tc.unsigned, got, tc.want)
		}
	}
	// An unmodelled width falls to the widest rather than to a narrower
	// guess; only the narrow direction can truncate.
	if got := PGStorageIntegerWidth(ir.Integer{Width: 0}); got != 64 {
		t.Errorf("PGStorageIntegerWidth(width=0) = %d; want 64 (widest-wins fallback)", got)
	}
}
