package mysql

import (
	"reflect"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestBuildRowPlaceholder(t *testing.T) {
	cases := []struct {
		cols int
		want string
	}{
		{0, "()"},
		{1, "(?)"},
		{2, "(?, ?)"},
		{3, "(?, ?, ?)"},
		{5, "(?, ?, ?, ?, ?)"},
	}
	for _, c := range cases {
		if got := buildRowPlaceholder(c.cols); got != c.want {
			t.Errorf("buildRowPlaceholder(%d) = %q; want %q", c.cols, got, c.want)
		}
	}
}

func TestBuildBatchInsert(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
	}

	cases := []struct {
		rows int
		want string
	}{
		{1, "INSERT INTO `users` (`id`, `email`) VALUES (?, ?)"},
		{3, "INSERT INTO `users` (`id`, `email`) VALUES (?, ?), (?, ?), (?, ?)"},
	}
	for _, c := range cases {
		got := buildBatchInsert(table, c.rows)
		if got != c.want {
			t.Errorf("buildBatchInsert(%d):\n got  %q\n want %q", c.rows, got, c.want)
		}
	}
}

func TestBuildBatchInsertEscapesIdentifiers(t *testing.T) {
	table := &ir.Table{
		Name: "weird`table",
		Columns: []*ir.Column{
			{Name: "weird`col", Type: ir.Integer{Width: 32}},
		},
	}
	got := buildBatchInsert(table, 1)
	want := "INSERT INTO `weird``table` (`weird``col`) VALUES (?)"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestFlattenArgs(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Integer{Width: 32}},
			{Name: "b", Type: ir.Varchar{Length: 32}},
		},
	}
	batch := []ir.Row{
		{"a": int64(1), "b": "first"},
		{"a": int64(2), "b": "second"},
	}
	got := flattenArgs(batch, table)
	want := []any{int64(1), "first", int64(2), "second"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("flattenArgs:\n got  %#v\n want %#v", got, want)
	}
}

func TestFlattenArgsMissingValueIsNil(t *testing.T) {
	// A row that omits a column: the missing key looks up to nil
	// (the zero value of any), which is what the driver expects for
	// SQL NULL.
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Integer{Width: 32}},
			{Name: "b", Type: ir.Varchar{Length: 32}},
		},
	}
	batch := []ir.Row{
		{"a": int64(1)}, // b is omitted
	}
	got := flattenArgs(batch, table)
	want := []any{int64(1), nil}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("flattenArgs (missing value):\n got  %#v\n want %#v", got, want)
	}
}

func TestPrepareValue(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		in   any
		t    ir.Type
		want any
	}{
		// Pass-through types — driver handles them natively.
		{"nil", nil, ir.Integer{Width: 32}, nil},
		{"bool true", true, ir.Boolean{}, true},
		{"int64", int64(42), ir.Integer{Width: 32}, int64(42)},
		{"uint64", uint64(1 << 63), ir.Integer{Width: 64, Unsigned: true}, uint64(1 << 63)},
		{"float64", 3.14, ir.Float{Precision: ir.FloatDouble}, 3.14},
		{"string", "hello", ir.Varchar{Length: 32}, "hello"},
		{"bytes", []byte{0xde, 0xad}, ir.Blob{Size: ir.BlobRegular}, []byte{0xde, 0xad}},
		{"time", now, ir.Timestamp{Precision: 0, WithTimeZone: true}, now},
		{"decimal as string", "19.95", ir.Decimal{Precision: 10, Scale: 2}, "19.95"},

		// Special case: Set's canonical []string becomes a comma-joined string.
		{"set with members", []string{"a", "b", "c"}, ir.Set{Values: []string{"a", "b", "c", "d"}}, "a,b,c"},
		{"set empty", []string{}, ir.Set{Values: []string{"a"}}, ""},

		// JSON []byte → string. Without this conversion the driver
		// labels the parameter with the _binary charset prefix on
		// the wire and Vitess rejects with "Cannot create a JSON
		// value from a string with CHARACTER SET 'binary'". Real-
		// world failure surfaced during PlanetScale-target
		// integration testing.
		{"json bytes → string", []byte(`{"k":"v"}`), ir.JSON{Binary: true}, `{"k":"v"}`},
		{"json textual → string", []byte(`["a",1]`), ir.JSON{Binary: false}, `["a",1]`},
		{"json string passthrough", `{"k":"v"}`, ir.JSON{Binary: true}, `{"k":"v"}`},

		// A non-Set column receiving []string passes through unchanged
		// — the driver would error, which is what we want when the
		// caller has a type confusion bug.
		{"unexpected []string", []string{"x"}, ir.Varchar{Length: 32}, []string{"x"}},

		// Bug 14: PG array values landing on a MySQL JSON column
		// (operator-supplied --type-override=col=jsonb) get
		// re-encoded as a JSON array string the MySQL JSON parser
		// accepts. Without this branch the driver hands the
		// driver-incompatible Go slice or the PG-array literal
		// straight through, and MySQL rejects with "Invalid JSON
		// text".
		{
			"json target with []any → JSON array",
			[]any{int64(1), int64(2), int64(3)},
			ir.JSON{Binary: true},
			`[1,2,3]`,
		},
		{
			"json target with []any of strings",
			[]any{"a", "b", "c"},
			ir.JSON{Binary: true},
			`["a","b","c"]`,
		},
		{
			"json target with PG-array-literal string",
			"{a,b,c}",
			ir.JSON{Binary: true},
			`["a","b","c"]`,
		},
		{
			"json target with quoted PG-array elements",
			`{"alpha","beta","with,comma"}`,
			ir.JSON{Binary: true},
			`["alpha","beta","with,comma"]`,
		},
		{
			"json target with PG-array NULL element",
			"{a,NULL,c}",
			ir.JSON{Binary: true},
			`["a",null,"c"]`,
		},
		{
			"json target with empty PG-array literal",
			"{}",
			ir.JSON{Binary: true},
			`[]`,
		},
		{
			"json target with []any containing nil",
			[]any{int64(1), nil, int64(3)},
			ir.JSON{Binary: true},
			`[1,null,3]`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := prepareValue(c.in, c.t)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("prepareValue(%#v, %T) = %#v; want %#v", c.in, c.t, got, c.want)
			}
		})
	}
}

// TestPrepareValue_PGArrayLiteralOnNonJSONPasses through unchanged —
// the array→JSON transformation is gated on the target being JSON.
// A `--type-override=col=text` (the documented Bug 14 workaround) on
// a PG-array column should land the literal as a string, not be
// re-shaped.
func TestPrepareValue_PGArrayLiteralOnNonJSONPassesThrough(t *testing.T) {
	got := prepareValue("{a,b,c}", ir.Text{Size: ir.TextLong})
	if got != "{a,b,c}" {
		t.Errorf("prepareValue text-target: got %#v; want \"{a,b,c}\"", got)
	}
}

// TestPrepareValue_AnySliceOnNonJSONPassesThrough — the conversion
// is intentionally narrow. A []any landing on a non-JSON column is
// a caller bug, but it shouldn't silently morph; the driver's own
// error surface should report the type mismatch.
func TestPrepareValue_AnySliceOnNonJSONPassesThrough(t *testing.T) {
	in := []any{int64(1), int64(2)}
	got := prepareValue(in, ir.Varchar{Length: 32})
	if !reflect.DeepEqual(got, in) {
		t.Errorf("prepareValue varchar-target: got %#v; want %#v", got, in)
	}
}
