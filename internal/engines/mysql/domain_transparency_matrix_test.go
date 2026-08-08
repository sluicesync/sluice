// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The TYPE-PATH axis of the MySQL array/JSON landing story — the third
// axis of one class, and the one Bug 233 cost.
//
// # The three axes, and why this file exists
//
//  1. ELEMENT FAMILY  — item 152. json[] vs bytea[] vs everything else.
//     Pinned by TestEveryDecodableArrayElementHasAMySQLLeafVerdict.
//  2. ARRIVAL SHAPE   — the item-152 value-fidelity review. The same
//     declared family encoded differently depending on whether the value
//     arrived as a decoded []any, a PG array literal string, or that
//     literal as []byte. Pinned by
//     TestConvertArrayLikeToJSON_LiteralArmsTakeTheSameLeafPolicy.
//  3. TYPE PATH       — Bug 233. The same family reached through a
//     `CREATE DOMAIN` wrapper never reaches the leaf policy at all,
//     because the LOAD DATA lane's own dispatch matched no arm and took
//     the default branch. This file.
//
// Each of the first two was closed by a matrix over VALUES, and neither
// could ever have seen the third: the defect is in which BRANCH runs,
// not in what the branch computes. So the invariant here is a RELATION
// rather than a constant — for every cell, the LOAD DATA lane's whole
// output must be IDENTICAL whether the column is bare or reached
// through a domain — and the bare side is what the two matrices above
// and the real-server integration tests already ground-truth against
// PostgreSQL's own rendering.
//
// # The independent expected value, named (the 2026-08-01 rule)
//
// There is none HERE, and that is deliberate: this file compares two
// readings of sluice's own code against each other. It is a
// TRANSPARENCY check, not a fidelity one. The fidelity anchor is
// TestMigrate_PGToMySQL_JSONAndByteaArrays / its DOMAIN sibling on a
// real MySQL server, where PostgreSQL's `::text`, `encode(...,'base64')`
// and `array_dims` are the oracle. Neither substitutes for the other:
// this one would pass if BOTH paths were wrong in the same way, and
// that one would pass on the bare path while the domain path copied
// zero rows — which is exactly what shipped.
//
// # What is covered, stated so the name cannot be read as broader
//
// The cube is covered by the UNION of three slices, not in full, and
// the reason is honest rather than economic: rendering an arbitrary
// element family's PG array LITERAL means reproducing PostgreSQL's
// `array_out`, which this package cannot do faithfully. So:
//
//   - EVERY element family the PG reader can produce (derived from
//     mysqlArrayLeafRoster, which is itself derived from that reader's
//     own map) × every type path, in the decoded-[]any arrival shape.
//   - Every arrival shape × every type path, on the families whose
//     literal spelling is already written down next door.
//   - Every SCALAR arm of columnSetExpr (derived by reading its source,
//     so a new arm fails this test until it is probed) × every type
//     path.
package mysql

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// domainTypePaths is the TYPE-PATH axis: the ways one storage type can
// be spelled in a column's declared type.
//
// The domain carries a CHECK so the wrapper is a realistic one — a
// domain with no checks is a bare type alias, and a transparency bug
// that only fired for a checked domain would slip a checkless probe.
// The nested path is not reachable from `information_schema` (Postgres
// reports the ultimate base type for a domain over a domain), but it IS
// reachable from the backup manifest, whose wire encodes
// `domain_base_type` recursively — so it is a real arrival shape for
// the restore lane, not a hypothetical.
var domainTypePaths = []struct {
	name string
	wrap func(ir.Type) ir.Type
}{
	{"bare", func(t ir.Type) ir.Type { return t }},
	{"domain", func(t ir.Type) ir.Type {
		return ir.Domain{
			Name:     "d_probe",
			BaseType: t,
			Checks:   []ir.DomainCheck{{Name: "d_probe_check", Body: "VALUE IS NOT NULL"}},
		}
	}},
	{"domain-over-domain", func(t ir.Type) ir.Type {
		return ir.Domain{
			Name:     "d_outer",
			BaseType: ir.Domain{Name: "d_inner", BaseType: t},
		}
	}},
}

// loadDataLaneOutcome runs the WHOLE LOAD DATA lane for one column and
// one value and returns what the server would see: the serialised TSV
// field, and the SET-clause expression that field is assigned through.
//
// Both halves are needed and neither is sufficient. The Bug 233 defect
// was entirely in the SET clause — the field bytes were correct the
// whole time, which is why the batched-INSERT core carried every DOMAIN
// column faithfully while the LOAD DATA core copied zero rows. The
// mirror defect (the ir.Bit branch in encodeRowsTSV) is entirely in the
// field bytes. Comparing the pair is what makes the two halves one
// agreement.
func loadDataLaneOutcome(t *testing.T, col *ir.Column, raw any) (field, setExpr string) {
	t.Helper()
	rows := make(chan ir.Row, 1)
	rows <- ir.Row{col.Name: raw}
	close(rows)
	var sb strings.Builder
	n, drained, err := encodeRowsTSV(context.Background(), &sb, []*ir.Column{col}, rows, 0)
	if err != nil {
		t.Fatalf("encodeRowsTSV(%s, %T): %v", col.Type.String(), raw, err)
	}
	if n != 1 || !drained {
		t.Fatalf("encodeRowsTSV(%s) wrote %d rows (drained=%v); want exactly 1", col.Type.String(), n, drained)
	}
	return strings.TrimSuffix(sb.String(), "\n"), columnSetExpr(col, "@c0")
}

// assertTypePathTransparent is the assertion every cell of the matrix
// makes: the bare column's LOAD DATA outcome, and the same column
// reached through each domain spelling, must be identical.
func assertTypePathTransparent(t *testing.T, cell string, base, srcType ir.Type, raw any) {
	t.Helper()
	var wantField, wantSet string
	for i, path := range domainTypePaths {
		col := &ir.Column{Name: "c", Type: path.wrap(base)}
		if srcType != nil {
			col.SourceColumnType = path.wrap(srcType)
		}
		field, setExpr := loadDataLaneOutcome(t, col, raw)
		if i == 0 {
			wantField, wantSet = field, setExpr
			continue
		}
		if setExpr != wantSet {
			t.Errorf("%s [%s]: SET clause is %q through the wrapper and %q bare — a DOMAIN is a CONSTRAINT "+
				"wrapper, not a storage one, so it cannot change which MySQL column the value lands in "+
				"(this exact divergence is Bug 233: the missing CONVERT(... USING utf8mb4) made the server "+
				"answer Error 3144 and copy zero rows)", cell, path.name, setExpr, wantSet)
		}
		if field != wantField {
			t.Errorf("%s [%s]: the TSV field is %q through the wrapper and %q bare — the serialised value "+
				"must not depend on how the type was spelled", cell, path.name, field, wantField)
		}
	}
}

// TestDomainTypePath_EveryArrayElementFamily is slice 1: every element
// family the Postgres reader can decode, through every type path.
//
// The family list is [mysqlArrayLeafRoster], which is itself derived
// from the PG schema reader's `builtinArrayElement` map — so a family
// added there fails the roster gate first, and lands here for free once
// its verdict is stated.
func TestDomainTypePath_EveryArrayElementFamily(t *testing.T) {
	names := make([]string, 0, len(mysqlArrayLeafRoster))
	for udt := range mysqlArrayLeafRoster {
		names = append(names, udt)
	}
	sort.Strings(names)
	if len(names) < 20 {
		t.Fatalf("the derived family roster has only %d entries; it has come unbound from the Postgres "+
			"reader's map and this matrix is grading almost nothing", len(names))
	}
	for _, udt := range names {
		entry := mysqlArrayLeafRoster[udt]
		t.Run(udt, func(t *testing.T) {
			arr := ir.Array{Element: entry.elem}
			// Both spellings of an array column the writer really sees:
			// the migrate/restore lane's own ir.Array, and the
			// override/retarget lane's ir.JSON with the array parked in
			// SourceColumnType. The second is the one Bug 232 lived in.
			assertTypePathTransparent(t, udt+" 1-D", arr, nil, []any{entry.leaf})
			assertTypePathTransparent(t, udt+" 2-D", arr, nil, []any{[]any{entry.leaf, nil}})
			assertTypePathTransparent(t, udt+" NULL element", arr, nil, []any{nil, entry.leaf})
			assertTypePathTransparent(t, udt+" empty", arr, nil, []any{})
			assertTypePathTransparent(t, udt+" parked-array spelling", ir.JSON{}, arr, []any{entry.leaf})
		})
	}
}

// TestDomainTypePath_EveryArrivalShape is slice 2: the arrival-shape
// axis crossed with the type-path axis, on the families whose PG array
// LITERAL spelling is already written down (see the file header for why
// the literal half cannot be generated for every family).
func TestDomainTypePath_EveryArrivalShape(t *testing.T) {
	for _, tc := range []struct {
		name    string
		elem    ir.Type
		decoded []any
		literal string
	}{
		{"bytea", ir.Blob{}, []any{[]byte{0xde, 0xad}, []byte{0xbe, 0xef}}, `{"\\xdead","\\xbeef"}`},
		{"json", ir.JSON{}, []any{[]byte(`{"a":1}`), []byte(`[1,2]`)}, `{"{\"a\":1}","[1,2]"}`},
		{"text", ir.Text{}, []any{"a", "b"}, `{a,b}`},
		{"bytea with a NULL element", ir.Blob{}, []any{[]byte{0xaa}, nil}, `{"\\xaa",NULL}`},
		{"empty", ir.Blob{}, []any{}, `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arr := ir.Array{Element: tc.elem}
			for _, arrival := range []struct {
				lane string
				raw  any
			}{
				{"decoded []any", tc.decoded},
				{"literal string", tc.literal},
				{"literal []byte", []byte(tc.literal)},
			} {
				assertTypePathTransparent(t, tc.name+" / "+arrival.lane+" / Type", arr, nil, arrival.raw)
				assertTypePathTransparent(t, tc.name+" / "+arrival.lane+" / parked", ir.JSON{}, arr, arrival.raw)
			}
		})
	}
}

// columnSetExprProbes is slice 3: one probe per SCALAR arm of
// [columnSetExpr], keyed by the ir type name the arm matches. The keys
// are held to the arms by [TestColumnSetExprArmsAreAllProbed], which
// reads columnSetExpr's own source — so an arm added without a probe
// fails, which is what makes this a derived table rather than a list
// someone remembered to update.
var columnSetExprProbes = map[string]struct {
	typ ir.Type
	raw any
}{
	"JSON":          {ir.JSON{}, []byte(`{"a":1}`)},
	"Array":         {ir.Array{Element: ir.Text{}}, []any{"a", "b"}},
	"Bit":           {ir.Bit{Length: 8}, "10101010"},
	"Varchar":       {ir.Varchar{Length: 32}, "hello"},
	"Text":          {ir.Text{Size: ir.TextLong}, "hello"},
	"Set":           {ir.Set{Values: []string{"a", "b"}}, []string{"a", "b"}},
	"ExtensionType": {ir.ExtensionType{Extension: "hstore"}, []byte(`{"k":"v"}`)},
}

// TestDomainTypePath_EveryColumnSetExprArm crosses slice 3 with the
// type-path axis, plus one control that takes the fall-through so a
// change making EVERY column CONVERT-wrapped would still fail
// something.
func TestDomainTypePath_EveryColumnSetExprArm(t *testing.T) {
	names := make([]string, 0, len(columnSetExprProbes))
	for name := range columnSetExprProbes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		probe := columnSetExprProbes[name]
		t.Run(name, func(t *testing.T) {
			assertTypePathTransparent(t, name, probe.typ, nil, probe.raw)
		})
	}
	t.Run("Integer control (no arm)", func(t *testing.T) {
		assertTypePathTransparent(t, "Integer", ir.Integer{Width: 64}, nil, int64(7))
		col := &ir.Column{Name: "c", Type: ir.Integer{Width: 64}}
		if _, setExpr := loadDataLaneOutcome(t, col, int64(7)); setExpr != "@c0" {
			t.Errorf("an Integer column's SET clause is %q; want the bare variable — if every type is now "+
				"wrapped, the transparency assertions above are comparing two constants", setExpr)
		}
	})
}

// TestColumnSetExprArmsAreAllProbed binds [columnSetExprProbes] to
// [columnSetExpr]'s actual arms by reading its source. Without this the
// probe table is a list someone updated by hand, and the next arm added
// to columnSetExpr — the exact event that produced Bug 233 — would go
// unprobed and green.
func TestColumnSetExprArmsAreAllProbed(t *testing.T) {
	arms := columnSetExprArmTypeNames(t)
	if len(arms) < 6 {
		t.Fatalf("only %d ir types found in columnSetExpr's arms (%v); the reader has come unbound from "+
			"the function and this gate is vacuous", len(arms), arms)
	}
	for _, name := range arms {
		if _, ok := columnSetExprProbes[name]; !ok {
			t.Errorf("columnSetExpr has an arm for ir.%s and no probe for it in columnSetExprProbes — that "+
				"arm's behaviour through a DOMAIN wrapper is ungraded, which is the shape Bug 233 shipped in",
				name)
		}
	}
	have := make(map[string]bool, len(arms))
	for _, name := range arms {
		have[name] = true
	}
	for name := range columnSetExprProbes {
		if !have[name] {
			t.Errorf("columnSetExprProbes probes ir.%s, which columnSetExpr no longer has an arm for — a "+
				"stale probe grades nothing and hides the shrinking arm set", name)
		}
	}
}

// columnSetExprArmTypeNames reads columnSetExpr's source and returns
// every `ir.X` type named in one of its type switches or type
// assertions.
func columnSetExprArmTypeNames(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "load_data_writer.go", nil, 0)
	if err != nil {
		t.Fatalf("parse load_data_writer.go: %v", err)
	}
	var fn *ast.FuncDecl
	for _, decl := range f.Decls {
		if d, ok := decl.(*ast.FuncDecl); ok && d.Name.Name == "columnSetExpr" {
			fn = d
			break
		}
	}
	if fn == nil {
		t.Fatal("columnSetExpr not found in load_data_writer.go — this gate has been renamed out from " +
			"under itself and would otherwise pass forever")
	}
	seen := make(map[string]bool)
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.CaseClause:
			for _, e := range v.List {
				if name, ok := irTypeName(e); ok {
					seen[name] = true
				}
			}
		case *ast.TypeAssertExpr:
			if v.Type != nil {
				if name, ok := irTypeName(v.Type); ok {
					seen[name] = true
				}
			}
		}
		return true
	})
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// irTypeName returns the X of an `ir.X` selector expression.
func irTypeName(e ast.Expr) (string, bool) {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "ir" {
		return "", false
	}
	return sel.Sel.Name, true
}
