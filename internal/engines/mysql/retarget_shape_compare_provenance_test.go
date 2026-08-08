// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 153 — the FOURTH type path, and the two premises that
// only hold jointly.
//
// [translate.RetargetForShapeCompare] flattens a `CREATE DOMAIN`
// wrapper to the storage a MySQL catalog reads back, and parks the
// wrapper in [ir.Column.SourceColumnType]. That produces a column shape
// nothing produced before — Type is the base type's MySQL landing,
// SourceColumnType is an [ir.Domain] — and two of this package's
// decisions read that field:
//
//   - columnIsNativelyBinary, whose safety argument treats a nil
//     SourceColumnType as "natively binary";
//   - columnIsArrayLike / arrayElementType, the only route to an array
//     column's element family once the retarget has replaced Type.
//
// # Why both pins live in ONE file
//
// Because the two changes are only JOINTLY safe, and the cost of
// leaving that to institutional memory is already written down. The
// flattening is safe only because both readers call [ir.UnwrapDomain]
// (Bug 233, a44c0fa7); removing either unwrap while the flattening
// stands re-opens Bug 232 through domains — a SILENT loss, the element
// family dropped and every json[] / bytea[] element rendered by the
// wrong policy. Nothing in the type system binds the two, so a test
// does: both pins below build their column by running the REAL retarget
// rather than hand-spelling its output.
//
// # The independent expected value, named (the 2026-08-01 rule)
//
// For the array matrix: the MIGRATE lane's own output for the same
// source column — a different code path through this package's leaf
// policy, ground-truthed against PostgreSQL itself by
// TestMigrate_PGToMySQL_ArrayFamiliesThroughADomain on a real server.
// For the binary pin: the verdict the UN-retargeted column gets, which
// is the verdict every pre-v0.117.0 release gave.

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// retargetedShapeCompareColumn returns the column descriptor the
// item-153 shape-compare retarget produces for a PG source column of
// the given type — by running the real pass, not by spelling its
// output.
func retargetedShapeCompareColumn(t *testing.T, name string, srcType ir.Type) *ir.Column {
	t.Helper()
	out := translate.RetargetForShapeCompare(&ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: name, Type: srcType}},
	}}}, "postgres", "mysql")
	return out.Tables[0].Columns[0]
}

// TestColumnIsNativelyBinary_SurvivesTheRetargetsProvenance is the
// binding half of the re-derived premise in internal/translate
// (TestRetargetRules_BinaryOutputOnlyFromABinarySource).
//
// That test proves the retarget never manufactures binariness. This one
// proves the property that ACTUALLY matters here: the provenance the
// retarget records never changes columnIsNativelyBinary's answer from
// the one the un-retargeted column would have given. Two facts pinned
// with nothing joining them is the gap CLAUDE.md's premise-naming step
// names; running the real pass is what closes it.
//
// The `bytea` DOMAIN rows are the live cells — they are what made the
// old "no rule produces a binary type" claim false — and they pass ONLY
// because columnIsNativelyBinary reads its field through
// [ir.UnwrapDomain]. Delete that unwrap and this fails.
func TestColumnIsNativelyBinary_SurvivesTheRetargetsProvenance(t *testing.T) {
	binaryOutputs := 0
	for _, tc := range []struct {
		name string
		src  ir.Type
	}{
		{"bare bytea", ir.Blob{}},
		{"bare binary(16)", ir.Binary{Length: 16}},
		{"bare varbinary", ir.Varbinary{Length: 255}},
		{"DOMAIN over bytea", ir.Domain{Name: "d_blob", BaseType: ir.Blob{}}},
		{"DOMAIN over binary(16)", ir.Domain{Name: "d_bin", BaseType: ir.Binary{Length: 16}}},
		{"DOMAIN over varbinary", ir.Domain{Name: "d_vbin", BaseType: ir.Varbinary{Length: 255}}},
		{"DOMAIN over DOMAIN over bytea", ir.Domain{
			Name:     "d_outer",
			BaseType: ir.Domain{Name: "d_inner", BaseType: ir.Blob{}},
		}},
		{"DOMAIN over bytea with CHECKs", ir.Domain{
			Name:     "d_chk",
			BaseType: ir.Blob{},
			Checks:   []ir.DomainCheck{{Name: "c", Body: "octet_length(VALUE) < 16"}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			col := retargetedShapeCompareColumn(t, "c", tc.src)
			switch col.Type.(type) {
			case ir.Binary, ir.Varbinary, ir.Blob:
				binaryOutputs++
			default:
				// columnIsNativelyBinary is only ever consulted from
				// prepareValue's binary arm, so a non-binary Type means
				// this row grades nothing and the roster has drifted.
				t.Fatalf("the retarget produced Type %s for %s; this row can never reach "+
					"columnIsNativelyBinary and is grading nothing", col.Type, tc.src)
			}
			if !columnIsNativelyBinary(col) {
				t.Errorf("columnIsNativelyBinary = false for a column the retarget flattened from %s "+
					"(Type %s, SourceColumnType %s).\n\n"+
					"The un-retargeted column answers TRUE, so the retarget's provenance INVERTED the "+
					"verdict: the writer will now store a PostgreSQL `\\x…` bytea rendering VERBATIM "+
					"instead of decoding it — `\\xdead` as 6 bytes instead of 2 — on the restore and "+
					"chain-restore lanes only, so the same cell disagrees with what migrate landed. "+
					"Either columnIsNativelyBinary stopped reading its field through ir.UnwrapDomain, "+
					"or a retarget rule started manufacturing a binary type",
					tc.src, col.Type, col.SourceColumnType)
			}
		})
	}
	// Anti-vacuity: at least the four DOMAIN-over-binary rows must have
	// actually produced a binary-typed column, or the loop above graded
	// only the bare cells this test was not written for.
	if binaryOutputs < 7 {
		t.Errorf("only %d roster rows produced a binary-typed column; this gate exists for the DOMAIN rows "+
			"and is no longer reaching them", binaryOutputs)
	}
}

// TestRetargetedDomainColumn_KeepsItsArrayElementFamily is the item-153
// / Bug 232 entanglement pin, over EVERY element family rather than a
// representative (the Bug 74 lesson — the defect is invisible on the 9
// families whose leaf policy does not dispatch on []byte).
//
// The shape-compare retarget rewrites `Domain{d AS X[]}` all the way to
// `ir.JSON{Binary: true}`, two hops, so the element family survives ONLY
// through SourceColumnType — and only if columnIsArrayLike and
// arrayElementType read that field through [ir.UnwrapDomain]. Before
// a44c0fa7 they did not, and flattening the retarget on top of that
// would have re-opened Bug 232 for every DOMAIN-wrapped array column on
// the restore lane, silently.
func TestRetargetedDomainColumn_KeepsItsArrayElementFamily(t *testing.T) {
	if len(mysqlArrayLeafRoster) < 20 {
		t.Fatalf("the derived element-family roster holds only %d entries; this matrix would grade almost "+
			"nothing", len(mysqlArrayLeafRoster))
	}

	byteShaped := 0
	for udt, entry := range mysqlArrayLeafRoster {
		if _, isBytes := entry.leaf.([]byte); isBytes {
			byteShaped++
		}
		domainCol := retargetedShapeCompareColumn(t, "c",
			ir.Domain{Name: "d_" + udt, BaseType: ir.Array{Element: entry.elem}})

		// The premise of the whole matrix: the wrapper AND the array are
		// both gone from Type. If either survives, the assertions below
		// are grading the pre-item-153 shape.
		if _, isJSON := domainCol.Type.(ir.JSON); !isJSON {
			t.Fatalf("%s: the shape-compare retarget left Type as %s; a DOMAIN over an array must flatten "+
				"to the JSON column MySQL actually holds", udt, domainCol.Type)
		}
		if !columnIsArrayLike(domainCol) {
			t.Errorf("%s: columnIsArrayLike = false for a retargeted DOMAIN-over-array column "+
				"(SourceColumnType %s) — the array provenance is unreachable, which is Bug 232 through "+
				"domains: the empty-array disambiguation lands `{}` instead of `[]`", udt, domainCol.SourceColumnType)
		}
		if got := arrayElementType(domainCol); got == nil {
			t.Errorf("%s: arrayElementType = nil for a retargeted DOMAIN-over-array column "+
				"(SourceColumnType %s) — the element family did not survive the flatten, and every "+
				"json[]/jsonb[]/bytea[] value on this column now takes the applier-lane refusal. "+
				"ir.UnwrapDomain has been removed from arrayElementType while the retarget still flattens",
				udt, domainCol.SourceColumnType)
			continue
		}

		for _, shape := range []struct {
			name string
			in   []any
		}{
			{"1-D", []any{entry.leaf}},
			{"multi-dim 2-D", []any{[]any{entry.leaf}, []any{entry.leaf}}},
			{"NULL element", []any{entry.leaf, nil}},
			{"empty", []any{}},
		} {
			t.Run(udt+"/"+shape.name, func(t *testing.T) {
				migrateCol := &ir.Column{Name: "c", Type: ir.Array{Element: entry.elem}}

				wantValue, wantOK, wantErr := convertArrayLikeToJSON(shape.in, migrateCol)
				if wantErr != nil {
					t.Fatalf("the MIGRATE lane refused %s %s: %v", udt, shape.name, wantErr)
				}
				gotValue, gotOK, gotErr := convertArrayLikeToJSON(shape.in, domainCol)
				if gotErr != nil {
					t.Fatalf("the retargeted DOMAIN column refused %s %s while migrate carried it as %v — "+
						"the element family did not survive the flatten (Bug 232 through domains): %v",
						udt, shape.name, wantValue, gotErr)
				}
				if gotOK != wantOK || gotValue != wantValue {
					t.Errorf("%s %s: retargeted DOMAIN column = %v (recognized=%v); migrate lane = %v "+
						"(recognized=%v). The same source column must store the same value on both lanes",
						udt, shape.name, gotValue, gotOK, wantValue, wantOK)
				}
			})
		}
	}

	if byteShaped < 2 {
		t.Errorf("only %d roster families carry a []byte leaf; json/jsonb and bytea are the families this "+
			"gate exists for and it is no longer reaching them", byteShaped)
	}
}

// TestEmitTableDef_FlatteningTheRetargetWouldDropTheDomainCheck is the
// EVIDENCE for the item-153 decision that [translate.RetargetForEngine]
// keeps the DOMAIN wrapper while [translate.RetargetForShapeCompare]
// flattens it — recorded as a gate rather than as prose in an ADR,
// because the argument is a claim about this emitter's behaviour and
// this is the one place it can be checked.
//
// A `CREATE DOMAIN … CHECK (…)` column reaching a MySQL 8.0.16+ target
// has its CHECK translated into an inline table CHECK, via [ir.DomainOf]
// — which reads `Column.Type`. Put the EMIT lanes (`restore`, `chain
// restore`, the broker's schema-delta replay, schema-forward) on the
// flattening pass and that constraint vanishes from the emitted DDL with
// no error and no WARN: a silent loss, strictly worse than the loud
// false-refusal item 153 fixes.
//
// Read the two halves together. The wrapper-keeping pass MUST emit the
// CHECK; the flattening pass MUST NOT. The second half is not a wish —
// it is what makes the first half load-bearing, and it is the assertion
// that fails if someone "simplifies" the two entry points back into one.
func TestEmitTableDef_FlatteningTheRetargetWouldDropTheDomainCheck(t *testing.T) {
	source := &ir.Schema{Tables: []*ir.Table{{
		Name: "gl_users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "pct", Type: ir.Domain{
				Name:     "percentage",
				BaseType: ir.Decimal{Precision: 5, Scale: 2},
				Checks: []ir.DomainCheck{{
					Name: "percentage_check",
					Body: `((VALUE >= (0)::numeric) AND (VALUE <= (100)::numeric))`,
				}},
			}},
		},
	}}}

	emit := func(t *testing.T, s *ir.Schema) string {
		t.Helper()
		stmt, err := emitTableDefWithDomainChecks(s.Tables[0], true)
		if err != nil {
			t.Fatalf("emitTableDefWithDomainChecks: %v", err)
		}
		return stmt
	}

	keptDDL := emit(t, translate.RetargetForEngine(source, "postgres", "mysql"))
	if !strings.Contains(keptDDL, "CHECK") {
		t.Errorf("RetargetForEngine's output emitted NO CHECK — the emit lanes rely on the DOMAIN wrapper "+
			"surviving this pass so ir.DomainOf can still find its constraints:\n%s", keptDDL)
	}

	flatDDL := emit(t, translate.RetargetForShapeCompare(source, "postgres", "mysql"))
	if strings.Contains(flatDDL, "CHECK") {
		t.Errorf("RetargetForShapeCompare's output emitted a CHECK; this test's whole premise is that it "+
			"CANNOT, which is why the emit lanes must not use it. If the emitter learned to read the "+
			"wrapper from SourceColumnType instead, ir.DomainOf's doc records why that was deliberately "+
			"not done and this decision needs re-taking:\n%s", flatDDL)
	}
}
