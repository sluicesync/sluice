// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 232 — the RESTORE lane's array column, and the two refusals that
// used to be one.
//
// `sluice restore` of a PG-sourced backup into a MySQL-family target
// runs the manifest schema through translate.RetargetForEngine before
// handing it to this writer. That rewrite turned `ir.Array{Element: X}`
// into `ir.JSON{Binary: true}` and dropped X, so arrayElementType
// returned nil and every json[] / jsonb[] / bytea[] value took the
// applier-lane refusal: SLUICE-E-VALUE-UNREPRESENTABLE, 0 rows, on
// chains taken by ANY release. The element was in the manifest all
// along.
//
// The fix records the pre-rewrite type (internal/translate), so this
// file's job is to BIND the two halves: it builds its restore-lane
// column by running the REAL retarget, not by hand-spelling the shape it
// produces. Two separately-pinned facts with nothing joining them is the
// gap CLAUDE.md's premise-naming step names; running the real pass is
// what closes it here, and
// TestRestore_PGToMySQL_ArrayElementFamiliesMatchMigrate closes it
// against real servers.

package mysql

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
	"sluicesync.dev/sluice/internal/translate"
)

// retargetedRestoreColumn returns the column descriptor a cross-engine
// `restore` into MySQL hands prepareValue for a PG array column: the
// manifest's source-native IR, put through the same
// translate.RetargetForEngine call internal/pipeline/backup's Restore
// makes at step 3 of its run.
func retargetedRestoreColumn(t *testing.T, name string, elem ir.Type) *ir.Column {
	t.Helper()
	out := translate.RetargetForEngine(&ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: name, Type: ir.Array{Element: elem}}},
	}}}, "postgres", "mysql")
	col := out.Tables[0].Columns[0]
	// The shape this whole file is about: MySQL has no array type, so the
	// retarget legitimately rewrites Type to JSON. If it ever stops doing
	// that, the parity below would be comparing a column against itself.
	if _, isJSON := col.Type.(ir.JSON); !isJSON {
		t.Fatalf("the retarget no longer rewrites an array column to JSON (Type = %#v); this test would "+
			"be comparing the migrate lane against itself", col.Type)
	}
	return col
}

// TestArrayLeafForJSON_RetargetedRestoreColumnMatchesTheMigrateLane is
// the parity gate Bug 232 asked for: the SAME source column must land
// the SAME stored value through `migrate` (which hands the writer the
// source's own ir.Array) and through `backup` + `restore` (which hands
// it the retargeted column).
//
// It runs EVERY element family the Postgres reader can decode — the
// derived roster mysqlArrayLeafRoster, not a representative — × the four
// shapes an array value takes. The Bug 74 lesson applies exactly: the
// defect was invisible on 8 of the 11 families, because only the two
// []byte-leaf ones dispatch on the element type at all.
//
// Scope: it grades the WRITER's leaf policy for a retargeted column. It
// does not exercise the backup encoder or the chunk reader; the
// integration sibling named in this file's header does.
func TestArrayLeafForJSON_RetargetedRestoreColumnMatchesTheMigrateLane(t *testing.T) {
	// Anti-vacuity: the roster is derived from the postgres reader, so a
	// derivation that broke would compare an empty matrix and pass.
	if len(mysqlArrayLeafRoster) < 20 {
		t.Fatalf("the derived element-family roster holds only %d entries; this matrix would grade almost "+
			"nothing", len(mysqlArrayLeafRoster))
	}

	byteShaped := 0
	for udt, entry := range mysqlArrayLeafRoster {
		if _, isBytes := entry.leaf.([]byte); isBytes {
			byteShaped++
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
				restoreCol := retargetedRestoreColumn(t, "c", entry.elem)

				wantValue, wantOK, wantErr := convertArrayLikeToJSON(shape.in, migrateCol)
				if wantErr != nil {
					t.Fatalf("the MIGRATE lane refused %s %s: %v", udt, shape.name, wantErr)
				}
				gotValue, gotOK, gotErr := convertArrayLikeToJSON(shape.in, restoreCol)
				if gotErr != nil {
					t.Fatalf("the RESTORE lane refused %s %s while migrate carried it as %v — the element "+
						"family did not survive the cross-engine retarget (Bug 232): %v",
						udt, shape.name, wantValue, gotErr)
				}
				if gotOK != wantOK || gotValue != wantValue {
					t.Errorf("%s %s: restore lane = %v (recognized=%v); migrate lane = %v (recognized=%v). "+
						"The same source column must store the same value through both lanes",
						udt, shape.name, gotValue, gotOK, wantValue, wantOK)
				}
			})
		}
	}

	// Anti-vacuity on the half that actually regressed: if no family in
	// the roster has a []byte leaf any more, this matrix cannot see the
	// defect it was written for.
	if byteShaped < 2 {
		t.Errorf("only %d roster families carry a []byte leaf; json/jsonb and bytea are the families this "+
			"gate exists for and it is no longer reaching them", byteShaped)
	}
}

// TestArrayLeafForJSON_ArrayColumnWithNoElementDoesNotClaimTheApplierLane
// pins the second half of the fix: WHICH lane the refusal says it is, is
// now derived from the column rather than asserted by the text.
//
// A column with array provenance came from a lane holding a real schema
// (migrate, a sync cold copy, a restore's bulk load). Telling that
// operator they are in "the continuous-sync lane" and handing them
// `sluice migrate` — which cannot read an archive — is precisely what
// v0.116.0 printed during a restore.
func TestArrayLeafForJSON_ArrayColumnWithNoElementDoesNotClaimTheApplierLane(t *testing.T) {
	// An ir.Array whose Element is nil: nothing sluice ships produces
	// one, which is the point — the arm exists so the applier's message
	// cannot be reached by a lane that has a schema.
	for _, col := range []*ir.Column{
		{Name: "c", Type: ir.Array{}},
		{Name: "c", Type: ir.JSON{Binary: true}, SourceColumnType: ir.Array{}},
	} {
		_, _, err := convertArrayLikeToJSON([]any{[]byte{0xde, 0xad}}, col)
		if err == nil {
			t.Fatal("an array column with no element type was accepted; the encoding was guessed")
		}
		for _, forbidden := range []string{"continuous-sync lane", "CHANGE-APPLIER lane", "sluice migrate"} {
			if strings.Contains(err.Error(), forbidden) {
				t.Errorf("a SCHEMA-carrying lane's refusal names %q:\n%v", forbidden, err)
			}
		}
		for _, want := range []string{`column "c"`, "IS declared as an array", "--exclude-table", "report it"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the schema-lane refusal is missing %q:\n%v", want, err)
			}
		}
		var coded *sluicecode.CodedError
		if !errors.As(err, &coded) || coded.Code != sluicecode.CodeValueUnrepresentable {
			t.Errorf("refusal is not a %s error: %v", sluicecode.CodeValueUnrepresentable, err)
		}
	}
}

// TestArrayLeafForJSON_ApplierRefusalNeverPrescribesMigrateForARestore
// is the narrow reading of Bug 232's second half: whatever else the
// applier-lane refusal says, an operator holding only a backup must not
// be told to run a command that cannot read one.
//
// The applier lane is reached by a column with NO array provenance,
// which is what MySQL's own schema reader produces — the property
// TestArrayLeafForJSON_NoMySQLReaderReconstructsAnArray pins.
func TestArrayLeafForJSON_ApplierRefusalNeverPrescribesMigrateForARestore(t *testing.T) {
	targetCol := &ir.Column{Name: "c", Type: ir.JSON{Binary: true}}
	_, _, err := convertArrayLikeToJSON([]any{[]byte(`{"a":1}`)}, targetCol)
	if err == nil {
		t.Fatal("no refusal on the applier lane")
	}
	msg := err.Error()
	// It may name `sluice migrate` — that IS the remedy for a continuous
	// sync — but only as one of the SOURCE-schema lanes, so a reader on a
	// chain replay sees restore named too rather than being sent to a
	// command that cannot open an archive.
	if strings.Contains(msg, "sluice migrate") && !strings.Contains(msg, "restore") {
		t.Errorf("the applier refusal prescribes `sluice migrate` without naming the restore lane; a "+
			"chain restore's incremental replay reaches this message and its operator has only an "+
			"archive:\n%v", err)
	}
	if strings.Contains(msg, "this is the continuous-sync lane") {
		t.Errorf("the refusal still ASSERTS the sync lane; a chain restore's and the from-backup "+
			"broker's incremental replay reach it too:\n%v", err)
	}
}
