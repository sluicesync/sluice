// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The `--where` half of the generated-row-identity class (2026-08-08).
//
// [whereCDCFilter.narrowBefore] is the ONE re-narrowing site in the tree
// that is not inside an engine. A filtered table is streamed with an
// UN-narrowed before-image on purpose — the client-side predicate has to
// read every OLD column — so each engine reader's own identity guard is
// deliberately bypassed for exactly these tables, and this function
// re-narrows to the IR schema's PRIMARY KEY afterwards.
//
// That made it the class's blind spot from two directions at once: the
// Postgres reader never sees the narrowing, and the MySQL binlog
// reader's refusal sits on the arms that narrow, which this path does
// not take.
//
// The guard is on the EVIDENCE — a key column absent from the image —
// rather than on the SHAPE — a generated column in the PK. That is what
// keeps it from over-refusing a source that does carry the value, and it
// is checked in both directions below.

// genPKFilter builds a filter for `t` WHERE region='EU' whose PRIMARY KEY
// is (a, g) — the composite shape where losing one member leaves a
// NON-UNIQUE prefix rather than nothing.
func genPKFilter(t *testing.T) *whereCDCFilter {
	t.Helper()
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Integer{}},
			{Name: "g", Type: ir.Integer{}},
			{Name: "region", Type: ir.Varchar{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "a"}, {Column: "g"}}},
	}}}
	f, err := buildWhereCDCFilter(ir.ByteExactCollationResolver{}, map[string]string{"t": "region = 'EU'"}, schema, false)
	if err != nil {
		t.Fatalf("buildWhereCDCFilter: %v", err)
	}
	return f
}

// TestNarrowBefore_RefusesAPartialKey walks the narrowing decision family:
// {complete key, partial key, no key} × {matching casing, source casing},
// directly on the function.
func TestNarrowBefore_RefusesAPartialKey(t *testing.T) {
	f := genPKFilter(t)

	cases := []struct {
		name    string
		before  ir.Row
		wantErr bool
		wantOut ir.Row
		msgHas  []string
	}{
		{
			// A Vitess VStream before-image, or a PG 18 publication with
			// publish_generated_columns=stored: the key is whole, and the
			// guard must be invisible.
			name:    "the whole key is present",
			before:  ir.Row{"a": int64(1), "g": int64(10), "region": "EU"},
			wantOut: ir.Row{"a": int64(1), "g": int64(10)},
		},
		{
			// The Postgres pgoutput / MySQL binlog shape: g never arrives.
			name:    "one key column is missing — the over-delete shape",
			before:  ir.Row{"a": int64(1), "region": "EU"},
			wantErr: true,
			msgHas:  []string{`"g"`, "NON-UNIQUE prefix", "GENERATED"},
		},
		{
			name:    "every key column is missing",
			before:  ir.Row{"region": "EU"},
			wantErr: true,
			msgHas:  []string{`"a"`, `"g"`},
		},
		{
			// A present-but-NULL key entry is NOT a missing one: the
			// applier can bind it, and refusing would break a legitimate
			// nullable-unique identity. Only ABSENCE is the defect.
			name:    "a key column present with a nil value",
			before:  ir.Row{"a": int64(1), "g": nil, "region": "EU"},
			wantOut: ir.Row{"a": int64(1), "g": nil},
		},
		{
			// The row is keyed by the SOURCE's casing while pkCols is
			// lower-cased; the completeness test must be case-insensitive
			// or every mixed-case key would read as partial. The output
			// keeps the image's casing, because that is what the applier
			// binds.
			name:    "the image uses the source's column casing",
			before:  ir.Row{"A": int64(1), "G": int64(10), "Region": "EU"},
			wantOut: ir.Row{"A": int64(1), "G": int64(10)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := f.narrowBefore("DELETE", "public", "t", tc.before)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("unexpected refusal: %v", err)
				}
				if len(got) != len(tc.wantOut) {
					t.Fatalf("narrowed = %#v; want %#v", got, tc.wantOut)
				}
				for k, want := range tc.wantOut {
					if got[k] != want {
						t.Errorf("narrowed[%q] = %#v; want %#v", k, got[k], want)
					}
				}
				return
			}
			if err == nil {
				t.Fatalf("expected a refusal; got %#v — this is the silent over-delete cell", got)
			}
			coded, ok := sluicecode.FromError(err)
			if !ok || coded.Code != sluicecode.CodeCDCGeneratedPrimaryKey {
				t.Fatalf("refusal is not %s: %v", sluicecode.CodeCDCGeneratedPrimaryKey, err)
			}
			if coded.Hint == "" {
				t.Error("refusal carries no remedy hint")
			}
			for _, want := range tc.msgHas {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal does not mention %q:\n%s", want, err.Error())
				}
			}
		})
	}
}

// TestNarrowBefore_UnfilteredAndKeylessTablesAreUntouched are the two
// over-refusal controls that live outside the key-completeness question.
func TestNarrowBefore_UnfilteredAndKeylessTablesAreUntouched(t *testing.T) {
	f := genPKFilter(t)

	// A table with no `--where` has no pkCols entry, so the before-image
	// passes through verbatim — the reader already narrowed it.
	before := ir.Row{"x": int64(1)}
	got, err := f.narrowBefore("DELETE", "public", "other", before)
	if err != nil {
		t.Fatalf("an unfiltered table was refused: %v", err)
	}
	if len(got) != 1 || got["x"] != int64(1) {
		t.Errorf("an unfiltered table's before-image was narrowed: %#v", got)
	}

	// A nil image is the caller's own "no before-image" case, refused by
	// missingBeforeImage upstream with a different code; this function
	// must not shadow it.
	if got, err := f.narrowBefore("DELETE", "public", "t", nil); err != nil || got != nil {
		t.Errorf("nil before-image: got (%#v, %v); want (nil, nil)", got, err)
	}
}

// TestRoute_PartialKeyRefusedOnEveryNarrowingArm is the sibling-sweep
// step at the call-site level. route() narrows on THREE arms — DELETE,
// the in-scope UPDATE, and the move-OUT UPDATE that synthesises a DELETE
// — and a guard wired into two of three is the exact shape this project
// keeps paying for. The move-OUT arm is the one that matters most: it
// emits a DELETE the source never issued.
func TestRoute_PartialKeyRefusedOnEveryNarrowingArm(t *testing.T) {
	f := genPKFilter(t)
	partialIn := ir.Row{"a": int64(1), "region": "EU"}  // in scope, key incomplete
	partialOut := ir.Row{"a": int64(1), "region": "US"} // out of scope, key incomplete

	arms := []struct {
		name   string
		change ir.Change
	}{
		{"DELETE", ir.Delete{Schema: "public", Table: "t", Before: partialIn}},
		{"UPDATE staying in scope", ir.Update{Schema: "public", Table: "t", Before: partialIn, After: partialIn}},
		{"UPDATE moving out of scope", ir.Update{Schema: "public", Table: "t", Before: partialIn, After: partialOut}},
	}
	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			emit, err := f.route(arm.change)
			if err == nil {
				t.Fatalf("arm not guarded: route emitted %#v with no refusal", emit)
			}
			coded, ok := sluicecode.FromError(err)
			if !ok || coded.Code != sluicecode.CodeCDCGeneratedPrimaryKey {
				t.Fatalf("refusal is not %s: %v", sluicecode.CodeCDCGeneratedPrimaryKey, err)
			}
		})
	}

	// The move-IN arm INSERTs the after-image whole and never narrows, so
	// it must NOT be refused — an over-refusal there would drop a row the
	// target legitimately needs.
	emit, err := f.route(ir.Update{Schema: "public", Table: "t", Before: partialOut, After: partialIn})
	if err != nil {
		t.Fatalf("the move-IN arm does not narrow and must not refuse: %v", err)
	}
	if len(emit) != 1 {
		t.Fatalf("move-IN emitted %d changes; want 1", len(emit))
	}
	if _, ok := emit[0].(ir.Insert); !ok {
		t.Fatalf("move-IN emitted %T; want ir.Insert", emit[0])
	}
}

// whereCDCEngineRoster is the enumeration the audit entry asked for:
// which engines actually reach [whereCDCFilter.narrowBefore] with a
// generated row identity. `sync --where` is gated on
// [ir.FilteredCDCPreflighter] (preflightRowFilters), so the population is
// exactly the engines pinning that interface — mysql and postgres today,
// six CDC readers between them minus the ones that cannot get here.
//
// The value is the reason each entry is or is not the class's carrier.
var whereCDCEngineRoster = map[string]string{
	"mysql": "REACHES IT, three readers, two verdicts. The BINLOG reader's decoder drops generated " +
		"columns, so a filtered table's un-narrowed before-image arrives without the key and " +
		"narrowBefore refuses — the binlog reader's own refuseGeneratedPKIdentity cannot, because it " +
		"sits on the narrowing arms this path bypasses. The two VSTREAM readers transmit the generated " +
		"value, so the key is whole and narrowBefore passes: that is the over-refusal control, and it " +
		"is why the guard keys on the value's ABSENCE rather than on the column being generated.",

	"postgres": "REACHES IT. pgoutput does not publish a generated column before PG 18, so the " +
		"before-image never carries it and narrowBefore refuses. The reader refuses first at its own " +
		"RelationMessage (refuseUnpublishedGeneratedIdentity), so in practice a filtered PG sync stops " +
		"there — this is the floor behind it, and the only guard on a warm resume attached to a stream " +
		"an older build started.",
}

// TestWhereCDCEngineRosterMatchesTheFilteredSyncGate binds the
// enumeration above to the code rather than leaving it as prose: the
// roster's keys must be exactly the engine packages that pin
// ir.FilteredCDCPreflighter, which is what gates `sync --where`. An
// engine that gains filtered sync fails this until someone records
// whether it carries the class.
func TestWhereCDCEngineRosterMatchesTheFilteredSyncGate(t *testing.T) {
	found, err := packagesPinningFilteredCDCPreflighter(filepath.Join("..", "engines"))
	if err != nil {
		t.Fatalf("walk engine sources: %v", err)
	}
	if len(found) == 0 {
		t.Fatal("found no ir.FilteredCDCPreflighter pins at all — the walker is broken, not the tree")
	}
	got := sortedSetKeys(found)
	want := sortedRosterKeys(whereCDCEngineRoster)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf(
			"the `sync --where` engine set is %v but whereCDCEngineRoster covers %v.\n"+
				"narrowBefore re-narrows for EVERY engine that reaches it, so a new one has to be "+
				"classified — does its change stream carry a GENERATED identity column, or not?",
			got, want,
		)
	}
	for pkg, reason := range whereCDCEngineRoster {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("%s: roster entry has no reason; the reason IS the gate", pkg)
		}
	}
}

// packagesPinningFilteredCDCPreflighter returns the engine directories
// whose non-test sources carry a `var _ ir.FilteredCDCPreflighter = …`
// compile-time assertion — the mechanical answer to "which engines
// support `sync --where`", per the working agreement that prefers a
// capability pin to reasoning about which engines lack one.
func packagesPinningFilteredCDCPreflighter(root string) (map[string]bool, error) {
	out := map[string]bool{}
	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return err
		}
		pkgDir := filepath.Base(filepath.Dir(path))
		ast.Inspect(file, func(n ast.Node) bool {
			spec, ok := n.(*ast.ValueSpec)
			if !ok || spec.Type == nil {
				return true
			}
			sel, ok := spec.Type.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "FilteredCDCPreflighter" {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "ir" {
				out[pkgDir] = true
			}
			return true
		})
		return nil
	})
	return out, err
}

func sortedSetKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedRosterKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
