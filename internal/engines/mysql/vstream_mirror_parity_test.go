// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The mirror-parity gate for audit 2026-07-26 SL-12.
//
// vstreamCDCReader and vstreamSnapshotStream carry HAND-MIRRORED dispatch
// methods. Hand-mirrored code has one predictable failure: a fix lands on the
// copy that was debugged and not on the twin. That is exactly what happened
// with the post-DDL FIELD-cache wedge — the CLASSIFICATION half of the fix was
// mirrored, the RECOVERY half was not, so the cold-start CDC tail still wiped
// every table's cached FIELDs on any DDL anywhere in the keyspace.
//
// A comment saying "keep these in sync" is what was already there. This is the
// version that fails.
package mysql

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStreamDispatchTwinsShareTheirRecoveryHelpers asserts that helpers whose
// absence caused a silent divergence exist as methods on BOTH types. It is
// name-based on purpose: it cannot verify the bodies agree, but it does catch
// the shape the real defect took — one type gaining a helper the other never
// got.
func TestVStreamDispatchTwinsShareTheirRecoveryHelpers(t *testing.T) {
	// Helpers that MUST exist on both dispatch types. Add to this list
	// whenever a fix introduces a helper on one twin.
	required := []string{
		"invalidateFieldsForDDL",
	}
	const (
		readerType   = "vstreamCDCReader"
		snapshotType = "vstreamSnapshotStream"
	)

	fset := token.NewFileSet()
	methods := map[string]map[string]bool{
		readerType:   {},
		snapshotType: {},
	}
	for _, file := range []string{"cdc_vstream.go", "cdc_vstream_snapshot.go"} {
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			id, ok := star.X.(*ast.Ident)
			if !ok {
				continue
			}
			if m, tracked := methods[id.Name]; tracked {
				m[fn.Name.Name] = true
			}
		}
	}

	// Anti-vacuity: if the type names change, the maps stay empty and every
	// assertion below passes trivially.
	if len(methods[readerType]) < 5 || len(methods[snapshotType]) < 5 {
		t.Fatalf("found %d methods on %s and %d on %s — the parse is not finding the dispatch types, so this "+
			"gate is vacuous; re-point it",
			len(methods[readerType]), readerType, len(methods[snapshotType]), snapshotType)
	}

	for _, name := range required {
		for _, typ := range []string{readerType, snapshotType} {
			if !methods[typ][name] {
				other := readerType
				if typ == readerType {
					other = snapshotType
				}
				t.Errorf("%s has no %s method while %s does. These dispatch paths are hand-mirrored, so a fix "+
					"applied to one and not the other silently diverges the cold-start CDC tail from the steady-state "+
					"reader — the shape of audit SL-12.", typ, name, other)
			}
		}
	}
}

// TestVStreamSnapshotDDLDoesNotBlanketClear pins the specific behaviour: an
// attributable DDL must invalidate ONLY its target table's entries.
func TestVStreamSnapshotDDLDoesNotBlanketClear(t *testing.T) {
	s := &vstreamSnapshotStream{
		fields: map[string][]*query.Field{
			"-/ks.orders":   nil,
			"-/ks.users":    nil,
			"0-80/ks.users": nil,
		},
	}
	s.invalidateFieldsForDDL("CREATE TABLE ks.audit (id BIGINT PRIMARY KEY)")
	if len(s.fields) != 3 {
		t.Errorf("a CREATE TABLE invalidated %d of 3 cached entries; it cannot change an existing table's shape, "+
			"and wiping the cache is what made the next ROW event on a long-established table trip the loud floor",
			3-len(s.fields))
	}

	s.invalidateFieldsForDDL("ALTER TABLE ks.users ADD COLUMN tier INT")
	if _, ok := s.fields["-/ks.orders"]; !ok {
		t.Error("an ALTER on users dropped the cached fields for orders")
	}
	if _, ok := s.fields["-/ks.users"]; ok {
		t.Error("an ALTER on users did NOT drop its own cached fields")
	}
	if _, ok := s.fields["0-80/ks.users"]; ok {
		t.Error("an ALTER on users left a sibling SHARD's entry cached; the shape changes on every shard")
	}

	// An unattributable statement must fall back to the blanket clear rather
	// than leave a stale entry behind.
	s2 := &vstreamSnapshotStream{fields: map[string][]*query.Field{"-/ks.orders": nil}}
	s2.invalidateFieldsForDDL("SOMETHING THE PARSER CANNOT ATTRIBUTE")
	if len(s2.fields) != 0 {
		t.Error("an unattributable DDL must degrade to the blanket clear (a stale entry risks a wrong decode)")
	}
}

// TestVStreamSnapshotDispatchCDCDDL_UsesScopedInvalidation drives the REAL
// dispatch path rather than the helper.
//
// The first version of the test above called invalidateFieldsForDDL directly,
// and reverting the fix at its call site left it green — it pinned the
// FUNCTION, not the PATH that reaches it. That is the same defect class this
// whole audit item belongs to, reproduced in its own test. Nothing but going
// through dispatchCDCDDL proves the helper is actually wired in.
func TestVStreamSnapshotDispatchCDCDDL_UsesScopedInvalidation(t *testing.T) {
	s := &vstreamSnapshotStream{
		keyspace: "ks",
		fields: map[string][]*query.Field{
			"-/ks.orders": nil,
			"-/ks.users":  nil,
		},
	}
	out := make(chan ir.Change, 4)
	ev := &binlogdata.VEvent{
		Type:      binlogdata.VEventType_DDL,
		Keyspace:  "ks",
		Statement: "CREATE TABLE ks.audit (id BIGINT PRIMARY KEY)",
	}
	if err := s.dispatchCDCDDL(context.Background(), ev, out); err != nil {
		t.Fatalf("dispatchCDCDDL: %v", err)
	}
	if len(s.fields) != 2 {
		t.Errorf("dispatchCDCDDL wiped %d of 2 cached field sets for a CREATE TABLE. A CREATE cannot change an "+
			"existing table's shape, so the next ROW event on a long-established table now trips the loud floor — "+
			"the wedge the reader-side fix removed and this twin did not get (audit SL-12).", 2-len(s.fields))
	}
}
