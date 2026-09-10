// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Neki detection is a FIELD on four different types now — RowReader,
// RowWriter, ChangeApplier and SchemaWriter — and each one gates different
// behaviour: declining the raw-copy lane, the shard-key statement shaping,
// the CDC applier's refusals, and whether sequence creation may use a
// transaction. Every one of them is invisible when it is wrong: an unset
// `isNeki` does not fail, it silently restores the pre-Neki behaviour that
// the field exists to change.
//
// The SchemaWriter case is why this gate exists. It was the fourth type to
// need the flag, and it was added because a PlanetScale Postgres to Neki
// migration failed at sequence priming — `BEGIN; CREATE SEQUENCE s; setval(s);
// COMMIT` gets `relation "s" does not exist` on Neki, because DDL inside a
// transaction is not visible to the rest of that transaction. If a later
// refactor drops the wiring at the constructor, that failure comes straight
// back and nothing in the unit suite notices.
//
// So: every struct type in this package that DECLARES an isNeki field must
// have it ASSIGNED at least once in a composite literal. The universe is
// derived from the AST, so a fifth type is covered the day it is added.
func TestEveryNekiFlagIsWiredAtConstruction(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()

	declares := map[string]string{} // type name -> file that declares the field
	assigns := map[string]bool{}    // type name -> assigned in some composite literal

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			// Declaration: `type X struct { … isNeki bool … }`
			if ts, ok := n.(*ast.TypeSpec); ok {
				st, ok := ts.Type.(*ast.StructType)
				if !ok || st.Fields == nil {
					return true
				}
				for _, fld := range st.Fields.List {
					for _, id := range fld.Names {
						if id.Name == "isNeki" {
							declares[ts.Name.Name] = name
						}
					}
				}
				return true
			}
			// Assignment: `&X{ … isNeki: v … }`
			if cl, ok := n.(*ast.CompositeLit); ok {
				id, ok := cl.Type.(*ast.Ident)
				if !ok {
					return true
				}
				for _, el := range cl.Elts {
					kv, ok := el.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "isNeki" {
						assigns[id.Name] = true
					}
				}
			}
			return true
		})
	}

	// Anti-vacuity: four types carry the flag today. A walker that finds
	// fewer has stopped reaching the code, and would pass by finding nothing.
	if len(declares) < 4 {
		t.Fatalf("found only %d types declaring an isNeki field (%v); the walker is not reaching the "+
			"package and this gate would pass vacuously", len(declares), declares)
	}

	for typeName, file := range declares {
		if !assigns[typeName] {
			t.Errorf("%s declares an isNeki field (%s) but nothing ever assigns it in a composite literal — "+
				"the flag is permanently false, which silently restores the pre-Neki behaviour it exists to "+
				"change, with no error anywhere", typeName, file)
		}
	}
}
