// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The emitted-name roster (audit 2026-09-15 A0915-MYSQL-HIGH-1).
//
// On a folding server (lower_case_table_names != 0) every name this
// engine's CDC lanes emit must be the spelling the SERVER stores, because
// the pipeline's dispatch filter and the target-side appliers match it
// byte-exactly against the names the ROW events carry. Row events take
// their names from the binlog's Table_map (or VStream's FIELD event),
// which is the stored spelling by construction. Anything built from
// QUERY TEXT is the operator's spelling and must be folded at the emit
// site — RC-1b folded the admission compare and left the one text-sourced
// payload, the TRUNCATE arm, raw: a `TRUNCATE TABLE T1` was skipped on a
// case-sensitive target and applied to an EXCLUDED table on a folding one.
//
// This gate derives its universe from the AST: every composite literal
// of a table-bearing ir.Change type in the non-test files of this
// package, keyed file:Receiver.Method:ir.Type, must be classified in the
// roster below, fail-by-default. A site classified as folded-at-emit is
// additionally checked MECHANICALLY: its Schema and Table values must be
// identifiers assigned in the same function from a call to
// truncateEmitNames, so a site that keeps the classification and drops
// the call fails here as well as in the real-server pin.
//
// WHAT THIS GATE REACHES: the construction sites and, for the folded
// class, the presence of the fold call. It does not grade what the fold
// does — TestTruncateEmitNames_FollowsTheServersFold and
// TestCDCReader_EmittedTruncateNameFollowsTheServersFold own that.

package mysql

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type emittedNameClass int

const (
	// nameFromTableMap — the binlog Table_map event: the server's stored
	// spelling by construction. No fold needed.
	nameFromTableMap emittedNameClass = iota
	// nameFromVStreamEvent — a VStream FIELD/ROW event: the spelling
	// Vitess's schema tracker reports. No fold needed.
	nameFromVStreamEvent
	// nameFoldedAtEmit — query text, folded through truncateEmitNames
	// before the literal is built. Verified mechanically below.
	nameFoldedAtEmit
	// nameFromQueryTextExempt — query text emitted unfolded, with a
	// stated reason. The reason is the record; keep it honest.
	nameFromQueryTextExempt
)

type emittedNameEntry struct {
	class  emittedNameClass
	reason string
}

// emittedChangeNameRoster classifies every table-bearing ir.Change
// construction site in this package. A new site fails the build until it
// is classified here.
var emittedChangeNameRoster = map[string]emittedNameEntry{
	// Binlog lane: the row and boundary events take the name from the
	// Table_map-backed tableSchema; the TRUNCATE arm is the one
	// text-sourced payload and is folded (the A0915-MYSQL-HIGH-1 fix).
	"cdc_reader.go:CDCReader.dispatch:ir.Truncate":                    {nameFoldedAtEmit, "TRUNCATE parsed from QUERY_EVENT text; folded through truncateEmitNames"},
	"cdc_reader.go:CDCReader.dispatchRows:ir.Insert":                  {nameFromTableMap, "tableSchema resolved from the Table_map's qualified name"},
	"cdc_reader.go:CDCReader.dispatchRows:ir.Update":                  {nameFromTableMap, "tableSchema resolved from the Table_map's qualified name"},
	"cdc_reader.go:CDCReader.dispatchRows:ir.Delete":                  {nameFromTableMap, "tableSchema resolved from the Table_map's qualified name"},
	"cdc_reader.go:CDCReader.maybeSnapshotSchemaB1:ir.SchemaSnapshot": {nameFromTableMap, "tableSchema re-resolved from the Table_map's qualified name after the DDL"},

	// VStream lane (two hand-mirrored readers): rows and boundaries from
	// FIELD/ROW events; the TRUNCATE arms parse DDL text and are emitted
	// UNFOLDED. Stated, not implied: this lane never reads
	// lower_case_table_names (no lowerCaseTableNames on either reader) and its
	// keyspace-bound scope compare is byte-exact too, so a fold here would
	// be a rule the lane has no evidence for. The names it emits for a
	// TRUNCATE follow whatever case the operator spelled — the same
	// residual its keyspace compare already carries. Not measured on a
	// Vitess cluster in this change (out of scope, ZERO cloud); a folding
	// vttablet is derived-not-verified as a gap.
	"cdc_vstream.go:vstreamCDCReader.dispatchDDL:ir.Truncate":                                {nameFromQueryTextExempt, "VStream DDL text; the lane has no fold knowledge (no lower_case_table_names read) — unfolded by design, gap stated above"},
	"cdc_vstream.go:vstreamCDCReader.maybeSnapshotSchema:ir.SchemaSnapshot":                  {nameFromVStreamEvent, "FIELD event's keyspace/table"},
	"cdc_vstream.go:vstreamCDCReader.dispatchRow:ir.Insert":                                  {nameFromVStreamEvent, "ROW event's keyspace/table"},
	"cdc_vstream.go:vstreamCDCReader.dispatchRow:ir.Update":                                  {nameFromVStreamEvent, "ROW event's keyspace/table"},
	"cdc_vstream.go:vstreamCDCReader.dispatchRow:ir.Delete":                                  {nameFromVStreamEvent, "ROW event's keyspace/table"},
	"cdc_vstream_snapshot.go:vstreamSnapshotStream.dispatchCDCDDL:ir.Truncate":               {nameFromQueryTextExempt, "VStream DDL text on the snapshot-then-CDC stream; same posture as vstreamCDCReader.dispatchDDL, hand-mirrored"},
	"cdc_vstream_snapshot.go:vstreamSnapshotStream.maybeSnapshotSchemaCDC:ir.SchemaSnapshot": {nameFromVStreamEvent, "FIELD event's keyspace/table"},
	"cdc_vstream_snapshot.go:vstreamSnapshotStream.dispatchCDCRow:ir.Insert":                 {nameFromVStreamEvent, "ROW event's keyspace/table"},
	"cdc_vstream_snapshot.go:vstreamSnapshotStream.dispatchCDCRow:ir.Update":                 {nameFromVStreamEvent, "ROW event's keyspace/table"},
	"cdc_vstream_snapshot.go:vstreamSnapshotStream.dispatchCDCRow:ir.Delete":                 {nameFromVStreamEvent, "ROW event's keyspace/table"},
}

// tableBearingChangeTypes are the ir.Change types that carry a table
// name (TxBegin/TxCommit carry none and are not graded).
var tableBearingChangeTypes = map[string]bool{"Insert": true, "Update": true, "Delete": true, "Truncate": true, "SchemaSnapshot": true}

func TestEmittedChangeNamesFollowTheServersFold(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	found := map[string]bool{}
	byClass := map[emittedNameClass]int{}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(src), "ir.") {
			continue
		}
		f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				typ := irChangeTypeName(lit.Type)
				if typ == "" || !tableBearingChangeTypes[typ] {
					return true
				}
				key := fmt.Sprintf("%s:%s:ir.%s", path, funcQualifiedName(fn), typ)
				where := fset.Position(lit.Pos())
				found[key] = true
				entry, classified := emittedChangeNameRoster[key]
				if !classified {
					t.Errorf("%s: ir.%s built here is not in emittedChangeNameRoster (key %q). Classify it: a name from "+
						"the Table_map / VStream event needs no fold; a name from QUERY TEXT must be folded through "+
						"truncateEmitNames on a folding server, or exempted with a reason (A0915-MYSQL-HIGH-1)",
						where, typ, key)
					return true
				}
				byClass[entry.class]++
				if len(strings.TrimSpace(entry.reason)) < 20 {
					t.Errorf("%s: roster entry %q carries no real reason (%q)", where, key, entry.reason)
				}
				if entry.class == nameFoldedAtEmit {
					for _, field := range []string{"Schema", "Table"} {
						if !fieldAssignedFromCall(lit, fn, field, "truncateEmitNames") {
							t.Errorf("%s: %s is classified folded-at-emit but its %s value is not an identifier assigned from "+
								"truncateEmitNames in %s — the fold has been dropped or bypassed (A0915-MYSQL-HIGH-1)",
								where, key, field, funcQualifiedName(fn))
						}
					}
				}
				return true
			})
		}
	}
	// Stale entries: a roster row for a site that no longer exists is a
	// gate that reads as broader than the truth.
	var stale []string
	for key := range emittedChangeNameRoster {
		if !found[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	for _, key := range stale {
		t.Errorf("roster entry %q names no construction site in this package any more; remove or re-key it", key)
	}
	// Anti-vacuity: the binlog lane's four Table_map sites and its one
	// folded site, plus the VStream lane's rows. A scan that finds fewer
	// has broken, not passed.
	if byClass[nameFoldedAtEmit] < 1 {
		t.Fatalf("no construction site is classified folded-at-emit; the TRUNCATE arm must be — the scan or the "+
			"roster broke (found %d sites)", len(found))
	}
	if byClass[nameFromTableMap] < 4 {
		t.Fatalf("only %d Table_map-sourced sites found; the binlog lane has at least 4", byClass[nameFromTableMap])
	}
	if len(found) < 10 {
		t.Fatalf("found only %d table-bearing ir.Change construction sites; expected at least 10", len(found))
	}
}

// irChangeTypeName returns X for a composite literal typed ir.X, else "".
func irChangeTypeName(expr ast.Expr) string {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "ir" {
		return ""
	}
	return sel.Sel.Name
}

// funcQualifiedName renders Receiver.Method (or Func) for the roster key.
func funcQualifiedName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return fn.Name.Name
	}
	recv := ""
	switch e := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := e.X.(*ast.Ident); ok {
			recv = id.Name
		}
	case *ast.Ident:
		recv = e.Name
	}
	return recv + "." + fn.Name.Name
}

// fieldAssignedFromCall reports whether the literal's named field is an
// identifier that, somewhere in fn's body, is assigned (as one of the
// left-hand sides) from a call whose method name is callee.
func fieldAssignedFromCall(lit *ast.CompositeLit, fn *ast.FuncDecl, field, callee string) bool {
	var ident string
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if k, ok := kv.Key.(*ast.Ident); ok && k.Name == field {
			if v, ok := kv.Value.(*ast.Ident); ok {
				ident = v.Name
			}
		}
	}
	if ident == "" {
		return false
	}
	assigned := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Rhs) != 1 {
			return true
		}
		call, ok := as.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != callee {
			return true
		}
		for _, lhs := range as.Lhs {
			if id, ok := lhs.(*ast.Ident); ok && id.Name == ident {
				assigned = true
			}
		}
		return true
	})
	return assigned
}
