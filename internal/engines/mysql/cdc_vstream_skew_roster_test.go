// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// vstreamSkewRoster classifies every function in the tree that builds a
// vtgate.VStreamFlags literal (VSTREAM-TAIL-SKEW-HOLD, Bug 334), keyed by
// package, receiver type and function name. A COPY builder must set
// MinimizeSkew: vstreamCopyMinimizeSkew. A CDC builder must set
// MinimizeSkew: vstreamCDCMinimizeSkew(<recv>.preserveSkew) or, on the
// steady-state reader whose field is the inverse, vstreamCDCMinimizeSkew(!<recv>.relaxSkew),
// where <recv> is the builder's own receiver: the operator's setting on the
// stream being opened, not a constant and not another stream's. A new builder
// fails the roster until it is classified here.
var vstreamSkewRoster = map[string]string{
	// The snapshot stream's first request: COPY. On the single-stream path
	// it continues as the tail only when the CDC policy is also
	// MinimizeSkew=true; otherwise startPump reopens (cdcHandoffReopenReason).
	"mysql.(Engine).openVStreamSnapshotStreamFrom":        "COPY",
	"mysql.(*vstreamSnapshotStream).reopenForTableSeeded": "COPY", // auto-shard per-table COPY
	"mysql.(*copyStream).openStreamForTable":              "COPY", // ADR-0099 concurrent COPY
	"mysql.(*vstreamSnapshotStream).reopenForCDC":         "CDC",  // the copy→CDC handoff tail
	"mysql.(*vstreamSnapshotStream).openReshardCDCStream": "CDC",  // the snapshot stream's reshard tail
	"mysql.(*vstreamCDCReader).buildVStreamRequest":       "CDC",  // the steady-state reader (and its reshard reopen)
}

// cdcSkewFields is, per receiver type, the field a CDC builder must pass
// vstreamCDCMinimizeSkew, and whether it is negated (the reader stores the
// inverse, relaxSkew).
var cdcSkewFields = map[string]struct {
	field   string
	negated bool
}{
	"*vstreamSnapshotStream": {field: "preserveSkew"},
	"*vstreamCDCReader":      {field: "relaxSkew", negated: true},
}

// TestVStreamRequestSkewRoster_EveryBuilderClassified holds every VStream
// request the code builds to ADR-0120's skew policy.
//
// REACH: every non-test .go file under internal/ and cmd/, parsed (not
// type-checked), so a literal is recognized by its type's selector name
// (VStreamFlags / VStreamRequest), whatever the import alias. It fails on:
//   - a VStreamFlags literal in an unclassified function, or outside any
//     function (a package-level var cannot be classified);
//   - a classified literal whose MinimizeSkew is missing, a literal, or not
//     the value its class requires (for CDC: the builder's own receiver
//     field, see vstreamSkewRoster);
//   - any assignment to a .MinimizeSkew field (set it in the literal, where
//     the roster can see it);
//   - a VStreamRequest literal with no Flags, or Flags that are not an
//     inline VStreamFlags literal (vtgate would default MinimizeSkew and
//     StopOnReshard to false, unclassified).
//
// It does not reach requests built by reflection, by a helper in a test
// file, or flags built field by field through a non-selector path (none
// exist). Floor: every roster entry must be found, so a rename cannot leave
// it passing vacuously.
func TestVStreamRequestSkewRoster_EveryBuilderClassified(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	found := map[string]bool{}
	var problems []string
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fset := token.NewFileSet()
			file, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return perr
			}
			problems = append(problems, skewRosterFile(fset, file, found)...)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	for name := range vstreamSkewRoster {
		if !found[name] {
			problems = append(problems, "roster entry "+name+" builds no VStreamFlags any more (renamed or removed?): the roster would pass vacuously for it")
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Error(p)
	}
}

// skewRosterFile checks one parsed file and records the roster entries it
// builds in found.
func skewRosterFile(fset *token.FileSet, file *ast.File, found map[string]bool) []string {
	var problems []string
	pos := func(n ast.Node) string { return fset.Position(n.Pos()).String() }
	for _, decl := range file.Decls {
		fn, isFunc := decl.(*ast.FuncDecl)
		if !isFunc {
			// Package-level declarations: a flags literal here has no builder.
			ast.Inspect(decl, func(n ast.Node) bool {
				if lit, ok := n.(*ast.CompositeLit); ok && typeSelName(lit.Type) == "VStreamFlags" {
					problems = append(problems, pos(lit)+": VStreamFlags literal outside a function: build it in a classified builder")
				}
				return true
			})
			continue
		}
		if fn.Body == nil {
			continue
		}
		key, recvType, recvName := rosterKey(file.Name.Name, fn)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "MinimizeSkew" {
						problems = append(problems, pos(n)+": "+key+" assigns .MinimizeSkew: set it in the VStreamFlags literal, where the roster classifies it")
					}
				}
			case *ast.CompositeLit:
				switch typeSelName(n.Type) {
				case "VStreamRequest":
					if p := checkRequestFlags(n); p != "" {
						problems = append(problems, pos(n)+": "+key+": "+p)
					}
				case "VStreamFlags":
					class, listed := vstreamSkewRoster[key]
					if !listed {
						problems = append(problems, pos(n)+": "+key+" builds VStreamFlags but is not classified COPY or CDC in vstreamSkewRoster")
						return true
					}
					found[key] = true
					if p := checkMinimizeSkew(n, class, recvType, recvName); p != "" {
						problems = append(problems, pos(n)+": "+key+" ("+class+"): "+p)
					}
				}
			}
			return true
		})
	}
	return problems
}

// rosterKey is pkg.(*Recv).name for a method, pkg.name for a function, plus
// the receiver's type spelling and name.
func rosterKey(pkg string, fn *ast.FuncDecl) (key, recvType, recvName string) {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return pkg + "." + fn.Name.Name, "", ""
	}
	field := fn.Recv.List[0]
	switch rt := field.Type.(type) {
	case *ast.StarExpr:
		if id, ok := rt.X.(*ast.Ident); ok {
			recvType = "*" + id.Name
		}
	case *ast.Ident:
		recvType = rt.Name
	}
	if len(field.Names) > 0 {
		recvName = field.Names[0].Name
	}
	return pkg + ".(" + recvType + ")." + fn.Name.Name, recvType, recvName
}

// typeSelName is the selector name of a composite literal's type
// (vtgate.VStreamFlags → "VStreamFlags"), or "".
func typeSelName(e ast.Expr) string {
	if sel, ok := e.(*ast.SelectorExpr); ok {
		return sel.Sel.Name
	}
	return ""
}

// checkRequestFlags requires a VStreamRequest literal to carry its Flags as
// an inline VStreamFlags literal (which the roster then classifies).
func checkRequestFlags(lit *ast.CompositeLit) string {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); !ok || key.Name != "Flags" {
			continue
		}
		v := kv.Value
		if u, ok := v.(*ast.UnaryExpr); ok && u.Op == token.AND {
			v = u.X
		}
		if inner, ok := v.(*ast.CompositeLit); ok && typeSelName(inner.Type) == "VStreamFlags" {
			return ""
		}
		return "VStreamRequest Flags is not an inline VStreamFlags literal, so the roster cannot classify its MinimizeSkew"
	}
	return "VStreamRequest literal has no Flags: vtgate would default MinimizeSkew and StopOnReshard to false, unclassified"
}

// checkMinimizeSkew returns "" when lit's MinimizeSkew is the value its
// class requires, else what is wrong.
func checkMinimizeSkew(lit *ast.CompositeLit, class, recvType, recvName string) string {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); !ok || key.Name != "MinimizeSkew" {
			continue
		}
		switch class {
		case "COPY":
			if id, ok := kv.Value.(*ast.Ident); ok && id.Name == "vstreamCopyMinimizeSkew" {
				return ""
			}
			return "MinimizeSkew must be vstreamCopyMinimizeSkew"
		case "CDC":
			return checkCDCSkewArg(kv.Value, recvType, recvName)
		default:
			return "unknown class " + class
		}
	}
	return "no MinimizeSkew key (it would default to false silently)"
}

// checkCDCSkewArg requires vstreamCDCMinimizeSkew(<recv>.<field>) with the
// receiver's own skew field (negated for the reader's relaxSkew).
func checkCDCSkewArg(v ast.Expr, recvType, recvName string) string {
	want, known := cdcSkewFields[recvType]
	if !known || recvName == "" {
		return "a CDC builder must be a method of a stream type listed in cdcSkewFields (got receiver " + recvType + ")"
	}
	spelled := "vstreamCDCMinimizeSkew(" + recvName + "." + want.field + ")"
	if want.negated {
		spelled = "vstreamCDCMinimizeSkew(!" + recvName + "." + want.field + ")"
	}
	call, ok := v.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return "MinimizeSkew must be " + spelled + " (ADR-0120's CDC policy)"
	}
	if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "vstreamCDCMinimizeSkew" {
		return "MinimizeSkew must be " + spelled + " (ADR-0120's CDC policy)"
	}
	arg := call.Args[0]
	if want.negated {
		u, ok := arg.(*ast.UnaryExpr)
		if !ok || u.Op != token.NOT {
			return "MinimizeSkew must be " + spelled + " (the reader stores the inverse)"
		}
		arg = u.X
	}
	sel, ok := arg.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != want.field {
		return "MinimizeSkew must be " + spelled + ": the stream's own skew setting, not anything else"
	}
	if x, ok := sel.X.(*ast.Ident); !ok || x.Name != recvName {
		return "MinimizeSkew must be " + spelled + ": the builder's own receiver, not another stream's"
	}
	return ""
}
