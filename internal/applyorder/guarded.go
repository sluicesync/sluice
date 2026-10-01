// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applyorder

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// GuardedCallSites walks every non-test .go file in dir and, for each call to
// a function named in builders, requires a call to one of guards in the same
// SCOPE: the innermost enclosing `case` clause, or the whole function body
// when the call sits in no case. It returns how many builder call sites it
// found (the caller's anti-vacuity floor) and one problem per unguarded site.
//
// Reach, stated: calls are matched by bare name (an identifier or a selector's
// last element), with no type resolution, and "guarded" means the guard is
// called somewhere in that scope — not that it reads THIS call's result. It
// catches a new dispatch arm, a new dispatcher, or a guard dropped from one
// arm; it does not prove the guard is wired to the right value. Written for
// GC-42's key-scoped-write check (TestKeyScopedWriteRoster in each engine).
func GuardedCallSites(dir string, builders, guards []string) (sites int, problems []string, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, nil, err
	}
	isBuilder, isGuard := nameSet(builders), nameSet(guards)
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if perr != nil {
			return 0, nil, perr
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			var stack []ast.Node
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return true
				}
				stack = append(stack, n)
				call, ok := n.(*ast.CallExpr)
				if !ok || !isBuilder[calleeName(call)] {
					return true
				}
				sites++
				var scope ast.Node = fd.Body
				for i := len(stack) - 1; i >= 0; i-- {
					if cc, isCase := stack[i].(*ast.CaseClause); isCase {
						scope = cc
						break
					}
				}
				if !callsAny(scope, isGuard) {
					problems = append(problems, fmt.Sprintf("%s: %s calls %s with no %v in the same case/function",
						fset.Position(call.Pos()), funcKey(fd), calleeName(call), guards))
				}
				return true
			})
		}
	}
	return sites, problems, nil
}

func nameSet(names []string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

// calleeName is a call's bare callee name, or "" for anything else.
func calleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	}
	return ""
}

// callsAny reports whether scope contains a call to a name in set.
func callsAny(scope ast.Node, set map[string]bool) bool {
	found := false
	ast.Inspect(scope, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && set[calleeName(call)] {
			found = true
		}
		return !found
	})
	return found
}
