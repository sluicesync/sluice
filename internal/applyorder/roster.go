// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applyorder

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// Func is one function declaration of a package: its key ("Recv.Name" for a
// method, "Name" for a function), the rendered types of its receiver and
// parameters, and the bare names of everything its body (closures included)
// calls.
type Func struct {
	Key    string
	Params []string
	Calls  map[string]bool
	// Order is every call (and method value) of the body by bare name, in
	// source order, for gates that grade sequence (see [Func.Before]).
	Order []string
	// Strings are the body's string literals, unquoted.
	Strings []string
}

// Before reports whether the body calls both a and b and EVERY call of b
// follows its first call of a — so one stray b ahead of a fails it.
func (f *Func) Before(a, b string) bool {
	firstA, firstB := -1, -1
	for i, c := range f.Order {
		if c == a && firstA < 0 {
			firstA = i
		}
		if c == b && firstB < 0 {
			firstB = i
		}
	}
	return firstA >= 0 && firstB > firstA
}

// ParseFuncs reads every non-test .go file in dir. Calls — and method values,
// which reach a method just as surely — are matched by BARE name with no type
// resolution, so two methods sharing a name are one callee. That
// over-approximates: a roster classifies more callers (an unrelated same-named
// method's are marked [Unrelated]), never misses one.
func ParseFuncs(dir string) (map[string]*Func, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	funcs := map[string]*Func{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			return nil, err
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			fn := &Func{Key: funcKey(fd), Calls: map[string]bool{}}
			if fd.Recv != nil {
				for _, p := range fd.Recv.List {
					fn.Params = append(fn.Params, types.ExprString(p.Type))
				}
			}
			for _, p := range fd.Type.Params.List {
				fn.Params = append(fn.Params, types.ExprString(p.Type))
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.CallExpr:
					if id, ok := n.Fun.(*ast.Ident); ok {
						fn.Calls[id.Name] = true
						fn.Order = append(fn.Order, id.Name)
					}
				case *ast.SelectorExpr:
					// A method call AND a method value (a.applySchemaEvent
					// handed to a config struct): both reach the method.
					fn.Calls[n.Sel.Name] = true
					fn.Order = append(fn.Order, n.Sel.Name)
				case *ast.BasicLit:
					if n.Kind == token.STRING {
						if s, err := strconv.Unquote(n.Value); err == nil {
							fn.Strings = append(fn.Strings, s)
						}
					}
				}
				return true
			})
			funcs[fn.Key] = fn
		}
	}
	return funcs, nil
}

func funcKey(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	t := fd.Recv.List[0].Type
	if s, ok := t.(*ast.StarExpr); ok {
		t = s.X
	}
	return types.ExprString(t) + "." + fd.Name.Name
}

// bareName is a Func key's callable name.
func bareName(key string) string {
	if i := strings.LastIndexByte(key, '.'); i >= 0 {
		return key[i+1:]
	}
	return key
}

// Helper is the [WriterCallers] classification of a caller that is not a
// write core itself: its own callers are walked in turn.
const Helper = "helper"

// Unrelated is the [WriterCallers] classification of a caller that reached
// the walk only through bare-name matching — it calls a same-named function
// of another type — so it is neither walked nor a core.
const Unrelated = "unrelated"

// WriterCallers walks up the call graph from the control writers. Every
// caller found must be classified: Helper (keep walking), Unrelated (a
// bare-name collision), or anything else — a write core the engine's order
// roster drives, named by the value. It
// returns the classified callers it reached and a problem per unclassified
// caller, per classification it never reached (stale), and per writer with no
// caller at all (the writer list is stale).
func WriterCallers(funcs map[string]*Func, writers []string, class map[string]string) (reached map[string]string, problems []string) {
	reached = map[string]string{}
	frontier := append([]string(nil), writers...)
	walked := map[string]bool{}
	for len(frontier) > 0 {
		callee := frontier[0]
		frontier = frontier[1:]
		if walked[callee] {
			continue
		}
		walked[callee] = true
		var callers []string
		for key, fn := range funcs {
			if fn.Calls[callee] {
				callers = append(callers, key)
			}
		}
		sort.Strings(callers)
		if len(callers) == 0 {
			problems = append(problems, fmt.Sprintf("nothing in the package calls %s — the writer list or the roster is stale", callee))
		}
		for _, key := range callers {
			c, ok := class[key]
			if !ok {
				problems = append(problems, fmt.Sprintf("%s calls %s and is not classified: name the write core it is (and drive it in the order roster) or mark it %q", key, callee, Helper))
				continue
			}
			reached[key] = c
			if c == Helper {
				frontier = append(frontier, bareName(key))
			}
		}
	}
	for key := range class {
		if _, ok := reached[key]; !ok {
			problems = append(problems, fmt.Sprintf("roster entry %s was never reached from a control writer — stale", key))
		}
	}
	sort.Strings(problems)
	return reached, problems
}

// TakingAny returns, sorted, the keys of every function whose receiver or a
// parameter has one of the rendered types — the universe an engine's roster
// classifies so a NEW transaction-writing function cannot go unexamined.
func TakingAny(funcs map[string]*Func, typeNames ...string) []string {
	var out []string
	for key, fn := range funcs {
		for _, p := range fn.Params {
			if slices.Contains(typeNames, p) {
				out = append(out, key)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// Reaching returns the keys of every function that calls one of targets, by
// bare name, directly or through other functions of the package — the same
// over-approximating bare-name matching as [ParseFuncs].
func Reaching(funcs map[string]*Func, targets []string) map[string]bool {
	reach := map[string]bool{}
	names := map[string]bool{}
	for _, t := range targets {
		names[t] = true
	}
	for changed := true; changed; {
		changed = false
		for key, fn := range funcs {
			if reach[key] {
				continue
			}
			for callee := range fn.Calls {
				if names[callee] {
					reach[key], names[bareName(key)], changed = true, true, true
					break
				}
			}
		}
	}
	return reach
}

// Cores is the set of write-core names a classification names.
func Cores(class map[string]string) map[string]bool {
	out := map[string]bool{}
	for _, c := range class {
		if c != Helper && c != Unrelated {
			out[c] = true
		}
	}
	return out
}
