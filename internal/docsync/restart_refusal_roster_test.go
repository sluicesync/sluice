// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The fleet supervisor's do-not-restart roster must be named, marker by
// marker, by every doc that tells an operator or an agent which refusals a
// restart repeats.
//
// # Why this exists (v0.159.0 drift report, rows 2–4)
//
// `refusalsARestartRepeats` (internal/pipeline/supervisor_restart_refusals.go)
// grew by two sentinels in each of two consecutive releases, and each time
// the four docs that list it — AGENTS.md, its embedded copy, the fleet
// skill and the service doc — drifted: an entry missing, and a stated count
// ("eleven more", "any of these twelve") that was wrong once the next
// sentinel landed. Nothing tied the docs to the code. A count is the worst
// kind of claim to leave unchecked: it reads as precise, so nobody recounts.
//
// # What it derives, and from where
//
// The universe is the code, through the AST:
//
//  1. every element of the `refusalsARestartRepeats` composite literal,
//     each a selector `pkg.Sentinel`, resolved through the file's imports
//     to the package that declares it and from there to the string its
//     `errors.New` carries — a literal, or a package-level const (applymarks'
//     `ErrMismatch = errors.New(MismatchMarker)`). That string IS the
//     refusal's grep-stable marker (internal/ir/restart_refusals.go);
//  2. every `ir.` sentinel the supervisor (internal/pipeline/supervisor.go)
//     tests with `errors.Is` ahead of the roster — UNFORWARDED-SCHEMA-CHANGE,
//     which has its own branch and is not in the list.
//
// A sentinel whose doc comment names a `SLUICE-E-…` code may be named in the
// docs by that code instead (the coded KEY-SCOPED-WRITE-MATCHED-MULTIPLE-ROWS
// is SLUICE-E-CDC-KEY-MATCHED-MULTIPLE-ROWS to an operator).
//
// # What it checks, and what it does not
//
// Each doc carries exactly one region between `<!-- restart-refusal-roster -->`
// and `<!-- /restart-refusal-roster -->`, and every derived marker appears
// backticked inside it. The region carries no count of ten or more spelled
// in words, because the counts were the half that drifted: the docs say
// "these" and the gate checks the list. It does NOT check the reverse — a
// marker a region names that the roster lacks — because the regions
// legitimately name the wrapper markers that ride a roster sentinel
// (COLUMN-IDENTITY-UNREAD, KEY-COLLATION-GUARD-UNSUPPORTED) and the remedy
// markers inside an entry; a doc that tells an operator a restartable
// refusal is not restarted is outside its reach. It also reaches only these
// four docs: the site (sluicesync.com) keeps its own copy, gated nowhere
// here.
func TestRestartRefusalRosterIsNamedByEveryDoc(t *testing.T) {
	root := repoRootFromDocsync(t)
	markers := restartRefusalMarkers(t, root)

	// Anti-vacuity: the roster held 14 sentinels plus UNFORWARDED when this
	// gate was written. A walk that resolved far fewer has stopped reading
	// the literal, not found a shorter roster.
	if len(markers) < 12 {
		t.Fatalf("derived only %d restart-refusal markers (%v); the AST walk has gone blind", len(markers), sortedRosterKeys(markers))
	}
	if _, ok := markers["UNFORWARDED-SCHEMA-CHANGE"]; !ok {
		t.Fatal("the supervisor's UNFORWARDED-SCHEMA-CHANGE branch was not derived")
	}

	docs := []string{
		"AGENTS.md",
		"internal/agentguide/AGENTS.md",
		"docs/operator/running-as-a-service.md",
		"skills/fleet-operator/SKILL.md",
	}
	countWord := regexp.MustCompile(`(?i)\b(ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)\b`)
	for _, doc := range docs {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(doc)))
		if err != nil {
			t.Fatalf("read %s: %v", doc, err)
		}
		text := string(raw)
		const open, closing = "<!-- restart-refusal-roster -->", "<!-- /restart-refusal-roster -->"
		if strings.Count(text, open) != 1 || strings.Count(text, closing) != 1 {
			t.Errorf("%s: want exactly one %s … %s region", doc, open, closing)
			continue
		}
		start := strings.Index(text, open) + len(open)
		end := strings.Index(text, closing)
		if end < start {
			t.Errorf("%s: the roster region closes before it opens", doc)
			continue
		}
		region := text[start:end]
		for _, m := range sortedRosterKeys(markers) {
			named := strings.Contains(region, "`"+m+"`")
			for _, code := range markers[m] {
				named = named || strings.Contains(region, "`"+code+"`")
			}
			if !named {
				t.Errorf("%s: the restart-refusal region does not name `%s`, which the supervisor does not restart", doc, m)
			}
		}
		if w := countWord.FindString(region); w != "" {
			t.Errorf("%s: the restart-refusal region states a count (%q); name the refusals instead, the gate checks the list", doc, w)
		}
	}
}

// restartRefusalMarkers returns every marker the fleet supervisor does not
// restart, each with the SLUICE-E codes its sentinel's doc comment names.
func restartRefusalMarkers(t *testing.T, root string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	add := func(file *ast.File, sel *ast.SelectorExpr) {
		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			t.Fatalf("roster element %s.%v is not a package selector", exprString(sel.X), sel.Sel.Name)
		}
		marker, codes := resolveSentinel(t, root, importDir(t, root, file, pkg.Name), sel.Sel.Name)
		out[marker] = codes
	}

	rosterFile := parseGoFile(t, filepath.Join(root, "internal", "pipeline", "supervisor_restart_refusals.go"))
	found := false
	ast.Inspect(rosterFile, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok || len(vs.Names) != 1 || vs.Names[0].Name != "refusalsARestartRepeats" || len(vs.Values) != 1 {
			return true
		}
		lit, ok := vs.Values[0].(*ast.CompositeLit)
		if !ok {
			t.Fatal("refusalsARestartRepeats is no longer a composite literal; teach the gate its new shape")
		}
		found = true
		for _, elt := range lit.Elts {
			sel, ok := elt.(*ast.SelectorExpr)
			if !ok {
				t.Fatalf("refusalsARestartRepeats element %T is not a pkg.Sentinel selector", elt)
			}
			add(rosterFile, sel)
		}
		return false
	})
	if !found {
		t.Fatal("refusalsARestartRepeats not found in supervisor_restart_refusals.go")
	}

	supervisor := parseGoFile(t, filepath.Join(root, "internal", "pipeline", "supervisor.go"))
	ast.Inspect(supervisor, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}
		fn, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || fn.Sel.Name != "Is" || exprString(fn.X) != "errors" {
			return true
		}
		if sel, ok := call.Args[1].(*ast.SelectorExpr); ok && exprString(sel.X) == "ir" {
			add(supervisor, sel)
		}
		return true
	})
	return out
}

// resolveSentinel finds `var name = errors.New(<lit or const>)` in the
// package at dir and returns its text and the SLUICE-E codes its doc
// comment names.
func resolveSentinel(t *testing.T, root, dir, name string) (marker string, codes []string) {
	t.Helper()
	consts := map[string]string{}
	var arg ast.Expr
	var doc string
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f := parseGoFile(t, filepath.Join(dir, e.Name()))
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, id := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					if gd.Tok == token.CONST {
						if bl, ok := vs.Values[i].(*ast.BasicLit); ok && bl.Kind == token.STRING {
							consts[id.Name], _ = strconv.Unquote(bl.Value)
						}
					}
					if gd.Tok == token.VAR && id.Name == name {
						call, ok := vs.Values[i].(*ast.CallExpr)
						if !ok || exprString(call.Fun) != "errors.New" || len(call.Args) != 1 {
							t.Fatalf("%s.%s is not an errors.New sentinel", filepath.Base(dir), name)
						}
						arg = call.Args[0]
						for _, c := range []*ast.CommentGroup{vs.Doc, gd.Doc} {
							if c != nil {
								doc += c.Text()
							}
						}
					}
				}
			}
		}
	}
	switch a := arg.(type) {
	case *ast.BasicLit:
		marker, _ = strconv.Unquote(a.Value)
	case *ast.Ident:
		marker = consts[a.Name]
	}
	if marker == "" {
		rel, _ := filepath.Rel(root, dir)
		t.Fatalf("could not resolve the marker text of %s.%s", rel, name)
	}
	codes = regexp.MustCompile(`SLUICE-E-[A-Z0-9-]*[A-Z0-9]`).FindAllString(doc, -1)
	return marker, codes
}

// importDir maps a package name used in file to its directory in the module.
func importDir(t *testing.T, root string, file *ast.File, pkg string) string {
	t.Helper()
	const module = "sluicesync.dev/sluice/"
	for _, imp := range file.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		name := path[strings.LastIndex(path, "/")+1:]
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if name == pkg && strings.HasPrefix(path, module) {
			return filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(path, module)))
		}
	}
	t.Fatalf("package %s is not a module import of the roster file", pkg)
	return ""
}

func parseGoFile(t *testing.T, path string) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return f
}

// exprString renders an identifier or a selector chain; anything else is
// rendered as its node type, which no caller here compares equal to a name.
func exprString(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return exprString(v.X) + "." + v.Sel.Name
	}
	return "<expr>"
}

func sortedRosterKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
