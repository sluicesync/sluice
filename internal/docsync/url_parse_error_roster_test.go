// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// EVERY ERROR RETURNED BY net/url.Parse IS EITHER DISCARDED, COMPARED
// TO nil, OR ROUTED THROUGH SafeParseError — never wrapped or returned
// raw (audit 2026-09-15 A0915-SEC-MEDIUM-1).
//
// The defect: `*url.Error` embeds the VERBATIM input in its `.URL`
// field and prints it, so `fmt.Errorf("parse %q: %w", redacted(s), err)`
// redacts the string once and then leaks it through the wrapped error
// in the same message. blobcodec did exactly that at two sites, after a
// 2026-08-04 fix had hardened the redactor those sites called — the
// sibling-miss shape where a redactor is fixed and the wrapped parse
// error beside it is not enumerated. Every Postgres/pgtrigger DSN site
// already routed through safeerr.SafeParseError; this gate is the
// mechanical form of that rule.
//
// # What it grades
//
// Every call to `url.Parse` / `url.ParseRequestURI` in non-test Go under
// cmd/ and internal/. The universe is derived from the AST, not listed.
// For each call the error result is followed from its assignment to
// the end of its scope (or its next reassignment) and every use is
// classified:
//
//   - `err == nil` / `err != nil` — safe (a comparison prints nothing);
//   - an argument to SafeParseError (safeerr's or diagnose's) — safe;
//   - an argument to errors.Is / errors.As — safe;
//   - ANYTHING ELSE — an offender, named with its shape: `%w`-wrapped
//     by fmt.Errorf, returned raw, or otherwise used raw.
//
// A call whose result is not the right-hand side of a two-value
// assignment (`u, err := url.Parse(s)` or the if-init form) is an
// offender too, as an unrecognised shape: fail by default, exempt by
// name.
//
// # Scope, stated so it cannot be read as broader
//
// This reaches url.Parse's ERROR only. A site that echoes the raw input
// string in its own message with %q — the third blobcodec site — is
// outside its reach; that shape is pinned by
// TestBlobStore_ParseFailureErrorsDoNotLeakCredentials (blobcodec) and
// has no repo-wide gate. Nor does it follow an error handed to a helper
// (`wrap(err)`): that is classified as a raw use and must be exempted
// with a reason, which is the intended pressure.
func TestEveryURLParseErrorIsWrappedThroughSafeParseError(t *testing.T) {
	// urlParseExempt records, per "relative/file.go:funcName", why a raw
	// use of a url.Parse error is acceptable there. Keyed by function so
	// a moved line does not silently drop the entry; an entry naming a
	// site that no longer offends is reported as stale.
	urlParseExempt := map[string]string{
		"internal/crypto/azure_kms.go:parseAzureKeyID": "the key ID is a public Key Vault locator by contract (https://VAULT.vault.azure.net/keys/KEY[/VERSION]) with no credential position, and the message echoes it with %q deliberately at flag-validation time; SafeParseError would hide nothing the %q does not already print",
	}

	root := repoRootFromDocsync(t)
	fset := token.NewFileSet()
	var (
		sites     int
		routed    int
		offenders []string
		exempted  = map[string]bool{}
	)

	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return fmt.Errorf("parse %s: %w", path, perr)
			}
			urlName := importLocalName(f, "net/url")
			if urlName == "" {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)

			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				key := rel + ":" + fn.Name.Name
				for _, site := range urlParseSites(fn.Body, urlName) {
					sites++
					uses := site.classify()
					if uses.routed {
						routed++
					}
					if len(uses.raw) == 0 {
						continue
					}
					if reason, ok := urlParseExempt[key]; ok && strings.TrimSpace(reason) != "" {
						exempted[key] = true
						continue
					}
					pos := fset.Position(site.call.Pos())
					for _, u := range uses.raw {
						offenders = append(offenders, fmt.Sprintf("%s:%d  %s (url.Parse at line %d)", rel, fset.Position(u.pos).Line, u.shape, pos.Line))
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}

	t.Logf("url.Parse sites=%d routed-through-SafeParseError=%d exempt=%d offenders=%d", sites, routed, len(exempted), len(offenders))

	// Anti-vacuity floors. Sixteen url.Parse sites exist across cmd/ and
	// internal/ today and seven of them route through SafeParseError;
	// a walker that stopped matching calls, or stopped recognising the
	// routed spelling, would be green for exactly the defect it exists
	// to catch.
	if sites < 10 {
		t.Fatalf("found %d url.Parse site(s) under cmd/ and internal/; floor 10 — the walk is vacuous, re-point it", sites)
	}
	if routed < 3 {
		t.Fatalf("found %d url.Parse site(s) routed through SafeParseError; floor 3 (the Postgres/pgtrigger DSN sites) — the routed-use classifier no longer recognises the spelling", routed)
	}
	for key := range urlParseExempt {
		if !exempted[key] {
			t.Errorf("exemption %q names a site that no longer uses its url.Parse error raw — drop the stale entry", key)
		}
	}
	if len(offenders) > 0 {
		sort.Strings(offenders)
		t.Errorf("%d raw use(s) of a url.Parse error:\n  %s\n\n"+
			"*url.Error prints the VERBATIM input — userinfo and query string included — so wrapping or "+
			"returning it leaks whatever the URL carried, past any redaction applied to the string beside it. "+
			"Pass the error through safeerr.SafeParseError (or diagnose.SafeParseError), which keeps the reason "+
			"and drops the locator; or add a keyed exemption with a reason the input cannot carry a credential.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
}

// importLocalName returns the identifier a file uses for the given
// import path ("" when the file does not import it).
func importLocalName(f *ast.File, path string) string {
	for _, imp := range f.Imports {
		if strings.Trim(imp.Path.Value, `"`) != path {
			continue
		}
		if imp.Name != nil {
			return imp.Name.Name
		}
		return path[strings.LastIndex(path, "/")+1:]
	}
	return ""
}

// urlParseSite is one `url.Parse` / `url.ParseRequestURI` call, with the
// error identifier it was assigned to and the statements in which that
// identifier is live.
type urlParseSite struct {
	call *ast.CallExpr
	// errName is the identifier bound to the error result; "" when the
	// result was discarded (`u, _ :=`) or the call shape is unrecognised.
	errName string
	// scope holds the statements after the assignment in which errName
	// may be used (the if-init form's Cond/Body/Else, or the enclosing
	// block's remaining statements). nil with a non-empty errName means
	// the call shape was not recognised.
	scope []ast.Node
	// unrecognised is set when the call is not the RHS of a two-value
	// assignment; graded as an offender.
	unrecognised bool
}

type rawUse struct {
	pos   token.Pos
	shape string
}

type useReport struct {
	raw    []rawUse
	routed bool
}

// urlParseSites finds every url.Parse call in body and binds each to
// the scope its error identifier is live in.
func urlParseSites(body *ast.BlockStmt, urlName string) []urlParseSite {
	var out []urlParseSite
	seen := map[*ast.CallExpr]bool{}

	isParse := func(e ast.Expr) (*ast.CallExpr, bool) {
		call, ok := e.(*ast.CallExpr)
		if !ok {
			return nil, false
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return nil, false
		}
		x, ok := sel.X.(*ast.Ident)
		if !ok || x.Name != urlName {
			return nil, false
		}
		return call, sel.Sel.Name == "Parse" || sel.Sel.Name == "ParseRequestURI"
	}
	bind := func(as *ast.AssignStmt) (call *ast.CallExpr, errName string, ok bool) {
		if len(as.Rhs) != 1 || len(as.Lhs) != 2 {
			return nil, "", false
		}
		call, ok = isParse(as.Rhs[0])
		if !ok {
			return nil, "", false
		}
		if id, isIdent := as.Lhs[1].(*ast.Ident); isIdent && id.Name != "_" {
			return call, id.Name, true
		}
		return call, "", true
	}

	// Walk every block so the statement list a site lives in is known.
	ast.Inspect(body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.IfStmt:
			if as, ok := node.Init.(*ast.AssignStmt); ok {
				if call, errName, ok := bind(as); ok {
					seen[call] = true
					scope := []ast.Node{node.Cond, node.Body}
					if node.Else != nil {
						scope = append(scope, node.Else)
					}
					out = append(out, urlParseSite{call: call, errName: errName, scope: scope})
				}
			}
		case *ast.BlockStmt:
			for i, stmt := range node.List {
				as, ok := stmt.(*ast.AssignStmt)
				if !ok {
					continue
				}
				call, errName, ok := bind(as)
				if !ok || seen[call] {
					continue
				}
				seen[call] = true
				scope := make([]ast.Node, 0, len(node.List)-i-1)
				for _, later := range node.List[i+1:] {
					scope = append(scope, later)
				}
				out = append(out, urlParseSite{call: call, errName: errName, scope: scope})
			}
		}
		return true
	})
	// Any parse call not bound above is an unrecognised shape.
	ast.Inspect(body, func(n ast.Node) bool {
		e, isExpr := n.(ast.Expr)
		if !isExpr {
			return true
		}
		if call, ok := isParse(e); ok && !seen[call] {
			seen[call] = true
			out = append(out, urlParseSite{call: call, unrecognised: true})
		}
		return true
	})
	return out
}

// classify follows the site's error identifier through its scope and
// reports every raw use; routed is set when SafeParseError consumed it.
func (s urlParseSite) classify() useReport {
	var rep useReport
	if s.unrecognised {
		rep.raw = append(rep.raw, rawUse{pos: s.call.Pos(), shape: "url.Parse result is not a two-value assignment (unrecognised shape)"})
		return rep
	}
	if s.errName == "" {
		return rep
	}
	for _, node := range s.scope {
		reassigned := false
		// parents tracks the enclosing expression of each ident so a
		// use can be named by its shape.
		var stack []ast.Node
		ast.Inspect(node, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, n)
			if as, ok := n.(*ast.AssignStmt); ok && assignsName(as, s.errName) {
				// The RHS may still use the old value (`err = wrap(err)`);
				// the walk continues into it, and the scan stops after
				// this statement.
				reassigned = true
			}
			id, ok := n.(*ast.Ident)
			if !ok || id.Name != s.errName || len(stack) < 2 {
				return true
			}
			parent := stack[len(stack)-2]
			if as, ok := parent.(*ast.AssignStmt); ok && isLHS(as, id) {
				return true
			}
			switch p := parent.(type) {
			case *ast.BinaryExpr:
				if (p.Op == token.EQL || p.Op == token.NEQ) && (isNil(p.X) || isNil(p.Y)) {
					return true
				}
			case *ast.CallExpr:
				switch name := calleeName(p); name {
				case "SafeParseError":
					rep.routed = true
					return true
				case "errors.Is", "errors.As":
					return true
				}
				rep.raw = append(rep.raw, rawUse{pos: id.Pos(), shape: describeCall(p)})
				return true
			case *ast.ReturnStmt:
				rep.raw = append(rep.raw, rawUse{pos: id.Pos(), shape: "url.Parse error returned raw"})
				return true
			}
			rep.raw = append(rep.raw, rawUse{pos: id.Pos(), shape: fmt.Sprintf("url.Parse error used raw as %T operand", parent)})
			return true
		})
		if reassigned {
			break
		}
	}
	return rep
}

func assignsName(as *ast.AssignStmt, name string) bool {
	for _, l := range as.Lhs {
		if id, ok := l.(*ast.Ident); ok && id.Name == name {
			return true
		}
	}
	return false
}

func isLHS(as *ast.AssignStmt, id *ast.Ident) bool {
	for _, l := range as.Lhs {
		if l == id {
			return true
		}
	}
	return false
}

func isNil(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == "nil"
}

// calleeName renders `pkg.Fn` / `Fn` for a call; the SafeParseError
// match is on the bare selector so both safeerr's and diagnose's
// spellings count.
func calleeName(c *ast.CallExpr) string {
	switch f := c.Fun.(type) {
	case *ast.SelectorExpr:
		if x, ok := f.X.(*ast.Ident); ok {
			if f.Sel.Name == "SafeParseError" {
				return "SafeParseError"
			}
			return x.Name + "." + f.Sel.Name
		}
		return f.Sel.Name
	case *ast.Ident:
		return f.Name
	}
	return ""
}

func describeCall(c *ast.CallExpr) string {
	name := calleeName(c)
	if name == "fmt.Errorf" && len(c.Args) > 0 {
		if lit, ok := c.Args[0].(*ast.BasicLit); ok && strings.Contains(lit.Value, "%w") {
			return "url.Parse error %w-wrapped raw by fmt.Errorf"
		}
	}
	return fmt.Sprintf("url.Parse error passed raw to %s", name)
}
