// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	// The same blank-import set the sibling roster files carry, so this
	// package's registry view matches the shipped binary's. Held to
	// cmd/sluice/main.go by [TestEngineRegistryCoversTheShippedBinary].
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// The migrate/sync TARGET engine list, kept honest against the code.
//
// # Why this exists
//
// sluice published, across two releases, that a behaviour "fires on SQLite and
// D1 targets" — and there is no D1 target. `d1` is a migrate SOURCE only; its
// `OpenSchemaWriter` returns `ErrD1NotImplemented`, and the `sqlite` engine's
// DSN parser has no `d1://` case, so the routing the prose described
// ("a D1 target goes through the `sqlite` engine") does not exist either: a
// `d1://` DSN handed to `--target-driver sqlite` is read as a literal FILE
// PATH. The claim had already been corrected once, in the v0.110.1 notes on
// 2026-08-04; the NEXT release made it again. That is the tell that a
// correction is not a fix — nothing was holding the sentence to anything.
//
// It is a claim with a mechanical answer. An engine can be a migrate/sync
// target exactly when it implements the two write doors the copy needs: create
// the tables ([ir.Engine.OpenSchemaWriter]) and write the rows
// ([ir.Engine.OpenRowWriter]). A source-only engine refuses both with a
// sentinel. This test derives the set from that and holds the operator doc to
// it.
//
// # Scope — which doors it grades, and which it deliberately does not
//
// It grades `OpenSchemaWriter` and `OpenRowWriter`, requires them to AGREE per
// engine, and answers one question: can this engine be the target of a
// `migrate` or a `sync` cold-start copy.
//
// It does NOT grade `OpenChangeApplier`, because that is a genuinely different
// question — CDC apply. `sqlite` is a migrate target and refuses
// `OpenChangeApplier`; production-readiness.md's "CDC apply targets" sentence
// is therefore a SEPARATE claim, held by its own gate
// ([TestCDCApplyTargetListMatchesTheCode], the `cdc-apply-targets` marker).
// Naming that here rather than leaving it implied: a gate whose coverage is
// narrower than its name reads as covering the sibling.
//
// # Which DOCUMENTS it reaches — one, and the claim is spread wider than that
//
// The marker this test holds lives in `docs/production-readiness.md` and
// NOWHERE else. Every other home of the same claim is prose bound to nothing,
// and that is not hypothetical: on 2026-08-07 a `perf-parity-checker` pass
// found `docs/dev/perf-parity-matrix.md` row 3 still asserting "D1-as-target
// ABSENT (stage-local instead)" — the FOURTH home of the sentence, wrong twice
// over (there is no D1 target, and `--stage-local` is a D1 SOURCE mechanism),
// contradicted by row 32 of the same file, and untouched by `78f04f19`, which
// corrected the identical sentence in ADR-0150. That is one release AFTER this
// gate was built, which is the point worth writing down: a gate whose coverage
// is narrower than its claim's SPREAD does not merely miss a home, it stops
// anyone from looking at the others.
//
// The matrix is deliberately OUT of scope rather than accidentally so: it is a
// performance document that mentions the target set in passing, and giving it
// an engine-list marker would be a marker on a sentence rather than on a list.
// Widening the gate is the alternative if a fifth home appears — extend
// `docPath` to a slice of (file, marker) pairs; the derivation halves need no
// change.
//
// # Two derivations that share no evidence
//
// A verification whose answer comes from the same artifact as the thing it
// verifies is not a verification. So the set is derived twice, independently,
// and the two must agree:
//
//   - STRUCTURE (the source). An AST pass over internal/engines asks whether
//     each door's body is a bare `return nil, <sentinel>` — the refusal-stub
//     shape — keyed on the concrete receiver TYPE, because `sqlite` and `d1`
//     live in one package with opposite answers.
//   - BEHAVIOUR (the running registry). Every registered engine's door is
//     actually CALLED with an empty DSN and the returned error classified. A
//     refusal stub ignores its arguments and returns its sentinel; a real
//     writer gets past that and fails parsing the DSN instead. Both are
//     immediate and open nothing — verified: no engine's empty-DSN path dials,
//     touches the filesystem, or takes measurable time.
//
// Neither reads the other's evidence: one is the shape of the code, the other
// is the error an operator would actually see. A disagreement fails loudly
// rather than picking a winner.
func TestTargetEngineListMatchesTheCode(t *testing.T) {
	fromStructure := targetEnginesByStructure(t)
	fromBehaviour := targetEnginesByBehaviour(t)

	if !equalStringSets(fromStructure, fromBehaviour) {
		t.Fatalf("the two derivations of the target-engine set disagree — resolve that before trusting "+
			"either.\n  structure (AST: writer door is not a bare `return nil, <sentinel>`): %s\n"+
			"  behaviour (the door called with an empty DSN did not answer \"not implemented\"): %s",
			strings.Join(fromStructure, ", "), strings.Join(fromBehaviour, ", "))
	}
	fromCode := fromStructure

	// Anti-vacuity floors. An empty set, or a set that swallowed every
	// registered engine, would agree with a doc marker saying anything.
	if len(fromCode) == 0 {
		t.Fatalf("no registered engine classifies as a migrate/sync target — the derivation broke; this gate "+
			"cannot be allowed to pass on an empty set (%d engines registered)", len(engines.Names()))
	}
	if len(fromCode) == len(engines.Names()) {
		t.Fatalf("every one of the %d registered engines classifies as a target, so the derivation separates "+
			"nothing. sluice ships source-only engines (d1, csv, mydumper, …); if that genuinely changed, "+
			"delete this gate — do not delete this check", len(engines.Names()))
	}
	for _, name := range fromCode {
		if name == "d1" || name == "d1-trigger" {
			t.Fatalf("%q classified as a migrate/sync TARGET. Cloudflare D1 is a source-only engine and the "+
				"published claim that a D1 target exists is the reason this gate was written — if a D1 target "+
				"engine has genuinely shipped, remove this check IN THE SAME COMMIT as the doc update", name)
		}
	}

	docPath := filepath.Join("..", "..", "docs", "production-readiness.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	marker := regexp.MustCompile(`<!--\s*target-engines:\s*([^>]*?)\s*-->`)
	m := marker.FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/production-readiness.md carries no `<!-- target-engines: … -->` marker. It is the "+
			"checkable form of a claim sluice published wrong across two releases; add it listing: %s",
			strings.Join(fromCode, ", "))
	}

	fromDoc := splitList(string(m[1]))
	if !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the operator doc's target-engine list disagrees with the code.\n"+
			"  code (engines implementing both writer doors): %s\n"+
			"  doc  (marker):                                 %s\n\n"+
			"An engine absent from this list refuses OpenSchemaWriter/OpenRowWriter and CANNOT be named as a "+
			"target — not in the docs, not in a CHANGELOG entry, not in release notes. Update the marker AND "+
			"the prose beside it, and take any \"which targets does this affect\" sentence from this list "+
			"rather than from the shape of the fix.",
			strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}
}

// writerDoors are the two [ir.Engine] methods a migrate/sync target must
// implement: create the tables, then write the rows. `OpenChangeApplier` is
// deliberately absent — see the scope note on
// [TestTargetEngineListMatchesTheCode].
var writerDoors = []string{"OpenSchemaWriter", "OpenRowWriter"}

// targetEnginesByStructure derives the target set from the SOURCE: an engine
// is source-only when its writer door is a bare `return nil, <sentinel>`.
//
// The two doors must agree for a given engine. A half-implemented target —
// tables but no rows, or the reverse — is not a coherent answer to "can this
// be a target", so it fails rather than being resolved one way.
func targetEnginesByStructure(t *testing.T) []string {
	t.Helper()
	byType := registeredEngineNamesByPackageType(t)

	var out []string
	for key, names := range byType {
		var isTarget *bool
		for _, door := range writerDoors {
			stub, found := refusalStubDoor(t, key, door)
			if !found {
				t.Fatalf("no %s method declared on %s, but it is registered as engine(s) %s. The AST scan "+
					"cannot classify it — a promoted or generated door would make this gate silently "+
					"under-report", door, key, strings.Join(names, ", "))
			}
			target := !stub
			if isTarget != nil && *isTarget != target {
				t.Fatalf("%s implements one writer door and refuses the other (engine(s) %s). A half-target "+
					"is not a classification this gate can make on your behalf: decide whether it is a "+
					"target and make both doors say so", key, strings.Join(names, ", "))
			}
			isTarget = &target
		}
		if *isTarget {
			out = append(out, names...)
		}
	}
	sort.Strings(out)
	return out
}

// targetEnginesByBehaviour derives the same set by CALLING each registered
// engine's writer doors with an empty DSN and reading the error.
//
// This is the operator-visible half and shares no evidence with the AST pass.
// The empty DSN is what makes it safe to run as a unit test: every refusal stub
// returns its sentinel without looking at the argument, and every real writer
// refuses the empty DSN at parse time — none of them dials or opens a file.
func targetEnginesByBehaviour(t *testing.T) []string {
	t.Helper()

	// A door that ever did reach out would hang here rather than pass; the
	// bound makes that a failure instead of a stalled CI job.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var out []string
	for _, name := range engines.Names() {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() listed %q but engines.Get(%q) missed", name, name)
		}

		var isTarget *bool
		for _, door := range writerDoors {
			var err error
			switch door {
			case "OpenSchemaWriter":
				_, err = e.OpenSchemaWriter(ctx, "")
			case "OpenRowWriter":
				_, err = e.OpenRowWriter(ctx, "")
			}
			if err == nil {
				t.Fatalf("%s.%s accepted an EMPTY DSN. Every engine is expected to refuse it — either loudly "+
					"(a real target, at DSN parse) or with its source-only sentinel; a nil error means this "+
					"probe opened something it should not have", name, door)
			}
			// Every source-only sentinel in internal/engines spells its
			// refusal "not implemented" (sqlite.ErrD1NotImplemented,
			// flatfile/mydumper/sqlite-trigger's ErrNotImplemented, and
			// d1-trigger's re-export of the D1 one). A real writer's
			// empty-DSN error is a parse refusal and never says it. The AST
			// derivation is what catches this classifier going stale.
			target := !strings.Contains(err.Error(), "not implemented")
			if isTarget != nil && *isTarget != target {
				t.Fatalf("%s answers the two writer doors differently (%s disagreed with its sibling); a "+
					"half-target cannot be classified", name, door)
			}
			isTarget = &target
		}
		if *isTarget {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// refusalStubDoor reports whether `<pkgDir>.<TypeName>`'s named method is a
// bare `return nil, <sentinel>` — the shape every source-only engine uses to
// refuse a door it does not implement.
//
// Anything else is a real implementation, including a one-line DELEGATION:
// pgtrigger's `return e.pg.OpenSchemaWriter(ctx, dsn)` is a single return of
// one call expression, not a two-value return of a sentinel identifier, so it
// classifies as the target it is.
func refusalStubDoor(t *testing.T, key, method string) (stub, found bool) {
	t.Helper()
	pkgDir, typeName, ok := strings.Cut(key, ".")
	if !ok {
		t.Fatalf("malformed package.type key %q", key)
	}

	dir := filepath.Join("..", "..", "internal", "engines", pkgDir)
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}

	fset := token.NewFileSet()
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".go") || strings.HasSuffix(f.Name(), "_test.go") {
			continue
		}
		parsed, perr := parser.ParseFile(fset, filepath.Join(dir, f.Name()), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", filepath.Join(dir, f.Name()), perr)
		}
		for _, decl := range parsed.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc || fn.Recv == nil || len(fn.Recv.List) != 1 || fn.Name.Name != method {
				continue
			}
			if receiverTypeName(fn.Recv.List[0].Type) != typeName {
				continue
			}
			return isBareSentinelReturn(fn.Body), true
		}
	}
	return false, false
}

// receiverTypeName yields the bare type name of a method receiver, seeing
// through a pointer receiver.
func receiverTypeName(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if id, ok := expr.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

// isBareSentinelReturn reports whether a body is exactly
// `return nil, <identifier>` — one statement, two results, a literal nil and a
// package-level error value. A sentinel reached through a selector
// (`sqlite.ErrD1NotImplemented`) counts; a call expression does not.
func isBareSentinelReturn(body *ast.BlockStmt) bool {
	if body == nil || len(body.List) != 1 {
		return false
	}
	ret, ok := body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 2 {
		return false
	}
	if id, ok := ret.Results[0].(*ast.Ident); !ok || id.Name != "nil" {
		return false
	}
	switch ret.Results[1].(type) {
	case *ast.Ident, *ast.SelectorExpr:
		return true
	default:
		return false
	}
}
