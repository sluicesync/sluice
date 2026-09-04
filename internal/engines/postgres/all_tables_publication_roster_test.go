// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// Every caller of [ensureAllTablesPublication] must warn about what it is
// about to break.
//
// WHY THIS EXISTS. A `FOR ALL TABLES` publication stops Postgres accepting
// UPDATE and DELETE on every permanent logged table in the DATABASE that has
// no replica identity — not only the tables the run reads. INSERT keeps
// working, so the breakage is partial and surfaces as an error inside
// whatever application owns those tables, with nothing connecting it to
// sluice.
//
// The A2-4b warning was written for that hazard and shipped reaching ONE of
// the two callers. The other, `backup full --chain-slot`, had neither a
// refusal nor a warning — and is the worse of the two, because it
// deliberately PERSISTS the publication so the chain's incrementals can
// decode through it, so the exposure outlives the run. Found by the v0.141.0
// pre-tag value-fidelity review, not by the commit that introduced the
// warning, whose own subject was this exact hazard. One grep would have done
// it, which is why this is now a test rather than a resolution to grep.
//
// WHAT IT ASSERTS, stated so the name cannot be read wider than the truth:
// that each calling function ALSO calls something that audits the exposure,
// in the same function body. It is syntactic. It does not prove the audit is
// correct, or that its scope is right — those are
// TestAuditPublicationExposure_MatchesRealPublicationCoverage's job and the
// pipeline's ordering gate's. What it makes impossible is a THIRD caller
// joining silently, which is how the second one got here.
//
// A caller whose warning lives OUTSIDE this package is exempt through
// [allTablesPublicationExempt], and the exemption is checked rather than
// trusted: the gate it names must still exist, by name, in the file it
// names.
// allTablesPublicationExempt records callers whose exposure warning lives
// outside this package, with the gate that covers them instead.
var allTablesPublicationExempt = map[string]struct {
	reason       string
	coveringTest string
	coveringFile string
}{
	"ensureAllTablesPublication": {
		reason: "a RENDERER, not a call site. It emits the statement, and BOTH of its callers appear in " +
			"this same roster where each is separately required to warn or be exempt. Warning here as well " +
			"would double-warn the backup path, and would add a blanket warning alongside the sync path's " +
			"precise one, which reports the complement of what the refusal actually graded.",
		coveringTest: "TestEveryAllTablesPublicationEmitterWarnsAboutExposure",
		coveringFile: "all_tables_publication_roster_test.go",
	},
	"openSnapshotStreamShared": {
		reason: "the multi-schema sync warns from the PIPELINE, not the engine: the warning reports the " +
			"complement of what the refusing replica-identity preflight graded, and only the pipeline knows " +
			"that set. Moving it here would mean reconstructing the set from the table filter, which is the " +
			"approximation that left leaf partitions covered by nobody.",
		coveringTest: "TestPublicationExposureWarnsAfterThePreflightAndBeforeThePublication",
		coveringFile: "../../pipeline/coldstart_preflight_roster_test.go",
	},
}

func TestEveryAllTablesPublicationEmitterWarnsAboutExposure(t *testing.T) {
	// Keyed on the SQL LITERAL, not on a helper's identifier. The first cut
	// of this gate greped "ensureAllTablesPublication" and pinned the
	// universe at its two callers -- and there is a THIRD emitter of the
	// same statement, ensurePublication's no-scope arm, whose callers
	// include every StreamChanges. A gate named for "every caller" that
	// greps one function name is the coverage-narrower-than-its-name shape
	// this repo keeps paying for, and it discouraged the next grep for a
	// full day. The statement is the thing that causes the hazard, so the
	// statement is what the universe is derived from.
	const (
		emitted     = "CREATE PUBLICATION %s FOR ALL TABLES"
		publication = "ensureAllTablesPublication"
		// The two shapes that reach the audit: the engine-local warner used
		// by the backup path, and the pipeline-side warning, which reaches
		// the sync path through the ir.PublicationExposureAuditor surface
		// rather than through a call in this package.
		localWarn = "warnPublicationExposure"
		surface   = "AuditPublicationExposure"
	)

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	type site struct {
		fn   string
		file string
		line int
		ok   bool
	}
	var sites []site

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		for _, decl := range f.Decls {
			fn, isFn := decl.(*ast.FuncDecl)
			if !isFn || fn.Body == nil {
				continue
			}
			calls := map[string]token.Pos{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					calls[fun.Name] = call.Pos()
				case *ast.SelectorExpr:
					calls[fun.Sel.Name] = call.Pos()
				}
				return true
			})
			// A function EMITS a database-wide publication if it renders the
			// statement itself, or delegates to the helper that does.
			litAt := token.NoPos
			ast.Inspect(fn.Body, func(nd ast.Node) bool {
				lit, isLit := nd.(*ast.BasicLit)
				if !isLit || lit.Kind != token.STRING || !strings.Contains(lit.Value, emitted) {
					return true
				}
				if !litAt.IsValid() {
					litAt = lit.Pos()
				}
				return true
			})
			pubAt, creates := calls[publication]
			if !creates && !litAt.IsValid() {
				continue
			}
			if !creates {
				pubAt = litAt
			}
			_, warnsLocally := calls[localWarn]
			_, warnsViaSurface := calls[surface]
			sites = append(sites, site{
				fn:   fn.Name.Name,
				file: name,
				line: fset.Position(pubAt).Line,
				ok:   warnsLocally || warnsViaSurface,
			})
		}
	}

	// Anti-vacuity. Two callers exist; a walker that stopped seeing them
	// would report an empty roster and pass, which is the failure mode that
	// makes a gate worse than none. The floor is the whole universe on
	// purpose: a THIRD caller is exactly what this exists to catch, and a
	// caller REMOVED should be a deliberate edit here, not something a
	// margin absorbs.
	if len(sites) != 4 {
		names := make([]string, 0, len(sites))
		for _, s := range sites {
			names = append(names, s.file+":"+s.fn)
		}
		sort.Strings(names)
		t.Fatalf("found %d emitter(s) of a database-wide publication, expected exactly 4 (the two renderers "+
			"in publication.go, plus backup --chain-slot and the multi-schema sync snapshot, which delegate "+
			"to one of them): %v.\n\nIf you ADDED one, it must warn about the database-wide exposure before "+
			"creating the publication, and this count goes up. If you REMOVED one, say so here.",
			len(sites), names)
	}

	for _, s := range sites {
		if s.ok {
			continue
		}
		if ex, exempt := allTablesPublicationExempt[s.fn]; exempt {
			// An exemption is only as good as the thing it points at, so it
			// is not taken on faith: the covering gate must actually exist,
			// by name, in the file the exemption names. An exemption that
			// outlives its justification is how a roster quietly stops
			// meaning anything.
			body, rerr := os.ReadFile(ex.coveringFile)
			if rerr != nil {
				t.Errorf("%s is exempt because %q covers it, but %s is unreadable: %v",
					s.fn, ex.coveringTest, ex.coveringFile, rerr)
				continue
			}
			if !strings.Contains(string(body), "func "+ex.coveringTest+"(") {
				t.Errorf("%s is exempt because %q covers it, but %s no longer declares that test — "+
					"the exemption has outlived its justification and this caller is now ungated",
					s.fn, ex.coveringTest, ex.coveringFile)
			}
			continue
		}
		t.Errorf("%s:%d: %s emits a database-wide publication (renders the statement, or calls %s) "+
			"without auditing the exposure in the same function.\n"+
			"  A FOR ALL TABLES publication stops UPDATE and DELETE on every keyless permanent logged table in "+
			"the DATABASE, including ones this run never reads, while INSERT keeps working — so the failure "+
			"lands inside an unrelated application with nothing pointing back here.\n"+
			"  Call %s (engine-local, takes a *sql.DB) or route through ir.PublicationExposureAuditor, BEFORE "+
			"creating the publication.",
			s.file, s.line, s.fn, publication, localWarn)
	}
}
