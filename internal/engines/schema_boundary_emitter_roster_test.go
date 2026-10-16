// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package engines

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// schemaBoundaryOwners maps an emitting type that is not itself a CDC reader
// to the reader that owns it, per engine package, with the reason.
var schemaBoundaryOwners = map[string]struct{ owner, why string }{
	"mysql.vstreamSnapshotStream": {
		owner: "vstreamSnapshotChanges",
		why:   "the cold-start VStream snapshot stream builds the boundaries; its CDC-half facade is the reader the streamer wires (cdc_session_tz_cast.go)",
	},
	"pgtrigger.firstTouch": {
		owner: "CDCReader",
		why:   "the pump-owned helper that builds the reader's first-touch boundaries (cdc_first_touch.go)",
	},
}

// The pipeline's two surfaces a boundary-emitting reader must declare, by
// method name (internal/pipeline: schemaSeedSetter, schemaBoundaryEmitter).
// The compile-time pins in pipeline/optional_dispatch_pin_test.go hold the
// names to the interfaces.
var schemaBoundarySurfaces = []string{"SetSchemaSeed", "EmitsSchemaBoundaries"}

// TestSchemaBoundaryEmitterRoster_EveryEmitterDeclaresASurface is the roster
// the re-review of the postgres-trigger first-touch follow-ups asked for. A
// CDC reader that builds an ir.SchemaSnapshot feeds boundaries to the
// pipeline's schema intercepts, and those judge each boundary against a prior
// that the warm-resume load supplies — which the streamer runs only for a
// reader declaring schemaSeedSetter or schemaBoundaryEmitter. A reader that
// emits boundaries and declares neither resumes with no prior, which is
// TRIGGER-WARM-RESUME-NO-PRIOR: a narrowing --type-override refused on every
// restart.
//
// WHAT IT REACHES: every non-test file of every engine package under
// internal/engines, by AST — each `ir.SchemaSnapshot{…}` composite literal,
// attributed to the receiver type of the method that builds it (or that
// type's owner, [schemaBoundaryOwners]); a builder that is a plain function
// fails until classified. It grades DECLARATION (the owner has a method of
// one of the two names, and is a CDC reader — it has StreamChanges), not that
// the method does the right thing. It also binds the reader-level
// declaration to the engine-level one the Shape A door reads: a trigger-CDC
// engine package whose reader declares EmitsSchemaBoundaries must declare
// Engine.CDCReaderEmitsSchemaBoundaries.
//
// WHAT IT DOES NOT REACH, stated: ir.SchemaSnapshot builders outside
// internal/engines — the pipeline's shard-consolidation intercept
// (shard_consolidation_intercept.go) and boundary router
// (shard_consolidation_router.go), and chain restore's synthetic versions
// (backup/chain_restore.go, chain_restore_history_order.go). Those are not
// CDC readers and need no warm-resume surface; they are out of this gate's
// scope, not covered by it.
func TestSchemaBoundaryEmitterRoster_EveryEmitterDeclaresASurface(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read engines dir: %v", err)
	}
	sites, receivers, twinBound := 0, map[string]bool{}, 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pkg := e.Name()
		methods, emitters := scanSchemaBoundaryPackage(t, pkg)
		for _, em := range emitters {
			sites++
			key := pkg + "." + em.receiver
			if em.receiver == "" {
				t.Errorf("%s: %s builds an ir.SchemaSnapshot in a plain function; attribute it to its reader in schemaBoundaryOwners", pkg, em.pos)
				continue
			}
			receivers[key] = true
			owner := em.receiver
			if o, ok := schemaBoundaryOwners[key]; ok {
				owner = o.owner
			}
			declared := false
			for _, m := range schemaBoundarySurfaces {
				if methods[owner][m] {
					declared = true
				}
			}
			if !declared {
				t.Errorf("%s: %s builds an ir.SchemaSnapshot (in a method of %s) but %s declares neither %s — "+
					"the streamer's warm resume will not load the prior its boundaries are judged against "+
					"(TRIGGER-WARM-RESUME-NO-PRIOR)", pkg, em.pos, em.receiver, owner, strings.Join(schemaBoundarySurfaces, " nor "))
			}
			// The owner must BE a CDC reader: an owner map that points at a
			// helper declares the surface on something the streamer never
			// type-asserts.
			if !methods[owner]["StreamChanges"] {
				t.Errorf("%s: %s is the owner of the boundaries %s builds but has no StreamChanges method — it is not the CDC "+
					"reader the streamer wires, so a surface declared on it is never seen", pkg, owner, em.receiver)
			}
		}
		// The engine-level twin: a TRIGGER-CDC engine whose reader emits
		// boundaries must say so on its Engine, because the Shape A
		// coordination door decides before any reader is open
		// (TRIGGER-SHAPE-A-FORWARD). Nothing else binds the reader-level and
		// engine-level declarations.
		bindsTwin := packageDeclaresBoundaryEmitter(methods) && packageIsTriggerCDC(t, pkg)
		if bindsTwin {
			twinBound++
		}
		if bindsTwin && !methods["Engine"]["CDCReaderEmitsSchemaBoundaries"] {
			t.Errorf("%s: its CDC reader declares EmitsSchemaBoundaries and the engine is a trigger-CDC engine (ir.CDCTriggers), "+
				"but Engine has no CDCReaderEmitsSchemaBoundaries method — live cross-shard coordination would forward its "+
				"boundaries (TRIGGER-SHAPE-A-FORWARD)", pkg)
		}
	}
	for key := range schemaBoundaryOwners {
		if !receivers[key] {
			t.Errorf("schemaBoundaryOwners names %s, which builds no ir.SchemaSnapshot — drop the stale entry", key)
		}
	}
	// The engine-twin rule must have reached at least one package
	// (postgres-trigger today), or a blind CDCTriggers match made it vacuous.
	if twinBound < 1 {
		t.Fatalf("the engine-twin rule reached no trigger-CDC package whose reader emits boundaries; floor 1 — the CDCTriggers match is blind")
	}
	// Anti-vacuity floor: five building sites over five receiver types today
	// (the binlog, VStream and VStream-snapshot lanes, pgoutput, and the
	// postgres-trigger first-touch helper).
	if sites < 5 || len(receivers) < 5 {
		keys := make([]string, 0, len(receivers))
		for k := range receivers {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		t.Fatalf("found %d ir.SchemaSnapshot building site(s) over %d receiver type(s) %v; floor 5 and 5 — the walk is vacuous", sites, len(receivers), keys)
	}
}

// packageDeclaresBoundaryEmitter reports whether any type in the package
// declares EmitsSchemaBoundaries.
func packageDeclaresBoundaryEmitter(methods map[string]map[string]bool) bool {
	for _, ms := range methods {
		if ms["EmitsSchemaBoundaries"] {
			return true
		}
	}
	return false
}

// triggerCDCDecl matches a CDC capability declared as ir.CDCTriggers, in a
// struct literal (CDC: ir.CDCTriggers) or an assignment (c.CDC = ir.CDCTriggers).
var triggerCDCDecl = regexp.MustCompile(`\bCDC\s*[:=]\s*ir\.CDCTriggers\b`)

// packageIsTriggerCDC reports whether the package's non-test source declares
// its CDC capability as ir.CDCTriggers (source inspection: this package cannot
// import the engines, which import it).
func packageIsTriggerCDC(t *testing.T, pkg string) bool {
	t.Helper()
	found := false
	err := filepath.Walk(pkg, func(path string, info os.FileInfo, werr error) error {
		if werr != nil || info.IsDir() || found || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return werr
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		found = triggerCDCDecl.MatchString(string(b))
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", pkg, err)
	}
	return found
}

type schemaBoundaryEmitterSite struct {
	receiver string // the building method's receiver type, "" for a plain function
	pos      string
}

// scanSchemaBoundaryPackage returns, for one engine package, each receiver
// type's declared method names and every ir.SchemaSnapshot building site.
func scanSchemaBoundaryPackage(t *testing.T, pkg string) (map[string]map[string]bool, []schemaBoundaryEmitterSite) {
	t.Helper()
	methods := map[string]map[string]bool{}
	var sites []schemaBoundaryEmitterSite
	fset := token.NewFileSet()
	err := filepath.Walk(pkg, func(path string, info os.FileInfo, werr error) error {
		if werr != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return werr
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok {
				continue
			}
			recv := funcReceiverType(fn)
			if recv != "" {
				if methods[recv] == nil {
					methods[recv] = map[string]bool{}
				}
				methods[recv][fn.Name.Name] = true
			}
			if fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				if sel, ok := lit.Type.(*ast.SelectorExpr); ok {
					if x, ok := sel.X.(*ast.Ident); ok && x.Name == "ir" && sel.Sel.Name == "SchemaSnapshot" {
						sites = append(sites, schemaBoundaryEmitterSite{receiver: recv, pos: fset.Position(lit.Pos()).String()})
					}
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", pkg, err)
	}
	return methods, sites
}

// funcReceiverType is fn's receiver type name ([receiverTypeName], shared
// with the skip-ledger roster), "" for a plain function.
func funcReceiverType(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	return receiverTypeName(fn.Recv.List[0].Type)
}
