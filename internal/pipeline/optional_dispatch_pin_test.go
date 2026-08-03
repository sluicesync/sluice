// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Compile-time pins for the pipeline-local optional interfaces
// (audit 2026-08-01 A1).
//
// internal/ir has a gate — TestEveryRuntimeDispatchedIRSurfaceHasACompileTimePin
// — requiring every `ir.Surface` discovered by type assertion to carry a
// `var _ ir.Surface = (*Engine)(nil)` pin in the engine that implements it.
// The pipeline declares 33 MORE optional interfaces of its own and asserts
// them against real engine values, and NONE of them had a pin. A method
// renamed on the engine side silently stops satisfying the interface, the
// assertion quietly returns false, and whatever the assertion guarded simply
// stops happening.
//
// WHY THEY WERE UNPINNED, because it is structural rather than an oversight:
// production `internal/pipeline` never imports an engine package — that is the
// layering tenet, and it is intact. So the pin cannot live beside the
// interface the way an ir pin lives beside the engine. It CAN live in a test
// file, which may import both, and that is what this file is.
//
// The audit's sharpest observation was about the defence, not the gap: the
// code claimed these shapes were "pinned by the pipeline unit tests", and
// those tests use a test-local stub that satisfies the interface BY
// CONSTRUCTION. A stub proves the pipeline calls the method it declared; it
// says nothing about whether any real engine still implements it. Only a pin
// against the concrete engine type does that.
//
// THE PREFLIGHT PROBERS ARE PINNED FIRST, deliberately. Each guards a
// REFUSAL — the foreign-table census, legacy inheritance, large objects,
// partitioned tables, row-level security, XID wraparound, replication
// capability. When one of those assertions silently stops matching, the
// preflight becomes a no-op and its refusal never fires: the migration
// proceeds past a check the operator believes ran. That is the
// silent-loss-shaped half of the 33, which is why it is the half that is
// closed here rather than frozen.
//
// The remaining interfaces are recorded in unpinnedPipelineSurfaces below with
// the reason, and the gate fails if a NEW one joins them unlisted.

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines/postgres"
)

// ---- the pins ----
//
// Every preflight prober below is satisfied by *postgres.SchemaReader. If a
// method is renamed or its signature changes on the engine side, THIS FILE
// stops compiling — which is the entire point, because the alternative is a
// type assertion that quietly returns false and a preflight that silently
// stops running.
var (
	_ foreignTablePreflightProber = (*postgres.SchemaReader)(nil)
	_ inheritancePreflightProber  = (*postgres.SchemaReader)(nil)
	_ largeObjectPreflightProber  = (*postgres.SchemaReader)(nil)
	_ partitionPreflightProber    = (*postgres.SchemaReader)(nil)
	_ rlsPreflightProber          = (*postgres.SchemaReader)(nil)
	_ xidWraparoundProber         = (*postgres.SchemaReader)(nil)
	_ replicationCapabilityProber = (*postgres.SchemaReader)(nil)
)

// unpinnedPipelineSurfaces is the FROZEN remainder: pipeline-local interfaces
// that are runtime-asserted and have no compile-time pin yet, each with the
// reason it is still on the list.
//
// It is a debt list, not an allowlist. Pinning one is a deletion from here,
// and the gate then holds it pinned forever. The list is frozen so a NEW
// interface cannot join it silently — which is the state that produced the
// audit finding.
//
// The common reason is simply "not yet done": these are Postgres CDC-reader
// and slot-lifecycle surfaces whose concrete types are reachable from a test
// file exactly as the probers' are, and pinning them is mechanical follow-up
// work rather than a design question.
var unpinnedPipelineSurfaces = map[string]string{
	"RawDefaultReader":            "not yet pinned",
	"chainAckController":          "not yet pinned",
	"currentRoleReporter":         "not yet pinned",
	"currentWALPositionReader":    "not yet pinned",
	"liveAddedTablesReader":       "not yet pinned",
	"liveAddedTablesWriter":       "not yet pinned",
	"lsnComparer":                 "not yet pinned",
	"lsnTrackerAttacher":          "not yet pinned",
	"lsnTrackerProvider":          "not yet pinned",
	"pollIntervalSetter":          "not yet pinned",
	"publicationAdder":            "not yet pinned",
	"publicationEnsurer":          "not yet pinned",
	"publicationNameSetter":       "not yet pinned",
	"publicationPostSlotVerifier": "not yet pinned",
	"replicationHeadroomProber":   "not yet pinned",
	"rowFilterHashSetter":         "not yet pinned",
	"schemaForwardModeSetter":     "not yet pinned",
	"schemaHistoryCachePrimer":    "not yet pinned",
	"shardPreflightProber":        "not yet pinned; the concrete type is a Vitess/PlanetScale reader, not the PG SchemaReader the other probers share",
	"slotDropper":                 "not yet pinned",
	"slotNameSetter":              "not yet pinned",
	"slotPositionReader":          "not yet pinned",
	"snapshotLSNExtractor":        "not yet pinned",
	"snapshotSlotOpener":          "not yet pinned",
	"stopFlagReader":              "not yet pinned",
	"targetSchemaSetter":          "not yet pinned",
}

// TestEveryRuntimeDispatchedPipelineSurfaceIsPinnedOrFrozen is the gate.
func TestEveryRuntimeDispatchedPipelineSurfaceIsPinnedOrFrozen(t *testing.T) {
	ifaces, asserted, pinned := scanPipelineSurfaces(t)

	// Anti-vacuity on all three populations: a walk that stops finding any of
	// them would let this gate pass on nothing, which is precisely the state
	// it was written to end.
	if len(ifaces) < 20 {
		t.Fatalf("found only %d interface declarations in package pipeline; the scan broke", len(ifaces))
	}
	if len(asserted) < 20 {
		t.Fatalf("found only %d runtime-asserted pipeline surfaces; the AST walk is not reaching the tree "+
			"and this gate is vacuous", len(asserted))
	}
	if len(pinned) < 5 {
		t.Fatalf("found only %d compile-time pins; the walk is not seeing this file's var block and the "+
			"gate is vacuous", len(pinned))
	}

	var missing []string
	for name := range asserted {
		if pinned[name] {
			continue
		}
		if _, frozen := unpinnedPipelineSurfaces[name]; frozen {
			continue
		}
		missing = append(missing, name)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("pipeline-local interface(s) %v are dispatched at RUNTIME with no compile-time pin and are "+
			"not in unpinnedPipelineSurfaces.\n\n"+
			"A type assertion against an engine value is capability DISCOVERY: when the engine's method is "+
			"renamed the assertion returns false and whatever it guarded stops happening, silently. Add a "+
			"`var _ <iface> = (*<engine type>)(nil)` pin to this file, or add the name to "+
			"unpinnedPipelineSurfaces with the reason it cannot be pinned yet.\n\n"+
			"Note a pipeline unit-test stub does NOT count: it satisfies the interface by construction and "+
			"says nothing about any real engine.", missing)
	}

	// The other direction: a frozen entry that has since been pinned, or whose
	// interface is gone, must leave the list — otherwise the debt count lies
	// and a re-introduced gap hides behind a stale name.
	var stale []string
	for name := range unpinnedPipelineSurfaces {
		if pinned[name] {
			stale = append(stale, name+" (now pinned)")
			continue
		}
		if !asserted[name] {
			stale = append(stale, name+" (no longer runtime-asserted)")
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("unpinnedPipelineSurfaces is stale — remove: %v", stale)
	}
}

// scanPipelineSurfaces walks package pipeline's non-test sources for interface
// declarations and runtime type assertions against them, plus this file's
// compile-time pins.
func scanPipelineSurfaces(t *testing.T) (ifaces, asserted, pinned map[string]bool) {
	t.Helper()
	ifaces, asserted, pinned = map[string]bool{}, map[string]bool{}, map[string]bool{}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	fset := token.NewFileSet()
	parse := func(name string) *ast.File {
		f, perr := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if perr != nil {
			return nil
		}
		return f
	}

	// Pass 1: interface declarations, from PRODUCTION files only — a
	// test-local interface is not a capability surface.
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f := parse(e.Name())
		if f == nil {
			continue
		}
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if _, isIface := ts.Type.(*ast.InterfaceType); isIface {
				ifaces[ts.Name.Name] = true
			}
			return true
		})
	}

	// Pass 2: assertions (production only) and pins (this file).
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		isTest := strings.HasSuffix(e.Name(), "_test.go")
		if isTest && e.Name() != "optional_dispatch_pin_test.go" {
			continue
		}
		f := parse(e.Name())
		if f == nil {
			continue
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.TypeAssertExpr:
				if isTest {
					return true
				}
				if id, ok := node.Type.(*ast.Ident); ok && ifaces[id.Name] {
					asserted[id.Name] = true
				}
			case *ast.CaseClause:
				if isTest {
					return true
				}
				for _, typ := range node.List {
					if id, ok := typ.(*ast.Ident); ok && ifaces[id.Name] {
						asserted[id.Name] = true
					}
				}
			case *ast.ValueSpec:
				if len(node.Names) != 1 || node.Names[0].Name != "_" || node.Type == nil {
					return true
				}
				if id, ok := node.Type.(*ast.Ident); ok {
					pinned[id.Name] = true
				}
			}
			return true
		})
	}
	return ifaces, asserted, pinned
}
