// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// The source-side cold-start preflights (RLS, replication role, slot
// headroom, XID wraparound, partitioning, inheritance, replica identity)
// live in the single-namespace cold start ([Streamer.coldStartReadSourceSchema])
// and migrate's schema-read phase ([Migrator.phaseReadSourceSchema]). The
// multi-namespace `sync start` fan-out ([Streamer.coldStartMultiDatabase] →
// [Streamer.coldStartCopyOneDatabase]) is a THIRD cold-start entry point with
// its own schema read, and nothing tied its preflight set to the other two —
// so the Bug 100 partition and item-68b inheritance refusals reached migrate
// and the single-schema sync but not `sync start --include-schema`, which
// flattened a partitioned parent AND copied its leaves, then froze the parent
// at exit 0 (audit 2026-09-01 A2-2). The moved-door sibling shape.
//
// This gate derives the universe from the AST rather than a hand list: every
// `preflight*` symbol the two reference functions call must ALSO be called
// from the multi-namespace entry points, or carry an explicit, fail-by-default
// exemption below. Both directions are guarded — an unreached, unexempted
// preflight fails; so does an exemption for a symbol the reference set does
// not call (a phantom), or one the fan-out now reaches (a stale exemption that
// would hide a later regression).
//
// Scope, stated so the name cannot be read wider than the truth: the roster
// matches identifiers with the `preflight` prefix only. Advisory `warn*`
// helpers (warnForeignTables) are outside it; so are the target-side and
// emit-side preflights, which the fan-out already shares with its siblings
// through migcore.

// coldStartPreflightExemptionClass says WHY a reference preflight is allowed
// to be absent from the multi-namespace fan-out. The class is load-bearing:
// a filed gap is a known defect with a tracker reference, not a design
// decision, and must not be mistaken for one.
type coldStartPreflightExemptionClass int

const (
	// exemptArchitectural: the preflight cannot or must not apply to the
	// multi-namespace fan-out by design (state the mechanism in the reason).
	exemptArchitectural coldStartPreflightExemptionClass = iota
	// exemptFiledGap: the preflight DOES apply and is not yet wired; the
	// reason names the audit/backlog entry that tracks it. Removing the
	// entry once wired is what this gate enforces (a stale exemption fails).
	exemptFiledGap
)

type coldStartPreflightExemption struct {
	class  coldStartPreflightExemptionClass
	reason string
}

// coldStartPreflightReferenceFuncs are the two sibling cold-start entry
// points whose source-side preflight set defines the roster.
var coldStartPreflightReferenceFuncs = []string{
	"(*Streamer).coldStartReadSourceSchema",
	"(*Migrator).phaseReadSourceSchema",
}

// coldStartPreflightMultiDatabaseFuncs are the multi-namespace fan-out's
// cold-start functions; a reference preflight is "reached" when any of them
// calls it (the once-per-run ones belong in coldStartMultiDatabase, the
// per-namespace ones in coldStartReadOneDatabaseSchema — the fan-out's
// schema-read step, where the scoped reader is still open — or in
// coldStartCopyOneDatabase for anything that needs the target writers).
var coldStartPreflightMultiDatabaseFuncs = []string{
	"(*Streamer).coldStartMultiDatabase",
	"(*Streamer).coldStartReadOneDatabaseSchema",
	"(*Streamer).coldStartCopyOneDatabase",
}

// coldStartPreflightMultiDatabaseExempt is the fail-by-default exemption map:
// a reference preflight absent from the fan-out passes ONLY with an entry
// here. Every entry today is a filed gap, not a design decision — the A2-2
// roster sweep found them; each cites where it is tracked.
var coldStartPreflightMultiDatabaseExempt = map[string]coldStartPreflightExemption{
	"preflightSourceReplicaIdentity": {exemptFiledGap, "audit 2026-09-01 A2-4: the fan-out ensures the FOR ALL TABLES publication before " +
		"any replica-identity check, so a keyless table breaks the SOURCE application's UPDATE/DELETE; " +
		"must run per namespace BEFORE the spanning snapshot open (which creates the publication), not inside the copy loop."},
	"preflightRLS": {exemptFiledGap, "audit 2026-09-01 A2-2 roster sweep (source-side RLS): a BYPASSRLS-less role reads a " +
		"silently-filtered snapshot; applies per namespace against the scoped reader, same shape as the partition preflight."},
	"preflightSourceReplication": {exemptFiledGap, "audit 2026-09-01 A2-2 roster sweep: the ADR-0075 spanning snapshot creates a " +
		"logical slot, so the REPLICATION-role refusal applies once per run, before the spanning open."},
	"preflightReplicationHeadroom": {exemptFiledGap, "audit 2026-09-01 A2-2 roster sweep: applies once per run and must run BEFORE " +
		"the spanning snapshot open — inside the copy loop it would count sluice's own freshly-created slot and refuse falsely."},
	"preflightSourceXIDWraparound": {exemptFiledGap, "audit 2026-09-01 A2-2 roster sweep: a database-level probe; applies once per run " +
		"(all selected PG schemas share one database), not per namespace."},
}

func TestColdStartPreflightRoster_MultiDatabaseReachesEverySibling(t *testing.T) {
	calls := discoverPreflightCallsByFunc(t)

	reference := unionPreflightCalls(t, calls, coldStartPreflightReferenceFuncs)
	reached := unionPreflightCalls(t, calls, coldStartPreflightMultiDatabaseFuncs)

	// Anti-vacuity floors. The reference set is the seven source-side
	// preflights the single-namespace cold start runs today; a genuine
	// addition raises it (and shows up below as unreached-and-unexempted
	// first), a matcher regression drops it. The reached floor is
	// deliberately ONE, not the two A2-2 wired: with the floor at the wired
	// count, deleting a single call tripped the floor before the forward
	// check below ever ran, so the mutation run was grading the floor, not
	// the gate (the step-5 trap). At one, a single deletion reaches the
	// forward check; only a matcher that finds nothing at all trips here.
	if len(reference) < 7 {
		t.Fatalf("anti-vacuity: expected >=7 preflight* symbols across %v, found %d: %v — the AST matcher is likely broken",
			coldStartPreflightReferenceFuncs, len(reference), sortedPreflightKeys(reference))
	}
	var reachedReference int
	for sym := range reference {
		if _, ok := reached[sym]; ok {
			reachedReference++
		}
	}
	if reachedReference < 1 {
		t.Fatalf("anti-vacuity: expected >=1 reference preflight reached from %v, found none (reached=%v) — the AST matcher is likely broken",
			coldStartPreflightMultiDatabaseFuncs, sortedPreflightKeys(reached))
	}

	// Forward: every reference preflight is reached by the fan-out or
	// carries an exemption with a stated reason.
	var missing []string
	for _, sym := range sortedPreflightKeys(reference) {
		if _, ok := reached[sym]; ok {
			continue
		}
		ex, ok := coldStartPreflightMultiDatabaseExempt[sym]
		if !ok {
			missing = append(missing, sym)
			continue
		}
		switch ex.class {
		case exemptArchitectural:
			if strings.TrimSpace(ex.reason) == "" {
				t.Errorf("architectural exemption for %s has no reason; the mechanism is the load-bearing part", sym)
			}
		case exemptFiledGap:
			// A filed gap must be findable: the reason names the audit finding,
			// bug, or roadmap item that tracks it, so nobody reads the map as a
			// list of design decisions.
			if !strings.Contains(ex.reason, "audit ") && !strings.Contains(ex.reason, "Bug ") && !strings.Contains(ex.reason, "roadmap item") {
				t.Errorf("filed-gap exemption for %s cites no tracker (want \"audit <date> <id>\", \"Bug N\", or \"roadmap item N\"): %q", sym, ex.reason)
			}
		default:
			t.Errorf("exemption for %s has an unknown class %d", sym, ex.class)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("source-side preflight(s) called from %v but NOT from the multi-namespace cold start %v and not exempted:\n  %s\n\n"+
			"Either call each from coldStartReadOneDatabaseSchema (per namespace, against the scoped reader + post-filter schema) / "+
			"coldStartMultiDatabase (once per run), or add a coldStartPreflightMultiDatabaseExempt entry with a class and a reason "+
			"(exemptArchitectural with the mechanism, or exemptFiledGap with the tracker reference).",
			coldStartPreflightReferenceFuncs, coldStartPreflightMultiDatabaseFuncs, strings.Join(missing, "\n  "))
	}

	// Reverse: an exemption must name a live reference preflight that the
	// fan-out does NOT reach. A phantom (not in the reference set) is a typo
	// or a removed preflight; a stale one (now reached) would silently
	// re-cover a future regression the moment the call is deleted again.
	for _, sym := range sortedPreflightKeys(coldStartPreflightMultiDatabaseExempt) {
		if _, ok := reference[sym]; !ok {
			t.Errorf("phantom exemption %q: no such preflight is called from %v — remove it or fix the name", sym, coldStartPreflightReferenceFuncs)
			continue
		}
		if _, ok := reached[sym]; ok {
			t.Errorf("stale exemption %q: the multi-namespace cold start now calls it — remove the exemption so the gate holds the call", sym)
		}
	}
}

// discoverPreflightCallsByFunc parses every non-test Go file in the pipeline
// package and returns, per top-level function (keyed by [funcDeclName]), the
// set of called identifiers with the `preflight` prefix — both the bare form
// `preflightX(...)` and the method form `recv.preflightX(...)`.
func discoverPreflightCallsByFunc(t *testing.T) map[string]map[string]struct{} {
	t.Helper()
	out := make(map[string]map[string]struct{})
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read pipeline dir: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %q: %v", name, err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			funcName := funcDeclName(fd)
			set := out[funcName]
			if set == nil {
				set = make(map[string]struct{})
				out[funcName] = set
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				var sym string
				switch fn := call.Fun.(type) {
				case *ast.Ident:
					sym = fn.Name
				case *ast.SelectorExpr:
					sym = fn.Sel.Name
				default:
					return true
				}
				if strings.HasPrefix(sym, "preflight") {
					set[sym] = struct{}{}
				}
				return true
			})
		}
	}
	return out
}

// unionPreflightCalls returns the union of the preflight call sets of the
// named functions, failing if any of them is absent from the AST — a rename
// of a roster anchor must fail here rather than silently empty the set.
func unionPreflightCalls(t *testing.T, calls map[string]map[string]struct{}, funcs []string) map[string]struct{} {
	t.Helper()
	union := make(map[string]struct{})
	for _, fn := range funcs {
		set, ok := calls[fn]
		if !ok {
			t.Fatalf("roster anchor %q not found in the pipeline AST — renamed? update the roster's function lists", fn)
		}
		for sym := range set {
			union[sym] = struct{}{}
		}
	}
	return union
}

func sortedPreflightKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
