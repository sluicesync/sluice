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
	// A2-4: the replica-identity preflight cannot run inside the copy loop
	// (the spanning snapshot has already created the FOR ALL TABLES
	// publication by then), so it has its own pre-snapshot pass. Listed here
	// because this roster only sees the functions it is told about — the
	// first cut of the A2-4 fix added the call in a helper OUTSIDE this list
	// and the gate went green while still carrying the exemption.
	"(*Streamer).preflightMultiNamespaceReplicaIdentity",
	"(*Streamer).preflightOneNamespaceReplicaIdentity",
	"(*Streamer).coldStartMultiDatabase",
	"(*Streamer).coldStartReadOneDatabaseSchema",
	"(*Streamer).coldStartCopyOneDatabase",
}

// coldStartPreflightMultiDatabaseExempt is the fail-by-default exemption map:
// a reference preflight absent from the fan-out passes ONLY with an entry
// here. Every entry today is a filed gap, not a design decision — the A2-2
// roster sweep found them; each cites where it is tracked.
var coldStartPreflightMultiDatabaseExempt = map[string]coldStartPreflightExemption{
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

// TestMultiNamespaceReplicaIdentityRunsBeforeThePublicationExists binds the
// A2-4 fix's load-bearing property, which the roster above cannot see.
//
// Two mutation runs taught this. Deleting the call site in
// coldStartMultiDatabase left the roster green, because the roster asks
// "does a listed fan-out function call preflightSourceReplicaIdentity?" and
// the pre-pass helper still did. And before the helper was added to that
// list, the roster was green while still carrying the exemption. A roster
// over a hand-listed function set answers a narrower question than its name
// suggests; this states the two facts that actually matter.
//
//  1. coldStartMultiDatabase CALLS the pre-pass. Nothing else does.
//  2. It calls it BEFORE opening the spanning snapshot. That ordering IS
//     the fix: the snapshot open creates the FOR ALL TABLES publication,
//     and a published table with no replica identity is one Postgres
//     refuses to UPDATE — so a refusal that arrives afterwards has already
//     let sluice break the operator's application.
//
// TestUnselectedNamespaceExposureWarnsBeforeThePublicationExists is A2-4b's
// call-site gate, and it exists because its two immediate predecessors both
// shipped a working helper that nothing reached.
//
// A2-3's first pin graded the two slot-aware helpers and stayed green when a
// CALL SITE was reverted to the unnamed opener. A2-4's roster asked whether a
// listed function calls the preflight and stayed green when the call site was
// deleted, because an orphaned helper still called it. Both were found only by
// mutating. A warning is even easier to lose than a refusal: nothing fails
// when it stops being emitted, so there is no symptom at all.
//
// The ordering matters for the same reason it does for the refusing sibling.
// The spanning snapshot open is what creates the FOR ALL TABLES publication,
// which is what breaks the unselected schemas' writes. A warning emitted after
// it describes damage already done.
func TestUnselectedNamespaceExposureWarnsBeforeThePublicationExists(t *testing.T) {
	const (
		file     = "streamer_multidb.go"
		warn     = "warnUnselectedNamespaceExposure"
		snapshot = "openMultiDatabaseSnapshotStreamWithOptionalSlot"
		entry    = "coldStartMultiDatabase"
	)
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}

	var fn *ast.FuncDecl
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if ok && fd.Name.Name == entry && fd.Body != nil {
			fn = fd
			break
		}
	}
	if fn == nil {
		t.Fatalf("%s declares no %s — this gate has lost its subject", file, entry)
	}

	warnAt, snapshotAt := token.NoPos, token.NoPos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			name = fun.Name
		case *ast.SelectorExpr:
			name = fun.Sel.Name
		}
		switch name {
		case warn:
			if !warnAt.IsValid() {
				warnAt = call.Pos()
			}
		case snapshot:
			if !snapshotAt.IsValid() {
				snapshotAt = call.Pos()
			}
		}
		return true
	})

	if !warnAt.IsValid() {
		t.Fatalf("%s does not call %s — a multi-schema sync would silently break UPDATE and DELETE on tables in schemas the operator never selected, with nothing said about it (audit 2026-09-01 A2-4b)",
			entry, warn)
	}
	// Anti-vacuity: without the snapshot call the ordering claim below is
	// unverifiable and must fail rather than pass by finding nothing.
	if !snapshotAt.IsValid() {
		t.Fatalf("%s no longer calls %s, so the ordering this gate asserts cannot be checked — re-anchor it on whatever now creates the publication",
			entry, snapshot)
	}
	if warnAt > snapshotAt {
		t.Errorf("%s calls %s AFTER %s (%s vs %s); the spanning snapshot creates the FOR ALL TABLES publication, so a warning after it describes exposure that has already happened (audit 2026-09-01 A2-4b)",
			entry, warn, snapshot, fset.Position(warnAt), fset.Position(snapshotAt))
	}
}

func TestMultiNamespaceReplicaIdentityRunsBeforeThePublicationExists(t *testing.T) {
	const (
		file      = "streamer_multidb.go"
		preflight = "preflightMultiNamespaceReplicaIdentity"
		snapshot  = "openMultiDatabaseSnapshotStreamWithOptionalSlot"
		entry     = "coldStartMultiDatabase"
	)
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}

	var fn *ast.FuncDecl
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if ok && fd.Name.Name == entry && fd.Body != nil {
			fn = fd
			break
		}
	}
	if fn == nil {
		t.Fatalf("%s declares no %s — this gate has lost its subject", file, entry)
	}

	preflightAt, snapshotAt := token.NoPos, token.NoPos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			name = fun.Name
		case *ast.SelectorExpr:
			name = fun.Sel.Name
		}
		switch name {
		case preflight:
			if !preflightAt.IsValid() {
				preflightAt = call.Pos()
			}
		case snapshot:
			if !snapshotAt.IsValid() {
				snapshotAt = call.Pos()
			}
		}
		return true
	})

	if !preflightAt.IsValid() {
		t.Fatalf("%s does not call %s — a multi-namespace sync would publish a keyless table and break the source application's UPDATE/DELETE (audit 2026-09-01 A2-4)",
			entry, preflight)
	}
	// Anti-vacuity: if the snapshot open moved or was renamed, the ordering
	// claim below is unverifiable and must fail rather than pass silently.
	if !snapshotAt.IsValid() {
		t.Fatalf("%s no longer calls %s, so the ordering this gate asserts cannot be checked — re-anchor it on whatever now creates the publication",
			entry, snapshot)
	}
	if preflightAt > snapshotAt {
		t.Errorf("%s calls %s AFTER %s (%s vs %s); the spanning snapshot creates the FOR ALL TABLES publication, so the refusal must come first or it arrives after sluice has already exposed the source application (audit 2026-09-01 A2-4)",
			entry, preflight, snapshot, fset.Position(preflightAt), fset.Position(snapshotAt))
	}
}
