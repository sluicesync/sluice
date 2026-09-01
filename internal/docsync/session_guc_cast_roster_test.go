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
	"strconv"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
)

// WHICH CDC LANES REFUSE A SESSION-GUC-RESOLVED CAST, kept honest against
// the engine registry and against the lanes' own declarations (audit
// 2026-08-31 SL-2/SL-3).
//
// # The class
//
// Some `ALTER COLUMN TYPE` shapes are converted by the database using a
// value the EXECUTING SESSION holds, not one the replication wire carries:
// PostgreSQL's `timestamp`⇄`timestamptz` and `time`⇄`timetz` swaps resolve
// against `TimeZone`; MySQL's `TIMESTAMP`⇄`DATETIME` `MODIFY` resolves
// against `time_zone`. Forwarding such a DDL re-casts the TARGET's
// pre-existing rows against a DIFFERENT session's setting, so every row
// written before the ALTER silently diverges while the sync exits 0 and
// `verify --depth count` matches. Each lane must refuse instead.
//
// # Why a CROSS-ENGINE roster and not two per-engine tests
//
// Because per-engine fixes leak, and this class has leaked twice already.
// The PG refusal shipped in v0.134.0 having enumerated the MySQL member in
// a comment inside a PG file — where no MySQL-lane change would ever trip
// over it — and the same release narrowed the PG array half back out. The
// roster's job is the CROSS-lane question a per-engine test cannot ask:
// does every lane that could carry this class have a declaration AND a
// refusal wired to it, and does a newly-registered engine have to answer.
//
// # What it grades, stated so it cannot be read as broader
//
//  1. Registry coverage — every name in [engines.Names] is served by a
//     declared lane or carries a written exemption. A new engine fails
//     until someone classifies it.
//  2. Emitter coverage — every FILE under internal/engines that builds an
//     `ir.SchemaSnapshot` (the boundary a forward path consumes) is a
//     declared refusal site of some lane. This is the half that catches a
//     new lane INSIDE an existing engine, which is exactly how MySQL's two
//     VStream emitters would have been missed.
//  3. Declaration non-vacuity — each lane's pair universe is read out of
//     that lane's OWN declaration function by AST (its `return …, true`
//     arms, the type/OID symbols it names, the pair labels it emits) and
//     floored. Removing a pair from a declaration fails the lane's cell.
//  4. Wiring — each declared refusal site must actually call the lane's
//     refusal predicate, and the lane's refusal message must name the
//     session-GUC MECHANISM and the drained-model remedy. A lane that
//     declares pairs and refuses nowhere is a FAILURE, not a pass.
//
// It does NOT grade whether a lane's pair set is COMPLETE for its engine —
// that is each lane's own family gate
// (postgres.TestSessionTZSwapGate_EveryZoneSiblingPair,
// mysql.TestSessionTZSwapPair_EveryZoneSiblingPair), both of which derive
// their universe from the engine's projections rather than from the
// predicate. Nor does it prove the refusal fires end-to-end; that is the
// integration pin on each lane.
//
// The grading logic is a pure function so it can be driven with synthetic
// inputs by TestSessionGUCCastRoster_MetaFailsClosed below — the gate on
// the gate, which is how the fail-closed posture is verified without
// mutating production code.

// gucCastRefusalSite is one file that emits a schema boundary and must
// therefore reach the lane's refusal predicate before doing so.
type gucCastRefusalSite struct {
	// file is the path, relative to internal/docsync, of the emitter.
	file string
	// callee is the predicate the emitter must call — the lane's own
	// wiring, named rather than assumed.
	callee string
}

// gucCastLane is one CDC lane: a schema-boundary path with its own
// declaration of session-GUC-resolved cast pairs and its own refusal.
type gucCastLane struct {
	// id is the lane's stable name, used in failure messages.
	id string

	// engineNames are the registry names this lane serves.
	engineNames []string

	// declFile / declFunc locate the lane's PAIR DECLARATION — the single
	// function whose arms define which type changes are session-GUC casts.
	// The roster reads the universe out of this function; it never
	// hand-lists the pairs.
	declFile string
	declFunc string

	// minArms / minSymbols / minLabels are the lane's anti-vacuity floors,
	// each with its reason in floorReason. They are FLOORS, so adding a
	// pair never breaks the gate while removing one does.
	minArms     int
	minSymbols  int
	minLabels   int
	floorReason string

	// refusalSites are the boundary emitters that must reach the predicate.
	refusalSites []gucCastRefusalSite

	// messageMarkers must each appear in some string literal of the refusal
	// sites' package — the honesty half. A refusal that does not name the
	// mechanism leaves the operator unable to tell whether they were
	// exposed.
	messageMarkers []string
}

// sessionGUCCastLanes is the lane roster. Two engines, four lanes today.
var sessionGUCCastLanes = []gucCastLane{
	{
		id:          "postgres-pgoutput",
		engineNames: []string{"postgres"},
		declFile:    filepath.Join("..", "engines", "postgres", "cdc_relations.go"),
		declFunc:    "sessionTZSwapPair",
		minArms:     2,
		minSymbols:  4,
		minLabels:   4,
		floorReason: "two return arms (time⇄timetz, timestamp⇄timestamptz), the four distinct pgtype OID constants they name, and the four label fragments the scalar/array pair names are concatenated from",
		refusalSites: []gucCastRefusalSite{
			// The predicate is consumed by unforwardableTypmodColumn, which
			// checkSchemaRace calls before the boundary is emitted; both
			// live in the declaring file.
			{file: filepath.Join("..", "engines", "postgres", "cdc_relations.go"), callee: "sessionTZSwapPair"},
			{file: filepath.Join("..", "engines", "postgres", "cdc_reader.go"), callee: "checkSchemaRace"},
		},
		messageMarkers: []string{"TimeZone", "drained model"},
	},
	{
		id:          "mysql-binlog",
		engineNames: []string{"mysql", "mariadb", "planetscale", "vitess"},
		declFile:    filepath.Join("..", "engines", "mysql", "cdc_session_tz_cast.go"),
		declFunc:    "sessionTZSwapPair",
		minArms:     1,
		minSymbols:  2,
		minLabels:   1,
		floorReason: "one return arm (MySQL has exactly one zone-aware temporal), the two ir types it names (ir.Timestamp / ir.DateTime), and the one pair label",
		refusalSites: []gucCastRefusalSite{
			{file: filepath.Join("..", "engines", "mysql", "cdc_reader.go"), callee: "unforwardableSessionTZColumn"},
		},
		messageMarkers: []string{"time_zone", "drained model"},
	},
	{
		id:          "mysql-vstream",
		engineNames: nil, // served by the same registry names as mysql-binlog
		declFile:    filepath.Join("..", "engines", "mysql", "cdc_session_tz_cast.go"),
		declFunc:    "sessionTZSwapPair",
		minArms:     1,
		minSymbols:  2,
		minLabels:   1,
		floorReason: "shares the binlog lane's declaration by design — one predicate, three emitters — so it carries the same floors",
		refusalSites: []gucCastRefusalSite{
			{file: filepath.Join("..", "engines", "mysql", "cdc_vstream.go"), callee: "unforwardableSessionTZColumn"},
		},
		messageMarkers: []string{"time_zone", "drained model"},
	},
	{
		id:          "mysql-vstream-snapshot",
		engineNames: nil, // same registry names again; a third emitter, not a third engine
		declFile:    filepath.Join("..", "engines", "mysql", "cdc_session_tz_cast.go"),
		declFunc:    "sessionTZSwapPair",
		minArms:     1,
		minSymbols:  2,
		minLabels:   1,
		floorReason: "shares the binlog lane's declaration; the cold-start snapshot stream is a separate EMITTER, which is why it is a separate cell",
		refusalSites: []gucCastRefusalSite{
			{file: filepath.Join("..", "engines", "mysql", "cdc_vstream_snapshot.go"), callee: "unforwardableSessionTZColumn"},
		},
		messageMarkers: []string{"time_zone", "drained model"},
	},
}

// sessionGUCCastExempt records, per registered engine, why the class is
// UNREACHABLE there. Every entry must name a MECHANISM, not a likelihood —
// "these engines rarely ALTER" is not a reason.
var sessionGUCCastExempt = map[string]string{
	"sqlite":           "no CDC reader at all (the trigger variant is a separate engine), so no schema boundary is ever emitted and nothing forwards a MODIFY",
	"d1":               "same as sqlite — the CDC lane is the separate d1-trigger engine",
	"postgres-trigger": "the trigger-CDC readers emit row changes only; they build no ir.SchemaSnapshot, so no observed DDL can reach a forward path (the emitter walk below is what keeps this true)",
	"sqlite-trigger":   "same trigger-CDC reader shape as postgres-trigger — no schema-boundary emission",
	"d1-trigger":       "same trigger-CDC reader shape as postgres-trigger — no schema-boundary emission",
	"csv":              "flat-file SOURCE only: no CDC reader, and its temporal values carry no engine-side session zone to be re-cast against",
	"tsv":              "same flatfile engine as csv",
	"ndjson":           "same flatfile engine as csv",
	"mydumper":         "dump-directory source: no CDC reader, no mid-stream DDL",
}

// gucCastLaneFacts is what the AST reader extracts for one lane — kept as
// plain data so the meta-test can synthesize it.
type gucCastLaneFacts struct {
	// arms is the number of `return <expr>, true` statements in the
	// declaration: one per declared pair family.
	arms int
	// symbols are the type/OID identifiers the declaration names.
	symbols []string
	// labels are the string literals the declaration emits — the pair
	// vocabulary the refusal text is built from.
	labels []string
	// calls maps each declared refusal-site file to the number of calls it
	// makes to that site's callee. Zero is the fail-closed case.
	calls map[string]int
	// messages are the string literals found across the refusal sites,
	// searched for the lane's messageMarkers.
	messages []string
}

// gradeSessionGUCCastRoster is the pure grading half. It returns one
// problem string per failure so the caller can report them all at once,
// and so the meta-test can assert the fail-closed cases without touching
// production code.
func gradeSessionGUCCastRoster(
	engineNames []string,
	lanes []gucCastLane,
	exempt map[string]string,
	facts map[string]gucCastLaneFacts,
	emitterFiles []string,
) []string {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	// (1) Registry coverage.
	served := map[string]string{}
	for _, lane := range lanes {
		for _, name := range lane.engineNames {
			if prior, dup := served[name]; dup {
				add("engine %q is claimed by both lane %q and lane %q; one engine name maps to one lane roster entry (extra emitters are extra lanes with no engineNames)", name, prior, lane.id)
				continue
			}
			served[name] = lane.id
		}
	}
	for _, name := range engineNames {
		laneID, hasLane := served[name]
		reason, isExempt := exempt[name]
		switch {
		case hasLane && isExempt:
			add("engine %q is listed BOTH on lane %q and as exempt (%s) — one of the two entries is stale", name, laneID, reason)
		case isExempt && strings.TrimSpace(reason) == "":
			add("engine %q has an empty exemption reason; name the mechanism that makes the class unreachable", name)
		case !hasLane && !isExempt:
			add("engine %q appears in no session-GUC-cast lane and carries no exemption. A CDC lane that forwards an ALTER COLUMN TYPE whose conversion is resolved by a SESSION setting (PG TimeZone, MySQL time_zone) silently diverges every pre-existing target row at exit 0. Declare its pairs and its refusal, or record why the class cannot reach it.", name)
		}
	}
	known := map[string]bool{}
	for _, name := range engineNames {
		known[name] = true
	}
	for name := range exempt {
		if !known[name] {
			add("exemption names %q, which is not a registered engine — drop or rename the stale entry", name)
		}
	}
	for name, laneID := range served {
		if !known[name] {
			add("lane %q claims engine %q, which is not registered — drop or rename the stale entry", laneID, name)
		}
	}

	// (2) Emitter coverage: every schema-boundary emitter is a declared
	// refusal site somewhere.
	declaredSites := map[string]string{}
	for _, lane := range lanes {
		for _, site := range lane.refusalSites {
			declaredSites[filepath.ToSlash(site.file)] = lane.id
		}
	}
	for _, f := range emitterFiles {
		if _, ok := declaredSites[filepath.ToSlash(f)]; !ok {
			add("%s builds an ir.SchemaSnapshot but is not a declared refusal site of any lane — a boundary emitter with no session-GUC-cast door forwards the class silently (this is the half that would have caught MySQL's two VStream emitters)", filepath.ToSlash(f))
		}
	}

	// (3) + (4) Per-lane declaration and wiring.
	for _, lane := range lanes {
		f, ok := facts[lane.id]
		if !ok {
			add("lane %q has no extracted facts — the declaration %s:%s could not be read", lane.id, lane.declFile, lane.declFunc)
			continue
		}
		if f.arms < lane.minArms {
			add("lane %q: declaration %s has %d `return …, true` arm(s); floor %d (%s). A pair was removed from the declaration, or the declaration moved.",
				lane.id, lane.declFunc, f.arms, lane.minArms, lane.floorReason)
		}
		if len(f.symbols) < lane.minSymbols {
			add("lane %q: declaration %s names %d type/OID symbol(s) (%v); floor %d (%s)",
				lane.id, lane.declFunc, len(f.symbols), f.symbols, lane.minSymbols, lane.floorReason)
		}
		if len(f.labels) < lane.minLabels {
			add("lane %q: declaration %s emits %d pair label(s) (%v); floor %d (%s)",
				lane.id, lane.declFunc, len(f.labels), f.labels, lane.minLabels, lane.floorReason)
		}
		for _, site := range lane.refusalSites {
			if f.calls[filepath.ToSlash(site.file)] == 0 {
				add("lane %q DECLARES session-GUC cast pairs but %s never calls %s — a declaration with no refusal path is worse than none, because it reads as coverage",
					lane.id, filepath.ToSlash(site.file), site.callee)
			}
		}
		for _, marker := range lane.messageMarkers {
			found := false
			for _, msg := range f.messages {
				if strings.Contains(msg, marker) {
					found = true
					break
				}
			}
			if !found {
				add("lane %q: no refusal message names %q. The refusal must name the MECHANISM (which session setting decides the cast) and the drained-model remedy, or the operator cannot tell whether they were exposed.",
					lane.id, marker)
			}
		}
	}

	sort.Strings(problems)
	return problems
}

// TestSessionGUCCastRoster_EveryCDCLane is the roster itself, run against
// the real registry and the real source tree.
func TestSessionGUCCastRoster_EveryCDCLane(t *testing.T) {
	names := engines.Names()
	// Anti-vacuity floor, same shape and reason as the other registry
	// rosters in this package: an empty registry passes everything.
	if len(names) < 8 {
		t.Fatalf("registry holds %d engines (%v); the blank-import list in this package has drifted from cmd/sluice — the roster would under-report", len(names), names)
	}
	if len(sessionGUCCastLanes) < 2 {
		t.Fatalf("roster holds %d lane(s); floor 2 — the audit's whole finding is that a one-lane roster is how the class leaks", len(sessionGUCCastLanes))
	}

	facts := map[string]gucCastLaneFacts{}
	for _, lane := range sessionGUCCastLanes {
		f, err := readGUCCastLaneFacts(lane)
		if err != nil {
			t.Fatalf("lane %q: %v", lane.id, err)
		}
		facts[lane.id] = f
	}

	emitters, err := findSchemaSnapshotEmitters(filepath.Join("..", "engines"))
	if err != nil {
		t.Fatalf("walk internal/engines for schema-boundary emitters: %v", err)
	}
	// Anti-vacuity floor: four production emitters are known to exist (PG's
	// pgoutput reader plus MySQL's binlog, VStream and VStream-snapshot
	// lanes). A walker that stopped matching would otherwise be green for
	// exactly the defect it was built for.
	if len(emitters) < 4 {
		t.Fatalf("found %d ir.SchemaSnapshot emitter file(s) under internal/engines (%v); floor 4 — the walk is vacuous, re-point it", len(emitters), emitters)
	}

	if problems := gradeSessionGUCCastRoster(names, sessionGUCCastLanes, sessionGUCCastExempt, facts, emitters); len(problems) > 0 {
		t.Errorf("session-GUC-cast roster: %d problem(s):\n  %s", len(problems), strings.Join(problems, "\n  "))
	}
}

// TestSessionGUCCastRoster_MetaFailsClosed is the gate on the gate. It
// drives the pure grader with synthetic inputs so the three fail-closed
// postures are pinned permanently, rather than only demonstrated once by a
// throwaway mutation:
//
//	(a) a lane that declares pairs and wires no refusal → FAIL;
//	(b) a lane whose declaration lost an arm → FAIL;
//	(c) a newly-registered engine classified as neither → FAIL;
//
// plus a positive control, because a grader that reports problems for
// everything is no more useful than one that reports none.
func TestSessionGUCCastRoster_MetaFailsClosed(t *testing.T) {
	goodLane := gucCastLane{
		id:          "fake-lane",
		engineNames: []string{"fakeengine"},
		declFile:    "decl.go",
		declFunc:    "sessionTZSwapPair",
		minArms:     2,
		minSymbols:  4,
		minLabels:   2,
		floorReason: "synthetic",
		refusalSites: []gucCastRefusalSite{
			{file: "reader.go", callee: "unforwardableSessionTZColumn"},
		},
		messageMarkers: []string{"time_zone", "drained model"},
	}
	goodFacts := gucCastLaneFacts{
		arms:     2,
		symbols:  []string{"a", "b", "c", "d"},
		labels:   []string{"x and y", "p and q"},
		calls:    map[string]int{"reader.go": 1},
		messages: []string{"the source session's time_zone decided the cast; use the drained model"},
	}
	names := []string{"fakeengine"}

	t.Run("positive control: a complete lane reports nothing", func(t *testing.T) {
		got := gradeSessionGUCCastRoster(names, []gucCastLane{goodLane},
			map[string]string{}, map[string]gucCastLaneFacts{"fake-lane": goodFacts},
			[]string{"reader.go"})
		if len(got) != 0 {
			t.Fatalf("complete lane reported problems: %v", got)
		}
	})

	t.Run("(a) declares pairs, wires no refusal", func(t *testing.T) {
		f := goodFacts
		f.calls = map[string]int{"reader.go": 0}
		got := gradeSessionGUCCastRoster(names, []gucCastLane{goodLane},
			map[string]string{}, map[string]gucCastLaneFacts{"fake-lane": f}, []string{"reader.go"})
		requireProblemMentioning(t, got, "never calls")
	})

	t.Run("(b) declaration lost an arm", func(t *testing.T) {
		f := goodFacts
		f.arms = 1
		got := gradeSessionGUCCastRoster(names, []gucCastLane{goodLane},
			map[string]string{}, map[string]gucCastLaneFacts{"fake-lane": f}, []string{"reader.go"})
		requireProblemMentioning(t, got, "arm(s); floor")
	})

	t.Run("(c) a new registered engine classified as neither", func(t *testing.T) {
		got := gradeSessionGUCCastRoster(append(append([]string(nil), names...), "brandnew"),
			[]gucCastLane{goodLane}, map[string]string{},
			map[string]gucCastLaneFacts{"fake-lane": goodFacts}, []string{"reader.go"})
		requireProblemMentioning(t, got, `engine "brandnew" appears in no session-GUC-cast lane`)
	})

	t.Run("(d) a boundary emitter no lane declares", func(t *testing.T) {
		got := gradeSessionGUCCastRoster(names, []gucCastLane{goodLane},
			map[string]string{}, map[string]gucCastLaneFacts{"fake-lane": goodFacts},
			[]string{"reader.go", "brand_new_lane.go"})
		requireProblemMentioning(t, got, "brand_new_lane.go")
	})

	t.Run("(e) a refusal that does not name the mechanism", func(t *testing.T) {
		f := goodFacts
		f.messages = []string{"this ALTER cannot be forwarded; use the drained model"}
		got := gradeSessionGUCCastRoster(names, []gucCastLane{goodLane},
			map[string]string{}, map[string]gucCastLaneFacts{"fake-lane": f}, []string{"reader.go"})
		requireProblemMentioning(t, got, `names "time_zone"`)
	})
}

func requireProblemMentioning(t *testing.T, problems []string, want string) {
	t.Helper()
	for _, p := range problems {
		if strings.Contains(p, want) {
			return
		}
	}
	t.Fatalf("grader did not report a problem mentioning %q; got: %v", want, problems)
}

// readGUCCastLaneFacts extracts one lane's declaration and wiring from the
// source tree. The pair universe comes out of the lane's OWN declaration
// function — never a list maintained here.
func readGUCCastLaneFacts(lane gucCastLane) (gucCastLaneFacts, error) {
	facts := gucCastLaneFacts{calls: map[string]int{}}

	fset := token.NewFileSet()
	declAST, err := parser.ParseFile(fset, lane.declFile, nil, 0)
	if err != nil {
		return facts, fmt.Errorf("parse %s: %w", lane.declFile, err)
	}
	fn := findFuncDecl(declAST, lane.declFunc)
	if fn == nil {
		return facts, fmt.Errorf("declaration %s not found in %s — the lane's pair universe is unreadable, which is a failure and not a skip", lane.declFunc, lane.declFile)
	}
	// Local names — parameters and `:=` bindings — so a field access on the
	// function's OWN variables (`pc.OID`) is not miscounted as a declared
	// type/OID symbol. What survives is exactly what the declaration names
	// from OUTSIDE itself: `pgtype.TimestamptzOID`, `ir.DateTime`, …
	locals := map[string]bool{}
	if fn.Type.Params != nil {
		for _, f := range fn.Type.Params.List {
			for _, n := range f.Names {
				locals[n.Name] = true
			}
		}
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if as, ok := n.(*ast.AssignStmt); ok && as.Tok == token.DEFINE {
			for _, lhs := range as.Lhs {
				if id, ok := lhs.(*ast.Ident); ok {
					locals[id.Name] = true
				}
			}
		}
		return true
	})

	symbols := map[string]bool{}
	labels := map[string]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.ReturnStmt:
			// An arm is `return <pair>, true`.
			if len(node.Results) == 2 {
				if id, ok := node.Results[1].(*ast.Ident); ok && id.Name == "true" {
					facts.arms++
				}
			}
		case *ast.SelectorExpr:
			// pgtype.TimeOID, ir.Timestamp, …
			if x, ok := node.X.(*ast.Ident); ok && !locals[x.Name] {
				symbols[x.Name+"."+node.Sel.Name] = true
			}
		case *ast.BasicLit:
			if node.Kind == token.STRING {
				if s, err := strconv.Unquote(node.Value); err == nil && strings.TrimSpace(s) != "" {
					labels[s] = true
				}
			}
		}
		return true
	})
	facts.symbols = sortedSetKeys(symbols)
	facts.labels = sortedSetKeys(labels)

	messages := map[string]bool{}
	for _, site := range lane.refusalSites {
		siteAST, err := parser.ParseFile(fset, site.file, nil, 0)
		if err != nil {
			return facts, fmt.Errorf("parse %s: %w", site.file, err)
		}
		calls := 0
		ast.Inspect(siteAST, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == site.callee {
				calls++
			}
			return true
		})
		facts.calls[filepath.ToSlash(site.file)] = calls
		ast.Inspect(siteAST, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if s, err := strconv.Unquote(lit.Value); err == nil {
					messages[s] = true
				}
			}
			return true
		})
	}
	// The refusal TEXT for a lane may live in a helper file beside the
	// emitters (mysql's sessionTZCastRefusal does), so the message search
	// widens to the declaring file too.
	ast.Inspect(declAST, func(n ast.Node) bool {
		if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			if s, err := strconv.Unquote(lit.Value); err == nil {
				messages[s] = true
			}
		}
		return true
	})
	facts.messages = sortedSetKeys(messages)
	return facts, nil
}

func findFuncDecl(file *ast.File, name string) *ast.FuncDecl {
	for _, d := range file.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if ok && fn.Name != nil && fn.Name.Name == name && fn.Body != nil {
			return fn
		}
	}
	return nil
}

func sortedSetKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// findSchemaSnapshotEmitters returns every non-test .go file under root
// that builds an `ir.SchemaSnapshot` composite literal — the boundary a
// forward path consumes. This is the roster's own universe derivation: it
// does not ask which lanes exist, it asks which files EMIT, so a new
// emitter must be classified before this gate goes green again.
func findSchemaSnapshotEmitters(root string) ([]string, error) {
	var out []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
		emits := false
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			sel, ok := lit.Type.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			x, ok := sel.X.(*ast.Ident)
			if ok && x.Name == "ir" && sel.Sel.Name == "SchemaSnapshot" {
				emits = true
			}
			return true
		})
		if emits {
			out = append(out, path)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}
