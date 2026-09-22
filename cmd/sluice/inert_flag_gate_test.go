// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// GC-19 (census C-4): the ADR-0118 inert-flag WARN reached 3 flags of a
// documented family of ~11, and its engine predicate was a hand-kept
// allowlist. These gates hold the registry in inert_flags.go to two
// derived universes — the kong model (which flags SAY they are inert) and
// the engine registry (which engines they are inert ON) — so a new
// inert-marked flag, a new engine, or an unwired command fails the build
// instead of shipping another silent no-op.
//
// Scope, stated so the names cannot be read as broader than the truth:
//   - TestInertFlagWarnCoversEveryInertMarkedFlag grades flags whose --help
//     carries one of the inertMarkerRe phrases. A flag that is inert and
//     says nothing is outside its reach (that is a help-text defect).
//   - TestInertFlagWarnWiringRoster proves every registry command has a
//     warnInertFlags CALL SITE in cmd/sluice; it does not prove the call is
//     on every path of that command's Run.
//   - TestInertFlagPredicates_EngineMatrix pins every predicate against
//     every registered engine, on the pure inertFlagsUsed core.

// inertMarkerRe is the phrase set that marks a flag's help text as
// declaring engine-inertness. Tuned to this tree: the bare `-only` suffix
// is restricted to an engine word because the CLI also says hash-only,
// verify-only, read-only, alert-only, env-only and migrate-only, none of
// which is an engine claim; the flag-dependency spellings ("only consulted
// when --x", "only used when", "only active with") are deliberately NOT
// markers — they describe another flag, not an engine.
var inertMarkerRe = regexp.MustCompile(`(?i)` +
	`\binert\b` +
	`|\bignored (on|for|when|with)\b` +
	`|\bno effect\b` +
	`|\bonly meaningful\b` +
	`|\bapplies only\b` +
	`|\b(Postgres|PostgreSQL|PG|MySQL|MariaDB|SQLite|D1|VStream|PlanetScale|Vitess)(/[A-Za-z0-9]+)*-(only|target only)\b` +
	`|\b(sources?|targets?) only\b` +
	`|\) only\b`)

// inertMarkedFlag is one (command, flag) the kong walk found.
type inertMarkedFlag struct {
	command, flag, matched string
}

// key is the registry/exemption spelling.
func (f inertMarkedFlag) key() string { return f.command + " --" + f.flag }

// inertMarkedFlags walks the real kong model and returns every flag whose
// help text carries an inert marker, with the phrase that matched. Root
// (global) flags come back with command "" and are graded like any other:
// the exemption map's "* --flag" form is how a global earns its pass.
func inertMarkedFlags(t *testing.T) []inertMarkedFlag {
	t.Helper()
	var cli CLI
	model, err := kong.New(&cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	var out []inertMarkedFlag
	var walk func(n *kong.Node, prefix []string)
	walk = func(n *kong.Node, prefix []string) {
		for _, f := range n.Flags {
			if m := inertMarkerRe.FindString(f.Help); m != "" {
				out = append(out, inertMarkedFlag{strings.Join(prefix, " "), f.Name, m})
			}
		}
		for _, c := range n.Children {
			if c.Name == "" {
				continue
			}
			walk(c, append(append([]string(nil), prefix...), c.Name))
		}
	}
	walk(model.Model.Node, nil)
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// exemptionFor resolves the exact "<command> --flag" key, then the
// "* --flag" wildcard.
func exemptionFor(f inertMarkedFlag) (string, bool) {
	if r, ok := inertFlagExemptions[f.key()]; ok {
		return r, true
	}
	r, ok := inertFlagExemptions["* --"+f.flag]
	return r, ok
}

func TestInertFlagWarnCoversEveryInertMarkedFlag(t *testing.T) {
	marked := inertMarkedFlags(t)
	for _, f := range marked {
		t.Logf("inert-marked: %-40s (matched %q)", f.key(), f.matched)
	}
	// Anti-vacuity floor: the family the census counted was ~11 on `sync
	// start` alone; a walk that finds fewer than 10 across the whole CLI
	// means the matcher or the model moved and this gate grades nothing.
	if len(marked) < 10 {
		t.Fatalf("only %d inert-marked flags found across the CLI; the marker set or the kong walk is broken and this gate is vacuous", len(marked))
	}

	registered := map[string]inertFlag{}
	for _, row := range inertFlagRegistry {
		registered[row.command+" --"+row.flag] = row
	}
	if len(registered) < 10 {
		t.Fatalf("only %d registry rows; GC-19's floor is 10 (the finding counted ~11 on sync start alone)", len(registered))
	}
	if len(registered) != len(inertFlagRegistry) {
		t.Fatalf("inertFlagRegistry has a duplicate (command, flag) row: %d rows, %d distinct", len(inertFlagRegistry), len(registered))
	}

	// Direction 1: every marked flag is registered or reasoned-exempt.
	markedKeys := map[string]bool{}
	for _, f := range marked {
		markedKeys[f.key()] = true
		if _, ok := registered[f.key()]; ok {
			continue
		}
		if reason, ok := exemptionFor(f); ok {
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s: exemption has an empty reason", f.key())
			}
			continue
		}
		t.Errorf("%s: its help text says it is inert (%q) but it is neither in inertFlagRegistry nor in inertFlagExemptions — "+
			"register it with the capability predicate that decides it, or exempt it with a written reason", f.key(), f.matched)
	}

	// Direction 2: every registry row names a real, inert-marked flag on a
	// real command — so a renamed flag, a moved command, or a help text
	// that no longer tells the operator what the WARN will tell them, fails.
	for key := range registered {
		if !markedKeys[key] {
			t.Errorf("registry row %s does not match an inert-marked flag in the kong model (renamed flag? help text no longer says inert?)", key)
		}
	}

	// Direction 3: no stale exemption — each key must still match a marked
	// flag, so the map cannot accumulate entries for flags that moved on.
	for key := range inertFlagExemptions {
		if !strings.HasPrefix(key, "* --") {
			if !markedKeys[key] {
				t.Errorf("exemption %q matches no inert-marked flag (stale?)", key)
			}
			if _, alsoRegistered := registered[key]; alsoRegistered {
				t.Errorf("%q is both registered and exempt; pick one", key)
			}
			continue
		}
		flag := strings.TrimPrefix(key, "* --")
		hit := false
		for _, f := range marked {
			if f.flag == flag {
				hit = true
				break
			}
		}
		if !hit {
			t.Errorf("wildcard exemption %q matches no inert-marked flag (stale?)", key)
		}
	}
}

// TestInertFlagWarnWiringRoster: every command with a registry row must
// have a warnInertFlags call site somewhere in cmd/sluice (non-test), with
// the command path as its second argument — resolved from the AST, not
// grepped from rendered text. Floor: at least 5 call sites, so an emptied
// registry or a renamed function cannot pass vacuously.
func TestInertFlagWarnWiringRoster(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	wired := map[string]string{} // command → file
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || id.Name != "warnInertFlags" || len(call.Args) < 2 {
				return true
			}
			lit, ok := call.Args[1].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				t.Errorf("%s: warnInertFlags call's command argument must be a string literal so this roster can read it", fset.Position(call.Pos()))
				return true
			}
			cmd, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Fatalf("%s: unquote %s: %v", fset.Position(call.Pos()), lit.Value, err)
			}
			wired[cmd] = path
			return true
		})
	}
	if len(wired) < 5 {
		t.Fatalf("found only %d warnInertFlags call sites in cmd/sluice; the function was renamed or the wiring was lost", len(wired))
	}
	want := map[string]bool{}
	for _, row := range inertFlagRegistry {
		want[row.command] = true
	}
	for cmd := range want {
		if _, ok := wired[cmd]; !ok {
			t.Errorf("registry has rows for %q but no Run path calls warnInertFlags(ctx, %q, …) — the rows are dead", cmd, cmd)
		}
	}
	for cmd, file := range wired {
		if !want[cmd] {
			t.Errorf("%s wires warnInertFlags for %q but the registry has no rows for it (dead call, or a command path typo)", file, cmd)
		}
	}
}

// inertApplies is, per capability predicate, the set of registered engines
// the flag APPLIES on; every other registered engine must read as inert.
// Pinned by name on purpose: a new engine that lands unclassified shows up
// here as a failure rather than inheriting a verdict.
var inertApplies = map[string]map[string]bool{
	"fast-cold-start (ir.SnapshotImporterOpener)":          set("postgres"),
	"binlog source (Capabilities.CDC == CDCBinlog)":        set("mysql", "mariadb"),
	"vstream source (Capabilities.CDC == CDCVStream)":      set("planetscale", "vitess"),
	"trigger source (Capabilities.CDC == CDCTriggers)":     set("postgres-trigger", "sqlite-trigger", "d1-trigger"),
	"logical-replication source (CDCLogicalReplication)":   set("postgres"),
	"index-build tuning (Capabilities.PostgresBackend)":    set("postgres", "postgres-trigger"),
	"connection budget (ir.TargetConnectionBudgetProber)":  set("mysql", "mariadb", "planetscale", "vitess", "postgres"),
	"stale-backend reaper (ir.TargetStaleBackendReaper)":   set("postgres"),
	"control keyspace (mysql.Engine)":                      set("mysql", "mariadb", "planetscale", "vitess"),
	"namespaced schema (Capabilities.SchemaScope != Flat)": set("postgres", "postgres-trigger"),
}

// predicateFamily maps a registry row to its inertApplies key by the
// reason clause it carries — the clause is the family's identity in the
// table, so a row that invents a new reason without a family fails here.
func predicateFamily(t *testing.T, row inertFlag) string {
	t.Helper()
	switch {
	case row.reason == reasonFastColdStart, strings.HasPrefix(row.reason, "the raw-copy passthrough lane"):
		return "fast-cold-start (ir.SnapshotImporterOpener)"
	case row.reason == reasonBinlogOnly:
		return "binlog source (Capabilities.CDC == CDCBinlog)"
	case strings.HasPrefix(row.reason, reasonVStreamOnly):
		return "vstream source (Capabilities.CDC == CDCVStream)"
	case strings.HasPrefix(row.reason, "it reaps a trigger-CDC source"):
		return "trigger source (Capabilities.CDC == CDCTriggers)"
	case strings.HasPrefix(row.reason, reasonNoLSN):
		return "logical-replication source (CDCLogicalReplication)"
	case row.reason == reasonNoTuner:
		return "index-build tuning (Capabilities.PostgresBackend)"
	case row.reason == reasonNoProber:
		return "connection budget (ir.TargetConnectionBudgetProber)"
	case row.reason == reasonNoReaper:
		return "stale-backend reaper (ir.TargetStaleBackendReaper)"
	case row.reason == reasonNoKeyspace:
		return "control keyspace (mysql.Engine)"
	case row.reason == reasonFlatSchema:
		return "namespaced schema (Capabilities.SchemaScope != Flat)"
	}
	t.Fatalf("registry row %s --%s carries a reason no predicate family claims: %q", row.command, row.flag, row.reason)
	return ""
}

func set(names ...string) map[string]bool {
	m := map[string]bool{}
	for _, n := range names {
		m[n] = true
	}
	return m
}

// TestInertFlagPredicates_EngineMatrix: every registry row × every
// registered engine, on the pure core. For each cell the WARN must fire
// iff the engine is outside the row's applies-set, must never fire when the
// flag is not passed, and must fire on an explicit default-valued pass
// (spelling, not value — ADR-0118's zero-value-safety property).
func TestInertFlagPredicates_EngineMatrix(t *testing.T) {
	names := engines.Names()
	if len(names) < 10 {
		t.Fatalf("engine registry has %d engines; expected the full matrix (≥10)", len(names))
	}
	for family, applies := range inertApplies {
		for name := range applies {
			if _, ok := engines.Get(name); !ok {
				t.Errorf("inertApplies[%q] names unregistered engine %q", family, name)
			}
		}
	}
	for _, row := range inertFlagRegistry {
		family := predicateFamily(t, row)
		applies := inertApplies[family]
		for _, name := range names {
			eng := mustEngine(t, name)
			var source, target ir.Engine
			if row.side == inertOnTarget {
				target = eng
			} else {
				source = eng
			}
			wantInert := !applies[name]
			args := append(strings.Fields(row.command), "--"+row.flag+"=1")
			hits := inertFlagsUsed(args, row.command, source, target)
			gotInert := len(hits) == 1 && hits[0].flag == row.flag
			if gotInert != wantInert {
				t.Errorf("%s --%s on %s %s: inert=%v, want %v (%s)", row.command, row.flag, name, sideWord(row.side), gotInert, wantInert, family)
			}
			if got := inertFlagsUsed(strings.Fields(row.command), row.command, source, target); len(got) != 0 {
				t.Errorf("%s --%s on %s: fired without the flag being passed", row.command, row.flag, name)
			}
			// The bare "--flag" spelling (bool form / space-separated value)
			// must count too, and so must an explicit default value.
			bare := append(strings.Fields(row.command), "--"+row.flag, "0")
			if got := inertFlagsUsed(bare, row.command, source, target); (len(got) == 1) != wantInert {
				t.Errorf("%s --%s on %s: bare spelling inert=%v, want %v", row.command, row.flag, name, len(got) == 1, wantInert)
			}
		}
		// A row never judges the side it does not name: with only the OTHER
		// engine resolved, nothing fires (the nil side satisfies no predicate).
		var otherSource, otherTarget ir.Engine
		if row.side == inertOnTarget {
			otherSource = mustEngine(t, "sqlite")
		} else {
			otherTarget = mustEngine(t, "sqlite")
		}
		if got := inertFlagsUsed(append(strings.Fields(row.command), "--"+row.flag+"=1"), row.command, otherSource, otherTarget); len(got) != 0 {
			t.Errorf("%s --%s: fired with its judged side nil", row.command, row.flag)
		}
	}
	// The command key is load-bearing: a row must not fire for another
	// command's argv.
	if got := inertFlagsUsed([]string{"migrate", "--bulk-parallelism=8"}, "migrate", mustEngine(t, "mysql"), mustEngine(t, "postgres")); len(got) != 0 {
		t.Errorf("--bulk-parallelism fired on migrate, where it applies to every source: %+v", got)
	}
}

func sideWord(s inertSide) string {
	if s == inertOnTarget {
		return "target"
	}
	return "source"
}

// TestInertFlagPredicates_IndexBuildTuningPremise binds the two facts
// targetLacksIndexBuildTuning's comment cites: (1) the PostgresBackend
// engine set is exactly {postgres, postgres-trigger}, so a new PG-family
// engine forces a re-check of the writer premise; (2) postgres-trigger's
// OpenSchemaWriter is a one-line delegate to its composed postgres engine
// (resolved from the AST, not grepped), so its writer IS the tuner.
func TestInertFlagPredicates_IndexBuildTuningPremise(t *testing.T) {
	var pgBacked []string
	for _, name := range engines.Names() {
		if mustEngine(t, name).Capabilities().PostgresBackend {
			pgBacked = append(pgBacked, name)
		}
	}
	if want := []string{"postgres", "postgres-trigger"}; strings.Join(pgBacked, ",") != strings.Join(want, ",") {
		t.Fatalf("PostgresBackend engines = %v, want %v — a new PG-family engine must be checked: does its OpenSchemaWriter return the ir.IndexBuildTuner writer?", pgBacked, want)
	}

	path := filepath.Join(repoRootForDocs(t), "internal", "engines", "pgtrigger", "engine.go")
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "OpenSchemaWriter" || fn.Recv == nil {
			continue
		}
		found = true
		if len(fn.Body.List) != 1 {
			t.Fatalf("pgtrigger.Engine.OpenSchemaWriter is no longer a one-line delegate (%d statements); re-verify it still returns postgres's SchemaWriter and update targetLacksIndexBuildTuning's premise", len(fn.Body.List))
		}
		ret, ok := fn.Body.List[0].(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 1 {
			t.Fatal("pgtrigger.Engine.OpenSchemaWriter: expected a single return")
		}
		call, ok := ret.Results[0].(*ast.CallExpr)
		if !ok {
			t.Fatal("pgtrigger.Engine.OpenSchemaWriter: expected `return <call>`")
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "OpenSchemaWriter" {
			t.Fatal("pgtrigger.Engine.OpenSchemaWriter: expected a delegate call to OpenSchemaWriter")
		}
		inner, ok := sel.X.(*ast.SelectorExpr)
		if !ok || inner.Sel.Name != "pg" {
			t.Fatalf("pgtrigger.Engine.OpenSchemaWriter delegates to %s, not the composed postgres engine (e.pg)", sel.Sel.Name)
		}
	}
	if !found {
		t.Fatal("pgtrigger.Engine.OpenSchemaWriter not found")
	}
}

// TestInertFlagWarnEmitsMarker: the live pass reads argv, resolves the
// engines by driver name, and emits one WARN per inert flag carrying the
// INERT-FLAG marker, the flag, the command and the reason — once per
// process, and never for an applicable engine or an unknown driver.
func TestInertFlagWarnEmitsMarker(t *testing.T) {
	prevArgs := os.Args
	defer func() { os.Args = prevArgs }()
	var buf logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	os.Args = []string{"sluice", "sync", "start", "--source-driver=sqlite-trigger", "--bulk-parallelism=8", "--index-build-mem=1GB"}
	warnInertFlags(context.Background(), "sync start", "sqlite-trigger", "sqlite")
	out := buf.String()
	for _, want := range []string{
		inertFlagMarker + ": --bulk-parallelism has no effect on `sync start` with a sqlite-trigger source",
		inertFlagMarker + ": --index-build-mem has no effect on `sync start` against a sqlite target",
		"snapshot-pinned readers",
		"maintenance_work_mem",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("WARN output missing %q:\n%s", want, out)
		}
	}
	if n := strings.Count(out, inertFlagMarker); n != 2 {
		t.Errorf("expected exactly 2 INERT-FLAG WARNs, got %d:\n%s", n, out)
	}

	// Once per process: a second resolution of the same command emits nothing.
	buf.Reset()
	warnInertFlags(context.Background(), "sync start", "sqlite-trigger", "sqlite")
	if buf.Len() != 0 {
		t.Errorf("second pass re-emitted:\n%s", buf.String())
	}

	// An applicable engine, or an unknown driver, is silent.
	buf.Reset()
	os.Args = []string{"sluice", "sync", "start", "--table-parallelism=4"}
	warnInertFlags(context.Background(), "sync start", "postgres", "postgres")
	warnInertFlags(context.Background(), "sync start", "no-such-engine", "")
	if buf.Len() != 0 {
		t.Errorf("applicable/unknown engines must be silent:\n%s", buf.String())
	}
}
