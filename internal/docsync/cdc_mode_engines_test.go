// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	// Blank-imported for their init() self-registration, the same set
	// cmd/sluice/main.go links — this test iterates the REAL registry, so
	// a new engine package added there must be added here too (the
	// non-vacuity floor below fails rather than silently under-reporting).
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// WHICH SOURCE USES WHICH CDC MECHANISM, kept honest against the registry
// (roadmap item 105).
//
// # Why this exists
//
// The sibling gate next door (TestCDCGuideCoversEveryCDCCapableEngine)
// asks whether an engine is NAMED in the CDC guide. This one asks the
// question that keeps going wrong in prose: which MECHANISM each source
// uses — because "the engines this reader affects" is a sentence this
// project has now gotten wrong four releases running, always in the same
// direction, always by reasoning from the shape of a fix instead of from
// the dispatch.
//
// v0.104.4's first-draft release notes said a fix to the MySQL binlog CDC
// reader reached "MySQL, MariaDB, PlanetScale or Vitess". It reaches
// `mysql` and `mariadb`: `Flavor.usesVStream()` routes the other two to a
// different reader entirely, so the sentence promised a fix to operators
// who never had the defect and would have sent them looking for it. That
// draft was caught by reading the dispatch before publishing — which is
// exactly the manual diligence a gate exists to stop relying on.
//
// # What the marker holds
//
// The engine registry already knows the answer: every registered engine
// declares [ir.Capabilities.CDC], and [ir.CDCMethod] names the mechanism.
// So the derived truth is a name=mechanism pairing, and the operator doc
// carries a marker this test owns:
//
//	<!-- cdc-modes: mariadb=binlog, mysql=binlog, … -->
//
// The visible table beside it is for humans; the marker is what fails.
// Anyone about to write "this affects engines X and Y" now has one place
// to look whose answer is derived rather than remembered.
//
// # Scope, deliberately
//
// The pairing is checked for EVERY registered engine including the
// CDC-less ones (`none`), because "which sources have no CDC at all" is
// the same class of claim and is just as easy to get wrong from memory.
// Release notes and CHANGELOG entries stay unguarded prose — they are
// archival, each naming a moment — but the sentence in them should be
// derived from this table rather than from the shape of the fix.
func TestCDCModeTableMatchesTheRegistry(t *testing.T) {
	fromCode := map[string]string{}
	for _, name := range engines.Names() {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Get(%q) missing an engine its own Names() listed", name)
		}
		fromCode[name] = e.Capabilities().CDC.String()
	}

	// Non-vacuity floor. A blank-import that goes missing, or a registry
	// that fails to populate, must fail this gate rather than quietly
	// agreeing with a doc marker about a smaller world. The named engines
	// are the ones whose absence would be invisible in a set comparison —
	// each is a DIFFERENT mechanism, so losing one silently narrows the
	// very distinction this gate exists to hold.
	if len(fromCode) < 6 {
		t.Fatalf("only %d engines registered (%s) — a blank import is missing or the registry did not populate; "+
			"this gate must not pass on a partial world", len(fromCode), strings.Join(sortedKeys(fromCode), ", "))
	}
	for _, must := range []string{"mysql", "postgres", "planetscale"} {
		if _, ok := fromCode[must]; !ok {
			t.Fatalf("the %q engine is not in the registry this gate scanned — the mechanism distinction it holds "+
				"(binlog vs vstream vs logical replication) cannot be checked without it", must)
		}
	}
	// And at least one engine must declare a mechanism, or every pairing
	// below is "none" and the marker would agree with a table that says
	// nothing.
	sawMechanism := false
	for _, mode := range fromCode {
		if mode != ir.CDCNone.String() {
			sawMechanism = true
			break
		}
	}
	if !sawMechanism {
		t.Fatal("no registered engine declares a CDC mechanism — the capability plumbing broke, or every engine " +
			"reported none; either way this gate would pass vacuously")
	}

	docPath := filepath.Join("..", "..", "docs", "production-readiness.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	marker := regexp.MustCompile(`<!--\s*cdc-modes:\s*([^>]*?)\s*-->`)
	m := marker.FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/production-readiness.md carries no `<!-- cdc-modes: … -->` marker. It is the checkable "+
			"form of a claim this project has gotten wrong four releases running; add it listing:\n  %s",
			renderModes(fromCode))
	}

	fromDoc := map[string]string{}
	for _, pair := range splitList(string(m[1])) {
		name, mode, ok := strings.Cut(pair, "=")
		if !ok {
			t.Fatalf("cdc-modes marker entry %q is not `engine=mechanism`", pair)
		}
		fromDoc[strings.TrimSpace(name)] = strings.TrimSpace(mode)
	}

	if diff := modeDiff(fromCode, fromDoc); diff != "" {
		t.Errorf("the CDC-mode table's marker disagrees with the engine registry.\n%s\n"+
			"  code: %s\n  doc:  %s\n\n"+
			"Update the marker AND the table beside it. If you are about to write a release note saying a "+
			"CDC-path change affects engines X and Y, take the list from HERE — the mistake this gate exists "+
			"for is always the same shape: reasoning about which engines a reader touches instead of reading "+
			"the dispatch. `Flavor.usesVStream()` is why `planetscale` and `vitess` are not `binlog`.",
			diff, renderModes(fromCode), renderModes(fromDoc))
	}
}

// modeDiff renders the per-engine disagreements, so a failure names the
// offending engine rather than making the reader diff two long lines.
func modeDiff(code, doc map[string]string) string {
	var lines []string
	for _, name := range sortedKeys(code) {
		switch got, ok := doc[name]; {
		case !ok:
			lines = append(lines, fmt.Sprintf("  MISSING from the doc: %s (%s)", name, code[name]))
		case got != code[name]:
			lines = append(lines, fmt.Sprintf("  WRONG mechanism for %s: doc says %s, registry says %s", name, got, code[name]))
		}
	}
	for _, name := range sortedKeys(doc) {
		if _, ok := code[name]; !ok {
			lines = append(lines, fmt.Sprintf("  NOT IN the registry: %s (the doc lists it; no engine registers under that name)", name))
		}
	}
	return strings.Join(lines, "\n")
}

func renderModes(m map[string]string) string {
	parts := make([]string, 0, len(m))
	for _, k := range sortedKeys(m) {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ", ")
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
