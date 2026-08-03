// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package archgate

import (
	"os/exec"
	"sort"
	"strings"
	"testing"
)

const modulePrefix = "sluicesync.dev/sluice/"

// rule is one layering boundary: packages matching From must not transitively
// depend on anything matching Forbidden, except the exact paths in Allow.
type rule struct {
	Name string
	// From selects the packages the rule constrains. A trailing "/..."
	// matches the subtree; otherwise it is an exact package path.
	From string
	// Forbidden selects dependencies those packages must not have, same
	// matching. Self-matches are never violations.
	Forbidden string
	// Allow maps an exact dependency path to the REASON it is exempt. An
	// exception with an empty reason fails the gate.
	Allow map[string]string
	// AllowPrefix exempts a whole subtree. Deliberately blunt and deliberately
	// rare: it is for shared machinery that lives BELOW the layer the rule
	// polices, where enumerating members would be churn without signal.
	AllowPrefix string
	// Why is the consequence of a violation, quoted verbatim in the failure.
	Why string
}

var rules = []rule{
	{
		Name:      "the IR depends on nothing internal",
		From:      "internal/ir",
		Forbidden: "internal/...",
		Why: "internal/ir is the shared contract every engine and the orchestrator implement against. " +
			"If it depends on any of them, the contract is no longer independent of its implementors — " +
			"an engine's needs start shaping the IR, and the IR stops being the thing they all agree on.",
	},
	{
		Name:      "IR sub-packages may depend on core IR, never the reverse",
		From:      "internal/ir",
		Forbidden: "internal/ir/...",
		Why: "internal/ir/diff and internal/ir/backup are feature-scoped contracts layered OVER the core " +
			"IR. A dependency back the other way makes the core carry feature-specific concerns and turns " +
			"two independently-versionable surfaces into one.",
	},
	{
		Name:      "the orchestrator is engine-neutral",
		From:      "internal/pipeline",
		Forbidden: "internal/engines/...",
		// Measured, and stronger than CLAUDE.md's phrasing: production
		// internal/pipeline does not import internal/engines AT ALL, not even
		// the registry — cmd/sluice resolves the engine and injects it. The
		// rule is written against the registry subtree as a whole so that
		// stays true.
		Why: "engines reach the orchestrator as IR interface values injected by cmd/sluice, so a new engine " +
			"slots in without touching the pipeline. A dependency — even a transitive one through a helper — " +
			"means the orchestrator knows an engine by type, and the next engine cannot be added without " +
			"editing it.",
	},
	{
		Name:      "engines never depend on the orchestrator",
		From:      "internal/engines/...",
		Forbidden: "internal/pipeline",
		Why: "engines are implementations of the IR contract, consumed BY the pipeline. Depending back on " +
			"it inverts the layering, and makes an engine impossible to test or reuse without dragging the " +
			"whole orchestrator in.",
	},
	{
		Name:      "engines do not reach into a PEER engine",
		From:      "internal/engines/...",
		Forbidden: "internal/engines/...",
		// internal/engines/internal/* are the deliberately-shared helpers
		// (floatrepair, triggercdc, dumpsig) — common machinery factored out
		// BELOW the engines, not one engine reaching sideways into another.
		// Go's own internal/ visibility rule already scopes them to the
		// engines subtree, so they are excluded wholesale rather than listed.
		AllowPrefix: "internal/engines/internal/",
		Allow: map[string]string{
			"internal/engines/postgres": "pgtrigger is the trigger-CDC VARIANT of the postgres engine and " +
				"reuses its schema reader and type mapping — a dependency on its own BASE engine, not a peer",
			"internal/engines/sqlite": "sqlite-trigger, d1-trigger and flatfile all build on the sqlite " +
				"engine's SQL surface; flatfile stages through SQLite by design (ADR-0130)",
			"internal/engines/sqlite-trigger": "d1-trigger is the D1 variant of the sqlite trigger-CDC engine",
			"internal/engines/mysql": "mydumper is a MySQL-dialect dump READER and reuses the mysql engine's " +
				"type mapping and value decoding; same base-engine relationship",
		},
		Why: "engine-specific knowledge belongs behind the IR. One engine importing a PEER means a change " +
			"to the peer's internals can break it, which is exactly the coupling the IR exists to prevent. " +
			"The listed exceptions are all VARIANT-on-BASE relationships (a trigger flavour, a dump reader) " +
			"rather than peer coupling — a NEW entry here should be able to make that same claim.",
	},
}

func TestLayeringBoundariesHold(t *testing.T) {
	deps := loadDeps(t)
	if len(deps) < 20 {
		t.Fatalf("resolved only %d module packages; `go list` is not reaching the module and this gate would "+
			"pass on nothing", len(deps))
	}

	for _, r := range rules {
		t.Run(r.Name, func(t *testing.T) {
			for dep, why := range r.Allow {
				if strings.TrimSpace(why) == "" {
					t.Errorf("exception %q carries no reason; an unexplained exception is indistinguishable "+
						"from an oversight", dep)
				}
			}

			checked := 0
			for pkg, pkgDeps := range deps {
				if !matches(pkg, r.From) {
					continue
				}
				checked++
				var bad []string
				for _, d := range pkgDeps {
					if d == pkg || !matches(d, r.Forbidden) {
						continue
					}
					// A package never violates a rule by depending on itself
					// or on its own subtree root.
					if strings.HasPrefix(pkg, d+"/") || strings.HasPrefix(d, pkg+"/") {
						continue
					}
					if _, ok := r.Allow[d]; ok {
						continue
					}
					if r.AllowPrefix != "" && strings.HasPrefix(d, r.AllowPrefix) {
						continue
					}
					bad = append(bad, d)
				}
				if len(bad) > 0 {
					sort.Strings(bad)
					t.Errorf("%s depends on %v\n\nWHY THIS RULE EXISTS: %s\n\n"+
						"The dependency may be TRANSITIVE — `go list -deps %s%s` shows the full set. If it "+
						"is genuinely correct, add it to this rule's Allow map with the reason.",
						pkg, bad, r.Why, modulePrefix, pkg)
				}
			}
			// Anti-vacuity per rule: a From pattern that matches nothing
			// passes silently and forever.
			if checked == 0 {
				t.Errorf("rule %q matched NO packages — its From pattern %q is stale and the rule is "+
					"enforcing nothing", r.Name, r.From)
			}
		})
	}
}

// TestEveryAllowExceptionIsStillUsed keeps the exception lists honest: an
// exemption whose dependency has since been removed should go, or the list
// slowly becomes a record of things that used to be true.
func TestEveryAllowExceptionIsStillUsed(t *testing.T) {
	deps := loadDeps(t)
	for _, r := range rules {
		for allowed := range r.Allow {
			used := false
			for pkg, pkgDeps := range deps {
				if !matches(pkg, r.From) {
					continue
				}
				for _, d := range pkgDeps {
					if d == allowed && d != pkg {
						used = true
						break
					}
				}
				if used {
					break
				}
			}
			if !used {
				t.Errorf("rule %q allows %q but nothing depends on it any more — remove the exception so "+
					"the boundary is enforced again", r.Name, allowed)
			}
		}
	}
}

// loadDeps returns module-relative package path -> its transitive deps, for
// every package in the module. One `go list` invocation.
//
// SCOPE, and it is load-bearing: `.Deps` is the PRODUCTION dependency set — it
// excludes test imports, while `go list -deps <pkg>` on the command line
// includes them. The two disagree, so a hand-check that uses the wrong one
// reaches the wrong conclusion (see the package doc).
//
// Test imports are out of scope on purpose. A test legitimately reaches across
// the layering to build a fixture — a pipeline test importing an engine to
// register it, a trigger variant's test importing its base engine to set up a
// source — and forbidding that would either break real tests or push them into
// contortions. The tenet is about what the SHIPPED binary is coupled to.
func loadDeps(t *testing.T) map[string][]string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "go", "list", "-deps=false",
		// The MODULE pattern, not "./..." — a test's working directory is its
		// own package, so "./..." would resolve exactly one package and the
		// gate would pass on nothing. (The anti-vacuity floor below caught
		// precisely that while this file was being written.)
		"-f", "{{.ImportPath}}\t{{join .Deps \" \"}}", modulePrefix+"...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	deps := map[string][]string{}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		pkg := strings.TrimPrefix(parts[0], modulePrefix)
		if pkg == parts[0] {
			continue // outside the module
		}
		var ds []string
		if len(parts) == 2 {
			for _, d := range strings.Fields(parts[1]) {
				if strings.HasPrefix(d, modulePrefix) {
					ds = append(ds, strings.TrimPrefix(d, modulePrefix))
				}
			}
		}
		deps[pkg] = ds
	}
	return deps
}

// matches reports whether pkg is selected by pattern: an exact path, or a
// subtree when the pattern ends in "/...".
func matches(pkg, pattern string) bool {
	if sub, ok := strings.CutSuffix(pattern, "/..."); ok {
		return pkg == sub || strings.HasPrefix(pkg, sub+"/")
	}
	return pkg == pattern
}
