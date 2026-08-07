// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

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

// psdbHostClaimHomes are the docs that carry the operator-facing
// "which driver can drive a PlanetScale MySQL host" claim. Every one of
// them must carry BOTH markers, and every marker must agree with the
// code.
//
// Three homes is the whole population as of 2026-08-07, found by
// grepping the endpoint suffix across the tree — and the reason there
// is a list at all is that the previous two corrections each fixed ONE
// home and left the others: v0.99.169 corrected the README, v0.100.0
// shipped the refusal, and production-readiness.md went on saying the
// opposite of both. [TestPSDBMySQLHostClaimHasNoUngatedHome] is what
// keeps this list from going stale when a fourth home appears.
//
// Paths are slash-separated and relative to the repo root.
var psdbHostClaimHomes = []string{
	"README.md",
	"docs/production-readiness.md",
	"docs/operator/error-codes.md",
}

// psdbHostMentionExempt names the docs that mention a PlanetScale MySQL
// endpoint suffix WITHOUT making the driver claim, each with the reason
// it is not a home. A file here is deliberately ungated; a file in
// neither map nor [psdbHostClaimHomes] fails the build.
//
// Keys are slash-separated paths relative to the repo root.
var psdbHostMentionExempt = map[string]string{
	"CHANGELOG.md": "historical archive: entries describe the behaviour of the release they " +
		"belong to and must NOT be rewritten to match today's code",
	"docs/dev/roadmap.md": "developer history: the v0.8.1 entry records what that chunk shipped, " +
		"not what is true now",
}

// The "which driver can drive a PlanetScale MySQL host" claim, kept
// honest against the code.
//
// # Why this exists
//
// This is the same sentence corrected for the third time. v0.99.169
// corrected the README's version of it ("a `*.connect.psdb.cloud` host
// does not auto-select VStream"); v0.100.0 went further and made the
// pairing an up-front REFUSAL
// ([mysql.Engine.ValidateDSN] → `SLUICE-E-DRIVER-HOST-MISMATCH`); and
// docs/production-readiness.md kept telling operators the host "also
// works under `--source-driver mysql`" through every release after
// that. Nothing was holding the sentence to anything, so each
// correction was a correction of one copy.
//
// It is a claim with a mechanical answer, and the answer has two
// halves that have to be checked together — WHICH HOSTS count, and
// WHICH DRIVERS can drive them. Pinning either alone leaves the
// operator-facing sentence half-bound: the suffix list is a
// package-level slice whose own comment invites a one-line edit, and
// the driver verdict is a runtime branch on the flavor.
//
// # The two markers
//
// Prose stays prose; these are what fail:
//
//	<!-- psdb-mysql-host-suffixes: *.connect.psdb.cloud, *.private-connect.psdb.cloud -->
//	<!-- psdb-mysql-host-drivers: mariadb=refused, mysql=refused, planetscale=ok, vitess=ok -->
//
// The drivers marker carries the whole graded map rather than just the
// engines that WORK, so it cannot be read as broader than the truth:
// every driver that implements the host check appears with its actual
// verdict, and an engine's absence means only that it does not
// implement the check.
//
// # Scope — what it grades, and what it deliberately does not
//
// The graded universe is the registered engines implementing
// [ir.DSNValidator]. Today that is exactly the four mysql-package
// flavors. It is NOT "every engine an operator could type": an engine
// without the surface (postgres, sqlite, the flat-file readers) is
// skipped by the pipeline preflight entirely, which is a different
// answer from "accepts" and is not what the marker records.
//
// It grades the DSN-string refusal and nothing downstream. In
// particular it says nothing about which COMMANDS consult it —
// `preflightDSNValidation` runs on `migrate` and `sync` only, so
// `backup` / `schema diff` / `schema preview` reach a non-VStream
// flavor against a PlanetScale host without ever asking. That
// reachability split is what
// `TestDefaultExcludeCallSitesRecordTheirDSNPreflight` in
// internal/pipeline records; it is stated here so this gate's name
// cannot be read as covering it.
//
// # Two derivations that share no evidence
//
// The graded set is derived twice and the two must agree: from the
// SOURCE (an AST pass for the `var _ ir.DSNValidator` capability pin,
// mapped through the live registry to engine names) and from
// BEHAVIOUR (a runtime type assertion on each registered engine). The
// verdicts themselves come from actually CALLING ValidateDSN with a
// DSN built from each suffix the engine source declares — which is
// also what binds the two halves: a suffix nobody refuses fails here
// rather than sitting in the marker as decoration.
func TestPSDBMySQLHostDriverClaimMatchesTheCode(t *testing.T) {
	suffixes := psdbMySQLHostSuffixesFromCode(t)
	verdicts := psdbMySQLHostVerdicts(t, suffixes)

	wantSuffixes := make([]string, 0, len(suffixes))
	for _, s := range suffixes {
		wantSuffixes = append(wantSuffixes, "*"+s)
	}
	sort.Strings(wantSuffixes)

	wantDrivers := make([]string, 0, len(verdicts))
	for name, verdict := range verdicts {
		wantDrivers = append(wantDrivers, name+"="+verdict)
	}
	sort.Strings(wantDrivers)

	markers := []struct {
		name string
		re   *regexp.Regexp
		want []string
		why  string
	}{
		{
			name: "psdb-mysql-host-suffixes",
			re:   regexp.MustCompile(`<!--\s*psdb-mysql-host-suffixes:\s*([^>]*?)\s*-->`),
			want: wantSuffixes,
			why: "These are the endpoint suffixes mysql.planetScaleMySQLHostSuffixes declares, rendered as " +
				"the globs the prose uses. Adding a suffix in the engine is a one-line edit BY DESIGN — this " +
				"failure is that edit reaching the docs, not a bug: put the new suffix in every marker and in " +
				"the sentence beside it.",
		},
		{
			name: "psdb-mysql-host-drivers",
			re:   regexp.MustCompile(`<!--\s*psdb-mysql-host-drivers:\s*([^>]*?)\s*-->`),
			want: wantDrivers,
			why: "Every registered driver implementing ir.DSNValidator, with what it ACTUALLY does when handed " +
				"a host under those suffixes. `refused` means the run stops at the preflight with " +
				"SLUICE-E-DRIVER-HOST-MISMATCH before any connection — so that driver cannot be described as " +
				"working against such a host, not in the docs, not in a CHANGELOG entry, not in release notes.",
		},
	}

	for _, home := range psdbHostClaimHomes {
		raw, err := os.ReadFile(repoFile(home))
		if err != nil {
			t.Fatalf("read %s: %v", home, err)
		}
		for _, m := range markers {
			sub := m.re.FindSubmatch(raw)
			if sub == nil {
				t.Errorf("%s carries no `<!-- %s: … -->` marker. It is one of the %d homes of a claim this "+
					"project has now corrected three times; add it listing: %s",
					home, m.name, len(psdbHostClaimHomes), strings.Join(m.want, ", "))
				continue
			}
			if got := splitList(string(sub[1])); !equalStringSets(m.want, got) {
				t.Errorf("%s's `%s` marker disagrees with the code.\n  code: %s\n  doc:  %s\n\n%s",
					home, m.name, strings.Join(m.want, ", "), strings.Join(got, ", "), m.why)
			}
		}
	}
}

// TestPSDBMySQLHostClaimHasNoUngatedHome fails when a doc names a
// PlanetScale MySQL endpoint suffix and is neither a gated home nor
// exempt with a written reason.
//
// The gate above holds three files. On its own that is exactly the
// shape this project keeps paying for — a check whose coverage is a
// hand-kept list, silently narrower than the claim it protects — since
// a fourth copy of the sentence would be ungated and invisible. This
// makes a new copy a build failure that forces the decision: gate it,
// or say why it does not need gating.
//
// # Scope
//
// It walks the repo-root markdown files and docs/ recursively, minus
// docs/releases/ (published release notes are immutable records of what
// a past version did). It does not walk Go doc comments or files
// outside those two roots.
func TestPSDBMySQLHostClaimHasNoUngatedHome(t *testing.T) {
	suffixes := psdbMySQLHostSuffixesFromCode(t)

	homes := map[string]bool{}
	for _, h := range psdbHostClaimHomes {
		homes[h] = true
	}

	root := filepath.Join("..", "..")
	var mentions []string
	walk := func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			// Published release notes are a record, not a claim
			// surface; dot-directories are tooling state.
			if rel == "docs/releases" || strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".md") {
			return nil
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for _, s := range suffixes {
			if strings.Contains(string(raw), s) {
				mentions = append(mentions, rel)
				return nil
			}
		}
		return nil
	}

	for _, sub := range []string{".", "docs"} {
		dir := filepath.Join(root, sub)
		var err error
		if sub == "." {
			// Root: the markdown files only, not the whole tree.
			entries, rerr := os.ReadDir(dir)
			if rerr != nil {
				t.Fatalf("read %s: %v", dir, rerr)
			}
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				err = walk(filepath.Join(dir, e.Name()), e, nil)
				if err != nil {
					t.Fatalf("scan %s: %v", e.Name(), err)
				}
			}
			continue
		}
		if err = filepath.WalkDir(dir, walk); err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}

	// Anti-vacuity floor. A scan that finds nothing agrees with any
	// roster — and it would, silently, if the suffix literals ever moved
	// or the walk roots changed.
	if len(mentions) < len(psdbHostClaimHomes) {
		t.Fatalf("the markdown scan found only %d file(s) naming a PlanetScale MySQL endpoint suffix (%s), "+
			"fewer than the %d gated homes — the scan broke rather than the docs shrinking",
			len(mentions), strings.Join(mentions, ", "), len(psdbHostClaimHomes))
	}

	sort.Strings(mentions)
	for _, rel := range mentions {
		if homes[rel] {
			continue
		}
		if _, exempt := psdbHostMentionExempt[rel]; exempt {
			continue
		}
		t.Errorf("%s names a PlanetScale MySQL endpoint suffix but is neither gated nor exempt.\n\n"+
			"If it makes the driver claim, add it to psdbHostClaimHomes and give it both markers — the claim "+
			"has been corrected in one copy at a time twice already, which is the whole reason this check "+
			"exists. If it only mentions the host in passing, add it to psdbHostMentionExempt with the reason.",
			rel)
	}

	for rel := range psdbHostMentionExempt {
		found := false
		for _, m := range mentions {
			if m == rel {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("psdbHostMentionExempt lists %q, which no longer names a PlanetScale MySQL endpoint "+
				"suffix. Drop the exemption — a stale one is an unexamined hole the next time that file "+
				"grows the claim", rel)
		}
	}
}

// repoFile turns a slash-separated repo-relative path into one this
// package's tests can open.
func repoFile(rel string) string {
	return filepath.Join("..", "..", filepath.FromSlash(rel))
}

// psdbMySQLHostSuffixesFromCode reads the endpoint suffix list out of
// the mysql engine's source.
//
// The list is a package-level `[]string` whose own comment says adding
// one is a one-line edit, so the doc marker derives from it rather than
// restating it: a new suffix then fails the doc gate by design, and the
// failure message says so.
func psdbMySQLHostSuffixesFromCode(t *testing.T) []string {
	t.Helper()
	const varName = "planetScaleMySQLHostSuffixes"

	dir := filepath.Join("..", "..", "internal", "engines", "mysql")
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}

	var out []string
	fset := token.NewFileSet()
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".go") || strings.HasSuffix(f.Name(), "_test.go") {
			continue
		}
		parsed, perr := parser.ParseFile(fset, filepath.Join(dir, f.Name()), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", filepath.Join(dir, f.Name()), perr)
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			vs, ok := n.(*ast.ValueSpec)
			if !ok || len(vs.Names) != 1 || vs.Names[0].Name != varName || len(vs.Values) != 1 {
				return true
			}
			lit, ok := vs.Values[0].(*ast.CompositeLit)
			if !ok {
				t.Fatalf("%s is no longer a composite literal; this derivation cannot read it", varName)
			}
			for _, elt := range lit.Elts {
				bl, ok := elt.(*ast.BasicLit)
				if !ok || bl.Kind != token.STRING {
					t.Fatalf("%s holds a non-literal element %T; this derivation cannot read it", varName, elt)
				}
				v, uerr := strconv.Unquote(bl.Value)
				if uerr != nil {
					t.Fatalf("unquote %s element %s: %v", varName, bl.Value, uerr)
				}
				out = append(out, v)
			}
			return false
		})
	}

	if len(out) == 0 {
		t.Fatalf("found no %s in internal/engines/mysql — the derivation broke, and an empty suffix list "+
			"would agree with a doc marker saying anything", varName)
	}
	for _, s := range out {
		if !strings.HasPrefix(s, ".") || !strings.Contains(s, "psdb") {
			t.Fatalf("%s element %q is not a PSDB DNS suffix; the AST pass read the wrong declaration", varName, s)
		}
	}
	sort.Strings(out)
	return out
}

// psdbMySQLHostVerdicts asks every registered engine implementing
// [ir.DSNValidator] what it does with a host under each declared
// suffix, returning "ok" or "refused" per engine name.
//
// Three things are checked on the way, because a verdict map is only
// worth as much as the thing it separates:
//
//   - The behaviour-derived universe (a runtime type assertion) must
//     equal the structure-derived one (the AST capability pin, mapped
//     through the registry). The two share no evidence.
//   - Every suffix must produce the SAME verdict for a given engine. A
//     suffix-dependent answer cannot be written as a flat marker, so it
//     fails here rather than being flattened into one.
//   - A control host that matches no suffix must be accepted by ALL of
//     them, and so must a PlanetScale POSTGRES host — otherwise
//     "refused" would be recording a blanket refusal or a substring
//     match rather than the MySQL-endpoint rule the docs describe.
func psdbMySQLHostVerdicts(t *testing.T, suffixes []string) map[string]string {
	t.Helper()

	fromStructure := registeredEnginesImplementing(t, "DSNValidator")

	var fromBehaviour []string
	validators := map[string]ir.DSNValidator{}
	for _, name := range engines.Names() {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() listed %q but engines.Get(%q) missed", name, name)
		}
		if v, ok := e.(ir.DSNValidator); ok {
			validators[name] = v
			fromBehaviour = append(fromBehaviour, name)
		}
	}
	sort.Strings(fromBehaviour)

	if !equalStringSets(fromStructure, fromBehaviour) {
		t.Fatalf("the two derivations of the DSN-validating engine set disagree — resolve that before "+
			"trusting either.\n  structure (AST: package declares `var _ ir.DSNValidator`): %s\n"+
			"  behaviour (registered engine satisfies the interface):     %s",
			strings.Join(fromStructure, ", "), strings.Join(fromBehaviour, ", "))
	}
	if len(fromBehaviour) == 0 {
		t.Fatal("no registered engine implements ir.DSNValidator — the driver/host preflight is a no-op for " +
			"every engine, and this gate cannot be allowed to pass on an empty set")
	}

	// Controls. `.pg.psdb.cloud` is PlanetScale's POSTGRES endpoint and
	// is deliberately absent from the MySQL suffix list — it is here so
	// a derivation that matched on "psdb" anywhere in the host would
	// fail instead of quietly widening the claim.
	const (
		controlHost = "u:p@tcp(db.internal.example.com:3306)/db"
		pgPSDBHost  = "u:p@tcp(aws.pg.psdb.cloud:5432)/db"
	)
	for name, v := range validators {
		for _, dsn := range []string{controlHost, pgPSDBHost} {
			if err := v.ValidateDSN(dsn); err != nil {
				t.Fatalf("%s.ValidateDSN(%q) = %v; want nil. The refusal is supposed to be keyed on the "+
					"PlanetScale MySQL endpoint suffixes, so a control host refusing means the \"refused\" "+
					"verdicts below record something broader than the docs claim", name, dsn, err)
			}
		}
	}

	verdicts := map[string]string{}
	for name, v := range validators {
		for _, s := range suffixes {
			dsn := fmt.Sprintf("u:p@tcp(aws%s:3306)/db?tls=true", s)
			verdict := "ok"
			if err := v.ValidateDSN(dsn); err != nil {
				verdict = "refused"
			}
			if prior, seen := verdicts[name]; seen && prior != verdict {
				t.Fatalf("%s answers the endpoint suffixes differently (%q vs %q at suffix %q). A flat "+
					"marker cannot represent a per-suffix verdict; decide what this driver does with a "+
					"PlanetScale MySQL host and make every suffix say so", name, prior, verdict, s)
			}
			verdicts[name] = verdict
		}
	}

	// Anti-vacuity floor. If every graded driver gave the same answer
	// the marker would separate nothing, and the sentence beside it —
	// which is entirely about the difference between the two answers —
	// would be bound to a constant.
	var nOK, nRefused int
	for _, verdict := range verdicts {
		if verdict == "ok" {
			nOK++
			continue
		}
		nRefused++
	}
	if nOK == 0 || nRefused == 0 {
		t.Fatalf("every DSN-validating driver returned the same verdict (%d ok, %d refused) for a "+
			"PlanetScale MySQL host, so this derivation separates nothing. sluice ships both kinds — the "+
			"VStream flavors drive that host and the binlog flavors refuse it; if that genuinely changed, "+
			"rewrite the docs and this gate together — do not delete this check", nOK, nRefused)
	}
	return verdicts
}
