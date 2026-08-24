// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
)

// The registered-engine ENUMERATIONS in the README and the production-
// readiness page, kept honest against the live registry — the G-10
// "README freshness floor" residual.
//
// # Why this exists
//
// Both documents spell out the full engine list ("fourteen registered
// engines today (`sluice engines` lists them): `mysql`, …") — a claim with
// a mechanical answer that had no gate, so the fifteenth engine would ship
// with every copy of the sentence silently one short, exactly the
// doc-lags-code shape this package exists to remove. Unlike the sibling
// gates, the checkable form here is not a hidden marker: the VISIBLE
// enumeration is machine-parseable as written, so this gate grades the
// prose itself — a marker beside it would just be a second copy of the
// same list.
//
// # What it grades
//
//   - Every enumeration site — a "`sluice engines`…): `a`, `b`, …" sentence
//     — must list exactly the registered engine names. Three sites today:
//     README "Architecture in one paragraph", README glossary "Engine",
//     production-readiness "Supported engines and directions".
//   - Every spelled-out engine COUNT ("fourteen" within a hundred-odd
//     characters of the word "engine") must be the English word for the
//     registry's size, so "Most of the fourteen are source-only" cannot
//     quietly stay at fourteen.
//
// # Anti-vacuity floors, and why each exists
//
// A prose-anchored gate's failure mode is the anchor phrase being reworded
// out from under it — the gate then finds zero sites and passes while
// grading nothing. So the site count per home is a FLOOR, not a scan
// result: fewer sites than the home is known to carry fails loudly. The
// count-word check has the same floor for the same reason, and the
// registry side fails on a partial world (a missing blank import would
// otherwise shrink the "truth" to match a stale doc).
//
// # Scope, stated
//
// It reaches README.md and docs/production-readiness.md — every home of
// the full-enumeration sentence (re-grepped 2026-08-23: no other tracked
// doc spells out the complete registered-engine list; per-capability
// SUBSETS are the sibling markers' job, not this gate's). The CLI's own
// `sluice engines` output needs no gate: it iterates the registry at
// runtime and cannot drift from it.
func TestEngineEnumerationsMatchTheRegistry(t *testing.T) {
	fromCode := append([]string(nil), engines.Names()...)
	sort.Strings(fromCode)

	// Registry floors: a shrunken registry means the enumeration broke,
	// not that sluice lost engines. The three named engines live in three
	// different packages, so losing any one blank-import strand is caught.
	if len(fromCode) < 10 {
		t.Fatalf("only %d engines registered (%s) — a blank import is missing or the registry did not "+
			"populate; this gate must not hold the docs to a partial world",
			len(fromCode), strings.Join(fromCode, ", "))
	}
	for _, must := range []string{"mysql", "postgres", "d1-trigger"} {
		if !containsString(fromCode, must) {
			t.Fatalf("the %q engine is not in the registry this gate scanned — the enumeration cannot be "+
				"checked against a world that lost it", must)
		}
	}

	countWord, ok := engineCountWords[len(fromCode)]
	if !ok {
		t.Fatalf("engineCountWords has no English word for %d engines — extend the table (it exists so the "+
			"docs' spelled-out count is graded, not just the list)", len(fromCode))
	}

	// An enumeration site is the anchor phrase `sluice engines` followed by
	// a colon and a comma-separated run of backtick-quoted engine names.
	// The name charset is lowercase+digits+hyphen, which is what keeps the
	// match from running on into the next sentence's `pkg.Symbol` spans.
	siteRe := regexp.MustCompile("`sluice engines`[^)\n]*\\):\\s*((?:`[a-z0-9-]+`,\\s*)+`[a-z0-9-]+`)")
	nameRe := regexp.MustCompile("`([a-z0-9-]+)`")
	wordRe := regexp.MustCompile(`(?i)\b(ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)\b`)

	homes := []struct {
		path string
		// minSites/minCounts are floors on what the home is KNOWN to carry
		// (see the doc comment); a rewording that removes one must lower
		// the floor here deliberately, not silently shrink the scan.
		minSites, minCounts int
	}{
		{filepath.Join("..", "..", "README.md"), 2, 2},
		{filepath.Join("..", "..", "docs", "production-readiness.md"), 1, 2},
	}
	for _, home := range homes {
		raw, err := os.ReadFile(home.path)
		if err != nil {
			t.Fatalf("read %s: %v", home.path, err)
		}
		text := string(raw)

		sites := siteRe.FindAllStringSubmatch(text, -1)
		if len(sites) < home.minSites {
			t.Errorf("%s: found %d engine-enumeration site(s), expected at least %d. Either the enumeration "+
				"was removed, or the \"(`sluice engines` …): `a`, `b`\" anchor phrase was reworded — rework "+
				"this gate's anchor with it; a gate that finds nothing grades nothing", home.path, len(sites), home.minSites)
			continue
		}
		for _, m := range sites {
			var fromDoc []string
			for _, nm := range nameRe.FindAllStringSubmatch(m[1], -1) {
				fromDoc = append(fromDoc, nm[1])
			}
			sort.Strings(fromDoc)
			if !equalStringSets(fromCode, fromDoc) {
				t.Errorf("%s: an engine enumeration disagrees with the registry.\n"+
					"  registry: %s\n"+
					"  doc:      %s\n\n"+
					"Update every enumeration site AND the spelled-out count near it (this file may carry "+
					"more than one).", home.path, strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
			}
		}

		// The spelled-out counts. Any ten..twenty number-word with the word
		// "engine" nearby is read as an engine count and must be the
		// registry's; a count claim in prose this gate cannot see would be
		// exactly the staleness it exists to stop.
		counts := 0
		for _, loc := range wordRe.FindAllStringIndex(text, -1) {
			lo, hi := loc[0]-120, loc[1]+120
			if lo < 0 {
				lo = 0
			}
			if hi > len(text) {
				hi = len(text)
			}
			if !strings.Contains(strings.ToLower(text[lo:hi]), "engine") {
				continue
			}
			counts++
			if got := strings.ToLower(text[loc[0]:loc[1]]); got != countWord {
				t.Errorf("%s: a spelled-out engine count says %q where the registry has %d (%q) engines — "+
					"context: …%s…", home.path, got, len(fromCode), countWord,
					strings.TrimSpace(text[lo:hi]))
			}
		}
		if counts < home.minCounts {
			t.Errorf("%s: found %d spelled-out engine count(s), expected at least %d — either the counts "+
				"were reworded to digits (fine: lower the floor here deliberately) or the scan broke",
				home.path, counts, home.minCounts)
		}
	}
}

// engineCountWords maps a registry size to the English word the docs
// spell it with. Deliberately narrow: a size outside it fails the gate
// with instructions rather than passing ungraded.
var engineCountWords = map[int]string{
	10: "ten", 11: "eleven", 12: "twelve", 13: "thirteen", 14: "fourteen",
	15: "fifteen", 16: "sixteen", 17: "seventeen", 18: "eighteen",
	19: "nineteen", 20: "twenty",
}
