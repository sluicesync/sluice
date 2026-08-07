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
)

// An ADR whose Status says "Proposed … no code written yet", for a design
// that shipped, is the same lie as a stale roadmap header — and the two
// gates next door could not catch it, for two different reasons worth
// naming rather than leaving implied.
//
// [TestADRIndexStatusParity] compares the ADR file against the ADR INDEX
// ROW. When both say Proposed they AGREE, and agreement is all that gate
// measures; neither side is evidence about the code.
// [TestADRStatusSelfContradiction] fires only in the opposite direction —
// a header that says Accepted over a body that still says "stays
// Proposed". A file that is uniformly, confidently wrong passes both.
//
// This gate supplies the missing INDEPENDENT number (the 2026-08-01 rule):
// the shipped source. An ADR is a design document, so non-test code under
// `internal/`/`cmd/` citing it by number is evidence someone built it. When
// that meets a `Proposed` status, one of the two is wrong and a human has
// to say which — so this is fail-by-default with written exemptions, not a
// heuristic that guesses.
//
// # Scope, stated so the name cannot be read as broader than the truth
//
//   - It grades STATUS-vs-CITATION, not status-vs-behaviour. An ADR nobody
//     cites by number can be fully shipped and this gate is silent about it.
//     That is the residual, and the cheapest cover for it is the ADR
//     carrying its shipping tag in the Status line as ADR-0067 and
//     ADR-0071 now do.
//   - It scans `internal/` and `cmd/`, non-test files only. Test files are
//     excluded because a test naming an ADR it is REFUSING to implement is
//     a normal thing to write.
//   - A citation is a mention of the ADR's four-digit id in the form
//     `ADR-NNNN`. A comment that cites an ADR to explain why sluice does
//     NOT do something is a legitimate citation of a legitimately-Proposed
//     ADR — hence the exemption roster, not a narrower regex.
//
// It found two: ADR-0067 (shipped in the same squashed commit that
// published "No code written yet") and ADR-0071 (a live, load-bearing
// bounded-memory pump under a bare `Proposed.`).

// adrProposedButCitedExempt names each `Proposed` ADR that non-test code
// legitimately cites, with the reason. Fail-by-default: an unlisted one
// fails the build, and a listed one that stops qualifying fails too, so
// the roster cannot go stale in either direction.
var adrProposedButCitedExempt = map[string]string{
	"adr-0177-pg-publication-column-lists.md": "Discovery/research-only, and its recorded conclusion is NOT to adopt " +
		"(\"sluice has no surface this feature serves today\"). The two citations in engines/pgtrigger/setup.go are " +
		"operator hints naming it as the reason sluice has no column-scope filter — i.e. they cite the NON-adoption. " +
		"Proposed is the correct status and the citations are correct too.",
}

// adrCitedIDsFloor is the anti-vacuity floor on how many DISTINCT ADR ids
// the source scan finds. A scanner that silently stops matching (a path
// change, a regex narrowing) would find zero and this gate would report a
// confident green over a corpus it never read — the shape the mutation-run
// discipline exists to catch. It only ever rises.
const adrCitedIDsFloor = 60

var adrIDInDocName = regexp.MustCompile(`^adr-(\d{4})-`)

// adrCitationRe matches an ADR reference in Go source: `ADR-0067`.
var adrCitationRe = regexp.MustCompile(`\bADR-(\d{4})\b`)

func TestProposedADRsAreNotContradictedByShippedCode(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(adrDir(t), "adr-*.md"))
	if err != nil || len(files) < 100 {
		t.Fatalf("globbed %d ADR files (err %v) — discovery broke", len(files), err)
	}

	// The third vacuity guard (see adrStatusFilesParsed): how many ADR
	// files declare a status this package can read at all. Files that
	// declare none are out of scope for every gate here, and nothing
	// else counts them.
	parsed := 0
	proposed := map[string]string{} // adr filename -> four-digit id
	for _, path := range files {
		kw := fileStatus(t, path)
		if kw == "" {
			continue
		}
		parsed++
		if kw != "proposed" {
			continue
		}
		base := filepath.Base(path)
		m := adrIDInDocName.FindStringSubmatch(base)
		if m == nil {
			t.Fatalf("ADR filename %q does not carry a four-digit id — the id derivation broke", base)
		}
		proposed[base] = m[1]
	}
	if parsed < adrStatusFilesParsed {
		t.Errorf("resolved a status for only %d of %d ADR files, below the pinned floor of %d — a parser narrowed and "+
			"the files it stopped reading are now out of scope for EVERY gate in this package, silently. That is the "+
			"exact hole ADR-0067 sat in; fix the parser rather than lowering the floor", parsed, len(files), adrStatusFilesParsed)
	}
	if len(proposed) == 0 {
		t.Fatalf("no ADR declares Proposed — this gate grades the Proposed set, so an empty one means it is asserting "+
			"nothing. Either the status parser broke, or the repo genuinely has no open designs (in which case delete "+
			"this gate deliberately, do not delete this check). %d files parsed", parsed)
	}

	citations := adrCitationsInShippedCode(t)
	if len(citations) < adrCitedIDsFloor {
		t.Fatalf("the source scan found only %d distinct ADR ids cited in non-test code, below the floor of %d — the "+
			"scan is not reading what it thinks it is, and a green from it would be vacuous",
			len(citations), adrCitedIDsFloor)
	}

	for base, id := range proposed {
		sites := citations[id]
		if len(sites) == 0 {
			continue
		}
		reason, exempt := adrProposedButCitedExempt[base]
		if !exempt {
			t.Errorf("%s declares Status **Proposed** but %d non-test source file(s) under internal/ and cmd/ cite ADR-%s "+
				"(e.g. %s). One of the two is wrong:\n"+
				"  - if the design SHIPPED, fix the Status block — say Accepted, name the shipping tag and commit, and "+
				"correct any present-tense prose describing the problem it solved (ADR-0067 and ADR-0071 are the worked "+
				"examples);\n"+
				"  - if it genuinely has not shipped and the code merely REFERS to it (e.g. as the recorded reason not to "+
				"adopt something), add it to adrProposedButCitedExempt with that reason.",
				base, len(sites), id, strings.Join(firstN(sites, 3), ", "))
		} else if strings.TrimSpace(reason) == "" {
			t.Errorf("%s is exempt with an empty reason. The reason IS the gate here — an exemption nobody has to justify "+
				"is the same as no gate", base)
		}
	}

	for base := range adrProposedButCitedExempt {
		id, stillProposed := proposed[base]
		if !stillProposed {
			t.Errorf("adrProposedButCitedExempt lists %s, which no longer declares Status Proposed. A stale exemption "+
				"quietly blesses whatever that ADR says next — remove the entry", base)
			continue
		}
		if len(citations[id]) == 0 {
			t.Errorf("adrProposedButCitedExempt lists %s, but no non-test source file cites ADR-%s any more. The "+
				"exemption is describing a citation that no longer exists — remove it", base, id)
		}
	}
}

// adrCitationsInShippedCode maps a four-digit ADR id to the non-test Go
// files under internal/ and cmd/ that cite it.
//
// Non-test only, deliberately: a test naming an ADR it deliberately does
// not implement (a refusal pin, a fixture) is normal, and grading it would
// force in exactly the exemption sprawl this gate is designed to avoid.
func adrCitationsInShippedCode(t *testing.T) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, root := range []string{"internal", "cmd"} {
		dir := filepath.Join("..", "..", root)
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			seen := map[string]bool{}
			for _, m := range adrCitationRe.FindAllStringSubmatch(string(raw), -1) {
				if seen[m[1]] {
					continue
				}
				seen[m[1]] = true
				out[m[1]] = append(out[m[1]], filepath.ToSlash(path))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	for id := range out {
		sort.Strings(out[id])
	}
	return out
}

func firstN(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
