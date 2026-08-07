// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The ADR index (docs/adr/README.md) repeats each ADR's status in its
// summary row, and the row LAGS the file: the 2026-07-23 audit found
// rows 0175/0176 still saying "Proposed" while the ADR files' own
// Status blocks said Accepted-shipped / Implemented (DOC-3) — the exact
// stale-status class the CLAUDE.md working agreement documents as
// repeatedly costing ground-truthing passes. These tests are the G-17
// ratchet: index-vs-file status parity, plus the DOC-5 self-
// contradiction shape (a file whose Status header says Implemented
// while its body still says the decision "stays Proposed").
//
// A status counts however it is spelled. Both parsers originally read
// only bold tokens, which silently exempted every row and every
// `## Status` section that declared its status in plain text — 76 index
// rows and 26 files, including the ADR-0091 lag the gate exists to
// catch. Read declaredStatus and the vacuity constants together: the
// constants are what stop that from happening again unnoticed.

// adrDir returns the repo's docs/adr directory relative to this package.
func adrDir(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "docs", "adr")
}

// statusKeyword extracts the leading status word from a status blob
// ("Accepted — SHIPPED v0.99.287 (…)" → "accepted"). Returns "" when
// the text does not start with a known status vocabulary word —
// callers skip those (old index rows are one-line summaries with no
// status at all; the gate is deliberately scoped to rows/files that
// DO declare one). Leading `*` is trimmed so an unterminated bold
// run-on ("**Accepted — shipped …" wrapping to the next line) still
// reads as a declaration.
func statusKeyword(text string) string {
	word := strings.ToLower(strings.Trim(strings.Fields(text + " x")[0], "*.,;:—-()"))
	switch word {
	case "proposed", "accepted", "implemented", "superseded", "rejected", "discovery", "deferred":
		return word
	}
	return ""
}

var boldRe = regexp.MustCompile(`\*\*([^*]+)\*\*`)

// declaredStatus reads a status keyword out of one chunk of markdown —
// a bold blob if there is one ("**Accepted** — …"), otherwise the
// chunk's own leading word ("Accepted — …"). Both spellings are in
// live use in this repo, and reading only the bold one is what let the
// ADR-0091 lag survive the gate.
func declaredStatus(text string) string {
	if m := boldRe.FindStringSubmatch(text); m != nil {
		if kw := statusKeyword(m[1]); kw != "" {
			return kw
		}
	}
	return statusKeyword(text)
}

// The two vacuity guards below exist because this gate has already
// gone quietly vacuous once. Both parsers originally read ONLY bold
// tokens, so an index row or a `## Status` section that declared its
// status in plain text was silently exempted — and ADR-0091 declared
// "Accepted" unbolded in the index while its file still said
// "**Proposed (2026-06-14).**", i.e. exactly the lag this gate is for,
// sitting in the corpus, green, for ~40 releases. Counting what the
// gate evaluates is what turns a future re-narrowing from a silent
// coverage loss into a failure.

// adrIndexExemptRows caps how many docs/adr/README.md table rows may
// declare NO status in any form — the legacy one-line summary rows
// (roughly ADR-0001..0073, plus 0131 whose lead bold is a correction
// note rather than a status). That set is historical and can only
// SHRINK: every row written since carries a status.
const adrIndexExemptRows = 73

// adrStatusPairsChecked is the floor on how many index-row/ADR-file
// status PAIRS the gate actually cross-checks. It only ever rises (a
// new status-bearing ADR adds a pair); a drop means one of the two
// parsers narrowed and the gate went partly vacuous without failing.
const adrStatusPairsChecked = 110

// adrStatusFilesParsed is the floor on how many ADR FILES [fileStatus]
// resolves a status for. It is the third vacuity guard, and it exists
// because the first two could not see the hole that produced it: an ADR
// whose file declares no parseable status is out of scope for BOTH gates
// above, and neither of them counts that. ADR-0067 sat there for ~26
// releases saying "Proposed … No code written yet" about a design that
// shipped in the same commit, because the first non-blank line of its
// `## Status` section is an ADR-0087 amendment blockquote and the scan
// gave up on it. Fixed in the scan (blockquote/admonition lines are
// skipped, not terminal) and held here: if a parser narrows again, the
// count drops and this fails instead of the coverage silently shrinking.
const adrStatusFilesParsed = 182

// indexStatuses parses docs/adr/README.md's table rows into
// filename → status keyword. A row declares its status either as a
// bold blob ("| **Accepted** — …") or as plain leading text
// ("| Accepted — …"); both count. Returns the map plus the number of
// rows that declared no status at all.
func indexStatuses(t *testing.T) (statuses map[string]string, exemptRows int) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(adrDir(t), "README.md"))
	if err != nil {
		t.Fatalf("read ADR index: %v", err)
	}
	rowRe := regexp.MustCompile(`^\| \[[0-9a-z]+\]\(([^)]+)\) \| (.+) \|\s*$`)
	out := map[string]string{}
	exempt := 0
	for _, line := range strings.Split(string(raw), "\n") {
		m := rowRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		kw := declaredStatus(m[2])
		if kw == "" {
			exempt++ // one-line summary row without a status — out of scope
			continue
		}
		out[m[1]] = kw
	}
	return out, exempt
}

// adrStatusLineRe matches the two header-metadata spellings of a
// status line that ADRs written without a `## Status` section use:
// the MADR-ish bullet (`- Status: Accepted`, `- **Status:** Accepted`)
// and the bare lead paragraph (`**Status:** **Accepted (…)`).
var adrStatusLineRe = regexp.MustCompile(`^(?:[-*]\s+)?\*{0,2}Status:?\*{0,2}\s*:?\s*(.+)$`)

// fileStatus extracts the status keyword an ADR file declares, "" if it
// declares none. Three spellings are in live use and all three count —
// a `## Status` section (the oldest), a `- Status:` header bullet, and
// a `**Status:**` lead paragraph. Reading only the first left 32 of the
// index's status-bearing rows uncheckable, which is coverage the
// adrStatusPairsChecked floor now holds.
//
// A leading BLOCKQUOTE inside the `## Status` section is skipped rather
// than treated as the section's declaration. Amendment and correction
// notes are written as `> **Amended by ADR-XXXX:** …` above the status
// line, and stopping at the first one made the whole FILE invisible to
// both gates — the ADR-0067 hole (see [adrStatusFilesParsed]). Skipping
// is the honest reading: a blockquote annotates the status, it is not
// the status. Anything else non-blank still terminates the scan, so a
// section whose real declaration is genuinely absent stays out of scope
// rather than being guessed at from later prose.
func fileStatus(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	lines := strings.Split(string(raw), "\n")
	inStatus := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "## ") {
			inStatus = strings.EqualFold(trimmed, "## Status")
			continue
		}
		if !inStatus || trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, ">") {
			continue
		}
		// First non-blank, non-blockquote line of the section. Read it
		// bold-or-plain — "Accepted — design." and "Accepted
		// (implemented; shipping)." are as much a declaration as
		// "**Accepted**", and the bold-only reading exempted 26 ADRs
		// from the gate.
		if kw := declaredStatus(trimmed); kw != "" {
			return kw
		}
		break
	}
	for _, line := range lines {
		m := adrStatusLineRe.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			continue
		}
		if kw := declaredStatus(m[1]); kw != "" {
			return kw
		}
	}
	return ""
}

// TestADRIndexStatusParity fails when the index row's declared status
// contradicts the ADR file's own Status header in the lag directions
// that have bitten: index says Proposed while the file says
// Accepted/Implemented (the DOC-3 shape), and the reverse (an index row
// promoted ahead of its file). Rows/files without a parseable status
// are out of scope by design; the vacuity guards keep the scoping
// honest.
func TestADRIndexStatusParity(t *testing.T) {
	index, exempt := indexStatuses(t)
	if len(index) < 20 {
		t.Fatalf("parsed only %d status-bearing index rows — the README table shape changed; fix the parser before trusting this gate", len(index))
	}
	if exempt > adrIndexExemptRows {
		t.Errorf("%d ADR index rows declare no status, up from the pinned %d — the gate just stopped evaluating rows it used to cover. Either give the new rows a status, or (if a row genuinely has none) raise adrIndexExemptRows deliberately and say why", exempt, adrIndexExemptRows)
	}

	checked := 0
	for file, indexKw := range index {
		path := filepath.Join(adrDir(t), file)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("ADR index row links %s, which does not exist: %v", file, err)
			continue
		}
		fileKw := fileStatus(t, path)
		if fileKw == "" {
			continue // the ADR file declares no status — out of scope
		}
		checked++
		implemented := func(kw string) bool { return kw == "accepted" || kw == "implemented" }
		if indexKw == "proposed" && implemented(fileKw) {
			t.Errorf("%s: index row says Proposed but the ADR's Status header says %s — the index lags the decision (audit 2026-07-23 DOC-3 / G-17); update docs/adr/README.md", file, fileKw)
		}
		if fileKw == "proposed" && implemented(indexKw) {
			t.Errorf("%s: index row says %s but the ADR's Status header still says Proposed — one of the two is wrong; reconcile them", file, indexKw)
		}
	}
	if checked < adrStatusPairsChecked {
		t.Errorf("cross-checked only %d index-row/file status pairs, down from the pinned floor of %d — coverage shrank (a parser narrowed, or an ADR's Status block stopped being parseable). Fix that before lowering this floor; the repo has been burned by tests that pass because they stopped testing", checked, adrStatusPairsChecked)
	}
}

// TestADRStatusSelfContradiction pins the DOC-5 self-contradiction
// class: a file whose Status header says Accepted/Implemented while its
// body still asserts the decision "stays Proposed" (ADR-0176 line 20
// carried exactly this after its header flipped). "Prior status:
// Proposed" retrospectives are fine — only the present-tense "stays
// Proposed" phrasing is the contradiction.
func TestADRStatusSelfContradiction(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(adrDir(t), "adr-*.md"))
	if err != nil || len(files) < 100 {
		t.Fatalf("globbed %d ADR files (err %v) — discovery broke", len(files), err)
	}
	staysProposed := regexp.MustCompile(`(?i)\bstays Proposed\b`)
	for _, path := range files {
		kw := fileStatus(t, path)
		if kw != "accepted" && kw != "implemented" {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if loc := staysProposed.FindIndex(raw); loc != nil {
			t.Errorf("%s: Status header says %s but the body still says %q — present-tense Proposed prose survived the status flip (audit 2026-07-23 DOC-5 / G-17)", filepath.Base(path), kw, string(raw[loc[0]:loc[1]]))
		}
	}
}
