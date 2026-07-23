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

// adrDir returns the repo's docs/adr directory relative to this package.
func adrDir(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "docs", "adr")
}

// statusKeyword extracts the leading status word from a bold status
// blob ("Accepted — SHIPPED v0.99.287 (…)" → "accepted"). Returns ""
// when the text does not start with a known status vocabulary word —
// callers skip those (old index rows are one-line summaries with no
// status at all; the gate is deliberately scoped to rows/files that
// DO declare one).
func statusKeyword(bold string) string {
	word := strings.ToLower(strings.Trim(strings.Fields(bold + " x")[0], ".,;:—-()"))
	switch word {
	case "proposed", "accepted", "implemented", "superseded", "rejected", "discovery", "deferred":
		return word
	}
	return ""
}

// indexStatuses parses docs/adr/README.md's table rows into
// filename → status keyword, for rows that carry a bold status.
func indexStatuses(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(adrDir(t), "README.md"))
	if err != nil {
		t.Fatalf("read ADR index: %v", err)
	}
	rowRe := regexp.MustCompile(`^\| \[[0-9a-z]+\]\(([^)]+)\) \| (.+) \|\s*$`)
	boldRe := regexp.MustCompile(`\*\*([^*]+)\*\*`)
	out := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		m := rowRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		bold := boldRe.FindStringSubmatch(m[2])
		if bold == nil {
			continue // one-line summary row without a status — out of scope
		}
		if kw := statusKeyword(bold[1]); kw != "" {
			out[m[1]] = kw
		}
	}
	return out
}

// fileStatus extracts the status keyword from an ADR file's `## Status`
// section (the first bold token after the heading), "" if the file has
// no parseable bold status.
func fileStatus(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	lines := strings.Split(string(raw), "\n")
	boldRe := regexp.MustCompile(`\*\*([^*]+)\*\*`)
	inStatus := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "## ") {
			inStatus = strings.EqualFold(trimmed, "## Status")
			continue
		}
		if !inStatus {
			continue
		}
		if m := boldRe.FindStringSubmatch(line); m != nil {
			return statusKeyword(m[1])
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
	index := indexStatuses(t)
	if len(index) < 20 {
		t.Fatalf("parsed only %d status-bearing index rows — the README table shape changed; fix the parser before trusting this gate", len(index))
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
			continue // file's Status block has no bold keyword — out of scope
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
	if checked < 20 {
		t.Fatalf("cross-checked only %d index-row/file status pairs — the file-side Status parser stopped matching; fix it before trusting this gate", checked)
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
