// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// CHANGELOG.md must have one heading per released version, in descending
// order, with no duplicated section headings inside a version block.
//
// # Why this exists
//
// Adding a release entry means INSERTING a heading above the previous one.
// Twice on 2026-08-04 the edit REPLACED the previous heading instead: the
// v0.109.1 block ended up under `## [0.110.0]`, was fixed, and then the
// v0.110.1 edit did the identical thing to v0.110.0 — hours later, by the
// same author, who had just written a commit message about it.
//
// The consequence is not cosmetic. An operator reading the CHANGELOG sees a
// fix attributed to a version that did not contain it, and a released,
// tagged, asset-bearing version that appears never to have existed. The first
// occurrence made the v0.109.1 VStream cold-copy fix look unreleased to anyone
// already running it.
//
// Two independent tells catch the mistake, and both are checked here because
// each alone can be defeated: the version sequence skips a number, and the
// merged block contains its section headings twice.
//
// A comment reminding people to insert rather than replace would not have
// worked — the second occurrence happened with that lesson fresh.

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var changelogVersionRE = regexp.MustCompile(`^## \[(\d+)\.(\d+)\.(\d+)\](?: - \d{4}-\d{2}-\d{2})?\s*$`)

type semver struct{ major, minor, patch int }

func (v semver) String() string {
	return strconv.Itoa(v.major) + "." + strconv.Itoa(v.minor) + "." + strconv.Itoa(v.patch)
}

// less reports whether v sorts before o.
func (v semver) less(o semver) bool {
	if v.major != o.major {
		return v.major < o.major
	}
	if v.minor != o.minor {
		return v.minor < o.minor
	}
	return v.patch < o.patch
}

func TestChangelogVersionsAreDescendingAndUnique(t *testing.T) {
	lines := readChangelog(t)

	var versions []semver
	seen := map[string]int{}
	for i, ln := range lines {
		m := changelogVersionRE.FindStringSubmatch(ln)
		if m == nil {
			continue
		}
		maj, _ := strconv.Atoi(m[1])
		mnr, _ := strconv.Atoi(m[2])
		pat, _ := strconv.Atoi(m[3])
		v := semver{maj, mnr, pat}
		if prev, dup := seen[v.String()]; dup {
			t.Errorf("CHANGELOG has TWO headings for %s (lines %d and %d)", v, prev+1, i+1)
		}
		seen[v.String()] = i
		versions = append(versions, v)
	}

	// Anti-vacuity: a regex that stopped matching would pass on an empty set.
	if len(versions) < 50 {
		t.Fatalf("found only %d version headings in CHANGELOG.md; the heading regex is not matching and this "+
			"gate would pass on nothing", len(versions))
	}

	for i := 1; i < len(versions); i++ {
		if !versions[i].less(versions[i-1]) {
			t.Errorf("CHANGELOG versions are not strictly descending: %s appears after %s.\n\n"+
				"The usual cause is a release edit that REPLACED the previous heading instead of inserting "+
				"above it, which silently merges that version's entry into the new one.",
				versions[i], versions[i-1])
		}
	}
}

// TestChangelogNoDuplicateSectionsWithinAVersion is the second tell. A merged
// block carries its `### Fixed` / `### Changed` headings twice, which the
// version-sequence check alone would miss if the swallowed version happened to
// be adjacent in numbering.
func TestChangelogNoDuplicateSectionsWithinAVersion(t *testing.T) {
	lines := readChangelog(t)

	current := ""
	currentLine := 0
	sections := map[string]bool{}
	checked := 0

	flush := func() { sections = map[string]bool{} }

	for i, ln := range lines {
		if m := changelogVersionRE.FindStringSubmatch(ln); m != nil {
			current = m[1] + "." + m[2] + "." + m[3]
			currentLine = i + 1
			flush()
			checked++
			continue
		}
		if strings.HasPrefix(ln, "## ") {
			// Unreleased or another top-level block.
			current = ""
			flush()
			continue
		}
		if current == "" || !strings.HasPrefix(ln, "### ") {
			continue
		}
		sec := strings.TrimSpace(strings.TrimPrefix(ln, "### "))
		if sections[sec] {
			t.Errorf("version %s (heading at line %d) contains the section %q TWICE (line %d).\n\n"+
				"That is the signature of a release edit that replaced the previous version's heading "+
				"instead of inserting above it — two versions' entries are now merged under one heading.",
				current, currentLine, sec, i+1)
		}
		sections[sec] = true
	}

	if checked < 50 {
		t.Fatalf("walked only %d version blocks; the scan is not reaching the file", checked)
	}
}

func readChangelog(t *testing.T) []string {
	t.Helper()
	root := repoRootFromDocsync(t)
	b, err := os.ReadFile(filepath.Join(root, "CHANGELOG.md"))
	if err != nil {
		t.Fatalf("read CHANGELOG.md: %v", err)
	}
	return strings.Split(string(b), "\n")
}
