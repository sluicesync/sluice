// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"testing"
)

// docs/production-readiness.md opens with "Status as of vX.Y.Z". A page that
// calls itself "maintained against the code" and stamps a version thirty
// releases old undermines every "filed" and "queued" claim below the stamp —
// which is what it did: v0.124.0 stood while v0.154.0 shipped (gap census
// 2026-09-22, GC-20). Nothing gated a page's version stamp.
//
// The independent expected value is docs/releases/: the newest archived
// release-notes file is the newest published release, by the release
// process's own rule that the archive lands in the release commit. The stamp
// must name a release within the newest FIVE archives — close enough that
// the page was re-read recently, loose enough that a docs-only patch release
// does not force a re-stamp. The tolerance is deliberate and small; the
// failure names both versions so the fix is one edit. The page's own sentence
// beside the stamp says it is checked, and this is the check.
func TestProductionReadinessStatusStampIsCurrent(t *testing.T) {
	root := repoRootFromDocsync(t)
	raw, err := os.ReadFile(filepath.Join(root, "docs", "production-readiness.md"))
	if err != nil {
		t.Fatalf("read production-readiness.md: %v", err)
	}
	m := regexp.MustCompile(`Status as of v(\d+)\.(\d+)\.(\d+)`).FindSubmatch(raw)
	if m == nil {
		t.Fatal("docs/production-readiness.md carries no `Status as of vX.Y.Z` stamp; the sentence this gate holds has been reworded")
	}
	maj, _ := strconv.Atoi(string(m[1]))
	mnr, _ := strconv.Atoi(string(m[2]))
	pat, _ := strconv.Atoi(string(m[3]))
	stamp := semver{maj, mnr, pat}

	notes := archivedReleaseNotes(t, root)
	// Anti-vacuity: the archive holds hundreds of releases; a near-empty
	// listing means the directory moved.
	if len(notes) < 50 {
		t.Fatalf("only %d archived release-notes files found under docs/releases (floor 50); the archive moved and this gate is measuring nothing", len(notes))
	}
	sort.Slice(notes, func(i, j int) bool { return notes[j].version.less(notes[i].version) })

	const window = 5
	for _, n := range notes[:window] {
		if n.version == stamp {
			return
		}
	}
	t.Errorf("docs/production-readiness.md says \"Status as of v%s\", but the newest archived release is v%s and the stamp "+
		"is not within the newest %d — re-read the page's claims against the code and bump the stamp", stamp, notes[0].version, window)
}
