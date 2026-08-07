// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The roadmap's release claims, held to git.
//
// # Why this exists
//
// `docs/dev/roadmap.md` gives most items a status marker, and a large share of
// those markers say some version of "done, not released yet". That claim is
// true when written and nothing on earth updates it afterwards: the release
// flow writes CHANGELOG.md and docs/releases/, and the roadmap-staleness sweep
// runs when a human asks for it. So the marker decays silently, and the only
// thing that ever notices is somebody re-reading the item.
//
// Measured on 2026-08-07, in one sitting: items 152, 150 and 108 read
// "unreleased" minutes after shipping in v0.116.0; item 75's (b)+(c) shipped in
// v0.102.0 and still said unreleased FOURTEEN releases later; item 82 read
// fully open for thirteen releases after shipping in v0.102.1. Every one was
// found by a human, which is the expensive way to find it.
//
// git already knows the answer. `git tag --contains <commit>` is a total
// function on this question, and it disagrees with the roadmap for free.
//
// # What evidence this gate uses, and why it is not just the cited SHA
//
// The obvious design reads the commit SHA out of the marker and asks whether a
// tag contains it. That design would have caught ONE of the four instances
// above: markers cite a SHA only sometimes (item 152 did; 150, 108 and 75 did
// not). A gate that covers a quarter of its own subject and is named for the
// whole of it is the shape CLAUDE.md warns about, so this one uses two
// independent signals and fires on either:
//
//   - **Cited SHAs inside the marker.** Precise, and it survives a marker being
//     reworded later. Scoped to the marker span, never the item body — bodies
//     cite SHAs for other reasons entirely ("introduced by `abc1234`", the
//     commit that broke it, a cited parent), and every one of those would be a
//     false positive.
//   - **The blame commit of the marker's own line.** A marker asserting
//     "implemented, unreleased" is written at or after the work it describes,
//     so any tag containing the line that makes the claim also contains the
//     work. This is the signal that covers a marker with no SHA at all, which
//     is most of them. It is also sharper than it looks: run against the four
//     stale items above it named v0.102.0 for item 75 and v0.116.0 for item
//     108 — exactly the versions the human wrote in by hand.
//
// The blame signal has one honest failure mode. If a doc commit claims FIXED
// *before* the code lands, and a tag falls between them, the item really was
// unreleased at that tag and this fires anyway. That case is a defect of its
// own kind (the roadmap asserting a fix that does not exist yet), so firing on
// it is not the wrong outcome, but it is worth knowing the gate cannot tell the
// two apart.
//
// # Scope — read this before trusting the name
//
// This gate reads the trailing italic status marker of a `### ` heading and
// nothing else. That is where an item's canonical status lives, and it is where
// all four measured instances were wrong and were fixed.
//
// Deliberately UNGATED, so nobody reads the name as broader than the truth:
//
//   - **Sub-item markers in an item BODY** — e.g. item N-15's
//     `**(b) IMPLEMENTED 2026-07-08 (unreleased):**`. Body bold spans use the
//     same words narratively ("caught it while the item was still unreleased",
//     "that the change-chunk byte ceiling was unreleased"), and no discriminator
//     separated the two cleanly enough to be worth the false positives.
//   - **A marker that mixes a released claim with an unreleased one** and cites
//     no SHA. The heading is one line, so blame cannot attribute half of it; the
//     cited-SHA signal still applies, the blame signal is withheld. See
//     `usesBlameEvidence`.
//   - **Any doc other than the roadmap.** CHANGELOG's `[Unreleased]` section has
//     its own gate in changelog_structure_test.go.
//
// # This needs tags, and it refuses rather than skipping
//
// `actions/checkout@v7` defaults to a depth-1 clone with no tags, which is what
// ci.yml did when this was written. Under that checkout `git tag --contains`
// returns empty for every input and this gate passes everything — a vacuously
// green check, which this project treats as worse than no check, because it
// stops anyone from looking. Both halves of that are fixed: the `test` job now
// checks out with `fetch-depth: 0`, and `requireTagVisibility` fails loudly when
// tags are not visible regardless. A skip-on-missing-git would be the same
// defect wearing a different coat.
//
// # Why CI and not a step in the release flow
//
// The release flow is the thing that has been forgetting. It already carries a
// six-check publish gate, a CHANGELOG step and a notes-archive step, and it
// still let a claim rot for fourteen releases — adding a seventh item to a
// checklist that demonstrably does not get read is the weaker fix, and
// CLAUDE.md's own Tier-1 rule prefers a gate over remembering to look. CI also
// covers the case a release-time step never could: a roadmap edit that
// re-introduces a stale claim weeks after the release.
//
// The cost is real and worth stating. A tag push runs ci.yml, so an item still
// reading "unreleased" at the moment the tag is cut turns the tag's CI red and
// blocks publish check #2. That is deliberate: the fix is to mark items
// `SHIPPED vX.Y.Z` in the same commit that writes the CHANGELOG entry — the
// release flow already stops there to enumerate exactly what shipped — and a
// still-draft tag may be force-moved, which the release process explicitly
// allows. The alternative (fire only on the next push to main) would cap
// staleness at one push instead of fourteen releases, which is most of the
// value for none of the friction; it was rejected because it publishes a
// release with a knowingly false doc and trusts the follow-up to happen.
//
// One consequence of ci.yml's `paths-ignore` is worth knowing: a docs-only
// branch push skips CI, so a roadmap edit alone does not run this. That costs
// nothing here, because the event that makes a marker false is a TAG, and tag
// pushes ignore paths-ignore and always run everything.
func TestRoadmapUnreleasedClaimsAreNotContradictedByATag(t *testing.T) {
	git := requireTagVisibility(t)
	markers := parseRoadmapStatusMarkers(t)
	blame := blameRoadmapLines(t, git)

	type violation struct {
		heading  string
		line     int
		evidence string
		commit   string
		tag      string
		remedy   string
	}

	var (
		violations []violation
		unreleased int
	)

	for _, m := range markers {
		if !m.claimsUnreleased {
			continue
		}
		unreleased++

		// Cited-SHA evidence first: it names the actual commit, which makes for
		// a far more actionable failure than "the line that says so".
		fired := false
		for _, sha := range m.shas {
			full, ok := git.resolveCommit(sha)
			if !ok {
				// A hex-looking token in a marker that git cannot resolve is not
				// a commit (an abbreviation of something rebased away, or a word
				// that happens to be hex). The blame signal still covers the item.
				continue
			}
			if tag, ok := git.firstContainingTag(full); ok {
				violations = append(violations, violation{
					heading:  m.heading,
					line:     m.line,
					evidence: "the SHA the marker cites",
					commit:   sha,
					tag:      tag,
					remedy:   "mark it SHIPPED " + tag,
				})
				fired = true
			}
		}
		if fired || !m.usesBlameEvidence() {
			continue
		}

		commit, ok := blame[m.line]
		if !ok || isUncommitted(commit) {
			// An uncommitted working-tree edit is unreleased by construction.
			continue
		}
		if tag, ok := git.firstContainingTag(commit); ok {
			violations = append(violations, violation{
				heading:  m.heading,
				line:     m.line,
				evidence: "the commit that wrote this marker",
				commit:   commit,
				tag:      tag,
				// Deliberately weaker than the SHA case. Blame names the last
				// commit to touch the LINE, so the tag containing it is an upper
				// bound on the release, not the release: item 106 shipped in
				// v0.104.5 and this signal said v0.104.6, because the marker was
				// reworded a release later. It is exactly right about STALE and
				// only approximately right about WHICH VERSION.
				remedy: "the item shipped no later than " + tag + "; confirm the release in CHANGELOG.md and mark it SHIPPED",
			})
		}
	}

	// Anti-vacuity floor. Zero unreleased items is a legitimate state of the
	// roadmap, so the count of them cannot be floored — but the parser finding
	// nothing at all means the parser broke, and every marker silently stopped
	// being checked. The floors below are on the populations that cannot
	// legitimately empty out; the vocabulary itself is pinned separately by
	// TestRoadmapStatusMarkerClassifierPinsHistoricalSpellings.
	assertMarkerPopulationFloors(t, markers)

	for _, v := range violations {
		t.Errorf("roadmap.md:%d says unreleased, but %s (%s) is contained in tag %s\n  item: %s\n  fix:  %s",
			v.line, v.evidence, shortSHA(v.commit), v.tag, v.heading, v.remedy)
	}
	if len(violations) > 0 {
		t.Logf("%d of %d unreleased-claiming markers are contradicted by a tag", len(violations), unreleased)
	}
}

// The opposite error, and the negative control: a marker claiming a release it
// did not actually reach. The `SHIPPED vX.Y.Z` items are the population the gate
// above must NOT fire on, so it is worth knowing their version numbers are true
// rather than assumed.
//
// # What this checks, and the tighter version it deliberately does not
//
// Two things, both noise-free:
//
//  1. Every version a released marker names is a tag that EXISTS. A typo, or a
//     version that was planned and never cut, fails here.
//  2. Every commit a released marker cites is contained by at least one tag.
//     "SHIPPED vX.Y.Z (`abc1234`)" where `abc1234` is in no release at all is
//     the exact inverse of the staleness this file is about.
//
// The obviously stronger check — that the commit is in the version the marker
// NAMES — was written, run, and rejected on measurement: it produced eight
// findings on an unmodified tree, and every one was an arc heading whose version
// is a narrative label rather than a per-commit claim ("Repo-audit remediation,
// delta v0.99.231–256" naming six versions; "SHIPPED v0.99.175" over a
// follow-up commit that landed in v0.99.176). Those are not the error this is
// hunting, and a gate that cries eight times on a clean tree gets muted, which
// costs more than the check is worth. The near-misses are logged instead, so the
// information is available without being load-bearing.
//
// Scope, stated rather than implied: check (2) reaches only markers that cite a
// resolvable SHA, which is a minority of them. A `SHIPPED vX.Y.Z` marker with no
// SHA gets check (1) and nothing more — its version is confirmed to exist, not
// confirmed to be the right one. Both counts carry anti-vacuity floors.
func TestRoadmapShippedVersionClaimsAreTrue(t *testing.T) {
	git := requireTagVisibility(t)
	markers := parseRoadmapStatusMarkers(t)

	var versionsChecked, pairsChecked, nearMisses int
	for _, m := range markers {
		if m.claimsUnreleased || !m.claimsReleased {
			continue
		}
		for _, v := range m.versions {
			if _, err := git.run("rev-parse", "--verify", "--quiet", "refs/tags/"+v); err != nil {
				t.Errorf("roadmap.md:%d claims %s, but there is no such tag\n  item: %s", m.line, v, m.heading)
				continue
			}
			versionsChecked++
		}
		for _, sha := range m.shas {
			full, ok := git.resolveCommit(sha)
			if !ok {
				continue
			}
			pairsChecked++
			tags := git.containingTags(full)
			if len(tags) == 0 {
				t.Errorf("roadmap.md:%d claims %v, but `%s` is in no tag at all\n  item: %s",
					m.line, m.versions, sha, m.heading)
				continue
			}
			if !anyTagNamed(tags, m.versions) {
				nearMisses++
				t.Logf("roadmap.md:%d names %v; `%s` first appears in %s (informational — arc headings label a range)",
					m.line, m.versions, sha, tags[0])
			}
		}
	}

	if versionsChecked < releasedFloor {
		t.Fatalf("only %d named versions were checked (floor %d) — the marker parser or the released "+
			"classifier stopped matching, and this check would silently reach nothing", versionsChecked, releasedFloor)
	}
	if pairsChecked < shippedPairFloor {
		t.Fatalf("only %d cited commits were checked (floor %d) — the in-marker SHA extraction stopped "+
			"matching, so the stronger half of this check is passing vacuously", pairsChecked, shippedPairFloor)
	}
	t.Logf("reach: %d named versions confirmed to exist, %d cited commits confirmed released, %d near-misses logged",
		versionsChecked, pairsChecked, nearMisses)
}

// The vocabulary this gate keys on, pinned to how the roadmap has ACTUALLY
// spelled it — not to the four spellings anyone remembers.
//
// The fixtures below are verbatim marker text lifted from `git log -p` on
// docs/dev/roadmap.md, i.e. from what the file said BEFORE the fixes, which is
// the point: a fixture built from post-change values makes a gate
// self-referential (the item-104 lesson), so a fixture that only contained
// today's spellings would prove the classifier agrees with itself.
//
// This is the anti-vacuity floor for the unreleased branch specifically. A
// roadmap with zero unreleased items is legitimate, so the main gate cannot
// floor on its own hit count; if the vocabulary drifts and the classifier stops
// recognising markers, THIS is the test that goes red instead of everything
// quietly passing.
//
// That is not a hypothesis — it was measured. Mutating unreleasedVocabRe to
// match nothing leaves TestRoadmapUnreleasedClaimsAreNotContradictedByATag
// PASSING (its subject population is empty, which it cannot distinguish from a
// clean roadmap) and TestRoadmapShippedVersionClaimsAreTrue passing too. This
// test is the only one that goes red. Delete it and the gate above becomes
// unfalsifiable the first time someone invents a new spelling.
func TestRoadmapStatusMarkerClassifierPinsHistoricalSpellings(t *testing.T) {
	unreleasedSpellings := []string{
		// Bare, with every punctuation shape the file has used.
		"FIXED, unreleased",
		"✅ FIXED 2026-07-28, unreleased",
		"✅ FIXED 2026-08-05; unreleased. `-race` Integration must be green before the tag",
		"✅ FIXED (unreleased). The 08-04 HIGH that was never filed.",
		"✅ FIXED on `main` 2026-08-05 (both halves); unreleased",
		"✅ FIXED (fix shape (a): a source-side consumer registry) — unreleased on `main`",
		// With a SHA in the marker (the one shape a SHA-only gate would catch).
		"✅ FIXED 2026-08-07 (unreleased; `f67dd126` + the review follow-up below).",
		// Naming a version it has not shipped in yet — the trap case, because
		// the marker contains a version number but is NOT a shipped claim.
		"✅ FIXED 2026-08-07 (unreleased — ships after v0.114.0). NOT a concurrency chunk",
		"✅ RESOLVED on `main` 2026-08-06 as a CLAIM fix, not a behaviour change (unreleased — ships after v0.114.0).",
		"FIXED, ships in v0.104.4",
		"FIXED, ships in v0.104.6",
		// No "unreleased" token at all.
		"✅ IMPLEMENTED (`SLUICE-E-BACKUP-CHAIN-UNREADABLE`); ships in the next release",
		"✅ IMPLEMENTED (keep the root manifest + the shared readability gate); ships in the next release",
		"✅ root-caused + FIXED, pending release",
		// "SHIPPED" wording that means shipped-to-main, not released. The
		// unreleased vocabulary has to win over the release verb here.
		"✅ SHIPPED to `main` 2026-08-04 (`6ec63868`); CI green. Unreleased — rides the next tag.",
		"✅ SHIPPED 2026-07-27 (unreleased at time of writing)",
		"(b) + (c) IMPLEMENTED 2026-07-24 ([ADR-0180](../adr/adr-0180-metrics-watch-fleet-and-sample-sink.md), unreleased); (a) still BLOCKED on a question to PlanetScale",
	}
	for _, s := range unreleasedSpellings {
		if !markerClaimsUnreleased(s) {
			t.Errorf("classifier no longer recognises an unreleased marker the roadmap has really used:\n  %s", s)
		}
	}

	// The negative control. These must NOT be read as unreleased, or every
	// shipped item in the file starts failing the main gate.
	releasedSpellings := []string{
		"✅ SHIPPED v0.116.0",
		"✅ SHIPPED v0.116.0 ( `f67dd126` + the review follow-up below). NOT a concurrency chunk",
		"(b) + (c) IMPLEMENTED 2026-07-24 ([ADR-0180](../adr/adr-0180-metrics-watch-fleet-and-sample-sink.md), SHIPPED v0.102.0); (a) still BLOCKED on a question to PlanetScale",
		"SHIPPED v0.99.4–v0.99.5",
		"IMPLEMENTED (ADR-0152, 2026-07-08; released v0.99.202).",
		"SHIPPED v0.99.30, [ADR-0080](../adr/adr-0080-mysql-index-build-overlap.md) (`988ca79`)",
		"Phase 2 shipped v0.36.0.",
		// Genuinely open items — no claim of any kind.
		"BLOCKED on a question to PlanetScale",
		"filed 2026-08-05, not started",
		// Narrative use of the word in a released marker: the historical
		// correction note. This is the shape that makes body-bold spans
		// unusable as input, and it must not fire here either.
		"✅ SHIPPED v0.115.0 — the exclusion list was fixed while the item was still unreleased at the time",
	}
	for _, s := range releasedSpellings {
		if markerClaimsUnreleased(s) {
			t.Errorf("classifier now reads a released/open marker as unreleased, which would fail every shipped item:\n  %s", s)
		}
	}

	// The released/unreleased split decides whether the BLAME signal applies
	// (see statusMarker.usesBlameEvidence), so a marker misfiled as "released"
	// is not merely mislabelled — it drops out of the gate entirely. That is
	// how the first cut of this file let four stale items through, and this is
	// the pin for it: naming a version you intend to ship IN is not a claim to
	// have shipped.
	for _, s := range []string{
		"FIXED, ships in v0.104.4",
		"FIXED, ships in v0.104.5 — but see item 107",
		"✅ IMPLEMENTED (`SLUICE-E-SOURCE-REPLICA-IDENTITY`); ships in the next release",
		"✅ FIXED 2026-08-07 (unreleased — ships after v0.114.0). NOT a concurrency chunk",
	} {
		if markerClaimsReleased(s) {
			t.Errorf("a version named as a DESTINATION is being read as a shipped claim, which withholds the "+
				"blame signal and drops the item out of the gate:\n  %s", s)
		}
	}
	for _, s := range []string{
		"✅ SHIPPED v0.116.0",
		"SHIPPED v0.99.30, [ADR-0080](../adr/adr-0080-mysql-index-build-overlap.md) (`988ca79`)",
		"IMPLEMENTED (ADR-0152, 2026-07-08; released v0.99.202).",
	} {
		if !markerClaimsReleased(s) {
			t.Errorf("a genuine shipped claim is no longer recognised, so the negative-control population "+
				"this gate must not fire on is shrinking silently:\n  %s", s)
		}
	}
}

// ---------------------------------------------------------------------------
// Marker parsing
// ---------------------------------------------------------------------------

// Population floors for the parser. The roadmap carried 199 `### ` headings and
// 138 trailing status markers when this landed; the floors sit well below those
// so ordinary editing never trips them, and well above zero so a regex that
// stops matching cannot pass.
const (
	headingFloor     = 120
	markerFloor      = 80
	releasedFloor    = 40
	shippedPairFloor = 15
)

var (
	roadmapHeadRe = regexp.MustCompile(`^###\s+\S`)

	// SHAs are only read from inside backticks. The roadmap writes every commit
	// reference that way, and requiring the backticks keeps ordinary prose from
	// contributing hex-looking words.
	markerSHARe = regexp.MustCompile("`([0-9a-f]{7,40})`")

	markerVersionRe = regexp.MustCompile(`v\d+\.\d+\.\d+`)

	// The unreleased vocabulary, derived by grepping every `+### ` line in the
	// roadmap's whole git history rather than from the spellings anyone
	// remembers. TestRoadmapStatusMarkerClassifierPinsHistoricalSpellings holds
	// it to the real strings.
	unreleasedVocabRe = regexp.MustCompile(`(?i)\bunreleased\b|\bpending release\b|\bnot yet released\b|\bships (?:in|after)\b|\brides the next tag\b|\bin the next release\b`)

	// Narrative uses of the same words, which assert nothing about the item's
	// current state. Only ever consulted when a marker ALSO carries a released
	// version claim, so an ordinary unreleased marker can never be talked out
	// of firing by a stray adverb.
	unreleasedNarrativeRe = regexp.MustCompile(`(?i)\b(?:was|were|still|been)\s+unreleased\b`)

	// A release verb bound to a version number.
	releasedClaimRe = regexp.MustCompile(`(?i)\b(?:shipped|released|closed|fixed|implemented|resolved|landed)\b[^.;]{0,60}?\bv\d+\.\d+\.\d+`)

	// A version named as a DESTINATION rather than as a fact — "ships in
	// v0.104.4", "unreleased — ships after v0.114.0". These are stripped before
	// the released-claim test, and getting that wrong is not hypothetical: with
	// the naive regex, `*FIXED, ships in v0.104.4*` read as a released claim
	// because "FIXED" sat 12 characters from a version number. That flipped the
	// marker to mixed, withheld the blame signal, and left items 103, 104, 106
	// and 107 — all of them stale by a dozen releases — silently ungated by the
	// very gate written to find them.
	futureVersionRe = regexp.MustCompile(`(?i)\bships\s+(?:in|after)\s+(?:v\d+\.\d+\.\d+|the next release)`)
)

type statusMarker struct {
	heading          string
	line             int
	text             string
	shas             []string
	versions         []string
	claimsUnreleased bool
	claimsReleased   bool
}

// usesBlameEvidence reports whether the marker's own line may stand in for the
// item's commit. A marker that ALSO carries a released version claim is a mixed
// statement — "(b) SHIPPED v0.102.0; (c) unreleased" — and the heading is a
// single line, so blame attributes the whole of it to whichever half was edited
// last. Withholding the blame signal there trades coverage for the absence of a
// false-positive class; the cited-SHA signal still applies.
func (m statusMarker) usesBlameEvidence() bool { return !m.claimsReleased }

// markerClaimsReleased reports whether the marker asserts the item is IN a
// released version — as opposed to merely naming the version it is aimed at.
func markerClaimsReleased(text string) bool {
	return releasedClaimRe.MatchString(futureVersionRe.ReplaceAllString(text, " "))
}

func markerClaimsUnreleased(text string) bool {
	if !unreleasedVocabRe.MatchString(text) {
		return false
	}
	if markerClaimsReleased(text) && unreleasedNarrativeRe.MatchString(text) {
		// "SHIPPED v0.115.0 — … while the item was still unreleased" is a note
		// about the past, not a claim about now.
		return false
	}
	return true
}

// firstItalicStatusMarker splits a `### ` heading into its title and its status
// marker: the FIRST single-asterisk italic run introduced by an em-dash.
//
// This is a hand-written scan rather than a regex because both of the obvious
// regexes are wrong on real headings, and each is wrong on a shape the roadmap
// actually contains:
//
//   - Anchoring the marker to the END of the line ("greedy `.*` picks the last
//     em-dash") breaks on the ~dozen headings that carry the item's whole BODY
//     on the heading line. Item 27's line is 3.2 KB long; the last italic run on
//     it is body prose citing three commit SHAs, none of them a status claim.
//     That parse charged item 27 with three violations it had not committed.
//   - Taking the first em-dash breaks the other way, because item TITLES contain
//     em-dashes freely (item 108's does).
//
// So: first em-dash that introduces a single-asterisk italic run, closed by the
// next lone asterisk. Bold runs (`**…**`) appear inside markers and are skipped
// rather than mistaken for the close.
func firstItalicStatusMarker(line string) (heading, marker string, ok bool) {
	for i := 0; i < len(line); i++ {
		if line[i] != '*' {
			continue
		}
		// A bold delimiter, not the marker's opening italic.
		if i+1 < len(line) && line[i+1] == '*' {
			i++
			continue
		}
		// The marker is introduced by an em-dash: `… — *status*`. Anything else
		// is an italicised word inside the title.
		if !strings.HasSuffix(strings.TrimRight(line[:i], " "), "—") {
			continue
		}
		end := closingItalic(line, i+1)
		if end < 0 {
			return "", "", false
		}
		heading = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line[4:i]), "—"))
		return strings.TrimSpace(heading), line[i+1 : end], true
	}
	return "", "", false
}

// closingItalic returns the index of the lone `*` that closes an italic run
// opened at from-1, skipping over any `**…**` bold delimiters inside it.
func closingItalic(line string, from int) int {
	for i := from; i < len(line); i++ {
		if line[i] != '*' {
			continue
		}
		run := 0
		for i+run < len(line) && line[i+run] == '*' {
			run++
		}
		if run == 1 {
			return i
		}
		i += run - 1
	}
	return -1
}

func parseRoadmapStatusMarkers(t *testing.T) []statusMarker {
	t.Helper()

	path := filepath.Join("..", "..", "docs", "dev", "roadmap.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var markers []statusMarker
	for i, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		if !roadmapHeadRe.MatchString(line) {
			continue
		}
		heading, text, ok := firstItalicStatusMarker(line)
		if !ok {
			continue
		}
		marker := statusMarker{
			heading:          heading,
			line:             i + 1,
			text:             text,
			claimsUnreleased: markerClaimsUnreleased(text),
			claimsReleased:   markerClaimsReleased(text),
		}
		for _, sha := range markerSHARe.FindAllStringSubmatch(text, -1) {
			marker.shas = append(marker.shas, sha[1])
		}
		marker.versions = markerVersionRe.FindAllString(text, -1)
		markers = append(markers, marker)
	}
	return markers
}

func assertMarkerPopulationFloors(t *testing.T, markers []statusMarker) {
	t.Helper()

	path := filepath.Join("..", "..", "docs", "dev", "roadmap.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	headings := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if roadmapHeadRe.MatchString(line) {
			headings++
		}
	}
	if headings < headingFloor {
		t.Fatalf("found %d `### ` headings in roadmap.md (floor %d) — the heading scan broke, so every "+
			"status marker in the file just stopped being checked", headings, headingFloor)
	}
	if len(markers) < markerFloor {
		t.Fatalf("parsed %d status markers out of %d headings (floor %d) — the marker regex stopped "+
			"matching and this gate is passing vacuously", len(markers), headings, markerFloor)
	}
	released := 0
	for _, m := range markers {
		if m.claimsReleased && !m.claimsUnreleased {
			released++
		}
	}
	if released < releasedFloor {
		t.Fatalf("only %d markers classify as released (floor %d) — the classifier is no longer reading "+
			"the roadmap's shipped items, which is the population this gate must NOT fire on", released, releasedFloor)
	}

	// State the reach in the log. A gate that passes silently tells the next
	// reader nothing about whether it looked at anything.
	unreleased := 0
	for _, m := range markers {
		if m.claimsUnreleased {
			unreleased++
		}
	}
	t.Logf("reach: %d headings, %d status markers, %d released, %d unreleased-claiming", headings, len(markers), released, unreleased)
}

// ---------------------------------------------------------------------------
// git
// ---------------------------------------------------------------------------

type gitRepo struct {
	ctx      context.Context
	dir      string
	tagCache map[string][]string
	shaCache map[string]string
}

// requireTagVisibility proves that `git tag --contains` can actually answer
// before anything relies on it, and FAILS rather than skipping when it cannot.
//
// This is the whole reason the gate is trustworthy. Under a depth-1 checkout
// with no tags — the default for actions/checkout, and what ci.yml did until
// this landed — `git tag --contains` returns empty for every commit in the
// repository and the gate reports every roadmap item as legitimately
// unreleased. It would be green, permanently, and it would stop anyone from
// looking. Skipping on a missing or shallow git is the identical defect; it
// just reads as a decision instead of a bug.
func requireTagVisibility(t *testing.T) *gitRepo {
	t.Helper()

	const remedy = "this gate compares roadmap claims against `git tag --contains` and cannot run without " +
		"tags; it fails rather than skipping because a tagless run reports every item as unreleased and " +
		"passes. In CI, check out with `fetch-depth: 0` (ci.yml's test job does)"

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	repo := &gitRepo{ctx: t.Context(), dir: dir, tagCache: map[string][]string{}, shaCache: map[string]string{}}

	if _, err := exec.LookPath("git"); err != nil {
		t.Fatalf("git is not on PATH: %v\n%s", err, remedy)
	}
	if _, err := repo.run("rev-parse", "--git-dir"); err != nil {
		t.Fatalf("not inside a git work tree: %v\n%s", err, remedy)
	}
	if out, err := repo.run("rev-parse", "--is-shallow-repository"); err != nil {
		t.Fatalf("could not determine clone depth: %v\n%s", err, remedy)
	} else if out == "true" {
		t.Fatalf("this is a SHALLOW clone, so `git tag --contains` answers empty for every commit\n%s", remedy)
	}
	tags, err := repo.run("tag", "--list", "v*")
	if err != nil {
		t.Fatalf("git tag --list failed: %v\n%s", err, remedy)
	}
	if strings.TrimSpace(tags) == "" {
		t.Fatalf("the clone has no `v*` tags, so `git tag --contains` answers empty for every commit\n%s", remedy)
	}
	// Tags being present is not the same as tags being REACHABLE from HEAD: a
	// `--no-tags` fetch plus an explicit tag fetch can leave dangling tags that
	// contain nothing. `git describe` proves the ancestry is really there.
	if _, err := repo.run("describe", "--tags", "--abbrev=0", "HEAD"); err != nil {
		t.Fatalf("no tag is reachable from HEAD, so the history connecting HEAD to the tags is missing: %v\n%s", err, remedy)
	}
	return repo
}

func (g *gitRepo) run(args ...string) (string, error) {
	cmd := exec.CommandContext(g.ctx, "git", args...)
	cmd.Dir = g.dir
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(ee.Stderr)))
		}
		return "", fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out)), nil
}

func (g *gitRepo) resolveCommit(ref string) (string, bool) {
	if full, ok := g.shaCache[ref]; ok {
		return full, full != ""
	}
	full, err := g.run("rev-parse", "--verify", "--quiet", ref+"^{commit}")
	if err != nil || full == "" {
		g.shaCache[ref] = ""
		return "", false
	}
	g.shaCache[ref] = full
	return full, true
}

// containingTags returns the `v*` tags containing commit, oldest first by
// version order so the caller can name the release the work actually reached.
func (g *gitRepo) containingTags(commit string) []string {
	if tags, ok := g.tagCache[commit]; ok {
		return tags
	}
	out, err := g.run("tag", "--list", "v*", "--contains", commit, "--sort=version:refname")
	var tags []string
	if err == nil {
		for _, line := range strings.Split(out, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				tags = append(tags, line)
			}
		}
	}
	g.tagCache[commit] = tags
	return tags
}

func (g *gitRepo) firstContainingTag(commit string) (string, bool) {
	tags := g.containingTags(commit)
	if len(tags) == 0 {
		return "", false
	}
	return tags[0], true
}

// blameRoadmapLines maps roadmap line number -> the commit that last wrote it,
// in ONE git call. Blame runs against the working tree deliberately: the file
// this test parsed is the working-tree file, and an uncommitted edit blames to
// the all-zero SHA, which is exactly the "not released yet" answer.
func blameRoadmapLines(t *testing.T, g *gitRepo) map[int]string {
	t.Helper()

	out, err := g.run("blame", "--line-porcelain", "--", filepath.ToSlash(filepath.Join("..", "..", "docs", "dev", "roadmap.md")))
	if err != nil {
		t.Fatalf("git blame of the roadmap failed: %v\n"+
			"this gate needs blame to attribute a status marker with no cited SHA, and it fails rather "+
			"than degrading to the SHA-only signal, which would cover about a quarter of the markers", err)
	}

	header := regexp.MustCompile(`^([0-9a-f]{40})\s+\d+\s+(\d+)`)
	blame := make(map[int]string)
	for _, line := range strings.Split(out, "\n") {
		m := header.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		var lineNo int
		if _, err := fmt.Sscanf(m[2], "%d", &lineNo); err == nil {
			blame[lineNo] = m[1]
		}
	}
	if len(blame) == 0 {
		t.Fatal("git blame produced no attributable lines — the porcelain parse broke, and every marker " +
			"without a cited SHA would silently stop being checked")
	}
	return blame
}

func isUncommitted(sha string) bool { return strings.Trim(sha, "0") == "" }

func shortSHA(sha string) string {
	if len(sha) > 8 {
		return sha[:8]
	}
	return sha
}

func anyTagNamed(tags, versions []string) bool {
	want := make(map[string]bool, len(versions))
	for _, v := range versions {
		want[v] = true
	}
	for _, tag := range tags {
		if want[tag] {
			return true
		}
	}
	return false
}
