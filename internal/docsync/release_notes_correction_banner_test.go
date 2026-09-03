// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A release's archived notes are a CLAIM SURFACE that keeps being read
// long after the release. When a LATER release falsifies one of those
// claims, the archive must say so — otherwise an operator reading the
// older release page today gets a reassurance the project already knows
// is wrong. `docs/releases/release-notes-v0.132.1.md` established the
// house form: a `> **Correction (<date>):**` blockquote directly under
// the H1, naming what was wrong and which version fixed it, mirrored as
// a `**Correction (<date>):**` opener in that version's CHANGELOG block.
//
// # Why this exists
//
// v0.133.0's notes and CHANGELOG entry told operators "Existing
// pgtrigger installs are untouched (… the meta-table migration happens
// only at the next `trigger setup` run)". Bug 257 — whose trigger is
// exactly that run — shipped IN v0.133.0 and was fixed hours later in
// v0.133.1. The falsifying release was published, the fix was described
// in its own notes, and the falsified sentence sat uncorrected on the
// v0.133.0 page for four days, next to a v0.132.1 page that DOES carry
// a banner (so its absence here reads as "no known issues"). Found by
// the 2026-08-31 audit (D-3), not by anything mechanical.
//
// # What this gate does NOT catch — read this before trusting it
//
// It does not detect contradiction. Nothing here compares the MEANING
// of two release notes; a later release that quietly falsifies an older
// claim while saying nothing about it is invisible to this test, and no
// tractable test would see it. What it catches is the step AFTER
// noticing: an amendment that is *recorded somewhere* and then not
// banner-ed onto the archive it amends. Two recording shapes are read:
//
//   - DERIVED (fail-by-default, [TestReleaseNotesDefectAttributionsCarryACorrectionBanner]):
//     the house phrases that attribute a defect to a specific earlier
//     version — "new in vX.Y.Z" and "regression in vX.Y.Z" — are
//     extracted from every archived note and CHANGELOG block. Any
//     version so named, by a document that is not its own, must carry a
//     banner. "introduced in vX" is deliberately NOT in the phrase set:
//     it is used neutrally for features throughout the corpus, so
//     including it would fire on a dozen non-defects and the exemption
//     list would become the gate.
//   - DECLARED ([TestDeclaredNotesAmendmentsAreBannered]): the small
//     table below, for amendments recorded in neither phrase — it binds
//     the amended version to the exact sentence that was falsified, so
//     an edit that removes or rewrites the sentence fails rather than
//     silently leaving a banner pointing at nothing.
//
// It also grades the ARCHIVE, never the published GitHub release body —
// the same boundary `scripts/check-notes-claims.sh` states. If the two
// diverge, this says nothing; check them by hand.
//
// Versions before [correctionBannerFloor] are exempt as a CLASS, not
// individually: the banner convention began with v0.132.1's own banner
// on 2026-08-27, and retro-banner-ing archives from releases nobody is
// running has no operator value and real mis-description risk. The
// floor is the honest scope, written here rather than implied by an
// exemption list that would have to grow forever.

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// correctionBannerFloor is the first release expected to carry a
// correction banner when a later release falsifies one of its claims.
// v0.132.1 is the release that established the form.
var correctionBannerFloor = semver{0, 132, 1}

// notesAmendment binds an archived release to a claim a later release
// falsified. claimMarker is a verbatim fragment of the falsified
// sentence AS IT STILL STANDS in the archive: the archive keeps the
// original sentence (rewriting a published record silently is the thing
// the banner exists to avoid), so the marker must still match, and a
// marker that stops matching is a failure, not a skip.
type notesAmendment struct {
	amended     semver
	fixedIn     semver
	claimMarker string
	why         string
}

var declaredNotesAmendments = []notesAmendment{
	{
		amended:     semver{0, 137, 0},
		fixedIn:     semver{0, 137, 1},
		claimMarker: "Every existing pgtrigger install warns until `sluice trigger setup` is re-run once",
		why: "the STALE-CAPTURE-FUNCTION warning fires only when an install's capture function definitions " +
			"differ from what the binary renders; a byte-identical install (v0.136.0) opens silent, so the " +
			"headline over-claimed. The archive was silently rewritten and the CHANGELOG never corrected " +
			"(audit 2026-09-01 DDD-1); the original sentence is restored and both homes carry the banner",
	},
	{
		amended:     semver{0, 137, 2},
		fixedIn:     semver{0, 138, 0},
		claimMarker: "GTID mode, where GTID UUIDs are themselves instance-bound and were always checked",
		why: "the GTID resume arm checked only GTID_SUBSET(@@gtid_purged, resume), which a fresh instance's " +
			"empty gtid_purged satisfies; a foreign GTID-mode position was accepted and backup incremental " +
			"recorded the wrong instance's history as the chain delta at exit 0 (audit 2026-09-01 SLM-2)",
	},
	{
		amended:     semver{0, 133, 0},
		fixedIn:     semver{0, 133, 1},
		claimMarker: "the meta-table migration happens only at the next `trigger setup` run",
		why: "the named run is Bug 257's exact trigger — on a streamed install the migration ALTER was " +
			"recorded by the install's own DDL event trigger and the next warm resume refused; Bug 257 " +
			"shipped in v0.133.0 and was fixed in v0.133.1 (audit 2026-08-31 D-3)",
	},
	{
		amended:     semver{0, 132, 1},
		fixedIn:     semver{0, 132, 2},
		claimMarker: "cut before the first quote, paren, **or `=`**",
		why: "four items described in the v0.132.1 notes landed on a worktree branch and shipped in " +
			"v0.132.2 instead; the `=`-cut sentence is the one this marker anchors",
	},
}

// defectAttributionRE extracts the house phrases that attribute a
// defect to a named earlier version. Kept deliberately narrow — see the
// file comment on why "introduced in" is excluded.
var defectAttributionRE = regexp.MustCompile(`(?:new in|regression in) v(\d+)\.(\d+)\.(\d+)`)

// correctionBannerRE matches the archive's banner form: a blockquote
// opener naming a correction or a known issue.
var correctionBannerRE = regexp.MustCompile(`(?m)^> \*\*(Correction|Known issue)`)

// changelogCorrectionRE matches the CHANGELOG mirror of the banner.
var changelogCorrectionRE = regexp.MustCompile(`\*\*Correction`)

// TestReleaseNotesDefectAttributionsCarryACorrectionBanner is the
// derived half: every version some OTHER release's prose names as the
// origin of a defect must, at or above the convention floor, carry a
// correction banner in its archive.
func TestReleaseNotesDefectAttributionsCarryACorrectionBanner(t *testing.T) {
	root := repoRootFromDocsync(t)

	type attribution struct {
		named semver
		by    string // the document that named it
	}
	var found []attribution

	scan := func(doc, body string, self semver, hasSelf bool) {
		for _, m := range defectAttributionRE.FindAllStringSubmatch(body, -1) {
			maj, _ := strconv.Atoi(m[1])
			mnr, _ := strconv.Atoi(m[2])
			pat, _ := strconv.Atoi(m[3])
			named := semver{maj, mnr, pat}
			if hasSelf && named == self {
				// A release naming itself is not an amendment of an
				// earlier claim; it is the fix describing its own scope.
				continue
			}
			found = append(found, attribution{named: named, by: doc})
		}
	}

	for _, f := range archivedReleaseNotes(t, root) {
		scan("docs/releases/"+filepath.Base(f.path), f.body, f.version, true)
	}
	for _, blk := range changelogVersionBlocks(t, root) {
		scan("CHANGELOG.md §"+blk.version.String(), blk.body, blk.version, true)
	}

	// Two-part anti-vacuity floor. The first proves the extraction still
	// matches the corpus at all; the second proves it reaches the range
	// the gate actually enforces, so a floor bump that emptied the
	// enforced set could not pass silently.
	if len(found) < 3 {
		t.Fatalf("extracted only %d defect attributions from the release corpus; the phrase regex is not "+
			"matching and this gate would pass on nothing", len(found))
	}
	enforced := 0
	for _, a := range found {
		if !a.named.less(correctionBannerFloor) {
			enforced++
		}
	}
	if enforced < 1 {
		t.Fatalf("no defect attribution names a version at or above the convention floor v%s; the enforced "+
			"set is empty and this gate is vacuous", correctionBannerFloor)
	}

	for _, a := range found {
		if a.named.less(correctionBannerFloor) {
			continue // exempt as a class; see the file comment
		}
		path := filepath.Join(root, "docs", "releases", "release-notes-v"+a.named.String()+".md")
		b, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("%s attributes a defect to v%s but its notes archive is unreadable: %v", a.by, a.named, err)
			continue
		}
		if !correctionBannerRE.Match(b) {
			t.Errorf("%s attributes a defect to v%s, but docs/releases/release-notes-v%s.md carries no "+
				"`> **Correction` / `> **Known issue` banner.\n\n"+
				"An operator reading that release's page today gets a claim this project already knows is "+
				"wrong, sitting next to pages that DO carry banners — so its absence reads as \"no known "+
				"issues\". Add the banner naming what was wrong, what is true, and which version fixed it "+
				"(the v0.132.1 archive is the house form), and mirror it as a `**Correction (<date>):**` "+
				"opener in that version's CHANGELOG block.", a.by, a.named, a.named)
		}
	}
}

// TestDeclaredNotesAmendmentsAreBannered is the declared half: for each
// recorded amendment, the falsified sentence is still in the archive,
// the archive carries a banner naming the fixing version, and the
// CHANGELOG block mirrors it.
func TestDeclaredNotesAmendmentsAreBannered(t *testing.T) {
	root := repoRootFromDocsync(t)

	if len(declaredNotesAmendments) < 2 {
		t.Fatalf("only %d declared amendments; the table has been emptied and this gate proves nothing",
			len(declaredNotesAmendments))
	}

	blocks := map[semver]string{}
	for _, blk := range changelogVersionBlocks(t, root) {
		blocks[blk.version] = blk.body
	}

	for _, am := range declaredNotesAmendments {
		path := filepath.Join(root, "docs", "releases", "release-notes-v"+am.amended.String()+".md")
		b, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("v%s (amended by v%s): notes archive unreadable: %v", am.amended, am.fixedIn, err)
			continue
		}
		body := string(b)

		// The marker must be found in the BODY, not in the banner. The
		// banner quotes the falsified sentence (that is the house form),
		// so an unscoped search lets the marker satisfy itself from the
		// correction — the same self-reference defect audit T-1 found in
		// check-notes-claims.sh, and the mutation run that rewrote the
		// original sentence passed until this scoping was added.
		if !strings.Contains(stripBlockquotes(body), am.claimMarker) {
			t.Errorf("v%s: the falsified claim %q is no longer in its archive.\n\n"+
				"Either the sentence was edited (a published record must not be silently rewritten — the "+
				"banner is how it gets corrected) or this entry's marker has rotted. Re-anchor it or remove "+
				"the entry with a reason. Recorded why: %s", am.amended, am.claimMarker, am.why)
		}
		if !correctionBannerRE.MatchString(body) {
			t.Errorf("v%s carries no `> **Correction` banner although v%s amended it (%s)",
				am.amended, am.fixedIn, am.why)
		}
		if !strings.Contains(body, "v"+am.fixedIn.String()) {
			t.Errorf("v%s's correction banner does not name the fixing version v%s — an operator reading it "+
				"cannot tell what to upgrade to", am.amended, am.fixedIn)
		}

		blk, ok := blocks[am.amended]
		if !ok {
			t.Errorf("v%s has no CHANGELOG block", am.amended)
			continue
		}
		if !changelogCorrectionRE.MatchString(blk) {
			t.Errorf("CHANGELOG §%s carries no `**Correction` opener although v%s amended it (%s) — the "+
				"CHANGELOG is the second claim home and drifts from the archive when only one is fixed",
				am.amended, am.fixedIn, am.why)
		}
		if !strings.Contains(blk, "v"+am.fixedIn.String()) {
			t.Errorf("CHANGELOG §%s's correction does not name the fixing version v%s", am.amended, am.fixedIn)
		}
	}
}

// stripBlockquotes drops every markdown blockquote line, which is where
// the correction banners live. See the call site for why that scoping is
// load-bearing.
func stripBlockquotes(body string) string {
	var sb strings.Builder
	for _, ln := range strings.Split(body, "\n") {
		if strings.HasPrefix(ln, ">") {
			continue
		}
		sb.WriteString(ln)
		sb.WriteByte('\n')
	}
	return sb.String()
}

type archivedNotes struct {
	path    string
	version semver
	body    string
}

var notesFileRE = regexp.MustCompile(`^release-notes-v(\d+)\.(\d+)\.(\d+)\.md$`)

func archivedReleaseNotes(t *testing.T, root string) []archivedNotes {
	t.Helper()
	dir := filepath.Join(root, "docs", "releases")
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read docs/releases: %v", err)
	}
	var out []archivedNotes
	for _, e := range ents {
		m := notesFileRE.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		b, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rerr != nil {
			t.Fatalf("read %s: %v", e.Name(), rerr)
		}
		maj, _ := strconv.Atoi(m[1])
		mnr, _ := strconv.Atoi(m[2])
		pat, _ := strconv.Atoi(m[3])
		out = append(out, archivedNotes{
			path:    filepath.Join(dir, e.Name()),
			version: semver{maj, mnr, pat},
			body:    string(b),
		})
	}
	if len(out) < 25 {
		t.Fatalf("found only %d archived release notes; the filename pattern is not matching", len(out))
	}
	return out
}

type changelogBlock struct {
	version semver
	body    string
}

// changelogVersionBlocks splits CHANGELOG.md into per-version bodies,
// reusing changelogVersionRE (the same heading shape the structure gate
// pins, so the two cannot drift).
func changelogVersionBlocks(t *testing.T, root string) []changelogBlock {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "CHANGELOG.md"))
	if err != nil {
		t.Fatalf("read CHANGELOG.md: %v", err)
	}
	var out []changelogBlock
	var cur *changelogBlock
	var sb strings.Builder
	flush := func() {
		if cur != nil {
			cur.body = sb.String()
			out = append(out, *cur)
		}
		sb.Reset()
	}
	for _, ln := range strings.Split(string(b), "\n") {
		if m := changelogVersionRE.FindStringSubmatch(ln); m != nil {
			flush()
			maj, _ := strconv.Atoi(m[1])
			mnr, _ := strconv.Atoi(m[2])
			pat, _ := strconv.Atoi(m[3])
			cur = &changelogBlock{version: semver{maj, mnr, pat}}
			continue
		}
		if cur != nil {
			sb.WriteString(ln)
			sb.WriteByte('\n')
		}
	}
	flush()
	if len(out) < 50 {
		t.Fatalf("split only %d CHANGELOG version blocks; the heading regex is not matching", len(out))
	}
	return out
}
