// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// findingLineRE matches one entry of docs/dev/audit-findings-index.md:
// "- <ID> <disposition> [reason]".
var findingLineRE = regexp.MustCompile(`^-\s+([A-Za-z0-9][A-Za-z0-9-]*)\s+(filed|prose|withdrawn)\b(.*)$`)

// TestAuditFindingsAreFiled closes the process defect four consecutive
// reconcilers reported and none could fix: findings reach a worker
// report and never reach docs/dev/audit-backlog.md, so the next audit
// re-derives them — or re-litigates a decision already made.
//
// The 2026-09-06 pass measured 20 of the 09-01 section's own IDs with
// zero backlog hits, in a section opening "This section files EVERY
// finding".
//
// WHY THE PROPOSED VERSION COULD NOT HAVE WORKED, which is the part
// worth keeping. All three prior proposals were: read the newest
// `workspace/repo-audit-*.md` scorecard, assert every OPEN/PARTIAL ID
// appears in the backlog. But `workspace/` is GITIGNORED and nothing
// under it is tracked — so in CI that test parses zero reports, extracts
// zero IDs and PASSES. A gate whose existence implies coverage it does
// not have is the failure this repo keeps writing rules about, and it
// would have been indistinguishable from a working gate on the machine
// that ran the audit.
//
// So the artifact moved instead: an audit commits its finding IDs to
// docs/dev/audit-findings-index.md, and this grades a tracked file
// against a tracked file.
//
// WHAT IT REACHES: every ID in the index marked `filed` must appear in
// the backlog. It does NOT grade the backlog entry's CONTENT — a
// one-line entry satisfies it — because the alternative is unwritable
// and the measured defect was absence, not thinness.
func TestAuditFindingsAreFiled(t *testing.T) {
	root := repoRootFromDocsync(t)
	indexPath := filepath.Join(root, "docs", "dev", "audit-findings-index.md")
	backlogPath := filepath.Join(root, "docs", "dev", "audit-backlog.md")

	indexRaw, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read %s: %v — this gate's whole design is that it reads a TRACKED artifact; if the "+
			"index moved, re-anchor it rather than deleting the gate", indexPath, err)
	}
	backlogRaw, err := os.ReadFile(backlogPath)
	if err != nil {
		t.Fatalf("read %s: %v", backlogPath, err)
	}
	backlog := string(backlogRaw)

	type entry struct {
		id          string
		disposition string
		reason      string
		line        int
	}
	var entries []entry
	for i, line := range strings.Split(string(indexRaw), "\n") {
		m := findingLineRE.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			continue
		}
		entries = append(entries, entry{id: m[1], disposition: m[2], reason: strings.TrimSpace(m[3]), line: i + 1})
	}

	// ANTI-VACUITY, and it is the specific lesson of
	// TestManifestCommitter_SidecarCheckpointCost: an index this parser
	// cannot read must FAIL, never pass with zero rows. The floors sit
	// within ~20% of the measured truth (169 entries / 149 filed on
	// 2026-09-15), not at the 20/15 the first cut chose: at 20/15 a parser
	// drift dropping 85% of the index still passed (audit 2026-09-15,
	// reconciler §2 item 7 — the same slack A0909-TCI-H-3 corrected in the
	// publication roster). Ordinary additions only raise the counts; a
	// deliberate index prune lowers the floor in the same commit.
	if len(entries) < 135 {
		t.Fatalf("parsed only %d finding entries from %s; the index carries ~169, so the line "+
			"regex has drifted and this gate would pass on a fraction. Fix the parser, not the floor.",
			len(entries), indexPath)
	}

	var missing []string
	filedCount := 0
	for _, e := range entries {
		switch e.disposition {
		case "prose", "withdrawn":
			// Both require a stated reason — an unexplained exemption is
			// indistinguishable from an oversight.
			if e.reason == "" {
				t.Errorf("%s:%d: %s is marked %q with NO reason; say why it is not filed under its ID",
					filepath.Base(indexPath), e.line, e.id, e.disposition)
			}
			continue
		case "filed":
			filedCount++
			if !strings.Contains(backlog, e.id) {
				missing = append(missing, e.id)
			}
		}
	}

	// The second half of the floor: if every entry were `prose` or
	// `withdrawn`, the loop above would assert nothing at all. ~149 are
	// `filed` today; the floor is within ~20% of that.
	if filedCount < 120 {
		t.Fatalf("only %d of %d entries are marked `filed` (~149 are); this gate enforces nothing on the rest, so "+
			"a wholesale re-labelling would silence it", filedCount, len(entries))
	}

	if len(missing) > 0 {
		t.Errorf("%d audit finding ID(s) are listed as filed but appear nowhere in "+
			"docs/dev/audit-backlog.md:\n  %s\n\n"+
			"This is the leak four consecutive reconcilers reported: a finding that reaches a worker "+
			"report and not the backlog is one the next audit re-derives from scratch, or — worse — one "+
			"whose DECISION was recorded only in an untracked workspace file and gets re-litigated. "+
			"File each under its ID, or mark it `prose <reason>` / `withdrawn <reason>` in the index.",
			len(missing), strings.Join(missing, ", "))
	}
}

// codeMarkerRE matches the `audit YYYY-MM-DD <ID>` markers the codebase
// stamps beside a fix.
var codeMarkerRE = regexp.MustCompile(`audit (20\d\d)-(\d\d)-\d\d ([A-Z][A-Za-z0-9-]*)`)

// workerLabelRE matches a worker NUMBER ("W4" in "audit 2026-09-06 W4
// H3"), which is not a finding id. Skipped rather than exempted: it is
// not an unfiled finding, it is not a finding at all.
var workerLabelRE = regexp.MustCompile(`^W\d+$`)

// codeMarkerFloorYearMonth is the era this gate grades: markers dated on
// or after it must be filed. Earlier ones predate the findings index and
// are deliberately out of scope — see the doc below.
const codeMarkerFloorYearMonth = 202609

// codeMarkerExempt names an in-era marker that is deliberately unfiled,
// with the reason. Fail-by-default.
//
// THE THREE ENTRIES BELOW ARE TEMPORARY AND THE REASON SAYS SO. They are
// not "not backlog findings" — they are backlog findings whose entries
// are landing in this same release from the session that owns
// docs/dev/audit-backlog.md, which this change was explicitly scoped out
// of editing (a concurrent-edit conflict, not a judgement that they are
// unfileable). F-2, from the same review, is already in the backlog,
// which is what a filed one looks like. DELETE these three the moment
// the entries land; an exemption that outlives its reason is the rot
// this map exists to make visible.
var codeMarkerExempt = map[string]string{
	"F-1": "v0.154.0 pre-tag value-fidelity review, HIGH: the migrate --resume source_identity door was vacuous " +
		"for every engine whose DSN shape the orchestrator did not parse. FIXED in this commit " +
		"(ir.SourceIdentityDescriber). Backlog entry pending from the audit-backlog-owning session — remove this " +
		"exemption when it lands.",
	"F-3": "v0.154.0 pre-tag value-fidelity review, MEDIUM: libpq key/value extraction was not injective " +
		"(quoted values, first-wins). FIXED in this commit (postgres.parseKVFields). Backlog entry pending from " +
		"the audit-backlog-owning session — remove this exemption when it lands.",
	"F-4": "v0.154.0 pre-tag value-fidelity review, MEDIUM: ChangeLogConsumerID was rune-cut but not made " +
		"storable, so a pre-existing invalid byte still drew PostgreSQL 22021. FIXED in this commit " +
		"(storableIdentity). Backlog entry pending from the audit-backlog-owning session — remove this " +
		"exemption when it lands.",
}

// TestAuditFindingsInCodeMarkersAreFiled closes audit 2026-09-06
// PRE-TAG-5 — the gap in the gate above it.
//
// THE GAP. TestAuditFindingsAreFiled grades ids LISTED IN THE INDEX
// against the backlog, so it cannot see a finding that never reached the
// index at all. On the very release that shipped it, two ids the code's
// own comments cite (H2, H5) were in neither file, and a third (H1) had
// been fixed a release earlier and filed nowhere. A gate whose universe
// is narrower than the thing it is named for.
//
// This one derives its universe from the CODE: every `audit YYYY-MM-DD
// <ID>` marker a fix left behind. That is the independent evidence the
// index cannot supply, because the index is written by the same pass
// that would forget.
//
// WHY IT IS SCOPED TO AN ERA, stated plainly rather than left implied.
// Across the whole tree 76 of 171 marker ids are absent from the
// backlog — almost all of them historical (ARCH-*, D0-*, CRITICAL-*)
// from audits that predate this file. Demanding 76 retroactive filings
// would make the gate unlandable, and a gate nobody can land is a gate
// nobody builds — which is exactly how PRE-TAG-5's ancestor went three
// reconcilers without being written. So it grades the era in which the
// discipline exists (2026-09 onward, where 26 of 27 were already filed
// and the 27th is now), and says so here rather than pretending to
// cover everything.
//
// Lowering the floor is the ratchet: each older era becomes gradeable as
// its findings are filed.
func TestAuditFindingsInCodeMarkersAreFiled(t *testing.T) {
	root := repoRootFromDocsync(t)
	backlogRaw, err := os.ReadFile(filepath.Join(root, "docs", "dev", "audit-backlog.md"))
	if err != nil {
		t.Fatalf("read backlog: %v", err)
	}
	backlog := string(backlogRaw)

	ids := map[string]string{} // id -> first file it was seen in
	scanned := 0
	for _, dir := range []string{"internal", "cmd"} {
		werr := filepath.Walk(filepath.Join(root, dir), func(p string, info os.FileInfo, err error) error {
			if err != nil || info == nil || info.IsDir() || !strings.HasSuffix(p, ".go") {
				return nil //nolint:nilerr // one unreadable subtree must not fail the whole walk
			}
			b, rerr := os.ReadFile(p)
			if rerr != nil {
				return nil //nolint:nilerr // same
			}
			scanned++
			for _, m := range codeMarkerRE.FindAllStringSubmatch(string(b), -1) {
				ym, cerr := strconv.Atoi(m[1] + m[2])
				if cerr != nil || ym < codeMarkerFloorYearMonth {
					continue
				}
				id := m[3]
				if workerLabelRE.MatchString(id) {
					continue
				}
				if _, seen := ids[id]; !seen {
					ids[id] = strings.TrimPrefix(filepath.ToSlash(p), filepath.ToSlash(root)+"/")
				}
			}
			return nil
		})
		if werr != nil {
			t.Fatalf("walk %s: %v", dir, werr)
		}
	}

	// Anti-vacuity, both halves: the walk must be reading real files, and
	// the era must still contain markers. A regex that stops matching
	// FAILS rather than passing over an empty set.
	if scanned < 200 {
		t.Fatalf("walked only %d .go files; the tree has far more, so this gate is grading almost nothing", scanned)
	}
	if len(ids) < 10 {
		t.Fatalf("found only %d in-era audit markers %v; the 2026-09 era carries more, so the marker "+
			"regex has drifted", len(ids), ids)
	}

	// WORD-BOUNDED, not a substring test. The first cut used
	// strings.Contains and its own mutation run PASSED: renaming the
	// backlog entry to "H1-MUTANT" still contains "H1". Short ids make
	// that failure mode routine, and a gate that a rename satisfies is a
	// gate that reports filed when the finding has been edited away.
	filed := func(id string) bool {
		re := regexp.MustCompile(`(^|[^A-Za-z0-9-])` + regexp.QuoteMeta(id) + `($|[^A-Za-z0-9-])`)
		return re.MatchString(backlog)
	}
	var missing []string
	for id, file := range ids {
		if filed(id) {
			continue
		}
		if why, ok := codeMarkerExempt[id]; ok {
			if strings.TrimSpace(why) == "" {
				t.Errorf("%s is exempt with an EMPTY reason", id)
			}
			continue
		}
		missing = append(missing, id+" ("+file+")")
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("%d audit finding id(s) are cited by a code comment but appear nowhere in "+
			"docs/dev/audit-backlog.md:\n  %s\n\n"+
			"The code remembering a finding that the backlog does not is the leak four reconcilers "+
			"reported, and the index-driven gate beside this one cannot see it — an id that never "+
			"reached the index is invisible to a check that reads the index. File it, or add a "+
			"codeMarkerExempt entry saying why it is not a backlog finding.",
			len(missing), strings.Join(missing, "\n  "))
	}
}

// datedSectionRE matches a level-2 dated section heading in either file:
// "## 2026-09-15 — …".
var datedSectionRE = regexp.MustCompile(`^## (20\d\d-\d\d-\d\d)\b`)

// TestAuditFindingsIndexCoversTheNewestBacklogSection closes the one
// direction the two gates above cannot see (audit 2026-09-15, reconciler
// §5). TestAuditFindingsAreFiled grades index → backlog, so an ID never
// ENTERED in the index is invisible to it by construction — its own doc
// says so. TestAuditFindingsInCodeMarkersAreFiled derives its universe from
// in-code markers, and a pass whose fixes carry no marker leaves that
// universe empty for the era. Both floors were kept high by older eras
// while 0 of the 2026-09-15 pass's IDs (VF0915-F1..F3, PP0915, A0915-*)
// were indexed.
//
// THE RULE: the newest dated section in docs/dev/audit-backlog.md must
// have a section of the same date in docs/dev/audit-findings-index.md,
// and that index section must carry at least one parseable entry. A
// backlog section the index does not know about therefore FAILS instead
// of passing.
//
// WHAT IT REACHES, stated so it cannot be read as broader: the NEWEST
// backlog date only. Older unindexed sections are not graded — the
// ratchet is that each new pass has to index itself before the next one
// lands. A dated backlog section that genuinely carries no finding IDs
// (a measurement register) still owes the index a section, with a single
// `- <label> prose <reason>` line saying so; that is one line, and it is
// the acknowledgement that turns "nobody indexed it" into "there was
// nothing to index".
//
// Mutation-run both ways: delete the newest index section → red; delete
// the newest backlog section (so the newest becomes a date the index has
// no section for) → red.
func TestAuditFindingsIndexCoversTheNewestBacklogSection(t *testing.T) {
	root := repoRootFromDocsync(t)
	indexRaw, err := os.ReadFile(filepath.Join(root, "docs", "dev", "audit-findings-index.md"))
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	backlogRaw, err := os.ReadFile(filepath.Join(root, "docs", "dev", "audit-backlog.md"))
	if err != nil {
		t.Fatalf("read backlog: %v", err)
	}

	// Newest backlog date, by VALUE — the file is not strictly ordered
	// (a 2026-09-12 register sits below a 2026-09-13 entry today).
	newest := ""
	backlogDated := 0
	for _, line := range strings.Split(string(backlogRaw), "\n") {
		m := datedSectionRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		backlogDated++
		if m[1] > newest {
			newest = m[1]
		}
	}

	// Index sections, with the count of parseable entries under each.
	indexEntries := map[string]int{}
	current := ""
	indexDated := 0
	for _, line := range strings.Split(string(indexRaw), "\n") {
		if m := datedSectionRE.FindStringSubmatch(line); m != nil {
			current = m[1]
			indexDated++
			if _, seen := indexEntries[current]; !seen {
				indexEntries[current] = 0
			}
			continue
		}
		if current != "" && findingLineRE.MatchString(strings.TrimSpace(line)) {
			indexEntries[current]++
		}
	}

	// Anti-vacuity on both parses: the backlog carries dozens of dated
	// sections and the index several; a regex that stops matching fails
	// here rather than passing on an empty comparison.
	if backlogDated < 20 {
		t.Fatalf("parsed only %d dated sections from the backlog; it carries dozens, so datedSectionRE drifted", backlogDated)
	}
	if indexDated < 3 {
		t.Fatalf("parsed only %d dated sections from the index; it carries several, so datedSectionRE drifted", indexDated)
	}
	if newest == "" {
		t.Fatal("no dated backlog section found")
	}

	n, ok := indexEntries[newest]
	switch {
	case !ok:
		t.Errorf("the newest dated section in docs/dev/audit-backlog.md is %s, and docs/dev/audit-findings-index.md "+
			"has no '## %s' section. Every pass indexes itself: add the section listing each finding ID as "+
			"`filed` / `prose <reason>` / `withdrawn <reason>` — the index-driven gate cannot see an ID that "+
			"never reached the index, which is exactly how 0 of the 2026-09-15 pass's IDs were indexed.",
			newest, newest)
	case n == 0:
		t.Errorf("docs/dev/audit-findings-index.md has a '## %s' section for the newest backlog date but it carries "+
			"no parseable entry ('- <ID> filed|prose|withdrawn'); an empty section is the same leak with a heading on it",
			newest)
	}
}
