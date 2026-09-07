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
	// cannot read must FAIL, never pass with zero rows. The floor is well
	// under the current count so ordinary additions do not trip it, and
	// well above zero so a broken parser cannot hide.
	if len(entries) < 20 {
		t.Fatalf("parsed only %d finding entries from %s; the index carries far more, so the line "+
			"regex has drifted and this gate would pass on nothing. Fix the parser, not the floor.",
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
	// `withdrawn`, the loop above would assert nothing at all.
	if filedCount < 15 {
		t.Fatalf("only %d of %d entries are marked `filed`; this gate enforces nothing on the rest, so "+
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
