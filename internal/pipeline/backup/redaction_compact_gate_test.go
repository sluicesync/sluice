// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"os"
	"strings"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestCompactRefusesARedactedChain grades the compaction door added
// alongside the redaction-chain guards.
//
// WHAT IT IS FOR. The write doors stop a redacted chain being extended
// and the read doors refuse a chain whose links disagree. Compaction sat
// between the two and reached neither, while doing the one thing that
// defeats both: executeMergeGroup byte-copies the OLDEST segment's full
// manifest into the merged segment and folds every later link's changes
// on top, so a redacted marker ends up attributed to data written under
// a different posture. The merged chain then reads as internally
// consistent — every link agrees — and the read door passes it. That is
// a chain restore correctly refuses being converted into one it accepts.
//
// WHAT IT REACHES: [refuseCompactRedactedChain] over the segMeta slice
// CompactChain builds, in both directions. It does NOT boot a store or
// run a real merge; the wiring — that CompactChain calls this before it
// plans any group — is graded by
// TestCompactChainReachesTheRedactionDoor below, which is the half that
// would have caught the original defect.
func TestCompactRefusesARedactedChain(t *testing.T) {
	redacted := func() *irbackup.Manifest {
		return &irbackup.Manifest{Redaction: &irbackup.RedactionInfo{RuleCount: 2, Fingerprint: "83c844a641723823"}}
	}
	plain := func() *irbackup.Manifest { return &irbackup.Manifest{} }

	t.Run("a chain with no marker anywhere compacts", func(t *testing.T) {
		// The overwhelmingly common case. A door that fails this one is a
		// door that has broken compaction for every user who does not
		// redact, which is nearly all of them.
		metas := []segMeta{{fullMani: plain()}, {fullMani: plain()}, {fullMani: nil}}
		if err := refuseCompactRedactedChain(metas); err != nil {
			t.Fatalf("an unredacted chain must compact; got refusal: %v", err)
		}
	})

	t.Run("the laundering shape refuses", func(t *testing.T) {
		// Redacted oldest + plaintext later link: precisely the input whose
		// merged output would claim redacted over plaintext chunks.
		metas := []segMeta{{fullMani: redacted()}, {fullMani: plain()}}
		err := refuseCompactRedactedChain(metas)
		if err == nil {
			t.Fatal("compaction accepted a redacted base with a plaintext link — the merged segment would " +
				"carry the redaction marker over plaintext data, and the read door would then see a chain " +
				"whose links all agree and pass it")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeBackupRedactedChain {
			t.Errorf("refusal does not carry %s: %v", sluicecode.CodeBackupRedactedChain, err)
		}
		// The remedy must not be the posture remedy: the posture is already
		// decided here, and telling the operator to re-take the chain is
		// wrong advice for a chain that is fine and simply should not be
		// compacted.
		// The remedy rides on CodedError.Hint, not in the wrapped prose —
		// the first cut of this assertion greped err.Error() and failed a
		// correct refusal.
		if !strings.Contains(ce.Hint, "does not need compacting") {
			t.Errorf("compaction refusal reuses the posture remedy; it needs its own: %q", ce.Hint)
		}
	})

	t.Run("a marker anywhere refuses, not only at the base", func(t *testing.T) {
		// Grouping is a planning decision made AFTER this door, so a marker
		// on any segment can end up as some group's base. Checking only
		// metas[0] would be a door narrower than its name.
		metas := []segMeta{{fullMani: plain()}, {fullMani: plain()}, {fullMani: redacted()}}
		if err := refuseCompactRedactedChain(metas); err == nil {
			t.Fatal("a redacted marker on a non-first segment was accepted; grouping happens after this " +
				"door, so any segment can become a merge group's manifest donor")
		}
	})

	t.Run("an unfingerprinted marker still refuses", func(t *testing.T) {
		// The marker's PRESENCE is the load-bearing fact. A door that
		// degrades to allow on a marker it cannot fully parse fails open,
		// which for this class means shipping plaintext.
		metas := []segMeta{{fullMani: &irbackup.Manifest{Redaction: &irbackup.RedactionInfo{RuleCount: 1}}}}
		if err := refuseCompactRedactedChain(metas); err == nil {
			t.Fatal("a marker with an empty fingerprint was treated as absent — the guards must not " +
				"degrade to allow on an unrecognised marker")
		}
	})
}

// TestCompactChainReachesTheRedactionDoor is the wiring half, and it is
// the one that grades the actual defect: refuseCompactRedactedChain can
// be perfect and CompactChain can still never call it.
//
// It is a source assertion rather than a behavioural one because reaching
// the real merge path needs a populated store and several segments, and a
// gate that expensive gets skipped. The cheap version pins the two facts
// that matter: the call exists in CompactChain, and it is positioned
// BEFORE grouping — because grouping is what decides which manifest gets
// re-attributed, so a door placed after it would refuse only some of the
// chains it names.
func TestCompactChainReachesTheRedactionDoor(t *testing.T) {
	b, err := os.ReadFile("chain_compact.go")
	if err != nil {
		t.Fatalf("read chain_compact.go: %v", err)
	}
	src := string(b)

	callAt := strings.Index(src, "refuseCompactRedactedChain(metas)")
	if callAt < 0 {
		t.Fatal("CompactChain no longer calls refuseCompactRedactedChain. Compaction re-attributes one " +
			"segment's manifest to several segments' merged data; without this door it launders a " +
			"redaction marker onto plaintext and the read door then passes the result. Re-wire it " +
			"rather than deleting this gate.")
	}

	// Anti-vacuity + ordering in one: the grouping pass must appear AFTER
	// the door. If this anchor stops existing the test fails loudly rather
	// than silently grading nothing.
	// Anchor on the CALL, not the bare identifier: the first cut greped the
	// symbol name and matched a doc comment 400 lines above the call site,
	// so it reported a correctly-placed door as misordered. An anti-vacuity
	// anchor guessed from prose is the defect the anchor exists to catch.
	groupAt := strings.Index(src, "groups = subdivideAtCoverageGaps(")
	if groupAt < 0 {
		t.Fatal("anchor 'groups = subdivideAtCoverageGaps(' not found in chain_compact.go — the grouping " +
			"pass was renamed and this gate can no longer check the door's position. Re-anchor it.")
	}
	if callAt > groupAt {
		t.Error("the redaction door runs AFTER merge grouping; it must run before, because grouping is " +
			"what selects which segment's manifest is re-attributed to the merged data")
	}
}
