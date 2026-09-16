// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// cdcMethodRoster derives every recognised [ir.CDCMethod] from the type's
// own String() — the walk stops at the first value the type does not
// name — so a method added to the IR joins this matrix without anyone
// editing a list here. Anti-vacuity: the walk must find the ONE method
// the stamp exempts ([ir.CDCNone]), the one whose exemption this release
// removed ([ir.CDCTriggers]), and at least two more, or the matrix below
// is grading nothing.
func cdcMethodRoster(t *testing.T) []ir.CDCMethod {
	t.Helper()
	var roster []ir.CDCMethod
	for m := ir.CDCMethod(0); m.String() != "unknown"; m++ {
		roster = append(roster, m)
	}
	seen := map[ir.CDCMethod]bool{}
	for _, m := range roster {
		seen[m] = true
	}
	if !seen[ir.CDCTriggers] || !seen[ir.CDCNone] || len(roster) < 4 {
		t.Fatalf("CDCMethod roster derived from String() is %v — it must include CDCNone (the one exemption) and "+
			"CDCTriggers (the ex-exemption, audit 2026-09-15 F-2) by name, plus at least two more methods, "+
			"otherwise the stamp matrix is vacuous", roster)
	}
	return roster
}

// positionlessStampExpected is the rule under test, stated once so every
// cell below is graded against the same sentence: only a FULL, with an
// EMPTY position, on a source whose reader resumes from a recorded
// position, is raised to FormatVersionPositionlessFull.
func positionlessStampExpected(kind string, pos ir.Position, cdc ir.CDCMethod, before int) int {
	if canonicalKind(kind) != BackupKindFull || pos != (ir.Position{}) || cdc == ir.CDCNone {
		return before
	}
	return max(before, FormatVersionPositionlessFull)
}

// TestStampPositionlessFull_FamilyMatrix pins the stamp over every
// {kind} × {position shape} × {CDC method} × {starting version} cell.
// The Bug-74 lesson applied to a stamp: the predicate dispatches on three
// families, and one green representative per family proves nothing about
// the others — a token-only position, a legacy empty Kind, or a CDC method
// added later would each be its own silent miss.
func TestStampPositionlessFull_FamilyMatrix(t *testing.T) {
	kinds := []string{BackupKindFull, "" /* v0.16.x legacy: canonicalKind → full */, BackupKindIncremental}
	positions := []struct {
		name string
		pos  ir.Position
	}{
		{"empty", ir.Position{}},
		{"engine-only", ir.Position{Engine: "mysql"}},
		{"token-only", ir.Position{Token: "0/16B3748"}},
		{"recorded", ir.Position{Engine: "mysql", Token: "mysql-bin.000003:1234"}},
	}
	// Every tier at or below the new one, so the stamp's max() semantics
	// are graded from below AND from the tier it lands on.
	startingVersions := []int{
		FormatVersionLegacy, FormatVersionSecurityMetadata, FormatVersionStandaloneSequences,
		FormatVersionInjectiveChunkAAD, FormatVersionRedaction, FormatVersionPositionlessFull,
	}

	stamped, untouched := 0, 0
	for _, kind := range kinds {
		for _, p := range positions {
			for _, cdc := range cdcMethodRoster(t) {
				for _, before := range startingVersions {
					name := fmt.Sprintf("kind=%q/pos=%s/cdc=%s/from=%d", kind, p.name, cdc, before)
					m := fixedSignedManifest()
					m.Kind = kind
					m.EndPosition = p.pos
					m.FormatVersion = before
					StampPositionlessFull(m, cdc)
					want := positionlessStampExpected(kind, p.pos, cdc, before)
					if m.FormatVersion != want {
						t.Errorf("%s: FormatVersion = %d, want %d", name, m.FormatVersion, want)
					}
					if want != before {
						stamped++
					} else {
						untouched++
					}
					// Idempotent: a second stamp is a no-op.
					StampPositionlessFull(m, cdc)
					if m.FormatVersion != want {
						t.Errorf("%s: second stamp moved FormatVersion to %d, want %d", name, m.FormatVersion, want)
					}
				}
			}
		}
	}
	// Both arms must have been exercised, or a predicate stuck on one
	// answer would grade green.
	if stamped == 0 || untouched == 0 {
		t.Fatalf("matrix exercised stamped=%d untouched=%d cells; both must be non-zero", stamped, untouched)
	}
	StampPositionlessFull(nil, ir.CDCBinlog) // must not panic
}

// TestStampPositionlessFull_TheOnlyExemptionIsCDCNone pins the exemption
// set by NAME against the derived roster, so a new CDC method is stamped
// by default (the safe direction: a full it cannot chain from is refused
// by older readers) and only a deliberate edit here exempts it.
//
// [ir.CDCTriggers] was the second exemption until audit 2026-09-15 F-2.
// Its premise — "a trigger full records no position by construction, so
// stamping locks older readers out for no protection" — was falsified by
// roadmap item 163 in the SAME release: the trigger engines record an
// anchor now, and resumeStartFromParent stopped exempting them, so
// v0.154.0's own reader refuses a positionless trigger full while
// v0.154.0 could still PRODUCE one on the snapshot-open fault path and
// hand an older binary a chain to anchor "from now". The trigger cell
// below is what fails if the exemption comes back.
func TestStampPositionlessFull_TheOnlyExemptionIsCDCNone(t *testing.T) {
	exempt := map[ir.CDCMethod]bool{ir.CDCNone: true}
	graded := 0
	for _, cdc := range cdcMethodRoster(t) {
		m := fixedSignedManifest()
		m.Kind = BackupKindFull
		m.EndPosition = ir.Position{}
		m.FormatVersion = FormatVersionLegacy
		StampPositionlessFull(m, cdc)
		if got, want := m.FormatVersion == FormatVersionPositionlessFull, !exempt[cdc]; got != want {
			t.Errorf("cdc=%s: stamped=%v, want %v — the exemption set is {none} and nothing else", cdc, got, want)
		}
		graded++
	}
	if graded < 4 {
		t.Fatalf("graded %d CDC methods; the roster has collapsed", graded)
	}
}

// TestStampPositionlessFull_TriggerFullOnTheFaultPathIsStamped is the
// F-2 cell stated in the artifact's own terms rather than by enum name:
// the manifest a trigger-CDC `backup full` finalizes when its
// snapshot-anchored open refused and the post-sweep capturer answered
// ErrPositionUnavailable (which both trigger capturers always do on that
// door). That artifact is indistinguishable, to a pre-v0.154.0 reader,
// from the v0.16.x legacy full it would extend "from now" — so it must
// carry the stamp. The companion cell is the trigger full that DID
// anchor: not this shape, and untouched.
func TestStampPositionlessFull_TriggerFullOnTheFaultPathIsStamped(t *testing.T) {
	faulted := fixedSignedManifest()
	faulted.Kind = BackupKindFull
	faulted.EndPosition = ir.Position{}
	faulted.FormatVersion = FormatVersionLegacy
	if !IsPositionlessFull(faulted, ir.CDCTriggers) {
		t.Fatal("a trigger-CDC full that finalized with an empty EndPosition is not recognised as positionless; a " +
			"pre-v0.154.0 binary would take its own trigger exemption and anchor the chain from now")
	}
	StampPositionlessFull(faulted, ir.CDCTriggers)
	if faulted.FormatVersion != FormatVersionPositionlessFull {
		t.Errorf("fault-path trigger full stamped %d; want %d so older readers refuse it at their ceiling",
			faulted.FormatVersion, FormatVersionPositionlessFull)
	}

	anchored := fixedSignedManifest()
	anchored.Kind = BackupKindFull
	anchored.EndPosition = ir.Position{Engine: "postgres-trigger", Token: `{"last_id":42}`}
	anchored.FormatVersion = FormatVersionLegacy
	if IsPositionlessFull(anchored, ir.CDCTriggers) {
		t.Fatal("a trigger full that recorded its change-log anchor was treated as positionless")
	}
	StampPositionlessFull(anchored, ir.CDCTriggers)
	if anchored.FormatVersion != FormatVersionLegacy {
		t.Errorf("an anchored trigger full was stamped to %d; the happy path must keep its feature-minimum version "+
			"so ordinary trigger backups still restore on older binaries", anchored.FormatVersion)
	}
}

// TestComputeBackupID_NoNewFoldAtPositionlessFull pins that version 11
// adds NO identity fold: an 11-stamped manifest computes the same id a
// 10-stamped one does (the redaction fold, gated `>= 10`, still applies —
// with or without a marker). The emptiness the stamp records is already
// folded through end_position_engine/token in the base fields, so a
// stripped-or-edited position moves the id exactly as before.
func TestComputeBackupID_NoNewFoldAtPositionlessFull(t *testing.T) {
	for _, withMarker := range []bool{false, true} {
		at10 := fixedSignedManifest()
		at11 := fixedSignedManifest()
		for _, m := range []*Manifest{at10, at11} {
			m.Kind = BackupKindFull
			m.EndPosition = ir.Position{}
			if withMarker {
				m.Redaction = &RedactionInfo{RuleCount: 1, Fingerprint: "aaaabbbbccccdddd"}
			}
		}
		at10.FormatVersion = FormatVersionRedaction
		at11.FormatVersion = FormatVersionPositionlessFull
		if a, b := ComputeBackupID(at10), ComputeBackupID(at11); a != b {
			t.Errorf("marker=%v: BackupID at v10 = %s, at v11 = %s; version 11 must add no fold", withMarker, a, b)
		}
		// And the empty position IS part of the identity: recording one
		// later must move the id, as it always has.
		positioned := fixedSignedManifest()
		positioned.Kind = BackupKindFull
		positioned.FormatVersion = FormatVersionPositionlessFull
		positioned.EndPosition = ir.Position{Engine: "mysql", Token: "mysql-bin.000003:1234"}
		if withMarker {
			positioned.Redaction = at11.Redaction
		}
		if ComputeBackupID(positioned) == ComputeBackupID(at11) {
			t.Errorf("marker=%v: recording a position did not move the BackupID", withMarker)
		}
	}
}
