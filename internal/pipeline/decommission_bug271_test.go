// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// resolvingSlotMgr is a decomSlotMgr that also implements
// [ir.SlotNameResolver], recovering a slot name from a position token the way
// the real Postgres slot manager does.
type resolvingSlotMgr struct {
	decomSlotMgr
	fromPosition string // "" → cannot recover (the genuinely-legacy case)
}

func (m *resolvingSlotMgr) SlotNameFromPosition(ir.Position) (string, bool) {
	if m.fromPosition == "" {
		return "", false
	}
	return m.fromPosition, true
}

// TestDecommission_Bug271_EmptySlotNameIsRecoveredNotCalledLegacy pins both
// halves of Bug 271.
//
// A stream started WITHOUT `--slot-name` records an EMPTY slot_name — the
// convention is "empty means the engine default", and the fallback was left to
// each consumer. `add-table` implements it (activeSlotName); this command did
// not. It reported the fresh row as "a legacy row from an older sluice",
// dropped nothing, cleared the control row, printed success and exited 0,
// leaving an orphaned slot that blocks the next cold start — the exact failure
// this command exists to prevent.
//
// The SECOND half is the one that makes it more than cosmetic, and it is why
// the fix resolves the name BEFORE the switch rather than inside the empty
// arm: that arm sat FIRST, so an empty slot_name skipped the `slot.Active`
// refusal along with the drop. The command therefore completed against a
// RUNNING stream, deleting its control row underneath it.
//
// The old behaviour had a real rationale — "refuse to guess a name; dropping
// the engine DEFAULT slot on a hunch could take out a different stream" —
// which is sound for a genuinely legacy row and wrong for a default-named one,
// and an empty value alone cannot tell them apart. So the fix does not adopt
// the default: it recovers the name the stream itself recorded. The last cell
// below is the one that keeps that caution honest.
func TestDecommission_Bug271_EmptySlotNameIsRecoveredNotCalledLegacy(t *testing.T) {
	const recorded = "sluice_slot"

	t.Run("an empty slot_name is recovered from the position and DROPPED", func(t *testing.T) {
		var order []string
		applier := &decomApplier{
			streams: []ir.StreamStatus{decomRow("wave-a", "", "")},
			order:   &order,
		}
		mgr := &resolvingSlotMgr{
			decomSlotMgr: decomSlotMgr{slots: []ir.SlotInfo{{Name: recorded}}, order: &order},
			fromPosition: recorded,
		}

		rep, err := DecommissionStream(context.Background(), applier, mgr, "wave-a", false)
		if err != nil {
			t.Fatalf("DecommissionStream: %v", err)
		}
		if !rep.SlotDropped {
			t.Errorf("slot not dropped for an empty slot_name (skip reason %q).\n\n"+
				"This is Bug 271: the default configuration writes an empty slot_name, and the command "+
				"reported the fresh row as legacy, dropped nothing and exited 0 — leaving an orphaned "+
				"slot that blocks the next cold start", rep.SlotSkipped)
		}
		if rep.SlotName != recorded {
			t.Errorf("report names slot %q, want %q — an operator must be able to see WHICH slot this "+
				"command acted on", rep.SlotName, recorded)
		}
		if !strings.Contains(rep.SlotNameSource, "recovered") {
			t.Errorf("SlotNameSource = %q; it must say the name was recovered rather than recorded, "+
				"because that is the difference between evidence and a guess", rep.SlotNameSource)
		}
	})

	t.Run("an empty slot_name still REFUSES an active slot", func(t *testing.T) {
		applier := &decomApplier{streams: []ir.StreamStatus{decomRow("wave-a", "", "")}}
		mgr := &resolvingSlotMgr{
			decomSlotMgr: decomSlotMgr{slots: []ir.SlotInfo{{Name: recorded, Active: true}}},
			fromPosition: recorded,
		}

		rep, err := DecommissionStream(context.Background(), applier, mgr, "wave-a", false)
		if err == nil {
			t.Fatalf("DecommissionStream succeeded against a RUNNING stream (report %+v).\n\n"+
				"This is the half that makes Bug 271 destructive rather than merely useless: the "+
				"empty-name arm sat FIRST in the switch, so it skipped the active-slot refusal along "+
				"with the drop and deleted a live stream's control row underneath it", rep)
		}
		if rep != nil {
			t.Errorf("a refusal must return a nil report (nothing was done); got %+v", rep)
		}
	})

	t.Run("a genuinely unrecoverable name keeps the conservative skip", func(t *testing.T) {
		var order []string
		applier := &decomApplier{
			streams: []ir.StreamStatus{decomRow("wave-a", "", "")},
			order:   &order,
		}
		// fromPosition empty → the resolver cannot recover a name, which is
		// the real legacy row the original caution was written for.
		mgr := &resolvingSlotMgr{
			decomSlotMgr: decomSlotMgr{slots: []ir.SlotInfo{{Name: recorded}}, order: &order},
		}

		rep, err := DecommissionStream(context.Background(), applier, mgr, "wave-a", false)
		if err != nil {
			t.Fatalf("DecommissionStream: %v", err)
		}
		if rep.SlotDropped {
			t.Error("dropped a slot whose name could not be recovered — that is the GUESS the original " +
				"caution refused, and it could take out a different stream's slot")
		}
		if rep.SlotSkipped == "" {
			t.Error("skipped the slot silently; the reason must be reported")
		}
		if strings.Contains(rep.SlotSkipped, "a legacy row from an older sluice") {
			t.Errorf("skip reason still asserts the row is legacy: %q.\n\n"+
				"That sentence was the tell — it was printed for FRESH rows written by the same "+
				"binary moments earlier", rep.SlotSkipped)
		}
	})

	t.Run("an engine with no resolver is unchanged", func(t *testing.T) {
		applier := &decomApplier{streams: []ir.StreamStatus{decomRow("wave-a", "", "")}}
		// decomSlotMgr alone does NOT implement ir.SlotNameResolver, so this
		// is the pre-fix path: it must still skip rather than guess.
		mgr := &decomSlotMgr{slots: []ir.SlotInfo{{Name: recorded}}}

		rep, err := DecommissionStream(context.Background(), applier, mgr, "wave-a", false)
		if err != nil {
			t.Fatalf("DecommissionStream: %v", err)
		}
		if rep.SlotDropped {
			t.Error("dropped a slot on an engine that cannot resolve names — the capability is optional " +
				"and its absence must not change behaviour")
		}
	})
}
