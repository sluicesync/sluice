// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCheckSlotNotAckedPast pins the warm-resume door's arithmetic and its
// acknowledgement: at or behind the persisted position passes, past it
// refuses TERMINALLY with the marker, both LSNs and the slot named, and
// only an acknowledgement naming that exact confirmed_flush_lsn passes it.
func TestCheckSlotNotAckedPast(t *testing.T) {
	ctx := context.Background()
	resume := pglogrepl.LSN(0x2000)

	r := &CDCReader{slotName: "sluice_s"}
	for _, ok := range []string{"", "0/1000", "0/2000"} {
		if err := r.checkSlotNotAckedPast(ctx, ok, resume); err != nil {
			t.Errorf("confirmed_flush %q, resume %s: refused (%v); want pass", ok, resume, err)
		}
	}

	err := r.checkSlotNotAckedPast(ctx, "0/2001", resume)
	if err == nil {
		t.Fatal("confirmed_flush 0/2001 past resume 0/2000 passed; want SLOT-ACKED-PAST-TARGET-POSITION")
	}
	for _, want := range []string{SlotAckedPastTargetPositionMarker, `"sluice_s"`, "0/2001", "0/2000", "--restart-from-scratch", "--accept-slot-acked-past-position=0/2001"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not name %q: %v", want, err)
		}
	}
	if !ir.IsTerminal(err) {
		t.Error("refusal is not terminal: a retry would resume the same gap")
	}

	r.AcceptSlotAckedPastPosition("0/2002")
	if err := r.checkSlotNotAckedPast(ctx, "0/2001", resume); err == nil {
		t.Error("an acknowledgement of a DIFFERENT confirmed_flush_lsn passed the door")
	}
	r.AcceptSlotAckedPastPosition("0/2001")
	if err := r.checkSlotNotAckedPast(ctx, "0/2001", resume); err != nil {
		t.Errorf("the exact acknowledgement was refused: %v", err)
	}
}
