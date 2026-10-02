// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
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
	r.SetResumeOrigin(ir.CDCResumeOriginTargetControlRow)
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
	// The fleet supervisor keys on the sentinel to stop restarting the leg.
	if !errors.Is(err, ir.ErrSlotAckedPastTargetPosition) || ir.ErrSlotAckedPastTargetPosition.Error() != SlotAckedPastTargetPositionMarker {
		t.Errorf("refusal does not wrap ir.ErrSlotAckedPastTargetPosition (whose text must be the marker): %v", err)
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

// TestCheckSlotNotAckedPast_WordedByResumeOrigin pins GC-41 (k): the refusal
// names where the resume position came from and the remedy that fits it, for
// every origin. A backup chain's refusal names the chain manifest's end
// position, never the target's sluice_cdc_state, prescribes a new chain from
// a full backup, offers no `sync start` flag — and an acknowledgement does
// not pass it, because a chain has none by design (SetResumeOrigin).
func TestCheckSlotNotAckedPast_WordedByResumeOrigin(t *testing.T) {
	ctx := context.Background()
	resume := pglogrepl.LSN(0x2000)
	const ackFlag = "--accept-slot-acked-past-position=0/2001"
	cases := []struct {
		origin       ir.CDCResumeOrigin
		want, reject []string
		acknowledged bool // the exact acknowledgement passes the door
	}{
		{
			origin:       ir.CDCResumeOriginTargetControlRow,
			want:         []string{"sluice_cdc_state", "--restart-from-scratch", ackFlag},
			reject:       []string{"chain"},
			acknowledged: true,
		},
		{
			origin:       ir.CDCResumeOriginChainHandoff,
			want:         []string{"--position-from-manifest", "fresh full backup", "--restart-from-scratch", ackFlag},
			reject:       []string{"sluice_cdc_state"},
			acknowledged: true,
		},
		{
			origin: ir.CDCResumeOriginBackupChain,
			want:   []string{"backup chain's last committed manifest", "no link of the chain holds them", "backup full --chain-slot"},
			reject: []string{"sluice_cdc_state", "--accept-slot-acked-past-position", "--restart-from-scratch", "sync start"},
		},
		{
			origin: ir.CDCResumeOriginUnstated,
			want:   []string{"--restart-from-scratch", "new chain with a full backup"},
			reject: []string{"sluice_cdc_state", "--accept-slot-acked-past-position"},
		},
	}
	for _, tc := range cases {
		r := &CDCReader{slotName: "sluice_s"}
		r.SetResumeOrigin(tc.origin)
		err := r.checkSlotNotAckedPast(ctx, "0/2001", resume)
		if err == nil || !errors.Is(err, ir.ErrSlotAckedPastTargetPosition) || !ir.IsTerminal(err) {
			t.Fatalf("origin %d: want the terminal %s refusal; got %v", tc.origin, SlotAckedPastTargetPositionMarker, err)
		}
		for _, w := range append([]string{`"sluice_s"`, "0/2001", "0/2000"}, tc.want...) {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("origin %d: refusal does not name %q: %v", tc.origin, w, err)
			}
		}
		for _, w := range tc.reject {
			if strings.Contains(err.Error(), w) {
				t.Errorf("origin %d: refusal names %q, which is wrong for this caller: %v", tc.origin, w, err)
			}
		}
		r.AcceptSlotAckedPastPosition("0/2001")
		if got := r.checkSlotNotAckedPast(ctx, "0/2001", resume) == nil; got != tc.acknowledged {
			t.Errorf("origin %d: the exact acknowledgement passed = %v, want %v", tc.origin, got, tc.acknowledged)
		}
	}
}
