// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// stubHeadroomProber is a fake [replicationHeadroomProber] returning a
// canned census plus an optional error. callCount lets tests assert the
// capability gate short-circuits BEFORE the prober is consulted (the
// postgres-trigger exclusion — its delegated SchemaReader WOULD satisfy
// the interface).
type stubHeadroomProber struct {
	headroom  ir.ReplicationHeadroom
	err       error
	callCount int
}

func (s *stubHeadroomProber) SourceReplicationHeadroom(_ context.Context) (ir.ReplicationHeadroom, error) {
	s.callCount++
	if s.err != nil {
		return ir.ReplicationHeadroom{}, s.err
	}
	return s.headroom, nil
}

// roomyHeadroom is a census with headroom on both ceilings.
func roomyHeadroom() ir.ReplicationHeadroom {
	return ir.ReplicationHeadroom{
		MaxReplicationSlots: 10,
		SlotsInUse:          3,
		MaxWALSenders:       10,
		ActiveWALSenders:    2,
		Slots: []ir.SlotInfo{
			{Name: "sluice_wave_1", Active: true},
			{Name: "sluice_wave_2", Active: false},
			{Name: "other_consumer", Active: true},
		},
	}
}

// TestPreflightReplicationHeadroom_GateExcludesPostgresTrigger: the
// slot-less trigger engine consumes no slot, so the headroom gate must
// never consult its (delegated, interface-satisfying) prober.
func TestPreflightReplicationHeadroom_GateExcludesPostgresTrigger(t *testing.T) {
	prober := &stubHeadroomProber{headroom: ir.ReplicationHeadroom{MaxReplicationSlots: 1, SlotsInUse: 1}}
	if err := preflightReplicationHeadroom(context.Background(), prober, capsTriggerPG); err != nil {
		t.Errorf("postgres-trigger must never refuse on headroom; got %v", err)
	}
	if prober.callCount != 0 {
		t.Errorf("expected the capability gate to short-circuit BEFORE the prober; got %d calls", prober.callCount)
	}
}

// TestPreflightReplicationHeadroom_GateExcludesMySQL: MySQL sources
// never create a slot.
func TestPreflightReplicationHeadroom_GateExcludesMySQL(t *testing.T) {
	prober := &stubHeadroomProber{headroom: ir.ReplicationHeadroom{MaxReplicationSlots: 1, SlotsInUse: 1}}
	if err := preflightReplicationHeadroom(context.Background(), prober, capsMySQL); err != nil {
		t.Errorf("mysql source must not refuse on headroom; got %v", err)
	}
	if prober.callCount != 0 {
		t.Errorf("expected mysql to short-circuit at the gate; got %d prober calls", prober.callCount)
	}
}

// TestPreflightReplicationHeadroom_NonProberHandleSkips: a handle
// without the prober surface skips silently (opportunistic-skip).
func TestPreflightReplicationHeadroom_NonProberHandleSkips(t *testing.T) {
	if err := preflightReplicationHeadroom(context.Background(), stubWriterNoChecker{}, capsSlotPG); err != nil {
		t.Errorf("expected nil when handle lacks replicationHeadroomProber; got %v", err)
	}
}

// TestPreflightReplicationHeadroom_RoomPasses: headroom on both
// ceilings → nil.
func TestPreflightReplicationHeadroom_RoomPasses(t *testing.T) {
	prober := &stubHeadroomProber{headroom: roomyHeadroom()}
	if err := preflightReplicationHeadroom(context.Background(), prober, capsSlotPG); err != nil {
		t.Errorf("expected nil with headroom on both ceilings; got %v", err)
	}
	if prober.callCount != 1 {
		t.Errorf("expected exactly one prober call; got %d", prober.callCount)
	}
}

// TestPreflightReplicationHeadroom_SlotsFullRefuses is the core
// behaviour: max_replication_slots exhausted → the coded refusal naming
// the usage, the ceiling, the existing slots (with their active state),
// and the remedies.
func TestPreflightReplicationHeadroom_SlotsFullRefuses(t *testing.T) {
	h := roomyHeadroom()
	h.SlotsInUse = 10 // == MaxReplicationSlots
	prober := &stubHeadroomProber{headroom: h}
	err := preflightReplicationHeadroom(context.Background(), prober, capsSlotPG)
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCReplicationHeadroom {
		t.Fatalf("expected coded %s; got %v (err=%v)", sluicecode.CodeCDCReplicationHeadroom, ce, err)
	}
	msg := err.Error()
	for _, want := range []string{
		"max_replication_slots",    // the ceiling named
		"all 10",                   // the ceiling value
		"10 slot(s)",               // the usage
		`"sluice_wave_1" (active)`, // slot inventory with active state
		`"sluice_wave_2" (inactive)`,
		"sluice slot list",  // inspect remedy
		"sync decommission", // finished-stream remedy
		"sluice slot drop",  // abandoned-leftover remedy
		"postgresql.conf",   // raise-the-ceiling remedy
		"warm resume",       // the resume carve-out stated
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal message missing %q\nfull: %s", want, msg)
		}
	}
}

// TestPreflightReplicationHeadroom_SendersFullRefuses: the
// max_wal_senders ceiling refuses independently of the slot ceiling.
func TestPreflightReplicationHeadroom_SendersFullRefuses(t *testing.T) {
	h := roomyHeadroom()
	h.ActiveWALSenders = 10 // == MaxWALSenders; slots still roomy
	prober := &stubHeadroomProber{headroom: h}
	err := preflightReplicationHeadroom(context.Background(), prober, capsSlotPG)
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCReplicationHeadroom {
		t.Fatalf("expected coded %s; got %v (err=%v)", sluicecode.CodeCDCReplicationHeadroom, ce, err)
	}
	msg := err.Error()
	for _, want := range []string{"max_wal_senders", "10 active sender(s)", "max_replication_slots is fine at 3/10"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal message missing %q\nfull: %s", want, msg)
		}
	}
}

// TestPreflightReplicationHeadroom_ProbeErrorDegradesToWarn is the KEY
// advisory-degrade pin: a failed census (managed-PG permission
// variance, transient network) must WARN and CONTINUE — never a new
// hard failure on a path that worked before the preflight existed. The
// loud refusal is only for a SUCCESSFUL probe that proves the ceiling.
func TestPreflightReplicationHeadroom_ProbeErrorDegradesToWarn(t *testing.T) {
	prober := &stubHeadroomProber{err: errors.New("permission denied for view pg_stat_replication")}
	if err := preflightReplicationHeadroom(context.Background(), prober, capsSlotPG); err != nil {
		t.Errorf("probe failure must degrade to WARN-and-continue; got %v", err)
	}
	if prober.callCount != 1 {
		t.Errorf("expected the prober to be consulted once; got %d", prober.callCount)
	}
}

// TestPreflightReplicationHeadroom_ZeroCeilingsNeverRefuse: a defensive
// zero census (a prober that returns the zero value) must pass — a
// ceiling of 0 means "not read", not "full".
func TestPreflightReplicationHeadroom_ZeroCeilingsNeverRefuse(t *testing.T) {
	prober := &stubHeadroomProber{}
	if err := preflightReplicationHeadroom(context.Background(), prober, capsSlotPG); err != nil {
		t.Errorf("zero-value census must not refuse; got %v", err)
	}
}

// TestFormatHeadroomRefusal_CapsSlotInventory: a long slot inventory is
// capped with a summarizing count, pointing at `sluice slot list`.
func TestFormatHeadroomRefusal_CapsSlotInventory(t *testing.T) {
	h := ir.ReplicationHeadroom{MaxReplicationSlots: 12, SlotsInUse: 12, MaxWALSenders: 20, ActiveWALSenders: 1}
	for i := 0; i < 12; i++ {
		h.Slots = append(h.Slots, ir.SlotInfo{Name: fmt.Sprintf("slot_%02d", i)})
	}
	msg := formatHeadroomRefusal(h, true, false)
	if !strings.Contains(msg, `"slot_07" (inactive)`) {
		t.Errorf("expected the first %d slots named; got %q", headroomSlotsShown, msg)
	}
	if strings.Contains(msg, `"slot_08"`) {
		t.Errorf("expected slots past the cap to be summarized, not named; got %q", msg)
	}
	if !strings.Contains(msg, "and 4 more") {
		t.Errorf("expected the remainder summarized as a count; got %q", msg)
	}
}
