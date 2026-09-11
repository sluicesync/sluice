// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

type fakeSlotFailoverProber struct {
	posture ir.SlotFailoverPosture
	err     error
	calls   int
}

func (f *fakeSlotFailoverProber) SourceSlotFailoverPosture(context.Context) (ir.SlotFailoverPosture, error) {
	f.calls++
	return f.posture, f.err
}

// sluice creates slots with FAILOVER true on PG 17+ (ADR-0012). That flag is
// necessary and NOT sufficient — something still has to synchronize the slot,
// and on PG 17+ that is sync_replication_slots + hot_standby_feedback. A slot
// flagged FAILOVER on a cluster that never syncs it is silently primary-local,
// which is the exact failure ADR-0012 exists to prevent, and the operator has
// every reason to think they are covered because sluice set the flag.
//
// The package's captureWarn records at DEBUG, so "silent" here means "emitted
// no WARN" rather than "logged nothing" — the advisory's own skip path logs a
// DEBUG line on a failed probe.
func TestPreflightSlotFailover(t *testing.T) {
	const pg18 = 180006

	logical := ir.Capabilities{CDC: ir.CDCLogicalReplication}
	warned := func(out string) bool { return strings.Contains(out, "level=WARN") }

	t.Run("warns when the cluster will not sync the slot", func(t *testing.T) {
		p := &fakeSlotFailoverProber{posture: ir.SlotFailoverPosture{ServerVersionNum: pg18}}
		out := captureWarn(t, func() { preflightSlotFailover(context.Background(), p, logical) })
		if !warned(out) {
			t.Fatalf("no advisory emitted for a cluster with both GUCs off: %q", out)
		}
		// It must name what to change, and must NOT claim the operator is
		// broken — Patroni permanent slots are invisible from SQL, so a
		// correctly-configured cluster reads exactly like this one.
		// SINGLE-NODE is named because it is the most common reason the
		// advisory does not apply and it is NOT detectable from SQL —
		// pg_stat_replication hides other roles' rows, so an empty result
		// cannot distinguish "no standby" from "a standby this role cannot
		// see". An operator on a single-node instance has to be able to
		// dismiss this in one read.
		for _, want := range []string{"sync_replication_slots", "hot_standby_feedback", "Logical slot name", "SINGLE-NODE"} {
			if !strings.Contains(out, want) {
				t.Errorf("advisory does not mention %q: %q", want, out)
			}
		}
		if !strings.Contains(out, "does not mean you are unprotected") {
			t.Error("the advisory asserts breakage it cannot observe; a cluster preserving slots through Patroni " +
				"permanent slots is correctly configured and reads identically from pg_settings")
		}
	})

	t.Run("silent when native slot sync is properly configured", func(t *testing.T) {
		p := &fakeSlotFailoverProber{posture: ir.SlotFailoverPosture{
			ServerVersionNum: pg18, SyncReplicationSlots: true, HotStandbyFeedback: true,
		}}
		if out := captureWarn(t, func() { preflightSlotFailover(context.Background(), p, logical) }); warned(out) {
			t.Errorf("warned about a correctly-configured cluster: %q", out)
		}
	})

	t.Run("one GUC alone is not enough", func(t *testing.T) {
		// Anti-vacuity for the two cases above: both GUCs are required, so
		// pinning only both-off and both-on would pass with an OR.
		for _, posture := range []ir.SlotFailoverPosture{
			{ServerVersionNum: pg18, SyncReplicationSlots: true},
			{ServerVersionNum: pg18, HotStandbyFeedback: true},
		} {
			p := &fakeSlotFailoverProber{posture: posture}
			out := captureWarn(t, func() { preflightSlotFailover(context.Background(), p, logical) })
			if !warned(out) {
				t.Errorf("no advisory with posture %+v; PG 17 native slot sync needs BOTH", posture)
			}
		}
	})

	t.Run("silent on PG 16 and below, where slot_create.go already warns", func(t *testing.T) {
		p := &fakeSlotFailoverProber{posture: ir.SlotFailoverPosture{ServerVersionNum: 160015}}
		if out := captureWarn(t, func() { preflightSlotFailover(context.Background(), p, logical) }); warned(out) {
			t.Errorf("doubled the PG<=16 warning createLogicalReplicationSlot already emits: %q", out)
		}
	})

	t.Run("silent for non-slot sources, unprobeable handles, and failed probes", func(t *testing.T) {
		// A probe that cannot run says nothing. An unreadable pg_settings
		// turning into a scary data-loss warning would be noise on exactly
		// the setups least able to act on it.
		cases := []struct {
			name   string
			handle any
			caps   ir.Capabilities
		}{
			{"trigger CDC source", &fakeSlotFailoverProber{}, ir.Capabilities{CDC: ir.CDCTriggers}},
			{"no CDC", &fakeSlotFailoverProber{}, ir.Capabilities{}},
			{"handle cannot probe", struct{}{}, logical},
			{"probe failed", &fakeSlotFailoverProber{err: errors.New("permission denied")}, logical},
		}
		for _, tc := range cases {
			out := captureWarn(t, func() { preflightSlotFailover(context.Background(), tc.handle, tc.caps) })
			if warned(out) {
				t.Errorf("%s: emitted an advisory: %q", tc.name, out)
			}
		}
		// The capability gate must short-circuit BEFORE the probe, so a
		// non-slot source pays nothing at all.
		p := &fakeSlotFailoverProber{}
		preflightSlotFailover(context.Background(), p, ir.Capabilities{CDC: ir.CDCTriggers})
		if p.calls != 0 {
			t.Errorf("probed a non-logical-replication source %d times", p.calls)
		}
	})
}
