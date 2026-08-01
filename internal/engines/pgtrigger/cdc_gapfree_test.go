// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"strings"
	"testing"
	"time"
)

// TestPollAndAnchorShareTheSettledCeiling is the anti-drift pin. The
// steady-state gap (cdc_gapfree.go's header) shipped because the poll
// and the cold-start anchor were two hand-written SQL strings encoding
// two different correctness arguments, and only the anchor's was sound.
// Both now render the SAME [settledCeilingSQL] expression — over the
// batch window for the poll, over the whole table for the anchor — so
// an edit to the rule reaches both. If a future change inlines either
// one again, this fails.
func TestPollAndAnchorShareTheSettledCeiling(t *testing.T) {
	const tableRef = `"public"."sluice_change_log"`

	anchor := anchorQuery(tableRef)
	if !strings.Contains(anchor, settledCeilingSQL(tableRef)) {
		t.Errorf("anchor query no longer renders the shared settled ceiling:\n%s", anchor)
	}
	poll := pollQuery(tableRef)
	if !strings.Contains(poll, settledCeilingSQL(changeLogWindow)) {
		t.Errorf("poll query no longer renders the shared settled ceiling:\n%s", poll)
	}
}

// TestSettledCeilingSQL_ComparesInXID8Domain pins the epoch-safety
// shape of the shared ceiling: the arm compares the trigger-recorded
// `txid` (xid8-as-bigint, epoch-carrying, NOT NULL since the engine's
// first release) against pg_snapshot_xmin — never the row's system
// `xmin`. Row xmin is a 32-bit epoch-LESS xid, so an xmin-based arm
// stops matching once the cluster's lifetime txid count crosses 2^32,
// COALESCE falls through to MAX(id), and the ceiling silently stops
// holding anything back (live-confirmed on a pg_resetwal-epoch-bumped
// PG 16; the live pin is TestCDCReader_XIDEpochBump).
func TestSettledCeilingSQL_ComparesInXID8Domain(t *testing.T) {
	q := settledCeilingSQL("cl")
	if !strings.Contains(q, "txid >= pg_snapshot_xmin(pg_current_snapshot())::text::bigint") {
		t.Errorf("settled ceiling lost the xid8-domain arm:\n%s", q)
	}
	if strings.Contains(q, "xmin::text") {
		t.Errorf("settled ceiling compares the 32-bit row xmin against a 64-bit xid8 — the epoch-wrap silent-gap bug:\n%s", q)
	}
	if !strings.Contains(q, "MIN(id) - 1") || !strings.Contains(q, "COALESCE(MAX(id), 0)") {
		t.Errorf("settled ceiling lost the (first-unsettled − 1, else MAX) shape:\n%s", q)
	}
}

// TestHoleGuard_NeverSkipsWithoutAProof is the load-bearing negative
// pin: every state EXCEPT "armed, settled, and the hole inside the
// proven range" must refuse to skip. Skipping a hole whose allocating
// transaction is merely in flight is the silent loss the whole file
// exists to prevent, so the default answer is always "wait".
func TestHoleGuard_NeverSkipsWithoutAProof(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name  string
		setup func(g *holeGuard)
	}{
		{"unarmed", func(g *holeGuard) { g.observe(5, now) }},
		{"armed but unproven", func(g *holeGuard) {
			g.observe(5, now)
			g.arm(900, 9)
		}},
		{"proven for an OLDER range only", func(g *holeGuard) {
			g.observe(5, now)
			g.arm(900, 9)
			g.proven = true
			// A later hole above the proof's ceiling: its row may have
			// been allocated by a transaction assigned AFTER the bound,
			// so the settle probe says nothing about it.
			g.observe(12, now)
		}},
		{"cleared after the stream caught up", func(g *holeGuard) {
			g.observe(5, now)
			g.arm(900, 9)
			g.proven = true
			g.clear()
			g.observe(5, now)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var g holeGuard
			tc.setup(&g)
			if to, ok := g.skipTo(g.at, g.at+1); ok {
				t.Errorf("skipTo = (%d, true); want refusal — a hole with no completed abort proof must never be skipped", to)
			}
		})
	}
}

// TestHoleGuard_ProvenSkipCollapsesTheRun pins the positive path: once
// the bound's transactions have all settled and the hole is still
// missing, the guard skips the ENTIRE missing run below the next
// visible id (a rolled-back million-row batch must not cost a million
// polls) — but never past the ceiling the proof covers.
func TestHoleGuard_ProvenSkipCollapsesTheRun(t *testing.T) {
	now := time.Now()
	t.Run("whole run inside the proof", func(t *testing.T) {
		var g holeGuard
		g.observe(5, now)
		g.arm(900, 100)
		g.proven = true
		to, ok := g.skipTo(5, 40)
		if !ok || to != 39 {
			t.Errorf("skipTo(5, 40) = (%d, %v); want (39, true) — the run below the next visible id collapses in one step", to, ok)
		}
	})
	t.Run("run extends past the proof's ceiling", func(t *testing.T) {
		var g holeGuard
		g.observe(5, now)
		g.arm(900, 9)
		g.proven = true
		to, ok := g.skipTo(5, 40)
		if !ok || to != 9 {
			t.Errorf("skipTo(5, 40) = (%d, %v); want (9, true) — ids above the ceiling may belong to transactions assigned after the bound", to, ok)
		}
	})
}

// TestHoleGuard_ArmsOnlyAfterTheHolePersists pins the xid-frugality
// rule: a hole that fills within a poll or two (an ordinary in-flight
// transaction committing) must never cost an assigned xid, and a hole
// already covered by a proof-in-progress must not re-arm.
func TestHoleGuard_ArmsOnlyAfterTheHolePersists(t *testing.T) {
	now := time.Now()
	var g holeGuard
	g.observe(5, now)
	if g.needsBound() {
		t.Error("needsBound after ONE observation; want false (a transient hole must not spend an xid)")
	}
	g.observe(5, now)
	if !g.needsBound() {
		t.Errorf("needsBound after %d observations = false; want true", holeProofPolls)
	}
	g.arm(900, 20)
	g.observe(5, now)
	if g.needsBound() {
		t.Error("needsBound with an outstanding bound covering the hole; want false")
	}
	// A hole above the armed ceiling is outside the proof and needs its
	// own bound once it persists.
	g.observe(30, now)
	g.observe(30, now)
	if !g.needsBound() {
		t.Error("needsBound for a hole above the armed ceiling = false; want true")
	}
}

// TestHoleGuard_ObserveKeepsAnInFlightProof pins that moving to a
// different hole inside the already-covered range does NOT throw the
// proof away — the settle probe is about transactions, not about which
// id the stream happens to be sitting on.
func TestHoleGuard_ObserveKeepsAnInFlightProof(t *testing.T) {
	now := time.Now()
	var g holeGuard
	g.observe(5, now)
	g.arm(900, 50)
	g.proven = true
	g.observe(7, now)
	if to, ok := g.skipTo(7, 8); !ok || to != 7 {
		t.Errorf("skipTo(7, 8) after moving holes inside the proven range = (%d, %v); want (7, true)", to, ok)
	}
}
