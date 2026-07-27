// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pin for audit 2026-07-26 SL-6 — a half-observed connection pair must not
// publish a fabricated 0 as observed.
//
// The two counts come from INDEPENDENT metric series families (on
// PlanetScale-Postgres, a single unlabelled edge series for active and a
// per-pod role-labelled one for max), so either can be absent while the other
// resolves. One shared ConnKnown flag then reported the missing half as 0 with
// Known=true — which became `"active_connections":0` in the JSONL sink (whose
// Record doc promises an unobserved metric serializes as an explicit null,
// never a misleading 0), a `sluice_target_*_connections 0` series on /metrics,
// and a non-NULL 0 in target_metrics_history that the read side decodes as
// observed. The "37/0" rendering claims a target with no connection budget at
// all — a claim the telemetry never made.
//
// The storage triple next door already required all three columns non-NULL for
// StorageKnown, "so a fabricated partial reading cannot decode as observed".
// This is that discipline applied to the pair that skipped it.
package telemetry

import "testing"

func TestDistill_ConnectionHalvesAreIndependentlyKnown(t *testing.T) {
	const activeOnly = `
planetscale_edge_active_connections{database="db"} 37
`
	const maxOnly = `
planetscale_mysql_max_connections{planetscale_role="primary",planetscale_container="vttablet"} 1000
`
	const neither = `
planetscale_cpu_seconds_total{planetscale_role="primary"} 5
`

	t.Run("active observed, max absent", func(t *testing.T) {
		snap := distillText(activeOnly)
		if !snap.ActiveConnKnown || snap.ActiveConnections != 37 {
			t.Errorf("ActiveConnections = %d known=%v; want 37 true", snap.ActiveConnections, snap.ActiveConnKnown)
		}
		if snap.MaxConnKnown {
			t.Errorf("MaxConnKnown = true with NO max series in the exposition — a fabricated 0 published as "+
				"observed (audit SL-6); MaxConnections=%d", snap.MaxConnections)
		}
	})

	t.Run("max observed, active absent", func(t *testing.T) {
		snap := distillText(maxOnly)
		if !snap.MaxConnKnown || snap.MaxConnections != 1000 {
			t.Errorf("MaxConnections = %d known=%v; want 1000 true", snap.MaxConnections, snap.MaxConnKnown)
		}
		if snap.ActiveConnKnown {
			t.Errorf("ActiveConnKnown = true with NO active series in the exposition — a fabricated 0 published "+
				"as observed (audit SL-6); ActiveConnections=%d", snap.ActiveConnections)
		}
	})

	t.Run("neither observed", func(t *testing.T) {
		snap := distillText(neither)
		if snap.ActiveConnKnown || snap.MaxConnKnown {
			t.Errorf("a snapshot with no connection series reports known flags: active=%v max=%v",
				snap.ActiveConnKnown, snap.MaxConnKnown)
		}
	})
}

// TestFormatConnPairIsHonest is the operator-facing half of the same defect:
// whatever the flags say, the rendering must not invent a number. It lives
// here rather than in pipeline because it is the same finding; the function is
// exercised through its package's own test in pipeline as well.
func TestDistill_HalfObservedPairNeverRendersAsZeroDenominator(t *testing.T) {
	snap := distillText(`
planetscale_edge_active_connections{database="db"} 37
`)
	// The pipeline renderer formats "?" for an unobserved half. What this pin
	// guards is the precondition: the snapshot must not claim the max half.
	if snap.MaxConnKnown {
		t.Fatal("MaxConnKnown true for an absent series — the renderer would print 37/0, which reads as a " +
			"target whose connection budget is zero")
	}
}
