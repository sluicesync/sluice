// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// PlanetScale metric names (MySQL/Vitess), CONFIRMED against the live
// sluicesync endpoint 2026-06-21 (see ADR-0107 impl-plan §2a). Kept as a
// table so a docs change is a one-line edit, not a scatter of literals.
const (
	metricCPUUtilPct       = "planetscale_pods_cpu_util_percentages"
	metricMemUtilPct       = "planetscale_pods_mem_util_percentages"
	metricVolAvailableByte = "planetscale_vttablet_volume_available_bytes"
	metricVolCapacityByte  = "planetscale_vttablet_volume_capacity_bytes"
	metricReplicaLagSec    = "planetscale_mysql_replica_lag_seconds"
	metricActiveConns      = "planetscale_edge_active_connections"
	metricMaxConns         = "planetscale_mysql_max_connections"
)

// Label keys + the write-target selection values. The write-target health
// is the PRIMARY vttablet (the pod that takes apply writes); we select on
// these labels and fall back gracefully when a label is absent.
const (
	labelComponent  = "planetscale_component"
	labelTabletType = "planetscale_tablet_type"

	componentVTTablet = "vttablet"
	tabletTypePrimary = "primary"
)

// distill collapses a poll's parsed exposition samples into the
// engine-neutral [ir.TargetHealthSnapshot]. It performs the three
// PlanetScale-specific transforms ADR-0107 specifies:
//
//   - PERCENTAGE → FRACTION: CPU/mem arrive as 0–100 percentages; we divide
//     by 100 into the snapshot's [0,1] fraction.
//   - PRIMARY-VTTABLET SELECTION: a metric is emitted per pod; we pick the
//     write-target's series (component=vttablet, tablet_type=primary) and
//     fall back to a best-effort series when the primary label is absent.
//   - MISSING METRIC ⇒ *Known=false: a metric absent from the scrape leaves
//     its value 0 AND its companion flag false, so a consumer NEVER mistakes
//     "unobserved" for "idle" (the loud-failure / honesty tenet).
//
// now stamps SampledAt so freshness is measured from the poll, not the
// exposition's own (untrusted) timestamp.
func distill(samples []promSample, now time.Time) ir.TargetHealthSnapshot {
	snap := ir.TargetHealthSnapshot{SampledAt: now}

	if v, ok := selectPrimaryValue(samples, metricCPUUtilPct); ok {
		snap.CPUUtil = clampFraction(v / 100.0)
		snap.CPUKnown = true
	}
	if v, ok := selectPrimaryValue(samples, metricMemUtilPct); ok {
		snap.MemUtil = clampFraction(v / 100.0)
		snap.MemKnown = true
	}

	avail, availOK := selectPrimaryValue(samples, metricVolAvailableByte)
	capac, capacOK := selectPrimaryValue(samples, metricVolCapacityByte)
	if availOK && capacOK && capac > 0 {
		snap.StorageAvailableBytes = int64(avail)
		snap.StorageCapacityBytes = int64(capac)
		snap.StorageUtil = clampFraction(1.0 - avail/capac)
		snap.StorageKnown = true
	}

	if v, ok := selectPrimaryValue(samples, metricReplicaLagSec); ok {
		snap.ReplicaLagSeconds = v
		snap.LagKnown = true
	}

	active, activeOK := selectPrimaryValue(samples, metricActiveConns)
	maxc, maxOK := selectPrimaryValue(samples, metricMaxConns)
	if activeOK || maxOK {
		// Connections are a secondary signal; report whichever halves we
		// observed (a missing half stays 0). ConnKnown gates the whole pair.
		snap.ActiveConnections = int(active)
		snap.MaxConnections = int(maxc)
		snap.ConnKnown = true
	}

	return snap
}

// selectPrimaryValue finds the value for the named metric on the
// write-target's PRIMARY vttablet pod. Selection is a graceful cascade:
//
//  1. an exact match (component=vttablet AND tablet_type=primary) — the
//     write target;
//  2. else any series tagged tablet_type=primary (a primary pod whose
//     component label is absent/different);
//  3. else, if exactly one series exists for the metric, use it (single-pod
//     exposition with no distinguishing labels — the common small-DB case);
//  4. else no confident pick → ok=false (the metric stays *Known=false
//     rather than guessing the wrong pod).
//
// Returns ok=false when the metric is absent entirely.
func selectPrimaryValue(samples []promSample, name string) (float64, bool) {
	var (
		matches     []promSample
		primaryOnly []promSample
	)
	for _, s := range samples {
		if s.name != name {
			continue
		}
		matches = append(matches, s)
		isPrimary := s.label(labelTabletType) == tabletTypePrimary
		if isPrimary && s.label(labelComponent) == componentVTTablet {
			// (1) exact write-target match — take it immediately.
			return s.value, true
		}
		if isPrimary {
			primaryOnly = append(primaryOnly, s)
		}
	}
	switch {
	case len(matches) == 0:
		return 0, false
	case len(primaryOnly) > 0:
		// (2) primary-tagged but no vttablet component label.
		return primaryOnly[0].value, true
	case len(matches) == 1:
		// (3) single unambiguous series.
		return matches[0].value, true
	default:
		// (4) multiple pods, none identifiable as the primary write target —
		// refuse to guess.
		return 0, false
	}
}

// clampFraction bounds a derived utilisation into [0,1]; a provider quirk
// (a percentage slightly over 100, or available > capacity mid-resize)
// must never produce an out-of-range fraction the consumers treat as a
// nonsense saturation level.
func clampFraction(f float64) float64 {
	if f < 0 {
		return 0
	}
	if f > 1 {
		return 1
	}
	return f
}
