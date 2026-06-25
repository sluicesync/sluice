// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// metricNames is the per-engine PlanetScale metric-name table. The CPU/mem
// percentage metrics are engine-shared; storage, lag, and connection metric
// names differ between the Vitess/MySQL surface and the Postgres surface
// (ADR-0107 Phase 3). An empty field means "this engine exposes no metric we
// can read for this signal" — the corresponding snapshot *Known flag then
// stays false (the honest "unobserved" degrade), never a wrong reading.
type metricNames struct {
	cpuUtilPct       string
	memUtilPct       string
	volAvailableByte string
	volCapacityByte  string
	replicaLagSec    string
	activeConns      string
	maxConns         string

	// primaryContainer, when non-empty, is the `planetscale_container` label
	// value that identifies the write-target's primary series among MULTIPLE
	// container series of a per-pod metric (cpu/mem). Postgres emits cpu/mem
	// once per container (`postgres`, `pgbouncer`, `walg-daemon`) under
	// `planetscale_component="hzinstance"` with NO `tablet_type` — so the
	// Vitess vttablet/primary cascade can't pick one and (with >1 series) the
	// single-series fallback refuses, silently dropping CPU/mem on PG targets
	// (live-confirmed 2026-06-23). This selects the `postgres` container.
	// Empty (MySQL/Vitess) ⇒ the vttablet/primary cascade as before.
	primaryContainer string
}

// mysqlMetricNames is the MySQL/Vitess table, CONFIRMED against the live
// sluicesync endpoint 2026-06-21 (see ADR-0107 impl-plan §2a).
var mysqlMetricNames = metricNames{
	cpuUtilPct:       "planetscale_pods_cpu_util_percentages",
	memUtilPct:       "planetscale_pods_mem_util_percentages",
	volAvailableByte: "planetscale_vttablet_volume_available_bytes",
	volCapacityByte:  "planetscale_vttablet_volume_capacity_bytes",
	replicaLagSec:    "planetscale_mysql_replica_lag_seconds",
	activeConns:      "planetscale_edge_active_connections",
	maxConns:         "planetscale_mysql_max_connections",
}

// postgresMetricNames is the Postgres table (ADR-0107 Phase 3). CPU/mem are
// the engine-shared pod metrics; storage drops the vttablet-specific prefix
// (`planetscale_volume_*`); lag is the postgres-specific series. Connection
// metrics are left UNSET on purpose: the documented PG connection surface
// (`planetscale_postgres_connection_state`) is a per-state breakdown that
// does not fit the single-value-per-pod selection [selectPrimaryValue] uses,
// and — unlike the MySQL names — it has not been verified against the live
// endpoint, so reading it could silently mis-sum. ConnKnown therefore stays
// false for PG targets until the shape is confirmed live (a tracked
// follow-up). Connections are a SECONDARY signal (operator priority is
// CPU/mem/storage), so this is a clean, honest gap rather than a blocker.
var postgresMetricNames = metricNames{
	cpuUtilPct:       "planetscale_pods_cpu_util_percentages",
	memUtilPct:       "planetscale_pods_mem_util_percentages",
	volAvailableByte: "planetscale_volume_available_bytes",
	volCapacityByte:  "planetscale_volume_capacity_bytes",
	// replicaLagSec intentionally UNSET: the live PG endpoint exposes no
	// `planetscale_postgres_replica_lag_seconds` (probed 2026-06-23 — it has
	// `planetscale_postgres_wal_archiver_lag_bytes` / `wal_size_bytes`, a
	// different signal). A single-node PG has no replica lag anyway; leaving it
	// unset keeps LagKnown=false (honest unobserved) rather than naming a
	// non-existent series.
	// activeConns / maxConns intentionally unset — see doc comment above.
	primaryContainer: "postgres",
}

// metricNamesFor selects the metric-name table for a target engine registry
// name. The Postgres family is "postgres" and "postgres-trigger" (the two
// PG-storage engines sluice registers); everything else — "mysql",
// "planetscale", "vitess", and any empty/unknown name — falls back to the
// MySQL/Vitess table (the confirmed, default surface), so this Phase-3 split
// is byte-for-byte the old behaviour for every non-PG target.
func metricNamesFor(engine string) metricNames {
	switch engine {
	case "postgres", "postgres-trigger":
		return postgresMetricNames
	default:
		return mysqlMetricNames
	}
}

// Label keys + the write-target selection values. The write-target health
// is the PRIMARY vttablet (the pod that takes apply writes); we select on
// these labels and fall back gracefully when a label is absent.
const (
	labelComponent  = "planetscale_component"
	labelTabletType = "planetscale_tablet_type"
	labelContainer  = "planetscale_container"

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
// exposition's own (untrusted) timestamp. names is the per-engine metric-name
// table ([metricNamesFor]); an empty name in the table means the engine
// exposes no series for that signal, so the lookup finds nothing and the
// corresponding *Known flag stays false (honest "unobserved").
func distill(samples []promSample, names metricNames, now time.Time) ir.TargetHealthSnapshot {
	snap := ir.TargetHealthSnapshot{SampledAt: now}

	if v, ok := selectPrimaryValue(samples, names.cpuUtilPct, names.primaryContainer); ok {
		snap.CPUUtil = clampFraction(v / 100.0)
		snap.CPUKnown = true
	}
	if v, ok := selectPrimaryValue(samples, names.memUtilPct, names.primaryContainer); ok {
		snap.MemUtil = clampFraction(v / 100.0)
		snap.MemKnown = true
	}

	avail, availOK := selectPrimaryValue(samples, names.volAvailableByte, names.primaryContainer)
	capac, capacOK := selectPrimaryValue(samples, names.volCapacityByte, names.primaryContainer)
	if availOK && capacOK && capac > 0 {
		snap.StorageAvailableBytes = int64(avail)
		snap.StorageCapacityBytes = int64(capac)
		snap.StorageUtil = clampFraction(1.0 - avail/capac)
		snap.StorageKnown = true
	}

	if v, ok := selectPrimaryValue(samples, names.replicaLagSec, names.primaryContainer); ok {
		snap.ReplicaLagSeconds = v
		snap.LagKnown = true
	}

	active, activeOK := selectPrimaryValue(samples, names.activeConns, names.primaryContainer)
	maxc, maxOK := selectPrimaryValue(samples, names.maxConns, names.primaryContainer)
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
// write-target's PRIMARY series. Selection is a graceful cascade:
//
//  0. (Postgres) if primaryContainer is set, a series whose
//     `planetscale_container` matches it — the PG DB container, picked out of
//     the per-container fan (postgres / pgbouncer / walg-daemon) that has no
//     vttablet/tablet_type labels;
//  1. an exact match (component=vttablet AND tablet_type=primary) — the
//     Vitess write target;
//  2. else any series tagged tablet_type=primary (a primary pod whose
//     component label is absent/different);
//  3. else, if exactly one series exists for the metric, use it (single-pod
//     exposition with no distinguishing labels — the common small-DB case,
//     e.g. the single-series PG volume metric);
//  4. else no confident pick → ok=false (the metric stays *Known=false
//     rather than guessing the wrong pod).
//
// Returns ok=false when the metric is absent entirely (including the
// engine-table-unset case where name is "").
func selectPrimaryValue(samples []promSample, name, primaryContainer string) (float64, bool) {
	if name == "" {
		return 0, false
	}
	var (
		matches     []promSample
		primaryOnly []promSample
	)
	for _, s := range samples {
		if s.name != name {
			continue
		}
		matches = append(matches, s)
		// (0) Postgres container match — the write-target DB container among a
		// multi-container fan. Take it immediately.
		if primaryContainer != "" && s.label(labelContainer) == primaryContainer {
			return s.value, true
		}
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
