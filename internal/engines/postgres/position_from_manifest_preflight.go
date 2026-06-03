// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Phase 3.3.C source-side preflight for `sluice sync start
// --position-from-manifest`. Three checks against the source PG
// instance:
//
//  1. wal_keep_size sufficiency — soft warning when the configured
//     retention looks too small relative to the chain's apparent
//     cadence (caller-supplied; the helper here surfaces the number
//     and lets the orchestrator decide whether it covers the gap).
//  2. Patroni / HA-managed source detection — soft warning about the
//     idle-slot failover trap (see docs/postgres-source-prep.md).
//  3. Slot existence + health — fatal refusal when the slot named in
//     the chain's terminal position (or the operator-supplied default)
//     is missing or has wal_status='lost'/'unreserved'.
//
// The implementation lives on [SchemaReader] (parallel to
// [HealthReporter] / [BackupPositionCapturer]) so the streamer can
// run preflight via a one-shot SchemaReader open without a separate
// engine surface. Lives in its own file so the v0.17.2 diff is
// contained.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// PreflightPositionFromManifest implements
// [ir.PositionFromManifestPreflight]. Returns an
// [ir.PreflightReport] capturing soft warnings (wal_keep_size,
// Patroni detection) and an optional refusal (slot missing /
// invalidated). Engine-side query failures surface via the err return;
// the streamer treats those as preflight infrastructure failures
// distinct from refusals.
//
// The slotName argument is the streamer-resolved slot name (already
// run through the sluice-prefix convention); empty falls back to the
// engine's default. Implementation prefers slotName over chainTerminal
// when both reference slots — the streamer's CDC reader will use
// slotName, so that's the slot whose health matters.
func (r *SchemaReader) PreflightPositionFromManifest(
	ctx context.Context,
	chainTerminal ir.Position,
	slotName string,
) (ir.PreflightReport, error) {
	if r.db == nil {
		return ir.PreflightReport{}, errors.New("postgres: PreflightPositionFromManifest: reader not opened")
	}

	// Resolve which slot to inspect. Operator-supplied slotName wins;
	// otherwise extract the slot from the chain terminal position;
	// otherwise fall back to the engine's default.
	resolvedSlot := slotName
	if resolvedSlot == "" {
		if decoded, ok, err := decodePGPos(chainTerminal); err == nil && ok {
			resolvedSlot = decoded.Slot
		}
	}
	if resolvedSlot == "" {
		resolvedSlot = defaultSlot
	}

	report := ir.PreflightReport{}

	// Check 1: wal_keep_size. PG 13+. We surface the configured value
	// (in MB) so the operator can sanity-check it against the chain
	// cadence; without per-stream cadence telemetry the orchestrator
	// can't know what "enough" is, so the warning is informational
	// rather than threshold-driven. A future enhancement could read
	// pg_stat_wal and the chain's incremental timestamps to compute a
	// concrete safety-margin number; for v0.17.2 the simpler shape
	// keeps blast radius small and matches the design doc's "soft
	// warning" intent.
	if walKeepMB, ok, err := walKeepSizeMB(ctx, r.db); err != nil {
		return ir.PreflightReport{}, fmt.Errorf("read wal_keep_size: %w", err)
	} else if ok && walKeepMB < walKeepSizeWarnThresholdMB {
		report.Warnings = append(report.Warnings, fmt.Sprintf(
			"wal_keep_size = %d MB looks small (< %d MB) for a chain handoff; if the chain's typical incremental cadence exceeds the WAL volume per minute the slot covers, CDC may not have the WAL it needs to bridge to the chain's terminal position. Consider raising wal_keep_size on the source. See docs/postgres-source-prep.md.",
			walKeepMB, walKeepSizeWarnThresholdMB,
		))
	}

	// Check 2: Patroni / HA detection. Three signals checked in turn;
	// any positive signal triggers the warning.
	if isPatroni, why, err := detectPatroniSource(ctx, r.db); err != nil {
		return ir.PreflightReport{}, fmt.Errorf("detect patroni: %w", err)
	} else if isPatroni {
		report.Warnings = append(report.Warnings, fmt.Sprintf(
			"this PG cluster is HA-managed (%s). The slot you're starting CDC from is subject to the idle-slot failover trap — slots not actively consumed don't replicate to standbys and are silently lost on failover. Ensure the slot is being actively consumed; for low-traffic sources, consider a heartbeat-write strategy. See docs/postgres-source-prep.md.",
			why,
		))
	}

	// Check 3: slot existence + health. This is the refusal-grade
	// check — a missing or invalidated slot can't deliver what we
	// need and there's no recovery short of dropping/recreating.
	state, err := slotInfo(ctx, r.db, resolvedSlot)
	if err != nil {
		return ir.PreflightReport{}, fmt.Errorf("query slot %q: %w", resolvedSlot, err)
	}
	switch {
	case state == nil:
		report.Refusal = fmt.Sprintf(
			"replication slot %q does not exist on the source. The chain's terminal position references this slot; CDC has nowhere to start. "+
				"Recovery: re-create the slot at the chain's terminal LSN if WAL is still in retention, OR take a fresh full backup and start a new chain. "+
				"See docs/postgres-source-prep.md for slot lifecycle.",
			resolvedSlot,
		)
	case state.WALStatus == "lost":
		report.Refusal = fmt.Sprintf(
			"replication slot %q has wal_status=%q — required WAL has been permanently removed; the slot can't be resumed from. "+
				"Recovery: drop the slot, take a fresh full backup, and start a new chain. To prevent recurrence, raise max_slot_wal_keep_size on the source.",
			resolvedSlot, state.WALStatus,
		)
	case state.WALStatus == "unreserved":
		report.Refusal = fmt.Sprintf(
			"replication slot %q has wal_status=%q — required WAL is on the brink of being lost; CDC handoff is too risky. "+
				"Recovery: resume the existing CDC consumer immediately to advance the slot, OR drop the slot + take a fresh full + start a new chain.",
			resolvedSlot, state.WALStatus,
		)
	}

	return report, nil
}

// walKeepSizeWarnThresholdMB is the threshold below which the v0.17.2
// preflight emits a soft warning about wal_keep_size sufficiency.
// Operator-tunable at the source level; the constant here is the
// orchestrator's safety floor matching the design doc's "small
// configured value should at least raise eyebrows" tone. 64 MB is the
// default PG ships with, so this constant rejects only setups that
// have explicitly dialed wal_keep_size below the default — which is
// the exact pathology the chain handoff is sensitive to.
const walKeepSizeWarnThresholdMB = 64

// walKeepSizeMB reads wal_keep_size from pg_settings. Returns
// (mb, true, nil) on success; (0, false, nil) when the setting isn't
// present (older PG versions, pre-13). Errors on query failure.
//
// pg_settings exposes wal_keep_size as a unitful integer with a unit
// (MB / GB / TB depending on the setting); we normalise to MB by
// multiplying through the unit when we recognise it.
func walKeepSizeMB(ctx context.Context, db *sql.DB) (mb int64, ok bool, err error) {
	const q = `SELECT setting, COALESCE(unit, '') FROM pg_settings WHERE name = 'wal_keep_size'`
	var setting, unit string
	if err := db.QueryRowContext(ctx, q).Scan(&setting, &unit); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("query wal_keep_size: %w", err)
	}
	var raw int64
	if _, err := fmt.Sscanf(setting, "%d", &raw); err != nil {
		return 0, false, fmt.Errorf("parse wal_keep_size value %q: %w", setting, err)
	}
	// Normalise to MB. PG's pg_settings.unit for wal_keep_size is
	// typically "MB" but defensively handle the wider set.
	switch strings.ToLower(unit) {
	case "", "mb":
		return raw, true, nil
	case "kb":
		return raw / 1024, true, nil
	case "gb":
		return raw * 1024, true, nil
	case "tb":
		return raw * 1024 * 1024, true, nil
	default:
		// Unknown unit; report the raw number with the unit absent so
		// the caller's threshold logic doesn't trip on a bogus number.
		// Tracked via the boolean: false → "skip the wal_keep check".
		return 0, false, nil
	}
}

// detectPatroniSource probes the PG source for signals that an HA
// manager is in front of it. Six signals are checked in turn; any
// positive signal triggers the warning. The returned why string names
// the specific signal so the warning message gives operators a
// starting point for verification.
//
// Signals (most-specific first; first-match wins):
//
//   - pg_settings rows with name LIKE '%patroni%' (Patroni-set GUCs)
//   - pg_stat_replication.application_name LIKE 'patroni%'
//   - role names 'patroni' or 'replicator' present in pg_roles
//   - non-temporary physical replication slots present (HA-cluster
//     signal — most non-HA PG deployments don't carry standby physical
//     slots) — Bug 36 v0.17.3
//   - cluster_name GUC populated (Patroni convention; many managed
//     services also set it) — Bug 36 v0.17.3
//
// The hostname-pattern signal lives at the streamer layer (it needs
// the DSN, which isn't in scope here). See pipeline/sync_preflight.go.
//
// Signals 1-3 (the original v0.17.2 set) miss systematically on
// tenant-isolated managed PG (e.g. PlanetScale Postgres): Patroni
// sets standard GUCs not Patroni-prefixed ones, pg_stat_replication
// is permission-restricted to the superuser, and roles are
// tenant-prefixed. Signals 4 + 5 are added in v0.17.3 (Bug 36) to
// catch those cases. Permission-denied errors on any individual
// signal degrade gracefully via [isPermissionDenied] — managed
// services may restrict pg_replication_slots too.
func detectPatroniSource(ctx context.Context, db *sql.DB) (detected bool, why string, err error) {
	// Signal 1: Patroni-set GUCs. The cleanest tell because Patroni
	// itself sets these.
	var gucCount int
	if err := db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM pg_settings WHERE name ILIKE '%patroni%'`,
	).Scan(&gucCount); err != nil {
		return false, "", fmt.Errorf("query pg_settings for Patroni GUCs: %w", err)
	}
	if gucCount > 0 {
		return true, "Patroni-set GUC detected in pg_settings", nil
	}

	// Signal 2: pg_stat_replication.application_name. Catches Patroni's
	// standby connections.
	var appCount int
	if err := db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM pg_stat_replication WHERE application_name ILIKE 'patroni%'`,
	).Scan(&appCount); err != nil {
		// pg_stat_replication is privileged in some PG versions;
		// degrade gracefully on permission errors rather than failing
		// the whole preflight.
		if isPermissionDenied(err) {
			// Skip this signal; continue to the role-name signal.
		} else {
			return false, "", fmt.Errorf("query pg_stat_replication: %w", err)
		}
	}
	if appCount > 0 {
		return true, "Patroni standby in pg_stat_replication.application_name", nil
	}

	// Signal 3: role names. The loosest signal but the cheapest;
	// Patroni's default install creates a 'patroni' superuser and a
	// 'replicator' replication role.
	var roleCount int
	if err := db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM pg_roles WHERE rolname IN ('patroni', 'replicator')`,
	).Scan(&roleCount); err != nil {
		return false, "", fmt.Errorf("query pg_roles: %w", err)
	}
	if roleCount > 0 {
		return true, "Patroni-convention role names ('patroni' / 'replicator') present in pg_roles", nil
	}

	// Signal 4 (v0.17.3 Bug 36): non-temporary physical replication
	// slots present. Standby physical slots are a strong HA-cluster
	// signal — most non-HA PG deployments don't carry them. Catches
	// managed-PG services where signals 1-3 miss because Patroni sets
	// standard GUCs and pg_stat_replication is permission-restricted.
	var physSlotCount int
	if err := db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'physical' AND temporary = false`,
	).Scan(&physSlotCount); err != nil {
		// pg_replication_slots can be restricted on some managed
		// services (requires pg_read_all_stats or similar). Skip the
		// signal on permission denied; surface other failures.
		if !isPermissionDenied(err) {
			return false, "", fmt.Errorf("query pg_replication_slots: %w", err)
		}
	}
	if physSlotCount > 0 {
		return true, "non-temporary physical replication slots present (HA-cluster signal)", nil
	}

	// Signal 5 (v0.17.3 Bug 36): cluster_name GUC populated. Patroni
	// convention sets this, and many managed services follow suit.
	// Empty string = no signal.
	var clusterName string
	if err := db.QueryRowContext(
		ctx,
		`SELECT setting FROM pg_settings WHERE name = 'cluster_name'`,
	).Scan(&clusterName); err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// cluster_name GUC absent on very old PG; skip the signal.
			clusterName = ""
		case isPermissionDenied(err):
			// Skip the signal.
			clusterName = ""
		default:
			return false, "", fmt.Errorf("query cluster_name GUC: %w", err)
		}
	}
	if clusterName != "" {
		return true, fmt.Sprintf("cluster_name GUC is populated (%q) — HA-managed convention", clusterName), nil
	}

	return false, "", nil
}

// Hostname-pattern detection lives in pipeline/sync_preflight.go's
// matchManagedPGHostname — it needs the DSN in scope, which the
// engine's preflight surface deliberately doesn't carry (the IR
// interface accepts only chainTerminal + slotName so engines without
// network awareness can implement it).

// isPermissionDenied reports whether err is a Postgres "permission
// denied" error. Used by [detectPatroniSource] to degrade gracefully
// when pg_stat_replication is restricted on the connecting role.
func isPermissionDenied(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "permission denied") || strings.Contains(msg, "SQLSTATE 42501")
}
