// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Phase 3.3.C pre-flight checks for `sluice sync start
// --position-from-manifest`. Three checks against PG sources before
// the CDC reader opens its replication slot:
//
//  1. wal_keep_size sufficiency — soft warning when the configured
//     retention looks too small for the chain's typical incremental
//     cadence.
//  2. Patroni-managed source detection — soft warning about the
//     idle-slot failover trap (see docs/postgres-source-prep.md).
//  3. Slot existence + health — refusal when the slot named in the
//     chain's terminal position is missing or wal_status='lost'.
//
// MySQL has no operator-attention surface here: binlog retention is
// already covered by the CDC reader's verifyPositionResumable check
// (which surfaces ir.ErrPositionInvalid; the streamer's normal flow
// handles it). Phase 3.3.C is PG-only by design.
//
// Engines that don't implement [PositionFromManifestPreflight]
// silently skip — the streamer's main flow runs unchanged.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// PatroniMode constants for [Streamer.PatroniMode]. The flag is a
// string at the surface so kong can parse it; these constants pin the
// in-codebase values to keep callers from drifting on spelling.
const (
	// PatroniModeAuto is the default: run the engine heuristics, warn
	// if any signal fires.
	PatroniModeAuto = "auto"

	// PatroniModeOn skips the heuristics and forces the warning to
	// fire — operator opts in regardless of detection.
	PatroniModeOn = "on"

	// PatroniModeOff skips the heuristics and suppresses any Patroni
	// warning the engine would emit — operator opts out regardless of
	// detection.
	PatroniModeOff = "off"
)

// ValidatePatroniMode normalises and validates a --patroni-mode
// value. Empty is treated as auto (the default). Returns the
// canonical value or an error naming the accepted set.
func ValidatePatroniMode(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", PatroniModeAuto:
		return PatroniModeAuto, nil
	case PatroniModeOn:
		return PatroniModeOn, nil
	case PatroniModeOff:
		return PatroniModeOff, nil
	default:
		return "", fmt.Errorf("invalid --patroni-mode %q: want one of auto, on, off", s)
	}
}

// patroniWarningPrefix is the substring every Patroni-detection
// warning starts with, used to filter the engine's reports when
// --patroni-mode=off.
const patroniWarningPrefix = "this PG cluster is HA-managed"

// PositionFromManifestPreflight and PreflightReport now live in the
// `ir` package so engine packages can implement the interface without
// forming an import cycle through pipeline's integration tests. The
// type aliases below preserve the names at this package's surface so
// existing callers (tests, the streamer's preflight runner) keep
// compiling without prefix churn.
type (
	// PositionFromManifestPreflight is re-exported from ir for
	// streamer-side access. Engines should reference
	// [ir.PositionFromManifestPreflight] directly to keep their
	// imports minimal.
	PositionFromManifestPreflight = ir.PositionFromManifestPreflight

	// PreflightReport is re-exported from ir for streamer-side
	// access. Engines populate this struct value as the return of
	// PreflightPositionFromManifest.
	PreflightReport = ir.PreflightReport
)

// runPositionFromManifestPreflight runs the source-side pre-flight
// checks against the chain terminal position and surfaces the result
// according to s.StrictPreflight. Returns nil on clean preflight
// (warnings logged, run proceeds); a non-nil error on refusal or on
// any warning under StrictPreflight.
//
// The preflight surface is opt-in: engines that don't implement
// [PositionFromManifestPreflight] on their SchemaReader silently
// skip. A SchemaReader that fails to open is also skipped (with a
// debug log) — preflight is best-effort guidance, not a gate. The
// CDC reader's existing slot-state checks (checkSlotUsable) and
// resume-position validation (verifyPositionResumable) still run
// and surface fatal conditions even when preflight is unavailable.
func (s *Streamer) runPositionFromManifestPreflight(ctx context.Context, chainTerminal ir.Position) error {
	patroniMode, err := ValidatePatroniMode(s.PatroniMode)
	if err != nil {
		return err
	}

	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		slog.DebugContext(ctx, "position-from-manifest: could not open source schema reader for preflight; skipping",
			slog.String("engine", s.Source.Name()),
			slog.String("error", err.Error()),
		)
		return nil
	}
	defer closeIf(sr)

	preflighter, ok := sr.(PositionFromManifestPreflight)
	if !ok {
		slog.DebugContext(ctx, "position-from-manifest: source engine has no PreflightPositionFromManifest surface; skipping preflight",
			slog.String("engine", s.Source.Name()),
		)
		return nil
	}

	report, err := preflighter.PreflightPositionFromManifest(ctx, chainTerminal, s.SlotName)
	if err != nil {
		return fmt.Errorf("preflight query failed: %w", err)
	}

	// Mode-driven Patroni-warning shaping (Bug 36, v0.17.3):
	//
	//   auto: keep engine warnings as-is; layer in a hostname-pattern
	//         signal (covers managed-PG services where the engine's
	//         SQL-side signals miss because of permission isolation).
	//   on:   strip the engine's Patroni warning if any, append a
	//         single "operator forced via --patroni-mode=on" warning.
	//   off:  strip every Patroni warning the engine emitted; emit
	//         nothing.
	report = s.applyPatroniMode(report, patroniMode)

	for _, w := range report.Warnings {
		slog.WarnContext(ctx, "position-from-manifest preflight: "+w,
			slog.String("engine", s.Source.Name()),
		)
	}
	if report.Refusal != "" {
		return fmt.Errorf("preflight refused: %s", report.Refusal)
	}
	if s.StrictPreflight && len(report.Warnings) > 0 {
		return fmt.Errorf("preflight refused under --strict-preflight: %d warning(s); first: %s",
			len(report.Warnings), report.Warnings[0])
	}
	return nil
}

// applyPatroniMode shapes the engine's PreflightReport according to
// the operator's --patroni-mode choice. See
// [Streamer.runPositionFromManifestPreflight] for the per-mode
// semantics summary.
//
// The Refusal field is never modified — a slot-missing /
// wal_status='lost' refusal must trip regardless of patroni-mode.
func (s *Streamer) applyPatroniMode(report PreflightReport, mode string) PreflightReport {
	switch mode {
	case PatroniModeOff:
		// Suppress every Patroni warning the engine emitted; leave
		// other warnings (e.g. wal_keep_size) intact.
		filtered := report.Warnings[:0:0]
		for _, w := range report.Warnings {
			if !strings.HasPrefix(w, patroniWarningPrefix) {
				filtered = append(filtered, w)
			}
		}
		report.Warnings = filtered
		return report
	case PatroniModeOn:
		// Strip engine-emitted Patroni warnings (avoid double-warn)
		// and append the operator-forced one.
		filtered := report.Warnings[:0:0]
		for _, w := range report.Warnings {
			if !strings.HasPrefix(w, patroniWarningPrefix) {
				filtered = append(filtered, w)
			}
		}
		filtered = append(filtered,
			"this PG cluster is HA-managed (--patroni-mode=on; operator forced). "+
				"The slot you're starting CDC from is subject to the idle-slot failover trap — slots not actively consumed don't replicate to standbys and are silently lost on failover. "+
				"Ensure the slot is being actively consumed; for low-traffic sources, consider a heartbeat-write strategy. See docs/postgres-source-prep.md.",
		)
		report.Warnings = filtered
		return report
	default:
		// auto: append the DSN-hostname signal if it fires AND the
		// engine didn't already emit a Patroni warning (avoid
		// double-warn on the same condition).
		if pattern := matchManagedPGHostname(s.SourceDSN); pattern != "" {
			already := false
			for _, w := range report.Warnings {
				if strings.HasPrefix(w, patroniWarningPrefix) {
					already = true
					break
				}
			}
			if !already {
				report.Warnings = append(report.Warnings,
					fmt.Sprintf("this PG cluster is HA-managed (DSN hostname matches managed-PG pattern: %s). "+
						"The slot you're starting CDC from is subject to the idle-slot failover trap — slots not actively consumed don't replicate to standbys and are silently lost on failover. "+
						"Ensure the slot is being actively consumed; for low-traffic sources, consider a heartbeat-write strategy. See docs/postgres-source-prep.md.",
						pattern))
			}
		}
		return report
	}
}

// matchManagedPGHostname extracts the host (no port) from the DSN
// and tests it against the v0.17.3 managed-PG hostname-pattern set.
// Patterns are intentionally narrow — false positives erode the
// warning's signal value.
//
// Patterns:
//
//   - *.psdb.cloud (PlanetScale Postgres)
//   - *.aws.prod.archil.com / *.gcp.prod.archil.com (Archil)
//   - *.cluster*.rds.amazonaws.com (Aurora cluster endpoints)
//   - *.postgres.database.azure.com (Azure Database for PostgreSQL)
//   - *.cloudsql.google.internal (Cloud SQL via private IP)
func matchManagedPGHostname(dsn string) string {
	host := redactedHost(dsn)
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return ""
	}
	switch {
	case strings.HasSuffix(host, ".psdb.cloud"):
		return "*.psdb.cloud"
	case strings.HasSuffix(host, ".aws.prod.archil.com"):
		return "*.aws.prod.archil.com"
	case strings.HasSuffix(host, ".gcp.prod.archil.com"):
		return "*.gcp.prod.archil.com"
	case strings.HasSuffix(host, ".rds.amazonaws.com") &&
		(strings.Contains(host, ".cluster.") || strings.Contains(host, ".cluster-")):
		return "*.cluster*.rds.amazonaws.com (Aurora)"
	case strings.HasSuffix(host, ".postgres.database.azure.com"):
		return "*.postgres.database.azure.com"
	case strings.HasSuffix(host, ".cloudsql.google.internal"):
		return "*.cloudsql.google.internal"
	}
	return ""
}
