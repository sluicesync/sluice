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

	"github.com/orware/sluice/internal/ir"
)

// PositionFromManifestPreflight is the optional engine-side surface
// for the Phase 3.3.C pre-flight checks. PG implements it on the
// engine's [ir.SchemaReader] (parallel to [ir.HealthReporter] /
// [ir.BackupPositionCapturer]); engines without operator-attention
// surfaces simply omit the method.
//
// The contract: implementations inspect the source's slot/WAL state
// against the supplied chainTerminal position and the slotName the
// CDC reader will use, and return a [PreflightReport] capturing soft
// warnings + an optional refusal. The streamer surfaces refusals as
// run-aborting errors; warnings turn into refusals when StrictPreflight
// is true.
type PositionFromManifestPreflight interface {
	PreflightPositionFromManifest(
		ctx context.Context,
		chainTerminal ir.Position,
		slotName string,
	) (PreflightReport, error)
}

// PreflightReport bundles the result of a Phase 3.3.C pre-flight
// against the source. Warnings are operator-actionable advisories
// that don't block the run by default; Refusal is a fatal condition
// the operator must address before the run can proceed (slot lost,
// slot missing, WAL gap exceeds keep-size).
type PreflightReport struct {
	// Warnings is the slice of soft-warning messages emitted by the
	// preflight. Each is a single-sentence operator-facing string;
	// the streamer logs them via slog.WarnContext and (when
	// StrictPreflight is true) escalates to a refusal.
	Warnings []string

	// Refusal is non-empty when the preflight encountered a fatal
	// condition. The streamer surfaces it as a wrapped run error.
	// Empty means "no refusal" — warnings only.
	Refusal string
}

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
