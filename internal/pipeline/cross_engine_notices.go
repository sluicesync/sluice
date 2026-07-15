// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// emitCrossEngineTranslationNotices emits the cluster of ADVISORY
// cross-engine schema-narrowing notices — the MySQL→PG unsigned-bigint
// range narrowing (Bug 11), the PG→MySQL unconstrained-numeric widening
// (Bug 69), the PG→MySQL wide-varchar down-map (Bug 72), and the
// MySQL→PG TIME duration-range mismatch (Bug 187) — at WARN so they
// stand out in default-level logs.
//
// Each scanner SELF-SHORT-CIRCUITS by engine pair (returns nil for a pair
// it does not apply to), so this helper calls all of them unconditionally
// and they no-op for non-applicable pairs. A same-engine run (MySQL→MySQL,
// PG→PG) therefore emits ZERO notices — the bigint-unsigned→MySQL and
// varchar→PG paths are lossless, and there must be no false WARN on them
// (the Bug-157 ground truth). The caller need not pre-gate on the engine
// pair.
//
// These are NOTICES, never refusals: the common case (values that fit)
// must still flow. The mode label is the scanners' contextID arg — it
// prefixes the message so the operator sees the right command context
// ("migrate", "sync cold-start") at whichever surface emitted it.
//
// Both the `migrate` orchestrator (phaseTranslateAndGateSchema) and the
// `sync` cold-start path (streamer_coldstart.go / streamer_multidb.go)
// call this, so a cross-engine `sync` cold-copy gets the same up-front
// warning `migrate` always did (Bug 157 Q2) — emitted before any target
// table is created or any row moves.
func emitCrossEngineTranslationNotices(ctx context.Context, schema *ir.Schema, sourceName, targetName, mode string) {
	// ---- Unsigned-bigint range-narrowing notice (Bug 11) ----
	// MySQL `bigint unsigned` maps uniformly to PG `bigint`; the
	// (2^63, 2^64) range loss is a deliberate, documented policy. The
	// scanner short-circuits non-MySQL→PG pairs.
	if noticeErr := translate.UnsignedBigintNoticeError(
		schema, sourceName, targetName, mode,
	); noticeErr != nil {
		slog.WarnContext(ctx, noticeErr.Error())
	}

	// ---- Unconstrained-numeric widening notice (Bug 69) ----
	// An unconstrained PG `numeric` maps to MySQL `DECIMAL(65,30)`; the
	// scanner short-circuits non-MySQL targets (PG→PG round-trips bare).
	if noticeErr := translate.UnconstrainedNumericNoticeError(
		schema, sourceName, targetName, mode,
	); noticeErr != nil {
		slog.WarnContext(ctx, noticeErr.Error())
	}

	// ---- Wide-varchar down-map notice (Bug 72) ----
	// A wide bounded PG `varchar(N)` over MySQL's VARCHAR cap is
	// down-mapped to a MySQL TEXT tier; the scanner short-circuits
	// non-MySQL targets (PG→PG round-trips unchanged).
	if noticeErr := translate.WideVarcharNoticeError(
		schema, sourceName, targetName, mode,
	); noticeErr != nil {
		slog.WarnContext(ctx, noticeErr.Error())
	}

	// ---- MySQL TIME duration-range notice (Bug 187) ----
	// MySQL TIME is a duration (-838:59:59..838:59:59); PG `time` holds
	// only a time-of-day, so an out-of-range value refuses loudly
	// mid-copy. The notice moves that discovery up front and names the
	// lossless `--type-override TABLE.COL=interval` remedy. The scanner
	// short-circuits non-MySQL-family→PG pairs.
	if noticeErr := translate.MySQLTimeRangeNoticeError(
		schema, sourceName, targetName, mode,
	); noticeErr != nil {
		slog.WarnContext(ctx, noticeErr.Error())
	}
}
