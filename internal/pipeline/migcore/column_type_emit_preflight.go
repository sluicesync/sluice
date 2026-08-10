// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// PreflightColumnTypeEmit asks the TARGET engine whether it can render every
// COLUMN TYPE in schema, and refuses before any data moves if it cannot.
//
// # The problem it closes, and how it differs from its four siblings
//
// The other four members of this family close cases where a refusal fires LATE
// (indexes, views) or NOT AT ALL (the two table-name ones). This one is about
// REACH. The refusal already exists in every target engine — a type its
// `emitColumnType` has no arm for is refused, loudly, at CREATE TABLE — and the
// hand-coded early door in front of it, [CheckCrossEngineSupportable], covers
// exactly one direction: a Postgres-family source into a MySQL-family target.
// A PG `inet` bound for SQLite, or a MySQL `VARCHAR(0)` bound for Postgres,
// passes that check untouched and is found by the target's own DDL.
//
// That is loud and zero-loss, and it is still the wrong place: schema-apply
// runs AFTER the dry-run plan is printed and the target writers are open, and
// it creates tables in order — so a schema whose tenth table carries the
// offending column leaves nine tables created on the target before it stops.
// Asking the engine up front makes the answer the same and the target
// untouched.
//
// # Why asking the engine rather than adding to the hand-coded check
//
// [CheckCrossEngineSupportable] is a SECOND statement of what each engine can
// hold, keyed on engine-name strings. The 2026-08-10 audit deleted seven
// declarative capability fields for being exactly that — a second copy of a
// truth the engines already hold structurally — after finding it had drifted in
// three separate directions. Growing that check to cover PG → SQLite would
// re-create the same shape one pair at a time, and it can never reach a pair
// nobody wrote down. The engine's own type dispatch IS the answer; this asks
// it.
//
// # Scope, stated rather than implied
//
// This reaches exactly the refusals the implementing engines route through
// their own `emitColumnType`. It does NOT reach refusals that need the target
// server or the run's flags (Postgres's PostGIS-absent and
// `--enable-pg-extension` arms — see [ir.ColumnTypeEmitPreflighter] for why),
// nor refusals that come from a column's NAME or DEFAULT rather than its type.
// Those still fire in the create-tables phase, which is phase one.
//
// An engine without the optional surface is skipped silently, and that is a
// GAP: it is not evidence the engine can hold every type. The registry roster
// TestEveryTargetCapableEngineAnswersTheColumnTypePreflight is where that set
// is written down.
//
// # It deliberately runs AFTER [CheckCrossEngineSupportable]
//
// Both can refuse the same PG → MySQL column (a `numeric(p,-s)`, a PG-only
// extension type). The hand-coded check's message carries recovery guidance
// this one cannot derive — `--exclude-table`, the exact `--type-override`
// landing — so it is left in front and keeps owning those messages. Nothing
// here changes what an operator sees on the one pair that was already covered.
//
// mode labels the run ("migrate", "sync cold-start", "restore: …") and is
// logged rather than folded into the refusal, so the engine's message — the one
// an operator acts on — stays byte-identical to the one the emitter would have
// produced later.
func PreflightColumnTypeEmit(ctx context.Context, target ir.Engine, schema *ir.Schema, mode string) error {
	pf, ok := target.(ir.ColumnTypeEmitPreflighter)
	if !ok || schema == nil {
		return nil
	}
	if err := pf.PreflightColumnTypes(schema); err != nil {
		return fmt.Errorf("%s: %w", mode, err)
	}
	slog.DebugContext(ctx, "column-type-emit preflight clean",
		slog.String("mode", mode),
		slog.String("target_engine", target.Name()),
		slog.Int("tables", len(schema.Tables)))
	return nil
}
