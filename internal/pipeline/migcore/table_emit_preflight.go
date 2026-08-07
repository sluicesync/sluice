// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// PreflightTableEmit asks the TARGET engine whether every table in schema can
// actually be created under its own name, and refuses before any data moves if
// two would land on one (roadmap item 148).
//
// # The problem it closes, and why it is worse than its two siblings
//
// [PreflightIndexEmit] and [PreflightViewEmit] close cases where an object goes
// MISSING. This one closes the case where nothing goes missing and the ROWS go
// somewhere else: SQLite's `CREATE TABLE IF NOT EXISTS` with a bare name is a
// success that creates nothing when the (case-folded) name is taken, and the
// copy then INSERTs that table's rows under a name that resolves to the table
// which won. Both tables' rows sit in one table, the count is right for the
// surviving name and the run exits 0 with no failing statement. (A later `verify
// --depth count` WOULD catch it — it compares each SOURCE table against the
// target, so both mismatch the merged count. The migration is what is silent.)
// and the run exits 0. There is no signal anywhere in the run.
//
// # Why a third sibling rather than one more question inside either
//
// Same reason [PreflightViewEmit] is not a question inside PreflightIndexes: a
// method's name must not be broader than what it checks. The sharing that
// matters is NOT at this layer anyway — SQLite's table and view refusals are
// two reports off ONE claim pass over its single object namespace
// (engines/sqlite/object_namespace.go), which is where drift between them
// would actually happen. Here the parity that matters is that every copy entry
// point asks all three questions, and that is enforced mechanically by
// TestIndexEmitPreflightReachesEveryCopyEntryPoint, which rosters all three
// helpers against one entry-point list.
//
// # Scope, stated rather than implied
//
// This reaches exactly the engines that implement [ir.TableEmitPreflighter],
// which today is SQLite alone. The roster gate
// TestEveryTargetRefusesACollidingTableNamespace holds that statement to the
// registry — including the ONE engine whose exemption is a known GAP rather
// than a safety argument (MySQL under `lower_case_table_names != 0`), which
// that roster names in those words rather than letting an exemption line imply
// coverage.
//
// An engine without the optional surface is skipped silently.
//
// mode labels the run ("migrate", "sync cold-start", "restore: …") and is
// logged rather than folded into the refusal, so the engine's message stays
// byte-identical to the one its own phase would produce.
func PreflightTableEmit(ctx context.Context, target ir.Engine, schema *ir.Schema, mode string) error {
	pf, ok := target.(ir.TableEmitPreflighter)
	if !ok || schema == nil {
		return nil
	}
	if err := pf.PreflightTables(schema); err != nil {
		return fmt.Errorf("%s: %w", mode, err)
	}
	slog.DebugContext(ctx, "table-emit preflight clean",
		slog.String("mode", mode),
		slog.String("target_engine", target.Name()),
		slog.Int("tables", len(schema.Tables)))
	return nil
}
