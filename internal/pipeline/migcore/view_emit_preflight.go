// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// PreflightViewEmit asks the TARGET engine whether every view in schema can
// actually be created on it, and refuses before any data moves if one cannot
// (roadmap item 147).
//
// # The problem it closes
//
// Views are the LAST phase (create tables → copy → indexes → constraints →
// views), so any refusal raised there arrives after the entire database has
// been written. Worse on the engine this exists for: SQLite's view emit is
// `CREATE VIEW IF NOT EXISTS` into the namespace it shares with tables, so a
// view whose name a table already holds raises no refusal at all — the CREATE
// returns OK, creates nothing, and the run exits 0 with the view missing.
//
// # Why a sibling of [PreflightIndexEmit] rather than one more question inside
// it
//
// PreflightIndexes already answers two index questions. Folding a view
// question into a method named for indexes would make its name broader than
// its truth, which is the exact shape CLAUDE.md's gate-scope rule warns about:
// a gate whose coverage is narrower than its name is worse than no gate,
// because it stops anyone from looking. The parity that actually matters —
// every copy entry point asking BOTH questions — is enforced mechanically by
// TestIndexEmitPreflightReachesEveryCopyEntryPoint, which rosters both helpers
// against the same entry-point list.
//
// # Scope, stated rather than implied
//
// This reaches exactly the engines that implement [ir.ViewEmitPreflighter],
// which today is SQLite alone — and that is a claim about the other engines,
// not an omission: Postgres and MySQL both emit `CREATE OR REPLACE VIEW`, and
// both raise loudly when the name is held by a base table, so there is no
// silent no-op to preflight. The roster gate
// TestEveryTargetRefusesACollidingViewNamespace holds that statement to the
// registry.
//
// An engine without the optional surface is skipped silently.
//
// mode labels the run ("migrate", "sync cold-start", "restore: …") and is
// logged rather than folded into the refusal, so the engine's message — the
// one an operator acts on — stays byte-identical to the one its own phase
// would produce.
func PreflightViewEmit(ctx context.Context, target ir.Engine, schema *ir.Schema, mode string) error {
	pf, ok := target.(ir.ViewEmitPreflighter)
	if !ok || schema == nil {
		return nil
	}
	if err := pf.PreflightViews(schema); err != nil {
		return fmt.Errorf("%s: %w", mode, err)
	}
	slog.DebugContext(ctx, "view-emit preflight clean",
		slog.String("mode", mode),
		slog.String("target_engine", target.Name()),
		slog.Int("views", len(schema.Views)))
	return nil
}
