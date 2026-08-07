// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// PreflightTableNameFold asks the TARGET SERVER whether two tables in schema
// would land on one table name once the server's own identifier fold is
// applied, and refuses before any data moves if they would (roadmap item 149).
//
// # The problem it closes, and why its sibling could not
//
// [PreflightTableEmit] closes the same silent merge — two source tables, one
// target table, both tables' rows in it, the right count under the surviving
// name, exit 0 — for engines that fold by their own rule. MySQL does not: it
// folds table names when the server runs `lower_case_table_names != 0` (the
// default on Windows and macOS servers, and a legitimate deliberate setting on
// Linux) and not at all when it runs 0 (the stock Linux default). Measured on
// real `mysql:8` containers, both directions: under lct=1 the second
// `CREATE TABLE IF NOT EXISTS` returns Note 1050 — a WARNING — and inserting
// one row under each spelling leaves two rows in one table; under lct=0 both
// tables exist and nothing is wrong.
//
// So the answer is not derivable from the schema, and a schema-only check would
// have to pick a side: silence (the gap item 148 filed rather than papered
// over) or a fold that refuses a legal pair on every stock Linux MySQL. This
// helper asks the server instead. targetDSN names it; the database component is
// optional because the setting is global.
//
// # Scope, stated rather than implied
//
// This reaches exactly the engines that implement
// [ir.TableNameFoldPreflighter], which today is the MySQL engine and therefore
// all four of its flavors (mysql, planetscale, vitess, mariadb — one engine,
// one statement, one server-side fold). An engine without the surface is
// skipped silently, and a target whose server does not fold gets one variable
// read and no refusal.
//
// It is source-schema-internal: it claims the names in schema against each
// other, never against the tables already on the target. That residual is
// deliberate and is the one place where checking MORE would be wrong — under
// lct=1 the server stores the names sluice's OWN earlier run created already
// lowercased, so a `--resume` of a source table named `Orders` would find a
// target `orders` that folds onto it and be refused for having worked.
//
// mode labels the run ("migrate", "sync cold-start", "restore: …") and is
// logged rather than folded into the refusal, so the engine's message stays
// byte-identical to the one its own phase would produce.
func PreflightTableNameFold(ctx context.Context, target ir.Engine, targetDSN string, schema *ir.Schema, mode string) error {
	pf, ok := target.(ir.TableNameFoldPreflighter)
	if !ok || schema == nil {
		return nil
	}
	if err := pf.PreflightTableNameFold(ctx, targetDSN, schema); err != nil {
		return fmt.Errorf("%s: %w", mode, err)
	}
	slog.DebugContext(ctx, "table-name-fold preflight clean",
		slog.String("mode", mode),
		slog.String("target_engine", target.Name()),
		slog.Int("tables", len(schema.Tables)))
	return nil
}
