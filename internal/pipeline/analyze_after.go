// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// runAnalyzeAfterPhase runs the opt-in `--analyze-after` per-table
// statistics refresh (perf research delta 4) against the target: one
// [ir.TableAnalyzer.AnalyzeTable] per migrated table, after constraints
// and views are in place so the statistics reflect the final shape.
//
// The phase is ADVISORY by contract: it runs only after the migration's
// data and DDL phases have durably completed, so nothing here may fail
// the run. A per-table analyze error WARNs loudly (naming the table) and
// moves on; a target engine without the [ir.TableAnalyzer] surface gets
// one loud WARN (the operator asked for the phase explicitly — silently
// skipping would leave them believing statistics are fresh). No resume
// state is recorded — a resumed run re-runs the phase, and re-ANALYZE is
// idempotent by nature.
func runAnalyzeAfterPhase(ctx context.Context, schema *ir.Schema, sw ir.SchemaWriter) {
	analyzer, ok := sw.(ir.TableAnalyzer)
	if !ok {
		slog.WarnContext(ctx, "migration: --analyze-after requested but the target engine does not support per-table ANALYZE; skipping",
			slog.Int("tables", len(schema.Tables)))
		return
	}
	analyzed, failed := 0, 0
	for _, table := range schema.Tables {
		if err := analyzer.AnalyzeTable(ctx, table); err != nil {
			failed++
			slog.WarnContext(ctx, "migration: analyze-after failed for table; continuing (advisory phase — the migrated data is already complete)",
				slog.String("table", table.Name),
				slog.String("err", err.Error()))
			continue
		}
		analyzed++
	}
	slog.InfoContext(ctx, "migration: phase complete",
		slog.String("phase", "analyze"),
		slog.Int("tables_analyzed", analyzed),
		slog.Int("tables_failed", failed))
}
