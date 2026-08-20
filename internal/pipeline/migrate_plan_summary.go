// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// logMigrationPlanSummary gathers the primitives the plan/gotcha report needs
// and hands them to [migcore.LogMigrationPlanSummary]. The presentation +
// gating live in migcore, shared with the sync cold-start (which adopts the
// report via [Streamer.logColdStartPlanSummary]); this method is only the
// gather, using the already-open source reader
// for the largest-table estimate so no extra probe pass is added to a run that
// would not otherwise estimate. fkStatus is the verdict the earlier FK preflight
// returned.
func (m *Migrator) logMigrationPlanSummary(ctx context.Context, rr ir.RowReader, schema *ir.Schema, fkStatus migcore.PlanetScaleFKStatus) {
	largestTable, largestRows, known := largestEstimatedTable(ctx, rr, schema)
	migcore.LogMigrationPlanSummary(ctx, migcore.MigrationPlanSummary{
		Command:             "migrate",
		TargetEngine:        m.Target.Name(),
		TargetIsPlanetScale: m.Target.Name() == enginePlanetScale,
		TableCount:          len(schema.Tables),
		ForeignKeyCount:     migcore.CountForeignKeys(schema),
		FKStatus:            fkStatus,
		LargestTable:        largestTable,
		LargestTableRows:    largestRows,
		LargestTableKnown:   known,
		UpfrontIndexes:      m.UpfrontIndexes,
		RaiseQueryTimeout:   m.RaiseQueryTimeout,
	})
}

// logColdStartPlanSummary is the sync cold-start counterpart of
// [Migrator.logMigrationPlanSummary] — the same gather + hand-off to the shared
// [migcore.LogMigrationPlanSummary], reading the Streamer's own flags. The
// Command noun is "sync cold-start" so the report reads naturally on this entry
// point; fkStatus is the verdict the cold-start FK preflight returned. The
// reader is the just-opened snapshot stream: it does not implement
// [ir.RowCountEstimator] today (a pinned snapshot / VStream tail yields no
// estimate), so LargestTableKnown degrades to false and the report is
// counts-only — the PlanetScale FK/gotcha lines still fire, which is the value.
func (s *Streamer) logColdStartPlanSummary(ctx context.Context, rr ir.RowReader, schema *ir.Schema, fkStatus migcore.PlanetScaleFKStatus) {
	largestTable, largestRows, known := largestEstimatedTable(ctx, rr, schema)
	migcore.LogMigrationPlanSummary(ctx, migcore.MigrationPlanSummary{
		Command:             "sync cold-start",
		TargetEngine:        s.Target.Name(),
		TargetIsPlanetScale: s.Target.Name() == enginePlanetScale,
		TableCount:          len(schema.Tables),
		ForeignKeyCount:     migcore.CountForeignKeys(schema),
		FKStatus:            fkStatus,
		LargestTable:        largestTable,
		LargestTableRows:    largestRows,
		LargestTableKnown:   known,
		UpfrontIndexes:      s.UpfrontIndexes,
		RaiseQueryTimeout:   s.RaiseQueryTimeout,
	})
}

// largestEstimatedTable returns the name + estimated row count of the largest
// table by the source reader's optional [ir.RowCountEstimator], and whether any
// estimate was available. The name-carrying sibling of [largestEstimatedRows]
// (query_timeout_raise.go); estimate failures are skipped, never fatal. The
// estimate is from STATISTICS and is routinely off by a wide margin — the plan
// report labels it an estimate for exactly that reason.
func largestEstimatedTable(ctx context.Context, rr ir.RowReader, schema *ir.Schema) (name string, rows int64, known bool) {
	estimator, ok := rr.(ir.RowCountEstimator)
	if !ok {
		return "", 0, false
	}
	for _, t := range schema.Tables {
		n, err := estimator.EstimateRowCount(ctx, t)
		if err != nil {
			slog.DebugContext(ctx, "planetscale: plan-report row estimate failed for a table; ignoring it",
				slog.String("table", t.Name), slog.String("error", err.Error()))
			continue
		}
		if n <= 0 {
			continue
		}
		if !known || n > rows {
			name, rows, known = t.Name, n, true
		}
	}
	return name, rows, known
}
