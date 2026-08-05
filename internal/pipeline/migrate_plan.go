// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// Dry-run migration plan in build-then-render form (docs/research/
// ai-friendly-sluice.md recommendation #2). One struct feeds both
// renderers: [Migrator.logPlan]'s slog lines (text mode — same lines
// as the pre-split print-as-you-go shape) and the CLI's
// `--dry-run --format json` plan object (via [Migrator.PlanSink]).

// MigrationPlan is what a dry-run migrate would do: the translated,
// filtered table/view set with best-effort per-table row counts. JSON
// tags define the CLI's `--format json` plan payload.
type MigrationPlan struct {
	SourceEngine string      `json:"source_engine"`
	TargetEngine string      `json:"target_engine"`
	Views        int         `json:"views"`
	Tables       []PlanTable `json:"tables"`

	// AnalyzeAfter mirrors [Migrator.AnalyzeAfter] so a dry run shows the
	// opt-in post-migration statistics-refresh phase in the plan. Omitted
	// from the JSON when off (the default) — the plan payload is unchanged
	// for operators who don't engage the flag.
	AnalyzeAfter bool `json:"analyze_after,omitempty"`
}

// PlanTable is one table's dry-run summary. RowCount is -1 when the
// count is unavailable (engine doesn't implement [ir.RowCounter], the
// probe failed, or the plan flavor never probes — the streamer's
// dry-run doesn't count rows) so consumers can't mistake "unknown"
// for "empty". SecondaryIndexes excludes the primary key, which is
// reported separately via PrimaryKey — see logPlan's field-naming note.
type PlanTable struct {
	Name             string `json:"name"`
	Columns          int    `json:"columns"`
	PrimaryKey       bool   `json:"primary_key"`
	SecondaryIndexes int    `json:"secondary_indexes"`
	ForeignKeys      int    `json:"foreign_keys"`
	RowCount         int64  `json:"row_count"`

	// RowCountEstimated reports that RowCount came from the engine's
	// STATISTICS rather than from counting rows — MySQL's
	// information_schema.TABLE_ROWS, Postgres's pg_class.reltuples. True
	// whenever a count was obtained at all; false only when RowCount is
	// -1 (unavailable).
	//
	// It exists because the number is routinely wrong by a wide margin and
	// nothing here said so. A 2026-08-04 field report on a MariaDB source
	// saw 1,026,026 planned against 1,701,520 actually copied — 40% low —
	// and read the difference as sluice reporting a mismatch. Nothing was
	// wrong: InnoDB's estimate lags cardinality badly on a table that has
	// not been ANALYZEd recently, and it can even read 0 for a freshly
	// loaded one. The copy-progress line has carried
	// `total_rows_estimated` for this reason since roadmap #22; the plan
	// had not, so the one number an operator naturally diffs against the
	// final count was the one presented as exact (roadmap item 124).
	//
	// `sluice verify` is unaffected and always was: it counts both sides
	// exactly, which is why it agreed with the target while this did not.
	RowCountEstimated bool `json:"row_count_estimated"`
}

// buildDryRunPlan assembles the migration plan from the already-read,
// already-translated schema. Per-table row counts (v0.10.2) are
// best-effort via [dryRunRowCounts]; unavailable counts stay -1.
func (m *Migrator) buildDryRunPlan(ctx context.Context, schema *ir.Schema) *MigrationPlan {
	counts := dryRunRowCounts(ctx, m.Source, m.SourceDSN, schema)
	plan := &MigrationPlan{
		SourceEngine: m.Source.Name(),
		TargetEngine: m.Target.Name(),
		Views:        len(schema.Views),
		Tables:       make([]PlanTable, 0, len(schema.Tables)),
		AnalyzeAfter: m.AnalyzeAfter,
	}
	for _, t := range schema.Tables {
		plan.Tables = append(plan.Tables, PlanTable{
			Name:              t.Name,
			Columns:           len(t.Columns),
			PrimaryKey:        t.PrimaryKey != nil,
			SecondaryIndexes:  len(t.Indexes),
			ForeignKeys:       len(t.ForeignKeys),
			RowCount:          counts[t.Name],
			RowCountEstimated: counts[t.Name] >= 0,
		})
	}
	return plan
}

// logPlan writes the human-readable rendering of a built plan via
// slog. Used when DryRun is true and no PlanSink is attached.
//
// The plan is logged at Info level so it surfaces under the default
// handler. The header line is a single message; the per-table lines
// follow with structured attributes so an aggregator can pick out
// individual table summaries without parsing prose.
//
// Build-then-render note: the row-count probe now runs inside
// buildDryRunPlan, BEFORE the header line is emitted, so a count
// failure's WARN precedes the header instead of following it. The
// rendered lines themselves are unchanged.
func (m *Migrator) logPlan(ctx context.Context, plan *MigrationPlan) {
	slog.InfoContext(
		ctx, "dry run: migration plan",
		slog.String("source", plan.SourceEngine),
		slog.String("target", plan.TargetEngine),
		slog.Int("tables", len(plan.Tables)),
		slog.Int("views", plan.Views),
	)

	for _, t := range plan.Tables {
		// Field naming note: secondary_indexes excludes the primary
		// key (which is reported separately via primary_key) — the IR
		// stores PK on its own field, and operators comparing against
		// psql / SHOW INDEX output have been confused by a bare
		// "indexes" count that didn't include PK.
		slog.InfoContext(
			ctx, "dry run: table",
			slog.String("name", t.Name),
			slog.Int("columns", t.Columns),
			slog.Bool("primary_key", t.PrimaryKey),
			slog.Int("secondary_indexes", t.SecondaryIndexes),
			slog.Int("foreign_keys", t.ForeignKeys),
			slog.Int64("row_count", t.RowCount),
			slog.Bool("row_count_estimated", t.RowCountEstimated),
		)
	}
	if plan.AnalyzeAfter {
		slog.InfoContext(ctx, "dry run: post-migration phase",
			slog.String("phase", "analyze"),
			slog.String("note", "--analyze-after: per-table target ANALYZE after constraints/views (advisory; failures WARN, never fail the run)"))
	}
	slog.InfoContext(ctx, "dry run: for full target DDL with translation notes and advisory hints, run `sluice schema preview` (ADR-0024)")
}
