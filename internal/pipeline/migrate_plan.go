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
	}
	for _, t := range schema.Tables {
		plan.Tables = append(plan.Tables, PlanTable{
			Name:             t.Name,
			Columns:          len(t.Columns),
			PrimaryKey:       t.PrimaryKey != nil,
			SecondaryIndexes: len(t.Indexes),
			ForeignKeys:      len(t.ForeignKeys),
			RowCount:         counts[t.Name],
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
		)
	}
	slog.InfoContext(ctx, "dry run: for full target DDL with translation notes and advisory hints, run `sluice schema preview` (ADR-0024)")
}
