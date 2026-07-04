// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// StreamPlan is what a dry-run `sync start` would do: warm-resume from
// the persisted position, or cold-start over the summarised source
// schema. Built once and fed to both renderers — the slog lines below
// (text mode) and the CLI's `--dry-run --format json` plan object (via
// [Streamer.PlanSink]). Hosts are pre-redacted via [redactedHost] and
// the position token pre-truncated, so the plan carries no credentials
// in any field.
type StreamPlan struct {
	SourceEngine  string      `json:"source_engine"`
	SourceHost    string      `json:"source_host"`
	TargetEngine  string      `json:"target_engine"`
	TargetHost    string      `json:"target_host"`
	StreamID      string      `json:"stream_id"`
	WarmResume    bool        `json:"warm_resume"`
	PositionToken string      `json:"position_token,omitempty"`
	Tables        []PlanTable `json:"tables,omitempty"`

	// noSourceTables distinguishes "the source schema has no tables at
	// all" (render says nothing to stream) from "the filter selected
	// zero tables" (render shows the tables=0 header). Unexported —
	// render-only nuance; JSON consumers read the empty Tables list
	// either way.
	noSourceTables bool
}

// buildDryRunPlan assembles the stream plan. The source schema read
// for cold-start is the only source-side touch the dry-run does —
// same level of access the regular cold-start would do, just without
// then opening the snapshot stream or starting CDC. Warm-resume never
// touches the source. Per-table row counts are not probed on this
// path (the streamer's dry-run never did); PlanTable.RowCount stays
// -1 = unavailable.
func (s *Streamer) buildDryRunPlan(ctx context.Context, streamID string, persisted ir.Position, found bool) (*StreamPlan, error) {
	plan := &StreamPlan{
		SourceEngine: s.Source.Name(),
		SourceHost:   redactedHost(s.SourceDSN),
		TargetEngine: s.Target.Name(),
		TargetHost:   redactedHost(s.TargetDSN),
		StreamID:     streamID,
		WarmResume:   found,
	}
	if found {
		plan.PositionToken = truncateDryRunToken(persisted.Token, 80)
		return plan, nil
	}

	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: open source schema reader: %w", err))
	}
	defer closeIf(sr)
	if err := applyEnabledPGExtensions(ctx, sr, s.EnabledPGExtensions); err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: enable PG extensions on source: %w", err))
	}
	// ADR-0047 tier (b): live PG → PG sync may carry uncatalogued
	// extension types verbatim. Engine-name-only determination.
	applyVerbatimExtensionPassthrough(sr, verbatimLiveSameEnginePG(s.Source, s.Target))
	// catalog Bug 76: scope per-column type validation to the filtered
	// table set (s.Filter already has engine defaults merged in Run).
	applyTableScope(sr, s.Filter)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		plan.noSourceTables = true
		return plan, nil
	}
	if err := applyTableFilter(ctx, schema, s.Filter); err != nil {
		return nil, err
	}
	applyViewFilter(ctx, schema, s.ViewFilter, s.SkipViews)
	// ADR-0143: mirror the cold-start ORM-table skip so the dry-run preview
	// honestly reflects what a real run would copy.
	applyORMTableSkip(ctx, schema, s.SkipORMTables, s.Filter)
	mapped, err := translate.ApplyMappings(schema, s.Mappings)
	if err != nil {
		return nil, fmt.Errorf("pipeline: dry-run: apply mappings: %w", err)
	}
	if _, err := translate.ApplyExpressionOverrides(mapped, s.ExpressionMappings); err != nil {
		return nil, fmt.Errorf("pipeline: dry-run: apply expression overrides: %w", err)
	}
	plan.Tables = make([]PlanTable, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		plan.Tables = append(plan.Tables, PlanTable{
			Name:             t.Name,
			Columns:          len(t.Columns),
			PrimaryKey:       t.PrimaryKey != nil,
			SecondaryIndexes: len(t.Indexes),
			ForeignKeys:      len(t.ForeignKeys),
			RowCount:         -1, // never probed on the stream dry-run
		})
	}
	return plan, nil
}

// logDryRunPlan describes what Run would do without doing it. The
// plan is built first ([Streamer.buildDryRunPlan]), then either
// handed to PlanSink (the CLI's json mode) or rendered as structured
// slog records: cold-start logs the source schema summary so
// operators can catch missing-tables / unexpected-column-counts
// before the migration starts; warm-resume logs the persisted
// position token (truncated for readability) so operators can see
// whether the stream is positioned where they expect.
//
// Build-then-render note: the header line is now emitted after the
// plan is fully built, so when the cold-start schema read fails the
// error surfaces without a preceding "dry run: stream plan" line.
// The rendered lines themselves are unchanged.
func (s *Streamer) logDryRunPlan(ctx context.Context, streamID string, persisted ir.Position, found bool) error {
	plan, err := s.buildDryRunPlan(ctx, streamID, persisted, found)
	if err != nil {
		return err
	}
	if s.PlanSink != nil {
		s.PlanSink(plan)
		return nil
	}
	s.renderDryRunPlan(ctx, plan)
	return nil
}

// renderDryRunPlan emits the human slog rendering of a built plan.
func (s *Streamer) renderDryRunPlan(ctx context.Context, plan *StreamPlan) {
	slog.InfoContext(
		ctx, "dry run: stream plan",
		slog.String("source", plan.SourceEngine),
		slog.String("source_host", plan.SourceHost),
		slog.String("target", plan.TargetEngine),
		slog.String("target_host", plan.TargetHost),
		slog.String("stream_id", plan.StreamID),
	)
	if plan.WarmResume {
		slog.InfoContext(
			ctx, "dry run: warm resume from persisted position",
			slog.String("stream_id", plan.StreamID),
			slog.String("position_token", plan.PositionToken),
		)
		return
	}
	slog.InfoContext(
		ctx, "dry run: cold start — would capture snapshot, bulk-copy, then start CDC",
		slog.String("stream_id", plan.StreamID),
	)
	if plan.noSourceTables {
		slog.InfoContext(ctx, "dry run: source schema has no tables — nothing to stream")
		return
	}
	slog.InfoContext(
		ctx, "dry run: tables to bulk-copy and tail via CDC",
		slog.Int("tables", len(plan.Tables)),
	)
	for _, t := range plan.Tables {
		// secondary_indexes excludes the primary key (reported via
		// primary_key) — see migrate_plan.go logPlan for the rationale.
		slog.InfoContext(
			ctx, "dry run: table",
			slog.String("name", t.Name),
			slog.Int("columns", t.Columns),
			slog.Bool("primary_key", t.PrimaryKey),
			slog.Int("secondary_indexes", t.SecondaryIndexes),
			slog.Int("foreign_keys", t.ForeignKeys),
		)
	}
}

// truncateDryRunToken trims a position token to maxLen characters
// with an ellipsis when longer. Position tokens are JSON blobs that
// can run hundreds of bytes; the dry-run output stays scannable.
func truncateDryRunToken(token string, maxLen int) string {
	if len(token) <= maxLen {
		return token
	}
	return token[:maxLen-1] + "…"
}
