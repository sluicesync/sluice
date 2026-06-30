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

// logDryRunPlan describes what Run would do without doing it via
// structured slog records. Cold-start logs the source schema summary
// so operators can catch missing-tables / unexpected-column-counts
// before the migration starts; warm-resume logs the persisted
// position token (truncated for readability) so operators can see
// whether the stream is positioned where they expect.
//
// The source schema read for cold-start is the only source-side
// touch the dry-run does — same level of access the regular
// cold-start would do, just without then opening the snapshot
// stream or starting CDC.
func (s *Streamer) logDryRunPlan(ctx context.Context, streamID string, persisted ir.Position, found bool) error {
	slog.InfoContext(
		ctx, "dry run: stream plan",
		slog.String("source", s.Source.Name()),
		slog.String("source_host", redactedHost(s.SourceDSN)),
		slog.String("target", s.Target.Name()),
		slog.String("target_host", redactedHost(s.TargetDSN)),
		slog.String("stream_id", streamID),
	)
	if found {
		slog.InfoContext(
			ctx, "dry run: warm resume from persisted position",
			slog.String("stream_id", streamID),
			slog.String("position_token", truncateDryRunToken(persisted.Token, 80)),
		)
		return nil
	}
	slog.InfoContext(
		ctx, "dry run: cold start — would capture snapshot, bulk-copy, then start CDC",
		slog.String("stream_id", streamID),
	)

	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: open source schema reader: %w", err))
	}
	defer closeIf(sr)
	if err := applyEnabledPGExtensions(ctx, sr, s.EnabledPGExtensions); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: enable PG extensions on source: %w", err))
	}
	// ADR-0047 tier (b): live PG → PG sync may carry uncatalogued
	// extension types verbatim. Engine-name-only determination.
	applyVerbatimExtensionPassthrough(sr, verbatimLiveSameEnginePG(s.Source, s.Target))
	// catalog Bug 76: scope per-column type validation to the filtered
	// table set (s.Filter already has engine defaults merged in Run).
	applyTableScope(sr, s.Filter)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "dry run: source schema has no tables — nothing to stream")
		return nil
	}
	if err := applyTableFilter(ctx, schema, s.Filter); err != nil {
		return err
	}
	applyViewFilter(ctx, schema, s.ViewFilter, s.SkipViews)
	// ADR-0143: mirror the cold-start ORM-table skip so the dry-run preview
	// honestly reflects what a real run would copy.
	applyORMTableSkip(ctx, schema, s.SkipORMTables, s.Filter)
	mapped, err := translate.ApplyMappings(schema, s.Mappings)
	if err != nil {
		return fmt.Errorf("pipeline: dry-run: apply mappings: %w", err)
	}
	if _, err := translate.ApplyExpressionOverrides(mapped, s.ExpressionMappings); err != nil {
		return fmt.Errorf("pipeline: dry-run: apply expression overrides: %w", err)
	}
	slog.InfoContext(
		ctx, "dry run: tables to bulk-copy and tail via CDC",
		slog.Int("tables", len(schema.Tables)),
	)
	for _, t := range schema.Tables {
		// secondary_indexes excludes the primary key (reported via
		// primary_key) — see migrate.go logPlan for the rationale.
		slog.InfoContext(
			ctx, "dry run: table",
			slog.String("name", t.Name),
			slog.Int("columns", len(t.Columns)),
			slog.Bool("primary_key", t.PrimaryKey != nil),
			slog.Int("secondary_indexes", len(t.Indexes)),
			slog.Int("foreign_keys", len(t.ForeignKeys)),
		)
	}
	return nil
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
