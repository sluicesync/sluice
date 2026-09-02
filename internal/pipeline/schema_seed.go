// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The CDC reader's prior-shape seed (SLM-1, audit 2026-09-01).
//
// A reader that refuses a value-changing schema delta needs the shape the
// table had BEFORE the delta. The MySQL lanes derived that from a memo
// their own boundary emitter wrote, which is empty at every table's first
// boundary after a start — so the first DDL per table per process was
// never checked, and Shape A forwards exactly that boundary. This file is
// the streamer's half of the fix: it knows the prior on both paths and
// hands it to the reader through [schemaSeedSetter] before the stream
// starts.
//
//   - Cold start: the RAW source IR the SchemaReader produced, captured in
//     [Streamer.coldStartPrepareSchema] before mappings and the Shape A
//     rewrite touch it (those rewrite types FOR THE TARGET).
//   - Warm resume: the retained schema-history version in effect at the
//     persisted position, read back from the applier's control table —
//     the schema the stream last committed under.
//
// The cold-start seed for the pipeline's own intercepts
// ([synthesizeColdStartSeedSnapshots]) is a different artifact with a
// different consumer: post-mapping, normalized through the engine's
// comparison lens, and shaped as SchemaSnapshots. The two are kept apart
// on purpose — a mapped type in the reader's prev would read as a phantom
// swap on every overridden column.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// retainedSchemaSeedLimit bounds the warm-resume history read behind
// [loadRetainedSchemaSeed]. Rows are per boundary, not per table, and the
// ADR-0049 true-delta gate keeps the count small in practice. Reaching the
// bound is WARNed as a possibly-incomplete seed, never treated as complete.
const retainedSchemaSeedLimit = 10_000

// rawReaderSchemaSeed lists the in-scope tables of the raw source schema
// as the reader's cold-start prior. The pointers are the SchemaReader's
// own: every later pass ([translate.ApplyMappings],
// [translate.ApplyExpressionOverrides], [translate.InjectShardColumn]) is
// copy-on-write, so they stay the untouched source projection.
func rawReaderSchemaSeed(schema *ir.Schema) []*ir.Table {
	if schema == nil {
		return nil
	}
	out := make([]*ir.Table, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		if t != nil {
			out = append(out, t)
		}
	}
	return out
}

// loadRetainedSchemaSeed builds the warm-resume prior from the applier's
// ADR-0049 schema history: for every table with retained versions, the
// version in effect at the persisted position. That is a real prior — the
// history row and the position it anchors are committed on one
// transaction (ADR-0049 #4a), so the latest retained version per table is
// at or before persisted.
//
// Resolution is per table through [ir.ResolveSchemaVersion] under the
// source's [ir.PositionOrderer] rather than "first row wins": the
// listing is ordered by a second-precision created_at, and two boundaries
// of one table inside one second tie. A table whose versions cannot be
// resolved (no orderer, an ambiguous partial order) resumes WITHOUT a
// prior — WARNed and stated, since the alternative is a guessed prev that
// could refuse a forwardable DDL.
//
// An applier without [ir.SchemaHistoryReader] yields no seed: the reader
// keeps the pre-SLM-1 first-boundary window on that pair, stated here
// rather than hidden. A read error is returned, not degraded: the resume
// is armed for a refusal whose prev this read supplies, and proceeding
// without it would reopen the window silently.
func loadRetainedSchemaSeed(ctx context.Context, applier ir.ChangeApplier, source ir.Engine, streamID string, persisted ir.Position) ([]*ir.Table, error) {
	reader, ok := applier.(ir.SchemaHistoryReader)
	if !ok {
		return nil, nil
	}
	rows, err := reader.ListSchemaHistory(ctx, streamID, retainedSchemaSeedLimit)
	if err != nil {
		return nil, fmt.Errorf("pipeline: load retained schema seed: %w", err)
	}
	if len(rows) >= retainedSchemaSeedLimit {
		slog.WarnContext(
			ctx, "warm resume: schema history reached the seed read bound; tables whose latest version lies past it resume without a prior shape",
			slog.String("stream_id", streamID),
			slog.Int("limit", retainedSchemaSeedLimit),
		)
	}
	orderer, _ := source.(ir.PositionOrderer)
	type tableKey struct{ schema, table string }
	var order []tableKey
	versions := map[tableKey][]ir.RetainedSchemaVersion{}
	for _, row := range rows {
		k := tableKey{row.SchemaName, row.TableName}
		if _, seen := versions[k]; !seen {
			order = append(order, k)
		}
		versions[k] = append(versions[k], ir.RetainedSchemaVersion{
			// The anchors were persisted by the same reader that stamped
			// the persisted position, so they share its engine name.
			Anchor:    ir.Position{Engine: persisted.Engine, Token: row.AnchorPosition},
			TableJSON: row.TableJSON,
		})
	}
	out := make([]*ir.Table, 0, len(order))
	for _, k := range order {
		tbl, err := resolveRetainedSeedTable(orderer, versions[k], persisted)
		if err != nil {
			if errors.Is(err, errRetainedSeedUnresolvable) {
				slog.WarnContext(
					ctx, "warm resume: retained schema versions for a table cannot be resolved at the persisted position; it resumes without a prior shape, so its FIRST schema boundary is not checked for a session-zone cast",
					slog.String("stream_id", streamID),
					slog.String("table", k.schema+"."+k.table),
					slog.String("reason", err.Error()),
				)
				continue
			}
			return nil, fmt.Errorf("pipeline: load retained schema seed: %s.%s: %w", k.schema, k.table, err)
		}
		if tbl.Schema == "" {
			tbl.Schema = k.schema
		}
		if tbl.Name == "" {
			tbl.Name = k.table
		}
		out = append(out, tbl)
	}
	return out, nil
}

// errRetainedSeedUnresolvable marks a table that has retained versions
// but no single one resolvable at the persisted position — the
// degrade-with-WARN arm of [loadRetainedSchemaSeed], distinct from a
// corrupt row, which is an error.
var errRetainedSeedUnresolvable = errors.New("no single retained version resolves at the persisted position")

// resolveRetainedSeedTable picks the retained version in effect at
// persisted. A single version needs no ordering; several do, and without
// an orderer (or with an ambiguous partial order) the answer is
// [errRetainedSeedUnresolvable] rather than a guess.
func resolveRetainedSeedTable(orderer ir.PositionOrderer, versions []ir.RetainedSchemaVersion, persisted ir.Position) (*ir.Table, error) {
	if len(versions) == 1 {
		tbl, err := ir.UnmarshalTable(versions[0].TableJSON)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			return nil, errors.New("retained schema version decodes to no table")
		}
		return tbl, nil
	}
	if orderer == nil {
		return nil, fmt.Errorf("%w: %d versions retained and the source engine implements no PositionOrderer", errRetainedSeedUnresolvable, len(versions))
	}
	tbl, err := ir.ResolveSchemaVersion(orderer, versions, persisted)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errRetainedSeedUnresolvable, err)
	}
	return tbl, nil
}

// wireReaderSchemaSeed hands the pending prior-shape seed to a reader that
// accepts one and clears it, so every attempt derives its own (a warm
// resume after a cold start in the same process must not inherit the
// cold-start seed — its prior is the retained history). Readers without
// the surface silently keep their old behaviour.
func (s *Streamer) wireReaderSchemaSeed(r ir.CDCReader) {
	seed := s.readerSchemaSeed
	s.readerSchemaSeed = nil
	if len(seed) == 0 {
		return
	}
	if setter, ok := r.(schemaSeedSetter); ok {
		setter.SetSchemaSeed(seed)
	}
}
