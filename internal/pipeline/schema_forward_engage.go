// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0058 — Streamer-side engage / release for single-stream ADD
// COLUMN forwarding.
//
// Mirrors the [engageShardCoordination] pattern: opens the resources
// the [interceptAddColumnForward] intercept needs and stores them on
// the Streamer; [closeAddColumnForward] releases them at runOnce
// teardown. No-op when [Streamer.ForwardSchemaAddColumn] is false or
// Shape A is engaged (Shape A's intercept handles ADD COLUMN already).

import (
	"context"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// engageAddColumnForward opens the target SchemaWriter the ADR-0058
// intercept uses for ALTER TABLE … ADD COLUMN, and (when backfill is
// requested) the source RowReader for the bounded backfill SELECT.
// Stores both on the Streamer; [closeAddColumnForward] releases them.
//
// Refuse-loudly cases:
//   - --forward-schema-add-column set AND --inject-shard-column set:
//     Shape A's boundary router already handles every shape; the
//     forward intercept would be a redundant second route. Log a
//     warning and skip — the operator's intent on Shape A is served
//     by Shape A's path.
//   - --forward-schema-add-column set AND target SchemaWriter does
//     not implement [ir.SchemaDeltaApplier]: refuse with a clear
//     engine-name + recovery message. Every shipping engine
//     implements it; this guard catches future engines added
//     without the surface.
//   - --backfill-added-column set AND source RowReader does not
//     implement [ir.BatchedRowReader]: refuse with the same shape.
//     Every shipping engine implements [ir.BatchedRowReader] via
//     ADR-0018.
//
// Idempotent: re-running with already-set fields is a no-op (the
// existing fields are reused; no double-close in cleanup).
func (s *Streamer) engageAddColumnForward(ctx context.Context) error {
	if !s.ForwardSchemaAddColumn {
		return nil
	}
	if s.InjectShardColumn.Engaged() {
		// Shape A's intercept already forwards ADD COLUMN via the
		// lease. The forward flag is a no-op in this combination;
		// log so the operator notices the redundant flag.
		return nil
	}
	if s.Target == nil {
		return fmt.Errorf("pipeline: engage add-column-forward: nil target engine")
	}
	if s.Source == nil {
		return fmt.Errorf("pipeline: engage add-column-forward: nil source engine")
	}
	if s.addColumnForwardWriter == nil {
		sw, err := s.Target.OpenSchemaWriter(ctx, s.TargetDSN)
		if err != nil {
			return fmt.Errorf("pipeline: engage add-column-forward: open schema writer: %w", err)
		}
		if _, ok := sw.(ir.SchemaDeltaApplier); !ok {
			_ = closeIfErrIgnored(sw)
			return s.refuseEngineMissingAddColumnForward("schema delta applier (AlterAddColumn)")
		}
		// Honor --target-schema if set so DDL emits to the right
		// namespace. Mirrors the Shape A engage path.
		if s.TargetSchema != "" {
			if setter, ok := sw.(ir.SchemaSetter); ok {
				setter.SetSchema(s.TargetSchema)
			}
		}
		s.addColumnForwardWriter = sw
	}
	// Bug 90 (v0.79.1): a source-side SchemaReader is required for the
	// ADR-0058 §2a volatility probe. pgoutput's RelationMessage and
	// MySQL's TableMapEvent both drop the DEFAULT clause, so the
	// intercept can't classify a computed DEFAULT from the CDC IR
	// alone. The reader is opened once per stream and shared across
	// every ADD COLUMN forward (the probe issues one ReadSchema per
	// forward — a rare event).
	if s.addColumnForwardSchemaReader == nil {
		sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
		if err != nil {
			_ = closeIfErrIgnored(s.addColumnForwardWriter)
			s.addColumnForwardWriter = nil
			return fmt.Errorf("pipeline: engage add-column-forward: open source schema reader: %w", err)
		}
		s.addColumnForwardSchemaReader = sr
	}
	if s.BackfillAddedColumn && s.addColumnForwardReader == nil {
		rr, err := s.Source.OpenRowReader(ctx, s.SourceDSN)
		if err != nil {
			_ = closeIfErrIgnored(s.addColumnForwardWriter)
			s.addColumnForwardWriter = nil
			return fmt.Errorf("pipeline: engage add-column-forward backfill: open row reader: %w", err)
		}
		if _, ok := rr.(ir.BatchedRowReader); !ok {
			_ = closeIfErrIgnored(rr)
			_ = closeIfErrIgnored(s.addColumnForwardWriter)
			s.addColumnForwardWriter = nil
			return s.refuseSourceMissingBatchedReader()
		}
		s.addColumnForwardReader = rr
	}
	return nil
}

// closeAddColumnForward releases the SchemaWriter + (optional)
// RowReader + SchemaReader opened by [engageAddColumnForward].
// Idempotent — safe to call on streams that never engaged.
func (s *Streamer) closeAddColumnForward() {
	if s == nil {
		return
	}
	if s.addColumnForwardWriter != nil {
		_ = closeIfErrIgnored(s.addColumnForwardWriter)
		s.addColumnForwardWriter = nil
	}
	if s.addColumnForwardReader != nil {
		_ = closeIfErrIgnored(s.addColumnForwardReader)
		s.addColumnForwardReader = nil
	}
	if s.addColumnForwardSchemaReader != nil {
		_ = closeIfErrIgnored(s.addColumnForwardSchemaReader)
		s.addColumnForwardSchemaReader = nil
	}
}

// RawDefaultReader is the optional interface engines may implement to
// surface the unprocessed source `column_default` text for a single
// column, bypassing any engine-side translation that would otherwise
// drop or normalise the expression.
//
// Why this exists (Bug 91 closure, v0.79.1): PG's schema-reader
// classifies `nextval(`-prefixed defaults as auto-increment and
// returns [ir.DefaultNone] for them — the AutoIncrement flag on
// [ir.Integer] is the canonical representation for SERIAL/BIGSERIAL
// columns. But a user-created `BIGINT DEFAULT nextval('manual_seq')`
// (without OWNED BY) is structurally identical at column_default
// level and gets the same treatment, hiding the volatility from the
// ADR-0058 §2a classifier. A raw read is the cheapest correct probe:
// it bypasses translateDefault entirely so the classifier sees the
// original expression text. MySQL's translateDefault preserves the
// expression text for DEFAULT_GENERATED columns, so this interface is
// PG-only at the moment; an engine that doesn't implement it falls
// back to the IR-translated probe (which is correct for that engine's
// translation).
//
// Returns (rawText, hasDefault, err):
//   - rawText: the verbatim text PG returns in
//     information_schema.columns.column_default (may include type
//     casts, schema qualifiers, etc.).
//   - hasDefault: false when the column has no DEFAULT clause; the
//     classifier treats this as safe.
//   - err: non-nil when the probe failed (column not found, query
//     error). The caller's refuse-on-uncertainty wrapper escalates.
type RawDefaultReader interface {
	ReadRawColumnDefault(ctx context.Context, schema, table, column string) (rawText string, hasDefault bool, err error)
}

// newSourceDefaultProber returns a [defaultProberFunc] backed by the
// given source [ir.SchemaReader]. The closure issues a single
// targeted ReadSchema() per call and walks the result to find the
// column's [ir.DefaultValue].
//
// When the reader also implements [RawDefaultReader] (PG does), the
// closure prefers a raw-text probe — necessary because PG's
// schema-reader collapses `nextval(`-prefixed defaults to
// [ir.DefaultNone] under the AutoIncrement heuristic, hiding
// volatile sequence defaults from the classifier (Bug 91).
//
// This is wasteful at scale (ReadSchema reads every table the source
// exposes), but the intercept calls it at most once per ADD COLUMN
// forward — a rare event. A future refinement could add a
// per-column probe interface to [ir.SchemaReader]; until then,
// ReadSchema is the only available surface every shipping engine
// implements.
//
// Bug 90 (v0.79.1): probing the source is the only way to surface
// the canonical DEFAULT text — pgoutput's RelationMessage and
// MySQL's TableMapEvent both drop the field, so the in-band CDC IR
// cannot be the source of truth for ADR-0058 §2a's volatility
// classification.
func newSourceDefaultProber(sr ir.SchemaReader) defaultProberFunc {
	rawReader, hasRaw := sr.(RawDefaultReader)
	return func(ctx context.Context, schema, table, column string) (ir.DefaultValue, error) {
		// Bug 91: prefer the raw-text probe when the engine supports it.
		// PG's translateDefault collapses any `nextval(`-prefixed default
		// to [ir.DefaultNone] under the SERIAL auto-increment heuristic;
		// the IR-translated probe path therefore sees DefaultNone for
		// user-written sequence defaults and the classifier passes.
		// Reading the verbatim column_default text bypasses that
		// translation so the volatility classifier sees the original
		// expression.
		if hasRaw {
			raw, hasDefault, err := rawReader.ReadRawColumnDefault(ctx, schema, table, column)
			if err != nil {
				return nil, fmt.Errorf("read raw column default: %w", err)
			}
			if !hasDefault {
				return ir.DefaultNone{}, nil
			}
			return ir.DefaultExpression{Expr: raw}, nil
		}

		sch, err := sr.ReadSchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("read source schema: %w", err)
		}
		for _, t := range sch.Tables {
			if t == nil {
				continue
			}
			// Match on table name; schema may be empty on MySQL
			// (the SchemaReader convention) so prefer name-match
			// when the caller's schema is empty OR the catalog's
			// schema is empty.
			if t.Name != table {
				continue
			}
			if schema != "" && t.Schema != "" && schema != t.Schema {
				continue
			}
			for _, c := range t.Columns {
				if c == nil || c.Name != column {
					continue
				}
				if c.Default == nil {
					return ir.DefaultNone{}, nil
				}
				return c.Default, nil
			}
		}
		// Column not found — surface as a probe error so the
		// intercept refuses-on-uncertainty rather than silently
		// passing.
		return nil, fmt.Errorf("column %q on %q.%q not present in source catalog",
			column, schema, table)
	}
}

// refuseEngineMissingAddColumnForward is the shared shape of the
// "target engine doesn't implement X" refusal for the ADR-0058
// engagement path. Names the missing surface + the operator-
// actionable recovery (drop --forward-schema-add-column to fall back
// to the loud-failure-on-DDL behavior).
func (s *Streamer) refuseEngineMissingAddColumnForward(missingSurface string) error {
	engineName := ""
	if s.Target != nil {
		engineName = s.Target.Name()
	}
	return fmt.Errorf(
		"pipeline: target engine %q does not implement online ADD COLUMN "+
			"forwarding (missing %s, ADR-0058). Recovery: drop "+
			"--forward-schema-add-column to use the drained model "+
			"(stop the stream, run schema migrate, resume)",
		engineName, missingSurface,
	)
}

// refuseSourceMissingBatchedReader is the corresponding refusal when
// --backfill-added-column is set but the source engine's RowReader
// doesn't implement [ir.BatchedRowReader] (the PK-cursor surface from
// ADR-0018). Names the source engine + the recovery flag (drop the
// backfill flag; forwarding still works, just without per-row
// repopulation of already-shipped rows).
func (s *Streamer) refuseSourceMissingBatchedReader() error {
	engineName := ""
	if s.Source != nil {
		engineName = s.Source.Name()
	}
	return fmt.Errorf(
		"pipeline: source engine %q does not implement BatchedRowReader "+
			"(ADR-0018), required by --backfill-added-column. Recovery: "+
			"drop --backfill-added-column; the target ALTER still forwards "+
			"and existing target rows carry the column's DEFAULT (NULL if "+
			"none)",
		engineName,
	)
}
