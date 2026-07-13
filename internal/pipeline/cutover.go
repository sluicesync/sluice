// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/progress"
)

// Cutover pretty-view phases (ADR-0155 phase 2). Cutover keeps its
// historical stdout report (rendered by the CLI) on the non-TTY path —
// the sink is [progress.Nop] there and this checklist drives only the
// interactive view; the summary panel replaces the stdout report when
// the live view owns the terminal.
var (
	cutoverPhaseConnect = progress.Phase{Key: "connect", Label: "Connect"}
	cutoverPhaseRead    = progress.Phase{Key: "read", Label: "Read"}
	cutoverPhasePrime   = progress.Phase{Key: "prime", Label: "Prime"}
)

// CutoverProgressSpec is the pretty-view [progress.Spec] for `sluice
// cutover`. The CLI hands it to [progress.NewTTYSink].
var CutoverProgressSpec = progress.Spec{
	Title:      "sluice cutover",
	Phases:     []progress.Phase{cutoverPhaseConnect, cutoverPhaseRead, cutoverPhasePrime},
	LabelWidth: 12,
}

// Cutover drives the two-phase cutover sequence priming pass —
// severity-A finding F10 of the 2026-05-22 Reddit-research run, see
// ADR-0062.
//
// At cutover (operator-signaled, never auto-triggered), the orchestrator:
//
//  1. Opens a fresh source SchemaReader and target SchemaWriter from
//     the supplied engine pair + DSNs.
//  2. Reads the source schema (the IR contract for "which identity
//     columns exist").
//  3. Type-asserts the source reader to [ir.SequenceStateReader] and
//     reads the source's current sequence / AUTO_INCREMENT state.
//  4. Type-asserts the target writer to [ir.SequencePrimer] and
//     applies the state with the operator-supplied safety margin.
//  5. Returns the per-table [ir.SequencePrimeReport] and a top-level
//     error (non-nil when any table refused loudly).
//
// **No CDC pause / lock.** The two-phase shape is deliberately
// quiet: it doesn't require operators to suspend the CDC stream or
// acquire any catalog locks. F10 v1 trades a tiny window of
// concurrent source-side writes (between the read and the apply) for
// operational simplicity — the `--cutover-sequence-margin` knob
// absorbs that drift.
//
// **Refuse loudly on capability mismatch.** Engines without
// SequenceStateReader (source side) or SequencePrimer (target side)
// surface a clear error rather than silently no-op. The error
// message names the unsupported engine so the operator can verify
// the version / flavour combination.
type Cutover struct {
	Source ir.Engine
	Target ir.Engine

	SourceDSN string
	TargetDSN string

	// Margin is the operator-supplied safety margin (added to every
	// source sequence value before applying). Zero or negative is
	// normalised to [ir.CutoverSequenceMarginDefault].
	Margin int64

	// Filter restricts which tables participate in the priming pass.
	// The zero value matches every table in the source schema.
	Filter migcore.TableFilter

	// TargetSchema mirrors the migrator's per-source-namespace flag
	// for Postgres targets (ADR-0031). Threaded to the SchemaWriter
	// via [ir.SchemaSetter] so the cutover's pg_get_serial_sequence
	// lookups hit the namespace the migration landed in.
	TargetSchema string

	// Progress is the ADR-0155 presentation sink. nil is the [progress.Nop]
	// default (cutover's own report, rendered by the CLI, is the
	// byte-identical non-TTY output); the CLI sets a [progress.TTYSink] only
	// for an interactive terminal, where the summary panel replaces the
	// stdout report.
	Progress progress.Sink
}

// sink returns the presentation sink, defaulting a nil Progress to the
// no-op sink so every call-site is nil-free.
func (c *Cutover) sink() progress.Sink {
	if c.Progress == nil {
		return progress.Nop{}
	}
	return c.Progress
}

// Run executes the priming pass. Returns the per-table report
// (always non-nil unless one of the open / read steps failed) and an
// error describing top-level failures or per-table refusals.
//
// Lifecycle:
//   - Engine resolution + capability gating happens first.
//   - Source schema read uses the source's [ir.SchemaReader].
//   - The per-engine readers / writers carry their own connection
//     lifecycle (Close on the returned reader / writer when they
//     implement [io.Closer]).
//
// Concurrency-adjacent code paths are per-engine (engines own the
// SQL surface); the orchestrator itself is single-goroutine and the
// `-race` integration gate covers the engine implementations.
func (c *Cutover) Run(ctx context.Context) (*ir.SequencePrimeReport, error) {
	if c.Source == nil {
		return nil, errors.New("cutover: source engine is nil")
	}
	if c.Target == nil {
		return nil, errors.New("cutover: target engine is nil")
	}
	if c.SourceDSN == "" {
		return nil, errors.New("cutover: source DSN is empty")
	}
	if c.TargetDSN == "" {
		return nil, errors.New("cutover: target DSN is empty")
	}

	margin := c.Margin
	if margin <= 0 {
		margin = ir.CutoverSequenceMarginDefault
	}

	sink := c.sink()
	sink.PhaseStarted(cutoverPhaseConnect)

	// Open source SchemaReader.
	sourceReader, err := c.Source.OpenSchemaReader(ctx, c.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("cutover: open source schema reader: %w", err)
	}
	defer closeIfPossible(sourceReader)

	stateReader, ok := sourceReader.(ir.SequenceStateReader)
	if !ok {
		return nil, fmt.Errorf("cutover: source engine %q does not implement SequenceStateReader (sequence/AUTO_INCREMENT priming unsupported)",
			c.Source.Name())
	}

	// Open target SchemaWriter.
	targetWriter, err := c.Target.OpenSchemaWriter(ctx, c.TargetDSN)
	if err != nil {
		return nil, fmt.Errorf("cutover: open target schema writer: %w", err)
	}
	defer closeIfPossible(targetWriter)

	// Thread per-source target schema namespace through the writer
	// (ADR-0031), so pg_get_serial_sequence resolves against the
	// migration's target namespace rather than the DSN default.
	if c.TargetSchema != "" {
		if setter, ok := targetWriter.(ir.SchemaSetter); ok {
			setter.SetSchema(c.TargetSchema)
		}
	}

	primer, ok := targetWriter.(ir.SequencePrimer)
	if !ok {
		return nil, fmt.Errorf("cutover: target engine %q does not implement SequencePrimer (sequence/AUTO_INCREMENT priming unsupported)",
			c.Target.Name())
	}
	sink.PhaseCompleted(cutoverPhaseConnect)
	sink.PhaseStarted(cutoverPhaseRead)

	// Read source schema. Applying the operator-supplied table
	// filter at this layer keeps the per-engine source reader's
	// pg_get_serial_sequence / I_S.TABLES walk aligned with the rest
	// of the sluice toolchain — operators who excluded a table from
	// migrate also exclude it from cutover.
	schema, err := sourceReader.ReadSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("cutover: read source schema: %w", err)
	}
	schema = filterSchemaTables(schema, c.Filter)

	// Source-side: read sequence states.
	sourceStates, err := stateReader.ReadSequenceState(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("cutover: read source sequence state: %w", err)
	}

	slog.Info(
		"cutover: source sequence state captured",
		slog.String("source_engine", c.Source.Name()),
		slog.String("target_engine", c.Target.Name()),
		slog.Int("sequence_count", len(sourceStates)),
		slog.Int64("margin", margin),
	)
	sink.PhaseCompleted(cutoverPhaseRead)
	sink.PhaseStarted(cutoverPhasePrime)

	// Target-side: apply the priming pass.
	report, primeErr := primer.PrimeSequences(ctx, schema, sourceStates, margin)
	if report == nil {
		report = &ir.SequencePrimeReport{}
	}

	// Engine surfaces the refusal class via ErrCutoverSequenceTargetAhead;
	// the orchestrator propagates that verbatim so the CLI can branch
	// on it for the exit-code-2 path.
	if primeErr != nil {
		return report, primeErr
	}
	sink.PhaseCompleted(cutoverPhasePrime)

	// ADR-0155 summary panel (TTY only; Nop ignores it). The CLI's stdout
	// report is the byte-identical non-TTY output; these Fields are the
	// interactive rollup that replaces it under the live view.
	primed, noop, skipped, refused := report.Counts()
	sink.Summary(progress.Result{Fields: []progress.Field{
		{Label: "Engines", Value: c.Source.Name() + " -> " + c.Target.Name()},
		{Label: "Primed", Value: progress.HumanCount(int64(primed))},
		{Label: "Noop", Value: progress.HumanCount(int64(noop))},
		{Label: "Skipped", Value: progress.HumanCount(int64(skipped))},
		{Label: "Refused", Value: progress.HumanCount(int64(refused))},
	}})
	return report, nil
}

// filterSchemaTables returns a shallow-copied schema with non-matching
// tables filtered out. An empty filter returns schema verbatim.
func filterSchemaTables(schema *ir.Schema, filter migcore.TableFilter) *ir.Schema {
	if schema == nil {
		return nil
	}
	if filter.IsEmpty() {
		return schema
	}
	out := &ir.Schema{
		Tables:    make([]*ir.Table, 0, len(schema.Tables)),
		Views:     schema.Views,
		Sequences: schema.Sequences,
	}
	for _, t := range schema.Tables {
		if t == nil {
			continue
		}
		if filter.Allows(t.Name) {
			out.Tables = append(out.Tables, t)
		}
	}
	return out
}

// closeIfPossible calls Close on v when v implements [io.Closer].
// Used by the cutover orchestrator's defer-close path because the
// SchemaReader / SchemaWriter interfaces don't require Close (engines
// that don't implement it have nothing to release).
func closeIfPossible(v any) {
	if c, ok := v.(io.Closer); ok {
		_ = c.Close()
	}
}
