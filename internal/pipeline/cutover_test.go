// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestCutover_ValidatesInputs pins the validate-first ordering — any
// nil engine / empty DSN / negative margin (when ever reached) should
// fail before any Open* call lands.
func TestCutover_ValidatesInputs(t *testing.T) {
	cases := []struct {
		name string
		c    *Cutover
		want string
	}{
		{
			"nil source",
			&Cutover{Target: stubEngine{}, SourceDSN: "x", TargetDSN: "y"},
			"source engine is nil",
		},
		{
			"nil target",
			&Cutover{Source: stubEngine{}, SourceDSN: "x", TargetDSN: "y"},
			"target engine is nil",
		},
		{
			"empty source DSN",
			&Cutover{Source: stubEngine{}, Target: stubEngine{}, TargetDSN: "y"},
			"source DSN is empty",
		},
		{
			"empty target DSN",
			&Cutover{Source: stubEngine{}, Target: stubEngine{}, SourceDSN: "x"},
			"target DSN is empty",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.c.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v; want substring %q", err, tc.want)
			}
		})
	}
}

// TestCutover_RefusesEngineWithoutSequenceStateReader pins the loud
// refusal when the source engine doesn't implement
// [ir.SequenceStateReader]. Critical: the failure mode should NOT be
// a silent noop — operators on engines without sequence support need
// a clear "engine X does not support cutover sequence priming"
// error.
func TestCutover_RefusesEngineWithoutSequenceStateReader(t *testing.T) {
	// cutoverEngine with a plain reader that has no SequenceStateReader.
	src := &cutoverEngine{
		name:   "src",
		reader: &cutoverPlainReader{schema: &ir.Schema{}},
	}
	tgt := &cutoverEngine{
		name:   "tgt",
		writer: &cutoverPrimerWriter{},
	}
	c := &Cutover{
		Source:    src,
		Target:    tgt,
		SourceDSN: "x",
		TargetDSN: "y",
	}
	_, err := c.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "does not implement SequenceStateReader") {
		t.Errorf("err = %v; want SequenceStateReader-refusal error", err)
	}
}

// TestCutover_RefusesEngineWithoutSequencePrimer pins the corollary
// for the target side — engine without [ir.SequencePrimer] must
// refuse loudly.
func TestCutover_RefusesEngineWithoutSequencePrimer(t *testing.T) {
	src := &cutoverEngine{
		name:   "src",
		reader: &cutoverStateReader{schema: &ir.Schema{}, states: nil},
	}
	tgt := &cutoverEngine{
		name:   "tgt",
		writer: &cutoverPlainWriter{},
	}
	c := &Cutover{
		Source:    src,
		Target:    tgt,
		SourceDSN: "x",
		TargetDSN: "y",
	}
	_, err := c.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "does not implement SequencePrimer") {
		t.Errorf("err = %v; want SequencePrimer-refusal error", err)
	}
}

// TestCutover_RoutesStateThroughOrchestrator pins the full happy-path
// dispatch: source ReadSchema → ReadSequenceState → target
// PrimeSequences with margin threaded through.
func TestCutover_RoutesStateThroughOrchestrator(t *testing.T) {
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}}}},
		},
	}
	states := []ir.SequenceState{{Table: "orders", Column: "id", Value: 12345}}
	src := &cutoverEngine{
		name:   "src",
		reader: &cutoverStateReader{schema: schema, states: states},
	}
	primer := &cutoverPrimerWriter{}
	tgt := &cutoverEngine{name: "tgt", writer: primer}

	c := &Cutover{
		Source:    src,
		Target:    tgt,
		SourceDSN: "x",
		TargetDSN: "y",
		Margin:    100,
	}
	report, err := c.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned err = %v; want nil", err)
	}
	if primer.calls != 1 {
		t.Errorf("PrimeSequences calls = %d; want 1", primer.calls)
	}
	if primer.lastMargin != 100 {
		t.Errorf("primer received margin = %d; want 100", primer.lastMargin)
	}
	if len(primer.lastStates) != 1 || primer.lastStates[0].Value != 12345 {
		t.Errorf("primer received states = %+v; want one entry with Value=12345", primer.lastStates)
	}
	if report == nil {
		t.Fatal("report = nil; want non-nil report")
	}
}

// TestCutover_NormalizesMargin pins the orchestrator's default-margin
// normalisation: a zero or negative margin must be replaced with the
// IR contract's default (1000), not passed verbatim to the engine
// (the engine itself also normalises, but defence in depth matters
// for the "operator typo'd -1" class).
func TestCutover_NormalizesMargin(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{}}
	src := &cutoverEngine{name: "src", reader: &cutoverStateReader{schema: schema}}
	primer := &cutoverPrimerWriter{}
	tgt := &cutoverEngine{name: "tgt", writer: primer}

	c := &Cutover{
		Source:    src,
		Target:    tgt,
		SourceDSN: "x",
		TargetDSN: "y",
		Margin:    0,
	}
	if _, err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run err = %v", err)
	}
	if primer.lastMargin != ir.CutoverSequenceMarginDefault {
		t.Errorf("primer received margin = %d; want default %d",
			primer.lastMargin, ir.CutoverSequenceMarginDefault)
	}
}

// TestCutover_PropagatesRefusalError pins the loud-failure path: when
// the engine surfaces [ir.ErrCutoverSequenceTargetAhead], the
// orchestrator returns it verbatim AND returns the report so the CLI
// can render per-table detail before exiting non-zero.
func TestCutover_PropagatesRefusalError(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{}}
	src := &cutoverEngine{name: "src", reader: &cutoverStateReader{schema: schema}}
	primer := &cutoverPrimerWriter{
		returnErr: ir.ErrCutoverSequenceTargetAhead,
		returnReport: &ir.SequencePrimeReport{
			Actions: []ir.SequencePrimeAction{
				{Table: "users", Column: "id", Outcome: "refused", Reason: "ahead"},
			},
		},
	}
	tgt := &cutoverEngine{name: "tgt", writer: primer}

	c := &Cutover{Source: src, Target: tgt, SourceDSN: "x", TargetDSN: "y"}
	report, err := c.Run(context.Background())
	if !errors.Is(err, ir.ErrCutoverSequenceTargetAhead) {
		t.Errorf("err = %v; want ErrCutoverSequenceTargetAhead", err)
	}
	if report == nil || len(report.Actions) != 1 || report.Actions[0].Outcome != "refused" {
		t.Errorf("report = %+v; want one refused action", report)
	}
}

// TestCutover_AppliesTargetSchema pins the ADR-0031 plumbing — when
// TargetSchema is set, the target writer's SchemaSetter must be
// called before PrimeSequences runs.
func TestCutover_AppliesTargetSchema(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{}}
	src := &cutoverEngine{name: "src", reader: &cutoverStateReader{schema: schema}}
	primer := &cutoverPrimerWriter{}
	tgt := &cutoverEngine{name: "tgt", writer: primer}

	c := &Cutover{
		Source:       src,
		Target:       tgt,
		SourceDSN:    "x",
		TargetDSN:    "y",
		TargetSchema: "analytics",
	}
	if _, err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run err = %v", err)
	}
	if primer.appliedSchema != "analytics" {
		t.Errorf("SetSchema not threaded; primer.appliedSchema = %q want %q",
			primer.appliedSchema, "analytics")
	}
}

// TestCutover_FilterScopesSchema pins the table-filter integration —
// operator-supplied --include-table / --exclude-table must scope the
// schema fed to the engine's primer.
func TestCutover_FilterScopesSchema(t *testing.T) {
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}}}},
			{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}}}},
			{Name: "internal_audit", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}}}},
		},
	}
	src := &cutoverEngine{name: "src", reader: &cutoverStateReader{schema: schema}}
	primer := &cutoverPrimerWriter{}
	tgt := &cutoverEngine{name: "tgt", writer: primer}

	filter, err := NewTableFilter(nil, []string{"internal_audit"})
	if err != nil {
		t.Fatalf("NewTableFilter err = %v", err)
	}

	c := &Cutover{
		Source:    src,
		Target:    tgt,
		SourceDSN: "x",
		TargetDSN: "y",
		Filter:    filter,
	}
	if _, err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run err = %v", err)
	}
	// The filter passes through to the source state reader's view of
	// schema. The primer receives the filtered schema; we check its
	// table count.
	if primer.lastSchema == nil {
		t.Fatal("primer.lastSchema = nil")
	}
	if len(primer.lastSchema.Tables) != 2 {
		t.Errorf("primer.lastSchema.Tables count = %d; want 2 (orders, users)",
			len(primer.lastSchema.Tables))
	}
	for _, tbl := range primer.lastSchema.Tables {
		if tbl.Name == "internal_audit" {
			t.Errorf("primer received internal_audit despite exclude filter")
		}
	}
}

// ---- mocks ----

// cutoverEngine is a configurable ir.Engine for cutover tests. Each
// Open* returns the pre-configured reader/writer (or panics if the
// caller didn't supply one — surfaces a regression in the
// orchestrator's ordering).
type cutoverEngine struct {
	name   string
	reader ir.SchemaReader
	writer ir.SchemaWriter
}

func (e *cutoverEngine) Name() string                  { return e.name }
func (e *cutoverEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *cutoverEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	if e.reader == nil {
		return nil, errors.New("cutoverEngine: no reader configured")
	}
	return e.reader, nil
}

func (e *cutoverEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	if e.writer == nil {
		return nil, errors.New("cutoverEngine: no writer configured")
	}
	return e.writer, nil
}

func (*cutoverEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("not implemented")
}

func (*cutoverEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errors.New("not implemented")
}

func (*cutoverEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("not implemented")
}

func (*cutoverEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not implemented")
}

func (*cutoverEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not implemented")
}

// cutoverPlainReader is a SchemaReader without SequenceStateReader.
type cutoverPlainReader struct {
	schema *ir.Schema
}

func (r *cutoverPlainReader) ReadSchema(context.Context) (*ir.Schema, error) { return r.schema, nil }
func (r *cutoverPlainReader) Close() error                                   { return nil }

// cutoverStateReader implements both SchemaReader AND
// SequenceStateReader.
type cutoverStateReader struct {
	schema *ir.Schema
	states []ir.SequenceState
	err    error
}

func (r *cutoverStateReader) ReadSchema(context.Context) (*ir.Schema, error) {
	return r.schema, nil
}

func (r *cutoverStateReader) ReadSequenceState(_ context.Context, _ *ir.Schema) ([]ir.SequenceState, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.states, nil
}

func (r *cutoverStateReader) Close() error { return nil }

// cutoverPlainWriter is a SchemaWriter without SequencePrimer.
type cutoverPlainWriter struct{}

func (*cutoverPlainWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	return nil
}
func (*cutoverPlainWriter) CreateIndexes(context.Context, *ir.Schema) error     { return nil }
func (*cutoverPlainWriter) CreateConstraints(context.Context, *ir.Schema) error { return nil }
func (*cutoverPlainWriter) SyncIdentitySequences(context.Context, *ir.Schema) error {
	return nil
}
func (*cutoverPlainWriter) CreateViews(context.Context, *ir.Schema) error { return nil }
func (*cutoverPlainWriter) Close() error                                  { return nil }

// cutoverPrimerWriter implements SchemaWriter, SequencePrimer, and
// SchemaSetter. Records what the orchestrator threaded through.
type cutoverPrimerWriter struct {
	calls         int
	lastMargin    int64
	lastStates    []ir.SequenceState
	lastSchema    *ir.Schema
	appliedSchema string
	returnErr     error
	returnReport  *ir.SequencePrimeReport
}

func (*cutoverPrimerWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	return nil
}
func (*cutoverPrimerWriter) CreateIndexes(context.Context, *ir.Schema) error     { return nil }
func (*cutoverPrimerWriter) CreateConstraints(context.Context, *ir.Schema) error { return nil }
func (*cutoverPrimerWriter) SyncIdentitySequences(context.Context, *ir.Schema) error {
	return nil
}
func (*cutoverPrimerWriter) CreateViews(context.Context, *ir.Schema) error { return nil }
func (*cutoverPrimerWriter) Close() error                                  { return nil }

func (w *cutoverPrimerWriter) SetSchema(name string) { w.appliedSchema = name }

func (w *cutoverPrimerWriter) PrimeSequences(
	_ context.Context,
	schema *ir.Schema,
	states []ir.SequenceState,
	margin int64,
) (*ir.SequencePrimeReport, error) {
	w.calls++
	w.lastMargin = margin
	w.lastStates = states
	w.lastSchema = schema
	if w.returnReport != nil {
		return w.returnReport, w.returnErr
	}
	if w.returnErr != nil {
		return &ir.SequencePrimeReport{}, w.returnErr
	}
	return &ir.SequencePrimeReport{}, nil
}

// Compile-time guards: ensure our mocks implement the expected
// interfaces.
var (
	_ ir.SchemaReader        = (*cutoverPlainReader)(nil)
	_ ir.SchemaReader        = (*cutoverStateReader)(nil)
	_ ir.SequenceStateReader = (*cutoverStateReader)(nil)
	_ ir.SchemaWriter        = (*cutoverPlainWriter)(nil)
	_ ir.SchemaWriter        = (*cutoverPrimerWriter)(nil)
	_ ir.SequencePrimer      = (*cutoverPrimerWriter)(nil)
	_ ir.SchemaSetter        = (*cutoverPrimerWriter)(nil)
	_ io.Closer              = (*cutoverPrimerWriter)(nil)
)
