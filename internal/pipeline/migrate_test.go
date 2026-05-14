// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// captureSlog swaps slog.Default with a text handler writing into buf
// for the duration of the test, restoring the previous default on
// cleanup. Use it when an assertion needs to look at logged output.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return &buf
}

func TestRunValidates(t *testing.T) {
	cases := []struct {
		name string
		m    *Migrator
		want string
	}{
		{
			"nil source",
			&Migrator{Target: stubEngine{}, SourceDSN: "x", TargetDSN: "y"},
			"Source engine is nil",
		},
		{
			"nil target",
			&Migrator{Source: stubEngine{}, SourceDSN: "x", TargetDSN: "y"},
			"Target engine is nil",
		},
		{
			"empty source DSN",
			&Migrator{Source: stubEngine{}, Target: stubEngine{}, TargetDSN: "y"},
			"SourceDSN is empty",
		},
		{
			"empty target DSN",
			&Migrator{Source: stubEngine{}, Target: stubEngine{}, SourceDSN: "x"},
			"TargetDSN is empty",
		},
		{
			"resume + reset-target-data conflict",
			&Migrator{
				Source: stubEngine{}, Target: stubEngine{},
				SourceDSN: "x", TargetDSN: "y",
				Resume: true, ResetTargetData: true,
			},
			"--resume and --reset-target-data are mutually exclusive",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.m.Run(context.Background())
			if err == nil {
				t.Fatalf("expected error containing %q; got nil", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v; want contains %q", err, c.want)
			}
		})
	}
}

func TestRunEmptySchema(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = &ir.Schema{} // no tables
	tgt := newRecordingEngine("target")

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// No writers should have been opened.
	if tgt.openSchemaWriterCalls != 0 {
		t.Errorf("OpenSchemaWriter called %d times; want 0 (empty schema)", tgt.openSchemaWriterCalls)
	}
	if tgt.openRowWriterCalls != 0 {
		t.Errorf("OpenRowWriter called %d times; want 0", tgt.openRowWriterCalls)
	}
}

func TestRunDryRunDoesNotOpenWriters(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")

	logs := captureSlog(t)
	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		DryRun: true,
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if tgt.openSchemaWriterCalls != 0 {
		t.Errorf("OpenSchemaWriter called %d times in dry run; want 0", tgt.openSchemaWriterCalls)
	}
	if tgt.openRowWriterCalls != 0 {
		t.Errorf("OpenRowWriter called %d times in dry run; want 0", tgt.openRowWriterCalls)
	}
	if !strings.Contains(logs.String(), "dry run: migration plan") {
		t.Errorf("expected dry-run log to mention plan; got %q", logs.String())
	}
}

func TestRunCallsThreePhasesInOrder(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	wantPhases := []string{
		"CreateTablesWithoutConstraints",
		"WriteRows:users",
		"SyncIdentitySequences",
		"CreateIndexes",
		"CreateConstraints",
	}
	if len(tgt.phaseLog) != len(wantPhases) {
		t.Fatalf("got %d phases (%v); want %d", len(tgt.phaseLog), tgt.phaseLog, len(wantPhases))
	}
	for i, want := range wantPhases {
		if tgt.phaseLog[i] != want {
			t.Errorf("phase[%d] = %q; want %q", i, tgt.phaseLog[i], want)
		}
	}
}

// TestRunFilterPrunesTables exercises the orchestrator-side prune:
// with three source tables and an exclude filter that drops one,
// only the remaining two should be passed to the row writer.
func TestRunFilterPrunesTables(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			{Name: "orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			{Name: "audit_log", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
	tgt := newRecordingEngine("target")

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		Filter: TableFilter{Exclude: []string{"audit_*"}},
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	wantWrites := map[string]bool{
		"WriteRows:users":     true,
		"WriteRows:orders":    true,
		"WriteRows:audit_log": false,
	}
	got := map[string]bool{}
	for _, p := range tgt.phaseLog {
		if strings.HasPrefix(p, "WriteRows:") {
			got[p] = true
		}
	}
	for k, want := range wantWrites {
		if got[k] != want {
			t.Errorf("phaseLog has %q = %v; want %v", k, got[k], want)
		}
	}
}

// TestRunFilterEmptyResultErrors confirms a filter that excludes
// every source table surfaces a clear error rather than silently
// running a no-op migration.
func TestRunFilterEmptyResultErrors(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		Filter: TableFilter{Include: []string{"nonexistent"}},
	}
	err := m.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "every source table") {
		t.Errorf("err = %v; want a 'excluded every source table' message", err)
	}
}

func TestRunPropagatesReadSchemaError(t *testing.T) {
	src := newRecordingEngine("source")
	src.readSchemaErr = errors.New("connection refused")
	tgt := newRecordingEngine("target")

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
	}
	err := m.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("err = %v; want wrapping the schema-read error", err)
	}
}

// ---- mocks ----

// stubEngine is a placeholder ir.Engine for validation tests where Run
// shouldn't reach any of the Open* methods. Hitting them would be a
// regression in the validate-first ordering.
type stubEngine struct{}

func (stubEngine) Name() string                  { return "stub" }
func (stubEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (stubEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	panic("stubEngine.OpenSchemaReader called — Run should have failed validation first")
}

func (stubEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	panic("stubEngine.OpenSchemaWriter called")
}

func (stubEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	panic("stubEngine.OpenRowReader called")
}

func (stubEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	panic("stubEngine.OpenRowWriter called")
}

func (stubEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	panic("stubEngine.OpenCDCReader called")
}

func (stubEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	panic("stubEngine.OpenChangeApplier called")
}

func (stubEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	panic("stubEngine.OpenSnapshotStream called")
}

// recordingEngine is a fake ir.Engine that tracks which Open* methods
// were called and emits configurable readers/writers that record the
// orchestrator's interactions for assertion.
type recordingEngine struct {
	name                  string
	schema                *ir.Schema
	readSchemaErr         error
	openSchemaWriterCalls int
	openRowWriterCalls    int
	phaseLog              []string
}

func newRecordingEngine(name string) *recordingEngine {
	return &recordingEngine{name: name}
}

func (e *recordingEngine) Name() string                  { return e.name }
func (e *recordingEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *recordingEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	return &recordingSchemaReader{schema: e.schema, err: e.readSchemaErr}, nil
}

func (e *recordingEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	e.openSchemaWriterCalls++
	return &recordingSchemaWriter{phaseLog: &e.phaseLog}, nil
}

func (e *recordingEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	return &recordingRowReader{}, nil
}

func (e *recordingEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	e.openRowWriterCalls++
	return &recordingRowWriter{phaseLog: &e.phaseLog}, nil
}

func (*recordingEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("not implemented")
}

func (*recordingEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not implemented")
}

func (*recordingEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not implemented")
}

type recordingSchemaReader struct {
	schema *ir.Schema
	err    error
}

func (r *recordingSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.schema, nil
}

type recordingSchemaWriter struct {
	phaseLog *[]string
}

func (w *recordingSchemaWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	*w.phaseLog = append(*w.phaseLog, "CreateTablesWithoutConstraints")
	return nil
}

func (w *recordingSchemaWriter) CreateIndexes(context.Context, *ir.Schema) error {
	*w.phaseLog = append(*w.phaseLog, "CreateIndexes")
	return nil
}

func (w *recordingSchemaWriter) CreateConstraints(context.Context, *ir.Schema) error {
	*w.phaseLog = append(*w.phaseLog, "CreateConstraints")
	return nil
}

func (w *recordingSchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error {
	*w.phaseLog = append(*w.phaseLog, "SyncIdentitySequences")
	return nil
}

func (w *recordingSchemaWriter) CreateViews(context.Context, *ir.Schema) error {
	*w.phaseLog = append(*w.phaseLog, "CreateViews")
	return nil
}

type recordingRowReader struct{}

func (*recordingRowReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	close(ch) // no rows for these tests; orchestrator dispatch is the focus
	return ch, nil
}

type recordingRowWriter struct {
	phaseLog *[]string
}

func (w *recordingRowWriter) WriteRows(_ context.Context, table *ir.Table, _ <-chan ir.Row) error {
	*w.phaseLog = append(*w.phaseLog, "WriteRows:"+table.Name)
	return nil
}

func sampleSchema() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{
			{
				Name: "users",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
				},
			},
		},
	}
}

// recordingExecTimeoutSetter is a test double that records
// SetExecTimeout invocations. The orchestrator's [applyExecTimeout]
// helper is the unit under test; this lets us assert plumbing without
// instantiating a real engine.
type recordingExecTimeoutSetter struct {
	last  time.Duration
	calls int
}

func (r *recordingExecTimeoutSetter) SetExecTimeout(d time.Duration) {
	r.last = d
	r.calls++
}

// TestApplyExecTimeout pins the contract of the orchestrator's
// per-exec-timeout plumbing helper (GitHub #23 Phase B fix, v0.52.0):
//
//   - Zero / negative is a no-op (engines that don't want the setter
//     called keep their built-in default; the legacy unbounded
//     behaviour stays the default-default).
//   - Positive values call SetExecTimeout exactly once with the value.
//   - Non-setter targets pass through silently (engines that don't opt
//     into the optional surface degrade gracefully — same shape as
//     [applyMaxBufferBytes]).
func TestApplyExecTimeout(t *testing.T) {
	t.Run("zero is a no-op", func(t *testing.T) {
		r := &recordingExecTimeoutSetter{}
		applyExecTimeout(r, 0)
		if r.calls != 0 {
			t.Errorf("zero timeout: got %d calls; want 0", r.calls)
		}
	})

	t.Run("negative is a no-op", func(t *testing.T) {
		r := &recordingExecTimeoutSetter{}
		applyExecTimeout(r, -5*time.Second)
		if r.calls != 0 {
			t.Errorf("negative timeout: got %d calls; want 0", r.calls)
		}
	})

	t.Run("positive value sets exactly once", func(t *testing.T) {
		r := &recordingExecTimeoutSetter{}
		applyExecTimeout(r, 60*time.Second)
		if r.calls != 1 {
			t.Errorf("positive timeout: got %d calls; want 1", r.calls)
		}
		if r.last != 60*time.Second {
			t.Errorf("recorded duration = %v; want 60s", r.last)
		}
	})

	t.Run("non-setter target degrades silently", func(_ *testing.T) {
		applyExecTimeout(struct{}{}, 60*time.Second)
	})
}
