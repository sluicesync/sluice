// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// safeBuffer is a mutex-protected [bytes.Buffer] wrapper used as the
// slog handler's writer in [captureSlog]. Without the mutex, concurrent
// goroutines writing to slog.Default (streamer + CDC pump +
// go-mysql-org binlogsyncer all log from background goroutines while
// the test reads buf.String()) race on the underlying Buffer growth —
// caught by CI run 26134035839 in
// TestBackup_RecordsEndPosition_MySQLIntegration after Chunk E's new
// pin pulled the binlogsyncer-while-test-reads pattern into a -race
// run. The race was latent: present since the helper landed, exposed
// only by the longer-running streamer goroutines Chunk E exercised.
//
// API-compatible with [*bytes.Buffer] for the methods existing callers
// use (.String, .Bytes, .Len, .Write). .Bytes returns a defensive copy
// so the caller can read without holding the mutex (and writes that
// race the read can't corrupt the returned slice).
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *safeBuffer) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Defensive copy — internal slice may grow concurrently with the
	// caller's use of the returned bytes.
	out := make([]byte, s.buf.Len())
	copy(out, s.buf.Bytes())
	return out
}

func (s *safeBuffer) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Len()
}

// captureSlog swaps slog.Default with a text handler writing into a
// thread-safe buffer for the duration of the test, restoring the
// previous default on cleanup. Use it when an assertion needs to look
// at logged output.
func captureSlog(t *testing.T) *safeBuffer {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	buf := &safeBuffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return buf
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

// TestRun_Bug8_LoudGapRefusesAtPreflight pins the v0.68.1 structural
// backstop at the migrate surface: a MySQL→PG schema carrying an
// untranslatable loud MySQL-only construct must refuse at pre-flight
// — BEFORE the schema writer is opened and BEFORE any CREATE TABLE —
// so there is never a partially-migrated target. Pre-fix this aborted
// at the CREATE TABLE phase after some tables already existed.
func TestRun_Bug8_LoudGapRefusesAtPreflight(t *testing.T) {
	src := newRecordingEngine("mysql")
	src.schema = &ir.Schema{Tables: []*ir.Table{{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "status", Type: ir.Varchar{Length: 32}},
		},
		CheckConstraints: []*ir.CheckConstraint{{
			Name:        "events_status_valid",
			Expr:        "FIND_IN_SET(status, 'pending,active') > 0",
			ExprDialect: "mysql",
		}},
	}}}
	tgt := newRecordingEngine("postgres")

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
	}
	err := m.Run(context.Background())
	if err == nil {
		t.Fatal("expected pre-flight refusal for untranslatable MySQL-only construct; got nil")
	}
	for _, want := range []string{"migrate", "events", "events_status_valid", "FIND_IN_SET", "partially creating the target"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal message missing %q: %v", want, err)
		}
	}
	if tgt.openSchemaWriterCalls != 0 {
		t.Errorf("schema writer opened %d times; want 0 (refusal must precede any DDL — no partial target)",
			tgt.openSchemaWriterCalls)
	}
	if len(tgt.phaseLog) != 0 {
		t.Errorf("target phases ran despite refusal: %v (partial-migration leak)", tgt.phaseLog)
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

// TestRunUpfrontIndexesBuildsIndexesBeforeCopy pins the --upfront-indexes
// reorder (Migrator.UpfrontIndexes): CreateIndexes runs BEFORE the bulk copy
// (right after CreateTablesWithoutConstraints) and is NOT re-run after. FKs
// (CreateConstraints) stay last, so foreign-key ordering is preserved.
//
// recordingSchemaWriter does NOT implement ir.IncrementalIndexBuilder, so it
// takes the non-overlap branch — but the upfront reorder is a TOP-LEVEL branch
// that fires for every writer family (IIB or not), which is what makes it apply
// to a real MySQL/PG target (pin the class, not the representative). The false
// case is covered by TestRunCallsThreePhasesInOrder above (indexes after copy).
func TestRunUpfrontIndexesBuildsIndexesBeforeCopy(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		UpfrontIndexes: true,
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	wantPhases := []string{
		"CreateTablesWithoutConstraints",
		"CreateIndexes",
		"WriteRows:users",
		"SyncIdentitySequences",
		"CreateConstraints",
	}
	if len(tgt.phaseLog) != len(wantPhases) {
		t.Fatalf("got %d phases (%v); want %d (%v)", len(tgt.phaseLog), tgt.phaseLog, len(wantPhases), wantPhases)
	}
	for i, want := range wantPhases {
		if tgt.phaseLog[i] != want {
			t.Errorf("phase[%d] = %q; want %q", i, tgt.phaseLog[i], want)
		}
	}

	// Explicit relative-order guards (independent of exact phase count so the
	// intent survives a future phase insertion): indexes strictly before the
	// copy, and NOT rebuilt after it.
	idxAt, copyAt := indexOf(tgt.phaseLog, "CreateIndexes"), indexOf(tgt.phaseLog, "WriteRows:users")
	if idxAt < 0 || copyAt < 0 {
		t.Fatalf("expected both CreateIndexes and WriteRows in log; got %v", tgt.phaseLog)
	}
	if idxAt > copyAt {
		t.Errorf("CreateIndexes (at %d) must precede the bulk copy (at %d): %v", idxAt, copyAt, tgt.phaseLog)
	}
	if n := countOf(tgt.phaseLog, "CreateIndexes"); n != 1 {
		t.Errorf("CreateIndexes ran %d times; want exactly 1 (upfront, not re-run post-copy): %v", n, tgt.phaseLog)
	}
}

func indexOf(ss []string, target string) int {
	for i, s := range ss {
		if s == target {
			return i
		}
	}
	return -1
}

func countOf(ss []string, target string) int {
	n := 0
	for _, s := range ss {
		if s == target {
			n++
		}
	}
	return n
}

// staleBackendOrderEngine and its row writer record the sequence in
// which the stale-backend reap (DetectStaleBackends) and the cold-start
// empty-table probe (IsTableEmpty) are invoked during Migrator.Run, into
// a shared order log. It pins Bug 123: the reap MUST run before the
// cold-start preflight reads the target table. A hard-killed prior run's
// orphan holds an AccessExclusive lock on the table; the cold-start
// preflight's IsTableEmpty takes a conflicting AccessShare lock, so if
// the reap ran *after* the preflight it would block on (or be refused by)
// the very lock it exists to clear — making --reap-stale-backends
// unreachable in its primary designed scenario.
type staleBackendOrderEngine struct {
	*recordingEngine
	order *[]string
}

func (e *staleBackendOrderEngine) DetectStaleBackends(context.Context, string, []string, bool) (ir.StaleBackendReport, error) {
	*e.order = append(*e.order, "reap")
	return ir.StaleBackendReport{}, nil
}

func (e *staleBackendOrderEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return &staleBackendOrderRowWriter{
		recordingRowWriter: &recordingRowWriter{phaseLog: &e.phaseLog, mu: &e.mu},
		order:              e.order,
	}, nil
}

type staleBackendOrderRowWriter struct {
	*recordingRowWriter
	order *[]string
}

func (w *staleBackendOrderRowWriter) IsTableEmpty(context.Context, *ir.Table) (bool, error) {
	*w.order = append(*w.order, "coldstart")
	return true, nil // empty → cold-start preflight passes and the run proceeds
}

// TestRunReapsStaleBackendsBeforeColdStartPreflight pins that the
// stale-backend reap runs before the cold-start empty-table probe on the
// non-resume cold-start path (Bug 123 regression guard).
func TestRunReapsStaleBackendsBeforeColdStartPreflight(t *testing.T) {
	var order []string
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := &staleBackendOrderEngine{recordingEngine: newRecordingEngine("target"), order: &order}

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		ReapStaleBackends: true,
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reapIdx, coldIdx := -1, -1
	for i, ev := range order {
		if ev == "reap" && reapIdx == -1 {
			reapIdx = i
		}
		if ev == "coldstart" && coldIdx == -1 {
			coldIdx = i
		}
	}
	if reapIdx == -1 {
		t.Fatalf("stale-backend reap never ran; order=%v", order)
	}
	if coldIdx == -1 {
		t.Fatalf("cold-start preflight never probed the target; order=%v", order)
	}
	if reapIdx > coldIdx {
		t.Errorf("stale-backend reap ran AFTER the cold-start probe (Bug 123 regression): order=%v", order)
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
	// mu guards phaseLog appends from the recordingRowWriters this engine
	// hands out: under the ADR-0076 cross-table pool, peer tables call
	// WriteRows concurrently on sibling writers that share this slice.
	mu sync.Mutex
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
	return &recordingRowWriter{phaseLog: &e.phaseLog, mu: &e.mu}, nil
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

func (*recordingRowReader) Err() error { return nil }

type recordingRowWriter struct {
	phaseLog *[]string
	mu       *sync.Mutex // shared with the owning recordingEngine; guards phaseLog
}

func (w *recordingRowWriter) WriteRows(_ context.Context, table *ir.Table, _ <-chan ir.Row) error {
	// Guard the shared phaseLog append: the ADR-0076 cross-table pool calls
	// WriteRows on sibling writers concurrently. (Schema-phase appends run
	// strictly before/after the copy pool, so they need no lock.)
	w.mu.Lock()
	*w.phaseLog = append(*w.phaseLog, "WriteRows:"+table.Name)
	w.mu.Unlock()
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
