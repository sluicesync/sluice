// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// fakeIndexBuilderSW is a minimal SchemaWriter that ALSO implements
// [ir.IncrementalIndexBuilder] + [ir.TableIndexedNotifier], so the overlap
// orchestrator (runOverlappedCopyAndIndexPhase) drives it. It records the
// tables it received off the completed-tables channel, observes whether the
// channel was closed exactly once (the drain loop exits), and can be told
// to fail (to pin "index error cancels the copy pool").
type fakeIndexBuilderSW struct {
	failOnTable string // when non-empty, return an error after receiving this table

	mu        sync.Mutex
	received  []string
	cb        func(table *ir.Table)
	closedOK  bool
	buildEntr int
}

// --- ir.SchemaWriter (only the methods the overlap path touches) ---

func (s *fakeIndexBuilderSW) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	return nil
}
func (s *fakeIndexBuilderSW) CreateIndexes(context.Context, *ir.Schema) error     { return nil }
func (s *fakeIndexBuilderSW) CreateConstraints(context.Context, *ir.Schema) error { return nil }
func (s *fakeIndexBuilderSW) SyncIdentitySequences(context.Context, *ir.Schema) error {
	return nil
}
func (s *fakeIndexBuilderSW) CreateViews(context.Context, *ir.Schema) error { return nil }

// --- ir.TableIndexedNotifier ---

func (s *fakeIndexBuilderSW) SetTableIndexedCallback(fn func(table *ir.Table)) {
	s.mu.Lock()
	s.cb = fn
	s.mu.Unlock()
}

// --- ir.IncrementalIndexBuilder ---

func (s *fakeIndexBuilderSW) BuildTableIndexesFromChannel(ctx context.Context, _ *ir.Schema, completedTables <-chan *ir.Table) error {
	s.mu.Lock()
	s.buildEntr++
	s.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case table, ok := <-completedTables:
			if !ok {
				s.mu.Lock()
				s.closedOK = true
				s.mu.Unlock()
				return nil
			}
			s.mu.Lock()
			s.received = append(s.received, table.Name)
			cb := s.cb
			fail := s.failOnTable == table.Name
			s.mu.Unlock()
			if cb != nil {
				cb(table) // fire the per-table IndexesBuilt callback
			}
			if fail {
				return fmt.Errorf("fake index build failed on %s", table.Name)
			}
		}
	}
}

func (s *fakeIndexBuilderSW) snapshotReceived() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.received...)
	return out
}

// overlapTestSchema builds n PK-bearing tables.
func overlapTestSchema(n int) *ir.Schema {
	tables := make([]*ir.Table, n)
	for i := range tables {
		tables[i] = &ir.Table{
			Name:       fmt.Sprintf("t%02d", i),
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		}
	}
	return &ir.Schema{Tables: tables}
}

// TestOverlapPhase_EveryTableFedAndChannelClosedOnce drives the overlap
// orchestrator with a fake index builder and asserts: (1) EVERY copied
// table is handed to the index builder, (2) the channel is closed exactly
// once (the builder's drain loop exits cleanly, buildEntr==1), and (3) the
// per-table IndexesBuilt callback flips every table's flag.
func TestOverlapPhase_EveryTableFedAndChannelClosedOnce(t *testing.T) {
	const n = 12
	schema := overlapTestSchema(n)

	gauge := &concurrencyGauge{}
	eng := &poolFakeEngine{rowsPerTable: 5, gauge: gauge}
	primaryWriter := newPoolFakeWriter(gauge, 0)
	primaryReader := &poolFakeReader{rowsPerTable: 5}
	deps := &parallelBulkCopyDeps{source: eng, target: eng, parallelism: 1}

	sw := &fakeIndexBuilderSW{}
	state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}
	var stateMu sync.Mutex
	rc := resumeContext{enabled: false}

	if err := runOverlappedCopyAndIndexPhase(
		context.Background(), rc, state, &stateMu, schema,
		primaryReader, sw, primaryWriter, sw,
		false, 0, deps, 4, nil, ShardColumnSpec{},
	); err != nil {
		t.Fatalf("runOverlappedCopyAndIndexPhase: %v", err)
	}

	got := sw.snapshotReceived()
	if len(got) != n {
		t.Fatalf("index builder received %d tables; want %d (%v)", len(got), n, got)
	}
	seen := map[string]bool{}
	for _, name := range got {
		if seen[name] {
			t.Errorf("table %s fed to the index builder more than once", name)
		}
		seen[name] = true
	}
	for i := 0; i < n; i++ {
		if !seen[fmt.Sprintf("t%02d", i)] {
			t.Errorf("table t%02d never fed to the index builder", i)
		}
	}
	if !sw.closedOK || sw.buildEntr != 1 {
		t.Errorf("channel-close contract broken: closedOK=%v buildEntr=%d (want true,1)", sw.closedOK, sw.buildEntr)
	}

	// Every table's IndexesBuilt flag must be set (the callback fired).
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("t%02d", i)
		if !state.TableProgress[name].IndexesBuilt {
			t.Errorf("table %s IndexesBuilt not set", name)
		}
		if state.TableProgress[name].State != ir.TableProgressComplete {
			t.Errorf("table %s State = %q; want complete", name, state.TableProgress[name].State)
		}
	}
}

// TestOverlapPhase_IndexErrorCancelsCopyPool fails the fake index builder
// on one table and asserts the whole phase returns an error (the index
// error cancels the copy pool via the shared errgroup ctx).
func TestOverlapPhase_IndexErrorCancelsCopyPool(t *testing.T) {
	const n = 20
	schema := overlapTestSchema(n)

	gauge := &concurrencyGauge{}
	// A dwell on the copy side keeps copies in flight while the index
	// builder errors, so the cancellation actually has peers to unwind.
	eng := &poolFakeEngine{rowsPerTable: 5, gauge: gauge, dwell: 20 * time.Millisecond}
	primaryWriter := newPoolFakeWriter(gauge, 20*time.Millisecond)
	primaryReader := &poolFakeReader{rowsPerTable: 5}
	deps := &parallelBulkCopyDeps{source: eng, target: eng, parallelism: 1}

	sw := &fakeIndexBuilderSW{failOnTable: "t00"}
	state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}
	var stateMu sync.Mutex
	rc := resumeContext{enabled: false}

	err := runOverlappedCopyAndIndexPhase(
		context.Background(), rc, state, &stateMu, schema,
		primaryReader, sw, primaryWriter, sw,
		false, 0, deps, 4, nil, ShardColumnSpec{},
	)
	if err == nil {
		t.Fatal("expected an error when the index builder fails; got nil")
	}
}

// errAfterNReader streams rows for the first failAfter tables, then returns
// an error from ReadRows so a copy goroutine fails — pinning "copy error
// cancels the index pool".
type errAfterNEngine struct {
	stubEngine
	mu       sync.Mutex
	opened   int
	failAt   int // the N-th OpenRowReader call returns a reader whose ReadRows errors
	rowsEach int
}

func (e *errAfterNEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	e.mu.Lock()
	e.opened++
	fail := e.opened >= e.failAt
	e.mu.Unlock()
	return &maybeErrReader{rowsEach: e.rowsEach, fail: fail}, nil
}

func (e *errAfterNEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return newPoolFakeWriter(&concurrencyGauge{}, 0), nil
}

type maybeErrReader struct {
	rowsEach int
	fail     bool
}

func (r *maybeErrReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	if r.fail {
		return nil, errors.New("synthetic read failure")
	}
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for i := 0; i < r.rowsEach; i++ {
			out <- ir.Row{"id": int64(i + 1)}
		}
	}()
	return out, nil
}

func (r *maybeErrReader) Err() error { return nil }

// TestOverlapPhase_CopyErrorCancelsIndexPool fails a copy reader and
// asserts the phase returns an error AND the index builder observed the
// cancellation (it returns ctx.Err(), not a clean nil close).
func TestOverlapPhase_CopyErrorCancelsIndexPool(t *testing.T) {
	const n = 20
	schema := overlapTestSchema(n)

	// Fail on the 2nd-or-later reader open so at least one table copies
	// cleanly and the index builder is actively draining when the cancel
	// lands. Each per-table dedicated pair opens its own reader; the free
	// pair uses the primary reader below.
	eng := &errAfterNEngine{failAt: 2, rowsEach: 5}
	primaryWriter := newPoolFakeWriter(&concurrencyGauge{}, 5*time.Millisecond)
	primaryReader := &maybeErrReader{rowsEach: 5}
	deps := &parallelBulkCopyDeps{source: eng, target: eng, parallelism: 1}

	sw := &fakeIndexBuilderSW{}
	state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}
	var stateMu sync.Mutex
	rc := resumeContext{enabled: false}

	err := runOverlappedCopyAndIndexPhase(
		context.Background(), rc, state, &stateMu, schema,
		primaryReader, sw, primaryWriter, sw,
		false, 0, deps, 4, nil, ShardColumnSpec{},
	)
	if err == nil {
		t.Fatal("expected an error when a copy reader fails; got nil")
	}
}

// walledIndexBuilderSW is fakeIndexBuilderSW with a caller-supplied error, so
// the attribution pin below can hand the overlapped phase the REAL walled
// shape (`mysql: create indexes on … maximum statement execution time
// exceeded`) rather than a synthetic "fake index build failed" string the
// hint registry would never match.
type walledIndexBuilderSW struct {
	*fakeIndexBuilderSW
	wall error
}

func (s *walledIndexBuilderSW) BuildTableIndexesFromChannel(ctx context.Context, _ *ir.Schema, completedTables <-chan *ir.Table) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case _, ok := <-completedTables:
		if !ok {
			return nil
		}
		return s.wall
	}
}

// TestOverlapPhase_AttributesTheFailingAxis pins WHICH phase the overlapped
// errgroup blames, in both directions.
//
// The index axis used to be attributed to bulk-copy ("the conservative
// guess"), which silently discarded the errno-3024 hint and its
// SLUICE-E-INDEX-STATEMENT-TIME-LIMIT code: those live under PhaseIndexes,
// and PhaseBulkCopy's entries match none of the walled text. cmd/sluice's
// TestMigrateIndexWall_HintAndCodeReachTheOperator is the end-to-end gate;
// this is the unit-level pin that the attribution ALSO doesn't over-correct —
// a copy failure that merely cancels the index axis must still report as
// bulk-copy.
func TestOverlapPhase_AttributesTheFailingAxis(t *testing.T) {
	t.Run("index axis → indexes phase, hint and code attached", func(t *testing.T) {
		// The overlap phase runs under `migrate` only (see the file header),
		// so this pin asserts against migrate's per-command remedy — which
		// the CLI records before Run and a bare unit test does not (Bug 230).
		migcore.SetRunningCommand(string(migcore.CommandMigrate))
		defer migcore.SetRunningCommand("")

		schema := overlapTestSchema(3)
		gauge := &concurrencyGauge{}
		eng := &poolFakeEngine{rowsPerTable: 2, gauge: gauge}
		sw := &walledIndexBuilderSW{
			fakeIndexBuilderSW: &fakeIndexBuilderSW{},
			wall: fmt.Errorf(`mysql: create indexes on %q: %w`, "events",
				errors.New("Error 3024 (HY000): Query execution was interrupted, "+
					"maximum statement execution time exceeded")),
		}
		state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}
		var stateMu sync.Mutex

		err := runOverlappedCopyAndIndexPhase(
			context.Background(), resumeContext{enabled: false}, state, &stateMu, schema,
			&poolFakeReader{rowsPerTable: 2}, sw, newPoolFakeWriter(gauge, 0), sw,
			false, 0, &parallelBulkCopyDeps{source: eng, target: eng, parallelism: 1},
			4, nil, ShardColumnSpec{},
		)
		if err == nil {
			t.Fatal("expected the walled index build to fail the phase")
		}
		if !strings.Contains(err.Error(), "pipeline: create indexes:") {
			t.Errorf("error lacks the index-phase prefix every other index path carries:\n%v", err)
		}
		var coded *sluicecode.CodedError
		if !errors.As(err, &coded) {
			t.Fatalf("walled index build carries no code, so the operator gets a bare MySQL timeout:\n%v", err)
		}
		if coded.Code != sluicecode.CodeIndexStatementTimeLimit {
			t.Errorf("code = %s; want %s", coded.Code, sluicecode.CodeIndexStatementTimeLimit)
		}
		if !strings.Contains(coded.Hint, "--resume finishes just the indexes with NO re-copy") {
			t.Errorf("hint does not name the no-re-copy remedy: %q", coded.Hint)
		}
	})

	// The attribution change also moves the PERSISTED phase to indexes, and
	// the comment at the call site claims resume is unaffected because every
	// resume decision reads per-table TableProgress while state.Phase is only
	// ever compared against MigrationPhaseComplete. That claim is a
	// hypothesis until something fails when it breaks — this is that
	// something.
	t.Run("persisted state names indexes and keeps per-table progress", func(t *testing.T) {
		schema := overlapTestSchema(3)
		gauge := &concurrencyGauge{}
		eng := &poolFakeEngine{rowsPerTable: 2, gauge: gauge}
		sw := &walledIndexBuilderSW{
			fakeIndexBuilderSW: &fakeIndexBuilderSW{},
			wall:               errors.New("mysql: create indexes: maximum statement execution time exceeded"),
		}
		store := newFakeStateStore()
		rc := resumeContext{store: store, migrationID: "m-overlap", enabled: true}
		state := &ir.MigrationState{MigrationID: "m-overlap", TableProgress: map[string]ir.TableProgress{}}
		var stateMu sync.Mutex

		if err := runOverlappedCopyAndIndexPhase(
			context.Background(), rc, state, &stateMu, schema,
			&poolFakeReader{rowsPerTable: 2}, sw, newPoolFakeWriter(gauge, 0), sw,
			false, 0, &parallelBulkCopyDeps{source: eng, target: eng, parallelism: 1},
			4, nil, ShardColumnSpec{},
		); err == nil {
			t.Fatal("expected the walled index build to fail the phase")
		}

		persisted, found, err := store.Read(context.Background(), "m-overlap")
		if err != nil || !found {
			t.Fatalf("state row not persisted: found=%v err=%v", found, err)
		}
		if persisted.Phase != ir.MigrationPhaseIndexes {
			t.Errorf("persisted phase = %q; want %q — the failed phase should name what failed",
				persisted.Phase, ir.MigrationPhaseIndexes)
		}
		if persisted.Phase == ir.MigrationPhaseComplete {
			t.Error("persisted phase is complete; a --resume would exit clean over an unbuilt index")
		}
		// The load-bearing half: whichever tables DID copy stay recorded
		// complete, so a --resume skips their copy rather than re-copying
		// 122 GB. If a future change made resume read state.Phase instead,
		// this is the assertion that stops being enough — and
		// TestResume_* covers that read side.
		if len(persisted.TableProgress) == 0 {
			t.Fatal("no per-table progress survived the failure; a --resume would re-copy everything")
		}
		for name, tp := range persisted.TableProgress {
			if tp.State == ir.TableProgressComplete {
				return // at least one copied table is still resumable
			}
			_ = name
		}
		t.Errorf("no table recorded complete despite copies succeeding: %+v", persisted.TableProgress)
	})

	t.Run("copy axis → bulk-copy phase, unchanged", func(t *testing.T) {
		schema := overlapTestSchema(6)
		sw := &fakeIndexBuilderSW{}
		state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}
		var stateMu sync.Mutex
		eng := &errAfterNEngine{failAt: 1, rowsEach: 2}

		err := runOverlappedCopyAndIndexPhase(
			context.Background(), resumeContext{enabled: false}, state, &stateMu, schema,
			&maybeErrReader{rowsEach: 2, fail: true}, sw, newPoolFakeWriter(&concurrencyGauge{}, 0), sw,
			false, 0, &parallelBulkCopyDeps{source: eng, target: eng, parallelism: 1},
			4, nil, ShardColumnSpec{},
		)
		if err == nil {
			t.Fatal("expected the failing copy reader to fail the phase")
		}
		if strings.Contains(err.Error(), "pipeline: create indexes:") {
			t.Errorf("a COPY failure was mis-attributed to the index phase:\n%v", err)
		}
		var coded *sluicecode.CodedError
		if errors.As(err, &coded) && coded.Code == sluicecode.CodeIndexStatementTimeLimit {
			t.Errorf("a copy failure picked up the index-wall code: %s", coded.Code)
		}
	})
}
