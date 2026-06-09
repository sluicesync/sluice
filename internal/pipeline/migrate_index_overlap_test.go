// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
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
