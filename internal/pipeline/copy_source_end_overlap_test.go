// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// GC-41 (i), the RELEASED-code path the review traced: `migrate`'s
// overlapped copy+index phase with table parallelism ≥ 2.
//
// A peer table's failure cancels only the copy pool's errgroup context
// (tctx). The in-flight table's stream closes, its writer's "closed" arm
// wins on an empty batch, and — before the source-end verdict — copyTable
// returned nil. setTableProgressAndWrite then set the table COMPLETE in
// memory (its store write failed on tctx and was only warned), the
// onTableCopied hook handed the table to the index axis (whose context is
// still live), and the IndexesBuilt callback — markTableIndexesBuilt, on
// the OUTER, live context — wrote the whole in-memory entry durably:
// {complete, indexes_built, a partial RowsCopied}. A later
// `migrate --resume` skips that table and verifyBuiltIndexes passes: its
// unread tail is lost at exit 0.
//
// This cell drives the real runOverlappedCopyAndIndexPhase with two
// tables at parallelism 2: "fails" refuses its read once "inflight" has
// streamed rows and is blocked mid-table. Both reader end-shapes (a quiet
// close, a recorded ctx error) because they reach the old forgiveness by
// different routes.

// overlapPeerFailEngine opens per-table readers sharing one signal: the
// in-flight table reports when it is mid-table, and the failing table
// waits for that before refusing.
type overlapPeerFailEngine struct {
	stubEngine
	sticky   bool
	midTable chan struct{}
	once     sync.Once
}

func (e *overlapPeerFailEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return &overlapPeerFailReader{e: e}, nil
}

func (e *overlapPeerFailEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return &closedWinsWriter{}, nil
}

type overlapPeerFailReader struct {
	e   *overlapPeerFailEngine
	mu  sync.Mutex
	err error
}

func (r *overlapPeerFailReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	if table.Name == "fails" {
		select {
		case <-r.e.midTable:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, errors.New("synthetic peer read failure")
	}
	ch := make(chan ir.Row)
	go func() {
		defer close(ch)
		for i := 0; i < 5; i++ {
			select {
			case ch <- ir.Row{"id": int64(i)}:
			case <-ctx.Done():
				r.ended(ctx)
				return
			}
		}
		r.e.once.Do(func() { close(r.e.midTable) })
		<-ctx.Done() // a table with more rows to come
		r.ended(ctx)
	}()
	return ch, nil
}

func (r *overlapPeerFailReader) ended(ctx context.Context) {
	if r.e.sticky {
		r.mu.Lock()
		r.err = ctx.Err()
		r.mu.Unlock()
	}
}

func (r *overlapPeerFailReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func TestOverlapPhase_PeerFailureRecordsNothingForTheInFlightTable(t *testing.T) {
	for _, sticky := range []bool{false, true} {
		t.Run(fmt.Sprintf("sticky-cancel=%t", sticky), func(t *testing.T) {
			t.Parallel()
			eng := &overlapPeerFailEngine{sticky: sticky, midTable: make(chan struct{})}
			schema := stopMidCopySchema("inflight", "fails")
			store := ctxHonouringStateStore{newFakeStateStore()}
			rc := resumeContext{store: store, migrationID: "m", enabled: true}
			state := &ir.MigrationState{MigrationID: "m", TableProgress: map[string]ir.TableProgress{}}
			var stateMu sync.Mutex
			sw := &fakeIndexBuilderSW{}
			deps := &parallelBulkCopyDeps{source: eng, target: eng, parallelism: 1}

			err := runOverlappedCopyAndIndexPhase(
				context.Background(), rc, state, &stateMu, schema,
				&overlapPeerFailReader{e: eng}, sw, &closedWinsWriter{}, sw,
				false, 0, deps, 2, nil, ShardColumnSpec{},
			)
			if err == nil {
				t.Fatal("the overlapped phase returned nil although a table's read failed")
			}
			got, _ := store.get("m")
			if e := got.TableProgress["inflight"]; e.State == ir.TableProgressComplete || e.IndexesBuilt {
				t.Errorf("the in-flight table was recorded %+v after a PEER's failure cut its copy short: "+
					"`migrate --resume` skips it and its unread tail is lost at exit 0 (GC-41 (i))", e)
			}
			for _, name := range sw.snapshotReceived() {
				if name == "inflight" {
					t.Errorf("the in-flight table was handed to the index axis as copied")
				}
			}
		})
	}
}
