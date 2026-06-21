// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// These pins lock the ADR-0109 sibling-cancel silent-loss fix
// DETERMINISTICALLY (no -race-scheduler dependence). The bug: when a peer
// chunk's retriable source-read drop cancels the parallel-copy errgroup,
// the cancelled sibling chunk's reader closes its page channel early; both
// the copyChunkFast pump and the copyChunk idempotent loop then read the
// empty/short page as a CLEAN end-of-chunk and (because readerStreamErr
// filters ctx.Canceled to nil) post success → the chunk is recorded
// State=Complete with a PARTIAL/EMPTY copy → the whole-table retry SKIPS it
// → silent loss of the chunk's unread tail.
//
// The contract the pins enforce: a chunk whose context was cancelled MUST
// return the cancellation error (so it stays NOT-complete), NEVER nil.
// They reproduce the cancel deterministically by passing an
// already-cancelled context, so the reader yields a benign-cancel empty
// page exactly as the errgroup cancellation does — no timing window.

// cancelObservingReader serves NO rows once its context is already
// cancelled: it returns an immediately-closed channel and an Err() of
// ctx.Err(), exactly as a real reader does when its query/stream is
// cancelled mid-page (which readerStreamErr then filters to nil). It
// models the sibling chunk whose read the errgroup cancelled.
type cancelObservingReader struct{}

func (cancelObservingReader) Err() error { return context.Canceled }

func (cancelObservingReader) closedEmpty() <-chan ir.Row {
	ch := make(chan ir.Row)
	close(ch)
	return ch
}

func (r cancelObservingReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	return r.closedEmpty(), nil
}

func (r cancelObservingReader) ReadRowsBatch(context.Context, *ir.Table, []any, int) (<-chan ir.Row, error) {
	return r.closedEmpty(), nil
}

func (r cancelObservingReader) ReadRowsBatchBounded(context.Context, *ir.Table, []any, []any, int) (<-chan ir.Row, error) {
	return r.closedEmpty(), nil
}

// chunkOneInProgress returns a state whose single recorded chunk 0 is
// mid-copy (LowerPK nil, UpperPK 16, no LastPK, in_progress) — the shape a
// fresh chunk holds when the errgroup cancels it before it finishes.
func chunkOneInProgress(table *ir.Table) (*ir.MigrationState, ir.TableChunkProgress) {
	chunk := ir.TableChunkProgress{ChunkIndex: 0, UpperPK: []any{int64(16)}, State: ir.TableProgressInProgress}
	state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{
		table.Name: {State: ir.TableProgressInProgress, Chunks: []ir.TableChunkProgress{chunk}},
	}}
	return state, chunk
}

// TestCopyChunkFast_CancelNeverCompletes pins that copyChunkFast on a
// cancelled context returns the cancellation error AND does not mark the
// chunk Complete (the fast-loader path the cold-start sibling chunk takes).
func TestCopyChunkFast_CancelNeverCompletes(t *testing.T) {
	captureSlog(t)

	table := intPKTable("dropping")
	pkCols := primaryKeyColumnNames(table)
	state, chunk := chunkOneInProgress(table)
	tgt := newFakeTarget()
	var mu sync.Mutex

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the errgroup-cancel a peer chunk's drop triggers

	err := copyChunkFast(ctx, resumeContext{}, state, &mu, cancelObservingReader{}, tgt, table, pkCols,
		0, chunk, 7, nil, ShardColumnSpec{})
	if err == nil {
		t.Fatal("copyChunkFast returned nil on a cancelled ctx — the cancelled chunk would be marked Complete and its tail silently lost on the retry-skip")
	}
	if got := state.TableProgress[table.Name].Chunks[0].State; got == ir.TableProgressComplete {
		t.Fatalf("cancelled chunk marked Complete (state=%q) — the whole-table retry would skip it → silent loss", got)
	}
}

// TestCopyTableWithCursor_CancelNeverCompletes pins the same contract for
// the SINGLE-READER --resume cursor path (a separate pre-existing latent
// CRITICAL of the same class): a table cancelled mid-copy by a PEER table's
// terminal error (the cross-table pool cancels its errgroup ctx) must
// return the cancellation error, NEVER nil — because the caller marks a
// nil-returning table State=Complete and a later --resume would SKIP it
// with only a partial copy on disk → silent loss.
func TestCopyTableWithCursor_CancelNeverCompletes(t *testing.T) {
	captureSlog(t)

	table := intPKTable("orders")
	state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{
		table.Name: {State: ir.TableProgressInProgress},
	}}
	tgt := newFakeTarget()
	var mu sync.Mutex

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := copyTableWithCursor(ctx, resumeContext{}, state, &mu, tgt, cancelObservingReader{}, table,
		7, nil, ShardColumnSpec{})
	if err == nil {
		t.Fatal("copyTableWithCursor returned nil on a cancelled ctx — the caller would mark the table Complete and a --resume would skip it → silent loss of its unread tail")
	}
}

// TestCopyChunk_CancelNeverCompletes pins the same contract for the
// idempotent copyChunk loop (the path the resume/non-fast sibling chunk
// takes).
func TestCopyChunk_CancelNeverCompletes(t *testing.T) {
	captureSlog(t)

	table := intPKTable("dropping")
	pkCols := primaryKeyColumnNames(table)
	state, _ := chunkOneInProgress(table)
	tgt := newFakeTarget()
	var mu sync.Mutex

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// resuming=true so useFastLoader is false and copyChunk takes the
	// idempotent cursor loop (the branch with its own cancel guard).
	err := copyChunk(ctx, resumeContext{}, state, &mu, cancelObservingReader{}, tgt, table, pkCols,
		0, 7, nil, ShardColumnSpec{}, true /*resuming*/, false /*forceColdStart*/, false /*rawCopyOK*/, ir.RawCopyFormat(0))
	if err == nil {
		t.Fatal("copyChunk returned nil on a cancelled ctx — the cancelled chunk would be marked Complete and its tail silently lost on the retry-skip")
	}
	if got := state.TableProgress[table.Name].Chunks[0].State; got == ir.TableProgressComplete {
		t.Fatalf("cancelled chunk marked Complete (state=%q) — the whole-table retry would skip it → silent loss", got)
	}
}
