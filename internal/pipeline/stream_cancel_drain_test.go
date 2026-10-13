// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// cancelledUnselectable is a context that has been cancelled but whose Done
// channel never fires in a select. It pins the arm the scheduler picks when
// a cancel closes BOTH the window's ctx and the CDC reader's change channel
// before the capture loop wakes: a select over two ready cases picks either,
// and this makes the test take the closed-channel one every time.
type cancelledUnselectable struct{ context.Context }

func (cancelledUnselectable) Done() <-chan struct{} { return nil }
func (cancelledUnselectable) Err() error            { return context.Canceled }

// cancelAtClose is [cancelledUnselectable] that turns cancelled only when the
// feeder flips it — after the window has read (and rolled a chunk over) a
// live transaction, and just before the reader closes its channel.
type cancelAtClose struct {
	context.Context
	cancelled *atomic.Bool
}

func (cancelAtClose) Done() <-chan struct{} { return nil }

func (c cancelAtClose) Err() error {
	if c.cancelled.Load() {
		return context.Canceled
	}
	return nil
}

// errReader is a CDC reader whose Err() reports err: the shape of a reader
// that records the cancel (or anything else) as its own terminal error.
type errReader struct{ err error }

func (errReader) StreamChanges(context.Context, ir.Position) (<-chan ir.Change, error) {
	return nil, errors.New("unused")
}
func (r errReader) Err() error { return r.err }

func cancelDrainStream(t *testing.T) (*BackupStream, *irbackup.Manifest) {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &BackupStream{segStore: store, segCodec: blobcodec.CodecGzip, stopPollInterval: time.Hour},
		&irbackup.Manifest{
			FormatVersion: irbackup.BackupFormatVersion,
			SourceEngine:  "postgres",
			CreatedAt:     time.Unix(0, 0).UTC(),
			Kind:          irbackup.BackupKindIncremental,
		}
}

var (
	cdTxBegin  = ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/1"}`}}
	cdTxCommit = ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/9"}`}}
)

// feedSpanningTransaction sends one transaction whose rows roll a chunk of
// size 3 BEFORE its TxCommit (TxBegin, row, row → roll; TxCommit in the next
// chunk), then flips cancelled (when non-nil) and closes the channel. The
// channel is unbuffered, so each send returns only once the capture loop
// took the change: the roll happens under a live context, and the cancel is
// in force before the close is observed.
func feedSpanningTransaction(cancelled *atomic.Bool) <-chan ir.Change {
	ch := make(chan ir.Change)
	go func() {
		ch <- cdTxBegin
		ch <- wideChange(1, "x")
		ch <- wideChange(2, "y")
		ch <- cdTxCommit
		if cancelled != nil {
			cancelled.Store(true)
		}
		close(ch)
	}()
	return ch
}

// requireWholeTransactionCommitted asserts the window may be committed and
// that what it commits ends at the transaction's TxCommit, in both chunks.
func requireWholeTransactionCommitted(t *testing.T, out captureOutcome, m *irbackup.Manifest) {
	t.Helper()
	if r := abandonReason(out); r != "" {
		t.Fatalf("the window read a whole transaction before the cancel but was abandoned: %s", r)
	}
	if len(m.ChangeChunks) != 2 || out.EndPos != cdTxCommit.Position {
		t.Fatalf("stored %d chunks ending at %+v; want 2 ending at the TxCommit %+v — committing this window "+
			"would end the chain inside the transaction (F-E1-SEVERED-TAIL-REPLAY)", len(m.ChangeChunks), out.EndPos, cdTxCommit.Position)
	}
}

// TestBackupStream_CancelDrainCommitsWhicheverArmSeesTheCancel pins the
// ctx-cancel drain of `backup stream` (the SIGTERM contract) on the arm the
// CI failure of 2026-10-13 took. A cancel closes the window's context AND,
// through the same context, the reader's change channel; the capture loop
// can observe either first. The ctx.Done arm flushed the window's last chunk
// under a fresh context and returned ctx.Err(), so the drain-commit kept
// every change observed. The closed-channel arm flushed under the CANCELLED
// context instead: the store refused the put, the window's last chunk was
// never recorded, and the drain-commit committed whatever had been stored
// before the cancel.
//
// The independent expected value is the transaction fed to the loop: after
// either arm the window must hold its chunk and report the cancel.
func TestBackupStream_CancelDrainCommitsWhicheverArmSeesTheCancel(t *testing.T) {
	b, m := cancelDrainStream(t)
	ch := make(chan ir.Change, 3)
	ch <- cdTxBegin
	ch <- wideChange(1, "x")
	ch <- cdTxCommit
	close(ch)

	out, err := b.captureWindow(cancelledUnselectable{context.Background()}, nil /*cdc*/, ch, m, 100,
		time.Now().Add(time.Hour), 0, 0, "stream_state.json", time.Now, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("captureWindow = %v; want the cancel reported, so the caller drain-commits", err)
	}
	if out.OpenTransaction || out.AbandonReason != "" {
		t.Fatalf("window ended at a transaction boundary but was marked open/abandoned: %+v", out)
	}
	if len(m.ChangeChunks) != 1 || m.ChangeChunks[0].RowCount == 0 {
		t.Fatalf("the window's chunk was not stored (%d chunks): the cancel drain dropped a window it had read", len(m.ChangeChunks))
	}
}

// TestBackupStream_CancelDrainNeverCommitsASeveredTail is the harm the
// closed-channel arm actually did before the fix, v0.156.12 through v0.159.2
// (review of 8b3dfe31). Chunks roll on a change count, not at transaction
// boundaries, so the window's last transaction T spanned a roll: its head
// was stored under the live context, its TxCommit sat in the open chunk.
// The closed arm's flush of that chunk failed under the cancelled context,
// nothing marked the window abandoned (T's TxCommit had been consumed), and
// the drain-commit committed the stored head of T with an EndPosition inside
// it. The next window re-delivers T whole; a broker that applied the
// severed link as the tip applies T twice.
func TestBackupStream_CancelDrainNeverCommitsASeveredTail(t *testing.T) {
	b, m := cancelDrainStream(t)
	var cancelled atomic.Bool
	out, err := b.captureWindow(cancelAtClose{context.Background(), &cancelled}, nil, /*cdc*/
		feedSpanningTransaction(&cancelled), m, 3,
		time.Now().Add(time.Hour), 0, 0, "stream_state.json", time.Now, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("captureWindow = %v; want the cancel reported", err)
	}
	requireWholeTransactionCommitted(t, out, m)
}

// TestBackupStream_CancelDrainTakesAReaderThatRecordedTheCancel: the
// pgtrigger and sqlite-trigger pumps (and the Postgres reader's
// standby-status send) record the cancel as their own error before closing
// the channel. The closed arm used to surface any recorded error first, so
// for them the drain never ran; a cancel-class reader error on a cancelled
// window is the cancel, and drains like it.
func TestBackupStream_CancelDrainTakesAReaderThatRecordedTheCancel(t *testing.T) {
	b, m := cancelDrainStream(t)
	var cancelled atomic.Bool
	reader := errReader{err: fmt.Errorf("pgtrigger: cdc: poll: %w", context.Canceled)}
	out, err := b.captureWindow(cancelAtClose{context.Background(), &cancelled}, reader,
		feedSpanningTransaction(&cancelled), m, 3,
		time.Now().Add(time.Hour), 0, 0, "stream_state.json", time.Now, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("captureWindow = %v; want the cancel reported", err)
	}
	requireWholeTransactionCommitted(t, out, m)
}

// TestBackupStream_UnstoredTailAbandonsOnAnyContextError pins the
// structural backstop, not either arm: whatever exit returns a context-class
// error — the one runRollover drain-commits on — while changes the window
// read are still unstored, the window is abandoned rather than committed
// short of them. The case here reaches no drain: the window's own context is
// live and the reader reports a context deadline of its own.
func TestBackupStream_UnstoredTailAbandonsOnAnyContextError(t *testing.T) {
	b, m := cancelDrainStream(t)
	reader := errReader{err: fmt.Errorf("reader: %w", context.DeadlineExceeded)}
	out, err := b.captureWindow(context.Background(), reader, feedSpanningTransaction(nil), m, 3,
		time.Now().Add(time.Hour), 0, 0, "stream_state.json", time.Now, nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("captureWindow = %v; want the reader's deadline surfaced", err)
	}
	if len(m.ChangeChunks) != 1 {
		t.Fatalf("stored %d chunks; the case needs the transaction's head stored and its TxCommit not", len(m.ChangeChunks))
	}
	if abandonReason(out) == "" {
		t.Fatal("a context-class exit with the window's TxCommit unstored was not abandoned: the drain-commit " +
			"would commit the stored head of the transaction")
	}
}

// TestBackupStream_CancelInsideATransactionStillAbandons guards the
// in-transaction branch of the drain: a cancel that cuts a transaction must
// not flush its head (F-E1-SEVERED-TAIL-REPLAY) — the window stays
// open-transaction and nothing is stored. A drain that flushed regardless of
// inTransaction stores the chunk here and fails this.
func TestBackupStream_CancelInsideATransactionStillAbandons(t *testing.T) {
	b, m := cancelDrainStream(t)
	ch := make(chan ir.Change, 2)
	ch <- cdTxBegin
	ch <- wideChange(1, "x")
	close(ch)

	out, err := b.captureWindow(cancelledUnselectable{context.Background()}, nil /*cdc*/, ch, m, 100,
		time.Now().Add(time.Hour), 0, 0, "stream_state.json", time.Now, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("captureWindow = %v; want the cancel reported", err)
	}
	if !out.OpenTransaction {
		t.Error("a window cut inside a transaction must be marked open, so the caller abandons it")
	}
	if len(m.ChangeChunks) != 0 {
		t.Errorf("%d chunks stored for a window cut inside a transaction; want none (the severed tail)", len(m.ChangeChunks))
	}
}
