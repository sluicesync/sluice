// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
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

// TestBackupStream_CancelDrainCommitsWhicheverArmSeesTheCancel pins the
// ctx-cancel drain of `backup stream` (the SIGTERM contract) on the arm the
// CI failure of 2026-10-13 took. A cancel closes the window's context AND,
// through the same context, the reader's change channel; the capture loop
// can observe either first. The ctx.Done arm flushed the window's last chunk
// under a fresh context and returned ctx.Err(), so the drain-commit kept
// every change observed. The closed-channel arm flushed under the CANCELLED
// context instead: the store refused the put, the window's last chunk was
// never recorded, and the stream exited 0 having committed nothing of a
// window it had read in full (TestChainRestore_KeyCollationLoosenedInsideTheWindow:
// `apply = <nil>; source 1:61,100:7A7A,2:62,99:4132 target 1:61,2:62`).
//
// The independent expected value is the transaction fed to the loop: after
// either arm the window must hold its chunk and report the cancel.
func TestBackupStream_CancelDrainCommitsWhicheverArmSeesTheCancel(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &BackupStream{segStore: store, segCodec: blobcodec.CodecGzip, stopPollInterval: time.Hour}
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     time.Unix(0, 0).UTC(),
		Kind:          irbackup.BackupKindIncremental,
	}
	ch := make(chan ir.Change, 3)
	ch <- ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/1"}`}}
	ch <- wideChange(1, "x")
	ch <- ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/3"}`}}
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

// TestBackupStream_CancelInsideATransactionStillAbandons is the other half:
// taking the cancel on the closed-channel arm must not commit the head of a
// transaction the cancel cut (F-E1-SEVERED-TAIL-REPLAY) — the window stays
// open-transaction, nothing is stored, and the cancel is reported.
func TestBackupStream_CancelInsideATransactionStillAbandons(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &BackupStream{segStore: store, segCodec: blobcodec.CodecGzip, stopPollInterval: time.Hour}
	m := &irbackup.Manifest{FormatVersion: irbackup.BackupFormatVersion, SourceEngine: "postgres", Kind: irbackup.BackupKindIncremental}
	ch := make(chan ir.Change, 2)
	ch <- ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/1"}`}}
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
