// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// capturingApplier records the position of every change it receives (and
// every position it is asked to persist), for asserting the broker's
// position-atomicity invariant. It implements the batched + position-writer
// surfaces the broker uses.
type capturingApplier struct {
	mu       sync.Mutex
	received []ir.Position
	written  []ir.Position
}

func (a *capturingApplier) EnsureControlTable(context.Context) error { return nil }
func (a *capturingApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (a *capturingApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) { return nil, nil }
func (a *capturingApplier) RequestStop(context.Context, string) error              { return nil }
func (a *capturingApplier) ClearStopRequested(context.Context, string) error       { return nil }

func (a *capturingApplier) Apply(_ context.Context, _ string, ch <-chan ir.Change) error {
	for c := range ch {
		a.rec(c.Pos())
	}
	return nil
}

func (a *capturingApplier) ApplyBatch(_ context.Context, _ string, ch <-chan ir.Change, _ int) error {
	for c := range ch {
		a.rec(c.Pos())
	}
	return nil
}

func (a *capturingApplier) WritePosition(_ context.Context, _ string, p ir.Position) error {
	a.mu.Lock()
	a.written = append(a.written, p)
	a.mu.Unlock()
	return nil
}

func (a *capturingApplier) rec(p ir.Position) {
	a.mu.Lock()
	a.received = append(a.received, p)
	a.mu.Unlock()
}

// TestBroker_PartialIncremental_PositionStaysAtParent pins BRK-1 (the
// confirmed silent-loss): when a later chunk of a multi-chunk incremental
// fails, every change the applier received carries the PARENT resume token,
// never this incremental's own backupID. So any position a partial commit
// persists is the parent, and a restart re-applies the incremental whole
// (idempotent) instead of skipping it and silently losing its un-applied
// tail. Before the fix, changes carried the incremental's own backupID and a
// partial batch committed at it, over-advancing the position.
func TestBroker_PartialIncremental_PositionStaysAtParent(t *testing.T) {
	ctx := context.Background()
	store, incrPath := encryptedChainFixture(t)

	// Tamper the LAST change chunk (byte-flip + SHA fixup) so the earlier
	// chunks stream cleanly (emitting changes) but the final chunk fails its
	// GCM auth tag at open — the multi-chunk partial-apply scenario.
	im, err := lineage.ReadManifestAt(ctx, store, incrPath)
	if err != nil {
		t.Fatalf("read incremental manifest: %v", err)
	}
	if len(im.ChangeChunks) < 2 {
		t.Fatalf("fixture produced %d change chunks; need >= 2 for a partial-apply", len(im.ChangeChunks))
	}
	last := im.ChangeChunks[len(im.ChangeChunks)-1]
	last.SHA256 = flipStoreByte(t, store, last.File)
	if err := lineage.WriteManifestAt(ctx, store, incrPath, im); err != nil {
		t.Fatalf("rewrite incremental manifest: %v", err)
	}

	// Rebuild the envelope against the chain root's recorded Argon2id salt
	// (a fresh envelope would derive a different KEK and fail the unwrap).
	root, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("read root manifest: %v", err)
	}
	rp := root.ChainEncryption.Argon2id
	env, err := crypto.NewPassphraseEnvelope(tamperPassphrase, crypto.Argon2idParams{
		Salt: rp.Salt, Memory: rp.Memory, Iterations: rp.Iterations,
		Parallelism: rp.Parallelism, KeyLen: rp.KeyLen,
	})
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	b := &SyncFromBackup{
		Store:    store,
		ChainURL: "test://brk1",
		StreamID: "s",
		Envelope: env,
	}
	if err := b.preflightChainEncryption(ctx); err != nil {
		t.Fatalf("preflightChainEncryption: %v", err)
	}
	chain, err := b.brokerChain(ctx)
	if err != nil {
		t.Fatalf("brokerChain: %v", err)
	}
	var incrLink *lineage.SegmentRecord
	for i := range chain {
		if chain[i].Manifest.Kind == irbackup.BackupKindIncremental {
			incrLink = &chain[i]
			break
		}
	}
	if incrLink == nil {
		t.Fatal("no incremental link in fixture chain")
	}
	incrID := lineage.ManifestBackupID(incrLink.Manifest)
	incrToken := encodeBrokerPosition(b.ChainURL, incrID)
	parentToken := encodeBrokerPosition(b.ChainURL, "PARENT-RESUME-TOKEN")

	app := &capturingApplier{}
	if _, err := b.applyIncremental(ctx, app, incrLink, 100, "PARENT-RESUME-TOKEN"); err == nil {
		t.Fatal("tampered incremental applied cleanly; the BRK-1 partial-apply path was not exercised")
	}

	if len(app.received) == 0 {
		t.Fatal("no changes reached the applier before the failure; the partial-apply scenario needs the earlier chunk to stream")
	}
	for _, p := range app.received {
		if p == incrToken {
			t.Errorf("a change carried the incremental's OWN backupID token before the failure — a partial apply could over-advance and silently skip it on restart (BRK-1)")
		}
		if p != parentToken {
			t.Errorf("change position %+v != parent resume token %+v", p, parentToken)
		}
	}
	for _, p := range app.written {
		if p == incrToken {
			t.Errorf("the incremental's backupID position was persisted despite the mid-stream failure (BRK-1 over-advance)")
		}
	}
}
