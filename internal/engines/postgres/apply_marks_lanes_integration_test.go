//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// applyLaneTxsAcrossCheckpoints streams txs identity-carrying source
// transactions through ONE ApplyBatch run (batch size > 1, so the lanes
// the caller wired take them) and, after each, waits until the persisted
// position reaches that transaction's commit — so the run crosses a
// checkpoint per transaction, each closing a transaction (the Bug 293
// CloseTxs path), rather than one final checkpoint at channel close.
// rows(n) is transaction n's row changes; their positions and apply
// identities are stamped here.
func applyLaneTxsAcrossCheckpoints(ctx context.Context, t *testing.T, a *ChangeApplier, streamID string, txs int, rows func(n int64) []ir.Change) {
	t.Helper()
	ch := make(chan ir.Change, 64)
	done := make(chan error, 1)
	go func() { done <- a.ApplyBatch(ctx, streamID, ch, 50) }()
	for n := int64(1); n <= int64(txs); n++ {
		pos := ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"lsn":"0/%X"}`, 0x2000000+n*0x100)}
		txID := fmt.Sprintf("lanes-tx-%d", n)
		ch <- ir.TxBegin{Position: pos}
		for i, c := range rows(n) {
			id := ir.ApplyID{TxID: txID, Seq: uint64(i + 1)}
			switch v := c.(type) {
			case ir.Insert:
				v.Position, v.ApplyID = pos, id
				c = v
			case ir.Update:
				v.Position, v.ApplyID = pos, id
				c = v
			case ir.Delete:
				v.Position, v.ApplyID = pos, id
				c = v
			}
			ch <- c
		}
		ch <- ir.TxCommit{Position: pos}
		deadline := time.Now().Add(30 * time.Second)
		for {
			select {
			case err := <-done:
				t.Fatalf("ApplyBatch returned before transaction %d was checkpointed: %v", n, err)
			default:
			}
			got, ok, err := a.ReadPosition(ctx, streamID)
			if err == nil && ok && got.Token == pos.Token {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the position never reached transaction %d's commit %s (last %q, ok=%v, err=%v): its checkpoint did not persist",
					n, pos.Token, got.Token, ok, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	close(ch)
	if err := <-done; err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
}
