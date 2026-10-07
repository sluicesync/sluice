//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestBroker_BarrierFoldKeepsTheParentToken pins ADR-0190 amendment E on the
// `sync from-backup` broker (§E.7), as ADR-0191 §13 R6 restates it: the
// broker drives the same lane orchestrator, so its barriers fold their
// pre-apply checkpoints too — and what they fold must be a frontier token
// whose last-applied incremental is still the PARENT, standing INSIDE the
// interrupted incremental at a source transaction's COMMIT, never the
// incremental's own id as fully applied. BRK-1's invariant is that the
// advance to an incremental's id happens only in the post-stream
// writePositionDirect; ADR-0191 lets the position stand inside it, at a
// boundary only.
//
// The chain's incremental carries many source transactions, each a primary-
// key change (a lane barrier) after an insert, so every barrier after the
// first has a checkpoint to fold. A middle change chunk is corrupted, so the
// run fails after barriers have folded. The persisted position must name the
// parent (the full) as last applied, and if it stands inside the incremental
// the event it names must be a TxCommit — read back from the chain's own
// chunks, not from the broker.
func TestBroker_BarrierFoldKeepsTheParentToken(t *testing.T) {
	const txs = 150
	c := fe1SetupWith(t, `
		CREATE TABLE k (id INT PRIMARY KEY, note TEXT);
		INSERT INTO k VALUES (-1, 'seed');
	`, func(t *testing.T, src string) {
		for i := 1; i <= txs; i++ {
			applyDDL(t, src, fmt.Sprintf(`BEGIN; INSERT INTO k VALUES (%d, 'r'); UPDATE k SET id = %d WHERE id = %d; COMMIT;`, i, i+100000, i))
		}
	}, nil)
	incrID := lineage.ManifestBackupID(c.incr.Manifest)
	streamID := "bfold-parent"

	putBack := c.corruptMiddleChunk(t)
	err := c.broker(streamID, 0, c.fullID).Run(context.Background())
	putBack()
	if err == nil {
		t.Fatal("the broker ran over a corrupted chunk without an error; the interrupted-incremental shape was not built")
	}
	moved := fe1Q(t, c.dst, `SELECT count(*)::text FROM k WHERE id > 100000`)
	if moved == "0" {
		t.Fatal("no primary-key change reached the target before the failure: no barrier ran, so the cell grades nothing")
	}
	pos := fe1Q(t, c.dst, fmt.Sprintf(`SELECT coalesce(string_agg(source_position, ','), '') FROM sluice_cdc_state WHERE stream_id = '%s'`, streamID))
	t.Logf("after the failure: %s keys moved, persisted position %q (parent %s, incremental %s)", moved, pos, c.fullID, incrID)
	tok, err := decodeBrokerPosition(ir.Position{Token: pos})
	if err != nil {
		t.Fatalf("the persisted position %q is not a broker token: %v", pos, err)
	}
	if tok.LastAppliedBackupID != c.fullID {
		t.Fatalf("the persisted position %q names %q as fully applied; want the parent %s: a position written while the "+
			"incremental streamed advanced past its parent, and a restart would skip its un-applied tail (BRK-1)", pos, tok.LastAppliedBackupID, c.fullID)
	}
	if tok.InProgress == nil {
		t.Fatalf("the persisted position %q does not stand inside the incremental after barriers folded checkpoints: "+
			"the frontier (ADR-0191 §3.2) never reached the target", pos)
	}
	if tok.InProgress.BackupID != incrID {
		t.Fatalf("the frontier stands inside %s; want the interrupted incremental %s", tok.InProgress.BackupID, incrID)
	}
	if kind := eventKindAt(t, c, tok.InProgress.Through); kind != "ir.TxCommit" {
		t.Fatalf("the persisted frontier names event %d, a %s: a barrier folded a position that is not a source-transaction "+
			"boundary, and a restart would resume inside a transaction", tok.InProgress.Through, kind)
	}
}

// eventKindAt decodes the data incremental's chunks in order and returns the
// Go type of the event at ordinal n.
func eventKindAt(t *testing.T, c *fe1Chain, n int64) string {
	t.Helper()
	ctx := context.Background()
	var ord int64
	for idx, chunk := range c.incr.Manifest.ChangeChunks {
		src, err := blobcodec.FetchChunkVerified(ctx, c.incr.Segment.Store(c.store), chunk.File, chunk.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		cr, err := blobcodec.NewChangeChunkReader(src, chunk.SHA256, nil, c.incr.Segment.CodecOrDefault(), irbackup.ChangeChunkAADFor(c.incr.Manifest, chunk, idx))
		if err != nil {
			t.Fatal(err)
		}
		for {
			ch, err := cr.ReadChange()
			if err != nil {
				break
			}
			if ord == n {
				_ = cr.Close()
				return fmt.Sprintf("%T", ch)
			}
			ord++
		}
		_ = cr.Close()
	}
	t.Fatalf("the incremental has only %d events; the frontier names %d", ord, n)
	return ""
}
