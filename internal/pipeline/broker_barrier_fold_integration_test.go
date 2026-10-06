//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestBroker_BarrierFoldKeepsTheParentToken pins ADR-0190 amendment E on the
// `sync from-backup` broker (§E.7): the broker drives the same lane
// orchestrator, so its barriers fold their pre-apply checkpoints too — and
// what they fold must be the PARENT resume token, never the incremental's
// own id. BRK-1's invariant is that the advance to an incremental's id
// happens only in the post-stream writePositionDirect: every change of an
// incremental carries the parent token, so any position written while it
// streams — a checkpoint's or, now, a barrier's — names the parent, and a
// run interrupted mid-incremental re-applies it whole.
//
// The chain's incremental carries many source transactions, each a primary-
// key change (a lane barrier) after an insert, so every barrier after the
// first has a checkpoint to fold. A middle change chunk is corrupted, so the
// run fails after barriers have folded. The persisted position must name the
// parent (the full); the incremental's id must appear nowhere in it.
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
	if strings.Contains(pos, incrID) {
		t.Fatalf("the persisted position %q names the interrupted incremental %s: a position written while it streamed "+
			"advanced past the parent, and a restart would skip its un-applied tail (BRK-1)", pos, incrID)
	}
	if !strings.Contains(pos, c.fullID) {
		t.Fatalf("the persisted position %q does not name the parent %s: the barriers' folded checkpoints wrote something else", pos, c.fullID)
	}
}
