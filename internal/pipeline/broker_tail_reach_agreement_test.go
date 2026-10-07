// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The broker column of the shape-(C) agreement table. The table itself —
// door, chain restore and `backup verify` — is
// backup.TestSeveredTransactionDoor_ShapeC_AgreementTable; this package
// cannot be imported from there (pipeline imports backup), so the broker's
// in-apply backstop (streamIncrementalWithPosition) is graded here over the
// same rows, restated. The rows are the same shapes by name; a row added
// there and not here is a gap in THIS column only, and the broker's backstop
// calls the same backup.EndPositionUnreached the others do.

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func tailLSN(n int) ir.Position {
	return ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"lsn":%d}`, n)}
}

func tailTx(lsn int, rows ...string) []ir.Change {
	out := []ir.Change{ir.TxBegin{Position: tailLSN(lsn)}}
	for _, r := range rows {
		out = append(out, ir.Insert{Position: tailLSN(lsn), Table: "kl", Row: ir.Row{"v": r}})
	}
	return append(out, ir.TxCommit{Position: tailLSN(lsn)})
}

// tailLink writes changes as one change chunk (an empty list writes one
// header-only chunk, the shape a DDL-only rollover leaves) and returns the
// link.
func tailLink(t *testing.T, store irbackup.Store, name string, changes []ir.Change, start, end ir.Position) lineage.SegmentRecord {
	t.Helper()
	var buf bytes.Buffer
	cw, err := blobcodec.NewChangeChunkWriter(&buf, nil, blobcodec.CodecGzip, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if err := cw.WriteChange(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := cw.Close(); err != nil {
		t.Fatal(err)
	}
	path := "chunks/_changes/" + name + "/changes-0.jsonl.gz"
	if err := store.Put(context.Background(), path, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	return lineage.SegmentRecord{
		Segment: &lineage.Segment{Codec: blobcodec.CodecGzip},
		ManifestRecord: lineage.ManifestRecord{
			Path: "manifests/" + name + ".json",
			Manifest: &irbackup.Manifest{
				BackupID: name, SourceEngine: "postgres", Kind: irbackup.BackupKindIncremental,
				StartPosition: start, EndPosition: end,
				ChangeChunks: []*irbackup.ChunkInfo{{File: path, RowCount: cw.ChangeCount(), SHA256: cw.Hash()}},
			},
		},
	}
}

func TestShapeCAgreement_BrokerTailBackstop(t *testing.T) {
	full := lineage.SegmentRecord{
		Segment:        &lineage.Segment{Codec: blobcodec.CodecGzip},
		ManifestRecord: lineage.ManifestRecord{Path: lineage.ManifestFileName, Manifest: &irbackup.Manifest{BackupID: "full", SourceEngine: "postgres", Kind: irbackup.BackupKindFull}},
	}
	rows := []struct {
		name       string
		changes    []ir.Change
		start, end ir.Position
		refuse     bool
	}{
		{name: "ends on TxCommit, EndPosition = its position", changes: append(tailTx(10, "a"), tailTx(20, "b")...), start: tailLSN(1), end: tailLSN(20)},
		{name: "keepalive boundary last", changes: append(tailTx(10, "a"), ir.TxBegin{Position: tailLSN(50)}, ir.TxCommit{Position: tailLSN(55)}), start: tailLSN(1), end: tailLSN(55)},
		{name: "marker-less trigger-CDC stream ends on its last row's id", changes: []ir.Change{ir.Insert{Position: tailLSN(7), Table: "t", Row: ir.Row{"v": 1}}, ir.Insert{Position: tailLSN(8), Table: "t", Row: ir.Row{"v": 2}}}, start: tailLSN(1), end: tailLSN(8)},
		{name: "empty EndPosition", changes: []ir.Change{ir.TxBegin{}, ir.TxCommit{}}, start: tailLSN(1)},
		{name: "EndPosition = StartPosition", changes: tailTx(10, "a"), start: tailLSN(5), end: tailLSN(5)},
		{name: "DDL-only rollover: header-only chunk, EndPosition = StartPosition", start: tailLSN(5), end: tailLSN(5)},
		{name: "trailing position-less TxCommit", changes: append(tailTx(10, "a")[:2], ir.TxCommit{}), start: tailLSN(1), end: tailLSN(10)},
		{name: "old cancel-drain drop", changes: tailTx(10, "a"), start: tailLSN(1), end: tailLSN(30), refuse: true},
		{name: "advanced EndPosition with no positioned change", changes: []ir.Change{ir.TxBegin{}, ir.TxCommit{}}, start: tailLSN(1), end: tailLSN(9), refuse: true},
		{name: "header-only chunk with an advanced EndPosition", start: tailLSN(5), end: tailLSN(9), refuse: true},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			link := tailLink(t, store, "x", tc.changes, tc.start, tc.end)
			doorErr := backup.NewSeveredTransactionDoor(store, nil, nil).Check(ctx, []lineage.SegmentRecord{full, link})
			doorRefused := doorErr != nil && codeOfErr(doorErr) == sluicecode.CodeBackupIncomplete
			if doorErr != nil && !doorRefused {
				t.Fatalf("door refused for another reason: %v", doorErr)
			}

			out := make(chan ir.Change, 64)
			go func() {
				for range out { //nolint:revive // drain
				}
			}()
			b := &SyncFromBackup{Store: store}
			brokerErr := b.streamIncrementalWithPosition(ctx, &link, newBrokerFrontier("x", "parent", link.Manifest, -1), out)
			close(out)
			brokerRefused := brokerErr != nil && codeOfErr(brokerErr) == sluicecode.CodeBackupIncomplete
			if brokerErr != nil && !brokerRefused {
				t.Fatalf("broker backstop failed for another reason: %v", brokerErr)
			}

			if doorRefused != tc.refuse || brokerRefused != tc.refuse {
				t.Fatalf("want refuse=%v; door refused=%v, broker backstop refused=%v (%v)", tc.refuse, doorRefused, brokerRefused, brokerErr)
			}
		})
	}
}

func codeOfErr(err error) sluicecode.Code {
	if ce, ok := sluicecode.FromError(err); ok {
		return ce.Code
	}
	return ""
}
