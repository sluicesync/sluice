//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Review 2026-10-12 finding 4: the `backup stream` ctx-cancel drain-commit
// skips the schema refresh (the source read cannot run on the cancelled
// context), so a window that changed a key column's collation committed
// with no SchemaDelta — and so with no Bug 304 stamp. MEASURED before the
// fix (MySQL 8.4): FormatVersion 1, SchemaDelta 0, SchemaHistory 1 carrying
// the post-DDL `utf8mb4_bin` key; a pre-v0.159.1 binary reads such a
// segment and, with no key-collation guard before v0.159.0, replays 'A'
// onto the old `_ci` key, merging it at exit 0.
//
// The independent expected value is the source's rows read by id:HEX(k) and
// the drain segment's recorded format version, graded per cell: the key
// change stamps FormatVersionKeyCollationDelta, the non-key control keeps
// its version. The cell also pins what THIS binary does with the stamped
// segment today (KEY-COLLATION-DRAIN-NO-DELTA): its restore replays the
// window onto the old key and the applier guard refuses the merge loudly,
// with no merged row on the target.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

func TestBackupStream_DrainCommit_StampsAKeyCollationWindow(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	cells := []struct {
		name    string
		ddl     string
		stamped bool
	}{
		{"key collation change", "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL", true},
		{"control: non-key collation change", "ALTER TABLE kd MODIFY note VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL", false},
	}
	for i, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			srcDB, tgtDB := fmt.Sprintf("dkc_s%d", i), fmt.Sprintf("dkc_t%d", i)
			twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE "+srcDB+"; CREATE DATABASE "+tgtDB)
			srcDSN, err := buildMySQLDSN(myRoot, srcDB)
			if err != nil {
				t.Fatal(err)
			}
			tgtDSN, err := buildMySQLDSN(myRoot, tgtDB)
			if err != nil {
				t.Fatal(err)
			}
			src, tgt := twfbDB{engine: "mysql", dsn: srcDSN}, twfbDB{engine: "mysql", dsn: tgtDSN}
			src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL UNIQUE, "+
				"d INT, note VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL); "+
				"INSERT INTO kd VALUES (1,'a',1,'x'),(2,'b',2,'y'),(3,'c',3,'z')")
			ctx := context.Background()
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := (&backup.Backup{Source: myEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
				t.Fatalf("full: %v", err)
			}
			full, err := lineage.ReadManifest(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() {
				// One long window: only the cancel ends it, so the segment is the
				// drain-commit's.
				done <- (&BackupStream{
					Source: myEng, SourceDSN: srcDSN, Store: store, ParentRef: full.BackupID,
					RolloverWindow: 10 * time.Minute, RolloverMaxChanges: 1 << 30, RolloverMaxBytes: 1 << 40,
					ChunkChanges: 100, SluiceVersion: "test",
				}).Run(runCtx)
			}()
			src.exec(t, cell.ddl)
			if cell.stamped {
				// Distinct under the new `_bin`, one key under the old `_ci`.
				src.exec(t, "INSERT INTO kd VALUES (99,'A',99,'w')")
			} else {
				src.exec(t, "INSERT INTO kd VALUES (99,'q',99,'w')")
			}
			src.exec(t, "INSERT INTO kd VALUES (100,'zz',100,'v')")
			drain := drainSegmentWhenCaptured(t, store, cancel, done, 3)

			if len(drain.SchemaDelta) != 0 {
				t.Fatalf("the drain segment carries a SchemaDelta (%d entries) — the drain now refreshes the schema, "+
					"and this pin's premise (and KEY-COLLATION-DRAIN-NO-DELTA) needs re-reading", len(drain.SchemaDelta))
			}
			if len(drain.SchemaHistory) == 0 {
				t.Fatal("the drain segment recorded no SchemaHistory — the cell exercises nothing")
			}
			stamped := drain.FormatVersion >= irbackup.FormatVersionKeyCollationDelta
			if stamped != cell.stamped {
				t.Fatalf("drain segment FormatVersion = %d; stamped = %v, want %v", drain.FormatVersion, stamped, cell.stamped)
			}
			if !cell.stamped {
				return
			}
			// This binary's own restore of the stamped segment: the window
			// replays onto the old `_ci` key, and the guard refuses the merge.
			restoreErr := (&backup.ChainRestore{Target: myEng, TargetDSN: tgtDSN, Store: store}).Run(ctx)
			if !errors.Is(restoreErr, ir.ErrKeyCollationMerge) {
				t.Fatalf("restore = %v; want %v (target %s)", restoreErr, ir.ErrKeyCollationMerge, kcfbRender(kcfbRows(t, tgt)))
			}
			kcfbGrade(t, "drain/"+cell.name, src, tgt, restoreErr)
		})
	}
}

// drainSegmentWhenCaptured gives the stream time to capture the source's
// writes, cancels it (the drain-commit), and returns the single incremental
// segment it committed, which must hold at least want change records.
func drainSegmentWhenCaptured(t *testing.T, store irbackup.Store, cancel context.CancelFunc, done <-chan error, want int) *irbackup.Manifest {
	t.Helper()
	// The window's captured changes are not observable until it commits;
	// give the reader time to see the source's writes, then drain.
	time.Sleep(6 * time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("backup stream: %v", err)
	}
	records, err := lineage.ListAllSegmentManifests(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	var drain *irbackup.Manifest
	for _, rec := range records {
		if rec.Manifest.Kind != irbackup.BackupKindIncremental {
			continue
		}
		if drain != nil {
			t.Fatal("the stream committed more than one segment — the window was not ended by the drain")
		}
		drain = rec.Manifest
	}
	if drain == nil {
		t.Fatal("the drain committed no segment")
	}
	if n := manifestChangeRecordCount(drain); n < int64(want) {
		t.Fatalf("the drain segment holds %d change records; want at least %d (the window closed before the writes)", n, want)
	}
	return drain
}
