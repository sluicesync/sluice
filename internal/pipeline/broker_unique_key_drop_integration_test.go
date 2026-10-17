//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// UNIQUE-KEY-STOPPED-WINDOW-DOOR's backup-chain sibling. `sync from-backup
// run` and a chain restore do not run the stream's unique-key door; they
// replay a backup window whose schema delta records the source's DROP
// INDEX (the incremental's END schema against its parent's), and apply it
// to the target BEFORE the window's rows (broker.go "Schema deltas first";
// migcore.applyAlterIndexAspect, AlterAspectIndexDropped). This pins that
// claim: source kd(id PK, k UNIQUE kd_k, v), the window drops kd_k and
// inserts (99,'a','ninetynine') beside (1,'a','one'). Both the broker and
// a chain restore into a fresh target must end with every source row by
// id and without kd_k — not with row 1 overwritten by 99, the merge the
// stream door exists for.
//
// The independent expected value is the source's rows (ukdRows) against
// the target's.

package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

func TestSyncFromBackup_UniqueKeyDroppedInsideTheWindow(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	root := twfbDB{engine: "mysql", dsn: myRoot}
	root.exec(t, "CREATE DATABASE bukd_s; CREATE DATABASE bukd_t; CREATE DATABASE bukd_r")
	dsn := func(db string) string {
		d, err := buildMySQLDSN(myRoot, db)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	src, tgt, restored := twfbDB{"mysql", dsn("bukd_s")}, twfbDB{"mysql", dsn("bukd_t")}, twfbDB{"mysql", dsn("bukd_r")}
	src.exec(t, ukdShapePK.myCreate)

	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Backup{Source: myEng, SourceDSN: src.dsn, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("full: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Restore{Target: myEng, TargetDSN: tgt.dsn, Store: store}).Run(ctx); err != nil {
		t.Fatalf("seed restore: %v", err)
	}
	if err := krwCapture(t, "backup-incremental", myEng, src.dsn, "", store, full.BackupID, src,
		[]string{ukdShapePK.myDrop, ukdShapePK.duplicate}); err != nil {
		t.Fatalf("incremental: %v", err)
	}

	hasKey := func(db twfbDB) string {
		return db.scalar(t, "SELECT count(*) FROM information_schema.statistics WHERE table_schema = DATABASE() "+
			"AND table_name = 'kd' AND index_name = 'kd_k'")
	}

	t.Run("broker", func(t *testing.T) {
		bctx, bcancel := context.WithTimeout(ctx, 30*time.Second)
		defer bcancel()
		err := (&SyncFromBackup{
			Target: myEng, TargetDSN: tgt.dsn, Store: store,
			ChainURL: "test://bukd", StreamID: "bukd",
			PollInterval: time.Second, AtChainID: full.BackupID, SluiceVersion: "test",
		}).Run(bctx)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("broker: %v", err)
		}
		if s, g := ukdRows(t, src), ukdRows(t, tgt); s != g {
			t.Fatalf("the broker did not converge (a merge through the target's kd_k?)\nsource %s\ntarget %s", s, g)
		}
		if n := hasKey(tgt); n != "0" {
			t.Errorf("the broker's target still has kd_k (%s rows in statistics)", n)
		}
	})

	t.Run("chain restore", func(t *testing.T) {
		if err := (&backup.Restore{Target: myEng, TargetDSN: restored.dsn, Store: store}).Run(ctx); err != nil {
			t.Fatalf("chain restore: %v", err)
		}
		if s, g := ukdRows(t, src), ukdRows(t, restored); s != g {
			t.Fatalf("the chain restore did not converge\nsource %s\ntarget %s", s, g)
		}
		if n := hasKey(restored); n != "0" {
			t.Errorf("the restored target still has kd_k (%s rows in statistics)", n)
		}
	})
}
