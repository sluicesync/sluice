//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION review F-C, end to end: chain restore applies an
// incremental's window through the target applier, which arms the
// key-collation guard from the window's SchemaSnapshot — a schema DECODED
// from the manifest. ir.MarshalTable used to drop Char/Varchar/Text
// .Determinism, so a Postgres `COLLATE "C"` key decoded as Unknown and a
// Postgres-to-MySQL replay went unguarded. The MySQL emitter drops the
// Postgres collation and the column lands on the database default
// (`utf8mb4_0900_ai_ci`), which equates 'a' and 'A'; the source's "C"
// keeps them apart.
//
// The independent expected value is the Postgres source's own rows read
// by id:HEX(k). Either the restored target equals them key for key, or the
// restore refused and every row the target holds is the source's row with
// the same bytes.

package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

func TestKeyCollationFirstBoundary_ChainRestore_PGCollateCToMySQL(t *testing.T) {
	srcRoot, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	// A `text` key is not a shape here: MySQL cannot index TEXT without a
	// prefix length, and chain restore refuses it before any data moves
	// (measured: "Error 1170 … Refusing before any data moves") — loud, so
	// nothing for the guard to do.
	shapes := []struct{ name, keyType string }{
		{"varchar", "varchar(20)"},
		{"char", "char(4)"},
	}
	for i, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			srcDB, tgtDB := fmt.Sprintf("kccr_s%d", i), fmt.Sprintf("kccr_t%d", i)
			twfbDB{engine: "postgres", dsn: srcRoot}.exec(t, "CREATE DATABASE "+srcDB)
			srcDSN, err := buildPGDSN(srcRoot, srcDB)
			if err != nil {
				t.Fatal(err)
			}
			twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE "+tgtDB)
			tgtDSN, err := buildMySQLDSN(myRoot, tgtDB)
			if err != nil {
				t.Fatal(err)
			}
			src, tgt := twfbDB{engine: "postgres", dsn: srcDSN}, twfbDB{engine: "mysql", dsn: tgtDSN}
			src.exec(t, "CREATE TABLE kd (id INT NOT NULL, k "+sh.keyType+` COLLATE "C" NOT NULL PRIMARY KEY, d INT); `+
				"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3)")
			ctx := context.Background()
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := (&backup.Backup{Source: pgEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test", ChainSlot: true}).Run(ctx); err != nil {
				t.Fatalf("full: %v", err)
			}
			defer dropPGLogicalSlot(t, srcDSN, "sluice_slot")
			full, err := lineage.ReadManifest(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			// Distinct under the source's "C", one key under the target's
			// default 0900_ai_ci.
			src.exec(t, "INSERT INTO kd VALUES (99,'A',99); INSERT INTO kd VALUES (100,'z',100)")
			runCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			capErr := (&IncrementalBackup{
				Source: pgEng, SourceDSN: srcDSN, Store: store, ParentRef: full.BackupID,
				Window: 5 * time.Second, SluiceVersion: "test",
			}).Run(runCtx)
			cancel()
			if capErr != nil {
				t.Fatalf("capture: %v", capErr)
			}
			what := "chain-restore/pg-C-to-mysql/" + sh.name
			restoreErr := (&backup.ChainRestore{Target: myEng, TargetDSN: tgtDSN, Store: store}).Run(ctx)
			if restoreErr == nil {
				kcfbGrade(t, what, src, tgt, nil)
				return
			}
			t.Logf("%s: restore refused: %v", what, restoreErr)
			if tgt.scalar(t, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'kd'") != "1" {
				t.Fatalf("%s: the restore refused before the table existed, so this pin graded nothing: %v", what, restoreErr)
			}
			kcfbGrade(t, what, src, tgt, restoreErr)
		})
	}
}
