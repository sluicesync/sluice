// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pipeline

import (
	"context"
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

// TestItem60_AnchorImpliesSchemaDelta_MySQL is the MySQL-binlog half of the
// item-60 ground truth (see the PG file for the full rationale). OBSERVED here:
// like PG pgoutput, a pure DDL-only window on MySQL produces an EMPTY
// EndPosition (posBearing false) and never anchors a snapshot at a
// position-bearing EndPosition — the MySQL deferred snapshot needs a following
// row event to acquire a position, so a DDL alone barely advances the stream.
// (The earlier assumption that MySQL's binlog QueryEvent anchors a DDL-only
// snapshot AT EndPosition was disproven by this run — audit-2026-07-12.) So on
// BOTH engines the "anchor AT a position-bearing EndPosition, 0 chunks" shape
// has no legitimate producer; restore/broker therefore no longer trust the
// anchor at all and rest completeness on the change-chunk tail.
//
// Records the ground truth for both engines and asserts the invariant that
// justified retiring anchor-trust:
//
//	SchemaHistoryAnchors(EndPosition) && posBearing && claimsAdvance
//	    =>  len(SchemaDelta) > 0
func TestItem60_AnchorImpliesSchemaDelta_MySQL(t *testing.T) {
	for _, tc := range []struct {
		name            string
		windowSQL       string
		wantSchemaDelta bool
	}{
		{
			name:            "pure ADD COLUMN (no DML)",
			windowSQL:       `ALTER TABLE users ADD COLUMN nickname VARCHAR(50) NULL;`,
			wantSchemaDelta: true,
		},
		{
			name:            "ALTER COLUMN TYPE param change (no DML)",
			windowSQL:       `ALTER TABLE users MODIFY COLUMN email VARCHAR(512) NOT NULL;`,
			wantSchemaDelta: true,
		},
		{
			// The scenario that would make the gate UNSAFE if the reader
			// anchored a snapshot at EndPosition for it while DiffSchemas
			// (which ignores CheckConstraints) returned an empty delta.
			name:            "ADD CHECK CONSTRAINT only (no DML)",
			windowSQL:       `ALTER TABLE users ADD CONSTRAINT email_nonempty CHECK (CHAR_LENGTH(email) > 0);`,
			wantSchemaDelta: false,
		},
		{
			name:            "first-touch DATA window (INSERT, no DDL)",
			windowSQL:       `INSERT INTO users (email) VALUES ('carol@example.com');`,
			wantSchemaDelta: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourceDSN, _, cleanup := startMySQLBinlog(t)
			defer cleanup()

			const seedDDL = `
				CREATE TABLE users (
					id    BIGINT       NOT NULL AUTO_INCREMENT,
					email VARCHAR(255) NOT NULL,
					PRIMARY KEY (id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
				INSERT INTO users (email) VALUES ('alice@example.com');
			`
			applyDDLMySQL(t, sourceDSN, seedDDL)

			mysqlEng, ok := engines.Get("mysql")
			if !ok {
				t.Fatal("mysql engine not registered")
			}
			dir := t.TempDir()
			store, _ := blobcodec.NewLocalStore(dir)

			if err := (&backup.Backup{Source: mysqlEng, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test"}).Run(context.Background()); err != nil {
				t.Fatalf("Backup.Run: %v", err)
			}
			binlogFile, binlogPos := readMySQLBinlogPos(t, sourceDSN)
			full, _ := lineage.ReadManifest(context.Background(), store)
			full.Kind = irbackup.BackupKindFull
			full.EndPosition = ir.Position{
				Engine: "mysql",
				Token:  fmt.Sprintf(`{"mode":"file_pos","file":%q,"pos":%d}`, binlogFile, binlogPos),
			}
			full.BackupID = irbackup.ComputeBackupID(full)
			if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
				t.Fatalf("rewrite full: %v", err)
			}

			applyDDLMySQL(t, sourceDSN, tc.windowSQL)

			incrCtx, incrCancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer incrCancel()
			if err := (&IncrementalBackup{
				Source:        mysqlEng,
				SourceDSN:     sourceDSN,
				Store:         store,
				ParentRef:     full.BackupID,
				Window:        15 * time.Second,
				MaxChanges:    20,
				ChunkChanges:  10,
				SluiceVersion: "test",
			}).Run(incrCtx); err != nil {
				t.Fatalf("IncrementalBackup.Run: %v", err)
			}

			records, _ := lineage.ListAllManifestsViaWalk(context.Background(), store)
			var incr *irbackup.Manifest
			for _, r := range records {
				if r.Manifest.Kind == irbackup.BackupKindIncremental {
					incr = r.Manifest
				}
			}
			if incr == nil {
				t.Fatal("no incremental manifest found")
			}

			anchorsAtEnd := incr.SchemaHistoryAnchors(incr.EndPosition)
			posBearing := incr.EndPosition.Engine != "" || incr.EndPosition.Token != ""
			claimsAdvance := incr.EndPosition != incr.StartPosition

			t.Logf("[item60 ground truth MySQL] scenario=%q chunks=%d schemaDelta=%d "+
				"anchorsAtEnd=%v posBearing=%v claimsAdvance=%v end=%+v start=%+v history=%d",
				tc.name, len(incr.ChangeChunks), len(incr.SchemaDelta),
				anchorsAtEnd, posBearing, claimsAdvance,
				incr.EndPosition, incr.StartPosition, len(incr.SchemaHistory))

			if anchorsAtEnd && posBearing && claimsAdvance && len(incr.SchemaDelta) == 0 {
				t.Errorf("INVARIANT VIOLATED: scenario %q has a schema anchor AT EndPosition on a "+
					"position-advancing window but an EMPTY SchemaDelta — the len(SchemaDelta)>0 "+
					"gate would FALSE-REFUSE this legit window. Gate (b) is UNSAFE for this shape.",
					tc.name)
			}

			if got := len(incr.SchemaDelta) > 0; got != tc.wantSchemaDelta {
				t.Logf("[item60 note MySQL] scenario %q: len(SchemaDelta)>0 = %v, code-reading predicted %v",
					tc.name, got, tc.wantSchemaDelta)
			}
		})
	}
}
