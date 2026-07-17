// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// auditVStreamSrcName is a stand-in SourceEngine name for a VStream flavour
// (PlanetScale / Vitess), used to show that the emptied-window refusal is
// engine-agnostic. It is manifest DATA only — since the item-60 close
// (e722fb81: restore no longer trusts a schema anchor at EndPosition, so it
// never consults the engine registry for the recorded source engine) nothing
// looks the name up, so no registry entry is registered for it. This test
// file previously Register()ed a stub engine under this name from an init(),
// mutating the process-global registry for every test in the package
// (roadmap item 72 leftover); the registration was vestigial and is gone.
const auditVStreamSrcName = "audit_vstream_src"

// TestVerifyBackupIDs_EmptyIDIncremental_Refused pins audit-2026-07-11 H-1
// facet (b): a CDC segment (incremental / streaming) has recorded a BackupID
// since Phase 3, so an EMPTY one is never writer-legitimate — it is a store
// adversary blanking the id to slip the recompute-verify (and, on an FV8
// VStream segment, to un-bind the folded CDCPositionCommitsAfterRows flag).
// verifyBackupIDs must refuse it, while still skipping a legacy full's empty id.
func TestVerifyBackupIDs_EmptyIDIncremental_Refused(t *testing.T) {
	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	incr := makeManifest(t, irbackup.BackupKindIncremental, full, "0/200")

	t.Run("empty-id incremental refused", func(t *testing.T) {
		incr.BackupID = "" // blanked
		links := []lineage.SegmentRecord{
			{ManifestRecord: lineage.ManifestRecord{Manifest: full, Path: "full.json"}},
			{ManifestRecord: lineage.ManifestRecord{Manifest: incr, Path: "incr.json"}},
		}
		assertCoded(t, backup.VerifyBackupIDs(links), sluicecode.CodeBackupManifestInvalid)
	})

	t.Run("empty-id legacy full still skipped (restores clean)", func(t *testing.T) {
		legacy := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
		legacy.BackupID = ""
		links := []lineage.SegmentRecord{
			{ManifestRecord: lineage.ManifestRecord{Manifest: legacy, Path: "legacy.json"}},
		}
		if err := backup.VerifyBackupIDs(links); err != nil {
			t.Fatalf("legacy empty-id full refused: %v", err)
		}
	})

	t.Run("empty-id CDC segment RELABELED full still refused (structure key, audit-2026-07-12 LOW)", func(t *testing.T) {
		// The Kind field is itself attacker-controllable: blanking the id
		// AND relabeling the segment `full` (or blanking Kind, which
		// canonicalises to full) took the legacy-full skip pre-fix. The
		// refusal now also keys on the manifest CARRYING ChangeChunks — a
		// CDC segment whatever its label says.
		for _, kind := range []string{irbackup.BackupKindFull, ""} {
			relabeled := makeManifest(t, irbackup.BackupKindIncremental, full, "0/200")
			relabeled.Kind = kind
			relabeled.BackupID = "" // blanked
			relabeled.ChangeChunks = []*irbackup.ChunkInfo{{File: "changes/chunk-000001.jsonl.zst"}}
			links := []lineage.SegmentRecord{
				{ManifestRecord: lineage.ManifestRecord{Manifest: full, Path: "full.json"}},
				{ManifestRecord: lineage.ManifestRecord{Manifest: relabeled, Path: "incr.json"}},
			}
			assertCoded(t, backup.VerifyBackupIDs(links), sluicecode.CodeBackupManifestInvalid)
		}
	})
}

// TestChainRestore_EmptiedWindowForgedAnchor_RefusedRegardlessOfEngine pins
// audit-2026-07-12 (roadmap item 60): an emptied-DATA 0-chunk incremental that
// advances EndPosition, carrying a forged snapshot anchored AT EndPosition and
// a forged (no-op) SchemaDelta, is refused — regardless of source engine. The
// anchor and SchemaDelta fields are outside every signing-independent cover, so
// trusting them was a bar-raise, not a closure. Ground truth on real Postgres
// and MySQL (item60_anchor_schemadelta_{pg,mysql} integration tests) shows a
// LEGITIMATE DDL-only window emits its snapshot with an EMPTY EndPosition
// (posBearing false → the completeness guard is skipped, never refused), so
// this "anchor AT a position-bearing EndPosition with 0 chunks" shape is only
// ever a forgery. Both a registered VStream source (where a snapshot could also
// share a data row's position — Bug 184) and a Postgres source are refused.
func TestChainRestore_EmptiedWindowForgedAnchor_RefusedRegardlessOfEngine(t *testing.T) {
	users := &ir.Table{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	usersJSON, err := ir.MarshalTable(users)
	if err != nil {
		t.Fatalf("marshal users: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{users}}

	for _, tc := range []struct {
		name       string
		srcEngine  string
		manifest   bool // recorded CDCPositionCommitsAfterRows
		wantRefuse bool
	}{
		{"VStream source, snapshot AT EndPosition, emptied chunks — forge REFUSED", auditVStreamSrcName, false, true},
		{"postgres source, snapshot AT EndPosition, emptied chunks — forge REFUSED (anchor+SchemaDelta both forgeable)", "postgres", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			store, _ := blobcodec.NewLocalStore(dir)

			full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
			full.Schema = schema
			full.SourceEngine = tc.srcEngine
			full.BackupID = irbackup.ComputeBackupID(full)
			if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
				t.Fatalf("write full: %v", err)
			}
			_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

			incr := makeManifest(t, irbackup.BackupKindIncremental, full, "0/202")
			incr.Schema = schema
			incr.SourceEngine = tc.srcEngine
			incr.ChangeChunks = nil // EMPTIED (the attack) — no data-chunk tail
			incr.CDCPositionCommitsAfterRows = tc.manifest
			incr.SchemaHistory = []*irbackup.SchemaHistoryEntry{{
				Table:          "users",
				AnchorPosition: incr.EndPosition, // snapshot AT EndPosition
				TableJSON:      usersJSON,
			}}
			// The forge appends a no-op SchemaDelta (an AlterTable to the
			// current shape — apply-skipped) to satisfy the old len(SchemaDelta)>0
			// gate. Since SchemaDelta is outside every signing-independent cover,
			// this is free for a store adversary — which is why the gate no longer
			// trusts the anchor at all: the shape is refused regardless of engine.
			incr.SchemaDelta = []*irbackup.SchemaDeltaEntry{{
				Kind:  irbackup.SchemaDeltaAlterTable,
				Table: "users",
				After: users,
			}}
			incr.BackupID = irbackup.ComputeBackupID(incr)
			incrPath := "manifests/incr-0001.json"
			if err := lineage.WriteManifestAt(ctx, store, incrPath, incr); err != nil {
				t.Fatalf("write incr: %v", err)
			}
			_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, incr, incrPath, blobcodec.CodecGzip)

			tgt := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
			err := (&backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(ctx)
			if tc.wantRefuse {
				assertCoded(t, err, sluicecode.CodeBackupIncomplete)
				return
			}
			if err != nil {
				t.Fatalf("legit window refused: %v", err)
			}
		})
	}
}
