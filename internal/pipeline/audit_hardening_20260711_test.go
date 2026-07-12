// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// auditVStreamSrcEngine is a minimal registered engine whose Capabilities
// declare CDCPositionCommitsAfterRows — a stand-in for a VStream flavour
// (PlanetScale / Vitess) so the audit-2026-07-11 H-1 re-derivation can be
// exercised without importing a real engine package (which would cycle:
// engines import internal/pipeline).
type auditVStreamSrcEngine struct{ stubEngineBase }

func (auditVStreamSrcEngine) Name() string { return auditVStreamSrcName }

func (auditVStreamSrcEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{CDCPositionCommitsAfterRows: true}
}

const auditVStreamSrcName = "audit_vstream_src"

func init() { engines.Register(auditVStreamSrcEngine{}) }

// TestSourceEngineCommitsAfterRows pins the audit-2026-07-11 H-1 helper: the
// authoritative commit-after-rows signal comes from the binary's OWN engine
// registry (which no manifest edit can influence), not the manifest's
// tamperable bool. A registered VStream source reports true; a non-VStream and
// an unknown/unregistered source report false (caller then falls back to the
// manifest flag).
func TestSourceEngineCommitsAfterRows(t *testing.T) {
	if !backup.SourceEngineCommitsAfterRows(auditVStreamSrcName) {
		t.Errorf("registered VStream source: got false, want true")
	}
	if backup.SourceEngineCommitsAfterRows("this_engine_is_not_registered") {
		t.Errorf("unknown source: got true, want false")
	}
}

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
}

// TestChainRestore_FlippedFlagVStreamSource_Refused pins audit-2026-07-11 H-1
// facet (a): on an unsigned chain whose SourceEngine is a registered VStream
// engine, flipping the manifest's CDCPositionCommitsAfterRows false (the
// keyless-id-recomputable tamper the FV8 fold alone does not stop) must NOT
// re-open the Bug-184 bypass — restore ORs the flag with the source engine's
// OWN registered capability, so an emptied-DATA window whose snapshot sits AT
// EndPosition is still refused. The companion non-VStream case (SourceEngine
// postgres, same shape) legitimately passes: its anchor genuinely proves the
// window (a real DDL-only window), so the fix must not false-positive it.
func TestChainRestore_FlippedFlagVStreamSource_Refused(t *testing.T) {
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
		{"VStream source, flag flipped false, snapshot AT EndPosition — REFUSED via engine re-derivation", auditVStreamSrcName, false, true},
		{"non-VStream (postgres) source, snapshot AT EndPosition — legit DDL-only window passes", "postgres", false, false},
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
			lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

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
			// A realistic DDL-only window carries a SchemaDelta (item-60 ground
			// truth: a snapshot anchors at EndPosition only for a
			// column-signature DDL, which DiffSchemas records). The non-VStream
			// case is a legit such window; the VStream case is refused by the
			// engine re-derivation regardless of the delta.
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
			lineage.UpdateLineageForManifestBestEffort(ctx, store, incr, incrPath, blobcodec.CodecGzip)

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
