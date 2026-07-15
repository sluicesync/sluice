// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Security pins for the change-chunk / broker live-apply crypto path
// (audit 2026-07-10): F1 change-chunk tail-truncation backstop, BRK-2
// broker signature-flag wiring, BRK-3 mixed-mode / plaintext-chunk
// refusal, BRK-4 structural validation, BRK-5 rewritePosition
// unknown-shape hard error.

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// pgPos (Postgres-shaped position token) is shared with
// chain_contiguous_rotation_test.go; it renders byte-identically to
// makeManifest's EndPosition so an EndPosition built from the same lsn
// compares equal to a change carrying it.

// assertCoded fails unless err carries the expected sluicecode.
func assertCoded(t *testing.T, err error, want sluicecode.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("want coded error %s; got nil", want)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("err %v carries no sluicecode; want %s", err, want)
	}
	if ce.Code != want {
		t.Errorf("code = %s; want %s (err: %v)", ce.Code, want, err)
	}
}

// TestChainRestore_ChangeChunkTailTruncation_Refused pins F1 on the
// offline path: an unsigned incremental whose recorded EndPosition is
// AHEAD of its last replayed change — the on-disk footprint of a dropped
// tail change-chunk entry (survivors keep their ordinals so every GCM AAD
// still validates) — is refused with SLUICE-E-BACKUP-INCOMPLETE instead of
// returning exit 0 with fewer rows and a resume-poisoning EndPosition. The
// intact control (EndPosition == last change) restores cleanly.
func TestChainRestore_ChangeChunkTailTruncation_Refused(t *testing.T) {
	for _, tc := range []struct {
		name    string
		endLSN  string
		wantErr bool
	}{
		{"tail truncated: EndPosition ahead of last change", "0/204", true},
		{"intact: EndPosition == last change", "0/202", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			store, _ := blobcodec.NewLocalStore(dir)
			schema := &ir.Schema{Tables: []*ir.Table{{
				Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			}}}

			full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
			full.Schema = schema
			full.BackupID = irbackup.ComputeBackupID(full)
			if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
				t.Fatalf("write full: %v", err)
			}
			_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

			incr := makeManifest(t, irbackup.BackupKindIncremental, full, tc.endLSN)
			incr.Schema = schema
			changes := []ir.Change{
				ir.Insert{Position: pgPos("0/201"), Table: "users", Row: ir.Row{"id": int64(1)}},
				ir.Insert{Position: pgPos("0/202"), Table: "users", Row: ir.Row{"id": int64(2)}},
			}
			incr.ChangeChunks = []*irbackup.ChunkInfo{
				writeTestChangeChunk(t, store, "chunks/_changes/test/changes-0.jsonl.gz", changes),
			}
			incr.BackupID = irbackup.ComputeBackupID(incr)
			incrPath := "manifests/incr-0001.json"
			if err := lineage.WriteManifestAt(ctx, store, incrPath, incr); err != nil {
				t.Fatalf("write incr: %v", err)
			}
			_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, incr, incrPath, blobcodec.CodecGzip)

			tgt := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
			err := (&backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(ctx)
			if tc.wantErr {
				assertCoded(t, err, sluicecode.CodeBackupIncomplete)
				return
			}
			if err != nil {
				t.Fatalf("intact incremental refused: %v", err)
			}
		})
	}
}

// TestChainRestore_EmptiedChangeChunkList_Refused pins Bug 183/184 (the
// empty-list mirror of F1 tail-truncation): an unsigned incremental whose
// change-chunk list is EMPTIED (not just truncated) but whose EndPosition
// still advances must refuse, not silently apply nothing.
//
// Bug 184 is the case v0.99.223 MISSED: an emptied-DATA window still carries
// its routine first-touch schema-history snapshot (every real data incremental
// has one), anchored BEFORE the last row — so keying "legit 0-chunk window" on
// the mere PRESENCE of SchemaHistory let it pass. The completeness backstop now
// keys on the anchor POSITION: legit iff a change-chunk tail OR a schema-history
// snapshot is anchored EXACTLY at EndPosition. A DDL-only window's snapshot
// anchor equals EndPosition (passes); an emptied-data window's routine snapshot
// is anchored earlier (refused).
func TestChainRestore_EmptiedChangeChunkList_Refused(t *testing.T) {
	users := &ir.Table{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	usersJSON, err := ir.MarshalTable(users)
	if err != nil {
		t.Fatalf("marshal users: %v", err)
	}
	for _, tc := range []struct {
		name string
		// endLSN sets the incremental's EndPosition; StartPosition is the
		// full's EndPosition (0/100).
		endLSN string
		// schemaAnchorLSN, if non-empty, seeds one SchemaHistory entry
		// anchored at that LSN (models the routine/DDL schema snapshot).
		schemaAnchorLSN string
		// vstream marks the manifest as produced by an engine that stamps
		// CDC positions after its rows (Vitess/VStream), where a schema
		// anchor can coincide with a data change's position.
		vstream bool
		// schemaDelta seeds a non-empty (no-op) SchemaDelta. audit-2026-07-12:
		// a schema anchor at EndPosition is no longer trusted as proof of
		// completeness (the anchor and SchemaDelta are both outside every
		// signing-independent cover, so a forged SchemaDelta is free), so this
		// flag exists to prove the forge is refused EVEN WITH a SchemaDelta.
		schemaDelta bool
		wantErr     bool
	}{
		{"emptied list, no schema, EndPosition ahead of StartPosition", "0/202", "", false, false, true},
		{"emptied list, EndPosition == StartPosition (legit no-op window)", "0/100", "", false, false, false},
		// Bug 184: emptied-DATA window keeps its routine schema snapshot,
		// anchored at the window START (0/150), BEFORE the overstated
		// EndPosition (0/202). Presence of SchemaHistory must NOT exempt it.
		{"Bug 184: emptied DATA list, routine snapshot anchored BEFORE EndPosition", "0/202", "0/150", false, false, true},
		// item-60 anchor-forge WITH a forged SchemaDelta: the anchor-at-EndPosition
		// shape plus a no-op SchemaDelta was ACCEPTED before audit-2026-07-12 (the
		// len(SchemaDelta)>0 gate). Ground truth (item60 integration tests) shows a
		// legit DDL-only window has an EMPTY EndPosition, so this shape is only ever
		// a forgery — now REFUSED regardless of the forged delta.
		{"item 60 anchor-forge: snapshot AT EndPosition WITH forged SchemaDelta — REFUSED", "0/202", "0/202", false, true, true},
		// item 60 anchor-forge: the SAME anchor-at-EndPosition shape with an
		// EMPTY SchemaDelta — REFUSED (the anchor field is outside every
		// signing-independent cover).
		{"item 60 anchor-forge: snapshot AT EndPosition, EMPTY SchemaDelta (emptied-data window)", "0/202", "0/202", false, false, true},
		// Bug 184 (Vitess): on VStream a snapshot SHARES its transaction's
		// position, so an emptied-DATA window whose final tx first-touched a
		// table leaves a snapshot AT EndPosition. Refused (the anchor is never
		// trusted).
		{"Bug 184 (VStream): emptied DATA list, snapshot AT EndPosition, anchor untrusted", "0/202", "0/202", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			store, _ := blobcodec.NewLocalStore(dir)
			schema := &ir.Schema{Tables: []*ir.Table{users}}

			full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
			full.Schema = schema
			full.BackupID = irbackup.ComputeBackupID(full)
			if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
				t.Fatalf("write full: %v", err)
			}
			_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

			// StartPosition = full's EndPosition (0/100); EndPosition per tc.
			incr := makeManifest(t, irbackup.BackupKindIncremental, full, tc.endLSN)
			incr.Schema = schema
			incr.ChangeChunks = nil // EMPTIED (the attack) / legitimately empty
			incr.CDCPositionCommitsAfterRows = tc.vstream
			if tc.schemaAnchorLSN != "" {
				incr.SchemaHistory = []*irbackup.SchemaHistoryEntry{{
					Table:          "users",
					AnchorPosition: pgPos(tc.schemaAnchorLSN),
					TableJSON:      usersJSON,
				}}
			}
			if tc.schemaDelta {
				incr.SchemaDelta = []*irbackup.SchemaDeltaEntry{{
					Kind:  irbackup.SchemaDeltaAlterTable,
					Table: "users",
					After: users,
				}}
			}
			incr.BackupID = irbackup.ComputeBackupID(incr)
			incrPath := "manifests/incr-0001.json"
			if err := lineage.WriteManifestAt(ctx, store, incrPath, incr); err != nil {
				t.Fatalf("write incr: %v", err)
			}
			_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, incr, incrPath, blobcodec.CodecGzip)

			tgt := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
			err := (&backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(ctx)
			if tc.wantErr {
				assertCoded(t, err, sluicecode.CodeBackupIncomplete)
				return
			}
			if err != nil {
				t.Fatalf("legit window refused: %v", err)
			}
		})
	}
}

// TestSyncFromBackup_EmptiedChangeChunkList_Refused is the broker-path
// mirror of TestChainRestore_EmptiedChangeChunkList_Refused (Bug 183/184).
// applyIncremental's 0-chunk branch must refuse an emptied-DATA incremental
// (whose routine schema snapshot sits BEFORE the overstated EndPosition)
// rather than silently writePositionDirect past the dropped events, while
// still allowing a genuine DDL-only window (snapshot anchored AT EndPosition)
// and a no-op window (EndPosition == StartPosition).
func TestSyncFromBackup_EmptiedChangeChunkList_Refused(t *testing.T) {
	users := &ir.Table{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	usersJSON, err := ir.MarshalTable(users)
	if err != nil {
		t.Fatalf("marshal users: %v", err)
	}
	for _, tc := range []struct {
		name            string
		endLSN          string
		schemaAnchorLSN string
		vstream         bool
		wantErr         bool
	}{
		{"emptied list, no schema, EndPosition ahead of StartPosition", "0/202", "", false, true},
		{"emptied list, EndPosition == StartPosition (legit no-op window)", "0/100", "", false, false},
		{"Bug 184: emptied DATA list, routine snapshot anchored BEFORE EndPosition", "0/202", "0/150", false, true},
		// item 60 anchor-forge: a snapshot anchored AT EndPosition on an emptied
		// 0-chunk window that advances EndPosition is REFUSED (audit-2026-07-12).
		// Ground truth (item60 integration tests) shows a legit DDL-only window
		// has an EMPTY EndPosition, so this shape is only ever a store adversary's
		// emptied-DATA forgery — the anchor is not trusted as proof (nor is a
		// forged SchemaDelta, which is outside every signing-independent cover).
		{"item 60 anchor-forge: snapshot AT EndPosition (emptied-data window) — REFUSED", "0/202", "0/202", false, true},
		// Bug 184 (Vitess): a snapshot at EndPosition on a VStream manifest
		// must NOT be trusted — the broker must refuse, not advance past the
		// dropped events.
		{"Bug 184 (VStream): emptied DATA list, snapshot AT EndPosition, anchor untrusted", "0/202", "0/202", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
			incr := makeManifest(t, irbackup.BackupKindIncremental, full, tc.endLSN)
			incr.ChangeChunks = nil // EMPTIED (the attack) / legitimately empty
			incr.CDCPositionCommitsAfterRows = tc.vstream
			if tc.schemaAnchorLSN != "" {
				incr.SchemaHistory = []*irbackup.SchemaHistoryEntry{{
					Table:          "users",
					AnchorPosition: pgPos(tc.schemaAnchorLSN),
					TableJSON:      usersJSON,
				}}
			}
			incr.BackupID = irbackup.ComputeBackupID(incr)
			link := &lineage.SegmentRecord{
				ManifestRecord: lineage.ManifestRecord{Path: "manifests/incr-0001.json", Manifest: incr},
			}
			b := &SyncFromBackup{ChainURL: "test://brk184", StreamID: "s"}
			app := &capturingApplier{}
			_, err := b.applyIncremental(ctx, app, link, 100, "PARENT-RESUME-TOKEN")
			if tc.wantErr {
				assertCoded(t, err, sluicecode.CodeBackupIncomplete)
				if len(app.written) != 0 {
					t.Errorf("broker advanced its position (%d writes) on a refused emptied incremental", len(app.written))
				}
				return
			}
			if err != nil {
				t.Fatalf("legit window refused: %v", err)
			}
		})
	}
}

// TestSyncFromBackup_ChangeChunkTailTruncation_Refused pins F1 on the
// LIVE broker path: streamIncrementalWithPosition must refuse a truncated
// incremental (EndPosition ahead of the last change) so the broker never
// advances its resume token past a short tail. Mirrors the offline pin.
func TestSyncFromBackup_ChangeChunkTailTruncation_Refused(t *testing.T) {
	for _, tc := range []struct {
		name    string
		endLSN  string
		wantErr bool
	}{
		{"tail truncated", "0/204", true},
		{"intact", "0/202", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			store, _ := blobcodec.NewLocalStore(dir)

			incr := makeManifest(t, irbackup.BackupKindIncremental, nil, tc.endLSN)
			changes := []ir.Change{
				ir.Insert{Position: pgPos("0/201"), Table: "users", Row: ir.Row{"id": int64(1)}},
				ir.Insert{Position: pgPos("0/202"), Table: "users", Row: ir.Row{"id": int64(2)}},
			}
			incr.ChangeChunks = []*irbackup.ChunkInfo{
				writeTestChangeChunk(t, store, "chunks/_changes/test/changes-0.jsonl.gz", changes),
			}
			link := &lineage.SegmentRecord{
				ManifestRecord: lineage.ManifestRecord{Path: "manifests/incr-0001.json", Manifest: incr},
				Segment:        &lineage.Segment{Dir: "", Codec: blobcodec.CodecGzip},
			}

			b := &SyncFromBackup{Store: store, ChainURL: "file:///chain"}
			out := make(chan ir.Change, 16) // large enough that the 2 sends never block
			err := b.streamIncrementalWithPosition(ctx, link, encodeBrokerPosition("file:///chain", "parent"), out)
			close(out)
			if tc.wantErr {
				assertCoded(t, err, sluicecode.CodeBackupIncomplete)
				return
			}
			if err != nil {
				t.Fatalf("intact incremental refused: %v", err)
			}
		})
	}
}

// TestRewritePosition_UnknownShape_HardError pins BRK-5: an ir.Change
// shape the broker's position rewrite does not handle (here a real but
// unhandled ir.SchemaSnapshot — the broker never emits one into its change
// stream) is a HARD error, never a silent pass-through carrying a foreign
// position. The 6 handled shapes are covered by TestRewritePosition_AllChangeShapes.
func TestRewritePosition_UnknownShape_HardError(t *testing.T) {
	_, err := rewritePosition(ir.SchemaSnapshot{Position: ir.Position{Token: "src"}},
		ir.Position{Engine: BackupBrokerPositionEngine, Token: "T"})
	if err == nil {
		t.Fatal("unhandled ir.Change shape must be a hard error, not a silent pass-through")
	}
	if !strings.Contains(err.Error(), "unhandled ir.Change shape") {
		t.Errorf("err = %v; want it to name the unhandled shape", err)
	}
}

// TestSyncFromBackup_ChunkCEK_MixedMode pins BRK-3 at the per-chunk level:
// an encrypted chain must refuse a PLAINTEXT chunk (a store adversary's
// splice) rather than open it as cleartext; a genuinely plaintext chain
// still opens a plaintext chunk normally.
func TestSyncFromBackup_ChunkCEK_MixedMode(t *testing.T) {
	plaintextChunk := &irbackup.ChunkInfo{File: "chunks/_changes/c-0.jsonl.gz"} // Encryption == nil

	enc := &SyncFromBackup{chainEncrypted: true}
	if _, err := enc.chunkCEK(plaintextChunk); err == nil {
		t.Error("encrypted chain accepted a plaintext chunk; want a loud refusal")
	} else if !strings.Contains(err.Error(), "plaintext") {
		t.Errorf("err = %v; want a plaintext-splice refusal", err)
	}

	plain := &SyncFromBackup{chainEncrypted: false}
	cek, err := plain.chunkCEK(plaintextChunk)
	if err != nil || cek != nil {
		t.Errorf("plaintext chain / plaintext chunk: got cek=%v err=%v; want (nil, nil)", cek, err)
	}
}

// seedPlainChain writes an unsigned full + one-chunk incremental with a
// walkable lineage, returning the store. EndPosition == the last change so
// the F1 backstop passes; used by the verifyChainIntegrity pins.
func seedPlainChain(t *testing.T) irbackup.Store {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	full.Schema = schema
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

	incr := makeManifest(t, irbackup.BackupKindIncremental, full, "0/202")
	incr.Schema = schema
	changes := []ir.Change{
		ir.Insert{Position: pgPos("0/201"), Table: "users", Row: ir.Row{"id": int64(1)}},
		ir.Insert{Position: pgPos("0/202"), Table: "users", Row: ir.Row{"id": int64(2)}},
	}
	incr.ChangeChunks = []*irbackup.ChunkInfo{
		writeTestChangeChunk(t, store, "chunks/_changes/test/changes-0.jsonl.gz", changes),
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	incrPath := "manifests/incr-0001.json"
	if err := lineage.WriteManifestAt(ctx, store, incrPath, incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, incr, incrPath, blobcodec.CodecGzip)
	return store
}

// TestSyncFromBackup_VerifyChainIntegrity pins the broker's BRK-2/4
// verification parity: a clean unsigned chain passes; --require-signature
// on an unsigned chain refuses (BRK-2, the silently-ignored-flag hole); a
// null structural element refuses with the coded restore-parity error
// (BRK-4) rather than nil-derefing the tick.
func TestSyncFromBackup_VerifyChainIntegrity(t *testing.T) {
	ctx := context.Background()

	t.Run("clean unsigned chain passes", func(t *testing.T) {
		store := seedPlainChain(t)
		b := &SyncFromBackup{Store: store}
		chain, err := b.brokerChain(ctx)
		if err != nil {
			t.Fatalf("brokerChain: %v", err)
		}
		if err := b.verifyChainIntegrity(ctx, chain); err != nil {
			t.Fatalf("clean unsigned chain refused: %v", err)
		}
	})

	t.Run("require-signature on unsigned chain refuses (BRK-2)", func(t *testing.T) {
		store := seedPlainChain(t)
		b := &SyncFromBackup{Store: store, RequireSignature: true}
		chain, err := b.brokerChain(ctx)
		if err != nil {
			t.Fatalf("brokerChain: %v", err)
		}
		assertCoded(t, b.verifyChainIntegrity(ctx, chain), sluicecode.CodeBackupSignatureMissing)
	})

	t.Run("null structural element refuses (BRK-4)", func(t *testing.T) {
		store := seedPlainChain(t)
		b := &SyncFromBackup{Store: store}
		chain, err := b.brokerChain(ctx)
		if err != nil {
			t.Fatalf("brokerChain: %v", err)
		}
		// Simulate a tampered/bit-rotted manifest carrying a null table entry.
		chain[0].Manifest.Tables = []*irbackup.TableManifest{nil}
		assertCoded(t, b.verifyChainIntegrity(ctx, chain), sluicecode.CodeBackupSignatureInvalid)
	})
}
