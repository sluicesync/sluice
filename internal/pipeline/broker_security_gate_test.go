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
			lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

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
			lineage.UpdateLineageForManifestBestEffort(ctx, store, incr, incrPath, blobcodec.CodecGzip)

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
	lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

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
	lineage.UpdateLineageForManifestBestEffort(ctx, store, incr, incrPath, blobcodec.CodecGzip)
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
