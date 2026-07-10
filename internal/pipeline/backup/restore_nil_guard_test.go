// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"testing"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestValidateManifestStructure pins the nil-structural-element family
// (M0.4 / Bug 182): a null *TableManifest, a null row-chunk, and a null
// change-chunk are each rejected; a clean manifest passes.
func TestValidateManifestStructure(t *testing.T) {
	clean := &irbackup.Manifest{
		Tables: []*irbackup.TableManifest{
			{Name: "t", Chunks: []*irbackup.ChunkInfo{{File: "f", SHA256: "s"}}},
		},
		ChangeChunks: []*irbackup.ChunkInfo{{File: "c", SHA256: "cs"}},
	}
	if err := validateManifestStructure(clean); err != nil {
		t.Errorf("clean manifest: unexpected error %v", err)
	}

	reject := map[string]*irbackup.Manifest{
		"nil manifest": nil,
		"null table":   {Tables: []*irbackup.TableManifest{nil}},
		"null row-chunk": {Tables: []*irbackup.TableManifest{
			{Name: "t", Chunks: []*irbackup.ChunkInfo{nil}},
		}},
		"null change-chunk": {ChangeChunks: []*irbackup.ChunkInfo{nil}},
	}
	for name, m := range reject {
		if err := validateManifestStructure(m); err == nil {
			t.Errorf("%s: want a rejection error, got nil", name)
		}
	}
}

// TestRestoreRun_NullStructuralElement_CodedNotPanic pins the restore-path half
// of Bug 182 (the confirming-audit follow-up): a tampered/bit-rotted UNSIGNED
// manifest with a null structural element fed to `restore` (not just `verify`)
// must return the coded SLUICE-E-BACKUP-SIGNATURE-INVALID refusal, NEVER a
// nil-pointer panic in the chunk traversal. The signature-canon nil-skip meant
// such a manifest verified/loaded green and then crashed restore before the
// guard was wired into Restore.Run.
func TestRestoreRun_NullStructuralElement_CodedNotPanic(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		manifest *irbackup.Manifest
	}{
		{"null table", &irbackup.Manifest{
			FormatVersion: irbackup.FormatVersionLegacy,
			CreatedAt:     time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC),
			SourceEngine:  "postgres", Kind: irbackup.BackupKindFull, BackupID: "full0001",
			Tables: []*irbackup.TableManifest{nil},
		}},
		{"null row-chunk", &irbackup.Manifest{
			FormatVersion: irbackup.FormatVersionLegacy,
			CreatedAt:     time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC),
			SourceEngine:  "postgres", Kind: irbackup.BackupKindFull, BackupID: "full0002",
			Tables: []*irbackup.TableManifest{{Name: "t", Chunks: []*irbackup.ChunkInfo{nil}}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemStore()
			if err := lineage.WriteManifest(ctx, store, tc.manifest); err != nil {
				t.Fatal(err)
			}
			lineage.UpdateLineageForManifestBestEffort(ctx, store, tc.manifest, lineage.ManifestFileName, blobcodec.DefaultCodec)

			// The single-full lineage takes Restore.Run's single-manifest path;
			// the guard must fire (before any chunk traversal) with the coded
			// refusal, not a panic. stubEngine satisfies the Target validation;
			// it is never reached because the guard returns first.
			r := &Restore{Store: store, Target: stubEngine{}, TargetDSN: "target-dsn"}
			err := r.Run(ctx)
			if err == nil {
				t.Fatal("want a coded refusal, got nil")
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
				t.Fatalf("want %s, got %v", sluicecode.CodeBackupSignatureInvalid, err)
			}
		})
	}
}

// TestVerifyBackupWith_NullStructuralElement_CodedNotPanic pins Bug 182: a
// manifest carrying a null table/chunk on the store makes `backup verify`
// return the coded SLUICE-E-BACKUP-SIGNATURE-INVALID refusal (a Refusal-class
// exit) — NEVER a nil-pointer panic in the chunk-rehash traversal.
func TestVerifyBackupWith_NullStructuralElement_CodedNotPanic(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		manifest *irbackup.Manifest
	}{
		{"null table", &irbackup.Manifest{
			FormatVersion: irbackup.FormatVersionLegacy,
			CreatedAt:     time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC),
			SourceEngine:  "postgres", Kind: irbackup.BackupKindFull, BackupID: "full0001",
			Tables: []*irbackup.TableManifest{nil},
		}},
		{"null row-chunk", &irbackup.Manifest{
			FormatVersion: irbackup.FormatVersionLegacy,
			CreatedAt:     time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC),
			SourceEngine:  "postgres", Kind: irbackup.BackupKindFull, BackupID: "full0002",
			Tables: []*irbackup.TableManifest{{Name: "t", Chunks: []*irbackup.ChunkInfo{nil}}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemStore()
			if err := lineage.WriteManifest(ctx, store, tc.manifest); err != nil {
				t.Fatal(err)
			}
			lineage.UpdateLineageForManifestBestEffort(ctx, store, tc.manifest, lineage.ManifestFileName, blobcodec.DefaultCodec)

			// The rehash traversal would nil-deref here before the fix.
			_, _, err := VerifyBackupWith(ctx, store, VerifyOptions{})
			if err == nil {
				t.Fatal("want a coded refusal, got nil")
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
				t.Fatalf("want %s, got %v", sluicecode.CodeBackupSignatureInvalid, err)
			}
			if info, _ := sluicecode.Describe(ce.Code); info.Class != sluicecode.ClassRefusal {
				t.Errorf("code class = %v; want refusal", info.Class)
			}
		})
	}
}
