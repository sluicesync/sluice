// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Pins for the broker-callable exported gate wrappers (BRK-3/4). The
// underlying gates are shared with ChainRestore.Run; these assert the
// exported entry points the broker calls behave identically.

import (
	"testing"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

func mixedModeLink(path, kind string, chainEnc *irbackup.ChainEncryption, chunks []*irbackup.ChunkInfo) lineage.SegmentRecord {
	return lineage.SegmentRecord{
		ManifestRecord: lineage.ManifestRecord{
			Path: path,
			Manifest: &irbackup.Manifest{
				CreatedAt:       time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
				SourceEngine:    "postgres",
				Kind:            kind,
				ChainEncryption: chainEnc,
				ChangeChunks:    chunks,
			},
		},
	}
}

// TestCheckMixedModeChain_Exported pins BRK-3's manifest-level half: an
// encrypted segment full with a plaintext incremental is refused (a
// spliced plaintext incremental), while a uniformly-encrypted segment
// passes.
func TestCheckMixedModeChain_Exported(t *testing.T) {
	enc := &irbackup.ChainEncryption{Algorithm: "AES-256-GCM"}
	chunkEnc := &irbackup.ChunkEncryption{Algorithm: "AES-256-GCM"}

	t.Run("encrypted full + plaintext incremental refuses", func(t *testing.T) {
		chain := []lineage.SegmentRecord{
			mixedModeLink("manifest.json", irbackup.BackupKindFull, enc, nil),
			mixedModeLink("incr.json", irbackup.BackupKindIncremental, nil, []*irbackup.ChunkInfo{
				{File: "c-0", RowCount: 1, SHA256: "sha"}, // Encryption == nil → plaintext
			}),
		}
		if err := CheckMixedModeChain(chain); err == nil {
			t.Fatal("mixed-mode (encrypted full + plaintext incr) accepted; want refusal")
		}
	})

	t.Run("uniformly encrypted segment passes", func(t *testing.T) {
		chain := []lineage.SegmentRecord{
			mixedModeLink("manifest.json", irbackup.BackupKindFull, enc, nil),
			mixedModeLink("incr.json", irbackup.BackupKindIncremental, nil, []*irbackup.ChunkInfo{
				{File: "c-0", RowCount: 1, SHA256: "sha", Encryption: chunkEnc},
			}),
		}
		if err := CheckMixedModeChain(chain); err != nil {
			t.Fatalf("uniformly-encrypted segment refused: %v", err)
		}
	})
}

// TestValidateManifestStructure_Exported pins BRK-4's null-structural
// guard through the exported wrapper.
func TestValidateManifestStructure_Exported(t *testing.T) {
	good := &irbackup.Manifest{Tables: []*irbackup.TableManifest{{Name: "t"}}}
	if err := ValidateManifestStructure(good); err != nil {
		t.Errorf("well-formed manifest refused: %v", err)
	}
	bad := &irbackup.Manifest{Tables: []*irbackup.TableManifest{nil}}
	if err := ValidateManifestStructure(bad); err == nil {
		t.Error("null table entry accepted; want a structural refusal")
	}
}
