// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for ChainRestore.verifySchemaHashes (ADR-0152, audit N-8
// item 4): the recompute-and-compare corruption check the
// Manifest.SchemaHash doc had promised, including the named pre-v5
// standalone-sequences WARN carve-out.

package backup

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

func schemaHashLink(t *testing.T, mutate func(m *irbackup.Manifest)) lineage.SegmentRecord {
	t.Helper()
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	hash, err := irbackup.ComputeSchemaHash(schema)
	if err != nil {
		t.Fatalf("ComputeSchemaHash: %v", err)
	}
	m := &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionEncryptedChunkBinding,
		Kind:          irbackup.BackupKindFull,
		Schema:        schema,
		SchemaHash:    hash,
	}
	if mutate != nil {
		mutate(m)
	}
	return lineage.SegmentRecord{ManifestRecord: lineage.ManifestRecord{Path: "manifest.json", Manifest: m}}
}

func TestVerifySchemaHashes(t *testing.T) {
	ctx := context.Background()
	r := &ChainRestore{}

	t.Run("matching hash passes", func(t *testing.T) {
		if err := r.verifySchemaHashes(ctx, []lineage.SegmentRecord{schemaHashLink(t, nil)}); err != nil {
			t.Errorf("matching hash refused: %v", err)
		}
	})

	t.Run("empty recorded hash is skipped (pre-ADR-0152 fulls)", func(t *testing.T) {
		link := schemaHashLink(t, func(m *irbackup.Manifest) {
			m.SchemaHash = ""
			m.Schema.Tables[0].Name = "whatever" // unverifiable, must not matter
		})
		if err := r.verifySchemaHashes(ctx, []lineage.SegmentRecord{link}); err != nil {
			t.Errorf("hash-less manifest refused: %v", err)
		}
	})

	t.Run("mismatch refuses loudly, naming the manifest", func(t *testing.T) {
		link := schemaHashLink(t, func(m *irbackup.Manifest) {
			m.Schema.Tables[0].Columns[0].Name = "mangled"
		})
		err := r.verifySchemaHashes(ctx, []lineage.SegmentRecord{link})
		if err == nil {
			t.Fatal("mismatched hash passed; the corruption check is gone")
		}
		for _, want := range []string{"schema hash mismatch", "manifest.json"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal %q should contain %q", err.Error(), want)
			}
		}
	})

	t.Run("pre-v5 manifest with standalone sequences → WARN carve-out, not refusal", func(t *testing.T) {
		// The named wart: pre-ADR-0152 writers re-stamped the recorded
		// Schema with end-of-window sequence state without re-hashing
		// when sequence OPTIONS changed inside a no-DDL window.
		link := schemaHashLink(t, func(m *irbackup.Manifest) {
			m.FormatVersion = irbackup.FormatVersionStandaloneSequences
			m.Schema.Sequences = []*ir.Sequence{{Name: "s", Increment: 5}}
			// The recorded hash predates the sequence-option change.
		})
		if err := r.verifySchemaHashes(ctx, []lineage.SegmentRecord{link}); err != nil {
			t.Errorf("pre-v5 sequences carve-out refused a legitimate old chain: %v", err)
		}
	})

	t.Run("v5 manifest with sequences gets NO carve-out", func(t *testing.T) {
		// Current writers re-stamp the hash after the sequence refresh,
		// so a v5 mismatch is always corruption.
		link := schemaHashLink(t, func(m *irbackup.Manifest) {
			m.Schema.Sequences = []*ir.Sequence{{Name: "s", Increment: 5}}
		})
		if err := r.verifySchemaHashes(ctx, []lineage.SegmentRecord{link}); err == nil {
			t.Error("v5 manifest with a mismatched hash passed via the sequences carve-out; the carve-out must stay pre-v5-only")
		}
	})
}
