// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for ChainRestore.verifySchemaHashes (ADR-0152, audit N-8
// item 4): the recompute-and-compare corruption check the
// Manifest.SchemaHash doc had promised, including the named pre-v5
// standalone-sequences WARN carve-out.

package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
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

	t.Run("matching hash passes", func(t *testing.T) {
		if err := verifySchemaHashes(ctx, []lineage.SegmentRecord{schemaHashLink(t, nil)}); err != nil {
			t.Errorf("matching hash refused: %v", err)
		}
	})

	t.Run("empty recorded hash is skipped (pre-ADR-0152 fulls)", func(t *testing.T) {
		link := schemaHashLink(t, func(m *irbackup.Manifest) {
			m.SchemaHash = ""
			m.Schema.Tables[0].Name = "whatever" // unverifiable, must not matter
		})
		if err := verifySchemaHashes(ctx, []lineage.SegmentRecord{link}); err != nil {
			t.Errorf("hash-less manifest refused: %v", err)
		}
	})

	t.Run("mismatch refuses loudly, naming the manifest", func(t *testing.T) {
		link := schemaHashLink(t, func(m *irbackup.Manifest) {
			m.Schema.Tables[0].Columns[0].Name = "mangled"
		})
		err := verifySchemaHashes(ctx, []lineage.SegmentRecord{link})
		if err == nil {
			t.Fatal("mismatched hash passed; the corruption check is gone")
		}
		for _, want := range []string{"schema hash mismatch", "manifest.json"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal %q should contain %q", err.Error(), want)
			}
		}
	})

	// The refusal's WORDING is load-bearing, not cosmetic (roadmap item
	// 102). A fingerprint mismatch has TWO causes and this check cannot
	// tell them apart: a manifest written by a release whose IR schema
	// field set differs is INTACT and merely unreadable here. Three such
	// epochs already shipped, so telling that operator their manifest is
	// "corrupted or partially-rewritten" — during a recovery — is a false
	// accusation that sends them hunting a storage fault that is not
	// there. Prose is what the operator acts on, so prose is what is
	// pinned.
	t.Run("the refusal names BOTH causes and accuses neither", func(t *testing.T) {
		link := schemaHashLink(t, func(m *irbackup.Manifest) {
			m.SluiceVersion = "0.99.292"
			m.Schema.Tables[0].Columns[0].Name = "mangled"
		})
		err := verifySchemaHashes(ctx, []lineage.SegmentRecord{link})
		if err == nil {
			t.Fatal("mismatched hash passed; the check must still REFUSE — only its wording changed")
		}
		msg := err.Error()
		// The writing release is DATA (the manifest's own sluice_version),
		// never a hardcoded epoch table in the error string.
		for _, want := range []string{
			"written by sluice 0.99.292",
			"the chain is intact",
			"genuinely corrupt or tampered",
		} {
			if !strings.Contains(msg, want) {
				t.Errorf("refusal should contain %q; got:\n%s", want, msg)
			}
		}
		if strings.Contains(msg, "corrupted or partially-rewritten") {
			t.Errorf("refusal still accuses the operator of corruption as the ONLY cause; got:\n%s", msg)
		}
		ce, ok := sluicecode.FromError(err)
		if !ok {
			t.Fatalf("refusal lost its %s code: %v", sluicecode.CodeBackupManifestInvalid, err)
		}
		if ce.Code != sluicecode.CodeBackupManifestInvalid {
			t.Errorf("code = %s, want %s", ce.Code, sluicecode.CodeBackupManifestInvalid)
		}
		// The remedy must be actionable for BOTH causes, and must not
		// invent a flag: there is no skip-the-check flag to name.
		for _, want := range []string{
			"re-take the backup with this binary",
			"docs/operator/error-codes.md",
		} {
			if !strings.Contains(ce.Hint, want) {
				t.Errorf("hint should contain %q; got: %s", want, ce.Hint)
			}
		}
	})

	t.Run("a manifest with no recorded writer version says so", func(t *testing.T) {
		link := schemaHashLink(t, func(m *irbackup.Manifest) {
			m.SluiceVersion = ""
			m.Schema.Tables[0].Columns[0].Name = "mangled"
		})
		err := verifySchemaHashes(ctx, []lineage.SegmentRecord{link})
		if err == nil {
			t.Fatal("mismatched hash passed")
		}
		if !strings.Contains(err.Error(), "does not record the release that wrote it") {
			t.Errorf("refusal should say the writing release is unknown; got:\n%s", err)
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
		if err := verifySchemaHashes(ctx, []lineage.SegmentRecord{link}); err != nil {
			t.Errorf("pre-v5 sequences carve-out refused a legitimate old chain: %v", err)
		}
	})

	t.Run("skew-annotated signature failure", func(t *testing.T) {
		// The signature path's twin of the same false accusation.
		// CanonicalManifestBytes folds the RECORDED schema_hash verbatim,
		// but deltaTableFingerprint RECOMPUTES the fingerprint of each
		// SchemaDelta's before/after table — so a signed manifest carrying
		// a delta fails its MAC across a fingerprint boundary, and a MAC
		// failure carries no structure to read. Restore and the broker run
		// verifySchemaHashes first and preempt it; `backup verify` and
		// `export-as-parquet` do not, so the note rides the error there.
		skewed := schemaHashLink(t, func(m *irbackup.Manifest) {
			m.Schema.Tables[0].Columns[0].Name = "mangled"
		}).Manifest

		annotated := annotateSchemaFingerprintSkew(
			fmt.Errorf("manifest %q: mac mismatch: %w", "manifest.json", lineage.ErrSignatureInvalid), skewed,
		)
		if !errors.Is(annotated, lineage.ErrSignatureInvalid) {
			t.Fatal("the annotation broke the error chain — CodeForSignatureError would no longer classify it")
		}
		if !strings.Contains(annotated.Error(), "different IR schema field set") {
			t.Errorf("annotated signature failure should raise the release-skew possibility; got:\n%s", annotated)
		}

		// Clean manifest: the fingerprint reproduces, so there is no
		// evidence of skew and the error must be left exactly as-is —
		// absence of the note is not evidence of tampering either way, but
		// inventing it would be.
		clean := schemaHashLink(t, nil).Manifest
		sigErr := fmt.Errorf("manifest %q: mac mismatch: %w", "manifest.json", lineage.ErrSignatureInvalid)
		if got := annotateSchemaFingerprintSkew(sigErr, clean); got.Error() != sigErr.Error() {
			t.Errorf("annotated a manifest whose fingerprint reproduces: %v", got)
		}

		// A non-signature failure is never annotated. Compared by message
		// rather than by identity: the assertion is that nothing was
		// appended, and errors.Is would be satisfied by a wrapper.
		other := errors.New("store read failed")
		if got := annotateSchemaFingerprintSkew(other, skewed); got.Error() != other.Error() {
			t.Errorf("annotated a non-signature error: %v", got)
		}
	})

	t.Run("v5 manifest with sequences gets NO carve-out", func(t *testing.T) {
		// Current writers re-stamp the hash after the sequence refresh,
		// so a v5 mismatch is always corruption.
		link := schemaHashLink(t, func(m *irbackup.Manifest) {
			m.Schema.Sequences = []*ir.Sequence{{Name: "s", Increment: 5}}
		})
		if err := verifySchemaHashes(ctx, []lineage.SegmentRecord{link}); err == nil {
			t.Error("v5 manifest with a mismatched hash passed via the sequences carve-out; the carve-out must stay pre-v5-only")
		}
	})
}
