// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"strings"
	"testing"
	"time"
)

// fixedSignedManifest is a deterministic manifest exercising every field
// the canonical serialization covers: identity, parent pointer, schema
// hash, chain-encryption (with Argon2id), a multi-table row-chunk set,
// and an ordered change-chunk list.
func fixedSignedManifest() *Manifest {
	return &Manifest{
		FormatVersion:  FormatVersionSignedManifest,
		SluiceVersion:  "test", // NOT in the canonical bytes (informational)
		CreatedAt:      time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		SchemaHash:     "abc123",
		BackupID:       "deadbeefcafe0001",
		ParentBackupID: "deadbeefcafe0000",
		Kind:           BackupKindIncremental,
		ChainEncryption: &ChainEncryption{
			Algorithm:  "AES-256-GCM",
			Mode:       "per-chain",
			KEKMode:    "passphrase-argon2id",
			WrappedCEK: []byte{0xde, 0xad},
			Argon2id: &Argon2idParams{
				Salt:        []byte{0x01, 0x02},
				Memory:      65536,
				Iterations:  3,
				Parallelism: 4,
				KeyLen:      32,
			},
		},
		Tables: []*TableManifest{
			// Deliberately out of sorted order to prove the canonical
			// serialization sorts them.
			{Schema: "public", Name: "orders", RowCount: 5, Chunks: []*ChunkInfo{
				{File: "chunks/public__orders/public__orders-0.jsonl.gz", RowCount: 5, SHA256: "sha-orders-0"},
			}},
			{Schema: "public", Name: "customers", RowCount: 2, Chunks: []*ChunkInfo{
				{File: "chunks/public__customers/public__customers-0.jsonl.gz", RowCount: 2, SHA256: "sha-cust-0"},
			}},
		},
		ChangeChunks: []*ChunkInfo{
			{File: "chunks/_changes/c-0.jsonl.gz", RowCount: 3, SHA256: "sha-chg-0"},
			{File: "chunks/_changes/c-1.jsonl.gz", RowCount: 4, SHA256: "sha-chg-1"},
		},
	}
}

// TestCanonicalManifestBytes_Golden pins the exact canonical serialization
// (on-disk contract — changing it strands every signature). If this
// changes intentionally, bump ManifestCanonVersion in the same edit.
func TestCanonicalManifestBytes_Golden(t *testing.T) {
	got := string(CanonicalManifestBytes(fixedSignedManifest(), 2))
	want := strings.Join([]string{
		"sluice-manifest-canon/v1",
		"format_version=6",
		"source_engine=postgres",
		"created_at=2026-07-09T12:00:00Z",
		"kind=incremental",
		"backup_id=deadbeefcafe0001",
		"parent_backup_id=deadbeefcafe0000",
		"schema_hash=abc123",
		"sequence=2",
		"chunk_count=4",
		"chain_encryption=algorithm=AES-256-GCM|mode=per-chain|kek_mode=passphrase-argon2id|kek_ref=|wrapped_cek=dead|argon2id_salt=0102|argon2id_memory=65536|argon2id_iterations=3|argon2id_parallelism=4|argon2id_keylen=32",
		"table:public.customers=2",
		"table:public.orders=5",
		"rowchunk:chunks/public__customers/public__customers-0.jsonl.gz=sha-cust-0:2",
		"rowchunk:chunks/public__orders/public__orders-0.jsonl.gz=sha-orders-0:5",
		"changechunk:0:chunks/_changes/c-0.jsonl.gz=sha-chg-0:3",
		"changechunk:1:chunks/_changes/c-1.jsonl.gz=sha-chg-1:4",
		"",
	}, "\n")
	if got != want {
		t.Fatalf("canonical bytes drift (on-disk contract):\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

// TestCanonicalManifestBytes_TamperSensitivity re-derives the family of
// security-relevant fields and pins that mutating ANY of them changes the
// canonical bytes (so the signature covers it). Pin-the-class discipline:
// every field the ADR names as signed is exercised, not one representative.
func TestCanonicalManifestBytes_TamperSensitivity(t *testing.T) {
	base := string(CanonicalManifestBytes(fixedSignedManifest(), 2))
	mutations := map[string]func(*Manifest){
		"format_version":  func(m *Manifest) { m.FormatVersion = 5 },
		"source_engine":   func(m *Manifest) { m.SourceEngine = "mysql" },
		"created_at":      func(m *Manifest) { m.CreatedAt = m.CreatedAt.Add(time.Second) },
		"kind":            func(m *Manifest) { m.Kind = BackupKindFull },
		"backup_id":       func(m *Manifest) { m.BackupID = "x" },
		"parent_pointer":  func(m *Manifest) { m.ParentBackupID = "x" },
		"schema_hash":     func(m *Manifest) { m.SchemaHash = "x" },
		"row_count":       func(m *Manifest) { m.Tables[0].RowCount = 99 },
		"row_chunk_sha":   func(m *Manifest) { m.Tables[0].Chunks[0].SHA256 = "x" },
		"chain_enc_wrap":  func(m *Manifest) { m.ChainEncryption.WrappedCEK = []byte{0x00} },
		"argon2id_memory": func(m *Manifest) { m.ChainEncryption.Argon2id.Memory = 1 },
		"change_sha":      func(m *Manifest) { m.ChangeChunks[0].SHA256 = "x" },
		"change_reorder":  func(m *Manifest) { m.ChangeChunks[0], m.ChangeChunks[1] = m.ChangeChunks[1], m.ChangeChunks[0] },
		"truncate_tail":   func(m *Manifest) { m.ChangeChunks = m.ChangeChunks[:1] },
	}
	for name, mut := range mutations {
		m := fixedSignedManifest()
		mut(m)
		if string(CanonicalManifestBytes(m, 2)) == base {
			t.Errorf("mutation %q did not change the canonical bytes (field is NOT under the signature)", name)
		}
	}
	// Sequence is a signed freshness anchor — a different seq differs.
	if string(CanonicalManifestBytes(fixedSignedManifest(), 3)) == base {
		t.Error("sequence change did not alter the canonical bytes")
	}
	// The informational SluiceVersion is NOT signed (it does not gate
	// correctness) — mutating it must NOT change the canonical bytes.
	m := fixedSignedManifest()
	m.SluiceVersion = "different"
	if string(CanonicalManifestBytes(m, 2)) != base {
		t.Error("SluiceVersion (informational) unexpectedly changed the canonical bytes")
	}
}

// TestManifestChunkCount pins the freshness count (row + change chunks).
func TestManifestChunkCount(t *testing.T) {
	if got := ManifestChunkCount(fixedSignedManifest()); got != 4 {
		t.Fatalf("chunk count %d != 4", got)
	}
	if got := ManifestChunkCount(nil); got != 0 {
		t.Fatalf("nil chunk count %d != 0", got)
	}
}

// TestManifestSignature_RoundTrip pins the detached-sig JSON round-trip
// and IsSignedFormat gating.
func TestManifestSignature_RoundTrip(t *testing.T) {
	sig := &ManifestSignature{
		CanonVersion: ManifestCanonVersion,
		Scheme:       SignatureSchemeHMACKEK,
		KeyID:        "abcd1234",
		Sequence:     2,
		ChunkCount:   4,
		MAC:          "deadbeef",
	}
	body, err := MarshalManifestSignature(sig)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalManifestSignature(body)
	if err != nil {
		t.Fatal(err)
	}
	if *got != *sig {
		t.Fatalf("round-trip mismatch: %+v != %+v", got, sig)
	}
	if !IsSignedFormat(&Manifest{FormatVersion: FormatVersionSignedManifest}) {
		t.Error("v6 manifest not recognised as signed")
	}
	if IsSignedFormat(&Manifest{FormatVersion: FormatVersionEncryptedChunkBinding}) {
		t.Error("v5 manifest wrongly recognised as signed")
	}
}
