// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// fixedSignedManifest is a deterministic manifest exercising every field
// the canonical serialization covers: identity, parent pointer, schema
// hash, positions, chain-encryption (with Argon2id), a multi-table
// row-chunk set, an ordered change-chunk list, schema deltas, and
// schema-history entries.
func fixedSignedManifest() *Manifest {
	col := &ir.Column{Name: "id", Type: ir.Integer{Width: 8}}
	tbl := &ir.Table{Name: "orders", Columns: []*ir.Column{col}}
	tblJSON, _ := ir.MarshalTable(tbl)
	return &Manifest{
		FormatVersion:  FormatVersionSignedManifest,
		SluiceVersion:  "test", // NOT in the canonical bytes (informational)
		CreatedAt:      time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		SchemaHash:     "abc123",
		BackupID:       "deadbeefcafe0001",
		ParentBackupID: "deadbeefcafe0000",
		Kind:           BackupKindIncremental,
		StartPosition:  ir.Position{Engine: "postgres", Token: `{"lsn":"0/100"}`},
		EndPosition:    ir.Position{Engine: "postgres", Token: `{"lsn":"0/200"}`},
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
		SchemaDelta: []*SchemaDeltaEntry{
			{Kind: SchemaDeltaAddTable, Schema: "public", Table: "orders", After: tbl},
		},
		SchemaHistory: []*SchemaHistoryEntry{
			{StreamID: "s1", Schema: "public", Table: "orders", AnchorPosition: ir.Position{Engine: "postgres", Token: "0/150"}, TableJSON: tblJSON},
		},
	}
}

func canon(t *testing.T, m *Manifest, seq int) string {
	t.Helper()
	b, err := CanonicalManifestBytes(m, seq, SignatureSchemeHMACKEK)
	if err != nil {
		t.Fatalf("CanonicalManifestBytes: %v", err)
	}
	return string(b)
}

// TestCanonicalManifestBytes_MinimalGolden pins the EXACT length-prefixed
// encoding for a minimal manifest (on-disk contract, human-inspectable).
// Each token is `<len>:<bytes>\n`. Changing the encoding strands every
// signature — bump ManifestCanonVersion in the same edit if intentional.
func TestCanonicalManifestBytes_MinimalGolden(t *testing.T) {
	m := &Manifest{
		FormatVersion: FormatVersionSignedManifest,
		CreatedAt:     time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          BackupKindFull,
	}
	want := strings.Join([]string{
		"24:sluice-manifest-canon/v3",
		"6:scheme", "8:hmac-kek",
		"14:format_version", "1:6",
		"13:source_engine", "8:postgres",
		"10:created_at", "20:2026-07-09T12:00:00Z",
		"4:kind", "4:full",
		"9:backup_id", "0:",
		"16:parent_backup_id", "0:",
		"11:schema_hash", "0:",
		"8:sequence", "1:0",
		"11:chunk_count", "1:0",
		"14:start_position", "0:", "0:",
		"12:end_position", "0:", "0:",
		"16:chain_encryption", "4:none",
		"",
	}, "\n")
	if got := canon(t, m, 0); got != want {
		t.Fatalf("minimal canonical drift:\n--- got ---\n%q\n--- want ---\n%q", got, want)
	}
}

// TestCanonicalManifestBytes_FullGolden pins the SHA-256 of the full
// fixture's canonical bytes — a compact golden over every folded field.
func TestCanonicalManifestBytes_FullGolden(t *testing.T) {
	b, err := CanonicalManifestBytes(fixedSignedManifest(), 2, SignatureSchemeHMACKEK)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	const want = "d194bad3abdbb5cdde7fbb2f66d9324aa4866a71abe3e59a4cfc2ddbdd5e1579"
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("full canonical SHA drift (on-disk contract): got %s want %s\n(canonical bytes:\n%q)", got, want, b)
	}
}

// TestCanonicalManifestBytes_V2PreservedGolden is the BACK-COMPAT guard:
// the v2 canonicalization must byte-match what the shipped Phase-1
// (v0.99.208) binary signed — the SHA is the ORIGINAL v2 full golden, and
// the minimal v2 rendering has NO scheme token. Changing v2 would strand
// every chain the Phase-1 binary wrote, so this pin must never move.
func TestCanonicalManifestBytes_V2PreservedGolden(t *testing.T) {
	// Full-fixture SHA — identical to what v0.99.208's CanonicalManifestBytes
	// (the pre-scheme-token function) emitted.
	b, err := CanonicalManifestBytesForVersion(fixedSignedManifest(), 2, ManifestCanonVersionV2, "")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	const wantSHA = "ebabd37f70c0da8d71b266991bc7ea8074f37abb3138be34f891b2c2267f4185"
	if got := hex.EncodeToString(sum[:]); got != wantSHA {
		t.Fatalf("v2 canonical SHA drift — this BREAKS every Phase-1-signed chain: got %s want %s", got, wantSHA)
	}
	// Minimal v2 rendering: v2 tag, NO scheme token, then the shared body.
	m := &Manifest{
		FormatVersion: FormatVersionSignedManifest,
		CreatedAt:     time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          BackupKindFull,
	}
	minB, err := CanonicalManifestBytesForVersion(m, 0, ManifestCanonVersionV2, "")
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		"24:sluice-manifest-canon/v2",
		"14:format_version", "1:6",
		"13:source_engine", "8:postgres",
		"10:created_at", "20:2026-07-09T12:00:00Z",
		"4:kind", "4:full",
		"9:backup_id", "0:",
		"16:parent_backup_id", "0:",
		"11:schema_hash", "0:",
		"8:sequence", "1:0",
		"11:chunk_count", "1:0",
		"14:start_position", "0:", "0:",
		"12:end_position", "0:", "0:",
		"16:chain_encryption", "4:none",
		"",
	}, "\n")
	if string(minB) != want {
		t.Fatalf("v2 minimal canonical drift:\n got %q\nwant %q", minB, want)
	}
	// An unknown (future) version is refused with ErrUnsupportedCanonVersion,
	// NOT a silent wrong-bytes render.
	if _, err := CanonicalManifestBytesForVersion(m, 0, "sluice-manifest-canon/v4", ""); !errors.Is(err, ErrUnsupportedCanonVersion) {
		t.Fatalf("future canon version: got %v, want ErrUnsupportedCanonVersion", err)
	}
}

// TestCanonicalManifestBytes_Injective is the forgery-primitive pin: two
// DISTINCT (schema,name)/(chunk path) tuples that raw-concatenation would
// collapse to identical bytes must produce DIFFERENT canonical bytes.
func TestCanonicalManifestBytes_Injective(t *testing.T) {
	mk := func(mut func(*Manifest)) string {
		m := &Manifest{FormatVersion: FormatVersionSignedManifest, SourceEngine: "pg", Kind: BackupKindFull}
		mut(m)
		return canon(t, m, 0)
	}
	// (schema="a", name="b.c") vs (schema="a.b", name="c") — the classic
	// dot-boundary collision.
	a := mk(func(m *Manifest) {
		m.Tables = []*TableManifest{{Schema: "a", Name: "b.c", RowCount: 1}}
	})
	b := mk(func(m *Manifest) {
		m.Tables = []*TableManifest{{Schema: "a.b", Name: "c", RowCount: 1}}
	})
	if a == b {
		t.Error("(a, b.c) and (a.b, c) collided — canonicalization is not injective")
	}
	// Embedded-newline table name must not inject a fake structural line
	// that collides with a two-table manifest.
	c := mk(func(m *Manifest) {
		m.Tables = []*TableManifest{{Schema: "s", Name: "t\n4:table", RowCount: 1}}
	})
	d := mk(func(m *Manifest) {
		m.Tables = []*TableManifest{{Schema: "s", Name: "t", RowCount: 1}, {Schema: "table", Name: "x", RowCount: 1}}
	})
	if c == d {
		t.Error("embedded-newline table name collided with a two-table manifest — not injective")
	}
	// Embedded delimiter in a chunk path must not collide with a shift into
	// the sha field.
	e := mk(func(m *Manifest) {
		m.Tables = []*TableManifest{{Name: "t", Chunks: []*ChunkInfo{{File: "a:b", SHA256: "s", RowCount: 1}}}}
	})
	f := mk(func(m *Manifest) {
		m.Tables = []*TableManifest{{Name: "t", Chunks: []*ChunkInfo{{File: "a", SHA256: "b:s", RowCount: 1}}}}
	})
	if e == f {
		t.Error("chunk path/sha delimiter collision — not injective")
	}
}

// TestCanonicalManifestBytes_TamperSensitivity re-derives the family of
// security-relevant fields and pins that mutating ANY of them changes the
// canonical bytes (so the signature covers it). Pin-the-class discipline.
func TestCanonicalManifestBytes_TamperSensitivity(t *testing.T) {
	base := canon(t, fixedSignedManifest(), 2)
	mutations := map[string]func(*Manifest){
		"format_version":   func(m *Manifest) { m.FormatVersion = 5 },
		"source_engine":    func(m *Manifest) { m.SourceEngine = "mysql" },
		"created_at":       func(m *Manifest) { m.CreatedAt = m.CreatedAt.Add(time.Second) },
		"kind":             func(m *Manifest) { m.Kind = BackupKindFull },
		"backup_id":        func(m *Manifest) { m.BackupID = "x" },
		"parent_pointer":   func(m *Manifest) { m.ParentBackupID = "x" },
		"schema_hash":      func(m *Manifest) { m.SchemaHash = "x" },
		"start_position":   func(m *Manifest) { m.StartPosition.Token = "x" },
		"end_position":     func(m *Manifest) { m.EndPosition.Token = "x" },
		"row_count":        func(m *Manifest) { m.Tables[0].RowCount = 99 },
		"row_chunk_sha":    func(m *Manifest) { m.Tables[0].Chunks[0].SHA256 = "x" },
		"chain_enc_wrap":   func(m *Manifest) { m.ChainEncryption.WrappedCEK = []byte{0x00} },
		"argon2id_memory":  func(m *Manifest) { m.ChainEncryption.Argon2id.Memory = 1 },
		"change_sha":       func(m *Manifest) { m.ChangeChunks[0].SHA256 = "x" },
		"change_reorder":   func(m *Manifest) { m.ChangeChunks[0], m.ChangeChunks[1] = m.ChangeChunks[1], m.ChangeChunks[0] },
		"truncate_tail":    func(m *Manifest) { m.ChangeChunks = m.ChangeChunks[:1] },
		"schemadelta_kind": func(m *Manifest) { m.SchemaDelta[0].Kind = SchemaDeltaDropTable },
		"schemadelta_tbl":  func(m *Manifest) { m.SchemaDelta[0].After.Columns[0].Type = ir.Text{} },
		"schemahistory":    func(m *Manifest) { m.SchemaHistory[0].TableJSON = []byte("{}") },
	}
	for name, mut := range mutations {
		m := fixedSignedManifest()
		mut(m)
		if canon(t, m, 2) == base {
			t.Errorf("mutation %q did not change the canonical bytes (field is NOT under the signature)", name)
		}
	}
	// Sequence is a signed freshness anchor.
	if canon(t, fixedSignedManifest(), 3) == base {
		t.Error("sequence change did not alter the canonical bytes")
	}
	// The informational SluiceVersion is NOT signed.
	m := fixedSignedManifest()
	m.SluiceVersion = "different"
	if canon(t, m, 2) != base {
		t.Error("SluiceVersion (informational) unexpectedly changed the canonical bytes")
	}
}

// TestCanonicalManifestBytes_SchemeBinding is the Phase 2 scheme-confusion
// pin: the signature scheme is folded into the canonical bytes, so signing
// the SAME manifest under a different scheme produces DIFFERENT bytes. An
// adversary who relabels an HMAC `.sig` as ed25519 (or vice versa) cannot
// make the recomputed bytes match — the primitive AND the bytes differ.
func TestCanonicalManifestBytes_SchemeBinding(t *testing.T) {
	m := fixedSignedManifest()
	hmacBytes, err := CanonicalManifestBytes(m, 2, SignatureSchemeHMACKEK)
	if err != nil {
		t.Fatal(err)
	}
	edBytes, err := CanonicalManifestBytes(m, 2, SignatureSchemeEd25519)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(hmacBytes, edBytes) {
		t.Error("hmac-kek and ed25519 canonical bytes collided — scheme is NOT bound into the signature")
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
