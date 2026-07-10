// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

func testSigner(t *testing.T) *Signer {
	t.Helper()
	key, err := crypto.DeriveManifestHMACKey(bytes.Repeat([]byte{0x42}, crypto.KEKLen))
	if err != nil {
		t.Fatal(err)
	}
	return &Signer{Key: key, KeyID: crypto.ManifestSigKeyID(key)}
}

func testManifest() *irbackup.Manifest {
	return &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionSignedManifest,
		CreatedAt:     time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		BackupID:      "abc0001",
		SchemaHash:    "hash",
		Tables: []*irbackup.TableManifest{
			{Name: "t", RowCount: 1, Chunks: []*irbackup.ChunkInfo{{File: "chunks/t/t-0.jsonl.gz", RowCount: 1, SHA256: "s0"}}},
		},
	}
}

// twoTableManifest builds a v7 manifest with two SAME-COLUMN-SET tables
// (`public.orders_2023` / `public.orders_2024`), each owning one encrypted
// row chunk — the shape the SEC-F1 chunk-reassignment attack targets. The
// two chunks are deliberately distinguishable only by their parent table
// once the (schema, name) tokens are folded in; before v4 they flatten
// into a parent-blind list that a swap leaves byte-identical.
func twoTableManifest() *irbackup.Manifest {
	enc := func() *irbackup.ChunkEncryption {
		return &irbackup.ChunkEncryption{Algorithm: "AES-256-GCM"}
	}
	return &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionChunkTableBinding,
		CreatedAt:     time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		BackupID:      "abc0001",
		SchemaHash:    "hash",
		Tables: []*irbackup.TableManifest{
			{Schema: "public", Name: "orders_2023", RowCount: 3, Chunks: []*irbackup.ChunkInfo{
				{File: "chunks/public__orders_2023/c-0.jsonl.gz", RowCount: 3, SHA256: "sha-2023", Encryption: enc()},
			}},
			{Schema: "public", Name: "orders_2024", RowCount: 3, Chunks: []*irbackup.ChunkInfo{
				{File: "chunks/public__orders_2024/c-0.jsonl.gz", RowCount: 3, SHA256: "sha-2024", Encryption: enc()},
			}},
		},
	}
}

// TestSignature_ChunkReassignmentBetweenSameSchemaTables_Refused is the
// SEC-F1 repro, permanently pinned. It exercises BOTH binding layers:
//
//   - Signature layer: sign the manifest (canon v4), swap the two tables'
//     [TableManifest.Chunks], and assert VerifyManifest now REFUSES — the
//     parent (schema, name) folded into each rowchunk token makes the swap
//     change the signed bytes. A witness confirms that at the pre-fix v3
//     canonicalization the SAME swap is INVISIBLE (byte-identical), i.e.
//     this is the exact hole v4 closes.
//   - Decrypt layer: assert each table's row-chunk AAD ([irbackup.ChunkAADFor])
//     is DISTINCT, so a reassigned ciphertext fails its GCM tag even if the
//     signature were absent.
func TestSignature_ChunkReassignmentBetweenSameSchemaTables_Refused(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)
	store := newMemStore()
	m := twoTableManifest()

	// Sign the honest manifest and confirm it verifies.
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, s); err != nil {
		t.Fatal(err)
	}
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, s); err != nil {
		t.Fatalf("honest manifest did not verify: %v", err)
	}

	// Decrypt-layer pin: the two tables' row-chunk AADs must differ, so a
	// ciphertext moved from one table to the other fails to decrypt.
	t0, t1 := m.Tables[0], m.Tables[1]
	aad0 := irbackup.ChunkAADFor(m, t0.Chunks[0], t0.Schema, t0.Name)
	aad1 := irbackup.ChunkAADFor(m, t1.Chunks[0], t1.Schema, t1.Name)
	if aad0 == nil || aad1 == nil {
		t.Fatalf("v7 encrypted row chunks derived a nil AAD (aad0=%v aad1=%v)", aad0, aad1)
	}
	if bytes.Equal(aad0, aad1) {
		t.Fatal("SEC-F1 decrypt layer: the two same-schema tables' row-chunk AADs are IDENTICAL — a reassigned ciphertext would still decrypt")
	}

	// Witness the pre-fix hole: at canon v3 the chunk swap is invisible.
	v3Before, err := irbackup.CanonicalManifestBytesForVersion(m, 0, irbackup.ManifestCanonVersionV3, s.schemeTag())
	if err != nil {
		t.Fatal(err)
	}

	// Signature-layer pin: swap the two tables' chunk assignments.
	m.Tables[0].Chunks, m.Tables[1].Chunks = m.Tables[1].Chunks, m.Tables[0].Chunks

	v3After, err := irbackup.CanonicalManifestBytesForVersion(m, 0, irbackup.ManifestCanonVersionV3, s.schemeTag())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v3Before, v3After) {
		t.Fatal("witness failed: the v3 (pre-SEC-F1) canonicalization was expected to be BLIND to the chunk swap")
	}

	// Under the current v4 signature the swap is caught.
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, s); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("SEC-F1 signature layer: chunk reassignment between same-schema tables: got %v, want ErrSignatureInvalid", err)
	}
}

// TestVerifyManifest_RoundTrip pins that a written signature verifies.
func TestVerifyManifest_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)
	store := newMemStore()
	m := testManifest()
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, s); err != nil {
		t.Fatal(err)
	}
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, s); err != nil {
		t.Fatalf("valid signature did not verify: %v", err)
	}
}

// TestVerifyManifest_TamperMatrix exercises every refusal class the
// per-manifest signature must catch.
func TestVerifyManifest_TamperMatrix(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)

	cases := []struct {
		name    string
		mutate  func(store irbackup.Store, m *irbackup.Manifest) // after signing
		seq     int
		wantErr error
	}{
		{
			name: "missing signature",
			mutate: func(store irbackup.Store, _ *irbackup.Manifest) {
				_ = store.Delete(ctx, ManifestSigPath(ManifestFileName))
			},
			seq:     0,
			wantErr: ErrSignatureMissing,
		},
		{
			name:    "tampered manifest (schema hash)",
			mutate:  func(_ irbackup.Store, m *irbackup.Manifest) { m.SchemaHash = "tampered" },
			seq:     0,
			wantErr: ErrSignatureInvalid,
		},
		{
			name:    "rolled-back sequence",
			mutate:  func(_ irbackup.Store, _ *irbackup.Manifest) {},
			seq:     1, // signed at 0, verified at 1
			wantErr: ErrSignatureInvalid,
		},
		{
			name:    "truncated change-list",
			mutate:  func(_ irbackup.Store, m *irbackup.Manifest) { m.Tables[0].Chunks = nil },
			seq:     0,
			wantErr: ErrSignatureInvalid,
		},
		{
			name: "corrupt sig mac",
			mutate: func(store irbackup.Store, _ *irbackup.Manifest) {
				corruptSigMAC(t, ctx, store, ManifestSigPath(ManifestFileName))
			},
			seq:     0,
			wantErr: ErrSignatureInvalid,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemStore()
			m := testManifest()
			if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, s); err != nil {
				t.Fatal(err)
			}
			tc.mutate(store, m)
			err := VerifyManifest(ctx, store, ManifestFileName, m, tc.seq, s)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want errors.Is %v", err, tc.wantErr)
			}
		})
	}
}

// TestVerifyManifest_NilEntries_CodedInvalidNotPanic is the M0.4 verify
// -side pin: a signed manifest whose tables/chunks have been replaced by
// nil entries (bit-rot / tamper: `"tables":[null]`) draws the coded
// SLUICE-E-BACKUP-SIGNATURE-INVALID error, NEVER a panic. The nil-skip in
// the canonical serialization keeps the recompute panic-free; the MAC then
// mismatches (the honest signer never emits nils) and CodeForSignatureError
// wraps it in the coded class. Table-driven across the nil shapes.
func TestVerifyManifest_NilEntries_CodedInvalidNotPanic(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)

	cases := map[string]func(*irbackup.Manifest){
		"tables replaced by null": func(m *irbackup.Manifest) { m.Tables = []*irbackup.TableManifest{nil} },
		"chunk replaced by null":  func(m *irbackup.Manifest) { m.Tables[0].Chunks = []*irbackup.ChunkInfo{nil} },
		"null table appended":     func(m *irbackup.Manifest) { m.Tables = append(m.Tables, nil) },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			store := newMemStore()
			m := testManifest()
			if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, s); err != nil {
				t.Fatal(err)
			}
			mut(m)
			// Must not panic. Whatever verify returns, its coded form is the
			// stable class the CLI exit boundary reports — never a raw panic.
			err := VerifyManifest(ctx, store, ManifestFileName, m, 0, s)
			coded := CodeForSignatureError(err)
			if err != nil && coded == nil {
				t.Fatalf("verify error %v produced no coded form", err)
			}
			// The replace cases MUST refuse (their bytes changed); the inert
			// append case may verify green (a skipped-nil is normalized out) —
			// either way, no panic and no uncoded error.
			if name != "null table appended" && !errors.Is(err, ErrSignatureInvalid) {
				t.Fatalf("%s: got %v, want ErrSignatureInvalid (coded)", name, err)
			}
		})
	}
}

// TestVerifyManifest_WrongKey pins that re-keying the chain (a different
// signer) refuses.
func TestVerifyManifest_WrongKey(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	m := testManifest()
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, testSigner(t)); err != nil {
		t.Fatal(err)
	}
	otherKey, _ := crypto.DeriveManifestHMACKey(bytes.Repeat([]byte{0x99}, crypto.KEKLen))
	other := &Signer{Key: otherKey, KeyID: crypto.ManifestSigKeyID(otherKey)}
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, other); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("wrong key: got %v, want ErrSignatureInvalid", err)
	}
}

// TestVerifyLineage_DroppedNewestLink pins that dropping the newest link
// from the catalog (shrinking the signed enumeration) refuses.
func TestVerifyLineage_DroppedNewestLink(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)
	store := newMemStore()
	cat := &Catalog{
		FormatVersion: 1,
		SourceEngine:  "postgres",
		Segments: []Segment{{
			SegmentID:        "seg0",
			FullManifestPath: ManifestFileName,
			Incrementals:     []string{"manifests/incr-1.json", "manifests/incr-2.json"},
		}},
	}
	if err := WriteLineageSig(ctx, store, cat, s); err != nil {
		t.Fatal(err)
	}
	// Untampered verifies.
	if err := VerifyLineage(ctx, store, cat, s); err != nil {
		t.Fatalf("valid lineage sig did not verify: %v", err)
	}
	// Drop the newest incremental from the enumeration.
	cat.Segments[0].Incrementals = cat.Segments[0].Incrementals[:1]
	if err := VerifyLineage(ctx, store, cat, s); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("dropped-newest-link: got %v, want ErrSignatureInvalid", err)
	}
}

// TestCodeForSignatureError pins the coded-error mapping used at the CLI
// exit boundary.
func TestCodeForSignatureError(t *testing.T) {
	if got := CodeForSignatureError(ErrSignatureMissing); got == nil {
		t.Fatal("missing mapped to nil")
	}
	if got := CodeForSignatureError(ErrSignatureInvalid); got == nil {
		t.Fatal("invalid mapped to nil")
	}
	plain := errors.New("unrelated")
	if got := CodeForSignatureError(plain); !errors.Is(got, plain) {
		t.Fatal("unrelated error should pass through unchanged")
	}
}

// TestNewSigner_NonSignerEnvelope pins that a non-ManifestSigner envelope
// yields ok=false (the write side refuses; the read side warns).
func TestNewSigner_NonSignerEnvelope(t *testing.T) {
	_, ok, err := NewSigner(nonSignerEnvelope{})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("non-signer envelope reported ok=true")
	}
}

type nonSignerEnvelope struct{}

func (nonSignerEnvelope) WrapCEK([]byte) ([]byte, error)   { return nil, nil }
func (nonSignerEnvelope) UnwrapCEK([]byte) ([]byte, error) { return nil, nil }
func (nonSignerEnvelope) Mode() string                     { return "test" }

var _ crypto.EnvelopeEncryption = nonSignerEnvelope{}

func corruptSigMAC(t *testing.T, ctx context.Context, store irbackup.Store, path string) {
	t.Helper()
	rc, err := store.Get(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	var sig irbackup.ManifestSignature
	if err := json.Unmarshal(body, &sig); err != nil {
		t.Fatal(err)
	}
	// Flip the MAC to a valid-hex but wrong value.
	if sig.MAC != "" {
		if sig.MAC[0] == '0' {
			sig.MAC = "1" + sig.MAC[1:]
		} else {
			sig.MAC = "0" + sig.MAC[1:]
		}
	}
	nb, _ := json.Marshal(&sig)
	if err := store.Put(ctx, path, bytes.NewReader(nb)); err != nil {
		t.Fatal(err)
	}
}
