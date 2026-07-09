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
