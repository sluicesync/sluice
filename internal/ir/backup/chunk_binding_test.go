// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"testing"
	"time"
)

func bindingTestManifest(version int) *Manifest {
	return &Manifest{
		FormatVersion: version,
		CreatedAt:     time.Date(2026, 7, 8, 12, 30, 45, 123456789, time.UTC),
		SourceEngine:  "postgres",
		Kind:          BackupKindFull,
	}
}

// TestChunkAAD_VersionGate pins the single load-bearing gate: every
// pre-binding format version derives nil (the legacy nil-AAD decrypt
// path), the binding version and above derive the AAD. The gate lives
// in ONE function used by both writer and reader, so this is the whole
// version-dispatch pin.
func TestChunkAAD_VersionGate(t *testing.T) {
	for _, v := range []int{
		FormatVersionLegacy,
		FormatVersionSecurityMetadata,
		FormatVersionProgressSidecar,
		FormatVersionStandaloneSequences,
	} {
		if aad := ChunkAAD(bindingTestManifest(v), "chunks/t-0.jsonl.gz"); aad != nil {
			t.Errorf("FormatVersion %d derived a chunk AAD %q; pre-binding chunks were written UNBOUND and must decrypt nil-AAD", v, aad)
		}
		if b := CEKBinding(bindingTestManifest(v)); b != "" {
			t.Errorf("FormatVersion %d derived a CEK binding %q; pre-binding CEKs were wrapped unbound", v, b)
		}
	}
	m := bindingTestManifest(FormatVersionEncryptedChunkBinding)
	if ChunkAAD(m, "chunks/t-0.jsonl.gz") == nil {
		t.Error("FormatVersion 5 derived no chunk AAD")
	}
	if CEKBinding(m) == "" {
		t.Error("FormatVersion 5 derived no CEK binding")
	}
	if ChunkAAD(nil, "x") != nil || CEKBinding(nil) != "" {
		t.Error("nil manifest must derive no bindings")
	}
}

// TestChunkAAD_GoldenDerivation pins the exact derived bytes — the
// binding derivations are ON-DISK CONTRACT for every FormatVersion-5+
// chunk; an accidental format change strands every chain written since
// (the strings can only change together with a new FormatVersion and
// its own gate).
func TestChunkAAD_GoldenDerivation(t *testing.T) {
	m := bindingTestManifest(FormatVersionEncryptedChunkBinding)
	wantAAD := "sluice-chunk-aad/v1\n" +
		"created_at=2026-07-08T12:30:45.123456789Z\n" +
		"source_engine=postgres\n" +
		"kind=full\n" +
		"file=chunks/users-0.jsonl.gz"
	if got := string(ChunkAAD(m, "chunks/users-0.jsonl.gz")); got != wantAAD {
		t.Errorf("ChunkAAD derivation changed:\n got %q\nwant %q\n(on-disk contract — changing it requires a new FormatVersion)", got, wantAAD)
	}
	wantCEK := "sluice-cek-binding/v1\n" +
		"created_at=2026-07-08T12:30:45.123456789Z\n" +
		"source_engine=postgres\n" +
		"kind=full"
	if got := CEKBinding(m); got != wantCEK {
		t.Errorf("CEKBinding derivation changed:\n got %q\nwant %q", got, wantCEK)
	}
	wantChange := wantAAD + "\nindex=3"
	if got := string(ChangeChunkAAD(m, "chunks/users-0.jsonl.gz", 3)); got != wantChange {
		t.Errorf("ChangeChunkAAD derivation changed:\n got %q\nwant %q", got, wantChange)
	}
	// Empty Kind canonicalizes to full — the same rule ComputeBackupID
	// applies, so a legacy manifest re-written with Kind filled in
	// derives the same binding.
	m2 := bindingTestManifest(FormatVersionEncryptedChunkBinding)
	m2.Kind = ""
	if !bytes.Equal(ChunkAAD(m2, "f"), ChunkAAD(bindingTestManifest(FormatVersionEncryptedChunkBinding), "f")) {
		t.Error("empty Kind must canonicalize to full in the binding, mirroring ComputeBackupID")
	}
}

// TestChunkAAD_DistinctPerIdentityAndPosition pins that every identity
// axis and the position axis produce DISTINCT bindings — the property
// the splice/replay refusals rest on.
func TestChunkAAD_DistinctPerIdentityAndPosition(t *testing.T) {
	base := bindingTestManifest(FormatVersionEncryptedChunkBinding)
	ref := ChunkAAD(base, "chunks/t-0")

	variants := map[string]*Manifest{}
	other := bindingTestManifest(FormatVersionEncryptedChunkBinding)
	other.CreatedAt = other.CreatedAt.Add(time.Second)
	variants["created_at"] = other
	other = bindingTestManifest(FormatVersionEncryptedChunkBinding)
	other.SourceEngine = "mysql"
	variants["source_engine"] = other
	other = bindingTestManifest(FormatVersionEncryptedChunkBinding)
	other.Kind = BackupKindIncremental
	variants["kind"] = other
	for axis, m := range variants {
		if bytes.Equal(ChunkAAD(m, "chunks/t-0"), ref) {
			t.Errorf("changing %s did not change the chunk AAD; cross-backup replay on that axis would decrypt", axis)
		}
	}
	if bytes.Equal(ChunkAAD(base, "chunks/t-1"), ref) {
		t.Error("changing the chunk path did not change the AAD; position splice within a backup would decrypt")
	}
	if bytes.Equal(ChangeChunkAAD(base, "chunks/t-0", 0), ChangeChunkAAD(base, "chunks/t-0", 1)) {
		t.Error("changing the change-chunk ordinal did not change the AAD; manifest-entry reorder would replay out of order")
	}
	// EndPosition and ParentBackupID are DELIBERATELY excluded (the
	// former is unknown at write time for incrementals; the latter is
	// re-stitched by compaction) — pin the exclusion so a well-meaning
	// future addition doesn't strand resumed/compacted chains.
	other = bindingTestManifest(FormatVersionEncryptedChunkBinding)
	other.EndPosition.Token = "moved"
	other.ParentBackupID = "restitched"
	if !bytes.Equal(ChunkAAD(other, "chunks/t-0"), ref) {
		t.Error("EndPosition/ParentBackupID leaked into the binding; resume (adopted anchor) and compaction (parent re-stitch) rewrite those and would strand their chunks")
	}
}

// TestChunkAADFor_GatesOnChunkEncryption pins the read-side rule:
// only a chunk RECORDED as encrypted carries a binding, regardless of
// the manifest version.
func TestChunkAADFor_GatesOnChunkEncryption(t *testing.T) {
	m := bindingTestManifest(FormatVersionEncryptedChunkBinding)
	plain := &ChunkInfo{File: "chunks/t-0"}
	if aad := ChunkAADFor(m, plain); aad != nil {
		t.Errorf("plaintext chunk under a v5 manifest derived AAD %q; plaintext chunks have no ciphertext to bind", aad)
	}
	enc := &ChunkInfo{File: "chunks/t-0", Encryption: &ChunkEncryption{Algorithm: "AES-256-GCM"}}
	if !bytes.Equal(ChunkAADFor(m, enc), ChunkAAD(m, "chunks/t-0")) {
		t.Error("encrypted chunk's read-side AAD must equal the writer-side derivation")
	}
	if ChunkAADFor(m, nil) != nil {
		t.Error("nil chunk must derive no AAD")
	}
	if !bytes.Equal(ChangeChunkAADFor(m, enc, 2), ChangeChunkAAD(m, "chunks/t-0", 2)) {
		t.Error("encrypted change chunk's read-side AAD must equal the writer-side derivation")
	}
	if ChangeChunkAADFor(m, plain, 2) != nil {
		t.Error("plaintext change chunk must derive no AAD")
	}
}
