// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Manifest round-trip via standard json.Marshal. Validates that the
// public-contract type is JSON-stable end-to-end. Includes a Schema
// with a column to exercise the Column custom marshal pathway.
func TestManifestJSON_RoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 30, 0, 0, time.UTC)
	original := &Manifest{
		FormatVersion: BackupFormatVersion,
		SluiceVersion: "0.14.1",
		CreatedAt:     now,
		SourceEngine:  "postgres",
		Schema: &ir.Schema{
			Tables: []*ir.Table{
				{
					Name: "users",
					Columns: []*ir.Column{
						{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
						{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
					},
				},
			},
		},
		Tables: []*TableManifest{
			{
				Name:     "users",
				RowCount: 12345,
				Chunks: []*ChunkInfo{
					{File: "chunks/users/users-0.jsonl.gz", RowCount: 10000, SHA256: "abc123"},
					{File: "chunks/users/users-1.jsonl.gz", RowCount: 2345, SHA256: "def456"},
				},
			},
		},
	}
	b, err := json.MarshalIndent(original, "", "  ")
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got Manifest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v\nJSON:\n%s", err, b)
	}
	if got.FormatVersion != original.FormatVersion {
		t.Errorf("FormatVersion: got %d want %d", got.FormatVersion, original.FormatVersion)
	}
	if got.SourceEngine != "postgres" {
		t.Errorf("SourceEngine: got %q", got.SourceEngine)
	}
	if !got.CreatedAt.Equal(now) {
		t.Errorf("CreatedAt: got %v want %v", got.CreatedAt, now)
	}
	if len(got.Schema.Tables) != 1 || got.Schema.Tables[0].Name != "users" {
		t.Fatalf("Schema.Tables: got %v", got.Schema.Tables)
	}
	if len(got.Schema.Tables[0].Columns) != 2 {
		t.Fatalf("Columns count: %d", len(got.Schema.Tables[0].Columns))
	}
	gotInt := got.Schema.Tables[0].Columns[0]
	if gotInt.Type == nil || gotInt.Type.String() != "Int64 AutoIncrement" {
		t.Errorf("id Type: got %v want Int64 AutoIncrement", gotInt.Type)
	}
	if len(got.Tables) != 1 {
		t.Fatalf("Tables: got %d", len(got.Tables))
	}
	if got.Tables[0].RowCount != 12345 {
		t.Errorf("RowCount: got %d", got.Tables[0].RowCount)
	}
	if len(got.Tables[0].Chunks) != 2 {
		t.Fatalf("Chunks: got %d", len(got.Tables[0].Chunks))
	}
	if got.Tables[0].Chunks[0].SHA256 != "abc123" {
		t.Errorf("SHA256[0]: got %q", got.Tables[0].Chunks[0].SHA256)
	}
}

// Phase 6: encrypted manifests round-trip through JSON without losing
// any of the new fields. Plaintext (no Encryption set) manifests stay
// shaped as before — verified via byte-comparison of marshalled JSON
// (the omitempty tags should keep the encryption fields off the wire
// when they're nil).
func TestManifest_EncryptedRoundTrip(t *testing.T) {
	in := &Manifest{
		FormatVersion: BackupFormatVersion,
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{Tables: []*ir.Table{{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}}},
		Tables: []*TableManifest{
			{
				Name:     "users",
				RowCount: 1,
				Chunks: []*ChunkInfo{
					{
						File:     "chunks/users/users-0.jsonl.gz",
						RowCount: 1,
						SHA256:   "abc123",
						Encryption: &ChunkEncryption{
							Algorithm:  "AES-256-GCM",
							NonceLen:   12,
							AuthTagLen: 16,
							// per-chain mode: empty WrappedCEK
						},
					},
				},
			},
		},
		ChainEncryption: &ChainEncryption{
			Algorithm:  "AES-256-GCM",
			Mode:       "per-chain",
			KEKMode:    "passphrase-argon2id",
			WrappedCEK: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			Argon2id: &Argon2idParams{
				Salt:        []byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f},
				Memory:      65536,
				Iterations:  3,
				Parallelism: 4,
				KeyLen:      32,
			},
		},
	}
	b, err := json.MarshalIndent(in, "", "  ")
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got Manifest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v\nJSON:\n%s", err, b)
	}
	if got.ChainEncryption == nil {
		t.Fatalf("ChainEncryption nil after round-trip")
	}
	if got.ChainEncryption.Algorithm != "AES-256-GCM" {
		t.Errorf("Algorithm: got %q", got.ChainEncryption.Algorithm)
	}
	if got.ChainEncryption.Mode != "per-chain" {
		t.Errorf("Mode: got %q", got.ChainEncryption.Mode)
	}
	if got.ChainEncryption.KEKMode != "passphrase-argon2id" {
		t.Errorf("KEKMode: got %q", got.ChainEncryption.KEKMode)
	}
	if len(got.ChainEncryption.WrappedCEK) != 8 {
		t.Errorf("WrappedCEK length: got %d", len(got.ChainEncryption.WrappedCEK))
	}
	if got.ChainEncryption.Argon2id == nil {
		t.Fatalf("Argon2id nil after round-trip")
	}
	if got.ChainEncryption.Argon2id.Memory != 65536 {
		t.Errorf("Argon2id.Memory: got %d", got.ChainEncryption.Argon2id.Memory)
	}
	if len(got.ChainEncryption.Argon2id.Salt) != 16 {
		t.Errorf("Argon2id.Salt length: got %d", len(got.ChainEncryption.Argon2id.Salt))
	}
	if len(got.Tables[0].Chunks) != 1 {
		t.Fatalf("Chunks: got %d", len(got.Tables[0].Chunks))
	}
	if got.Tables[0].Chunks[0].Encryption == nil {
		t.Fatalf("ChunkInfo.Encryption nil after round-trip")
	}
	if got.Tables[0].Chunks[0].Encryption.Algorithm != "AES-256-GCM" {
		t.Errorf("ChunkEncryption.Algorithm: got %q", got.Tables[0].Chunks[0].Encryption.Algorithm)
	}
}

// Plaintext manifests should stay encryption-shape-free after a JSON
// round-trip — pre-Phase-6 manifests are bit-identical post round-trip
// because all encryption fields use omitempty.
func TestManifest_PlaintextStaysPlaintext(t *testing.T) {
	in := &Manifest{
		FormatVersion: BackupFormatVersion,
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{Tables: []*ir.Table{}},
		Tables: []*TableManifest{
			{
				Name:     "users",
				RowCount: 1,
				Chunks: []*ChunkInfo{
					{File: "chunks/users/users-0.jsonl.gz", RowCount: 1, SHA256: "abc"},
				},
			},
		},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	bs := string(b)
	for _, banned := range []string{"chain_encryption", "encryption", "wrapped_cek", "argon2id"} {
		if strings.Contains(bs, banned) {
			t.Errorf("plaintext manifest JSON unexpectedly contains %q: %s", banned, bs)
		}
	}
	var got Manifest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.ChainEncryption != nil {
		t.Errorf("ChainEncryption non-nil after plaintext round-trip")
	}
	if got.Tables[0].Chunks[0].Encryption != nil {
		t.Errorf("ChunkEncryption non-nil after plaintext round-trip")
	}
}

// TestManifest_SchemaHistory_RoundTrip pins the ADR-0049 Chunk D
// Manifest.SchemaHistory wire shape: marshal a Manifest carrying
// entries that span the codec's type families (reuses the Bug-74
// "pin the class" discipline so every family the dispatched codec
// supports is exercised, not one representative), round-trip
// through encoding/json, and assert deep-equal recovery.
func TestManifest_SchemaHistory_RoundTrip(t *testing.T) {
	tblA := &ir.Table{
		Schema: "public",
		Name:   "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
		},
	}
	tblB := &ir.Table{
		Schema: "public",
		Name:   "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "tags", Type: ir.Array{Element: ir.Varchar{Length: 64}}, Nullable: true},
			{Name: "ts", Type: ir.Timestamp{Precision: 6, WithTimeZone: true}},
		},
	}
	tblC := &ir.Table{
		Schema: "public",
		Name:   "geo",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "loc", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326, IsGeography: true}},
			{Name: "tags", Type: ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{1536}}},
			{Name: "flags", Type: ir.Bit{Length: 8}},
		},
	}
	mkEntry := func(streamID, schema, table, anchorToken string, tbl *ir.Table) *SchemaHistoryEntry {
		payload, err := ir.MarshalTable(tbl)
		if err != nil {
			panic(err)
		}
		return &SchemaHistoryEntry{
			StreamID:       streamID,
			Schema:         schema,
			Table:          table,
			AnchorPosition: ir.Position{Engine: "postgres", Token: anchorToken},
			TableJSON:      payload,
		}
	}
	in := &Manifest{
		FormatVersion: BackupFormatVersion,
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{Tables: []*ir.Table{tblA, tblB, tblC}},
		Kind:          BackupKindIncremental,
		SchemaHistory: []*SchemaHistoryEntry{
			mkEntry("", "public", "users", "0/1000000", tblA),
			mkEntry("", "public", "events", "0/2000000", tblB),
			mkEntry("", "public", "geo", "0/3000000", tblC),
		},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got Manifest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got.SchemaHistory) != len(in.SchemaHistory) {
		t.Fatalf("SchemaHistory len: got %d want %d", len(got.SchemaHistory), len(in.SchemaHistory))
	}
	for i, want := range in.SchemaHistory {
		gotE := got.SchemaHistory[i]
		if gotE == nil {
			t.Errorf("[%d] entry nil", i)
			continue
		}
		if gotE.StreamID != want.StreamID || gotE.Schema != want.Schema || gotE.Table != want.Table {
			t.Errorf("[%d] identity mismatch: got %s/%s/%s want %s/%s/%s",
				i, gotE.StreamID, gotE.Schema, gotE.Table, want.StreamID, want.Schema, want.Table)
		}
		if gotE.AnchorPosition != want.AnchorPosition {
			t.Errorf("[%d] anchor mismatch: got %+v want %+v", i, gotE.AnchorPosition, want.AnchorPosition)
		}
		gotT, err := ir.UnmarshalTable(gotE.TableJSON)
		if err != nil {
			t.Errorf("[%d] UnmarshalTable: %v", i, err)
			continue
		}
		wantT, err := ir.UnmarshalTable(want.TableJSON)
		if err != nil {
			t.Errorf("[%d] UnmarshalTable want: %v", i, err)
			continue
		}
		if gotT.Name != wantT.Name || len(gotT.Columns) != len(wantT.Columns) {
			t.Errorf("[%d] table shape drift: got %s/%d cols want %s/%d cols",
				i, gotT.Name, len(gotT.Columns), wantT.Name, len(wantT.Columns))
		}
		for j := range wantT.Columns {
			if gotT.Columns[j].Name != wantT.Columns[j].Name {
				t.Errorf("[%d][%d] column name drift: got %q want %q",
					i, j, gotT.Columns[j].Name, wantT.Columns[j].Name)
			}
			if !typesEqualForTest(gotT.Columns[j].Type, wantT.Columns[j].Type) {
				t.Errorf("[%d][%d] column type drift: got %#v want %#v",
					i, j, gotT.Columns[j].Type, wantT.Columns[j].Type)
			}
		}
	}
}

// TestManifest_SchemaHistory_BackwardCompat_NoField pins the Chunk D
// append-only invariant: a pre-Chunk-D manifest (no schema_history
// key in the JSON) decodes clean with SchemaHistory == nil. Forward-
// compat for older sluice that ignores unknown fields is guaranteed
// by encoding/json default unknown-field behaviour; this test pins
// the reverse — a NEW sluice reading an OLD manifest sees the
// omitted field as a zero-length slice (nil, NOT an error).
func TestManifest_SchemaHistory_BackwardCompat_NoField(t *testing.T) {
	body := `{
		"format_version": 1,
		"source_engine": "postgres",
		"kind": "incremental",
		"schema": {"tables": []}
	}`
	var got Manifest
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("Unmarshal pre-D manifest: %v", err)
	}
	if got.SchemaHistory != nil {
		t.Errorf("SchemaHistory must be nil for a pre-D manifest; got %v", got.SchemaHistory)
	}
	b, err := json.Marshal(&got)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(b), "schema_history") {
		t.Errorf("re-emitted pre-D manifest unexpectedly carries schema_history: %s", b)
	}
}

// typesEqualForTest compares two IR Types by their codec form to
// avoid sealed-interface DeepEqual pitfalls on equivalent shapes.
func typesEqualForTest(a, b ir.Type) bool {
	ab, err := ir.MarshalType(a)
	if err != nil {
		return false
	}
	bb, err := ir.MarshalType(b)
	if err != nil {
		return false
	}
	return bytes.Equal(ab, bb)
}
