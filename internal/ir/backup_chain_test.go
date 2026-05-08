// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"encoding/json"
	"testing"
	"time"
)

// TestComputeSchemaHash_DeterministicAndDistinguishing pins the
// load-bearing claim that schema fingerprinting is content-defined:
// two schemas with the same shape produce the same hash, two
// distinct schemas produce different hashes.
func TestComputeSchemaHash_DeterministicAndDistinguishing(t *testing.T) {
	a := &Schema{Tables: []*Table{{
		Name:    "users",
		Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}},
	}}}
	b := &Schema{Tables: []*Table{{
		Name:    "users",
		Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}},
	}}}
	c := &Schema{Tables: []*Table{{
		Name: "users",
		Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64}},
			{Name: "email", Type: Varchar{Length: 255}},
		},
	}}}

	ha, err := ComputeSchemaHash(a)
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	hb, err := ComputeSchemaHash(b)
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	hc, err := ComputeSchemaHash(c)
	if err != nil {
		t.Fatalf("hash c: %v", err)
	}
	if ha != hb {
		t.Errorf("identical schemas produced different hashes: %s vs %s", ha, hb)
	}
	if ha == hc {
		t.Errorf("distinct schemas produced same hash: %s", ha)
	}
	if len(ha) != 64 {
		t.Errorf("hash length = %d; want 64 hex chars", len(ha))
	}
}

// TestComputeSchemaHash_NilStable confirms a nil schema doesn't
// crash and produces a stable sentinel hash.
func TestComputeSchemaHash_NilStable(t *testing.T) {
	h1, err := ComputeSchemaHash(nil)
	if err != nil {
		t.Fatalf("nil hash: %v", err)
	}
	h2, _ := ComputeSchemaHash(nil)
	if h1 != h2 {
		t.Errorf("nil hash unstable: %s vs %s", h1, h2)
	}
	// nil hash should not collide with any real schema's hash.
	hReal, _ := ComputeSchemaHash(&Schema{})
	if h1 == hReal {
		t.Errorf("nil schema hash collides with empty-schema hash: %s", h1)
	}
}

// TestComputeBackupID_DeterministicAndDistinguishing pins that the
// backup identity is content-defined and distinguishes manifests
// with different windows.
func TestComputeBackupID_DeterministicAndDistinguishing(t *testing.T) {
	t1 := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 8, 13, 0, 0, 0, time.UTC)

	m1 := &Manifest{
		CreatedAt:    t1,
		SourceEngine: "postgres",
		Kind:         BackupKindIncremental,
		EndPosition:  Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/16B7350"}`},
	}
	m2 := &Manifest{
		CreatedAt:    t1,
		SourceEngine: "postgres",
		Kind:         BackupKindIncremental,
		EndPosition:  Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/16B7350"}`},
	}
	// Same content → same ID.
	if id1, id2 := ComputeBackupID(m1), ComputeBackupID(m2); id1 != id2 {
		t.Errorf("identical manifests produced different IDs: %s vs %s", id1, id2)
	}
	// Different timestamp → different ID.
	m3 := *m1
	m3.CreatedAt = t2
	if id1, id3 := ComputeBackupID(m1), ComputeBackupID(&m3); id1 == id3 {
		t.Errorf("distinct timestamps produced same ID: %s", id1)
	}
	// Different end position → different ID.
	m4 := *m1
	m4.EndPosition = Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/16B7400"}`}
	if id1, id4 := ComputeBackupID(m1), ComputeBackupID(&m4); id1 == id4 {
		t.Errorf("distinct end-positions produced same ID: %s", id1)
	}
	// IDs are 16-hex (8 bytes truncated).
	if got := len(ComputeBackupID(m1)); got != 16 {
		t.Errorf("backup ID length = %d; want 16", got)
	}
}

// TestComputeBackupID_NilSafe confirms the helper is safe on nil
// input and returns the empty string.
func TestComputeBackupID_NilSafe(t *testing.T) {
	if got := ComputeBackupID(nil); got != "" {
		t.Errorf("nil manifest ID = %q; want empty", got)
	}
}

// TestManifestRoundTrip_Phase3Fields confirms the Phase 3 manifest
// extensions survive a JSON round-trip with their values intact.
func TestManifestRoundTrip_Phase3Fields(t *testing.T) {
	in := &Manifest{
		FormatVersion: BackupFormatVersion,
		SluiceVersion: "v0.17.0-test",
		CreatedAt:     time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema: &Schema{Tables: []*Table{{
			Name:    "users",
			Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}},
		}}},
		PartialState:   BackupStateComplete,
		BackupID:       "abc123def4567890",
		Kind:           BackupKindIncremental,
		ParentBackupID: "0011223344556677",
		StartPosition:  Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/16B7000"}`},
		EndPosition:    Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/16B7800"}`},
		SchemaHash:     "deadbeef",
		SchemaDelta: []*SchemaDeltaEntry{
			{
				Kind:  SchemaDeltaAlterTable,
				Table: "users",
				Before: &Table{
					Name:    "users",
					Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}},
				},
				After: &Table{
					Name: "users",
					Columns: []*Column{
						{Name: "id", Type: Integer{Width: 64}},
						{Name: "email", Type: Varchar{Length: 255}},
					},
				},
			},
		},
		ChangeChunks: []*ChunkInfo{
			{File: "chunks/_changes/changes-0.jsonl.gz", RowCount: 42, SHA256: "feed0001"},
		},
	}

	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out Manifest
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.Kind != BackupKindIncremental {
		t.Errorf("Kind = %q; want %q", out.Kind, BackupKindIncremental)
	}
	if out.ParentBackupID != "0011223344556677" {
		t.Errorf("ParentBackupID = %q", out.ParentBackupID)
	}
	if out.BackupID != "abc123def4567890" {
		t.Errorf("BackupID = %q", out.BackupID)
	}
	if out.StartPosition != in.StartPosition {
		t.Errorf("StartPosition = %+v; want %+v", out.StartPosition, in.StartPosition)
	}
	if out.EndPosition != in.EndPosition {
		t.Errorf("EndPosition = %+v; want %+v", out.EndPosition, in.EndPosition)
	}
	if out.SchemaHash != "deadbeef" {
		t.Errorf("SchemaHash = %q", out.SchemaHash)
	}
	if len(out.SchemaDelta) != 1 || out.SchemaDelta[0].Kind != SchemaDeltaAlterTable {
		t.Errorf("SchemaDelta = %+v", out.SchemaDelta)
	}
	if len(out.ChangeChunks) != 1 || out.ChangeChunks[0].RowCount != 42 {
		t.Errorf("ChangeChunks = %+v", out.ChangeChunks)
	}
	// Spot-check the table round-trip preserved the schema's columns.
	if len(out.SchemaDelta[0].After.Columns) != 2 {
		t.Errorf("After.Columns len = %d; want 2", len(out.SchemaDelta[0].After.Columns))
	}
}

// TestManifestRoundTrip_LegacyFullCompat confirms a v0.16.x-shape
// manifest (no Kind / BackupID / etc.) decodes cleanly under v0.17.0
// and the empty Kind is treated as full by the canonicaliser.
func TestManifestRoundTrip_LegacyFullCompat(t *testing.T) {
	legacy := []byte(`{
		"format_version": 1,
		"sluice_version": "v0.16.1",
		"created_at": "2026-05-08T12:00:00Z",
		"source_engine": "postgres",
		"schema": {"Tables":[{"Schema":"","Name":"users","Columns":[{"name":"id","type":{"kind":"Integer","width":64}}]}]},
		"tables": [],
		"partial_state": "complete"
	}`)
	var m Manifest
	if err := json.Unmarshal(legacy, &m); err != nil {
		t.Fatalf("decode legacy manifest: %v", err)
	}
	if m.Kind != "" {
		t.Errorf("Kind = %q; want empty (legacy)", m.Kind)
	}
	if m.BackupID != "" {
		t.Errorf("BackupID = %q; want empty (legacy)", m.BackupID)
	}
	// canonicalKind via ComputeBackupID should normalise empty to
	// "full" so legacy manifests get a stable ID under chain helpers.
	if got := ComputeBackupID(&m); got == "" {
		t.Errorf("ComputeBackupID on legacy manifest = empty; want non-empty")
	}
}
