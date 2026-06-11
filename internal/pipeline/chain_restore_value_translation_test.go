// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Phase 5.3 unit tests: change-event value translation in cross-engine
// chain restore.
//
// Cross-engine value translation lives at the engine-applier layer:
// each applier looks up its own *target* column types and calls its
// engine-specific prepareValue helper to shape incoming values for
// the target's wire protocol. The chain-restore stream feeds change
// events through the applier verbatim — no separate translation pass
// is needed at the chain-restore boundary because the applier already
// handles cross-engine value shaping for live CDC (PG → MySQL,
// MySQL → PG) since v0.4.0+.
//
// These tests assert the chain-restore stream preserves values
// faithfully end-to-end: an incremental's chunk that carries a UUID
// string, a JSONB []byte, a TIMESTAMPTZ time.Time, or a TINYINT(1)
// bool round-trips through the chunk codec into the applier's input
// channel without mutation. The applier then handles the cross-engine
// shaping per its existing live-CDC contract.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// writeOneChangeChunk encodes the supplied changes into a change
// chunk under chunkPath in store. Returns the chunk's recorded
// SHA-256 so the test can construct a manifest the chain restorer can
// verify.
func writeOneChangeChunk(t *testing.T, store irbackup.Store, chunkPath string, changes []ir.Change) (sha string, rowCount int64) {
	t.Helper()
	var buf bytes.Buffer
	// DefaultCodec so encode here agrees with the restore read-default
	// (these tests fix no codec in the manifest; codec is incidental to
	// value translation). v0.67.0: DefaultCodec is zstd.
	cw, err := newChangeChunkWriter(&buf, nil, DefaultCodec)
	if err != nil {
		t.Fatalf("newChangeChunkWriter: %v", err)
	}
	for _, c := range changes {
		if err := cw.WriteChange(c); err != nil {
			t.Fatalf("WriteChange: %v", err)
		}
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	sha = cw.Hash()
	rowCount = cw.ChangeCount()
	if err := store.Put(context.Background(), chunkPath, &buf); err != nil {
		t.Fatalf("Put: %v", err)
	}
	return sha, rowCount
}

// TestChainRestore_CrossEngine_UUIDValuePreserved verifies a UUID
// value (canonical Go shape: string "550e8400-...") in a change chunk
// arrives at the applier verbatim. The applier's prepareValue then
// binds the string to the MySQL CHAR(36) target column.
func TestChainRestore_CrossEngine_UUIDValuePreserved(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "tokens",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.UUID{}},
			{Name: "label", Type: ir.Varchar{Length: 64}},
		},
	}}}

	pos := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		Schema:        schema,
		EndPosition:   pos,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}

	const uuidVal = "550e8400-e29b-41d4-a716-446655440000"
	changes := []ir.Change{
		ir.TxBegin{Position: pos},
		ir.Insert{
			Position: pos, Table: "tokens",
			Row: ir.Row{"id": uuidVal, "label": "primary"},
		},
		ir.TxCommit{Position: pos},
	}
	sha, rowCount := writeOneChangeChunk(t, store, "chunks/_changes/changes-0.jsonl.gz", changes)

	incrPos := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/200"}`}
	incr := &irbackup.Manifest{
		FormatVersion:  irbackup.BackupFormatVersion,
		CreatedAt:      time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: full.BackupID,
		StartPosition:  pos,
		EndPosition:    incrPos,
		ChangeChunks: []*irbackup.ChunkInfo{{
			File: "chunks/_changes/changes-0.jsonl.gz", SHA256: sha, RowCount: rowCount,
		}},
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	if err := writeManifestAt(context.Background(), store, "manifests/incr-0001.json", incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("mysql"),
	}
	cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}
	if err := cr.Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}

	tgt.mu.Lock()
	got := append([]ir.Change(nil), tgt.applied...)
	tgt.mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("applied len = %d (want 3): %v", len(got), got)
	}
	ins, ok := got[1].(ir.Insert)
	if !ok {
		t.Fatalf("got[1] = %T; want Insert", got[1])
	}
	gotUUID, _ := ins.Row["id"].(string)
	if gotUUID != uuidVal {
		t.Errorf("UUID value = %q; want %q (chain stream must preserve verbatim; applier shapes for target)", gotUUID, uuidVal)
	}
}

// TestChainRestore_CrossEngine_JSONbytesPreserved verifies that JSONB
// values (canonical Go shape: []byte containing the JSON document)
// arrive at the applier verbatim. The MySQL applier's prepareValue
// converts []byte → string for JSON columns to avoid the `_binary`
// charset prefix that breaks Vitess JSON inserts.
func TestChainRestore_CrossEngine_JSONbytesPreserved(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "payload", Type: ir.JSON{Binary: true}},
		},
	}}}
	pos := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, CreatedAt: time.Now().UTC(),
		SourceEngine: "postgres", Kind: irbackup.BackupKindFull,
		Schema: schema, EndPosition: pos,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}

	jsonb := []byte(`{"type":"signup","when":"2026-05-08"}`)
	changes := []ir.Change{
		ir.TxBegin{Position: pos},
		ir.Insert{Position: pos, Table: "events", Row: ir.Row{"id": int64(1), "payload": jsonb}},
		ir.TxCommit{Position: pos},
	}
	sha, rowCount := writeOneChangeChunk(t, store, "chunks/_changes/changes-0.jsonl.gz", changes)
	incr := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, CreatedAt: time.Now().UTC(),
		SourceEngine: "postgres", Kind: irbackup.BackupKindIncremental,
		ParentBackupID: full.BackupID, StartPosition: pos,
		EndPosition: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/200"}`},
		ChangeChunks: []*irbackup.ChunkInfo{{
			File: "chunks/_changes/changes-0.jsonl.gz", SHA256: sha, RowCount: rowCount,
		}},
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	if err := writeManifestAt(context.Background(), store, "manifests/incr-0001.json", incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("mysql"),
	}
	cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}
	if err := cr.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tgt.mu.Lock()
	defer tgt.mu.Unlock()
	if len(tgt.applied) < 2 {
		t.Fatalf("applied = %d; want at least 2", len(tgt.applied))
	}
	ins, ok := tgt.applied[1].(ir.Insert)
	if !ok {
		t.Fatalf("got = %T; want Insert", tgt.applied[1])
	}
	gotPayload, ok := ins.Row["payload"].([]byte)
	if !ok {
		t.Fatalf("payload = %T; want []byte (chain stream preserves canonical IR shape)", ins.Row["payload"])
	}
	if !bytes.Equal(gotPayload, jsonb) {
		t.Errorf("payload bytes = %q; want %q", string(gotPayload), string(jsonb))
	}
}

// TestChainRestore_CrossEngine_TimestampPreserved verifies a
// time.Time value arrives at the applier verbatim. PG's TIMESTAMPTZ
// produces time.Time in UTC; the MySQL DATETIME applier writes it as
// a UTC-anchored datetime per the docs/value-types.md contract.
func TestChainRestore_CrossEngine_TimestampPreserved(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "occurred_at", Type: ir.Timestamp{Precision: 6}},
		},
	}}}
	pos := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, CreatedAt: time.Now().UTC(),
		SourceEngine: "postgres", Kind: irbackup.BackupKindFull,
		Schema: schema, EndPosition: pos,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}

	occ := time.Date(2026, 5, 8, 14, 30, 0, 123456000, time.UTC)
	changes := []ir.Change{
		ir.TxBegin{Position: pos},
		ir.Insert{Position: pos, Table: "events", Row: ir.Row{"id": int64(1), "occurred_at": occ}},
		ir.TxCommit{Position: pos},
	}
	sha, rowCount := writeOneChangeChunk(t, store, "chunks/_changes/changes-0.jsonl.gz", changes)
	incr := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, CreatedAt: time.Now().UTC(),
		SourceEngine: "postgres", Kind: irbackup.BackupKindIncremental,
		ParentBackupID: full.BackupID, StartPosition: pos,
		EndPosition: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/200"}`},
		ChangeChunks: []*irbackup.ChunkInfo{{
			File: "chunks/_changes/changes-0.jsonl.gz", SHA256: sha, RowCount: rowCount,
		}},
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	if err := writeManifestAt(context.Background(), store, "manifests/incr-0001.json", incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("mysql"),
	}
	cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}
	if err := cr.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tgt.mu.Lock()
	defer tgt.mu.Unlock()
	ins, ok := tgt.applied[1].(ir.Insert)
	if !ok {
		t.Fatalf("got = %T; want Insert", tgt.applied[1])
	}
	gotT, ok := ins.Row["occurred_at"].(time.Time)
	if !ok {
		t.Fatalf("occurred_at = %T; want time.Time", ins.Row["occurred_at"])
	}
	if !gotT.Equal(occ) {
		t.Errorf("time = %v; want %v", gotT, occ)
	}
}

// TestChainRestore_CrossEngine_BoolValuePreserved verifies a bool
// value (canonical Go shape: bool) arrives at the applier verbatim.
// The MySQL CDC reader produces bool for TINYINT(1) columns; the PG
// target's applier accepts bool natively for BOOLEAN columns via pgx.
func TestChainRestore_CrossEngine_BoolValuePreserved(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "active", Type: ir.Boolean{}},
		},
	}}}
	pos := ir.Position{Engine: "mysql", Token: `gtid:abc:1`}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, CreatedAt: time.Now().UTC(),
		SourceEngine: "mysql", Kind: irbackup.BackupKindFull,
		Schema: schema, EndPosition: pos,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}

	changes := []ir.Change{
		ir.TxBegin{Position: pos},
		ir.Update{
			Position: pos, Table: "users",
			Before: ir.Row{"id": int64(1), "active": true},
			After:  ir.Row{"id": int64(1), "active": false},
		},
		ir.TxCommit{Position: pos},
	}
	sha, rowCount := writeOneChangeChunk(t, store, "chunks/_changes/changes-0.jsonl.gz", changes)
	incr := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, CreatedAt: time.Now().UTC(),
		SourceEngine: "mysql", Kind: irbackup.BackupKindIncremental,
		ParentBackupID: full.BackupID, StartPosition: pos,
		EndPosition: ir.Position{Engine: "mysql", Token: `gtid:abc:2`},
		ChangeChunks: []*irbackup.ChunkInfo{{
			File: "chunks/_changes/changes-0.jsonl.gz", SHA256: sha, RowCount: rowCount,
		}},
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	if err := writeManifestAt(context.Background(), store, "manifests/incr-0001.json", incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}
	if err := cr.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tgt.mu.Lock()
	defer tgt.mu.Unlock()
	upd, ok := tgt.applied[1].(ir.Update)
	if !ok {
		t.Fatalf("got = %T; want Update", tgt.applied[1])
	}
	gotBefore, ok := upd.Before["active"].(bool)
	if !ok {
		t.Fatalf("Before.active = %T; want bool", upd.Before["active"])
	}
	gotAfter, ok := upd.After["active"].(bool)
	if !ok {
		t.Fatalf("After.active = %T; want bool", upd.After["active"])
	}
	if gotBefore != true || gotAfter != false {
		t.Errorf("before/after = %v/%v; want true/false", gotBefore, gotAfter)
	}
}
