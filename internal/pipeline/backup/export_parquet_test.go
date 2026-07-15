// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// exportSeed is one seeded backup store for export tests.
type exportSeed struct {
	store    irbackup.Store
	manifest *irbackup.Manifest
}

// exportTestSchema is the seeded shape: a "users" table with a value
// spread wide enough to prove the manifest→chunk→codec wiring (the
// exhaustive family matrix lives in internal/pipeline/parquetexport).
func exportTestSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "name", Type: ir.Text{}, Nullable: true},
				{Name: "score", Type: ir.Float{Precision: ir.FloatDouble}, Nullable: true},
			},
		},
		{
			Name: "empty",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
			},
		},
	}}
}

func exportTestRows() [][]ir.Row {
	return [][]ir.Row{
		{ // chunk 0
			{"id": int64(1), "name": "alice", "score": 1.5},
			{"id": int64(2), "name": nil, "score": math.NaN()},
		},
		{ // chunk 1
			{"id": int64(3), "name": "", "score": float64(0)},
		},
	}
}

// writeExportChunk writes one JSON-Lines chunk through the REAL chunk
// writer and returns its manifest entry.
func writeExportChunk(t *testing.T, store irbackup.Store, path string, cols []*ir.Column, rows []ir.Row, cek, aad []byte, codec blobcodec.Codec) *irbackup.ChunkInfo {
	t.Helper()
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	var buf bytes.Buffer
	w, err := blobcodec.NewChunkWriter(&buf, names, cek, codec, aad)
	if err != nil {
		t.Fatalf("NewChunkWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, cols); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("chunk writer close: %v", err)
	}
	if err := store.Put(context.Background(), path, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("put chunk: %v", err)
	}
	info := &irbackup.ChunkInfo{File: path, RowCount: w.RowCount(), SHA256: w.Hash()}
	if cek != nil {
		info.Encryption = &irbackup.ChunkEncryption{Algorithm: "AES-256-GCM", NonceLen: 12, AuthTagLen: 16}
	}
	return info
}

// seedPlaintextExportBackup writes a complete plaintext full backup
// (manifest + chunks + lineage) into a fresh local store.
func seedPlaintextExportBackup(t *testing.T) exportSeed {
	t.Helper()
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := exportTestSchema()
	m := &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionLegacy,
		CreatedAt:     time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		Schema:        schema,
		PartialState:  irbackup.BackupStateComplete,
	}
	users := schema.Tables[0]
	chunks := make([]*irbackup.ChunkInfo, 0, 2)
	var total int64
	for i, rows := range exportTestRows() {
		path := "chunks/users/users-" + string(rune('0'+i)) + ".jsonl.gz"
		info := writeExportChunk(t, store, path, users.Columns, rows, nil, nil, blobcodec.CodecGzip)
		chunks = append(chunks, info)
		total += info.RowCount
	}
	m.Tables = []*irbackup.TableManifest{
		{Name: "users", RowCount: total, Chunks: chunks},
		{Name: "empty", RowCount: 0},
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, m); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := lineage.UpdateLineageForManifestBestEffort(ctx, store, m, lineage.ManifestFileName, blobcodec.CodecGzip); err != nil {
		t.Fatalf("update lineage: %v", err)
	}
	return exportSeed{store: store, manifest: m}
}

// runExport runs a ParquetExport into a fresh output dir, failing the
// test on error, and returns the output dir.
func runExport(t *testing.T, e *ParquetExport) string {
	t.Helper()
	outDir := t.TempDir()
	out, err := blobcodec.NewLocalStore(outDir)
	if err != nil {
		t.Fatalf("NewLocalStore(out): %v", err)
	}
	e.Output = out
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("export run: %v", err)
	}
	return outDir
}

// readParquetRows loads an exported file's rows via a real Parquet
// reader.
func readParquetRows(t *testing.T, path string) ([]map[string]any, *parquet.File) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	r := parquet.NewGenericReader[map[string]any](bytes.NewReader(data), f.Schema())
	defer func() { _ = r.Close() }()
	rows := make([]map[string]any, f.NumRows())
	for i := range rows {
		rows[i] = map[string]any{}
	}
	if len(rows) > 0 {
		if _, err := r.Read(rows); err != nil && err.Error() != "EOF" {
			t.Fatalf("Read: %v", err)
		}
	}
	return rows, f
}

func TestParquetExport_FullBackupHappyPath(t *testing.T) {
	seed := seedPlaintextExportBackup(t)
	outDir := runExport(t, &ParquetExport{Store: seed.store, SluiceVersion: "test"})

	// The index is present, complete, and names both tables.
	var index parquetIndex
	b, err := os.ReadFile(filepath.Join(outDir, ParquetIndexFileName))
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	if err := json.Unmarshal(b, &index); err != nil {
		t.Fatalf("parse index: %v", err)
	}
	if index.BackupID != seed.manifest.BackupID || index.SourceEngine != "postgres" || len(index.Tables) != 2 {
		t.Fatalf("index = %+v", index)
	}

	// users.parquet: rows survive value-exactly; row groups align 1:1
	// with the source chunks; provenance metadata is stamped.
	rows, f := readParquetRows(t, filepath.Join(outDir, "users.parquet"))
	if len(rows) != 3 {
		t.Fatalf("rows = %d; want 3", len(rows))
	}
	if got := len(f.RowGroups()); got != 2 {
		t.Fatalf("row groups = %d; want 2 (one per source chunk)", got)
	}
	if rows[0]["name"] != "alice" || rows[1]["name"] != nil {
		t.Fatalf("name column = %#v / %#v", rows[0]["name"], rows[1]["name"])
	}
	// Zero-shaped values stay PRESENT (the boxLeafValue wart's pin at
	// the orchestrator level): "" and 0.0 are values, not NULLs.
	if rows[2]["name"] != "" || rows[2]["name"] == nil {
		t.Fatalf(`empty-string name = %#v; want present ""`, rows[2]["name"])
	}
	if v, ok := rows[2]["score"].(float64); !ok || v != 0 {
		t.Fatalf("zero score = %#v; want present 0.0", rows[2]["score"])
	}
	meta := map[string]string{}
	for _, kv := range f.Metadata().KeyValueMetadata {
		meta[kv.Key] = kv.Value
	}
	if meta["sluice:backup_id"] != seed.manifest.BackupID || meta["sluice:table"] != "users" {
		t.Fatalf("provenance metadata = %v", meta)
	}
	if !strings.Contains(meta["sluice:source_chunks"], `"sha256"`) {
		t.Fatalf("source_chunks metadata = %q", meta["sluice:source_chunks"])
	}

	// The chunkless empty table still exports (schema-bearing, 0 rows).
	emptyRows, _ := readParquetRows(t, filepath.Join(outDir, "empty.parquet"))
	if len(emptyRows) != 0 {
		t.Fatalf("empty table rows = %d", len(emptyRows))
	}
}

func TestParquetExport_TableFilter(t *testing.T) {
	seed := seedPlaintextExportBackup(t)
	filter, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	outDir := runExport(t, &ParquetExport{Store: seed.store, Filter: filter})
	if _, err := os.Stat(filepath.Join(outDir, "users.parquet")); err != nil {
		t.Fatalf("users.parquet missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "empty.parquet")); !os.IsNotExist(err) {
		t.Fatalf("empty.parquet should be filtered out; stat err = %v", err)
	}
}

func TestParquetExport_OverwriteSentinel(t *testing.T) {
	seed := seedPlaintextExportBackup(t)
	outDir := runExport(t, &ParquetExport{Store: seed.store})

	out, _ := blobcodec.NewLocalStore(outDir)
	again := &ParquetExport{Store: seed.store, Output: out}
	err := again.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--force-overwrite") {
		t.Fatalf("second export into the same destination = %v; want the overwrite refusal", err)
	}
	again.ForceOverwrite = true
	if err := again.Run(context.Background()); err != nil {
		t.Fatalf("forced overwrite: %v", err)
	}
}

func TestParquetExport_BackupIDSelection(t *testing.T) {
	ctx := context.Background()
	seed := seedPlaintextExportBackup(t)

	// Chain an incremental after the full so the walk has a tail.
	incr := &irbackup.Manifest{
		FormatVersion:  irbackup.FormatVersionLegacy,
		CreatedAt:      seed.manifest.CreatedAt.Add(time.Hour),
		SourceEngine:   "postgres",
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: seed.manifest.BackupID,
		StartPosition:  ir.Position{Engine: "postgres", Token: "0/100"},
		EndPosition:    ir.Position{Engine: "postgres", Token: "0/200"},
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	const incrPath = "manifests/incr-0001.json"
	if err := lineage.WriteManifestAt(ctx, seed.store, incrPath, incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}
	if err := lineage.UpdateLineageForManifestBestEffort(ctx, seed.store, incr, incrPath, blobcodec.CodecGzip); err != nil {
		t.Fatalf("update lineage: %v", err)
	}

	t.Run("explicit full id exports", func(t *testing.T) {
		outDir := runExport(t, &ParquetExport{Store: seed.store, BackupID: seed.manifest.BackupID})
		if _, err := os.Stat(filepath.Join(outDir, "users.parquet")); err != nil {
			t.Fatalf("users.parquet missing: %v", err)
		}
	})

	t.Run("incremental id refused, naming the fulls", func(t *testing.T) {
		out, _ := blobcodec.NewLocalStore(t.TempDir())
		e := &ParquetExport{Store: seed.store, Output: out, BackupID: incr.BackupID}
		err := e.Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "does not name a full snapshot") || !strings.Contains(err.Error(), seed.manifest.BackupID) {
			t.Fatalf("incremental-id refusal = %v", err)
		}
	})

	t.Run("unknown id refused", func(t *testing.T) {
		out, _ := blobcodec.NewLocalStore(t.TempDir())
		e := &ParquetExport{Store: seed.store, Output: out, BackupID: "deadbeef00000000"}
		if err := e.Run(ctx); err == nil {
			t.Fatal("unknown --backup-id must refuse")
		}
	})

	t.Run("trailing incrementals are counted for the WARN", func(t *testing.T) {
		records, err := lineage.ListAllSegmentManifests(ctx, seed.store)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		rec, trailing, err := selectExportFull(records, "")
		if err != nil {
			t.Fatalf("selectExportFull: %v", err)
		}
		if lineage.ManifestBackupID(rec.Manifest) != seed.manifest.BackupID || trailing != 1 {
			t.Fatalf("selected %s trailing %d; want %s / 1", lineage.ManifestBackupID(rec.Manifest), trailing, seed.manifest.BackupID)
		}
	})
}

func TestParquetExport_TamperRefusals(t *testing.T) {
	ctx := context.Background()

	t.Run("recorded SHA mismatch is coded CHUNK-CORRUPT", func(t *testing.T) {
		seed := seedPlaintextExportBackup(t)
		seed.manifest.Tables[0].Chunks[0].SHA256 = strings.Repeat("0", 64)
		if err := lineage.WriteManifestAt(ctx, seed.store, lineage.ManifestFileName, seed.manifest); err != nil {
			t.Fatalf("rewrite manifest: %v", err)
		}
		out, _ := blobcodec.NewLocalStore(t.TempDir())
		err := (&ParquetExport{Store: seed.store, Output: out}).Run(ctx)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeBackupChunkCorrupt {
			t.Fatalf("want %s, got %v", sluicecode.CodeBackupChunkCorrupt, err)
		}
	})

	t.Run("zeroed table RowCount is coded BACKUP-INCOMPLETE", func(t *testing.T) {
		seed := seedPlaintextExportBackup(t)
		seed.manifest.Tables[0].RowCount = 0
		for _, c := range seed.manifest.Tables[0].Chunks {
			c.RowCount = 0
		}
		if err := lineage.WriteManifestAt(ctx, seed.store, lineage.ManifestFileName, seed.manifest); err != nil {
			t.Fatalf("rewrite manifest: %v", err)
		}
		out, _ := blobcodec.NewLocalStore(t.TempDir())
		err := (&ParquetExport{Store: seed.store, Output: out}).Run(ctx)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeBackupIncomplete {
			t.Fatalf("want %s, got %v", sluicecode.CodeBackupIncomplete, err)
		}
	})

	t.Run("chunkless table recording rows is coded BACKUP-INCOMPLETE", func(t *testing.T) {
		seed := seedPlaintextExportBackup(t)
		seed.manifest.Tables[0].Chunks = nil
		if err := lineage.WriteManifestAt(ctx, seed.store, lineage.ManifestFileName, seed.manifest); err != nil {
			t.Fatalf("rewrite manifest: %v", err)
		}
		out, _ := blobcodec.NewLocalStore(t.TempDir())
		err := (&ParquetExport{Store: seed.store, Output: out}).Run(ctx)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeBackupIncomplete {
			t.Fatalf("want %s, got %v", sluicecode.CodeBackupIncomplete, err)
		}
	})
}

func TestParquetExport_MultiDimArrayRefusesCoded(t *testing.T) {
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "grids",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "grid", Type: ir.Array{Element: ir.Integer{Width: 64}}, Nullable: true},
		},
	}}}
	m := &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionLegacy,
		CreatedAt:     time.Date(2026, 7, 15, 11, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		Schema:        schema,
		PartialState:  irbackup.BackupStateComplete,
	}
	rows := []ir.Row{{"id": int64(1), "grid": []any{[]any{int64(1), int64(2)}}}}
	info := writeExportChunk(t, store, "chunks/grids/grids-0.jsonl.gz", schema.Tables[0].Columns, rows, nil, nil, blobcodec.CodecGzip)
	m.Tables = []*irbackup.TableManifest{{Name: "grids", RowCount: 1, Chunks: []*irbackup.ChunkInfo{info}}}
	m.BackupID = irbackup.ComputeBackupID(m)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, m); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := lineage.UpdateLineageForManifestBestEffort(ctx, store, m, lineage.ManifestFileName, blobcodec.CodecGzip); err != nil {
		t.Fatalf("update lineage: %v", err)
	}
	out, _ := blobcodec.NewLocalStore(t.TempDir())
	err = (&ParquetExport{Store: store, Output: out}).Run(ctx)
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeExportUnrepresentable {
		t.Fatalf("want %s, got %v", sluicecode.CodeExportUnrepresentable, err)
	}
	if !strings.Contains(err.Error(), "multi-dimensional") {
		t.Fatalf("refusal must name the multi-dim cause: %v", err)
	}
}

// TestParquetExport_EncryptedChain seeds a FormatVersion-7 per-chain
// encrypted full (identity-bound CEK wrap + table-bound chunk AAD —
// exactly the shape `backup full --encrypt` writes today) and pins:
// export with the right envelope decodes values exactly; export
// without a key refuses; a key against a plaintext chain refuses.
func TestParquetExport_EncryptedChain(t *testing.T) {
	ctx := context.Background()
	// One fixed Argon2id parameter set (salt included) for the writer
	// AND the reader envelopes — in production the reader re-derives
	// from the chain root's recorded params (buildReadEnvelope).
	params, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	params.Memory, params.Iterations, params.Parallelism = 1024, 1, 1
	newEnvelope := func(t *testing.T) crypto.EnvelopeEncryption {
		t.Helper()
		env, err := crypto.NewPassphraseEnvelope("export-pass", params)
		if err != nil {
			t.Fatalf("NewPassphraseEnvelope: %v", err)
		}
		return env
	}

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	env := newEnvelope(t)
	cek := make([]byte, crypto.CEKLen)
	if _, err := rand.Read(cek); err != nil {
		t.Fatalf("rand cek: %v", err)
	}

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "secrets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "payload", Type: ir.Text{}, Nullable: true},
		},
	}}}
	m := &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionChunkTableBinding,
		CreatedAt:     time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		Schema:        schema,
		PartialState:  irbackup.BackupStateComplete,
	}
	wrapped, err := lineage.WrapChainCEK(env, cek, m)
	if err != nil {
		t.Fatalf("WrapChainCEK: %v", err)
	}
	m.ChainEncryption = &irbackup.ChainEncryption{
		Algorithm:  "AES-256-GCM",
		Mode:       crypto.EncryptModePerChain,
		KEKMode:    env.Mode(),
		WrappedCEK: wrapped,
	}
	const chunkPath = "chunks/secrets/secrets-0.jsonl.gz"
	aad := irbackup.ChunkAADForWrite(m, chunkPath, "", "secrets", cek)
	rows := []ir.Row{{"id": int64(7), "payload": "s3cr3t"}}
	info := writeExportChunk(t, store, chunkPath, schema.Tables[0].Columns, rows, cek, aad, blobcodec.CodecZstd)
	m.Tables = []*irbackup.TableManifest{{Name: "secrets", RowCount: 1, Chunks: []*irbackup.ChunkInfo{info}}}
	m.BackupID = irbackup.ComputeBackupID(m)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, m); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := lineage.UpdateLineageForManifestBestEffort(ctx, store, m, lineage.ManifestFileName, blobcodec.CodecZstd); err != nil {
		t.Fatalf("update lineage: %v", err)
	}

	t.Run("with the chain key: values decode exactly", func(t *testing.T) {
		outDir := runExport(t, &ParquetExport{Store: store, Envelope: newEnvelope(t)})
		rows, _ := readParquetRows(t, filepath.Join(outDir, "secrets.parquet"))
		if len(rows) != 1 || rows[0]["payload"] != "s3cr3t" {
			t.Fatalf("decrypted rows = %#v", rows)
		}
	})

	t.Run("no key: refused up front", func(t *testing.T) {
		out, _ := blobcodec.NewLocalStore(t.TempDir())
		err := (&ParquetExport{Store: store, Output: out}).Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "requires --encrypt") {
			t.Fatalf("keyless export of an encrypted chain = %v; want the missing-key refusal", err)
		}
	})

	t.Run("key against a plaintext chain: refused (SEC-MIRROR)", func(t *testing.T) {
		seed := seedPlaintextExportBackup(t)
		out, _ := blobcodec.NewLocalStore(t.TempDir())
		err := (&ParquetExport{Store: seed.store, Output: out, Envelope: newEnvelope(t)}).Run(ctx)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeBackupChunkAuthFailed {
			t.Fatalf("want %s, got %v", sluicecode.CodeBackupChunkAuthFailed, err)
		}
	})
}

// TestParquetExport_ValidateInputs pins the loud precondition guards.
func TestParquetExport_ValidateInputs(t *testing.T) {
	err := (&ParquetExport{}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Store is nil") {
		t.Fatalf("nil store = %v", err)
	}
	store, _ := blobcodec.NewLocalStore(t.TempDir())
	err = (&ParquetExport{Store: store}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Output is nil") {
		t.Fatalf("nil output = %v", err)
	}
	out, _ := blobcodec.NewLocalStore(t.TempDir())
	err = (&ParquetExport{Store: store, Output: out}).Run(context.Background())
	if err == nil {
		t.Fatal("empty store must refuse (no manifests)")
	}
}
