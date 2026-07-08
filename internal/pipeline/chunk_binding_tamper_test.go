// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0152 (audit N-8) end-to-end pins: a store-level adversary who
// can rewrite chunk files AND the manifest entries that describe them
// (SHA-256 included — the pre-ADR state authenticated nothing beyond
// the hash the adversary controls) must not be able to splice, replay,
// or reorder encrypted chunks; the untampered chain keeps restoring;
// pre-FormatVersion-5 encrypted chains (nil-AAD chunks) keep restoring
// through the version-gated legacy path; and the restore-side
// chunk-header ↔ schema check refuses the renamed-column chunk the
// chunk-format doc had always promised to catch.

package pipeline

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

const tamperPassphrase = "tamper-matrix-pass"

// fastArgonParams keeps the per-test Argon2id derivations cheap (the
// production 64 MiB default × the several envelopes each case builds
// would dominate the suite).
func fastArgonParams(t *testing.T) crypto.Argon2idParams {
	t.Helper()
	p, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	p.Memory = 1024 // 1 MiB
	p.Iterations = 1
	p.Parallelism = 1
	return p
}

func tamperEncryption(t *testing.T) *lineage.BackupEncryption {
	t.Helper()
	env, err := crypto.NewPassphraseEnvelope(tamperPassphrase, fastArgonParams(t))
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	return &lineage.BackupEncryption{
		Envelope: env,
		RebuildForChain: func(p *irbackup.Argon2idParams) (crypto.EnvelopeEncryption, error) {
			return crypto.NewPassphraseEnvelope(tamperPassphrase, crypto.Argon2idParams{
				Salt: p.Salt, Memory: p.Memory, Iterations: p.Iterations,
				Parallelism: p.Parallelism, KeyLen: p.KeyLen,
			})
		},
	}
}

// encryptedChainFixture builds an encrypted (per-chain, passphrase)
// full + one incremental whose window rolls one change chunk PER
// change (ChunkChanges=1), giving the tamper cases multiple bound
// chunks to swap. Returns the store and the incremental manifest's
// path.
func encryptedChainFixture(t *testing.T) (store irbackup.Store, incrManifestPath string) {
	t.Helper()
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
		"users": {{"id": int64(1)}, {"id": int64(2)}},
	})
	if err := (&backup.Backup{Source: src, SourceDSN: "src", Store: store, Encryption: tamperEncryption(t)}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	// Give the full an EndPosition + BackupID so the incremental can
	// chain off it (same patch the chain-restore round-trip test
	// applies). Deliberately AFTER the chunks were written: the AAD
	// binding excludes EndPosition/BackupID for exactly this reason,
	// and a regression there would fail this fixture's baseline.
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("read full manifest: %v", err)
	}
	full.EndPosition = ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full manifest: %v", err)
	}

	cdc := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{schema, schema},
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/110"}`}},
			ir.Insert{Position: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/120"}`}, Table: "users", Row: ir.Row{"id": int64(3)}},
			ir.Insert{Position: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/130"}`}, Table: "users", Row: ir.Row{"id": int64(4)}},
			ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/140"}`}},
		},
	}
	ib := &IncrementalBackup{
		Source:       cdc,
		SourceDSN:    "src",
		Store:        store,
		ParentRef:    full.BackupID,
		Window:       time.Minute,
		ChunkChanges: 1, // one chunk per change → 4 bound chunks
		Encryption:   tamperEncryption(t),
	}
	if err := ib.Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	recs, err := lineage.ListAllManifestsViaWalk(ctx, store)
	if err != nil {
		t.Fatalf("list manifests: %v", err)
	}
	incrPath := ""
	for _, r := range recs {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			incrPath = r.Path
		}
	}
	if incrPath == "" {
		t.Fatal("fixture produced no incremental manifest")
	}
	return store, incrPath
}

// tamperRestore runs a chain restore against store with the correct
// passphrase envelope, returning the applied changes (nil on error).
func tamperRestore(t *testing.T, store irbackup.Store) ([]ir.Change, error) {
	t.Helper()
	ctx := context.Background()
	root, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("read root manifest: %v", err)
	}
	if root.ChainEncryption == nil || root.ChainEncryption.Argon2id == nil {
		t.Fatal("fixture root is not encrypted")
	}
	p := root.ChainEncryption.Argon2id
	env, err := crypto.NewPassphraseEnvelope(tamperPassphrase, crypto.Argon2idParams{
		Salt: p.Salt, Memory: p.Memory, Iterations: p.Iterations,
		Parallelism: p.Parallelism, KeyLen: p.KeyLen,
	})
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	tgt := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
	chain := &backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store, Envelope: env}
	if err := chain.Run(ctx); err != nil {
		return nil, err
	}
	tgt.mu.Lock()
	defer tgt.mu.Unlock()
	return append([]ir.Change(nil), tgt.applied...), nil
}

// swapStoreFiles exchanges the CONTENTS of two store paths.
func swapStoreFiles(t *testing.T, store irbackup.Store, a, b string) {
	t.Helper()
	ctx := context.Background()
	read := func(p string) []byte {
		rc, err := store.Get(ctx, p)
		if err != nil {
			t.Fatalf("get %s: %v", p, err)
		}
		defer rc.Close()
		data, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		return data
	}
	da, db := read(a), read(b)
	if err := store.Put(ctx, a, bytes.NewReader(db)); err != nil {
		t.Fatalf("put %s: %v", a, err)
	}
	if err := store.Put(ctx, b, bytes.NewReader(da)); err != nil {
		t.Fatalf("put %s: %v", b, err)
	}
}

// TestChunkBinding_TamperMatrix is the ADR-0152 end-to-end tamper
// matrix against a per-chain encrypted chain. The adversary model is a
// store-level rewriter who also fixes up the manifest's SHA-256 /
// RowCount (the pre-ADR integrity story) — only the GCM binding stands
// between them and a silent splice.
func TestChunkBinding_TamperMatrix(t *testing.T) {
	ctx := context.Background()

	t.Run("baseline: untampered chain restores", func(t *testing.T) {
		store, _ := encryptedChainFixture(t)
		applied, err := tamperRestore(t, store)
		if err != nil {
			t.Fatalf("untampered restore failed: %v", err)
		}
		if len(applied) != 4 {
			t.Errorf("applied %d changes; want 4", len(applied))
		}
	})

	t.Run("chunk contents swapped between positions (SHAs fixed up) → refuse", func(t *testing.T) {
		store, incrPath := encryptedChainFixture(t)
		im, err := lineage.ReadManifestAt(ctx, store, incrPath)
		if err != nil {
			t.Fatalf("read incremental manifest: %v", err)
		}
		if len(im.ChangeChunks) < 2 {
			t.Fatalf("fixture produced %d change chunks; want >= 2", len(im.ChangeChunks))
		}
		c0, c1 := im.ChangeChunks[0], im.ChangeChunks[1]
		swapStoreFiles(t, store, c0.File, c1.File)
		// The adversary fixes the hashes so the SHA layer passes and
		// only the binding can object.
		c0.SHA256, c1.SHA256 = c1.SHA256, c0.SHA256
		c0.RowCount, c1.RowCount = c1.RowCount, c0.RowCount
		if err := lineage.WriteManifestAt(ctx, store, incrPath, im); err != nil {
			t.Fatalf("rewrite incremental manifest: %v", err)
		}
		_, err = tamperRestore(t, store)
		if err == nil {
			t.Fatal("swapped chunks restored cleanly; the splice class (audit N-8) is back")
		}
		if !strings.Contains(err.Error(), "does not belong at this position") {
			t.Errorf("refusal %q should name the spliced-chunk hypothesis", err.Error())
		}
	})

	t.Run("manifest entries reordered (files untouched) → refuse", func(t *testing.T) {
		store, incrPath := encryptedChainFixture(t)
		im, err := lineage.ReadManifestAt(ctx, store, incrPath)
		if err != nil {
			t.Fatalf("read incremental manifest: %v", err)
		}
		// Each entry keeps its own File+SHA (self-consistent) — only
		// the REPLAY ORDER changes. The ordinal in the change-chunk
		// binding is what catches this.
		im.ChangeChunks[0], im.ChangeChunks[1] = im.ChangeChunks[1], im.ChangeChunks[0]
		if err := lineage.WriteManifestAt(ctx, store, incrPath, im); err != nil {
			t.Fatalf("rewrite incremental manifest: %v", err)
		}
		if _, err := tamperRestore(t, store); err == nil {
			t.Fatal("reordered change chunks replayed cleanly; out-of-order replay is silent corruption")
		}
	})

	t.Run("chunk replayed from a sibling backup in the same chain (same CEK) → refuse", func(t *testing.T) {
		store, incrPath := encryptedChainFixture(t)
		im, err := lineage.ReadManifestAt(ctx, store, incrPath)
		if err != nil {
			t.Fatalf("read incremental manifest: %v", err)
		}
		// Second incremental in the same chain: same per-chain CEK, a
		// different manifest identity.
		schema := &ir.Schema{Tables: []*ir.Table{{
			Name:    "users",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}}}
		cdc := &fakeCDCEngine{
			name:           "postgres",
			schemaSequence: []*ir.Schema{schema, schema},
			cdcChanges: []ir.Change{
				ir.Insert{Position: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/200"}`}, Table: "users", Row: ir.Row{"id": int64(9)}},
			},
		}
		ib := &IncrementalBackup{
			Source:       cdc,
			SourceDSN:    "src",
			Store:        store,
			ParentRef:    im.BackupID,
			Window:       time.Minute,
			ChunkChanges: 1,
			Encryption:   tamperEncryption(t),
		}
		if err := ib.Run(ctx); err != nil {
			t.Fatalf("second IncrementalBackup.Run: %v", err)
		}
		recs, err := lineage.ListAllManifestsViaWalk(ctx, store)
		if err != nil {
			t.Fatalf("list manifests: %v", err)
		}
		incr2Path := ""
		for _, r := range recs {
			if r.Manifest.Kind == irbackup.BackupKindIncremental && r.Path != incrPath {
				incr2Path = r.Path
			}
		}
		if incr2Path == "" {
			t.Fatal("second incremental manifest not found")
		}
		im2, err := lineage.ReadManifestAt(ctx, store, incr2Path)
		if err != nil {
			t.Fatalf("read second incremental manifest: %v", err)
		}
		// Replay incr1's first chunk (index 0) as incr2's first chunk
		// (also index 0, so only the manifest IDENTITY differs), fixing
		// up the entry so SHA/counts pass.
		donor := im.ChangeChunks[0]
		victim := im2.ChangeChunks[0]
		rc, err := store.Get(ctx, donor.File)
		if err != nil {
			t.Fatalf("get donor chunk: %v", err)
		}
		donorBytes, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("read donor chunk: %v", err)
		}
		if err := store.Put(ctx, victim.File, bytes.NewReader(donorBytes)); err != nil {
			t.Fatalf("overwrite victim chunk: %v", err)
		}
		victim.SHA256 = donor.SHA256
		victim.RowCount = donor.RowCount
		if err := lineage.WriteManifestAt(ctx, store, incr2Path, im2); err != nil {
			t.Fatalf("rewrite second incremental manifest: %v", err)
		}
		_, err = tamperRestore(t, store)
		if err == nil {
			t.Fatal("a chunk replayed from a sibling backup (same chain CEK) restored cleanly; the cross-backup replay class (audit N-8) is back")
		}
		if !strings.Contains(err.Error(), "does not belong at this position") {
			t.Errorf("refusal %q should name the spliced-chunk hypothesis", err.Error())
		}
	})
}

// TestChunkBinding_PreBindingChainStillRestores pins the compat leg:
// an encrypted chain written BEFORE FormatVersion 5 — nil-AAD chunks,
// unbound CEK wrap, manifest stamped at the pre-binding version — must
// keep restoring byte-identically through the version-gated legacy
// path. The fixture is assembled with the same primitives the old
// writer used: [blobcodec.NewChunkWriter] with a nil AAD produces
// byte-identical output to the pre-ADR-0152 writer (EncryptChunk ==
// EncryptChunkWithAAD(nil)), and the manifest is stamped
// FormatVersion 1 exactly as the old binary stamped this schema.
func TestChunkBinding_PreBindingChainStillRestores(t *testing.T) {
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	params := fastArgonParams(t)
	env, err := crypto.NewPassphraseEnvelope(tamperPassphrase, params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	cek, err := crypto.GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	// Old writers wrapped the chain CEK UNBOUND.
	wrapped, err := env.WrapCEK(cek)
	if err != nil {
		t.Fatalf("WrapCEK: %v", err)
	}

	table := &ir.Table{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	var buf bytes.Buffer
	w, err := blobcodec.NewChunkWriter(&buf, []string{"id"}, cek, blobcodec.DefaultCodec, nil)
	if err != nil {
		t.Fatalf("NewChunkWriter: %v", err)
	}
	if err := w.WriteRow(ir.Row{"id": int64(7)}, table.Columns); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	const chunkPath = "chunks/users-0.jsonl.gz"
	if err := store.Put(ctx, chunkPath, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("put chunk: %v", err)
	}

	manifest := &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionLegacy, // what the old binary stamped
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		Schema:        &ir.Schema{Tables: []*ir.Table{table}},
		PartialState:  irbackup.BackupStateComplete,
		Tables: []*irbackup.TableManifest{{
			Name: "users", RowCount: 1,
			Chunks: []*irbackup.ChunkInfo{{
				File: chunkPath, RowCount: 1, SHA256: w.Hash(),
				Encryption: &irbackup.ChunkEncryption{
					Algorithm: crypto.AlgorithmAESGCM, NonceLen: crypto.NonceLen, AuthTagLen: crypto.AuthTagLen,
				},
			}},
		}},
		ChainEncryption: &irbackup.ChainEncryption{
			Algorithm:  crypto.AlgorithmAESGCM,
			Mode:       crypto.EncryptModePerChain,
			KEKMode:    crypto.KEKModePassphrase,
			WrappedCEK: wrapped,
			Argon2id: &irbackup.Argon2idParams{
				Salt: params.Salt, Memory: params.Memory, Iterations: params.Iterations,
				Parallelism: params.Parallelism, KeyLen: params.KeyLen,
			},
		},
	}
	if err := lineage.WriteManifest(ctx, store, manifest); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	tgt := newRestoreRecorderEngine("postgres")
	readEnv, err := crypto.NewPassphraseEnvelope(tamperPassphrase, params)
	if err != nil {
		t.Fatalf("read envelope: %v", err)
	}
	rest := &backup.Restore{Target: tgt, TargetDSN: "tgt", Store: store, Envelope: readEnv}
	if err := rest.Run(ctx); err != nil {
		t.Fatalf("pre-binding encrypted chain failed to restore: %v (the FormatVersion gate must route pre-v5 chunks through the nil-AAD path)", err)
	}
	_, rowsByTable := tgt.snapshot()
	rows := rowsByTable["users"]
	if len(rows) != 1 || !valuesEquivalent(rows[0]["id"], int64(7)) {
		t.Errorf("restored rows = %v; want the single id=7 row byte-exact", rows)
	}
}

// TestRestore_ChunkHeaderColumnMismatchRefuses pins the restore-side
// chunk-header ↔ schema check (audit N-8 item 3 — the check the
// chunk-format doc promised since Phase 1): a chunk written against a
// RENAMED column must refuse loudly, naming the table, the chunk, and
// both column deltas — never silently mis-key rows.
func TestRestore_ChunkHeaderColumnMismatchRefuses(t *testing.T) {
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
		"users": {{"id": int64(1)}},
	})
	if err := (&backup.Backup{Source: src, SourceDSN: "src", Store: store}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	manifest, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	chunk := manifest.Tables[0].Chunks[0]

	// The comment-(3) scenario: the chunk on the store was written
	// against a schema whose column is named differently than the
	// manifest schema records (a rename across schema versions / a
	// mis-assembled backup directory).
	var buf bytes.Buffer
	w, err := blobcodec.NewChunkWriter(&buf, []string{"identifier"}, nil, blobcodec.DefaultCodec, nil)
	if err != nil {
		t.Fatalf("NewChunkWriter: %v", err)
	}
	renamed := []*ir.Column{{Name: "identifier", Type: ir.Integer{Width: 64}}}
	if err := w.WriteRow(ir.Row{"identifier": int64(1)}, renamed); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Put(ctx, chunk.File, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("overwrite chunk: %v", err)
	}
	chunk.SHA256 = w.Hash() // the adversary/mis-assembly keeps the SHA layer green
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, manifest); err != nil {
		t.Fatalf("rewrite manifest: %v", err)
	}

	tgt := newRestoreRecorderEngine("postgres")
	err = (&backup.Restore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(ctx)
	if err == nil {
		t.Fatal("renamed-column chunk restored cleanly; rows were silently mis-keyed (audit N-8 item 3)")
	}
	for _, want := range []string{"does not match table", "users", "id", "identifier"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q should contain %q", err.Error(), want)
		}
	}
}

// TestChainRestore_SchemaHashMismatchRefuses pins the manifest-level
// fingerprint check (audit N-8 item 4 — the check the SchemaHash field
// doc promised): a manifest whose carried schema no longer re-hashes
// to its recorded SchemaHash refuses BEFORE anything lands on the
// target.
func TestChainRestore_SchemaHashMismatchRefuses(t *testing.T) {
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
		"users": {{"id": int64(1)}},
	})
	if err := (&backup.Backup{Source: src, SourceDSN: "src", Store: store}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	manifest, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if manifest.SchemaHash == "" {
		t.Fatal("full manifest carries no SchemaHash; the ADR-0152 write-side stamp is missing")
	}
	// Corrupt the carried schema without re-stamping the hash — the
	// truncated-rewrite / bit-rot shape.
	manifest.Schema.Tables[0].Columns[0].Name = "mangled"
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, manifest); err != nil {
		t.Fatalf("rewrite manifest: %v", err)
	}
	tgt := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
	err = (&backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(ctx)
	if err == nil {
		t.Fatal("schema-hash-mismatched manifest restored cleanly; the corruption check (audit N-8 item 4) is missing")
	}
	if !strings.Contains(err.Error(), "schema hash mismatch") {
		t.Errorf("refusal %q should name the schema hash mismatch", err.Error())
	}
	phases, _ := tgt.snapshot()
	if len(phases) != 0 {
		t.Errorf("target saw phases %v before the refusal; the check must run before anything lands", phases)
	}
}
