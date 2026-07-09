// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

// Audit N-14 pins: the rebuilt / synthesised lineage record's codec is
// SNIFFED from chunk bytes, never assumed. Family discipline (Bug 74):
// the sniff dispatches on the codec family, so every test that proves
// "rebuild records the sniffed codec" runs for ALL THREE codecs, not a
// representative.

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// putRealChunk writes a REAL chunk (via blobcodec.ChunkWriter, so the
// bytes are exactly what production writes) at path with the given
// codec, optionally encrypted under cek/aad. Returns nothing the sniff
// needs beyond the store content — the sniff reads bytes, not hashes.
func putRealChunk(t *testing.T, store irbackup.Store, path string, codec blobcodec.Codec, cek, aad []byte) {
	t.Helper()
	var buf bytes.Buffer
	cw, err := blobcodec.NewChunkWriter(&buf, []string{"a"}, cek, codec, aad)
	if err != nil {
		t.Fatalf("NewChunkWriter(%s): %v", codec, err)
	}
	if err := cw.WriteRow(ir.Row{"a": int64(1)}, []*ir.Column{{Name: "a"}}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("chunk Close: %v", err)
	}
	if err := store.Put(context.Background(), path, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("store.Put(%q): %v", path, err)
	}
}

// sniffTestFull builds a complete full manifest referencing one row
// chunk at chunkPath.
func sniffTestFull(chunkPath string) *irbackup.Manifest {
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: "0/100"},
		PartialState:  irbackup.BackupStateComplete,
		Tables: []*irbackup.TableManifest{{
			Name: "t", RowCount: 1,
			Chunks: []*irbackup.ChunkInfo{{File: chunkPath, RowCount: 1}},
		}},
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	return m
}

// TestRebuildCatalog_SniffsCodec_EveryFamily: `--rebuild-catalog` on a
// chain whose lineage.json is gone records the codec the chunks were
// ACTUALLY written with — for every codec family. Pre-fix this stamped
// DefaultCodec unconditionally, so a gzip/none chain got a catalog that
// lied and restore died on a zstd decode error (the N-14 wrong-heal).
func TestRebuildCatalog_SniffsCodec_EveryFamily(t *testing.T) {
	for _, codec := range []blobcodec.Codec{blobcodec.CodecNone, blobcodec.CodecGzip, blobcodec.CodecZstd} {
		t.Run(string(codec), func(t *testing.T) {
			store := newMemStore()
			const chunkPath = "chunks/t/t-0.jsonl.gz"
			putRealChunk(t, store, chunkPath, codec, nil, nil)
			mustWriteManifest(t, store, ManifestFileName, sniffTestFull(chunkPath))

			segs, mans, err := RebuildLineageCatalogAt(context.Background(), store, nil)
			if err != nil {
				t.Fatalf("RebuildLineageCatalogAt: %v", err)
			}
			if segs != 1 || mans != 1 {
				t.Fatalf("rebuild = (%d segments, %d manifests); want (1, 1)", segs, mans)
			}
			cat, ok, err := LoadLineageCatalog(context.Background(), store)
			if err != nil || !ok {
				t.Fatalf("LoadLineageCatalog after rebuild: (%v, %v)", ok, err)
			}
			if got := cat.Segments[0].Codec; got != codec {
				t.Errorf("rebuilt segment codec = %q; want the sniffed %q (N-14: rebuild must not stamp the default)", got, codec)
			}
		})
	}
}

// TestRebuildCatalog_MixedCodecRefused: probes that disagree within one
// chain are a loud refusal, never a guess — recording either codec
// would corrupt the other half's restore.
func TestRebuildCatalog_MixedCodecRefused(t *testing.T) {
	store := newMemStore()
	const fullChunk = "chunks/t/t-0.jsonl.gz"
	putRealChunk(t, store, fullChunk, blobcodec.CodecGzip, nil, nil)
	full := sniffTestFull(fullChunk)
	mustWriteManifest(t, store, ManifestFileName, full)

	const incChunk = "chunks/_changes/changes-0.jsonl.gz"
	putRealChunk(t, store, incChunk, blobcodec.CodecZstd, nil, nil)
	inc := pgIncr(full.BackupID, "0/100", "0/200")
	inc.ChangeChunks = []*irbackup.ChunkInfo{{File: incChunk, RowCount: 1}}
	mustWriteManifest(t, store, IncrementalManifestPrefix+"incr-0001.json", inc)

	_, _, err := RebuildLineageCatalogAt(context.Background(), store, nil)
	if err == nil {
		t.Fatal("RebuildLineageCatalogAt: nil error for a mixed-codec chain; want a loud refusal")
	}
	if !strings.Contains(err.Error(), "disagree") {
		t.Errorf("mixed-codec refusal should name the disagreement; got: %v", err)
	}
}

// TestRebuildCatalog_TruncatedChunkRefused: a chunk too short to carry
// any codec signature refuses the rebuild loudly (corrupt chunk —
// recording a codec over it would be a guess).
func TestRebuildCatalog_TruncatedChunkRefused(t *testing.T) {
	store := newMemStore()
	const chunkPath = "chunks/t/t-0.jsonl.gz"
	if err := store.Put(context.Background(), chunkPath, bytes.NewReader([]byte{0x1F})); err != nil {
		t.Fatalf("store.Put: %v", err)
	}
	mustWriteManifest(t, store, ManifestFileName, sniffTestFull(chunkPath))
	if _, _, err := RebuildLineageCatalogAt(context.Background(), store, nil); err == nil {
		t.Fatal("RebuildLineageCatalogAt: nil error for a truncated chunk; want a loud refusal")
	}
}

// TestRebuildCatalog_NoChunks_DefaultAssumed: a chain with no chunks at
// all has nothing to probe — the documented last-resort default (with a
// WARN naming the assumption; harmless, since restore never opens a
// chunk on such a chain).
func TestRebuildCatalog_NoChunks_DefaultAssumed(t *testing.T) {
	store := newMemStore()
	m := sniffTestFull("unused")
	m.Tables = nil
	mustWriteManifest(t, store, ManifestFileName, m)

	if _, _, err := RebuildLineageCatalogAt(context.Background(), store, nil); err != nil {
		t.Fatalf("RebuildLineageCatalogAt: %v", err)
	}
	cat, ok, err := LoadLineageCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("LoadLineageCatalog: (%v, %v)", ok, err)
	}
	if got := cat.Segments[0].Codec; got != blobcodec.DefaultCodec {
		t.Errorf("chunk-less rebuild codec = %q; want the assumed default %q", got, blobcodec.DefaultCodec)
	}
}

// encryptedSniffFixture writes an encrypted (per-chain, passphrase)
// chain whose chunks were compressed with codec, mirroring the
// production write side: chain CEK wrapped via [WrapChainCEK] (identity
// binding decided by the manifest's FormatVersion) and the chunk
// encrypted under the v5 position-binding AAD. Returns the envelope.
func encryptedSniffFixture(t *testing.T, store irbackup.Store, codec blobcodec.Codec) crypto.EnvelopeEncryption {
	t.Helper()
	params, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	env, err := crypto.NewPassphraseEnvelope("sniff-test-passphrase", params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	cek := make([]byte, crypto.CEKLen)
	if _, err := rand.Read(cek); err != nil {
		t.Fatalf("rand: %v", err)
	}

	const chunkPath = "chunks/t/t-0.jsonl.gz"
	m := sniffTestFull(chunkPath)
	m.Tables[0].Chunks[0].Encryption = &irbackup.ChunkEncryption{
		Algorithm: crypto.AlgorithmAESGCM, NonceLen: crypto.NonceLen, AuthTagLen: crypto.AuthTagLen,
	}
	wrapped, err := WrapChainCEK(env, cek, m)
	if err != nil {
		t.Fatalf("WrapChainCEK: %v", err)
	}
	p := env.Params()
	m.ChainEncryption = &irbackup.ChainEncryption{
		Algorithm:  crypto.AlgorithmAESGCM,
		Mode:       crypto.EncryptModePerChain,
		KEKMode:    crypto.KEKModePassphrase,
		WrappedCEK: wrapped,
		Argon2id: &irbackup.Argon2idParams{
			Salt: p.Salt, Memory: p.Memory, Iterations: p.Iterations,
			Parallelism: p.Parallelism, KeyLen: p.KeyLen,
		},
	}
	putRealChunk(t, store, chunkPath, codec, cek, irbackup.ChunkAAD(m, chunkPath))
	mustWriteManifest(t, store, ManifestFileName, m)
	return env
}

// TestRebuildCatalog_EncryptedWithoutEnvelopeRefused: the codec sits
// INSIDE the encryption envelope, so a rebuild over an encrypted chain
// without key material must refuse (recording a guess is the N-14
// wrong-heal) and steer the operator to --encrypt.
func TestRebuildCatalog_EncryptedWithoutEnvelopeRefused(t *testing.T) {
	store := newMemStore()
	encryptedSniffFixture(t, store, blobcodec.CodecGzip)

	_, _, err := RebuildLineageCatalogAt(context.Background(), store, nil)
	if err == nil {
		t.Fatal("RebuildLineageCatalogAt(encrypted, nil env): nil error; want a loud refusal")
	}
	if !errors.Is(err, ErrCodecSniffEncrypted) {
		t.Errorf("error should wrap ErrCodecSniffEncrypted; got: %v", err)
	}
	if !strings.Contains(err.Error(), "--encrypt") {
		t.Errorf("refusal should steer the operator to --encrypt; got: %v", err)
	}
	if ok, _ := store.Exists(context.Background(), LineageCatalogFileName); ok {
		t.Error("refused rebuild must not write lineage.json")
	}
}

// TestRebuildCatalog_EncryptedWithEnvelopeSniffs: with the chain's key
// material supplied, the rebuild decrypts one chunk and records the
// TRUE codec (gzip here — deliberately not the default, so a
// default-stamping regression fails this test).
func TestRebuildCatalog_EncryptedWithEnvelopeSniffs(t *testing.T) {
	store := newMemStore()
	env := encryptedSniffFixture(t, store, blobcodec.CodecGzip)

	if _, _, err := RebuildLineageCatalogAt(context.Background(), store, env); err != nil {
		t.Fatalf("RebuildLineageCatalogAt(encrypted, env): %v", err)
	}
	cat, ok, err := LoadLineageCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("LoadLineageCatalog: (%v, %v)", ok, err)
	}
	if got := cat.Segments[0].Codec; got != blobcodec.CodecGzip {
		t.Errorf("encrypted rebuild codec = %q; want the decrypt-sniffed %q", got, blobcodec.CodecGzip)
	}
}

// TestResolveLineage_SyntheticRoot_SniffsCodec_EveryFamily: the
// lineage.json-ABSENT synthetic root gets the same treatment — its
// in-memory codec is sniffed, so restore / verify / chain-walk over a
// legacy gzip/none chain decode correctly without any rebuild step.
func TestResolveLineage_SyntheticRoot_SniffsCodec_EveryFamily(t *testing.T) {
	for _, codec := range []blobcodec.Codec{blobcodec.CodecNone, blobcodec.CodecGzip, blobcodec.CodecZstd} {
		t.Run(string(codec), func(t *testing.T) {
			store := newMemStore()
			const chunkPath = "chunks/t/t-0.jsonl.gz"
			putRealChunk(t, store, chunkPath, codec, nil, nil)
			mustWriteManifest(t, store, ManifestFileName, sniffTestFull(chunkPath))

			cat, err := ResolveLineage(context.Background(), store)
			if err != nil {
				t.Fatalf("ResolveLineage: %v", err)
			}
			if got := cat.Segments[0].CodecOrDefault(); got != codec {
				t.Errorf("synthetic root codec = %q; want the sniffed %q", got, codec)
			}
		})
	}
}

// TestResolveLineage_SyntheticRoot_IncrementalChunksProbedWhenFullEmpty:
// a chunk-less full (empty tables) falls through to the incrementals'
// change chunks, in chain order.
func TestResolveLineage_SyntheticRoot_IncrementalChunksProbedWhenFullEmpty(t *testing.T) {
	store := newMemStore()
	full := sniffTestFull("unused")
	full.Tables = nil
	mustWriteManifest(t, store, ManifestFileName, full)

	const incChunk = "chunks/_changes/changes-0.jsonl.gz"
	putRealChunk(t, store, incChunk, blobcodec.CodecGzip, nil, nil)
	inc := pgIncr(full.BackupID, "0/100", "0/200")
	inc.ChangeChunks = []*irbackup.ChunkInfo{{File: incChunk, RowCount: 1}}
	mustWriteManifest(t, store, IncrementalManifestPrefix+"incr-0001.json", inc)

	cat, err := ResolveLineage(context.Background(), store)
	if err != nil {
		t.Fatalf("ResolveLineage: %v", err)
	}
	if got := cat.Segments[0].CodecOrDefault(); got != blobcodec.CodecGzip {
		t.Errorf("synthetic root codec = %q; want gzip sniffed from the incremental's change chunk", got)
	}
}

// TestResolveLineage_SyntheticRoot_EncryptedAssumesDefault pins the
// synthetic path's ENCRYPTED posture: this layer has no key material,
// so it degrades to the write default with a WARN (an in-memory
// assumption — a wrong one still fails loudly at the first chunk
// decode, and the strict rebuild-with---encrypt path records the
// truth). It must NOT hard-fail: that would break every keyless flow
// (e.g. SHA-only `backup verify`) over encrypted no-lineage chains that
// works today.
func TestResolveLineage_SyntheticRoot_EncryptedAssumesDefault(t *testing.T) {
	store := newMemStore()
	encryptedSniffFixture(t, store, blobcodec.CodecGzip)

	cat, err := ResolveLineage(context.Background(), store)
	if err != nil {
		t.Fatalf("ResolveLineage(encrypted, no key material): %v; want WARN + default-codec assumption, not failure", err)
	}
	if got := cat.Segments[0].CodecOrDefault(); got != blobcodec.DefaultCodec {
		t.Errorf("synthetic root codec = %q; want the assumed default %q (no key material to decrypt-sniff)", got, blobcodec.DefaultCodec)
	}
}
