// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// The gate for `backup verify --depth read` (roadmap item 129).
//
// The property under test is a PAIR, and only the pair is meaningful:
//
//   - the hash-only depth stays GREEN on every hostile fixture here (that is
//     the defect — the bytes really are intact, and re-hashing them really
//     does prove nothing about readability); and
//   - the read depth REFUSES each one, coded, naming the chunk.
//
// Asserting only the second half would pass on a verify that had simply become
// stricter; asserting only the first would pass on a verify that does nothing.
//
// # How the hostile fixtures are built, and why not with the writer
//
// The acceptance case is an artifact written by a PRE-item-128 binary: a chunk
// carrying a row line longer than [blobcodec.MaxChunkLineBytes]. The current
// writers refuse exactly that row, on all three write cores — which is item
// 128's fix and is why the fixture cannot be built with them. Building it with
// the current writer is the item-104 trap in its purest form: a fixture made of
// post-change values proves the binary agrees with itself.
//
// So the hostile chunks are assembled at the BYTE level, and the split is
// deliberate about how much is hand-made:
//
//   - the header line and every ordinary record line come from the REAL
//     writers, so the wire format is never guessed;
//   - the over-long line is one of those real lines with a marker inflated
//     past the limit — a genuine record whose only defect is length, which is
//     precisely what a pre-item-128 writer emitted;
//   - the gzip framing is stdlib `compress/gzip`, NOT the codec under test
//     (writer-verifying-writer passes symmetric bugs);
//   - the GCM seal uses [crypto.EncryptChunkWithAAD] with the same CEK and AAD
//     the chain advertises, so the ciphertext is honest and only the plaintext
//     inside it is unreadable;
//   - the manifest's recorded SHA-256 is always stamped over the FINAL on-disk
//     bytes, so the hash depth has nothing to complain about.
//
// # Coverage
//
// {data chunk, change chunk} × {plaintext, encrypted} × {healthy, over-long
// line, truncated stream}. Both chunk KINDS because item 128's first cut closed
// two of three write cores and the third — the change-chunk lane — was found a
// day later; the read side covers both from the outset. Both encryption modes
// because the encrypted lane decrypts up-front into a different buffer and
// reaches the scanner by a different route.

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// readDepthShape is how a fixture's chunk bytes are produced.
type readDepthShape int

const (
	shapeHealthy readDepthShape = iota
	shapeOverlongLine
	shapeTruncatedStream
)

func (s readDepthShape) String() string {
	switch s {
	case shapeOverlongLine:
		return "over-long line"
	case shapeTruncatedStream:
		return "truncated stream"
	default:
		return "healthy"
	}
}

// readDepthFiller is the >64 MiB filler, built once for the whole package —
// four fixtures need it and it is 64 MiB apiece.
var (
	readDepthFillerOnce sync.Once
	readDepthFillerVal  string
)

func readDepthFiller() string {
	readDepthFillerOnce.Do(func() {
		readDepthFillerVal = strings.Repeat("x", blobcodec.MaxChunkLineBytes)
	})
	return readDepthFillerVal
}

// inflateLine replaces marker inside a real writer-produced record line with
// enough filler to push the line past [blobcodec.MaxChunkLineBytes]. The record
// stays genuine — only its LENGTH is the defect, which is exactly the artifact
// a writer without a length refusal produced.
func inflateLine(t *testing.T, line []byte, marker string) []byte {
	t.Helper()
	if !bytes.Contains(line, []byte(marker)) {
		t.Fatalf("marker %q absent from the writer's line — the fixture no longer inflates anything, "+
			"which would make every assertion below vacuous.\nline: %s", marker, truncateForLog(line))
	}
	out := bytes.Replace(line, []byte(marker), []byte(readDepthFiller()), 1)
	if len(out) <= blobcodec.MaxChunkLineBytes {
		t.Fatalf("inflated line is %d bytes, not over the %d-byte limit", len(out), blobcodec.MaxChunkLineBytes)
	}
	return out
}

func truncateForLog(b []byte) string {
	if len(b) > 200 {
		return string(b[:200]) + "…"
	}
	return string(b)
}

// splitChunkLines splits a plaintext, uncompressed chunk stream into its header
// line and its record lines.
func splitChunkLines(t *testing.T, raw []byte) (header []byte, records [][]byte) {
	t.Helper()
	lines := bytes.Split(bytes.TrimSuffix(raw, []byte("\n")), []byte("\n"))
	if len(lines) < 2 {
		t.Fatalf("chunk stream has %d line(s); want a header plus at least one record", len(lines))
	}
	return lines[0], lines[1:]
}

// gzipAndSeal reproduces the on-disk layout the chunk writers produce —
// compress, then (when cek is set) AES-GCM-seal under the chain's key and
// binding — using stdlib gzip rather than the codec under test.
func gzipAndSeal(t *testing.T, payload, cek, aad []byte, truncate bool) []byte {
	t.Helper()
	var gzBuf bytes.Buffer
	zw := gzip.NewWriter(&gzBuf)
	if _, err := zw.Write(payload); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	body := gzBuf.Bytes()
	if truncate {
		// Chop the tail of the COMPRESSED stream. Whether the cut lands in the
		// deflate data or only in the trailer, the reader hits an error before
		// it can honestly report end-of-stream — and the manifest SHA below is
		// stamped over the result, so the hash depth sees nothing wrong.
		if len(body) < 32 {
			t.Fatalf("compressed body is %d bytes; too small to truncate meaningfully", len(body))
		}
		body = body[:len(body)-16]
	}
	if cek == nil {
		return body
	}
	ct, err := crypto.EncryptChunkWithAAD(body, cek, aad)
	if err != nil {
		t.Fatalf("EncryptChunkWithAAD: %v", err)
	}
	return ct
}

const readDepthMarker = "INFLATE-ME"

// rowChunkBytes builds a DATA chunk's on-disk bytes for the given shape.
func rowChunkBytes(t *testing.T, shape readDepthShape, cek, aad []byte) []byte {
	t.Helper()
	cols := []string{"id", "body"}
	schemaCols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "body", Type: ir.Text{}},
	}
	rows := []ir.Row{
		{"id": int64(1), "body": "first"},
		{"id": int64(2), "body": readDepthMarker},
	}
	if shape == shapeHealthy {
		// The control comes off the real writer end to end — same codec, same
		// key, same binding a `backup full` would use.
		var buf bytes.Buffer
		w, err := blobcodec.NewChunkWriter(&buf, cols, cek, blobcodec.CodecGzip, aad)
		if err != nil {
			t.Fatalf("NewChunkWriter: %v", err)
		}
		for _, r := range rows {
			if err := w.WriteRow(r, schemaCols); err != nil {
				t.Fatalf("WriteRow: %v", err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("chunk writer Close: %v", err)
		}
		return buf.Bytes()
	}

	// Hostile: take the real writer's PLAINTEXT, UNCOMPRESSED stream apart and
	// reassemble it with the defect. CodecNone + no cek means the bytes are the
	// JSON-Lines stream verbatim.
	var raw bytes.Buffer
	w, err := blobcodec.NewChunkWriter(&raw, cols, nil, blobcodec.CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChunkWriter(raw): %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, schemaCols); err != nil {
			t.Fatalf("WriteRow(raw): %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("chunk writer Close(raw): %v", err)
	}
	header, records := splitChunkLines(t, raw.Bytes())

	var payload bytes.Buffer
	payload.Write(header)
	payload.WriteByte('\n')
	for i, rec := range records {
		if shape == shapeOverlongLine && i == len(records)-1 {
			rec = inflateLine(t, rec, readDepthMarker)
		}
		payload.Write(rec)
		payload.WriteByte('\n')
	}
	return gzipAndSeal(t, payload.Bytes(), cek, aad, shape == shapeTruncatedStream)
}

// changeChunkBytes is the change-chunk twin of [rowChunkBytes].
func changeChunkBytes(t *testing.T, shape readDepthShape, cek, aad []byte) []byte {
	t.Helper()
	changes := []ir.Change{
		ir.Insert{Schema: "", Table: "t", Row: ir.Row{"id": int64(1), "body": "first"}, Position: pos(10)},
		ir.Insert{Schema: "", Table: "t", Row: ir.Row{"id": int64(2), "body": readDepthMarker}, Position: pos(11)},
	}
	if shape == shapeHealthy {
		var buf bytes.Buffer
		w, err := blobcodec.NewChangeChunkWriter(&buf, cek, blobcodec.CodecGzip, aad)
		if err != nil {
			t.Fatalf("NewChangeChunkWriter: %v", err)
		}
		for _, c := range changes {
			if err := w.WriteChange(c); err != nil {
				t.Fatalf("WriteChange: %v", err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("change chunk writer Close: %v", err)
		}
		return buf.Bytes()
	}

	var raw bytes.Buffer
	w, err := blobcodec.NewChangeChunkWriter(&raw, nil, blobcodec.CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkWriter(raw): %v", err)
	}
	for _, c := range changes {
		if err := w.WriteChange(c); err != nil {
			t.Fatalf("WriteChange(raw): %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("change chunk writer Close(raw): %v", err)
	}
	header, records := splitChunkLines(t, raw.Bytes())

	var payload bytes.Buffer
	payload.Write(header)
	payload.WriteByte('\n')
	for i, rec := range records {
		if shape == shapeOverlongLine && i == len(records)-1 {
			rec = inflateLine(t, rec, readDepthMarker)
		}
		payload.Write(rec)
		payload.WriteByte('\n')
	}
	return gzipAndSeal(t, payload.Bytes(), cek, aad, shape == shapeTruncatedStream)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// seedReadDepthChain builds a one-segment chain: a full manifest carrying one
// DATA chunk, and an incremental carrying one CHANGE chunk, so both lanes of
// [verifyBackupScan] are walked in one pass.
func seedReadDepthChain(t *testing.T, encrypted bool, dataShape, changeShape readDepthShape) (irbackup.Store, crypto.EnvelopeEncryption) {
	t.Helper()
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	base := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
	}}}

	var (
		env crypto.EnvelopeEncryption
		cek []byte
	)
	if encrypted {
		env = verifyProbeEnvelope(t)
		cek, err = crypto.GenerateCEK()
		if err != nil {
			t.Fatalf("GenerateCEK: %v", err)
		}
	}

	const (
		dataFile   = "chunks/t/t-0.jsonl.gz"
		changeFile = "chunks/_changes/incr-0.jsonl.gz"
		incrPath   = "manifests/incr-00001.json"
	)

	dataChunk := &irbackup.ChunkInfo{File: dataFile, RowCount: 2}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     base,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   pos(10),
		PartialState:  irbackup.BackupStateComplete,
		Schema:        schema,
		Tables:        []*irbackup.TableManifest{{Name: "t", RowCount: 2, Chunks: []*irbackup.ChunkInfo{dataChunk}}},
	}
	if encrypted {
		dataChunk.Encryption = &irbackup.ChunkEncryption{
			Algorithm: crypto.AlgorithmAESGCM, NonceLen: crypto.NonceLen, AuthTagLen: crypto.AuthTagLen,
		}
		full.BackupID = irbackup.ComputeBackupID(full)
		wrapped, werr := lineage.WrapChainCEK(env, cek, full)
		if werr != nil {
			t.Fatalf("WrapChainCEK: %v", werr)
		}
		full.ChainEncryption = &irbackup.ChainEncryption{
			Algorithm:  crypto.AlgorithmAESGCM,
			KEKMode:    env.Mode(),
			Mode:       crypto.EncryptModePerChain,
			WrappedCEK: wrapped,
		}
	}
	full.BackupID = irbackup.ComputeBackupID(full)

	dataBytes := rowChunkBytes(t, dataShape, cek, irbackup.ChunkAADFor(full, dataChunk, "", "t"))
	dataChunk.SHA256 = sha256Hex(dataBytes)
	if err := store.Put(ctx, dataFile, bytes.NewReader(dataBytes)); err != nil {
		t.Fatalf("put data chunk: %v", err)
	}
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full manifest: %v", err)
	}

	changeChunk := &irbackup.ChunkInfo{File: changeFile, RowCount: 2}
	incr := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     base.Add(10 * time.Minute),
		Kind:          irbackup.BackupKindIncremental,
		StartPosition: pos(10),
		EndPosition:   pos(12),
		PartialState:  irbackup.BackupStateComplete,
		Schema:        schema,
		ChangeChunks:  []*irbackup.ChunkInfo{changeChunk},
	}
	if encrypted {
		changeChunk.Encryption = &irbackup.ChunkEncryption{
			Algorithm: crypto.AlgorithmAESGCM, NonceLen: crypto.NonceLen, AuthTagLen: crypto.AuthTagLen,
		}
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)

	changeBytes := changeChunkBytes(t, changeShape, cek, irbackup.ChangeChunkAADFor(incr, changeChunk, 0))
	changeChunk.SHA256 = sha256Hex(changeBytes)
	if err := store.Put(ctx, changeFile, bytes.NewReader(changeBytes)); err != nil {
		t.Fatalf("put change chunk: %v", err)
	}
	if err := lineage.WriteManifestAt(ctx, store, incrPath, incr); err != nil {
		t.Fatalf("write incremental manifest: %v", err)
	}

	cat := &lineage.Catalog{
		FormatVersion: 1,
		SourceEngine:  "postgres",
		CreatedAt:     base,
		UpdatedAt:     base,
		Segments: []lineage.Segment{{
			SegmentID:        full.BackupID,
			Dir:              "",
			FullManifestPath: lineage.ManifestFileName,
			Incrementals:     []string{incrPath},
			StartPosition:    pos(10),
			EndPosition:      pos(12),
			Codec:            blobcodec.CodecGzip,
		}},
	}
	if err := lineage.WriteLineageCatalog(ctx, store, cat); err != nil {
		t.Fatalf("write lineage catalog: %v", err)
	}
	return store, env
}

// TestVerifyDepthRead_HashDepthBlessesWhatTheReadDepthRefuses is the pair.
//
// The first half is the DEFECT, asserted as a property rather than described in
// a comment: on every hostile fixture the hash-only depth returns rc=0. That is
// not a bug in the hash depth — the bytes really are intact — it is the reason
// a second depth had to exist.
func TestVerifyDepthRead_HashDepthBlessesWhatTheReadDepthRefuses(t *testing.T) {
	ctx := context.Background()
	for _, encrypted := range []bool{false, true} {
		for _, lane := range []struct {
			name              string
			data, change      readDepthShape
			wantFailingChunks int
		}{
			{"data chunk", shapeOverlongLine, shapeHealthy, 1},
			{"change chunk", shapeHealthy, shapeOverlongLine, 1},
			{"data chunk truncated", shapeTruncatedStream, shapeHealthy, 1},
			{"change chunk truncated", shapeHealthy, shapeTruncatedStream, 1},
		} {
			mode := "plaintext"
			if encrypted {
				mode = "encrypted"
			}
			t.Run(mode+"/"+lane.name, func(t *testing.T) {
				store, env := seedReadDepthChain(t, encrypted, lane.data, lane.change)

				// Half one: the hash depth — with key material when the chain
				// has any, i.e. the STRONGEST thing verify could do before this
				// item — reports the chain healthy.
				rep, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{Envelope: env})
				if err != nil {
					t.Fatalf("the hash depth refused a byte-intact chain: %v\n\n"+
						"This fixture is supposed to hash perfectly — if it does not, the read-depth "+
						"assertion below proves nothing, because a hash failure would explain it.", err)
				}
				if rep.Chunks != 2 {
					t.Fatalf("walked %d chunks; want 2 (one data, one change) — the fixture is not exercising both lanes", rep.Chunks)
				}
				if encrypted && rep.Authenticated != 2 {
					t.Fatalf("authenticated %d of 2 chunks at the hash depth; the fixture's ciphertext is not honest", rep.Authenticated)
				}

				// Half two: the read depth refuses, coded, on the same store.
				rep, err = VerifyBackupCodedReport(ctx, store, VerifyOptions{Envelope: env, Depth: VerifyDepthRead})
				if err == nil {
					t.Fatal("the READ depth reported the chain healthy.\n\n" +
						"This chunk's bytes are intact and its own reader cannot read it — `restore` " +
						"fails on it. A verify that cannot see that is Bug 226 (roadmap item 129).")
				}
				ce, ok := sluicecode.FromError(err)
				if !ok || ce.Code != sluicecode.CodeBackupChunkUnreadable {
					t.Fatalf("refusal is not coded %s: %v", sluicecode.CodeBackupChunkUnreadable, err)
				}
				if rep.Failed != lane.wantFailingChunks {
					t.Errorf("Failed = %d; want %d", rep.Failed, lane.wantFailingChunks)
				}
			})
		}
	}
}

// The control, and the half that keeps the refusal from being a regression: a
// HEALTHY chain must pass BOTH depths, in both encryption modes, with the
// `decrypted=N` coverage signal intact at the read depth too (a depth that
// stopped opening ciphertext while still reporting a count would be Bug 215
// again, one depth over).
func TestVerifyDepthRead_HealthyChainPassesBothDepths(t *testing.T) {
	ctx := context.Background()
	for _, encrypted := range []bool{false, true} {
		mode := "plaintext"
		if encrypted {
			mode = "encrypted"
		}
		t.Run(mode, func(t *testing.T) {
			store, env := seedReadDepthChain(t, encrypted, shapeHealthy, shapeHealthy)
			for _, depth := range []VerifyDepth{VerifyDepthHash, VerifyDepthRead} {
				rep, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{Envelope: env, Depth: depth})
				if err != nil {
					t.Fatalf("depth %q refused a healthy chain: %v", depth, err)
				}
				if rep.Chunks != 2 {
					t.Fatalf("depth %q walked %d chunks; want 2", depth, rep.Chunks)
				}
				wantAuth := 0
				if encrypted {
					wantAuth = 2
				}
				if rep.Authenticated != wantAuth {
					t.Errorf("depth %q authenticated %d chunks; want %d — the coverage signal must stay honest at every depth",
						depth, rep.Authenticated, wantAuth)
				}
			}
		})
	}
}

// The layer-2 row-count refusal, at the read depth. This is a chunk whose
// bytes are intact, whose reader decodes every row happily, and whose manifest
// entry claims a different number — a shape `restore` refuses and the hash
// depth cannot see, because counting decoded rows requires decoding them.
//
// Scope note, stated because the asymmetry is deliberate: this reaches ROW
// chunks only. Chain restore does not compare a CHANGE chunk's decoded count
// against its recorded RowCount, and verify must never refuse a chain restore
// would accept — see [checkChunkRowCount].
func TestVerifyDepthRead_LayerTwoRowCountMismatch(t *testing.T) {
	ctx := context.Background()
	store, env := seedReadDepthChain(t, false, shapeHealthy, shapeHealthy)

	full, err := lineage.ReadManifestAt(ctx, store, lineage.ManifestFileName)
	if err != nil {
		t.Fatalf("ReadManifestAt: %v", err)
	}
	full.Tables[0].Chunks[0].RowCount = 5 // the chunk really holds 2
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("WriteManifestAt: %v", err)
	}

	if _, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{Envelope: env}); err != nil {
		t.Fatalf("the hash depth refused: %v (the bytes are untouched; only the manifest's count changed)", err)
	}
	_, err = VerifyBackupCodedReport(ctx, store, VerifyOptions{Envelope: env, Depth: VerifyDepthRead})
	if err == nil {
		t.Fatal("the read depth accepted a chunk whose decoded row count disagrees with the manifest — restore refuses this")
	}
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupIncomplete {
		t.Fatalf("refusal is not coded %s: %v", sluicecode.CodeBackupIncomplete, err)
	}
}

// `--depth read` on an encrypted chain with NO key material must name the
// missing input, not blame the artifact.
//
// Without the guard the depth hands ciphertext to the gzip reader and reports
// every chunk SLUICE-E-BACKUP-CHUNK-UNREADABLE — true of the run and a badly
// wrong diagnosis, on a chain that is perfectly healthy. The hash depth's
// existing graceful degradation (WARN + sha256-only) must be untouched: an
// unkeyed cron probe is exactly what that depth is for.
func TestVerifyDepthRead_EncryptedChainWithoutAKeyNamesTheMissingKey(t *testing.T) {
	ctx := context.Background()
	store, _ := seedReadDepthChain(t, true, shapeHealthy, shapeHealthy)

	if _, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{}); err != nil {
		t.Fatalf("the hash depth must still run key-less on an encrypted chain: %v", err)
	}

	_, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{Depth: VerifyDepthRead})
	if err == nil {
		t.Fatal("the read depth reported an encrypted chain green without ever decrypting anything")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupEncryptionMismatch {
		t.Fatalf("refusal is not coded %s — an operator reading %v would go looking for a corrupt backup "+
			"instead of for their passphrase", sluicecode.CodeBackupEncryptionMismatch, err)
	}
	if !strings.Contains(err.Error(), "--depth read") {
		t.Errorf("refusal does not name the flag that needs the key: %v", err)
	}
}

// The per-chunk backstop for the same thing: a chunk that is encrypted while
// its chain does not say so (a pre-ChainEncryption artifact) never reaches the
// chain-level preflight above.
func TestChunkReadTarget_RequireKey(t *testing.T) {
	plain := chunkReadTarget{chunk: &irbackup.ChunkInfo{File: "chunks/t/t-0.jsonl.gz"}}
	if err := plain.requireKey(); err != nil {
		t.Fatalf("a plaintext chunk needs no key: %v", err)
	}
	sealed := chunkReadTarget{chunk: &irbackup.ChunkInfo{
		File:       "chunks/t/t-0.jsonl.gz",
		Encryption: &irbackup.ChunkEncryption{Algorithm: crypto.AlgorithmAESGCM},
	}}
	err := sealed.requireKey()
	if err == nil {
		t.Fatal("an encrypted chunk with no CEK was accepted; the read would report it unreadable")
	}
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupEncryptionMismatch {
		t.Errorf("refusal is not coded %s: %v", sluicecode.CodeBackupEncryptionMismatch, err)
	}
	sealed.cek = make([]byte, crypto.CEKLen)
	if err := sealed.requireKey(); err != nil {
		t.Fatalf("an encrypted chunk WITH a cek was refused: %v", err)
	}
}

// The read depth must not be reachable by accident. A caller that never sets
// Depth gets the historical byte-level behaviour, because the CLI is the only
// thing that sets the field and every other construction gets the zero value
// (the v0.99.51 zero-value trap, and the reason the zero value is "hash").
func TestVerifyDepth_ZeroValueIsHashOnly(t *testing.T) {
	if VerifyDepth("").readsRows() {
		t.Fatal("the zero-value VerifyDepth reads rows — every non-CLI caller would silently pay a full read per chunk")
	}
	if !VerifyDepthRead.readsRows() {
		t.Fatal("VerifyDepthRead does not read rows — the depth is inert")
	}
	if VerifyDepthHash.readsRows() {
		t.Fatal("VerifyDepthHash reads rows")
	}
}

func TestParseVerifyDepth(t *testing.T) {
	for in, want := range map[string]VerifyDepth{
		"":     VerifyDepthHash,
		"hash": VerifyDepthHash,
		"read": VerifyDepthRead,
	} {
		got, err := ParseVerifyDepth(in)
		if err != nil {
			t.Fatalf("ParseVerifyDepth(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("ParseVerifyDepth(%q) = %q; want %q", in, got, want)
		}
	}
	for _, bad := range []string{"full", "count", "sample", "HASH", "rows"} {
		if _, err := ParseVerifyDepth(bad); err == nil {
			t.Errorf("ParseVerifyDepth(%q) accepted an unknown depth", bad)
		}
	}
}

// checkChunkRowCount is SHARED with [Restore.streamChunkRows] deliberately —
// the read depth must refuse exactly what restore refuses and nothing more.
// This pins the predicate itself so the two callers cannot drift the way the
// manifest-preflight LISTS did (Bug 217/218).
func TestCheckChunkRowCount_SharedPredicate(t *testing.T) {
	chunk := &irbackup.ChunkInfo{File: "chunks/t/t-0.jsonl.gz", RowCount: 3}
	if err := checkChunkRowCount(chunk, 3); err != nil {
		t.Fatalf("exact match refused: %v", err)
	}
	for _, decoded := range []int64{0, 2, 4} {
		err := checkChunkRowCount(chunk, decoded)
		if err == nil {
			t.Fatalf("decoded %d against a recorded 3 was accepted", decoded)
		}
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupIncomplete {
			t.Errorf("decoded %d: refusal is not coded %s: %v", decoded, sluicecode.CodeBackupIncomplete, err)
		}
	}
	// A recorded 0 is the zeroed-RowCount tamper only when rows decode; an
	// empty chunk with a recorded 0 is legitimate (compaction leaves them).
	zeroed := &irbackup.ChunkInfo{File: "chunks/t/t-1.jsonl.gz", RowCount: 0}
	if err := checkChunkRowCount(zeroed, 0); err != nil {
		t.Fatalf("an empty chunk recording 0 rows was refused: %v", err)
	}
	if err := checkChunkRowCount(zeroed, 1); err == nil {
		t.Fatal("a chunk recording 0 rows that decoded 1 was accepted")
	}
}

// verifyChunkKind is both the log label and the dispatch key; a String() that
// drifted from the historical strings would silently break log scraping, and a
// dispatch that treated a change chunk as a row chunk would read it with the
// wrong reader.
func TestVerifyChunkKind_LabelsAreStable(t *testing.T) {
	if got := verifyRowChunk.String(); got != "row chunk" {
		t.Errorf("verifyRowChunk label = %q; want %q", got, "row chunk")
	}
	if got := verifyChangeChunk.String(); got != "change chunk" {
		t.Errorf("verifyChangeChunk label = %q; want %q", got, "change chunk")
	}
	if verifyRowChunk == verifyChangeChunk {
		t.Fatal("the two chunk kinds compare equal — dispatch would send change chunks to the row reader")
	}
}

// A sanity floor on the fixture builder itself: the over-long line must
// actually be over the limit, and the healthy line must not be. Without it a
// filler that silently shrank would turn every refusal assertion above into a
// test of something else.
func TestReadDepthFixture_InflatedLineIsGenuinelyOverTheLimit(t *testing.T) {
	line := []byte(fmt.Sprintf(`{"id":2,"body":%q}`, readDepthMarker))
	if len(line) > blobcodec.MaxChunkLineBytes {
		t.Fatal("the uninflated control line is already over the limit")
	}
	if got := len(inflateLine(t, line, readDepthMarker)); got <= blobcodec.MaxChunkLineBytes {
		t.Fatalf("inflated line is %d bytes; want > %d", got, blobcodec.MaxChunkLineBytes)
	}
}
