// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package blobcodec is the wire-format + storage leaf of the logical-backup
// stack: the on-disk chunk codec (row chunks and change-event chunks — gzip/
// zstd-compressed JSON Lines with per-chunk SHA-256 verification and optional
// encryption), the per-segment compression [Codec], and the [irbackup.Store]
// backends ([LocalStore] and the cloud [BlobStore]). It depends only on the
// IR backup contract and knows nothing about orchestration; the backup /
// restore / broker / chain code in the parent pipeline package consumes it.
package blobcodec

// Chunk-file format for Phase 1 logical backups.
//
// Each chunk is a gzip-compressed JSON Lines stream: one row per line,
// each line a JSON object whose keys are column names and values are
// tagged-union envelopes that round-trip Go-native types unambiguously.
//
// The tagged-value envelope is `{"_t":"<kind>","v":<payload>}`. Kinds
// that JSON natively round-trips (string, bool, JSON null, integers
// that fit in float64, lists/maps of those) use the bare JSON value
// directly — no envelope. Kinds that don't round-trip cleanly through
// `encoding/json` (`[]byte`, `time.Time`, integer widths the operator
// relies on, []any with mixed types) wear the envelope:
//
//   - `{"_t":"bytes","v":"<base64>"}` for `[]byte`.
//   - `{"_t":"time","v":"<RFC3339Nano>"}` for `time.Time`.
//   - `{"_t":"i64","v":<number>}` for explicit int64 (so a value
//     declared `int64` doesn't lose its type to JSON's float64).
//   - `{"_t":"u64","v":"<decimal-string>"}` for uint64 (string-encoded
//     to avoid precision loss above 2^53).
//   - `{"_t":"f64","v":<number>}` for explicit float64.
//   - `{"_t":"f64s","v":"NaN"|"+Inf"|"-Inf"}` for non-finite floats
//     (IEEE specials JSON cannot carry as numbers — Bug 138; PG
//     float4/float8 columns hold them legally and `migrate` carries
//     them, so backup must too). ±Inf round-trips sign-exact; NaN
//     payload bits are not representable in the sentinel, so every
//     NaN decodes to the IEEE-canonical quiet NaN — the same
//     canonicalization PG's own text format performs. Additive tag:
//     binaries older than the tag refuse a chunk containing one
//     LOUDLY ("unknown value tag"), never silently.
//
// Why JSON Lines + gzip rather than gob: JSON Lines is debuggable
// (`zcat users-0.jsonl.gz | head -3 | jq .`), engine-portable (Phase 2
// could read these from non-Go tools), and forward-compat (new tag
// kinds can be added without a format-version bump). Gzip is stdlib,
// good enough for Phase 1; Phase 2 may swap to zstd if benchmarks show
// it matters.
//
// Header line: every chunk file starts with a single line containing
// `{"_h":1,"columns":["a","b","c"]}` — the format-version + the column
// list in declaration order. This serves two purposes: (1) the reader
// can sanity-check it's reading a sluice chunk before parsing rows;
// (2) the column list pins the schema the chunk was written against,
// so a column-rename across schema versions surfaces as a header
// mismatch on restore rather than silent data loss.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"math"
	"strconv"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// chunkHeaderVersion is the chunk-file format version embedded in
// every chunk's first JSON line. Bumped only on a non-additive change
// to the chunk format (new tag kinds are additive).
const chunkHeaderVersion = 1

// chunkHeader is the JSON shape of a chunk file's first line.
type chunkHeader struct {
	Version int      `json:"_h"`
	Columns []string `json:"columns"`
}

// ChunkWriter streams JSON-Lines rows to a gzip-compressed [io.Writer]
// while computing a SHA-256 over the bytes that land on disk (so
// restore-time verification matches what's actually on disk / in
// object storage).
//
// Two modes:
//
//   - Plaintext (cek == nil): rows → gzip → tee(out + sha256). The
//     bytes flow through directly; Close flushes gzip and the hash
//     covers exactly what's in `out`.
//   - Encrypted (cek != nil): rows → gzip → internal buffer. On
//     Close, the buffered gzipped bytes are encrypted via
//     [crypto.EncryptChunk] (AES-256-GCM with a fresh random nonce),
//     and the ciphertext is what's hashed AND written to `out`. The
//     manifest's recorded SHA-256 covers ciphertext bytes — `backup
//     verify` (sha256-only) doesn't need decryption.
//
// Lifecycle: NewChunkWriter → WriteRow* → Close. Close MUST be called
// (it flushes the gzip buffer and, in encrypted mode, performs the
// encryption + write). Hash() returns the final hex SHA-256 only
// after Close.
type ChunkWriter struct {
	out      io.Writer
	hasher   hash.Hash
	gzWriter codecWriteCloser
	bufW     *bufio.Writer
	rowCount int64
	closed   bool

	// cek, when non-nil, enables encrypted mode. It's the Content
	// Encryption Key handed in by the orchestrator; ChunkWriter is
	// not responsible for generating or wrapping it.
	cek []byte

	// gzBuf is the in-memory buffer used in encrypted mode. The gzip
	// writer writes here instead of `out` directly; on Close the
	// bytes get encrypted and pushed to `out`.
	gzBuf *bytes.Buffer

	// Fast-encoder state (see backup_chunk_fast.go). encBuf is the
	// reused per-row scratch buffer; sortedNames caches the stdlib
	// map-key order for the columns slice identified by colsPtr/
	// colsLen (stable per table in production — the identity check
	// keeps WriteRow correct if a caller ever varies it).
	encBuf      []byte
	sortedNames []string
	colsPtr     *ir.Column
	colsLen     int
}

// NewChunkWriter wraps out (the destination — typically a pipe to
// [irbackup.Store.Put]) with gzip + JSON-Lines machinery and writes
// the format header. Caller must call Close to flush.
//
// When cek is non-nil, encryption is applied at Close time (see
// [ChunkWriter] for the two modes). cek must be exactly
// [crypto.CEKLen] bytes.
func NewChunkWriter(out io.Writer, columns []string, cek []byte, codec Codec) (*ChunkWriter, error) {
	if cek != nil && len(cek) != crypto.CEKLen {
		return nil, fmt.Errorf("chunk writer: cek length %d != %d", len(cek), crypto.CEKLen)
	}
	hasher := sha256.New()
	var (
		gzDst io.Writer
		gzBuf *bytes.Buffer
	)
	if cek == nil {
		// Plaintext: codec writes directly to out + hasher via a tee.
		gzDst = io.MultiWriter(out, hasher)
	} else {
		// Encrypted: codec writes into an in-memory buffer; Close()
		// encrypts the buffered bytes and pushes them to out + hasher.
		gzBuf = &bytes.Buffer{}
		gzDst = gzBuf
	}
	gz, err := newCodecWriter(gzDst, codec)
	if err != nil {
		return nil, fmt.Errorf("chunk writer codec: %w", err)
	}
	bw := bufio.NewWriter(gz)

	// Header line.
	hdr := chunkHeader{Version: chunkHeaderVersion, Columns: columns}
	hb, err := json.Marshal(hdr)
	if err != nil {
		return nil, fmt.Errorf("chunk header marshal: %w", err)
	}
	if _, err := bw.Write(hb); err != nil {
		return nil, fmt.Errorf("chunk header write: %w", err)
	}
	if err := bw.WriteByte('\n'); err != nil {
		return nil, fmt.Errorf("chunk header newline: %w", err)
	}
	return &ChunkWriter{
		out:      out,
		hasher:   hasher,
		gzWriter: gz,
		bufW:     bw,
		cek:      cek,
		gzBuf:    gzBuf,
	}, nil
}

// WriteRow encodes row using the column order pinned at construction
// and emits one JSON Lines record. Returns the cumulative row count.
//
// The encode runs on the fast append-based path (backup_chunk_fast.go,
// byte-identical output); values the fast path doesn't model — alien
// Go types, non-finite floats — fall back to the legacy reflection
// marshal below, which owns both their bytes and their errors.
func (w *ChunkWriter) WriteRow(row ir.Row, columns []*ir.Column) error {
	if w.closed {
		return errors.New("chunk writer closed")
	}
	b, ok := appendRowJSON(w.encBuf[:0], row, w.columnNamesSorted(columns))
	if ok {
		w.encBuf = b
		if _, err := w.bufW.Write(b); err != nil {
			return fmt.Errorf("chunk row write: %w", err)
		}
		if err := w.bufW.WriteByte('\n'); err != nil {
			return fmt.Errorf("chunk row newline: %w", err)
		}
		w.rowCount++
		return nil
	}
	w.encBuf = b[:0]
	return w.writeRowLegacy(row, columns)
}

// writeRowLegacy is the original reflection-based encode — the
// semantic + error oracle for the fast path, and the fallback for
// shapes the fast path bails on. The differential tests in
// backup_chunk_fast_test.go pin the two paths byte-identical.
func (w *ChunkWriter) writeRowLegacy(row ir.Row, columns []*ir.Column) error {
	enc := make(map[string]any, len(columns))
	for _, c := range columns {
		v, ok := row[c.Name]
		if !ok {
			enc[c.Name] = nil
			continue
		}
		enc[c.Name] = encodeValue(v)
	}
	b, err := json.Marshal(enc)
	if err != nil {
		return fmt.Errorf("chunk row marshal: %w", err)
	}
	if _, err := w.bufW.Write(b); err != nil {
		return fmt.Errorf("chunk row write: %w", err)
	}
	if err := w.bufW.WriteByte('\n'); err != nil {
		return fmt.Errorf("chunk row newline: %w", err)
	}
	w.rowCount++
	return nil
}

// columnNamesSorted returns columns' names in stdlib map-key order,
// cached against the slice's identity (the production caller passes
// the same slice every row of a table).
func (w *ChunkWriter) columnNamesSorted(columns []*ir.Column) []string {
	if len(columns) == 0 {
		if w.sortedNames == nil {
			w.sortedNames = []string{}
		}
		return w.sortedNames[:0]
	}
	if w.colsPtr == columns[0] && w.colsLen == len(columns) {
		return w.sortedNames
	}
	w.colsPtr = columns[0]
	w.colsLen = len(columns)
	w.sortedNames = sortedColumnNames(columns)
	return w.sortedNames
}

// Close flushes the buffered writer and gzip stream. Safe to call
// twice; the second call is a no-op. Returns the SHA-256 hex digest
// of the chunk's bytes (post-encryption when in encrypted mode) after
// Close completes.
func (w *ChunkWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.bufW.Flush(); err != nil {
		return fmt.Errorf("chunk writer flush: %w", err)
	}
	if err := w.gzWriter.Close(); err != nil {
		return fmt.Errorf("chunk writer gzip close: %w", err)
	}
	if w.cek != nil {
		// Encrypted mode: encrypt the buffered gzipped bytes and emit
		// `[nonce | ciphertext | authtag]` to out + hasher. The
		// manifest's recorded SHA-256 covers ciphertext, matching
		// what `backup verify` re-hashes off disk.
		ct, err := crypto.EncryptChunk(w.gzBuf.Bytes(), w.cek)
		if err != nil {
			return fmt.Errorf("chunk writer encrypt: %w", err)
		}
		if _, err := w.hasher.Write(ct); err != nil {
			return fmt.Errorf("chunk writer hash: %w", err)
		}
		if _, err := w.out.Write(ct); err != nil {
			return fmt.Errorf("chunk writer ciphertext write: %w", err)
		}
	}
	return nil
}

// Hash returns the hex-encoded SHA-256 of the gzipped chunk bytes.
// Only valid after Close.
func (w *ChunkWriter) Hash() string {
	return fmt.Sprintf("%x", w.hasher.Sum(nil))
}

// RowCount returns the number of rows written so far.
func (w *ChunkWriter) RowCount() int64 { return w.rowCount }

// ChunkReader is the inverse of [ChunkWriter]: streams rows from a
// gzip-compressed JSON Lines stream while computing a SHA-256 to
// compare against the manifest entry. Returns ErrChunkHashMismatch
// at EOF if the recomputed hash doesn't match the expected value.
//
// When the chunk is encrypted, the entire ciphertext is read + hashed
// + decrypted up front in [NewChunkReader]; the rest of the read path
// then feeds plaintext bytes through the gzip reader as if the chunk
// were never encrypted.
type ChunkReader struct {
	src      io.ReadCloser
	hasher   hash.Hash
	gzReader codecReadCloser
	scanner  *bufio.Scanner
	expected string
	header   chunkHeader

	// encrypted reports whether [NewChunkReader] consumed src
	// up-front and is feeding the gzip reader from an in-memory
	// plaintext buffer.
	encrypted bool

	// consumedSrc, when true, means the encrypted-path read+drained
	// src already; the Close path skips the "drain underlying" step
	// to avoid reading from an already-consumed reader.
	consumedSrc bool

	// fastDec is the fast row-decode state (backup_chunk_fast.go):
	// a per-reader key-canonicalization cache. Zero value ready.
	fastDec fastRowDecoder
}

// ErrChunkHashMismatch surfaces when a chunk's recomputed SHA-256
// does not match the expected value carried in the manifest. The
// pipeline's restore path turns this into a hard failure (loud-
// failure tenet — backup integrity is the load-bearing claim).
var ErrChunkHashMismatch = errors.New("backup: chunk SHA-256 mismatch")

// NewChunkReader wraps src with the inverse machinery of [ChunkWriter].
// expectedSHA256 is the hex digest from the manifest; on Close the
// reader compares the recomputed hash and returns
// [ErrChunkHashMismatch] if they differ.
//
// When cek is non-nil, the reader treats src as encrypted bytes: it
// reads the entire ciphertext into memory, hashes ciphertext for
// verification, decrypts via [crypto.DecryptChunk], and feeds the
// resulting plaintext codec stream to the rest of the machinery.
// chunks are bounded (typically a few MB compressed); buffering whole
// chunk in RAM is acceptable.
//
// codec is the codec RECORDED for this chunk's segment in
// lineage.json — never inferred from the bytes. The caller threads it
// through from segment metadata; an unknown recorded codec is rejected
// loudly by [ValidateRecordedCodec] before this is reached.
func NewChunkReader(src io.ReadCloser, expectedSHA256 string, cek []byte, codec Codec) (*ChunkReader, error) {
	// Ownership guard: until the ChunkReader is successfully built (and
	// thereafter owns Close of both the codec reader and src), EVERY
	// early-return error path must release the underlying store handle
	// and any constructed codec reader. Without this a corrupt / bad-
	// codec / hash-mismatch chunk open leaks an FD per failure (on
	// Windows it also blocks temp-dir cleanup). One named guard instead
	// of a Close scattered on each path — and it covers the header-scan
	// paths the scattered form missed.
	var gz codecReadCloser
	success := false
	defer func() {
		if success {
			return
		}
		if gz != nil {
			_ = gz.Close()
		}
		_ = src.Close()
	}()

	if cek != nil && len(cek) != crypto.CEKLen {
		return nil, fmt.Errorf("chunk reader: cek length %d != %d", len(cek), crypto.CEKLen)
	}
	hasher := sha256.New()

	var (
		gzSrc       io.Reader
		encrypted   bool
		consumedSrc bool // true → we already drained src (encrypted path)
	)
	if cek == nil {
		gzSrc = io.TeeReader(src, hasher)
	} else {
		// Encrypted: read all ciphertext, hash it, decrypt, feed
		// plaintext (the codec stream) to the codec reader.
		ct, err := io.ReadAll(src)
		if err != nil {
			return nil, fmt.Errorf("chunk reader: read ciphertext: %w", err)
		}
		if _, err := hasher.Write(ct); err != nil {
			return nil, fmt.Errorf("chunk reader: hash ciphertext: %w", err)
		}
		pt, err := crypto.DecryptChunk(ct, cek)
		if err != nil {
			return nil, fmt.Errorf("chunk reader: decrypt: %w", err)
		}
		gzSrc = bytes.NewReader(pt)
		encrypted = true
		consumedSrc = true
	}
	cr, err := newCodecReader(gzSrc, codec)
	if err != nil {
		return nil, fmt.Errorf("chunk reader: codec header: %w", err)
	}
	gz = cr
	sc := bufio.NewScanner(gz)
	// Allow large rows: 64 MiB max line buffer covers the wide-row
	// workloads --max-buffer-bytes targets without blowing out memory.
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return nil, fmt.Errorf("chunk reader: read header line: %w", err)
		}
		return nil, errors.New("chunk reader: empty chunk file")
	}
	var hdr chunkHeader
	if err := json.Unmarshal(sc.Bytes(), &hdr); err != nil {
		return nil, fmt.Errorf("chunk reader: decode header: %w", err)
	}
	if hdr.Version != chunkHeaderVersion {
		return nil, fmt.Errorf("chunk reader: unsupported chunk format version %d (this build supports %d)",
			hdr.Version, chunkHeaderVersion)
	}
	r := &ChunkReader{
		src:         src,
		hasher:      hasher,
		gzReader:    gz,
		scanner:     sc,
		expected:    expectedSHA256,
		header:      hdr,
		encrypted:   encrypted,
		consumedSrc: consumedSrc,
	}
	success = true
	return r, nil
}

// Header returns the chunk's header (column list + format version).
func (r *ChunkReader) Header() chunkHeader { return r.header }

// ReadRow returns the next row from the chunk, or (nil, io.EOF) at
// end-of-stream. The caller should drain to EOF and then call Close
// to finalise the hash check.
//
// Decoding runs on the fast single-pass path (backup_chunk_fast.go);
// lines it doesn't model — alien envelope shapes, grammar violations
// — fall back to the legacy double-unmarshal below, which owns both
// their semantics and their errors.
func (r *ChunkReader) ReadRow() (ir.Row, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, fmt.Errorf("chunk reader: scan: %w", err)
		}
		return nil, io.EOF
	}
	line := r.scanner.Bytes()
	if row, ok := r.fastDec.decodeRow(line); ok {
		return row, nil
	}
	return readRowLegacy(line)
}

// readRowLegacy is the original two-hop typed decode — the semantic +
// error oracle for the fast path, and the fallback for lines it bails
// on. The differential tests in backup_chunk_fast_test.go pin the two
// paths equivalent on arbitrary input.
func readRowLegacy(line []byte) (ir.Row, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(line, &raw); err != nil {
		return nil, fmt.Errorf("chunk reader: row decode: %w", err)
	}
	row := make(ir.Row, len(raw))
	for k, v := range raw {
		decoded, err := decodeValue(v)
		if err != nil {
			return nil, fmt.Errorf("chunk reader: column %q: %w", k, err)
		}
		row[k] = decoded
	}
	return row, nil
}

// Close finishes reading the underlying stream so the SHA-256 covers
// every byte, then compares against the expected hash from the
// manifest. Returns [ErrChunkHashMismatch] on mismatch.
func (r *ChunkReader) Close() error {
	// Drain any unread bytes so the hasher sees the full stream.
	// (Most callers will have read to EOF already; this is defensive
	// for early-exit paths.)
	if _, err := io.Copy(io.Discard, r.gzReader); err != nil {
		_ = r.gzReader.Close()
		_ = r.src.Close()
		return fmt.Errorf("chunk reader: drain: %w", err)
	}
	if err := r.gzReader.Close(); err != nil {
		_ = r.src.Close()
		return fmt.Errorf("chunk reader: gzip close: %w", err)
	}
	if !r.consumedSrc {
		// Drain the underlying source through the tee so the hasher
		// sees any trailing bytes the gzip stream didn't consume.
		// Encrypted chunks have already had src fully consumed inside
		// NewChunkReader; skip to avoid reading nothing twice.
		if _, err := io.Copy(io.Discard, r.src); err != nil {
			_ = r.src.Close()
			return fmt.Errorf("chunk reader: drain underlying: %w", err)
		}
	}
	if err := r.src.Close(); err != nil {
		return fmt.Errorf("chunk reader: src close: %w", err)
	}
	got := fmt.Sprintf("%x", r.hasher.Sum(nil))
	if r.expected != "" && got != r.expected {
		return fmt.Errorf("%w: expected %s, got %s", ErrChunkHashMismatch, r.expected, got)
	}
	return nil
}

// chunkFetchMaxAttempts is the bounded number of times
// [FetchChunkVerified] re-fetches a content chunk whose object-store read
// came back transiently short / failed. Four attempts (1 + 3 retries) with
// the backoff below covers the flaky-S3-body case the live Track-C restore
// hit; a genuinely corrupt-at-rest chunk still surfaces loudly after the
// attempts are exhausted (re-fetching identical bad bytes can't fix it).
const chunkFetchMaxAttempts = 4

// chunkFetchBackoff is the inter-attempt delay for [FetchChunkVerified]:
// 200ms, 400ms, 800ms. Short because a truncated read is a transport blip,
// not a reparent — the next GET almost always returns the full object.
func chunkFetchBackoff(attempt int) time.Duration {
	return time.Duration(200*(1<<(attempt-1))) * time.Millisecond
}

// FetchChunkVerified reads the entire content chunk at `file` into memory,
// retrying on a transient object-store read failure, and returns a
// bytes-backed reader the caller hands to [NewChunkReader] /
// [NewChangeChunkReader] unchanged.
//
// Why this exists: a streaming chunk read straight into the restore's COPY
// emits rows as it decodes, so a truncated / short object-store GET (a
// flaky S3 body) surfaces deep in the decode path as a row-decode error
// ("unexpected end of JSON input") AFTER some rows have already been
// applied — it cannot be safely re-streamed (that would duplicate the
// emitted rows), so the prior code had no retry and ONE transport blip
// aborted an entire multi-hour restore. Caught live on Track-C: a 3220-chunk
// PlanetScale-Postgres cold-start died ~6 tables in on a single truncated
// chunk that `backup verify` then confirmed intact at rest. This is the
// chunk-read analog of ADR-0114's DDL-phase reparent retry.
//
// Completeness is checked by SHA-256 against the manifest. expectedSHA256
// is the hash of the RAW object bytes — gzip plaintext for an unencrypted
// chunk, ciphertext for an encrypted one (the chunk writer hashes
// post-codec / post-encrypt) — so a short read mismatches and triggers a
// re-fetch, BEFORE any row is emitted. Because the whole object is in hand
// and verified before decoding starts, the retry is safe (no partial
// emit) and idempotent (chunks are content-addressed). The downstream
// reader re-verifies the SHA on Close (a cheap in-memory double-check).
// A persistent mismatch — genuine at-rest corruption — surfaces loudly as
// [ErrChunkHashMismatch] once the attempts are exhausted.
func FetchChunkVerified(ctx context.Context, store irbackup.Store, file, expectedSHA256 string) (io.ReadCloser, error) {
	var lastErr error
	for attempt := 1; attempt <= chunkFetchMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		buf, sum, err := readChunkBytesAndHash(ctx, store, file)
		switch {
		case err != nil:
			lastErr = err
		case sum != expectedSHA256:
			lastErr = fmt.Errorf("%w: expected %s, got %s (chunk %s, attempt %d/%d)",
				ErrChunkHashMismatch, expectedSHA256, sum, file, attempt, chunkFetchMaxAttempts)
		default:
			if attempt > 1 {
				slog.DebugContext(ctx, "restore: chunk fetch recovered after transient short read",
					slog.String("chunk", file), slog.Int("attempt", attempt))
			}
			return io.NopCloser(bytes.NewReader(buf)), nil
		}
		if attempt < chunkFetchMaxAttempts {
			slog.WarnContext(ctx, "restore: chunk fetch failed; retrying",
				slog.String("chunk", file), slog.Int("attempt", attempt),
				slog.Int("max_attempts", chunkFetchMaxAttempts), slog.String("err", lastErr.Error()))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(chunkFetchBackoff(attempt)):
			}
		}
	}
	return nil, fmt.Errorf("fetch chunk %s after %d attempts: %w", file, chunkFetchMaxAttempts, lastErr)
}

// readChunkBytesAndHash GETs file from store, reads the whole body into a
// buffer, and returns the bytes plus their SHA-256 hex digest. The
// TeeReader hashes during the single read pass so a short body is detected
// by [FetchChunkVerified]'s digest comparison rather than slipping into the
// decoder. Closes the store handle before returning.
func readChunkBytesAndHash(ctx context.Context, store irbackup.Store, file string) (data []byte, sha string, err error) {
	src, err := store.Get(ctx, file)
	if err != nil {
		return nil, "", fmt.Errorf("open: %w", err)
	}
	defer func() { _ = src.Close() }()
	h := sha256.New()
	buf, err := io.ReadAll(io.TeeReader(src, h))
	if err != nil {
		return nil, "", fmt.Errorf("read: %w", err)
	}
	return buf, fmt.Sprintf("%x", h.Sum(nil)), nil
}

// HashChunkBytes streams r through a SHA-256 hasher and returns the
// hex digest. Used by `sluice backup verify` to recompute a chunk's
// hash without decoding rows.
func HashChunkBytes(ctx context.Context, r io.Reader) (string, error) {
	h := sha256.New()
	buf := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := r.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("hash chunk: %w", err)
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// ============================================================
// Tagged-value codec — round-trips Go-native row values through JSON.
// ============================================================

// The on-wire shape for non-natively-JSON-roundtrippable values is
// `{"_t":"<tag>","v":<payload>}`. The encoder builds it as a
// `map[string]any` (so the standard json marshaller does the work);
// the decoder probes the `_t` key directly via the
// `map[string]json.RawMessage` shape in [decodeValue]. No struct
// type for the envelope is needed because the encoder/decoder paths
// don't share it.

// encodeValue returns a JSON-safe representation of v that round-trips
// back to a Go-native shape via [decodeValue]. Most types pass through
// unchanged; types that don't survive raw JSON (`[]byte`, `time.Time`,
// explicit integer widths, etc.) wear the [taggedValue] envelope.
func encodeValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return map[string]any{"_t": "f64s", "v": nonFiniteString(x)}
		}
		return x
	case float32:
		if f := float64(x); math.IsNaN(f) || math.IsInf(f, 0) {
			return map[string]any{"_t": "f64s", "v": nonFiniteString(f)}
		}
		return x
	case string, bool:
		return x
	case []byte:
		return map[string]any{"_t": "bytes", "v": base64.StdEncoding.EncodeToString(x)}
	case time.Time:
		return map[string]any{"_t": "time", "v": x.UTC().Format(time.RFC3339Nano)}
	case int:
		return map[string]any{"_t": "i64", "v": int64(x)}
	case int8:
		return map[string]any{"_t": "i64", "v": int64(x)}
	case int16:
		return map[string]any{"_t": "i64", "v": int64(x)}
	case int32:
		return map[string]any{"_t": "i64", "v": int64(x)}
	case int64:
		return map[string]any{"_t": "i64", "v": x}
	case uint, uint8, uint16, uint32, uint64:
		var u uint64
		switch ux := x.(type) {
		case uint:
			u = uint64(ux)
		case uint8:
			u = uint64(ux)
		case uint16:
			u = uint64(ux)
		case uint32:
			u = uint64(ux)
		case uint64:
			u = ux
		}
		return map[string]any{"_t": "u64", "v": strconv.FormatUint(u, 10)}
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = encodeValue(e)
		}
		// Wrap the slice in an envelope so decoder can reliably
		// distinguish a list-of-encoded-values from a row-natural
		// JSON array (rare in practice but possible with PG arrays).
		return map[string]any{"_t": "list", "v": out}
	case []string:
		// Common shape from PG text-array decode.
		return map[string]any{"_t": "list_str", "v": x}
	case map[string]any:
		// Pass through — a JSON column's pre-decoded form.
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = encodeValue(e)
		}
		return map[string]any{"_t": "map", "v": out}
	}
	// Fallback: rely on the value's own JSON encoding. Unknown types
	// surface as a marshal error if they aren't JSON-safe — preferable
	// to silently dropping them.
	return v
}

// nonFiniteString renders a non-finite float64 as its f64s-envelope
// sentinel. Callers guarantee f is NaN or ±Inf.
func nonFiniteString(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	default:
		return "NaN"
	}
}

// canonicalNaN is the IEEE-754 canonical quiet NaN (0x7FF8…0000) —
// the bit pattern PG itself produces when parsing 'NaN', so a sluice
// restore is float8send-bit-identical to a pg_dump/pg_restore round
// trip. Go's math.NaN() is 0x7FF8…0001, one payload bit off; payload
// bits don't survive the string sentinel either way (documented
// canonicalization), so emit the pattern the ecosystem standardizes
// on.
var canonicalNaN = math.Float64frombits(0x7ff8000000000000)

// nonFiniteFromString is the strict inverse of [nonFiniteString].
func nonFiniteFromString(s string) (float64, error) {
	switch s {
	case "NaN":
		return canonicalNaN, nil
	case "+Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	default:
		return 0, fmt.Errorf("f64s payload %q is not one of NaN/+Inf/-Inf", s)
	}
}

// decodeValue is the inverse of [encodeValue]. Bare JSON values pass
// through; tagged envelopes are unwrapped to their native Go shape.
func decodeValue(raw json.RawMessage) (any, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	// Quick branch: only objects can carry the tagged envelope.
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		// Try to decode as a tagged envelope; fall through to map on
		// failure / non-tagged shape.
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(raw, &probe); err == nil {
			if tagRaw, ok := probe["_t"]; ok {
				var tag string
				if err := json.Unmarshal(tagRaw, &tag); err == nil {
					return decodeTaggedValue(tag, probe["v"])
				}
			}
			// Not a tagged envelope — decode the map naturally.
			out := make(map[string]any, len(probe))
			for k, v := range probe {
				dv, err := decodeValue(v)
				if err != nil {
					return nil, fmt.Errorf("map key %q: %w", k, err)
				}
				out[k] = dv
			}
			return out, nil
		}
	}
	// Fall back to natural JSON decoding.
	var natural any
	if err := json.Unmarshal(raw, &natural); err != nil {
		return nil, fmt.Errorf("decode value: %w", err)
	}
	return natural, nil
}

// decodeTaggedValue converts a tagged envelope back to its Go-native
// shape. Unknown tags are an error — the format-version field on the
// chunk header would have already gated the file open, so an unknown
// tag this far in indicates either a bug or a disk-corruption shape
// the loud-failure tenet prefers to surface.
func decodeTaggedValue(tag string, payload json.RawMessage) (any, error) {
	switch tag {
	case "bytes":
		var s string
		if err := json.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("bytes payload: %w", err)
		}
		out, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("bytes base64: %w", err)
		}
		return out, nil
	case "time":
		var s string
		if err := json.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("time payload: %w", err)
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, fmt.Errorf("time parse: %w", err)
		}
		return t, nil
	case "i64":
		var n int64
		if err := json.Unmarshal(payload, &n); err != nil {
			return nil, fmt.Errorf("i64 payload: %w", err)
		}
		return n, nil
	case "u64":
		var s string
		if err := json.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("u64 payload: %w", err)
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("u64 parse: %w", err)
		}
		return n, nil
	case "f64":
		var f float64
		if err := json.Unmarshal(payload, &f); err != nil {
			return nil, fmt.Errorf("f64 payload: %w", err)
		}
		return f, nil
	case "f64s":
		// Non-finite float sentinel (Bug 138). Strict: exactly the
		// three strings the encoder emits; anything else is corruption
		// and fails loudly.
		var s string
		if err := json.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("f64s payload: %w", err)
		}
		f, err := nonFiniteFromString(s)
		if err != nil {
			return nil, err
		}
		return f, nil
	case "list":
		var arr []json.RawMessage
		if err := json.Unmarshal(payload, &arr); err != nil {
			return nil, fmt.Errorf("list payload: %w", err)
		}
		out := make([]any, len(arr))
		for i, e := range arr {
			dv, err := decodeValue(e)
			if err != nil {
				return nil, fmt.Errorf("list[%d]: %w", i, err)
			}
			out[i] = dv
		}
		return out, nil
	case "list_str":
		var ss []string
		if err := json.Unmarshal(payload, &ss); err != nil {
			return nil, fmt.Errorf("list_str payload: %w", err)
		}
		return ss, nil
	case "map":
		var m map[string]json.RawMessage
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, fmt.Errorf("map payload: %w", err)
		}
		out := make(map[string]any, len(m))
		for k, v := range m {
			dv, err := decodeValue(v)
			if err != nil {
				return nil, fmt.Errorf("map[%q]: %w", k, err)
			}
			out[k] = dv
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown value tag %q", tag)
	}
}
