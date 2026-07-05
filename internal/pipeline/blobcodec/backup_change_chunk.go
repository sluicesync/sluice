// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

// Change-event chunk format for Phase 3 incremental backups.
//
// Mirrors the row-chunk format in [backup_chunk.go] (gzip-compressed
// JSON Lines, header + per-line records, SHA-256 over the gzipped
// bytes) but each line carries one serialised [ir.Change] event
// instead of one row. The schema-and-row format isn't reused verbatim
// because rows are positional-by-column-list whereas changes are
// kind-tagged sum types — there's no common column-list pin.
//
// On-wire shape of one chunk:
//
//   line 0: {"_h":1,"chunk_kind":"changes"}
//   line 1: {"_t":"insert","schema":"public","table":"users",
//            "row":{...},"position":{"engine":"postgres","token":"..."}}
//   line 2: {"_t":"update","schema":"public","table":"users",
//            "before":{...},"after":{...},"position":{...}}
//   line N: {"_t":"tx_commit","position":{...}}
//
// Row maps reuse the encodeValue / decodeValue helpers from the
// existing chunk codec so wide values (bytes, time, int64, etc.)
// round-trip through the same envelopes.

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
)

// changeChunkHeader is the on-wire shape of a change-chunk's first
// line. Distinct from [chunkHeader] because the writer / reader pair
// is responsible for asserting the chunk's flavour up front, before
// any row decode happens.
type changeChunkHeader struct {
	Version   int    `json:"_h"`
	ChunkKind string `json:"chunk_kind"`
}

const changeChunkKind = "changes"

// ChangeChunkWriter streams [ir.Change] events into a gzip-compressed
// JSON Lines stream while tracking SHA-256 over the bytes that land on
// disk (post-encryption when in encrypted mode). Lifecycle mirrors
// [ChunkWriter]: New → WriteChange* → Close.
type ChangeChunkWriter struct {
	out         io.Writer
	hasher      hash.Hash
	gzWriter    codecWriteCloser
	bufW        *bufio.Writer
	changeCount int64
	closed      bool

	// cek, when non-nil, enables encrypted mode (mirrors ChunkWriter).
	cek   []byte
	gzBuf *bytes.Buffer

	// snapshots collects every ir.SchemaSnapshot observed during this
	// chunk's lifetime so the caller can attach them to the Manifest's
	// SchemaHistory field at finalisation (ADR-0049 Chunk D —
	// supersedes the Chunk-B scope-fence skip). Snapshots ride the
	// Manifest, NOT the per-row JSONL stream (they have no row payload,
	// and the change-chunk codec dispatches on the row-shaped kinds).
	snapshots []ir.SchemaSnapshot
}

// NewChangeChunkWriter wraps out (typically a pipe-buffer destined
// for [irbackup.Store.Put]) with the gzip + JSONL machinery and
// writes the chunk header. Caller must call Close to flush.
//
// When cek is non-nil, the gzipped JSONL bytes are buffered in memory
// and AES-256-GCM-encrypted at Close time before being written to out.
// The hasher covers post-encryption bytes so `backup verify`'s
// sha256-only check matches what's on disk.
func NewChangeChunkWriter(out io.Writer, cek []byte, codec Codec) (*ChangeChunkWriter, error) {
	if cek != nil && len(cek) != crypto.CEKLen {
		return nil, fmt.Errorf("change chunk writer: cek length %d != %d", len(cek), crypto.CEKLen)
	}
	hasher := sha256.New()
	var (
		gzDst io.Writer
		gzBuf *bytes.Buffer
	)
	if cek == nil {
		gzDst = io.MultiWriter(out, hasher)
	} else {
		gzBuf = &bytes.Buffer{}
		gzDst = gzBuf
	}
	gz, err := newCodecWriter(gzDst, codec)
	if err != nil {
		return nil, fmt.Errorf("change chunk writer codec: %w", err)
	}
	bw := bufio.NewWriter(gz)

	hdr := changeChunkHeader{Version: chunkHeaderVersion, ChunkKind: changeChunkKind}
	hb, err := json.Marshal(hdr)
	if err != nil {
		return nil, fmt.Errorf("change chunk header marshal: %w", err)
	}
	if _, err := bw.Write(hb); err != nil {
		return nil, fmt.Errorf("change chunk header write: %w", err)
	}
	if err := bw.WriteByte('\n'); err != nil {
		return nil, fmt.Errorf("change chunk header newline: %w", err)
	}
	return &ChangeChunkWriter{
		out:      out,
		hasher:   hasher,
		gzWriter: gz,
		bufW:     bw,
		cek:      cek,
		gzBuf:    gzBuf,
	}, nil
}

// WriteChange encodes c as a JSONL record. Returns an error on
// unknown change kinds (a future ir.Change variant would land here as
// "unknown"; loud-failure surface).
func (w *ChangeChunkWriter) WriteChange(c ir.Change) error {
	if w.closed {
		return errors.New("change chunk writer closed")
	}
	// ADR-0049 Chunk D: collect SchemaSnapshot boundary events into a
	// side-channel so the orchestrator can attach them to the Manifest's
	// SchemaHistory field at finalisation. Snapshots ride the Manifest,
	// NOT the per-row JSONL stream — they have no row payload and the
	// change-chunk codec dispatches on row-shaped kinds. This supersedes
	// the Chunk-B scope-fence skip: a DDL during a backup window now
	// produces schema-history that a restore+resume can replay (the
	// resumed stream lands at the backup's EndPosition with a primed
	// schema-history, NOT the loud ADR-0022 cold-start floor that the
	// pre-Chunk-D state had). The chunk's JSONL bytes remain
	// byte-identical to pre-Chunk-B: no record written, no count bump.
	if s, ok := c.(ir.SchemaSnapshot); ok {
		w.snapshots = append(w.snapshots, s)
		return nil
	}
	wire, err := encodeChange(c)
	if err != nil {
		return err
	}
	b, err := json.Marshal(wire)
	if err != nil {
		return fmt.Errorf("change chunk record marshal: %w", err)
	}
	if _, err := w.bufW.Write(b); err != nil {
		return fmt.Errorf("change chunk record write: %w", err)
	}
	if err := w.bufW.WriteByte('\n'); err != nil {
		return fmt.Errorf("change chunk record newline: %w", err)
	}
	w.changeCount++
	return nil
}

// Close flushes the buffered writer and gzip stream. Idempotent. In
// encrypted mode, encrypts the gzipped buffer and writes the
// ciphertext to out before returning.
func (w *ChangeChunkWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.bufW.Flush(); err != nil {
		return fmt.Errorf("change chunk writer flush: %w", err)
	}
	if err := w.gzWriter.Close(); err != nil {
		return fmt.Errorf("change chunk writer gzip close: %w", err)
	}
	if w.cek != nil {
		ct, err := crypto.EncryptChunk(w.gzBuf.Bytes(), w.cek)
		if err != nil {
			return fmt.Errorf("change chunk writer encrypt: %w", err)
		}
		if _, err := w.hasher.Write(ct); err != nil {
			return fmt.Errorf("change chunk writer hash: %w", err)
		}
		if _, err := w.out.Write(ct); err != nil {
			return fmt.Errorf("change chunk writer ciphertext write: %w", err)
		}
	}
	return nil
}

// Hash returns the hex-encoded SHA-256 of the gzipped bytes.
func (w *ChangeChunkWriter) Hash() string {
	return fmt.Sprintf("%x", w.hasher.Sum(nil))
}

// ChangeCount returns the number of changes written so far.
func (w *ChangeChunkWriter) ChangeCount() int64 { return w.changeCount }

// Snapshots returns the [ir.SchemaSnapshot] events observed during
// this writer's lifetime so the incremental-backup orchestrator can
// attach them to the Manifest's SchemaHistory field at finalisation
// (ADR-0049 Chunk D). Snapshots do NOT appear in the chunk's JSONL
// stream and do NOT count toward ChangeCount — they ride the
// Manifest. The returned slice is the writer's own backing slice
// (callers should not mutate it; this is the same convention as
// other internal-pipeline accessors).
func (w *ChangeChunkWriter) Snapshots() []ir.SchemaSnapshot { return w.snapshots }

// ChangeChunkReader is the inverse: streams [ir.Change] events back
// from a change chunk while validating SHA-256. When cek is non-nil,
// the chunk's bytes are decrypted up-front (mirrors [ChunkReader]).
type ChangeChunkReader struct {
	src      io.ReadCloser
	hasher   hash.Hash
	gzReader codecReadCloser
	scanner  *bufio.Scanner
	expected string
	header   changeChunkHeader

	encrypted   bool
	consumedSrc bool
}

// NewChangeChunkReader opens a change-event chunk for reading, verifying
// its SHA-256 as events are streamed. The inverse of [NewChangeChunkWriter].
//
// codec is the codec RECORDED for this chunk's segment in
// lineage.json — never inferred from the bytes (DR data; an inferred
// codec is a latent corruption path).
func NewChangeChunkReader(src io.ReadCloser, expectedSHA256 string, cek []byte, codec Codec) (*ChangeChunkReader, error) {
	// Ownership guard: same as NewChunkReader — every early-return error
	// path releases the store handle + any constructed codec reader so a
	// corrupt / bad-codec / hash-mismatch change-chunk open doesn't leak
	// an FD (and on Windows block temp-dir cleanup). One named guard,
	// covering the header-scan paths the scattered closes missed.
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
		return nil, fmt.Errorf("change chunk reader: cek length %d != %d", len(cek), crypto.CEKLen)
	}
	hasher := sha256.New()
	var (
		gzSrc       io.Reader
		encrypted   bool
		consumedSrc bool
	)
	if cek == nil {
		gzSrc = io.TeeReader(src, hasher)
	} else {
		ct, err := io.ReadAll(src)
		if err != nil {
			return nil, fmt.Errorf("change chunk reader: read ciphertext: %w", err)
		}
		if _, err := hasher.Write(ct); err != nil {
			return nil, fmt.Errorf("change chunk reader: hash ciphertext: %w", err)
		}
		pt, err := crypto.DecryptChunk(ct, cek)
		if err != nil {
			return nil, fmt.Errorf("change chunk reader: decrypt: %w", err)
		}
		gzSrc = bytes.NewReader(pt)
		encrypted = true
		consumedSrc = true
	}
	cr, err := newCodecReader(gzSrc, codec)
	if err != nil {
		return nil, fmt.Errorf("change chunk reader: codec header: %w", err)
	}
	gz = cr
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return nil, fmt.Errorf("change chunk reader: read header: %w", err)
		}
		return nil, errors.New("change chunk reader: empty chunk file")
	}
	var hdr changeChunkHeader
	if err := json.Unmarshal(sc.Bytes(), &hdr); err != nil {
		return nil, fmt.Errorf("change chunk reader: decode header: %w", err)
	}
	if hdr.Version != chunkHeaderVersion {
		return nil, fmt.Errorf("change chunk reader: unsupported chunk format version %d (this build supports %d)",
			hdr.Version, chunkHeaderVersion)
	}
	if hdr.ChunkKind != changeChunkKind {
		return nil, fmt.Errorf("change chunk reader: chunk_kind = %q; want %q", hdr.ChunkKind, changeChunkKind)
	}
	r := &ChangeChunkReader{
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

// ReadChange returns the next [ir.Change] from the chunk, or
// (nil, io.EOF) at end-of-stream.
func (r *ChangeChunkReader) ReadChange() (ir.Change, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, fmt.Errorf("change chunk reader: scan: %w", err)
		}
		return nil, io.EOF
	}
	var wire changeWire
	if err := json.Unmarshal(r.scanner.Bytes(), &wire); err != nil {
		return nil, fmt.Errorf("change chunk reader: record decode: %w", err)
	}
	c, err := decodeChange(&wire)
	if err != nil {
		return nil, fmt.Errorf("change chunk reader: decode change: %w", err)
	}
	return c, nil
}

// Close drains the remaining bytes through the hasher and verifies
// the SHA-256 against the expected value from the manifest. Returns
// [ErrChunkHashMismatch] on mismatch.
func (r *ChangeChunkReader) Close() error {
	if _, err := io.Copy(io.Discard, r.gzReader); err != nil {
		_ = r.gzReader.Close()
		_ = r.src.Close()
		return fmt.Errorf("change chunk reader: drain: %w", err)
	}
	if err := r.gzReader.Close(); err != nil {
		_ = r.src.Close()
		return fmt.Errorf("change chunk reader: gzip close: %w", err)
	}
	if !r.consumedSrc {
		if _, err := io.Copy(io.Discard, r.src); err != nil {
			_ = r.src.Close()
			return fmt.Errorf("change chunk reader: drain underlying: %w", err)
		}
	}
	if err := r.src.Close(); err != nil {
		return fmt.Errorf("change chunk reader: src close: %w", err)
	}
	got := fmt.Sprintf("%x", r.hasher.Sum(nil))
	if r.expected != "" && got != r.expected {
		return fmt.Errorf("%w: expected %s, got %s", ErrChunkHashMismatch, r.expected, got)
	}
	return nil
}

// ============================================================
// Wire-codec for ir.Change values.
// ============================================================

// changeWire is the JSON shape one record in a change chunk takes.
// The fields are union-typed: Insert uses Row; Update uses Before /
// After; Delete uses Before; Truncate uses none of them; TxBegin /
// TxCommit use only Position. The decoder branches on Kind.
//
// Row / Before / After are map[string]json.RawMessage — NOT map[string]any.
// Bug 172: with map[string]any, json.Unmarshal of the record decodes the i64
// envelope's `"v":<number>` to float64, silently corrupting int64 values above
// 2^53 BEFORE decodeValue ever runs. Holding each value as a RawMessage hands
// decodeValue the exact wire bytes (the same approach the row-chunk decoder at
// backup_chunk.go uses), so int64 round-trips losslessly. The on-wire JSON is
// identical to the map[string]any form, so existing backups decode correctly.
type changeWire struct {
	Kind     string                     `json:"_t"`
	Schema   string                     `json:"schema,omitempty"`
	Table    string                     `json:"table,omitempty"`
	Row      map[string]json.RawMessage `json:"row,omitempty"`
	Before   map[string]json.RawMessage `json:"before,omitempty"`
	After    map[string]json.RawMessage `json:"after,omitempty"`
	Position ir.Position                `json:"position"`
}

const (
	changeKindInsert   = "insert"
	changeKindUpdate   = "update"
	changeKindDelete   = "delete"
	changeKindTruncate = "truncate"
	changeKindTxBegin  = "tx_begin"
	changeKindTxCommit = "tx_commit"
)

// encodeChange flattens an [ir.Change] into a [changeWire] suitable
// for JSON marshalling. Row values pass through encodeValue so wide
// types round-trip via the existing tagged-value envelope.
func encodeChange(c ir.Change) (*changeWire, error) {
	if c == nil {
		return nil, errors.New("encode change: nil change")
	}
	switch x := c.(type) {
	case ir.Insert:
		row, err := encodeRowValues(x.Row)
		if err != nil {
			return nil, err
		}
		return &changeWire{
			Kind:     changeKindInsert,
			Schema:   x.Schema,
			Table:    x.Table,
			Row:      row,
			Position: x.Position,
		}, nil
	case ir.Update:
		before, err := encodeRowValues(x.Before)
		if err != nil {
			return nil, err
		}
		after, err := encodeRowValues(x.After)
		if err != nil {
			return nil, err
		}
		return &changeWire{
			Kind:     changeKindUpdate,
			Schema:   x.Schema,
			Table:    x.Table,
			Before:   before,
			After:    after,
			Position: x.Position,
		}, nil
	case ir.Delete:
		before, err := encodeRowValues(x.Before)
		if err != nil {
			return nil, err
		}
		return &changeWire{
			Kind:     changeKindDelete,
			Schema:   x.Schema,
			Table:    x.Table,
			Before:   before,
			Position: x.Position,
		}, nil
	case ir.Truncate:
		return &changeWire{
			Kind:     changeKindTruncate,
			Schema:   x.Schema,
			Table:    x.Table,
			Position: x.Position,
		}, nil
	case ir.TxBegin:
		return &changeWire{Kind: changeKindTxBegin, Position: x.Position}, nil
	case ir.TxCommit:
		return &changeWire{Kind: changeKindTxCommit, Position: x.Position}, nil
	default:
		return nil, fmt.Errorf("encode change: unsupported change type %T", c)
	}
}

// decodeChange is the inverse of [encodeChange]. The wire-shape's
// Row / Before / After maps are the JSON-decoded form; we re-run
// decodeValue on each entry so tagged envelopes bounce back to their
// Go-native shape.
func decodeChange(w *changeWire) (ir.Change, error) {
	if w == nil {
		return nil, errors.New("decode change: nil wire")
	}
	switch w.Kind {
	case changeKindInsert:
		row, err := decodeRowValues(w.Row)
		if err != nil {
			return nil, err
		}
		return ir.Insert{
			Position: w.Position,
			Schema:   w.Schema,
			Table:    w.Table,
			Row:      row,
		}, nil
	case changeKindUpdate:
		before, err := decodeRowValues(w.Before)
		if err != nil {
			return nil, err
		}
		after, err := decodeRowValues(w.After)
		if err != nil {
			return nil, err
		}
		return ir.Update{
			Position: w.Position,
			Schema:   w.Schema,
			Table:    w.Table,
			Before:   before,
			After:    after,
		}, nil
	case changeKindDelete:
		before, err := decodeRowValues(w.Before)
		if err != nil {
			return nil, err
		}
		return ir.Delete{
			Position: w.Position,
			Schema:   w.Schema,
			Table:    w.Table,
			Before:   before,
		}, nil
	case changeKindTruncate:
		return ir.Truncate{
			Position: w.Position,
			Schema:   w.Schema,
			Table:    w.Table,
		}, nil
	case changeKindTxBegin:
		return ir.TxBegin{Position: w.Position}, nil
	case changeKindTxCommit:
		return ir.TxCommit{Position: w.Position}, nil
	default:
		return nil, fmt.Errorf("decode change: unknown kind %q", w.Kind)
	}
}

// encodeRowValues runs each value through encodeValue so wide types
// (bytes, time, int64) wear the tagged-value envelope, then marshals each
// envelope to a json.RawMessage. Holding RawMessage (rather than `any`) is what
// lets the decoder recover int64 losslessly (Bug 172) — the marshalled bytes
// are the same the map[string]any form produced, so the on-wire JSON is
// unchanged. nil rows (legitimate for Truncate / TxBegin / TxCommit and for
// Update / Delete with no before-image) round-trip to nil.
func encodeRowValues(r ir.Row) (map[string]json.RawMessage, error) {
	if r == nil {
		return nil, nil
	}
	out := make(map[string]json.RawMessage, len(r))
	for k, v := range r {
		raw, err := json.Marshal(encodeValue(v))
		if err != nil {
			return nil, fmt.Errorf("encode row column %q: %w", k, err)
		}
		out[k] = raw
	}
	return out, nil
}

// decodeRowValues is the inverse of encodeRowValues. Each value is already a
// json.RawMessage (the EXACT wire bytes — see changeWire: this is the Bug-172
// fix, avoiding the map[string]any float64 round-trip), so decodeValue branches
// on the tagged envelope directly with no precision loss.
func decodeRowValues(m map[string]json.RawMessage) (ir.Row, error) {
	if m == nil {
		return nil, nil
	}
	out := make(ir.Row, len(m))
	for k, v := range m {
		dec, err := decodeValue(v)
		if err != nil {
			return nil, fmt.Errorf("decode row column %q: %w", k, err)
		}
		out[k] = dec
	}
	return out, nil
}

// nopReadCloser wraps a [bytes.Reader] (or any [io.Reader]) to
// satisfy [io.ReadCloser] for change-chunk-reader test paths that
// feed in-memory bytes. Mirrors io.NopCloser but exposed locally so
// tests in other packages don't need to import this file's helpers.
type nopReadCloser struct {
	io.Reader
}

func (nopReadCloser) Close() error { return nil }

// nopReadCloserFromBytes wraps a byte slice into an [io.ReadCloser].
func nopReadCloserFromBytes(b []byte) io.ReadCloser {
	return nopReadCloser{Reader: bytes.NewReader(b)}
}
