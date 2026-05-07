// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

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
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"
	"time"

	"github.com/orware/sluice/internal/ir"
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

// chunkWriter streams JSON-Lines rows to a gzip-compressed [io.Writer]
// while computing a SHA-256 over the gzipped bytes (so restore-time
// verification matches what's actually on disk / in object storage).
//
// Lifecycle: NewChunkWriter → WriteRow* → Close. Close MUST be called
// (it flushes the gzip buffer). Hash() returns the final hex SHA-256
// only after Close.
type chunkWriter struct {
	out      io.Writer
	hasher   hash.Hash
	gzWriter *gzip.Writer
	bufW     *bufio.Writer
	rowCount int64
	closed   bool
}

// newChunkWriter wraps out (the destination — typically a pipe to
// [ir.BackupStore.Put]) with gzip + JSON-Lines machinery and writes
// the format header. Caller must call Close to flush.
func newChunkWriter(out io.Writer, columns []string) (*chunkWriter, error) {
	hasher := sha256.New()
	tee := io.MultiWriter(out, hasher)
	gz := gzip.NewWriter(tee)
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
	return &chunkWriter{
		out:      out,
		hasher:   hasher,
		gzWriter: gz,
		bufW:     bw,
	}, nil
}

// WriteRow encodes row using the column order pinned at construction
// and emits one JSON Lines record. Returns the cumulative row count.
func (w *chunkWriter) WriteRow(row ir.Row, columns []*ir.Column) error {
	if w.closed {
		return errors.New("chunk writer closed")
	}
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

// Close flushes the buffered writer and gzip stream. Safe to call
// twice; the second call is a no-op. Returns the SHA-256 hex digest
// of the gzipped bytes after Close completes.
func (w *chunkWriter) Close() error {
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
	return nil
}

// Hash returns the hex-encoded SHA-256 of the gzipped chunk bytes.
// Only valid after Close.
func (w *chunkWriter) Hash() string {
	return fmt.Sprintf("%x", w.hasher.Sum(nil))
}

// RowCount returns the number of rows written so far.
func (w *chunkWriter) RowCount() int64 { return w.rowCount }

// chunkReader is the inverse of [chunkWriter]: streams rows from a
// gzip-compressed JSON Lines stream while computing a SHA-256 to
// compare against the manifest entry. Returns ErrChunkHashMismatch
// at EOF if the recomputed hash doesn't match the expected value.
type chunkReader struct {
	src      io.ReadCloser
	hasher   hash.Hash
	gzReader *gzip.Reader
	scanner  *bufio.Scanner
	expected string
	header   chunkHeader
}

// ErrChunkHashMismatch surfaces when a chunk's recomputed SHA-256
// does not match the expected value carried in the manifest. The
// pipeline's restore path turns this into a hard failure (loud-
// failure tenet — backup integrity is the load-bearing claim).
var ErrChunkHashMismatch = errors.New("backup: chunk SHA-256 mismatch")

// newChunkReader wraps src with the inverse machinery of [chunkWriter].
// expectedSHA256 is the hex digest from the manifest; on Close the
// reader compares the recomputed hash and returns
// [ErrChunkHashMismatch] if they differ.
func newChunkReader(src io.ReadCloser, expectedSHA256 string) (*chunkReader, error) {
	hasher := sha256.New()
	tee := io.TeeReader(src, hasher)
	gz, err := gzip.NewReader(tee)
	if err != nil {
		_ = src.Close()
		return nil, fmt.Errorf("chunk reader: gzip header: %w", err)
	}
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
	return &chunkReader{
		src:      src,
		hasher:   hasher,
		gzReader: gz,
		scanner:  sc,
		expected: expectedSHA256,
		header:   hdr,
	}, nil
}

// Header returns the chunk's header (column list + format version).
func (r *chunkReader) Header() chunkHeader { return r.header }

// ReadRow returns the next row from the chunk, or (nil, io.EOF) at
// end-of-stream. The caller should drain to EOF and then call Close
// to finalise the hash check.
func (r *chunkReader) ReadRow() (ir.Row, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, fmt.Errorf("chunk reader: scan: %w", err)
		}
		return nil, io.EOF
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(r.scanner.Bytes(), &raw); err != nil {
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
func (r *chunkReader) Close() error {
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
	// Drain the underlying source through the tee so the hasher sees
	// any trailing bytes the gzip stream didn't consume.
	if _, err := io.Copy(io.Discard, r.src); err != nil {
		_ = r.src.Close()
		return fmt.Errorf("chunk reader: drain underlying: %w", err)
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

// hashChunkBytes streams r through a SHA-256 hasher and returns the
// hex digest. Used by `sluice backup verify` to recompute a chunk's
// hash without decoding rows.
func hashChunkBytes(ctx context.Context, r io.Reader) (string, error) {
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
	case string, bool, float64, float32:
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
