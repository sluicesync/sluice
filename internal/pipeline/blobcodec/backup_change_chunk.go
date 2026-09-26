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
	"strings"

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

	// encodedBytes is the cumulative UNCOMPRESSED size of the JSON Lines
	// accepted so far — the change-chunk twin of [ChunkWriter.encodedBytes],
	// so this lane can roll on bytes and not only on a row count.
	encodedBytes int64
	closed       bool

	// cek, when non-nil, enables encrypted mode (mirrors ChunkWriter).
	// aad, when additionally non-nil, is the chunk's position binding
	// applied at Close-time encryption ([irbackup.ChunkAAD]).
	cek   []byte
	aad   []byte
	gzBuf *bytes.Buffer

	// snapshots collects every ir.SchemaSnapshot observed during this
	// chunk's lifetime so the caller can attach them to the Manifest's
	// SchemaHistory field at finalisation (ADR-0049 Chunk D —
	// supersedes the Chunk-B scope-fence skip). Snapshots ride the
	// Manifest, NOT the per-row JSONL stream (they have no row payload,
	// and the change-chunk codec dispatches on the row-shaped kinds).
	snapshots []ir.SchemaSnapshot

	// carriedExactNumber records that a change this chunk encoded carried a
	// [json.Number] at any depth (see [ChangeChunkWriter.CarriedExactNumber]).
	carriedExactNumber bool
}

// NewChangeChunkWriter wraps out (typically a pipe-buffer destined
// for [irbackup.Store.Put]) with the gzip + JSONL machinery and
// writes the chunk header. Caller must call Close to flush.
//
// When cek is non-nil, the gzipped JSONL bytes are buffered in memory
// and AES-256-GCM-encrypted at Close time before being written to out.
// The hasher covers post-encryption bytes so `backup verify`'s
// sha256-only check matches what's on disk. aad mirrors
// [NewChunkWriter]: the chunk's position binding ([irbackup.ChunkAAD],
// nil for plaintext and pre-FormatVersion-5 chains); aad without cek
// is refused.
func NewChangeChunkWriter(out io.Writer, cek []byte, codec Codec, aad []byte) (*ChangeChunkWriter, error) {
	if cek != nil && len(cek) != crypto.CEKLen {
		return nil, fmt.Errorf("change chunk writer: cek length %d != %d", len(cek), crypto.CEKLen)
	}
	if cek == nil && aad != nil {
		return nil, errors.New("change chunk writer: aad supplied without a cek (plaintext chunks cannot carry a position binding)")
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
		aad:      aad,
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
	if !w.carriedExactNumber {
		w.carriedExactNumber = changeCarriesJSONNumber(c)
	}
	wire, err := encodeChange(c)
	if err != nil {
		return err
	}
	b, err := json.Marshal(wire)
	if err != nil {
		return fmt.Errorf("change chunk record marshal: %w", err)
	}
	// Bug 226's third write core. The data-chunk writer's two cores refuse an
	// unreadable row; this one did not, and its reader capped a line at a
	// LITERAL 64 MiB rather than the shared constant — so an incremental
	// carrying one wide CDC row reproduced the original defect exactly:
	// `backup incremental` rc=0, `backup verify` rc=0, `restore` "token too
	// long". A change event carries a full row image, so a wide TEXT/JSON/BLOB
	// column reaches it by the same route.
	if err := checkChunkLineLength(len(b)); err != nil {
		return err
	}
	if _, err := w.bufW.Write(b); err != nil {
		return fmt.Errorf("change chunk record write: %w", err)
	}
	if err := w.bufW.WriteByte('\n'); err != nil {
		return fmt.Errorf("change chunk record newline: %w", err)
	}
	w.changeCount++
	w.encodedBytes += int64(len(b)) + 1
	return nil
}

// BytesWritten returns the cumulative UNCOMPRESSED size of the JSON Lines this
// writer has accepted, mirroring [ChunkWriter.BytesWritten] — the number a
// caller rolls a chunk on (audit 2026-08-05 C-3).
//
// Uncompressed, deliberately and for the same reason as the data-chunk lane:
// where a chunk ends must not depend on how well it happened to compress, or
// the day a codec or its level changes, every boundary moves.
func (w *ChangeChunkWriter) BytesWritten() int64 { return w.encodedBytes }

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
		ct, err := crypto.EncryptChunkWithAAD(w.gzBuf.Bytes(), w.cek, w.aad)
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

	// preserveNumbers decodes bare JSON numbers as json.Number (see
	// [ChangeChunkReader.PreserveNumbers]).
	preserveNumbers bool
}

// NewChangeChunkReader opens a change-event chunk for reading, verifying
// its SHA-256 as events are streamed. The inverse of [NewChangeChunkWriter].
//
// codec is the codec RECORDED for this chunk's segment in
// lineage.json — never inferred from the bytes (DR data; an inferred
// codec is a latent corruption path).
//
// aad mirrors [NewChunkReader]: the chunk's position binding, derived
// from the owning manifest's RECORDED FormatVersion
// ([irbackup.ChunkAAD]); a bound chunk opened with the wrong (or
// missing) aad fails the GCM auth check before any event is emitted.
func NewChangeChunkReader(src io.ReadCloser, expectedSHA256 string, cek []byte, codec Codec, aad []byte) (*ChangeChunkReader, error) {
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
	if cek == nil && aad != nil {
		return nil, errors.New("change chunk reader: aad supplied without a cek (plaintext chunks carry no position binding)")
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
		pt, err := crypto.DecryptChunkWithAAD(ct, cek, aad)
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
	// The SAME constant the writer refuses on — see [MaxChunkLineBytes].
	sc.Buffer(make([]byte, 0, 64*1024), MaxChunkLineBytes)
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

// PreserveNumbers makes every subsequent [ChangeChunkReader.ReadChange]
// decode a bare JSON number — a row value's top level, a list or map
// envelope's elements, a naturally-decoded JSON structure's leaves — as a
// [json.Number] carrying its exact text, instead of a float64.
//
// The chunk bytes already hold the exact digits: [encodeValue] has no
// json.Number arm, so the value's own marshaller writes its literal text.
// What lost them was the float64 decode. A postgres-trigger change stream
// hands the codec json.Number for every non-integer numeric, every
// numeric[] element and every jsonb number leaf (its reader keeps them
// exact on purpose — see pgtrigger's normalizePayloadValue), so a chain
// restored `123456789012345678.123456789012` as `123456789012345680` at
// exit 0, and a jsonb `12345678901234567890` as `12345678901234567000`.
//
// It is opt-in per chunk, chosen by the caller from the owning manifest
// ([NumbersArePreserved]), not the default, because the rule changes a
// decoded value's Go type: a float64 a CDC reader produced (every other
// engine's Float column) would come back as json.Number. On a
// postgres-trigger chain that is exactly the type its live change stream
// hands every consumer, so restore, the broker and compaction see what
// the live path sees. Existing chains are repaired by this alone: their
// chunks were always written this way.
func (r *ChangeChunkReader) PreserveNumbers() { r.preserveNumbers = true }

// NumbersArePreserved reports whether change chunks written for a source
// engine must be read with [ChangeChunkReader.PreserveNumbers]. True for
// postgres-trigger only.
//
// Premise, pinned by TestNumbersArePreserved_PGTriggerCarriesNoFloats in
// the pgtrigger package: that engine's change stream carries no float64 or
// float32 values — integers become int64, every other number stays
// json.Number — so every bare number in its change chunks is a json.Number
// the reader produced. A change chunk can still carry a float64 from the
// ADD COLUMN fill (captured through the row reader, not the change
// stream); it round-trips exactly as json.Number too (its text is the
// shortest float64 rendering) and every writer accepts json.Number for a
// float column, as the live path requires.
func NumbersArePreserved(sourceEngine string) bool {
	return sourceEngine == PreservedNumberEngine
}

// CarriedExactNumber reports whether any change this writer encoded carried
// a [json.Number] — at the top level of a row image or inside a list or map
// value. It is what the capture lanes read to stamp their segment
// [irbackup.FormatVersionExactNumbers], so an older binary, whose reader
// would round the number through a float64, refuses the segment instead.
func (w *ChangeChunkWriter) CarriedExactNumber() bool { return w.carriedExactNumber }

// changeCarriesJSONNumber reports whether a row-bearing change holds a
// [json.Number] anywhere in its images.
func changeCarriesJSONNumber(c ir.Change) bool {
	switch x := c.(type) {
	case ir.Insert:
		return rowCarriesJSONNumber(x.Row)
	case ir.Update:
		return rowCarriesJSONNumber(x.Before) || rowCarriesJSONNumber(x.After)
	case ir.Delete:
		return rowCarriesJSONNumber(x.Before)
	}
	return false
}

func rowCarriesJSONNumber(r ir.Row) bool {
	for _, v := range r {
		if valueCarriesJSONNumber(v) {
			return true
		}
	}
	return false
}

// valueCarriesJSONNumber walks exactly the containers [encodeValue] walks
// (and the natural JSON structures a reader may hand over): a json.Number
// anywhere below them is an exact-text number in the chunk — unless a
// float64 carries it exactly (see [float64RoundTripsExactly]), in which
// case a pre-v0.156.4 reader restores it byte-for-byte and stamping the
// segment would lock older binaries out of it for nothing. That exemption
// is what keeps the common shape — a jsonb column holding small integers,
// which the reader leaves as json.Number inside the object — readable
// everywhere.
func valueCarriesJSONNumber(v any) bool {
	switch x := v.(type) {
	case json.Number:
		return !float64RoundTripsExactly(x)
	case []any:
		for _, e := range x {
			if valueCarriesJSONNumber(e) {
				return true
			}
		}
	case map[string]any:
		for _, e := range x {
			if valueCarriesJSONNumber(e) {
				return true
			}
		}
	}
	return false
}

// maxExactIntegerDigits bounds the integers [float64RoundTripsExactly]
// accepts: every integer of at most 15 digits is below 2^53, so float64
// holds it exactly, and encoding/json renders an integral float64 below
// 1e21 as its plain digits — the same text the chunk carried.
const maxExactIntegerDigits = 15

// float64RoundTripsExactly reports whether an older reader's float64
// decode of n renders back to n's exact text: a plain integer (optional
// '-', no leading zero, at most [maxExactIntegerDigits] digits). "-0" is
// excluded — float64 keeps the sign but the reader that produced n kept
// it as text on purpose. Anything with a point or an exponent is not
// exempt, whatever its value: that is the class the tier exists for.
func float64RoundTripsExactly(n json.Number) bool {
	s := strings.TrimPrefix(n.String(), "-")
	if s == "" || len(s) > maxExactIntegerDigits || (s[0] == '0' && (len(s) > 1 || n.String() != "0")) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// PreservedNumberEngine is the one source engine whose change chunks are
// read with [ChangeChunkReader.PreserveNumbers]. Kept as a literal here
// (blobcodec imports no engine package) and bound to
// pgtrigger.EngineName by the pgtrigger package's test.
const PreservedNumberEngine = "postgres-trigger"

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
	c, err := decodeChange(&wire, r.preserveNumbers)
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
		row, err := encodeRowValues(x.Row, "row")
		if err != nil {
			return nil, changeTableErr(x.Schema, x.Table, err)
		}
		return &changeWire{
			Kind:     changeKindInsert,
			Schema:   x.Schema,
			Table:    x.Table,
			Row:      row,
			Position: x.Position,
		}, nil
	case ir.Update:
		before, err := encodeRowValues(x.Before, "before-image")
		if err != nil {
			return nil, changeTableErr(x.Schema, x.Table, err)
		}
		after, err := encodeRowValues(x.After, "after-image")
		if err != nil {
			return nil, changeTableErr(x.Schema, x.Table, err)
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
		before, err := encodeRowValues(x.Before, "before-image")
		if err != nil {
			return nil, changeTableErr(x.Schema, x.Table, err)
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
func decodeChange(w *changeWire, numbersExact bool) (ir.Change, error) {
	if w == nil {
		return nil, errors.New("decode change: nil wire")
	}
	switch w.Kind {
	case changeKindInsert:
		row, err := decodeRowValues(w.Row, numbersExact)
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
		before, err := decodeRowValues(w.Before, numbersExact)
		if err != nil {
			return nil, err
		}
		after, err := decodeRowValues(w.After, numbersExact)
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
		before, err := decodeRowValues(w.Before, numbersExact)
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
func encodeRowValues(r ir.Row, role string) (map[string]json.RawMessage, error) {
	if r == nil {
		return nil, nil
	}
	out := make(map[string]json.RawMessage, len(r))
	for k, v := range r {
		// encodeValue's JSON would write a non-UTF-8 string as U+FFFD
		// (backup_value_utf8.go); refuse it with the column named.
		if err := refuseNonUTF8Value(k, role, v); err != nil {
			return nil, err
		}
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
func decodeRowValues(m map[string]json.RawMessage, numbersExact bool) (ir.Row, error) {
	if m == nil {
		return nil, nil
	}
	out := make(ir.Row, len(m))
	for k, v := range m {
		dec, err := decodeValueWith(v, numbersExact)
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
