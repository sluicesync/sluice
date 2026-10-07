// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
)

// applyIDFamilies is every reader's TxID alphabet (ADR-0190 phases 1, 4, 5;
// ADR-0191 §3.1) × the Seq values that break a float64 or a signed integer:
// the shapes the codec must carry byte-exact, because a mark is trusted only
// when a re-read identity equals the one written.
var applyIDFamilies = []struct {
	name string
	id   ir.ApplyID
}{
	{"mysql GTID uuid:n", ir.ApplyID{TxID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:23", Seq: 1}},
	{"mariadb domain-server-seqno", ir.ApplyID{TxID: "0-1-4711", Seq: 2}},
	{"mysql file/pos", ir.ApplyID{TxID: "filepos:3e11fa47-71ca-11e1-9e33-c80aa9429562:binlog.000017:4096", Seq: 3}},
	{"postgres sysid:timeline:lsn", ir.ApplyID{TxID: "pg:7426483021783401234:1:16/B374D848", Seq: 4}},
	{"vstream keyspace/shard:gtid", ir.ApplyID{TxID: "vstream:commerce/-80:MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77", Seq: 5}},
	{"postgres-trigger id:txid", ir.ApplyID{TxID: "postgres-trigger:918273:4294967296123", Seq: 1}},
	{"sqlite-trigger id:captured_at", ir.ApplyID{TxID: "sqlite-trigger:42:2026-10-06 12:34:56.789", Seq: 1}},
	{"non-ASCII and JSON-special stamp", ir.ApplyID{TxID: "sqlite-trigger:43:2026-10-06T12:34:56.789 \"Zürich\" \\   日本", Seq: 1}},
	{"seq above 2^53", ir.ApplyID{TxID: "pg:1:1:0/1", Seq: 1<<53 + 1}},
	{"seq above MaxInt64", ir.ApplyID{TxID: "pg:1:1:0/2", Seq: math.MaxInt64 + 1}},
	{"seq MaxUint64", ir.ApplyID{TxID: "pg:1:1:0/3", Seq: math.MaxUint64}},
	{"seq zero with a transaction", ir.ApplyID{TxID: "0-1-1", Seq: 0}},
}

// applyIDChunkModes are the chunk shapes the identity must survive: every
// codec, and the encrypted form.
var applyIDChunkModes = []struct {
	name  string
	codec Codec
	cek   bool
}{
	{"gzip", CodecGzip, false},
	{"zstd", CodecZstd, false},
	{"none", CodecNone, false},
	{"gzip encrypted", CodecGzip, true},
}

// rowChangesWith builds one change of every row kind carrying id, framed by
// a transaction, plus the identity-less kinds around them.
func rowChangesWith(id ir.ApplyID) []ir.Change {
	pos := ir.Position{Engine: "postgres", Token: `{"lsn":"0/10"}`}
	return []ir.Change{
		ir.TxBegin{Position: pos},
		ir.Insert{Position: pos, Schema: "public", Table: "t", Row: ir.Row{"id": int64(1), "v": "a"}, ApplyID: id},
		ir.Update{Position: pos, Schema: "public", Table: "t", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2), "v": "b"}, ApplyID: ir.ApplyID{TxID: id.TxID, Seq: id.Seq + 1}},
		ir.Delete{Position: pos, Schema: "public", Table: "t", Before: ir.Row{"id": int64(2)}, ApplyID: ir.ApplyID{TxID: id.TxID, Seq: id.Seq + 2}},
		ir.Truncate{Position: pos, Schema: "public", Table: "u"},
		ir.TxCommit{Position: pos},
	}
}

func writeChangeChunk(t *testing.T, changes []ir.Change, codec Codec, cek []byte) (body []byte, hash string) {
	t.Helper()
	buf := &bytes.Buffer{}
	w, err := NewChangeChunkWriter(buf, cek, codec, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkWriter: %v", err)
	}
	for _, c := range changes {
		if err := w.WriteChange(c); err != nil {
			t.Fatalf("WriteChange(%T): %v", c, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes(), w.Hash()
}

func readChangeChunk(t *testing.T, body []byte, hash string, codec Codec, cek []byte) []ir.Change {
	t.Helper()
	r, err := NewChangeChunkReader(nopReadCloserFromBytes(body), hash, cek, codec, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkReader: %v", err)
	}
	var out []ir.Change
	for {
		c, err := r.ReadChange()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadChange: %v", err)
		}
		out = append(out, c)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("reader Close: %v", err)
	}
	return out
}

// TestChangeChunk_ApplyIDRoundTrip is ADR-0191 §9 P1: the reader's identity
// survives the change chunk exactly, for every reader's TxID alphabet, an
// ordinal above 2^53 and above MaxInt64, every codec and the encrypted form,
// on every row kind — and a chunk that carries none decodes to the zero
// identity. Two checks, independent of each other: the decoded identity
// equals the one written (value), and re-encoding the decoded change yields
// the very bytes the first encode produced (byte-exact).
func TestChangeChunk_ApplyIDRoundTrip(t *testing.T) {
	cek := make([]byte, crypto.CEKLen)
	for i := range cek {
		cek[i] = byte(i + 7)
	}
	for _, mode := range applyIDChunkModes {
		var key []byte
		if mode.cek {
			key = cek
		}
		for _, fam := range applyIDFamilies {
			t.Run(mode.name+"/"+fam.name, func(t *testing.T) {
				in := rowChangesWith(fam.id)
				body, hash := writeChangeChunk(t, in, mode.codec, key)
				out := readChangeChunk(t, body, hash, mode.codec, key)
				if len(out) != len(in) {
					t.Fatalf("read %d changes, wrote %d", len(out), len(in))
				}
				for i := range in {
					want, got := ir.ApplyIDOf(in[i]), ir.ApplyIDOf(out[i])
					if want != got {
						t.Errorf("change %d (%T): identity %+v read back as %+v", i, in[i], want, got)
					}
					w1, err := encodeChange(in[i])
					if err != nil {
						t.Fatal(err)
					}
					w2, err := encodeChange(out[i])
					if err != nil {
						t.Fatal(err)
					}
					b1, _ := json.Marshal(w1)
					b2, _ := json.Marshal(w2)
					if !bytes.Equal(b1, b2) {
						t.Errorf("change %d (%T) is not byte-exact across a round trip:\n  wrote %s\n  re-encoded %s", i, in[i], b1, b2)
					}
				}
			})
		}
	}

	t.Run("absent aid decodes to the zero identity", func(t *testing.T) {
		in := rowChangesWith(ir.ApplyID{})
		body, hash := writeChangeChunk(t, in, CodecGzip, nil)
		if raw := decompressedLines(t, body); strings.Contains(raw, `"aid"`) {
			t.Fatalf("a change with no identity wrote an aid member:\n%s", raw)
		}
		for i, c := range readChangeChunk(t, body, hash, CodecGzip, nil) {
			if !ir.ApplyIDOf(c).IsZero() {
				t.Errorf("change %d (%T) carries %+v; want none", i, c, ir.ApplyIDOf(c))
			}
		}
	})
}

// TestChangeChunk_PreADR0191ChunkDecodesWithoutIdentity is the codec
// checklist's "a newer binary reading an old chain": a chunk line written
// exactly as v0.156.12 writes one (the record shape below has no `aid`;
// it is the wire the old changeWire marshals) decodes with zero identities,
// so the broker's keyless door still applies to it.
func TestChangeChunk_PreADR0191ChunkDecodesWithoutIdentity(t *testing.T) {
	lines := []string{
		`{"_t":"tx_begin","position":{"engine":"mysql","token":"x"}}`,
		`{"_t":"insert","schema":"s","table":"t","row":{"id":{"_t":"i64","v":7}},"position":{"engine":"mysql","token":"x"}}`,
		`{"_t":"update","schema":"s","table":"t","before":{"id":{"_t":"i64","v":7}},"after":{"id":{"_t":"i64","v":8}},"position":{"engine":"mysql","token":"x"}}`,
		`{"_t":"delete","schema":"s","table":"t","before":{"id":{"_t":"i64","v":8}},"position":{"engine":"mysql","token":"x"}}`,
		`{"_t":"tx_commit","position":{"engine":"mysql","token":"y"}}`,
	}
	for _, l := range lines {
		var w changeWire
		if err := json.Unmarshal([]byte(l), &w); err != nil {
			t.Fatalf("%s: %v", l, err)
		}
		c, err := decodeChange(&w, false)
		if err != nil {
			t.Fatalf("%s: %v", l, err)
		}
		if id := ir.ApplyIDOf(c); !id.IsZero() {
			t.Errorf("%s decoded with identity %+v; an old chain has none", l, id)
		}
	}
}

// changeWireV015611 is the record struct v0.156.11 (and v0.156.12) decodes
// every change-chunk line into — copied verbatim from
// `git show v0.156.11:internal/pipeline/blobcodec/backup_change_chunk.go`,
// NOT derived from today's struct, so this check is what the OLDER reader
// does rather than what this build agrees with itself about.
type changeWireV015611 struct {
	Kind     string                     `json:"_t"`
	Schema   string                     `json:"schema,omitempty"`
	Table    string                     `json:"table,omitempty"`
	Row      map[string]json.RawMessage `json:"row,omitempty"`
	Before   map[string]json.RawMessage `json:"before,omitempty"`
	After    map[string]json.RawMessage `json:"after,omitempty"`
	Position ir.Position                `json:"position"`
}

// TestChangeChunk_OlderReaderIgnoresTheIdentity is the codec checklist's
// "an older binary reading a new chain": every line this build writes with an
// identity decodes into v0.156.11's record struct the way v0.156.11's
// ReadChange decodes it (plain json.Unmarshal, no DisallowUnknownFields), with
// no error and every field it knows exactly as written — the `aid` member is
// skipped, so the older binary replays the change as it always did, with no
// identity. The behavioural cross-version run (a v0.156.12 binary restoring a
// chain this build wrote) is recorded in ADR-0191 §14.
func TestChangeChunk_OlderReaderIgnoresTheIdentity(t *testing.T) {
	for _, fam := range applyIDFamilies {
		for _, c := range rowChangesWith(fam.id) {
			w, err := encodeChange(c)
			if err != nil {
				t.Fatal(err)
			}
			line, _ := json.Marshal(w)
			var old changeWireV015611
			if err := json.Unmarshal(line, &old); err != nil {
				t.Fatalf("%s: the v0.156.11 record decode refused a new line %s: %v", fam.name, line, err)
			}
			// The fields the old reader knows must be exactly the new writer's.
			nw := changeWireV015611{Kind: w.Kind, Schema: w.Schema, Table: w.Table, Row: w.Row, Before: w.Before, After: w.After, Position: w.Position}
			b1, _ := json.Marshal(old)
			b2, _ := json.Marshal(nw)
			if !bytes.Equal(b1, b2) {
				t.Errorf("%s: the old reader decoded %s from %s; want %s", fam.name, b1, line, b2)
			}
		}
	}
}

// TestChangeChunk_ApplyIDRefusals: the identity is carried byte-exact or
// refused, never altered. A TxID that is not UTF-8 would be rewritten to U+FFFD
// by encoding/json, so the encode refuses it; a recorded identity with an empty
// transaction id, or one on a record kind that carries none, is no writer's
// output and refuses on decode rather than being read as "no identity".
func TestChangeChunk_ApplyIDRefusals(t *testing.T) {
	bad := ir.Insert{Table: "t", Row: ir.Row{"id": int64(1)}, ApplyID: ir.ApplyID{TxID: "sqlite-trigger:1:\xff\xfe", Seq: 1}}
	if _, err := encodeChange(bad); !errors.Is(err, ErrApplyIDNotUTF8) {
		t.Errorf("a non-UTF-8 transaction id encoded (err %v); encoding/json would have stored U+FFFD in its place", err)
	}
	for _, l := range []string{
		`{"_t":"insert","table":"t","row":{},"position":{},"aid":{"t":"","s":3}}`,
		`{"_t":"tx_commit","position":{},"aid":{"t":"pg:1:1:0/1","s":1}}`,
		`{"_t":"truncate","table":"t","position":{},"aid":{"t":"pg:1:1:0/1","s":1}}`,
	} {
		var w changeWire
		if err := json.Unmarshal([]byte(l), &w); err != nil {
			t.Fatal(err)
		}
		if c, err := decodeChange(&w, false); err == nil {
			t.Errorf("%s decoded as %+v; want a refusal", l, c)
		}
	}
}

// decompressedLines returns a plaintext gzip chunk's JSON lines.
func decompressedLines(t *testing.T, body []byte) string {
	t.Helper()
	r, err := newCodecReader(bytes.NewReader(body), CodecGzip)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var b strings.Builder
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		b.WriteString(sc.Text())
		b.WriteByte('\n')
	}
	return b.String()
}
