// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// pgHstoreBinaryCodec implements [pgtype.Codec] for the `hstore` type
// shipped by the hstore contrib extension. Mirrors the structural
// pattern of [pgvectorBinaryCodec] (v0.26.0): the extension does not
// register itself with pgx, and without this codec pgx's COPY FROM
// STDIN BINARY path silently routes hstore values through the `text`
// codec — i.e. it ships the canonical text form
// `"k"=>"v"` as raw UTF-8 bytes inside a binary-format column,
// where Postgres' `hstore_recv()` parser interprets the first four
// bytes as a big-endian pair-count and refuses with
// "incorrect binary data format" (or returns garbage for hstore-shaped
// text whose first four bytes happen to deserialize as a valid count).
//
// hstore's binary wire format (matches `hstore_send` /
// `hstore_recv` in contrib/hstore/hstore_io.c):
//
//	int32 BE  nentries        — number of key/value pairs (0 == empty)
//	for each pair:
//	  int32 BE  keylen        — length of key string (must be >= 0)
//	  bytes     key           — UTF-8 key bytes (no null terminator)
//	  int32 BE  vallen        — length of value string, OR -1 if NULL
//	  bytes     value         — UTF-8 value bytes (omitted when vallen == -1)
//
// Encoding accepts the canonical PG hstore text form (what the IR
// carries from the source reader for the cross-engine and same-engine
// passthrough paths — `"k"=>"v"`, `=>` separator, comma-comma-space
// separator between pairs, unquoted `NULL` keyword for nil values) as
// either `string` or `[]byte`. Other shapes return a clear error so a
// translator bug surfaces loudly.
//
// Decoding is implemented for completeness — symmetric round-trip in
// unit tests, and to make the registered codec drive both directions
// if a future code path scans into a target other than `*any`. The
// row reader's existing scan-into-`*any` path doesn't go through
// PlanScan, so registering this codec doesn't change read behaviour
// for v0.32.1.
type pgHstoreBinaryCodec struct{}

func (pgHstoreBinaryCodec) FormatSupported(format int16) bool {
	return format == pgtype.BinaryFormatCode || format == pgtype.TextFormatCode
}

// PreferredFormat reports binary as the preferred wire format. pgx's
// CopyFrom always uses binary, so this is the load-bearing case.
func (pgHstoreBinaryCodec) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

// PlanEncode dispatches by (format, value-type). Binary path emits the
// hstore wire format; text path passes the canonical text through.
func (pgHstoreBinaryCodec) PlanEncode(_ *pgtype.Map, _ uint32, format int16, value any) pgtype.EncodePlan {
	switch format {
	case pgtype.BinaryFormatCode:
		switch value.(type) {
		case string:
			return encodePlanHstoreBinaryString{}
		case []byte:
			return encodePlanHstoreBinaryBytes{}
		}
	case pgtype.TextFormatCode:
		switch value.(type) {
		case string:
			return encodePlanHstoreTextString{}
		case []byte:
			return encodePlanHstoreTextBytes{}
		}
	}
	return nil
}

// PlanScan and the Decode* methods are not exercised in v0.32.1's
// hot path (the row reader scans into `*any`, which routes through
// pgx's default-OID-unknown text path). They return nil / an error so
// future use of typed scan targets surfaces a clear "not implemented"
// rather than silently doing the wrong thing.
func (pgHstoreBinaryCodec) PlanScan(_ *pgtype.Map, _ uint32, _ int16, _ any) pgtype.ScanPlan {
	return nil
}

func (pgHstoreBinaryCodec) DecodeDatabaseSQLValue(_ *pgtype.Map, _ uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}
	switch format {
	case pgtype.TextFormatCode:
		return string(src), nil
	case pgtype.BinaryFormatCode:
		// driver.Value carries the canonical text form so SQL
		// scanners get something portable.
		return decodeHstoreBinary(src)
	}
	return nil, fmt.Errorf("postgres: hstore codec: unsupported scan format %d", format)
}

func (pgHstoreBinaryCodec) DecodeValue(_ *pgtype.Map, _ uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	switch format {
	case pgtype.TextFormatCode:
		return string(src), nil
	case pgtype.BinaryFormatCode:
		return decodeHstoreBinary(src)
	}
	return nil, fmt.Errorf("postgres: hstore codec: unsupported decode format %d", format)
}

// ---------- encode plans (binary) ----------

type encodePlanHstoreBinaryString struct{}

func (encodePlanHstoreBinaryString) Encode(value any, buf []byte) ([]byte, error) {
	return appendHstoreBinaryFromText(buf, value.(string))
}

type encodePlanHstoreBinaryBytes struct{}

func (encodePlanHstoreBinaryBytes) Encode(value any, buf []byte) ([]byte, error) {
	return appendHstoreBinaryFromText(buf, string(value.([]byte)))
}

// ---------- encode plans (text) ----------

type encodePlanHstoreTextString struct{}

func (encodePlanHstoreTextString) Encode(value any, buf []byte) ([]byte, error) {
	return append(buf, value.(string)...), nil
}

type encodePlanHstoreTextBytes struct{}

func (encodePlanHstoreTextBytes) Encode(value any, buf []byte) ([]byte, error) {
	return append(buf, value.([]byte)...), nil
}

// ---------- helpers ----------

// hstorePair carries one parsed key/value pair from the canonical text
// form. nilValue == true encodes NULL on the wire (vallen = -1, no
// value bytes); the Value field is unused in that case. PG hstore
// disallows NULL keys on the server side, so we don't represent that
// shape here.
type hstorePair struct {
	Key      string
	Value    string
	NilValue bool
}

// appendHstoreBinaryFromText parses the canonical hstore text form
// and appends the binary wire encoding to buf.
func appendHstoreBinaryFromText(buf []byte, s string) ([]byte, error) {
	pairs, err := parseHstoreTextPG(s)
	if err != nil {
		return nil, err
	}
	return appendHstoreBinaryFromPairs(buf, pairs), nil
}

// appendHstoreBinaryFromPairs emits the hstore binary wire form:
// int32 BE pair-count, then for each pair int32 BE keylen, key
// bytes, int32 BE vallen (-1 for NULL), value bytes.
func appendHstoreBinaryFromPairs(buf []byte, pairs []hstorePair) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(pairs)))
	buf = append(buf, hdr[:]...)
	for _, p := range pairs {
		var kl [4]byte
		binary.BigEndian.PutUint32(kl[:], uint32(len(p.Key)))
		buf = append(buf, kl[:]...)
		buf = append(buf, p.Key...)
		var vl [4]byte
		if p.NilValue {
			// -1 as int32 BE is 0xFFFFFFFF.
			binary.BigEndian.PutUint32(vl[:], 0xFFFFFFFF)
			buf = append(buf, vl[:]...)
			continue
		}
		binary.BigEndian.PutUint32(vl[:], uint32(len(p.Value)))
		buf = append(buf, vl[:]...)
		buf = append(buf, p.Value...)
	}
	return buf
}

// decodeHstoreBinary parses the hstore binary wire form back into the
// canonical text form (`"k"=>"v", "k2"=>NULL`). Used by DecodeValue /
// DecodeDatabaseSQLValue for symmetric round-trip in unit tests and
// future typed-scan paths; the row reader's scan-into-`*any` path
// doesn't reach here in v0.32.1.
func decodeHstoreBinary(src []byte) (string, error) {
	if len(src) < 4 {
		return "", fmt.Errorf(
			"postgres: hstore decode: header truncated (got %d bytes, want >= 4)", len(src))
	}
	n := int32(binary.BigEndian.Uint32(src[0:4]))
	if n < 0 {
		return "", fmt.Errorf("postgres: hstore decode: negative pair count %d", n)
	}
	off := 4
	var b strings.Builder
	for i := int32(0); i < n; i++ {
		if off+4 > len(src) {
			return "", fmt.Errorf(
				"postgres: hstore decode: truncated keylen at pair %d (offset %d, len %d)",
				i, off, len(src))
		}
		kl := int32(binary.BigEndian.Uint32(src[off : off+4]))
		off += 4
		if kl < 0 {
			return "", fmt.Errorf(
				"postgres: hstore decode: negative keylen %d at pair %d", kl, i)
		}
		if off+int(kl) > len(src) {
			return "", fmt.Errorf(
				"postgres: hstore decode: truncated key at pair %d (need %d, have %d)",
				i, kl, len(src)-off)
		}
		key := string(src[off : off+int(kl)])
		off += int(kl)
		if off+4 > len(src) {
			return "", fmt.Errorf(
				"postgres: hstore decode: truncated vallen at pair %d (offset %d, len %d)",
				i, off, len(src))
		}
		vl := int32(binary.BigEndian.Uint32(src[off : off+4]))
		off += 4
		if i > 0 {
			b.WriteString(", ")
		}
		writeHstoreQuoted(&b, key)
		b.WriteString("=>")
		if vl == -1 {
			b.WriteString("NULL")
			continue
		}
		if vl < 0 {
			return "", fmt.Errorf(
				"postgres: hstore decode: negative vallen %d (not -1) at pair %d", vl, i)
		}
		if off+int(vl) > len(src) {
			return "", fmt.Errorf(
				"postgres: hstore decode: truncated value at pair %d (need %d, have %d)",
				i, vl, len(src)-off)
		}
		writeHstoreQuoted(&b, string(src[off:off+int(vl)]))
		off += int(vl)
	}
	if off != len(src) {
		return "", fmt.Errorf(
			"postgres: hstore decode: trailing %d byte(s) after %d pairs", len(src)-off, n)
	}
	return b.String(), nil
}

// writeHstoreQuoted writes a `"..."` literal with backslash-escaped
// interior quotes / backslashes — the inverse of the parser's
// `readQuoted` step.
func writeHstoreQuoted(b *strings.Builder, s string) {
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	b.WriteByte('"')
}

// parseHstoreTextPG parses a PG hstore canonical text representation
// into an ordered slice of [hstorePair]. The grammar mirrors the
// shared parser in `internal/engines/mysql/row_writer.go::parseHstoreText`
// — duplicated here rather than imported to keep the IR-first tenet's
// "no cross-engine imports" rule. The MySQL-side parser returns a
// `map[string]any` (last-write-wins semantics matching PG's `hstore_in`);
// the binary codec needs ordered pairs so the round-trip is stable and
// the wire encoding doesn't reshuffle pair order. The two parsers
// share the same grammar and should be updated together.
//
// Grammar (per PG's "hstore Input and Output" docs):
//
//   - Each key/value pair is `"key"=>"value"` with double-quoted
//     strings on both sides.
//   - Pairs are separated by `, ` (comma + optional whitespace).
//   - Interior quotes are backslash-escaped (`\"` is a literal quote
//     in the key/value); literal backslashes are `\\`.
//   - The unquoted keyword `NULL` (case-insensitive) on the value side
//     is the SQL null marker. Keys cannot be NULL — PG hstore enforces
//     that on insert.
//   - The empty hstore is the empty string `""` (no braces).
func parseHstoreTextPG(s string) ([]hstorePair, error) {
	out := make([]hstorePair, 0)
	i := 0
	skipSpace := func() {
		for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
	}
	readQuoted := func() (string, error) {
		if i >= len(s) || s[i] != '"' {
			return "", fmt.Errorf("hstore: expected '\"' at offset %d in %q", i, s)
		}
		i++
		var sb []byte
		for i < len(s) {
			c := s[i]
			if c == '\\' && i+1 < len(s) {
				sb = append(sb, s[i+1])
				i += 2
				continue
			}
			if c == '"' {
				i++
				return string(sb), nil
			}
			sb = append(sb, c)
			i++
		}
		return "", fmt.Errorf("hstore: unterminated quoted string in %q", s)
	}
	for {
		skipSpace()
		if i >= len(s) {
			break
		}
		key, err := readQuoted()
		if err != nil {
			return nil, err
		}
		skipSpace()
		if i+1 >= len(s) || s[i] != '=' || s[i+1] != '>' {
			return nil, fmt.Errorf("hstore: expected '=>' at offset %d in %q", i, s)
		}
		i += 2
		skipSpace()
		switch {
		case i < len(s) && s[i] == '"':
			val, err := readQuoted()
			if err != nil {
				return nil, err
			}
			out = append(out, hstorePair{Key: key, Value: val})
		case i+4 <= len(s) && strings.EqualFold(s[i:i+4], "NULL"):
			out = append(out, hstorePair{Key: key, NilValue: true})
			i += 4
		default:
			return nil, fmt.Errorf("hstore: expected '\"' or NULL at offset %d in %q", i, s)
		}
		skipSpace()
		if i >= len(s) {
			break
		}
		if s[i] != ',' {
			return nil, fmt.Errorf("hstore: expected ',' or end at offset %d in %q", i, s)
		}
		i++
	}
	return out, nil
}

// ---------- OID lookup + registration ----------

// lookupHstoreOID queries the connected database for the OID assigned
// to the `hstore` type. Extension OIDs are dynamic (assigned at
// CREATE EXTENSION time), so the value must come from the catalog
// rather than a compile-time constant. Returns errHstoreTypeNotFound
// when the extension isn't installed (caller treats as "no codec to
// register, table has no hstore columns" — but the schema preflight
// should already have refused before reaching this path).
func lookupHstoreOID(ctx context.Context, q interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
},
) (uint32, error) {
	const sqlText = `SELECT oid FROM pg_type WHERE typname = 'hstore' LIMIT 1`
	var oid uint32
	switch err := q.QueryRowContext(ctx, sqlText).Scan(&oid); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, errHstoreTypeNotFound
	case err != nil:
		return 0, fmt.Errorf("postgres: lookup hstore OID: %w", err)
	}
	if oid == 0 {
		return 0, errHstoreTypeNotFound
	}
	return oid, nil
}

// errHstoreTypeNotFound signals that pg_type contains no row for
// `hstore`. The hstore extension isn't installed; callers either
// short-circuit codec registration (no hstore columns to ship) or
// surface a clear refusal.
var errHstoreTypeNotFound = errors.New(
	"postgres: hstore type not found in pg_type — extension not installed?")
