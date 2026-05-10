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
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// pgvectorBinaryCodec implements [pgtype.Codec] for the `vector` type
// shipped by the pgvector extension. The extension does not register
// itself with pgx; without this codec, pgx's COPY FROM STDIN BINARY
// (the default high-throughput bulk-load path) silently routes vector
// values through the `text` codec — i.e. it ships the canonical text
// form `[0.1,0.2,0.3]` as raw UTF-8 bytes inside a binary-format
// column, where Postgres' `vector_in()` parser interprets the first
// two bytes as a big-endian dimension count and rejects with
// "vector cannot have more than 16000 dimensions" (Bug 47).
//
// pgvector's binary wire format (matches `vector_recv` / `vector_send`
// in pgvector/src/vector.c):
//
//	int16 dim       — big-endian, number of float32 components
//	int16 unused    — big-endian, always 0
//	float32 × dim   — big-endian IEEE 754 components
//
// Encoding accepts the canonical text form ("[1,2,3]" — what pgx's
// stdlib mode hands the row reader for an unregistered OID) as either
// `string` or `[]byte`. Other shapes return a clear error so a
// translator bug surfaces loudly.
//
// Decoding is implemented for completeness — symmetric round-trip in
// unit tests, and to make the registered codec drive both directions
// if a future code path scans into a target other than `*any`. The
// row reader's existing scan-into-`*any` path doesn't go through
// PlanScan, so registering this codec doesn't change read behaviour
// for v0.26.1.
type pgvectorBinaryCodec struct{}

func (pgvectorBinaryCodec) FormatSupported(format int16) bool {
	return format == pgtype.BinaryFormatCode || format == pgtype.TextFormatCode
}

// PreferredFormat reports binary as the preferred wire format. pgx's
// CopyFrom always uses binary, so this is the load-bearing case.
func (pgvectorBinaryCodec) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

// PlanEncode dispatches by (format, value-type). Binary path emits the
// pgvector wire format; text path passes the canonical text through.
func (pgvectorBinaryCodec) PlanEncode(_ *pgtype.Map, _ uint32, format int16, value any) pgtype.EncodePlan {
	switch format {
	case pgtype.BinaryFormatCode:
		switch value.(type) {
		case string:
			return encodePlanVectorBinaryString{}
		case []byte:
			return encodePlanVectorBinaryBytes{}
		case []float32:
			return encodePlanVectorBinaryFloat32{}
		case []float64:
			return encodePlanVectorBinaryFloat64{}
		}
	case pgtype.TextFormatCode:
		switch value.(type) {
		case string:
			return encodePlanVectorTextString{}
		case []byte:
			return encodePlanVectorTextBytes{}
		case []float32:
			return encodePlanVectorTextFloat32{}
		case []float64:
			return encodePlanVectorTextFloat64{}
		}
	}
	return nil
}

// PlanScan and the Decode* methods are not exercised in v0.26.1's
// hot path (the row reader scans into `*any`, which routes through
// pgx's default-OID-unknown text path). They return nil / an error so
// future use of typed scan targets surfaces a clear "not implemented"
// rather than silently doing the wrong thing.
func (pgvectorBinaryCodec) PlanScan(_ *pgtype.Map, _ uint32, _ int16, _ any) pgtype.ScanPlan {
	return nil
}

func (pgvectorBinaryCodec) DecodeDatabaseSQLValue(_ *pgtype.Map, _ uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}
	switch format {
	case pgtype.TextFormatCode:
		return string(src), nil
	case pgtype.BinaryFormatCode:
		// driver.Value can't carry []float32 directly; surface the
		// canonical text form so SQL scanners get something portable.
		floats, err := decodeVectorBinary(src)
		if err != nil {
			return nil, err
		}
		return string(appendVectorTextFromFloat32(nil, floats)), nil
	}
	return nil, fmt.Errorf("postgres: pgvector codec: unsupported scan format %d", format)
}

func (pgvectorBinaryCodec) DecodeValue(_ *pgtype.Map, _ uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	switch format {
	case pgtype.TextFormatCode:
		return string(src), nil
	case pgtype.BinaryFormatCode:
		return decodeVectorBinary(src)
	}
	return nil, fmt.Errorf("postgres: pgvector codec: unsupported decode format %d", format)
}

// ---------- encode plans (binary) ----------

type encodePlanVectorBinaryString struct{}

func (encodePlanVectorBinaryString) Encode(value any, buf []byte) ([]byte, error) {
	return appendVectorBinaryFromText(buf, value.(string))
}

type encodePlanVectorBinaryBytes struct{}

func (encodePlanVectorBinaryBytes) Encode(value any, buf []byte) ([]byte, error) {
	return appendVectorBinaryFromText(buf, string(value.([]byte)))
}

type encodePlanVectorBinaryFloat32 struct{}

func (encodePlanVectorBinaryFloat32) Encode(value any, buf []byte) ([]byte, error) {
	v := value.([]float32)
	return appendVectorBinaryFromFloat32(buf, v)
}

type encodePlanVectorBinaryFloat64 struct{}

func (encodePlanVectorBinaryFloat64) Encode(value any, buf []byte) ([]byte, error) {
	v := value.([]float64)
	out := make([]float32, len(v))
	for i, f := range v {
		out[i] = float32(f)
	}
	return appendVectorBinaryFromFloat32(buf, out)
}

// ---------- encode plans (text) ----------

type encodePlanVectorTextString struct{}

func (encodePlanVectorTextString) Encode(value any, buf []byte) ([]byte, error) {
	return append(buf, value.(string)...), nil
}

type encodePlanVectorTextBytes struct{}

func (encodePlanVectorTextBytes) Encode(value any, buf []byte) ([]byte, error) {
	return append(buf, value.([]byte)...), nil
}

type encodePlanVectorTextFloat32 struct{}

func (encodePlanVectorTextFloat32) Encode(value any, buf []byte) ([]byte, error) {
	return appendVectorTextFromFloat32(buf, value.([]float32)), nil
}

type encodePlanVectorTextFloat64 struct{}

func (encodePlanVectorTextFloat64) Encode(value any, buf []byte) ([]byte, error) {
	v := value.([]float64)
	out := make([]float32, len(v))
	for i, f := range v {
		out[i] = float32(f)
	}
	return appendVectorTextFromFloat32(buf, out), nil
}

// ---------- helpers ----------

// appendVectorBinaryFromText parses pgvector's canonical text form and
// appends the binary wire encoding to buf. The text form is
// `[c1,c2,...,cN]` with optional whitespace between tokens.
func appendVectorBinaryFromText(buf []byte, s string) ([]byte, error) {
	floats, err := parseVectorText(s)
	if err != nil {
		return nil, err
	}
	return appendVectorBinaryFromFloat32(buf, floats)
}

// appendVectorBinaryFromFloat32 emits the pgvector binary wire form:
// dim (BE int16), unused (BE int16, 0), then dim BE float32 components.
func appendVectorBinaryFromFloat32(buf []byte, v []float32) ([]byte, error) {
	if len(v) > math.MaxInt16 {
		return nil, fmt.Errorf(
			"postgres: pgvector encode: dim %d exceeds int16 max", len(v))
	}
	header := [4]byte{}
	binary.BigEndian.PutUint16(header[0:2], uint16(len(v)))
	// unused word stays zero
	buf = append(buf, header[:]...)
	for _, f := range v {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], math.Float32bits(f))
		buf = append(buf, b[:]...)
	}
	return buf, nil
}

// appendVectorTextFromFloat32 renders pgvector's canonical text form
// for a float32 slice: "[c1,c2,...,cN]" with no spaces.
func appendVectorTextFromFloat32(buf []byte, v []float32) []byte {
	buf = append(buf, '[')
	for i, f := range v {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = strconv.AppendFloat(buf, float64(f), 'f', -1, 32)
	}
	buf = append(buf, ']')
	return buf
}

// parseVectorText decodes pgvector canonical text into a []float32.
// Accepts surrounding/internal whitespace and an empty body ("[]" → 0
// components, which pgvector itself rejects but we leave to the
// server to enforce so the codec doesn't double-up the validation).
func parseVectorText(s string) ([]float32, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return nil, fmt.Errorf(
			"postgres: pgvector parse: malformed text %q (missing brackets)", s)
	}
	body := strings.TrimSpace(s[1 : len(s)-1])
	if body == "" {
		return []float32{}, nil
	}
	parts := strings.Split(body, ",")
	out := make([]float32, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		f, err := strconv.ParseFloat(p, 32)
		if err != nil {
			return nil, fmt.Errorf(
				"postgres: pgvector parse: component %d %q: %w", i, p, err)
		}
		out[i] = float32(f)
	}
	return out, nil
}

// decodeVectorBinary parses pgvector binary wire form into []float32.
// Used by DecodeValue / DecodeDatabaseSQLValue for symmetry with the
// encode plans; the row reader's scan-into-`*any` path doesn't reach
// here in v0.26.1 (pgx's stdlib mode requests text format for the
// `*any` scan target).
func decodeVectorBinary(src []byte) ([]float32, error) {
	if len(src) < 4 {
		return nil, fmt.Errorf(
			"postgres: pgvector decode: header truncated (got %d bytes, want >= 4)", len(src))
	}
	dim := int(binary.BigEndian.Uint16(src[0:2]))
	// src[2:4] is the unused word; we don't validate it (pgvector itself
	// doesn't require zero on the receive side beyond cosmetic checks).
	expected := 4 + dim*4
	if len(src) != expected {
		return nil, fmt.Errorf(
			"postgres: pgvector decode: payload length %d does not match dim=%d (want %d)",
			len(src), dim, expected)
	}
	out := make([]float32, dim)
	for i := 0; i < dim; i++ {
		off := 4 + i*4
		out[i] = math.Float32frombits(binary.BigEndian.Uint32(src[off : off+4]))
	}
	return out, nil
}

// ---------- OID lookup + registration ----------

// lookupVectorOID queries the connected database for the OID assigned
// to the `vector` type. Extension OIDs are dynamic (assigned at
// CREATE EXTENSION time), so the value must come from the catalog
// rather than a compile-time constant. Returns ErrVectorTypeNotFound
// when the extension isn't installed (caller treats as "no codec to
// register, table has no vector columns" — but the schema preflight
// should already have refused before reaching this path).
func lookupVectorOID(ctx context.Context, q interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
},
) (uint32, error) {
	const sqlText = `SELECT oid FROM pg_type WHERE typname = 'vector' LIMIT 1`
	var oid uint32
	switch err := q.QueryRowContext(ctx, sqlText).Scan(&oid); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, errVectorTypeNotFound
	case err != nil:
		return 0, fmt.Errorf("postgres: lookup vector OID: %w", err)
	}
	if oid == 0 {
		return 0, errVectorTypeNotFound
	}
	return oid, nil
}

// errVectorTypeNotFound signals that pg_type contains no row for
// `vector`. The pgvector extension isn't installed; callers either
// short-circuit codec registration (no vector columns to ship) or
// surface a clear refusal.
var errVectorTypeNotFound = errors.New(
	"postgres: pgvector type not found in pg_type — extension not installed?")
