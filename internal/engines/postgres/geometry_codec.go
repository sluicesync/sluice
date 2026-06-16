// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// pgGeometryBinaryCodec implements [pgtype.Codec] for the PostGIS
// `geometry` type on the CDC applier connections.
//
// # Why this codec exists (the geometry-over-CDC gap)
//
// sluice carries an [ir.Geometry] value as WKB bytes; [prepareValue]
// wraps them in PostGIS EWKB framing (SRID-tagged) and hands the result
// to the writer as a `[]byte`. The COPY cold-start path ships that
// `[]byte` in COPY-BINARY format, which PostGIS `geometry_recv` accepts
// directly. The CDC applier, however, binds it as a query PARAMETER, and
// PostGIS's `geometry` type has a DYNAMIC, per-database OID (assigned at
// CREATE EXTENSION time) for which pgx has no registered codec. Without a
// codec pgx falls back to TEXT format, shipping the raw EWKB bytes as if
// they were text — and PostGIS `geometry_in` (the TEXT input parser)
// rejects them with "parse error - invalid geometry" (SQLSTATE XX000).
// Geometry was therefore un-appliable over CDC on BOTH the serial and the
// ADR-0092 pipelined paths (loud refusal, never silent corruption).
//
// This codec closes that gap: registered against the runtime `geometry`
// OID (see [afterConnectRegisterGeometry]) it forces BINARY format and
// ships the EWKB bytes verbatim to `geometry_recv` — the same bytes, same
// on-target value, as the COPY path. It mirrors the pre-existing
// [pgvectorBinaryCodec] / hstore / timetz per-conn codecs (pgvector and
// hstore happen to work on the applier via a text fallback because their
// IR value already IS the valid text form; geometry's binary EWKB is not,
// which is why it specifically needed a codec).
//
// # Wire shape
//
// The encode side is a passthrough: the value is EWKB ([]byte from
// prepareValue) and PostGIS's binary receive format for `geometry` IS
// EWKB, so binary encode appends the bytes unchanged. The TEXT branch
// (used only if a caller forces text format) hex-encodes the EWKB, which
// `geometry_in` accepts as hex-EWKB. A `string` value is treated as an
// already-hex-EWKB spelling and passes through (text) / hex-decodes
// (binary) so the codec is robust to either shape a future translator
// might hand it; anything else is a loud translator-bug error.
type pgGeometryBinaryCodec struct{}

func (pgGeometryBinaryCodec) FormatSupported(format int16) bool {
	return format == pgtype.BinaryFormatCode || format == pgtype.TextFormatCode
}

// PreferredFormat reports binary as the preferred wire format: EWKB is the
// native binary shape PostGIS `geometry_recv` reads, and prepareValue
// already produces EWKB, so binary is a verbatim passthrough.
func (pgGeometryBinaryCodec) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

func (pgGeometryBinaryCodec) PlanEncode(_ *pgtype.Map, _ uint32, format int16, value any) pgtype.EncodePlan {
	switch format {
	case pgtype.BinaryFormatCode:
		switch value.(type) {
		case []byte:
			return encodePlanGeometryBinaryBytes{}
		case string:
			return encodePlanGeometryBinaryString{}
		}
	case pgtype.TextFormatCode:
		switch value.(type) {
		case []byte:
			return encodePlanGeometryTextBytes{}
		case string:
			return encodePlanGeometryTextString{}
		}
	}
	return nil
}

// PlanScan / Decode* are implemented for round-trip symmetry (unit tests,
// and any future applier path that scans geometry back). The CDC applier
// itself never reads geometry; the CDC READER decodes it on its own path
// (decodePGGeometry, wired to the dynamic geometry OID in
// buildRelationCacheEntry — Bug 147).
func (pgGeometryBinaryCodec) PlanScan(_ *pgtype.Map, _ uint32, _ int16, _ any) pgtype.ScanPlan {
	return nil
}

func (pgGeometryBinaryCodec) DecodeDatabaseSQLValue(_ *pgtype.Map, _ uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}
	switch format {
	case pgtype.BinaryFormatCode:
		// EWKB bytes — surface the hex spelling so a SQL scanner gets
		// something portable (a string), matching geom::text on the server.
		return hex.EncodeToString(src), nil
	case pgtype.TextFormatCode:
		return string(src), nil
	}
	return nil, fmt.Errorf("postgres: geometry codec: unsupported scan format %d", format)
}

func (pgGeometryBinaryCodec) DecodeValue(_ *pgtype.Map, _ uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	switch format {
	case pgtype.BinaryFormatCode:
		// Return a copy: pgx may reuse the src buffer after this returns.
		out := make([]byte, len(src))
		copy(out, src)
		return out, nil
	case pgtype.TextFormatCode:
		// src is hex-EWKB text; decode to the EWKB bytes.
		return hex.DecodeString(string(src))
	}
	return nil, fmt.Errorf("postgres: geometry codec: unsupported decode format %d", format)
}

// ---------- encode plans (binary: EWKB verbatim) ----------

type encodePlanGeometryBinaryBytes struct{}

func (encodePlanGeometryBinaryBytes) Encode(value any, buf []byte) ([]byte, error) {
	b := value.([]byte)
	if len(b) == 0 {
		return nil, errors.New("postgres: geometry codec: empty EWKB bytes")
	}
	return append(buf, b...), nil
}

// encodePlanGeometryBinaryString accepts an already-hex-EWKB string and
// emits the decoded binary EWKB.
type encodePlanGeometryBinaryString struct{}

func (encodePlanGeometryBinaryString) Encode(value any, buf []byte) ([]byte, error) {
	decoded, err := hex.DecodeString(value.(string))
	if err != nil {
		return nil, fmt.Errorf("postgres: geometry codec: value is not hex-EWKB: %w", err)
	}
	if len(decoded) == 0 {
		return nil, errors.New("postgres: geometry codec: empty EWKB string")
	}
	return append(buf, decoded...), nil
}

// ---------- encode plans (text: hex-EWKB, the geometry_in spelling) ----------

type encodePlanGeometryTextBytes struct{}

func (encodePlanGeometryTextBytes) Encode(value any, buf []byte) ([]byte, error) {
	b := value.([]byte)
	if len(b) == 0 {
		return nil, errors.New("postgres: geometry codec: empty EWKB bytes")
	}
	return hex.AppendEncode(buf, b), nil
}

type encodePlanGeometryTextString struct{}

func (encodePlanGeometryTextString) Encode(value any, buf []byte) ([]byte, error) {
	s := value.(string)
	if s == "" {
		return nil, errors.New("postgres: geometry codec: empty EWKB string")
	}
	// Already a hex-EWKB spelling — pass through verbatim.
	return append(buf, s...), nil
}

// ---------- OID lookup + per-conn registration ----------

// errGeometryTypeNotFound signals that pg_type contains no `geometry` row
// — PostGIS isn't installed, so the target can hold no geometry columns
// and there is no codec to register. Callers treat it as a clean skip.
var errGeometryTypeNotFound = errors.New(
	"postgres: geometry type not found in pg_type — PostGIS not installed",
)

// lookupGeometryOIDConn resolves the runtime OID of the PostGIS `geometry`
// type on conn. The OID is dynamic (assigned at CREATE EXTENSION postgis
// time), so it must come from the catalog, not a compile-time constant.
func lookupGeometryOIDConn(ctx context.Context, conn *pgx.Conn) (uint32, error) {
	var oid uint32
	err := conn.QueryRow(ctx, `SELECT oid FROM pg_type WHERE typname = 'geometry' LIMIT 1`).Scan(&oid)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, errGeometryTypeNotFound
	case err != nil:
		return 0, fmt.Errorf("postgres: lookup geometry OID: %w", err)
	}
	if oid == 0 {
		return 0, errGeometryTypeNotFound
	}
	return oid, nil
}

// afterConnectRegisterGeometry is the [stdlib.OptionAfterConnect] hook the
// CDC applier installs on BOTH its pools (the serial primary pool and the
// ADR-0092 pipelined DescribeExec pool), so every applier connection can
// ship EWKB to a `geometry` column regardless of which apply path a batch
// takes. It runs once per new connection (connections are pooled, so the
// catalog lookup is amortised); a target without PostGIS is a clean no-op.
//
// Idempotent: re-running on a conn that already has the codec is skipped.
func afterConnectRegisterGeometry(ctx context.Context, conn *pgx.Conn) error {
	tm := conn.TypeMap()
	if _, already := tm.TypeForName("geometry"); already {
		return nil
	}
	oid, err := lookupGeometryOIDConn(ctx, conn)
	switch {
	case errors.Is(err, errGeometryTypeNotFound):
		// No PostGIS on this target — nothing to register. Geometry
		// columns cannot exist here, so the applier never needs the codec.
		return nil
	case err != nil:
		return err
	}
	tm.RegisterType(&pgtype.Type{
		Name:  "geometry",
		OID:   oid,
		Codec: pgGeometryBinaryCodec{},
	})
	return nil
}
