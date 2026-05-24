// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/orware/sluice/internal/ir"
)

// relationCacheEntry is the IR-typed projection of a pgoutput
// RelationMessage. Built once per RelationMessage and consulted on
// every subsequent DML event for the same relation OID.
//
// The pgoutput protocol guarantees a RelationMessage precedes the
// first DML event for any relation in a stream, and a fresh
// RelationMessage is emitted whenever the relation's schema changes.
// That makes the relations cache its own invalidation channel — no
// separate DDL listener is needed, in contrast to MySQL CDC where
// schema changes arrive as opaque QueryEvents.
type relationCacheEntry struct {
	Schema string
	Name   string

	// ReplicaIdentity is the pg_class.relreplident byte:
	//   'd' default (PK columns only in old tuple)
	//   'n' nothing (no old tuple at all)
	//   'f' full (every column in old tuple)
	//   'i' using-index (named index columns in old tuple)
	// Drives Update/Delete Before-image semantics; the dispatcher
	// records this so a future v2 can warn the user about tables
	// without a usable identity.
	ReplicaIdentity uint8

	Columns []relationColumn
}

// relationColumn carries the resolved IR view of one column. The raw
// OID is kept alongside the IR type so unknown-type errors can name
// the OID (the lookup table omission, not the IR type) for users.
type relationColumn struct {
	Name      string
	OID       uint32
	TypeMod   int32
	Type      ir.Type
	KeyColumn bool // RelationMessageColumn.Flags & 1
}

// projectRelation builds an [ir.Table] from a relationCacheEntry —
// the ADR-0049 Chunk B3 boundary projector. The entry is ALREADY
// IR-typed (buildRelationCacheEntry resolved every column's OID via
// oidToType when the RelationMessage arrived), so this is the
// cheapest of the three engine boundary paths: no re-introspection,
// no second type mapping (the locked decision #2 "build from
// in-stream position-anchored metadata, never re-introspection" is
// satisfied for free — pgoutput's RelationMessage IS that metadata).
//
// Nullability is not carried on a pgoutput RelationMessage column
// (the protocol only sends name/OID/typmod/key-flag), so projected
// columns are left Nullable=false. The schema-history decode contract
// (ir.SchemaSignature) compares only column name + IR type, so this
// does not affect resolve correctness — it is a faithful projection
// of exactly what the wire carries.
func projectRelation(rel *relationCacheEntry) *ir.Table {
	cols := make([]*ir.Column, len(rel.Columns))
	for i, c := range rel.Columns {
		cols[i] = &ir.Column{Name: c.Name, Type: c.Type}
	}
	tbl := &ir.Table{Schema: rel.Schema, Name: rel.Name, Columns: cols}
	// Bug 89: surface PK column names from the RelationMessage's
	// per-column KeyColumn flag. ADR-0058 backfill (and any future per-PK
	// path consuming a CDC-emitted SchemaSnapshot) needs the PK to drive
	// cursor-paginated iteration. KeyColumn=true on a pgoutput Relation
	// is set for replica-identity columns; with REPLICA IDENTITY DEFAULT
	// (the default) this is the PK column set, which is what
	// runBackfillForAddedColumn requires.
	var pkCols []ir.IndexColumn
	for _, c := range rel.Columns {
		if c.KeyColumn {
			pkCols = append(pkCols, ir.IndexColumn{Column: c.Name})
		}
	}
	if len(pkCols) > 0 {
		tbl.PrimaryKey = &ir.Index{Columns: pkCols}
	}
	return tbl
}

// oidToType maps a Postgres data-type OID (as carried in
// RelationMessageColumn.DataType) to the corresponding IR type.
// Unknown OIDs return an error rather than a fallback — silent
// type fallbacks produce data corruption that's hard to spot in
// review, while a loud error names the OID and stops the stream.
//
// Custom types (enums from CREATE TYPE, composite types, domains)
// have OIDs that aren't in pgtype's constant set; resolving those
// would require a one-time pg_type lookup. Punted to a follow-up
// chunk; for v1 they error out with the OID number so users have
// a concrete signal.
//
// typmod encodes per-instance metadata for parameterised types
// (VARCHAR length, NUMERIC precision/scale, TIMESTAMP precision).
// Postgres uses typmod = -1 to mean "no parameter set"; helpers
// below decode the conventional layouts.
func oidToType(oid uint32, typmod int32) (ir.Type, error) {
	switch oid {
	// ---- Boolean ----
	case pgtype.BoolOID:
		return ir.Boolean{}, nil

	// ---- Integer family ----
	case pgtype.Int2OID:
		return ir.Integer{Width: 16}, nil
	case pgtype.Int4OID:
		return ir.Integer{Width: 32}, nil
	case pgtype.Int8OID:
		return ir.Integer{Width: 64}, nil

	// ---- Decimal / float ----
	case pgtype.Float4OID:
		return ir.Float{Precision: ir.FloatSingle}, nil
	case pgtype.Float8OID:
		return ir.Float{Precision: ir.FloatDouble}, nil
	case pgtype.NumericOID:
		p, s := numericTypmod(typmod)
		return ir.Decimal{Precision: p, Scale: s}, nil

	// ---- Character ----
	case pgtype.VarcharOID:
		l := charTypmod(typmod)
		if l == 0 {
			// Unbounded VARCHAR is exotic but possible; the IR has
			// no "varchar with no length" so we land on Text/long.
			return ir.Text{Size: ir.TextLong}, nil
		}
		return ir.Varchar{Length: l}, nil
	case pgtype.BPCharOID:
		return ir.Char{Length: charTypmod(typmod)}, nil
	case pgtype.TextOID:
		return ir.Text{Size: ir.TextLong}, nil

	// ---- Binary ----
	case pgtype.ByteaOID:
		return ir.Blob{Size: ir.BlobLong}, nil

	// ---- Temporal ----
	case pgtype.DateOID:
		return ir.Date{}, nil
	case pgtype.TimeOID, pgtype.TimetzOID:
		return ir.Time{Precision: temporalTypmod(typmod)}, nil
	case pgtype.TimestampOID:
		return ir.DateTime{Precision: temporalTypmod(typmod)}, nil
	case pgtype.TimestamptzOID:
		return ir.Timestamp{Precision: temporalTypmod(typmod), WithTimeZone: true}, nil

	// ---- Structured ----
	case pgtype.JSONOID:
		return ir.JSON{Binary: false}, nil
	case pgtype.JSONBOID:
		return ir.JSON{Binary: true}, nil

	// ---- Identity / network ----
	case pgtype.UUIDOID:
		return ir.UUID{}, nil
	case pgtype.InetOID:
		return ir.Inet{}, nil
	case pgtype.CIDROID:
		return ir.Cidr{}, nil
	case pgtype.MacaddrOID, pgtype.Macaddr8OID:
		return ir.Macaddr{}, nil
	}
	return nil, fmt.Errorf("postgres: cdc: unsupported column type OID %d (typmod %d)", oid, typmod)
}

// charTypmod extracts the declared length N from a typmod value
// produced by character types (VARCHAR(N), CHAR(N)). Postgres stores
// these as N+4 with -1 meaning "no length specified".
func charTypmod(typmod int32) int {
	if typmod < 4 {
		return 0
	}
	return int(typmod - 4)
}

// numericTypmod decodes the (precision, scale) pair from a NUMERIC
// typmod value. Postgres encodes (P, S) as ((P << 16) | S) + 4 with
// -1 meaning "no precision specified" (max precision NUMERIC).
func numericTypmod(typmod int32) (precision, scale int) {
	if typmod < 4 {
		return 0, 0
	}
	t := typmod - 4
	return int((t >> 16) & 0xFFFF), int(t & 0xFFFF)
}

// temporalTypmod returns the fractional-second precision N from a
// TIMESTAMP(N) / TIME(N) typmod. Postgres stores precision directly
// (no +4 offset for these types) with -1 meaning "default".
func temporalTypmod(typmod int32) int {
	if typmod < 0 {
		return 0
	}
	return int(typmod)
}
