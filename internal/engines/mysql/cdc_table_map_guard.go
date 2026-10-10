// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Replayed events vs. the current catalog (audit CDC-4)
//
// The binlog reader decodes a row event's raw values against the column
// types it read from `information_schema`. The comment on [CDCReader.tableFor]
// once justified that with a server property — MySQL serialises DDL against
// DML on the same table, so the catalog reflects any DDL that preceded the
// event — which holds only for a reader AT the binlog head.
//
// A reader behind the head works forward through history while
// `information_schema` answers as of NOW: a warm resume after downtime, but
// equally every backup window and a live sync that lags (Bug 301 measured
// the last with no downtime at all). Every event recorded before a DDL the
// catalog already shows is then decoded against the post-DDL shape. Since
// Bug 301 the shape is proven or rebuilt for the event's position before
// this guard runs (cdc_ddl_lookahead.go, cdc_recorded_shape.go); this guard
// is the backstop.
//
// A DDL that changes the column COUNT is caught today — [decodeBinlogRow]
// refuses on arity. The dangerous one is the DDL that does not: a type
// change (or a rename that also retypes) leaves the count intact, so every
// replayed value is handed to the decoder for a type it was never written
// as. Right number of columns, wrong meaning, no error.
//
// # What this guard compares, and what it deliberately does not
//
// The TABLE_MAP_EVENT that precedes every row event carries the column
// type vector AS RECORDED — the server's own statement of the shape those
// values were written under. Comparing it against the shape sluice is
// about to decode with is a direct, per-event answer to "is this history
// older than the catalog?".
//
// The comparison is by DECODE FAMILY, not by exact type code, and that is
// a deliberate narrowing:
//
//   - `VARCHAR(20)` → `VARCHAR(40)` → `TEXT` all decode to the same Go
//     value shape. Refusing those would halt syncs for changes that cannot
//     corrupt anything.
//   - `INT` → `BIGINT` likewise: go-mysql decodes each width to int64 and
//     the value is the value.
//   - `DECIMAL` → `DOUBLE`, `INT` → `VARCHAR`, `ENUM` → `INT`,
//     `TIMESTAMP` → `DATETIME`, `JSON` → `TEXT` cross a family boundary and
//     DO change what a replayed value means. Those are what this refuses.
//
// Stated as a limitation rather than left implied: a same-family change
// this guard cannot see is a same-family change it does not claim to
// catch — a column RENAME or reorder with identical types (Bug 301
// measured the reorder and the rename swap landing values under the wrong
// names), or an ENUM/SET whose LABEL LIST was reordered. Neither is closed
// HERE. They are closed before this guard runs: under
// `binlog_row_metadata=FULL` the event's own column names and labels decide
// the shape (cdc_recorded_shape.go); without them the catalog shape must be
// proven to hold at the event's position, a reshaping DDL in between refuses,
// and an in-place redefinition of an ENUM/SET refuses for want of its labels
// (cdc_ddl_lookahead.go).
//
// Unknown on either side means unchecked, never refused — a type sluice
// cannot classify is not evidence of drift. TestBinlogTypeFamilyRoster is
// the fail-by-default roster over every `data_type` [translateType]
// accepts, so a newly-supported type cannot slip into the unchecked set
// without someone writing it down.

package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// binlogTypeFamily groups binlog column types that decode to the same Go
// value shape. Two columns in the same family can be substituted for one
// another without changing what a replayed value MEANS; two in different
// families cannot.
type binlogTypeFamily uint8

const (
	famUnknown binlogTypeFamily = iota
	famInt
	famDecimal
	famFloat
	famBit
	famString
	famDate
	famTime
	famDateTime
	famTimestamp
	famEnum
	famSet
	famJSON
	famGeometry
)

// String renders a family for the refusal message. Operator-facing, so
// these read as type words rather than identifiers.
func (f binlogTypeFamily) String() string {
	switch f {
	case famInt:
		return "integer"
	case famDecimal:
		return "decimal"
	case famFloat:
		return "float"
	case famBit:
		return "bit"
	case famString:
		return "string/binary"
	case famDate:
		return "date"
	case famTime:
		return "time"
	case famDateTime:
		return "datetime"
	case famTimestamp:
		return "timestamp"
	case famEnum:
		return "enum"
	case famSet:
		return "set"
	case famJSON:
		return "json"
	case famGeometry:
		return "geometry"
	case famUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

// binlogTypeFamilyForDataType maps an `information_schema.columns.DATA_TYPE`
// string (lowercased by the loader's query) to its decode family. ok is
// false for a type this guard deliberately does not classify; the caller
// then leaves the column unchecked.
//
// The arms mirror [translateType]'s, one family per group of arms that
// produce interchangeable decoded values. TestBinlogTypeFamilyRoster walks
// every data_type translateType accepts and fails if one is neither
// classified here nor listed as a deliberate exemption — this table cannot
// silently fall behind the type mapper.
func binlogTypeFamilyForDataType(dataType string) (binlogTypeFamily, bool) {
	switch dataType {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year":
		return famInt, true
	case "decimal", "numeric":
		return famDecimal, true
	case "float", "double", "real":
		return famFloat, true
	case "bit":
		return famBit, true
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext",
		"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return famString, true
	case "date":
		return famDate, true
	case "time":
		return famTime, true
	case "datetime":
		return famDateTime, true
	case "timestamp":
		return famTimestamp, true
	case "enum":
		return famEnum, true
	case "set":
		return famSet, true
	case "json":
		return famJSON, true
	case "geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection":
		return famGeometry, true
	}
	return famUnknown, false
}

// binlogTypeFamilyForCode maps a TABLE_MAP_EVENT column type byte (plus its
// metadata word, which is what distinguishes ENUM/SET from a plain CHAR) to
// its decode family. ok is false for a code this build does not classify —
// a newer server's type, for instance — which leaves the column unchecked
// rather than refusing on ignorance.
//
// The ENUM/SET normalisation mirrors go-mysql's own unexported
// TableMapEvent.realType: both are written to the binlog as
// MYSQL_TYPE_STRING with the real type in the high byte of the metadata
// word. Re-derived here because realType is not exported.
func binlogTypeFamilyForCode(code byte, meta uint16) (binlogTypeFamily, bool) {
	if code == mysql.MYSQL_TYPE_STRING {
		switch byte(meta >> 8) {
		case mysql.MYSQL_TYPE_ENUM:
			return famEnum, true
		case mysql.MYSQL_TYPE_SET:
			return famSet, true
		}
	}
	switch code {
	case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_INT24,
		mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_YEAR:
		return famInt, true
	case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL:
		return famDecimal, true
	case mysql.MYSQL_TYPE_FLOAT, mysql.MYSQL_TYPE_DOUBLE:
		return famFloat, true
	case mysql.MYSQL_TYPE_BIT:
		return famBit, true
	case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_VARCHAR,
		mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_TINY_BLOB,
		mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB:
		return famString, true
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE:
		return famDate, true
	case mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_TIME2:
		return famTime, true
	case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME2:
		return famDateTime, true
	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2:
		return famTimestamp, true
	case mysql.MYSQL_TYPE_ENUM:
		return famEnum, true
	case mysql.MYSQL_TYPE_SET:
		return famSet, true
	case mysql.MYSQL_TYPE_JSON:
		return famJSON, true
	case mysql.MYSQL_TYPE_GEOMETRY:
		return famGeometry, true
	}
	return famUnknown, false
}

// verifyTableMapMatchesSchema refuses when a row event's TABLE_MAP shape
// disagrees with the schema the reader is about to decode it with. It runs
// once per rows-event (not per row) and is O(columns).
//
// tbl.DataTypes is populated by [loadTableSchema] and is parallel to
// tbl.Columns. A *tableSchema without it — only hand-built ones in unit
// tests, since the loader is production's sole constructor — is skipped:
// the guard's whole content is a comparison, and there is nothing to
// compare against. TestLoadTableSchemaPopulatesDataTypes (integration) is
// the anti-vacuity floor that keeps that skip from silently becoming the
// production path: it asserts the real loader fills the field AND that
// this guard is live for a schema the loader produced.
func verifyTableMapMatchesSchema(ev *replication.RowsEvent, tbl *tableSchema) error {
	if ev == nil || ev.Table == nil || tbl == nil || len(tbl.DataTypes) != len(tbl.Columns) {
		return nil
	}
	// A column-count change is already fatal downstream (decodeBinlogRow
	// refuses on arity). Refusing here is a message upgrade, not a new
	// refusal: the operator gets the cause and the remedy instead of
	// "row has N values; schema has M columns".
	if int(ev.Table.ColumnCount) != len(tbl.Columns) {
		return errCDCSchemaReplayMismatch(tbl, fmt.Sprintf(
			"the event was recorded against %d columns and the table now has %d",
			ev.Table.ColumnCount, len(tbl.Columns),
		))
	}
	if len(ev.Table.ColumnType) != len(tbl.Columns) {
		// A truncated/absent type vector is not evidence of drift; the
		// arity check above already covers the case that matters.
		return nil
	}
	for i, col := range tbl.Columns {
		want, wantOK := binlogTypeFamilyForDataType(tbl.DataTypes[i])
		if !wantOK {
			continue
		}
		var meta uint16
		if i < len(ev.Table.ColumnMeta) {
			meta = ev.Table.ColumnMeta[i]
		}
		got, gotOK := binlogTypeFamilyForCode(ev.Table.ColumnType[i], meta)
		if !gotOK || got == want {
			continue
		}
		return errCDCSchemaReplayMismatch(tbl, fmt.Sprintf(
			"column %q was recorded as a value of the %s family but the table now declares it %s (%s family)",
			col.Name, got, tbl.DataTypes[i], want,
		))
	}
	return nil
}

// errCDCSchemaReplayMismatch is the loud half of the CDC-4 silent-remap
// class. It names the table (the operator's unit of remedy) and the exact
// disagreement, and points at the only remedy that actually works: there is
// no position from which this history can be replayed faithfully, so the
// table has to be re-snapshotted.
func errCDCSchemaReplayMismatch(tbl *tableSchema, detail string) error {
	return sluicecode.Wrap(
		sluicecode.CodeCDCSchemaReplayMismatch,
		"re-snapshot the table (sync --restart-from-scratch, or a fresh backup full for a backup chain); the binlog "+
			"history being read predates a schema change. With binlog_row_metadata=FULL on the source, later history "+
			"decodes across such a change",
		fmt.Errorf(
			"mysql: cdc: %s.%s: this binlog event was recorded under a DIFFERENT table shape than the one "+
				"it would be decoded with — %s. A schema change landed between the position this stream is "+
				"reading and the shape sluice holds for the table, which happens whenever the stream is behind "+
				"the binlog head when the change lands: a backup window, a resumed sync, or a live sync that is "+
				"lagging. Decoding these events against that shape would reinterpret every value in them without "+
				"any error. Refusing",
			tbl.Schema, tbl.Name, detail,
		),
	)
}
