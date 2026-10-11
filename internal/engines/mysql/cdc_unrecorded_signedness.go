// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # An integer the binlog does not say is unsigned (MARIADB-NOLOG-UNSIGNED)
//
// A row event stores an integer as its bare two's-complement bytes; whether
// they mean a signed or an unsigned number is recorded only in the
// TABLE_MAP's optional signedness field. go-mysql decodes by that field and,
// where it is absent, as SIGNED. MariaDB's default
// binlog_row_metadata=NO_LOG writes no optional metadata at all, so every
// UNSIGNED value above its type's signed maximum decoded negative —
// `BIGINT UNSIGNED` 18446744073709551615 as -1, `INT UNSIGNED` 4294967295 as
// -1 — at exit 0, MEASURED on mariadb:11.4 and 10.11. A Postgres target
// stored the negative value, and an UPDATE or DELETE keyed by such a column
// matched no target row, which the applier tolerates as a replay: the change
// was lost.
//
// The bytes are exact; only their interpretation is missing. So where the
// event does not record a column's signedness, the decoder takes it from the
// shape it decodes with — which Bug 301 has, by then, PROVEN to be the
// table's shape at this event's position (cdc_ddl_lookahead.go): no DDL
// between the event and the catalog read reshapes the table, and a column
// redefined in place ahead of the reader refuses unless the event records
// its signedness ([retypedColumnUnverified]). A shape that is not proven, or
// a TABLE_MAP that names its columns (FULL) yet omits their signedness —
// which no server sluice has met writes — refuses rather than guesses.
//
// Only the five integer codes are affected: DECIMAL stores its own sign,
// FLOAT/DOUBLE are IEEE bit patterns, and YEAR and BIT are unsigned by
// construction; go-mysql decodes none of them by the signedness field. The
// family × metadata × server matrix is TestCDCSignedness_EveryNumericFamily.
//
// MySQL is unaffected at its default binlog_row_metadata=MINIMAL, which
// records signedness (MEASURED on 8.0 and 8.4 by the same matrix, which
// also asserts the field was present, so the premise is checked rather than
// assumed).

package mysql

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

// unrecordedUnsigned is, parallel to a shape's columns, the TABLE_MAP type
// code of each UNSIGNED integer column whose event does not record its
// signedness — the columns [decodeBinlogRow] reinterprets — and 0 for every
// other column. nil when there is none.
type unrecordedUnsigned []byte

// binlogUnrecordedUnsigned resolves which columns of one rows event go-mysql
// decoded as signed for want of a signedness record although tbl declares
// them UNSIGNED. An integer column of either signedness whose event records
// none refuses when tbl is not proven to be the table's shape at the event:
// go-mysql's signed reading is then as much a guess as the shape's. It runs
// after the TABLE_MAP guard, so the column count and each column's type
// family already agree with tbl.
func binlogUnrecordedUnsigned(tbl *tableSchema, tm *replication.TableMapEvent) (unrecordedUnsigned, error) {
	if tm == nil {
		// Unreachable from dispatch (go-mysql attaches the TABLE_MAP to every
		// rows event it decodes); kept so a hand-built event cannot panic.
		return nil, nil
	}
	recorded := tm.UnsignedMap()
	var out unrecordedUnsigned
	for j, col := range tbl.Columns {
		it, isInt := col.Type.(ir.Integer)
		if !isInt || j >= len(tm.ColumnType) || !isBinlogIntegerCode(tm.ColumnType[j]) {
			continue
		}
		if _, known := recorded[j]; known {
			continue
		}
		if !tbl.proven || recordedColumnNames(tm) != nil {
			return nil, errCDCSchemaReplayMismatch(tbl, fmt.Sprintf(
				"the event does not record whether integer column %q was signed, and the definition sluice "+
					"holds for it (%s) is not proven to hold at this event's position", col.Name, it,
			))
		}
		if !it.Unsigned {
			continue
		}
		if out == nil {
			out = make(unrecordedUnsigned, len(tbl.Columns))
		}
		out[j] = tm.ColumnType[j]
	}
	return out, nil
}

// noteUnrecordedSignedness records that a rows event of tbl was decoded with
// signedness taken from the proven shape, once per shape, at DEBUG: the
// decode is exact, so it is not a warning, but it is the one place the
// source's binlog_row_metadata changes what the reader does for an integer.
func noteUnrecordedSignedness(tbl *tableSchema, us unrecordedUnsigned) {
	if us == nil || tbl.signednessFromShape {
		return
	}
	tbl.signednessFromShape = true
	var cols []string
	for j, code := range us {
		if code != 0 {
			cols = append(cols, tbl.Columns[j].Name)
		}
	}
	slog.Debug("cdc: the binlog does not record these UNSIGNED columns' signedness (binlog_row_metadata=NO_LOG); "+
		"decoding them by the table's definition, proven to hold at this position",
		slog.String("table", tbl.Schema+"."+tbl.Name), slog.String("columns", strings.Join(cols, ", ")))
}

// isBinlogIntegerCode reports whether a TABLE_MAP type code is one of the
// integer types go-mysql decodes by the signedness record.
func isBinlogIntegerCode(code byte) bool {
	switch code {
	case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_INT24,
		mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONGLONG:
		return true
	}
	return false
}

// reinterpretUnsigned reads a value go-mysql decoded as a signed integer of
// the width code names as the unsigned integer the same bytes hold. The bits
// are exact, so this is lossless; a value of any other Go type is not what
// the code promises and refuses.
func reinterpretUnsigned(v any, code byte) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case int8:
		if code == mysql.MYSQL_TYPE_TINY {
			return uint64(uint8(x)), nil
		}
	case int16:
		if code == mysql.MYSQL_TYPE_SHORT {
			return uint64(uint16(x)), nil
		}
	case int32:
		// go-mysql sign-extends a MEDIUMINT's three bytes into an int32.
		switch code {
		case mysql.MYSQL_TYPE_INT24:
			return uint64(uint32(x) & 0xFFFFFF), nil
		case mysql.MYSQL_TYPE_LONG:
			return uint64(uint32(x)), nil
		}
	case int64:
		if code == mysql.MYSQL_TYPE_LONGLONG {
			return uint64(x), nil
		}
	}
	return nil, fmt.Errorf("mysql: cdc: binlog value %v (%T) is not the signed integer its type code %d decodes to", v, v, code)
}
