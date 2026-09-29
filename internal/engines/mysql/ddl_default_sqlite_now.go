// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// sqliteNowDefaultMySQL renders a SQLite current-instant DEFAULT (see
// [translate.ClassifySQLiteNowDefault]) for a MySQL column of type t
// (GC-39 item 3). handled=false leaves the default to the general SQLite
// arm: it is not a current-instant spelling, or it is the zone-free epoch
// form, whose handling is unchanged. When handled, exactly one of body and
// lost is non-empty; lost says why no faithful spelling exists here, and
// the caller drops the DEFAULT loudly.
//
// SQLite evaluates 'now' in UTC and stores second-precision TEXT. MySQL's
// CURRENT_TIMESTAMP / NOW() evaluate in the writing SESSION's time_zone
// when stored into a DATETIME, DATE, TIME or text column, so the pre-GC-39
// carry stamped session wall-clock digits into columns that had held UTC —
// measured nine hours ahead under a `+09:00` session. (CURRENT_DATE and
// CURRENT_TIME were carried bare, which MySQL rejects in a DEFAULT: the
// CREATE TABLE failed with Error 1064.) The UTC_* functions read the UTC
// clock whatever the session zone:
//
//	column                   datetime / ISO-Z shape      date shape             time shape
//	TIMESTAMP (any ir.Timestamp) CURRENT_TIMESTAMP(p)        —                      —
//	DATETIME (naive)         UTC_TIMESTAMP()             UTC midnight           —
//	DATE                     UTC_DATE()                  UTC_DATE()             —
//	TIME                     —                           —                      UTC_TIME()
//	text / varchar / char    SQLite's exact text         SQLite's text          SQLite's text
//
// A MySQL TIMESTAMP stores an instant (it converts from the session zone
// on write), so CURRENT_TIMESTAMP is already correct there and needs no
// expression-default support. Every other cell is an expression DEFAULT,
// which MySQL accepts from 8.0.13 (and MariaDB from 10.2.1); on an older
// server, or one whose version sluice could not read, the cell is lost
// rather than degraded to the session-zone keyword. Blank cells are lost
// too — the Postgres writer's matrix, cell for cell.
func (m mysqlEmitter) sqliteNowDefaultMySQL(d ir.DefaultValue, t ir.Type) (body, lost string, handled bool) {
	v, ok := d.(ir.DefaultExpression)
	if !ok || v.Dialect != sqliteSourceDialect {
		return "", "", false
	}
	shape := translate.ClassifySQLiteNowDefault(v.Expr)
	if shape == translate.SQLiteNowNone || shape == translate.SQLiteNowEpoch {
		return "", "", false
	}
	dateTimeLike := shape == translate.SQLiteNowDateTime || shape == translate.SQLiteNowISOZ
	noFaithful := fmt.Sprintf("a SQLite current-instant DEFAULT has no faithful spelling on a %T column", ir.UnwrapDomain(t))
	switch tv := ir.UnwrapDomain(t).(type) {
	case ir.Timestamp:
		// Every ir.Timestamp, naive or not, is emitted as MySQL TIMESTAMP
		// (emitColumnType), which stores an instant converted from the
		// session zone — so CURRENT_TIMESTAMP is the correct default, and
		// UTC_TIMESTAMP() would be converted from the session zone a second
		// time (the pre-tag review measured the double shift under +09:00).
		if dateTimeLike {
			return matchTimestampDefaultPrecision("CURRENT_TIMESTAMP", t), "", true
		}
		return "", noFaithful, true
	case ir.DateTime:
		body = sqliteNowNaiveDateTimeMySQL(shape)
	case ir.Date:
		if dateTimeLike || shape == translate.SQLiteNowDate {
			body = "(UTC_DATE())"
		}
	case ir.Time:
		if !tv.WithTimeZone && shape == translate.SQLiteNowTime {
			body = "(UTC_TIME())"
		}
	case ir.Text, ir.Varchar, ir.Char:
		body = map[translate.SQLiteNowShape]string{
			translate.SQLiteNowDateTime: "(DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%d %H:%i:%s'))",
			translate.SQLiteNowISOZ:     "(DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%sZ'))",
			translate.SQLiteNowDate:     "(DATE_FORMAT(UTC_DATE(), '%Y-%m-%d'))",
			translate.SQLiteNowTime:     "(TIME_FORMAT(UTC_TIME(), '%H:%i:%s'))",
		}[shape]
	}
	if body == "" {
		return "", noFaithful, true
	}
	if m.lobDefaults == lobDefaultNone {
		return "", "the target server cannot evaluate a UTC expression DEFAULT (MySQL before 8.0.13, or a server " +
			"whose version sluice could not read), and its CURRENT_TIMESTAMP family would evaluate in the writing " +
			"session's zone where SQLite's 'now' is UTC", true
	}
	return body, "", true
}

func sqliteNowNaiveDateTimeMySQL(shape translate.SQLiteNowShape) string {
	switch shape {
	case translate.SQLiteNowDateTime, translate.SQLiteNowISOZ:
		return "(UTC_TIMESTAMP())"
	case translate.SQLiteNowDate:
		return "(CAST(UTC_DATE() AS DATETIME))"
	}
	return ""
}

// warnDropSQLiteNowDefaultMySQL is emitColumnDef's loud half of
// [mysqlEmitter.sqliteNowDefaultMySQL]: it reports whether c's DEFAULT is
// a current-instant spelling this target cannot carry, and WARNs naming
// the column and the reason when it is.
func (m mysqlEmitter) warnDropSQLiteNowDefaultMySQL(tableName string, c *ir.Column) bool {
	_, lost, handled := m.sqliteNowDefaultMySQL(c.Default, c.Type)
	if !handled || lost == "" {
		return false
	}
	expr := c.Default.(ir.DefaultExpression).Expr
	slog.Warn(
		fmt.Sprintf("mysql: dropped SQLite DEFAULT %q on %s.%s: %s; the column has no DEFAULT on the target — "+
			"re-create one there if your application relies on it",
			expr, quoteIdent(tableName), quoteIdent(c.Name), lost),
		slog.String("table", tableName),
		slog.String("column", c.Name),
		slog.String("expression", expr),
	)
	return true
}
