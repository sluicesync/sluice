// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// sqliteSourceDialect is the IR DefaultExpression / schema-feature dialect
// tag the SQLite engine stamps on every non-literal column DEFAULT (see
// internal/engines/sqlite/schema_reader.go::parseDefault). The PG writer
// recognises it on the DEFAULT path to translate the current-instant
// spellings below, then the shared SQLite→PG translator's allowlist
// (ADR-0133), and to loud-drop everything else — it is NEVER fed through
// the MySQL→PG expression translator (that path is gated on
// translatableSourceDialect == "mysql").
const sqliteSourceDialect = "sqlite"

// sqliteNowDefaultPG renders a SQLite current-instant DEFAULT (see
// [translate.ClassifySQLiteNowDefault]) for a Postgres column of type t.
// ok=false means the shape has no faithful spelling on that column type
// and the caller drops the DEFAULT loudly.
//
// SQLite evaluates 'now' in UTC and stores second-precision TEXT. Postgres'
// CURRENT_TIMESTAMP / CURRENT_DATE / CURRENT_TIME evaluate in the SESSION
// zone, so the pre-GC-39 mapping onto those keywords stamped the writing
// session's wall-clock digits into a naive column that had held UTC —
// measured nine hours ahead on an Asia/Tokyo database (item 3). Every
// spelling here is therefore built on [utcNowSQL], the UTC wall clock:
//
//	column                  datetime / ISO-Z shape   date shape         time shape
//	timestamp (naive)       UTC, whole seconds       UTC midnight       —
//	timestamptz             now(), whole seconds     —                  —
//	date                    UTC date                 UTC date           —
//	time (naive)            —                        —                  UTC, whole seconds
//	text / varchar / char   SQLite's exact text      SQLite's text      SQLite's text
//
// A timestamptz column holds an instant, so a bare now() is already
// correct whatever the session zone. The text arm reproduces the bytes
// SQLite would have stored, which is what the migrated rows in that column
// look like. The epoch shape is zone-free and keeps its numeric form on a
// numeric or text column (elsewhere PG would reject it at CREATE TABLE and
// abort the migration, so it drops). Every blank cell drops loudly: a DATE
// default on a time column, say, has no value SQLite would have stored
// there that a Postgres expression reproduces.
func sqliteNowDefaultPG(shape translate.SQLiteNowShape, t ir.Type) (string, bool) {
	const utcSecond = "pg_catalog.date_trunc('second', " + utcNowSQL + ")"
	if shape == translate.SQLiteNowEpoch {
		// Only where the number can land: on a temporal (or any other)
		// column PG rejects the double precision at CREATE TABLE, which
		// would abort the whole migration instead of dropping one DEFAULT.
		switch ir.UnwrapDomain(t).(type) {
		case ir.Integer, ir.Decimal, ir.Float, ir.Text, ir.Varchar, ir.Char:
			return "pg_catalog.floor(extract(epoch from pg_catalog.now()))", true
		}
		return "", false
	}
	dateTimeLike := shape == translate.SQLiteNowDateTime || shape == translate.SQLiteNowISOZ
	switch v := ir.UnwrapDomain(t).(type) {
	case ir.Timestamp:
		if v.WithTimeZone {
			if dateTimeLike {
				return "pg_catalog.date_trunc('second', pg_catalog.now())", true
			}
			return "", false
		}
		return sqliteNowNaiveTimestampPG(shape, utcSecond)
	case ir.DateTime:
		return sqliteNowNaiveTimestampPG(shape, utcSecond)
	case ir.Date:
		if dateTimeLike || shape == translate.SQLiteNowDate {
			return "(" + utcNowSQL + ")::date", true
		}
	case ir.Time:
		if !v.WithTimeZone && shape == translate.SQLiteNowTime {
			return utcSecond + "::time", true
		}
	case ir.Text, ir.Varchar, ir.Char:
		format := map[translate.SQLiteNowShape]string{
			translate.SQLiteNowDateTime: `YYYY-MM-DD HH24:MI:SS`,
			translate.SQLiteNowISOZ:     `YYYY-MM-DD"T"HH24:MI:SS"Z"`,
			translate.SQLiteNowDate:     `YYYY-MM-DD`,
			translate.SQLiteNowTime:     `HH24:MI:SS`,
		}[shape]
		return "pg_catalog.to_char(" + utcNowSQL + ", '" + format + "')", true
	}
	return "", false
}

func sqliteNowNaiveTimestampPG(shape translate.SQLiteNowShape, utcSecond string) (string, bool) {
	switch shape {
	case translate.SQLiteNowDateTime, translate.SQLiteNowISOZ:
		return utcSecond, true
	case translate.SQLiteNowDate:
		return "pg_catalog.date_trunc('day', " + utcNowSQL + ")", true
	}
	return "", false
}
