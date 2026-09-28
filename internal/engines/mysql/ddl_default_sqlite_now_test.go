// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestSQLiteNowDefaultMySQL_ShapeByColumnType pins the whole shape × column
// type matrix of [mysqlEmitter.sqliteNowDefaultMySQL] on an 8.0.13+
// target, every drop cell included, and that no expression cell survives
// on a server that cannot evaluate expression DEFAULTs (GC-39 item 3). The
// integration pin TestMigrate_SQLiteNowDefaults_AreUTC_OnANonUTCTarget
// grades the values against the server's own UTC clock.
func TestSQLiteNowDefaultMySQL_ShapeByColumnType(t *testing.T) {
	types := map[string]ir.Type{
		"timestamptz": ir.Timestamp{Precision: 6, WithTimeZone: true},
		"timestamp":   ir.Timestamp{Precision: 6},
		"datetime":    ir.DateTime{Precision: 6},
		"date":        ir.Date{},
		"time":        ir.Time{Precision: 6},
		"timetz":      ir.Time{WithTimeZone: true},
		"text":        ir.Text{},
		"varchar":     ir.Varchar{Length: 32},
		"char":        ir.Char{Length: 20},
		"integer":     ir.Integer{Width: 64},
	}
	shapes := map[string]string{
		"datetime": "datetime('now')", "keyword": "CURRENT_TIMESTAMP",
		"isoz": "strftime('%Y-%m-%dT%H:%M:%SZ','now')",
		"date": "CURRENT_DATE", "time": "(time('now'))",
	}
	dtFmt := "(DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%d %H:%i:%s'))"
	want := map[string]string{ // "<type>/<shape>" → body; absent = dropped
		"timestamptz/datetime": "CURRENT_TIMESTAMP(6)", "timestamptz/keyword": "CURRENT_TIMESTAMP(6)",
		"timestamptz/isoz":   "CURRENT_TIMESTAMP(6)",
		"timestamp/datetime": "(UTC_TIMESTAMP())", "timestamp/keyword": "(UTC_TIMESTAMP())",
		"timestamp/isoz": "(UTC_TIMESTAMP())", "timestamp/date": "(CAST(UTC_DATE() AS DATETIME))",
		"datetime/datetime": "(UTC_TIMESTAMP())", "datetime/keyword": "(UTC_TIMESTAMP())",
		"datetime/isoz": "(UTC_TIMESTAMP())", "datetime/date": "(CAST(UTC_DATE() AS DATETIME))",
		"date/datetime": "(UTC_DATE())", "date/keyword": "(UTC_DATE())", "date/isoz": "(UTC_DATE())",
		"date/date": "(UTC_DATE())",
		"time/time": "(UTC_TIME())",
	}
	for _, ty := range []string{"text", "varchar", "char"} {
		want[ty+"/datetime"] = dtFmt
		want[ty+"/keyword"] = dtFmt
		want[ty+"/isoz"] = "(DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%sZ'))"
		want[ty+"/date"] = "(DATE_FORMAT(UTC_DATE(), '%Y-%m-%d'))"
		want[ty+"/time"] = "(TIME_FORMAT(UTC_TIME(), '%H:%i:%s'))"
	}
	modern := mysqlEmitter{lobDefaults: lobDefaultMySQL}
	legacy := mysqlEmitter{lobDefaults: lobDefaultNone}
	for tyName, ty := range types {
		for shName, expr := range shapes {
			key := tyName + "/" + shName
			d := ir.DefaultExpression{Expr: expr, Dialect: sqliteSourceDialect}
			body, lost, handled := modern.sqliteNowDefaultMySQL(d, ty)
			if !handled {
				t.Fatalf("%s: not handled", key)
			}
			exp, mapped := want[key]
			switch {
			case mapped && body != exp:
				t.Errorf("%s: got (%q, lost %q); want %q", key, body, lost, exp)
			case !mapped && (body != "" || lost == ""):
				t.Errorf("%s: got %q; want a loud drop", key, body)
			}
			if body != "" && strings.Contains(strings.ToUpper(body), "NOW()") {
				t.Errorf("%s: %q reads the session-zone clock", key, body)
			}
			lbody, llost, _ := legacy.sqliteNowDefaultMySQL(d, ty)
			if tyName == "timestamptz" && mapped {
				if lbody != exp {
					t.Errorf("%s on a pre-8.0.13 target: got %q; the zoned TIMESTAMP keyword needs no expression support", key, lbody)
				}
			} else if lbody != "" || llost == "" {
				t.Errorf("%s on a pre-8.0.13 target: got %q; want a loud drop, never the session-zone keyword", key, lbody)
			}
		}
	}
	// The epoch form is zone-free and keeps its prior handling.
	if _, _, handled := modern.sqliteNowDefaultMySQL(ir.DefaultExpression{Expr: "strftime('%s','now')", Dialect: sqliteSourceDialect}, ir.Integer{Width: 64}); handled {
		t.Error("the strftime epoch form was claimed; it is out of scope")
	}
}

// TestEmitColumnDef_SQLiteNowDefault_MySQL is the column-emit pin: the UTC
// body reaches the DDL on a modern target (in place of the pre-GC-39
// session-zone CURRENT_TIMESTAMP, and of the bare CURRENT_DATE MySQL
// rejected), and a legacy target drops it with a WARN naming the column.
func TestEmitColumnDef_SQLiteNowDefault_MySQL(t *testing.T) {
	col := &ir.Column{
		Name: "d", Type: ir.Date{}, Nullable: true,
		Default: ir.DefaultExpression{Expr: "CURRENT_DATE", Dialect: sqliteSourceDialect},
	}
	def, err := mysqlEmitter{lobDefaults: lobDefaultMySQL}.emitColumnDef("ev", col)
	if err != nil {
		t.Fatalf("emitColumnDef: %v", err)
	}
	if !strings.Contains(def, "DEFAULT (UTC_DATE())") {
		t.Errorf("column def = %q; want DEFAULT (UTC_DATE())", def)
	}

	var buf logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)
	def, err = mysqlEmitter{lobDefaults: lobDefaultNone}.emitColumnDef("ev", col)
	if err != nil {
		t.Fatalf("emitColumnDef (legacy): %v", err)
	}
	if strings.Contains(def, "DEFAULT") {
		t.Errorf("legacy column def = %q; want no DEFAULT", def)
	}
	for _, w := range []string{"dropped SQLite DEFAULT", "CURRENT_DATE", "`d`", "8.0.13"} {
		if !strings.Contains(buf.String(), w) {
			t.Errorf("drop WARN %q missing %q", buf.String(), w)
		}
	}
}
