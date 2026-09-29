// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/translate"
)

// TestSQLiteNowDefaultPG_ShapeByColumnType pins the whole shape × column
// type matrix of [sqliteNowDefaultPG] (GC-39 item 3), including every
// drop cell. Every rendered spelling except timestamptz's and the epoch's
// reads the UTC wall clock — never a session-zone CURRENT_* keyword; the
// integration pin TestMigrate_SQLiteNowDefaults_AreUTC_OnANonUTCTarget
// grades the values against a real server's own UTC clock.
func TestSQLiteNowDefaultPG_ShapeByColumnType(t *testing.T) {
	utcSecond := "pg_catalog.date_trunc('second', " + utcNowSQL + ")"
	types := map[string]ir.Type{
		"timestamp":   ir.Timestamp{},
		"timestamptz": ir.Timestamp{WithTimeZone: true},
		"datetime":    ir.DateTime{},
		"date":        ir.Date{},
		"time":        ir.Time{},
		"timetz":      ir.Time{WithTimeZone: true},
		"text":        ir.Text{},
		"varchar":     ir.Varchar{Length: 32},
		"char":        ir.Char{Length: 19},
		"integer":     ir.Integer{Width: 64},
	}
	shapes := map[string]translate.SQLiteNowShape{
		"datetime": translate.SQLiteNowDateTime, "isoz": translate.SQLiteNowISOZ,
		"date": translate.SQLiteNowDate, "time": translate.SQLiteNowTime, "epoch": translate.SQLiteNowEpoch,
	}
	epoch := "pg_catalog.floor(extract(epoch from pg_catalog.now()))"
	toChar := func(f string) string { return "pg_catalog.to_char(" + utcNowSQL + ", '" + f + "')" }
	want := map[string]string{ // "<type>/<shape>" → spelling; absent = dropped
		"timestamp/datetime": utcSecond, "timestamp/isoz": utcSecond,
		"timestamp/date":    "pg_catalog.date_trunc('day', " + utcNowSQL + ")",
		"datetime/datetime": utcSecond, "datetime/isoz": utcSecond,
		"datetime/date":        "pg_catalog.date_trunc('day', " + utcNowSQL + ")",
		"timestamptz/datetime": "pg_catalog.date_trunc('second', pg_catalog.now())",
		"timestamptz/isoz":     "pg_catalog.date_trunc('second', pg_catalog.now())",
		"date/datetime":        "(" + utcNowSQL + ")::date", "date/isoz": "(" + utcNowSQL + ")::date",
		"date/date": "(" + utcNowSQL + ")::date",
		"time/time": utcSecond + "::time",
	}
	for _, ty := range []string{"text", "varchar", "char"} {
		want[ty+"/datetime"] = toChar("YYYY-MM-DD HH24:MI:SS")
		want[ty+"/isoz"] = toChar(`YYYY-MM-DD"T"HH24:MI:SS"Z"`)
		want[ty+"/date"] = toChar("YYYY-MM-DD")
		want[ty+"/time"] = toChar("HH24:MI:SS")
	}
	for _, ty := range []string{"integer", "text", "varchar", "char"} {
		want[ty+"/epoch"] = epoch
	}
	for tyName, ty := range types {
		for shName, sh := range shapes {
			key := tyName + "/" + shName
			got, ok := sqliteNowDefaultPG(sh, ty)
			exp, mapped := want[key]
			switch {
			case mapped && (!ok || got != exp):
				t.Errorf("%s: got (%q, %v); want %q", key, got, ok, exp)
			case !mapped && ok:
				t.Errorf("%s: got %q; want a loud drop (no faithful spelling)", key, got)
			}
			if ok && strings.Contains(strings.ToUpper(got), "CURRENT_") {
				t.Errorf("%s: %q reads a session-zone CURRENT_* keyword", key, got)
			}
		}
	}
}

// TestEmitDefault_SQLiteNowUnmappableDropsLoudly: a recognised
// current-instant DEFAULT on a column type with no faithful spelling must
// take the loud drop, never fall through to a verbatim CURRENT_* keyword.
func TestEmitDefault_SQLiteNowUnmappableDropsLoudly(t *testing.T) {
	var buf logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	col := &ir.Column{
		Name:    "opened",
		Type:    ir.Time{},
		Default: ir.DefaultExpression{Expr: "CURRENT_DATE", Dialect: "sqlite"},
	}
	got, ok := emitDefault(&ir.Table{Name: "t"}, col, emitOpts{})
	if ok || got != "" {
		t.Fatalf("emitDefault = (%q, %v); want a drop", got, ok)
	}
	if !strings.Contains(buf.String(), "dropped non-portable SQLite DEFAULT") {
		t.Errorf("drop was not loud: log %q", buf.String())
	}
}

// TestEmitDefault_SQLiteNonPortableWarns is the emit-level pin: a column with
// a non-portable SQLite DEFAULT emits NO DEFAULT clause (ok=false) AND the
// loud per-column warn fires naming the table, column, and dropped
// expression. The warn (not silent drop) is the load-bearing half of the
// loud-failure tenet on this path.
func TestEmitDefault_SQLiteNonPortableWarns(t *testing.T) {
	var buf logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	table := &ir.Table{Name: "flyway_schema_history"}
	col := &ir.Column{
		Name:    "installed_on",
		Type:    ir.Text{},
		Default: ir.DefaultExpression{Expr: "julianday('now')", Dialect: "sqlite"},
	}

	got, ok := emitDefault(table, col, emitOpts{})
	if ok || got != "" {
		t.Fatalf("emitDefault = (%q, %v); want (\"\", false) — the non-portable default must be dropped", got, ok)
	}

	logged := buf.String()
	for _, want := range []string{
		"dropped non-portable SQLite DEFAULT",
		"flyway_schema_history",
		"installed_on",
		"julianday('now')",
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("warn log %q missing %q (the drop must be LOUD and name table/column/expression)", logged, want)
		}
	}
}

// TestEmitColumnDef_SQLiteDefaultPortableAndDrop pins the column-emit
// integration of both halves: a portable SQLite default lands as a valid PG
// DEFAULT keyword, while a non-portable one yields a column with NO DEFAULT
// clause (so CREATE TABLE succeeds instead of aborting the whole migration).
func TestEmitColumnDef_SQLiteDefaultPortableAndDrop(t *testing.T) {
	table := &ir.Table{Name: "t"}

	portable := &ir.Column{
		Name:     "created_at",
		Type:     ir.Timestamp{},
		Nullable: false,
		Default:  ir.DefaultExpression{Expr: "(datetime('now'))", Dialect: "sqlite"},
	}
	def, err := emitColumnDef(table, portable, emitOpts{})
	if err != nil {
		t.Fatalf("emitColumnDef(portable): %v", err)
	}
	if want := "DEFAULT pg_catalog.date_trunc('second', " + utcNowSQL + ")"; !strings.Contains(def, want) {
		t.Errorf("portable column def = %q; want it to contain %q", def, want)
	}

	nonPortable := &ir.Column{
		Name:     "installed_on",
		Type:     ir.Text{},
		Nullable: false,
		Default:  ir.DefaultExpression{Expr: "randomblob(16)", Dialect: "sqlite"},
	}
	def, err = emitColumnDef(table, nonPortable, emitOpts{})
	if err != nil {
		t.Fatalf("emitColumnDef(non-portable): %v", err)
	}
	if strings.Contains(def, "DEFAULT") {
		t.Errorf("non-portable column def = %q; want NO DEFAULT clause (dropped)", def)
	}
	if !strings.Contains(def, "NOT NULL") {
		t.Errorf("non-portable column def = %q; want NOT NULL preserved (only the DEFAULT is dropped)", def)
	}
}
