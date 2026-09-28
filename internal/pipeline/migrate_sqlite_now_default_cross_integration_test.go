//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// sqliteNowCell is one column of the GC-39 item 3 matrix: a SQLite
// current-instant DEFAULT on one column type, and how its target value is
// graded.
type sqliteNowCell struct {
	col, decl string
	// grade is "ts" (naive timestamp read as UTC), "date", "time",
	// "instant" (a zoned column, read as epoch seconds), or a Go layout the
	// stored TEXT must parse under EXACTLY — SQLite's own text shape.
	grade string
}

var sqliteNowCells = []sqliteNowCell{
	{"dt_kw", "DATETIME DEFAULT CURRENT_TIMESTAMP", "ts"},
	{"dt_fn", "DATETIME DEFAULT (datetime('now'))", "ts"},
	{"ts_kw", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "ts"},
	{"d_fn", "DATE DEFAULT (date('now'))", "date"},
	{"d_kw", "DATE DEFAULT CURRENT_DATE", "date"},
	{"t_kw", "TIME DEFAULT CURRENT_TIME", "time"},
	{"txt_dt", "TEXT DEFAULT CURRENT_TIMESTAMP", "2006-01-02 15:04:05"},
	{"txt_isoz", "TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))", "2006-01-02T15:04:05Z"},
	{"txt_d", "TEXT DEFAULT (date('now'))", "2006-01-02"},
	{"txt_t", "TEXT DEFAULT (time('now'))", "15:04:05"},
	// --infer-types promotes these by name hint from the seeded row's
	// values: all-Z values to a zoned timestamp, naive ones to a naive one.
	{"created_at", "TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))", "instant"},
	{"updated_at", "TEXT DEFAULT CURRENT_TIMESTAMP", "ts"},
}

func seedSQLiteNowDefaults(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "now.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	ddl := "CREATE TABLE ev (id INTEGER PRIMARY KEY"
	for _, c := range sqliteNowCells {
		ddl += ", " + c.col + " " + c.decl
	}
	for _, s := range []string{ddl + ")", "INSERT INTO ev (id) VALUES (1)"} {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed %q: %v", s, err)
		}
	}
	return path
}

// TestMigrate_SQLiteNowDefaults_AreUTC_OnANonUTCTarget pins GC-39 item 3.
// SQLite's 'now' is UTC, always; the pre-fix carry mapped it onto the
// target's CURRENT_TIMESTAMP family, which evaluates in the writing
// SESSION's zone, so every row inserted after the migration without the
// column stamped session wall-clock digits — measured nine hours ahead on
// an Asia/Tokyo Postgres database and on a `+09:00` MySQL session, and on
// MySQL CURRENT_DATE / CURRENT_TIME failed CREATE TABLE outright.
//
// Each cell is migrated, then a row is inserted without it under two
// non-UTC session zones (one each side of UTC). The independent expected
// value is the TEST PROCESS's clock, not the server's: a default that
// reads UTC lands within a couple of minutes of time.Now().UTC() whatever
// the session zone. Text cells must also parse under SQLite's exact text
// shape, which is what the migrated rows in that column look like.
func TestMigrate_SQLiteNowDefaults_AreUTC_OnANonUTCTarget(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		_, target, cleanup := startPostgres(t)
		defer cleanup()
		runSQLiteNowMatrix(t, "postgres", "pgx", target, []string{"Asia/Tokyo", "America/Los_Angeles"},
			func(zone string) string { return "SET TIME ZONE '" + zone + "'" },
			map[string]string{"created_at": "timestamp with time zone", "updated_at": "timestamp without time zone"},
			"SELECT data_type FROM information_schema.columns WHERE table_name = 'ev' AND column_name = $1",
			func(c sqliteNowCell) string {
				if c.grade == "instant" {
					return "SELECT EXTRACT(EPOCH FROM " + c.col + ")::text FROM ev WHERE id = $1"
				}
				return "SELECT " + c.col + "::text FROM ev WHERE id = $1"
			})
	})
	t.Run("mysql", func(t *testing.T) {
		_, target, cleanup := startMySQL(t)
		defer cleanup()
		runSQLiteNowMatrix(t, "mysql", "mysql", target, []string{"+09:00", "-07:00"},
			func(zone string) string { return "SET time_zone = '" + zone + "'" },
			map[string]string{"created_at": "timestamp", "updated_at": "datetime"},
			"SELECT data_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'ev' AND column_name = ?",
			func(c sqliteNowCell) string {
				if c.grade == "instant" {
					return "SELECT CAST(UNIX_TIMESTAMP(" + c.col + ") AS CHAR) FROM ev WHERE id = ?"
				}
				return "SELECT CAST(" + c.col + " AS CHAR) FROM ev WHERE id = ?"
			})
	})
}

func runSQLiteNowMatrix(t *testing.T, engine, driver, target string, zones []string, setZone func(string) string,
	inferred map[string]string, typeQuery string, valueQuery func(sqliteNowCell) string,
) {
	t.Helper()
	src, _ := engines.Get("sqlite")
	tgt, _ := engines.Get(engine)
	mig := &Migrator{Source: src, Target: tgt, SourceDSN: seedSQLiteNowDefaults(t), TargetDSN: target, InferTypes: true}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite→%s): %v", engine, err)
	}
	db, err := sql.Open(driver, target)
	if err != nil {
		t.Fatalf("open %s: %v", engine, err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1) // the session zone must hold for the INSERT
	ctx := ctx2min(t)

	// Non-vacuity: the inference cells really are the zoned / naive
	// timestamp types they are meant to exercise.
	for col, want := range inferred {
		var got string
		if err := db.QueryRowContext(ctx, typeQuery, col).Scan(&got); err != nil || got != want {
			t.Fatalf("%s type = %q (err %v); want %q — --infer-types did not promote it, so the cell grades nothing", col, got, err, want)
		}
	}

	const tol = 2 * time.Minute
	for i, zone := range zones {
		id := i + 2
		if _, err := db.ExecContext(ctx, setZone(zone)); err != nil {
			t.Fatalf("set zone %s: %v", zone, err)
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO ev (id) VALUES ("+strconv.Itoa(id)+")"); err != nil {
			t.Fatalf("insert under %s: %v", zone, err)
		}
		now := time.Now().UTC()
		for _, c := range sqliteNowCells {
			var got sql.NullString
			if err := db.QueryRowContext(ctx, valueQuery(c), id).Scan(&got); err != nil {
				t.Fatalf("read %s: %v", c.col, err)
			}
			if !got.Valid {
				t.Errorf("%s under %s: NULL — the DEFAULT was not carried", c.col, zone)
				continue
			}
			if msg := gradeSQLiteNowValue(c.grade, got.String, now, tol); msg != "" {
				t.Errorf("%s (%s) under session zone %s: stored %q, %s (process UTC now %s)",
					c.col, c.decl, zone, got.String, msg, now.Format(time.DateTime))
			}
		}
	}
}

func gradeSQLiteNowValue(grade, got string, now time.Time, tol time.Duration) string {
	near := func(v time.Time) string {
		if d := v.Sub(now); d < -tol || d > tol {
			return "which is " + d.Round(time.Second).String() + " off UTC — the session zone's digits, not UTC"
		}
		return ""
	}
	switch grade {
	case "ts":
		v, err := time.Parse(time.DateTime, got)
		if err != nil {
			return "not a timestamp: " + err.Error()
		}
		return near(v)
	case "instant":
		sec, err := strconv.ParseFloat(got, 64)
		if err != nil {
			return "not an epoch: " + err.Error()
		}
		return near(time.Unix(int64(sec), 0).UTC())
	case "date":
		if got != now.Format(time.DateOnly) && got != now.Add(-tol).Format(time.DateOnly) {
			return "not today's UTC date " + now.Format(time.DateOnly)
		}
		return ""
	case "time":
		v, err := time.Parse(time.TimeOnly, got)
		if err != nil {
			return "not a time: " + err.Error()
		}
		d := time.Duration(v.Hour())*time.Hour + time.Duration(v.Minute())*time.Minute + time.Duration(v.Second())*time.Second -
			(time.Duration(now.Hour())*time.Hour + time.Duration(now.Minute())*time.Minute + time.Duration(now.Second())*time.Second)
		for d > 12*time.Hour {
			d -= 24 * time.Hour
		}
		for d < -12*time.Hour {
			d += 24 * time.Hour
		}
		if d < -tol || d > tol {
			return "which is " + d.Round(time.Second).String() + " off the UTC time of day"
		}
		return ""
	default: // an exact SQLite text layout
		v, err := time.Parse(grade, got)
		if err != nil || v.Format(grade) != got {
			return "not SQLite's text shape " + grade
		}
		switch grade {
		case time.DateOnly:
			return gradeSQLiteNowValue("date", got, now, tol)
		case time.TimeOnly:
			return gradeSQLiteNowValue("time", got, now, tol)
		}
		return near(v)
	}
}
