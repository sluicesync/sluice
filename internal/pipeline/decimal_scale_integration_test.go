//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// GC-37 (c): a Postgres `numeric` value with more fractional digits than the
// MySQL-family target column's scale (an unconstrained numeric lands as
// DECIMAL(65,30)) was ROUNDED by the server with only a Note — measured
// silent on the CDC applier (serial and lanes); the bulk-copy writers (LOAD
// DATA and batched INSERT) were already loud, because they refuse a
// strict-mode write whose warning list is non-empty. The fix refuses such a
// value client-side (DECIMAL-SCALE-EXCEEDED) on every lane; the rest of the
// family must stay exact.
//
// Independent expected value: the SOURCE's own `v::text` rendering, compared
// to the target's `CAST(v AS CHAR)` after trailing fractional zeros are
// trimmed on both sides (DECIMAL(65,30) pads to scale; a trimmed pad loses
// nothing, a rounded digit still differs).

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// dsCell is one value-family cell. refuse names what the run must fail with
// instead of landing a value ("" = must land exact).
type dsCell struct {
	name, def, val, refuse string
}

var dsCells = []dsCell{
	// Must refuse: significant fractional digits past scale 30.
	{"s31", "NUMERIC", "0.1234567890123456789012345678901", "DECIMAL-SCALE-EXCEEDED"},
	{"nines", "NUMERIC", "-0.99999999999999999999999999999999", "DECIMAL-SCALE-EXCEEDED"},
	{"long", "NUMERIC", "3.14159265358979323846264338327950288419716939937510", "DECIMAL-SCALE-EXCEEDED"},
	{"tiny", "NUMERIC", "0.0000000000000000000000000000001", "DECIMAL-SCALE-EXCEEDED"},
	// Already loud before the fix (the server refuses): unchanged.
	{"i36", "NUMERIC", "123456789012345678901234567890123456", "1264"},
	{"nan", "NUMERIC", "'NaN'", "NaN"},
	// Must land exact.
	{"s30", "NUMERIC", "0.123456789012345678901234567890", ""},
	{"tz", "NUMERIC", "1.50000000000000000000000000000000000", ""},
	{"i35", "NUMERIC", "12345678901234567890123456789012345.1", ""},
	{"neg", "NUMERIC", "-42.125", ""},
	{"zero", "NUMERIC", "0", ""},
	{"c102", "NUMERIC(10,2)", "12345678.90", ""},
	{"arr", "NUMERIC[]", "'{0.1234567890123456789012345678901,3.14159265358979323846}'", ""},
}

// dsTrimFrac trims trailing fractional zeros (and a bare trailing point).
func dsTrimFrac(s string) string {
	if !strings.Contains(s, ".") || strings.HasPrefix(s, "[") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

// dsSourceText is the source's own rendering of the cell's row.
func dsSourceText(t *testing.T, pgDSN, table string) string {
	t.Helper()
	db, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var s sql.NullString
	if err := db.QueryRow(fmt.Sprintf("SELECT v::text FROM %s WHERE id = 1", table)).Scan(&s); err != nil {
		t.Fatalf("source read %s: %v", table, err)
	}
	return s.String
}

// dsTargetText is the target's rendering, or an error when the row is absent.
func dsTargetText(dsn, table string) (string, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", err
	}
	defer func() { _ = db.Close() }()
	var s sql.NullString
	err = db.QueryRow(fmt.Sprintf("SELECT CAST(v AS CHAR) FROM `%s` WHERE id = 1", table)).Scan(&s)
	return s.String, err
}

// dsNewDatabase creates db on the server rootDSN points at and returns its DSN.
func dsNewDatabase(t *testing.T, rootDSN, db string) string {
	t.Helper()
	c, err := sql.Open("mysql", rootDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.Exec("CREATE DATABASE IF NOT EXISTS `" + db + "`"); err != nil {
		t.Fatalf("create database: %v", err)
	}
	return strings.Replace(rootDSN, "/source_db", "/"+db, 1)
}

// dsGrade grades one cell's outcome: runErr is what the run returned.
func dsGrade(t *testing.T, lane string, c dsCell, src string, runErr error, tgtDSN, table string) {
	t.Helper()
	if c.refuse != "" {
		if runErr == nil || !strings.Contains(runErr.Error(), c.refuse) {
			got, _ := dsTargetText(tgtDSN, table)
			t.Errorf("%s %s: source %s — want a refusal naming %q; run returned %v, target holds [%s]",
				lane, c.name, src, c.refuse, runErr, got)
		}
		return
	}
	if runErr != nil {
		t.Errorf("%s %s: source %s — must land exact, run failed: %v", lane, c.name, src, runErr)
		return
	}
	got, err := dsTargetText(tgtDSN, table)
	if err != nil {
		t.Errorf("%s %s: read target: %v", lane, c.name, err)
		return
	}
	want := src
	if c.name == "arr" {
		// PG renders {a,b}; the MySQL JSON column holds ["a", "b"].
		want = `["` + strings.ReplaceAll(strings.Trim(src, "{}"), ",", `", "`) + `"]`
		got = strings.ReplaceAll(got, `","`, `", "`)
	}
	if dsTrimFrac(got) != dsTrimFrac(want) {
		t.Errorf("%s %s: target holds [%s], source [%s] — the value did not survive", lane, c.name, got, src)
	}
}

func dsMigrateLane(t *testing.T, tgtEngine string, rootDSN string) {
	t.Helper()
	pgSrc, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	for _, c := range dsCells {
		applyPGDDL(t, pgSrc, fmt.Sprintf("CREATE TABLE ds_%s (id INT PRIMARY KEY, v %s); INSERT INTO ds_%s VALUES (1, %s);", c.name, c.def, c.name, c.val))
	}
	pgEng, _ := engines.Get("postgres")
	tgt, ok := engines.Get(tgtEngine)
	if !ok {
		t.Fatalf("%s engine not registered", tgtEngine)
	}
	for _, c := range dsCells {
		table := "ds_" + c.name
		dsn := dsNewDatabase(t, rootDSN, "m_"+c.name)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		mig := &Migrator{Source: pgEng, Target: tgt, SourceDSN: pgSrc, TargetDSN: dsn, Filter: migcore.TableFilter{Include: []string{table}}}
		err := mig.Run(ctx)
		cancel()
		dsGrade(t, "migrate->"+tgtEngine, c, dsSourceText(t, pgSrc, table), err, dsn, table)
	}
}

// TestMigrate_DecimalScale_PostgresToMySQL: migrate's default LOAD DATA path
// was already loud on over-scale (its strict-mode refusal counts the Note);
// the refusal now fires client-side, naming the marker, before the wire.
func TestMigrate_DecimalScale_PostgresToMySQL(t *testing.T) {
	root, _, cleanup := startMySQL(t)
	defer cleanup()
	dsMigrateLane(t, "mysql", root)
}

func TestMigrate_DecimalScale_PostgresToMariaDB(t *testing.T) {
	root, _, cleanup := startMariaDB(t)
	defer cleanup()
	dsMigrateLane(t, "mariadb", root)
}

// dsSyncLane runs the CDC applier. The exact cells ride one stream (one table
// per cell, each value inserted after the cold start); each refusal cell gets
// its own stream, because a refusal ends it.
func dsSyncLane(t *testing.T, concurrency int) {
	t.Helper()
	pgSrc, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	root, _, myCleanup := startMySQL(t)
	defer myCleanup()
	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")

	run := func(tag string, cells []dsCell) {
		var tables []string
		for _, c := range cells {
			tbl := fmt.Sprintf("ds%d_%s", concurrency, c.name)
			tables = append(tables, tbl)
			applyPGDDL(t, pgSrc, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, v %s)", tbl, c.def))
		}
		dsn := dsNewDatabase(t, root, fmt.Sprintf("s%d_%s", concurrency, tag))
		id := fmt.Sprintf("ds%d%s", concurrency, strings.ReplaceAll(tag, "_", ""))
		streamer := &Streamer{
			Source: pgEng, Target: myEng, SourceDSN: pgSrc, TargetDSN: dsn,
			StreamID: id, SlotName: id, PublicationName: "pub_" + id,
			Filter:           migcore.TableFilter{Include: tables},
			ApplyConcurrency: concurrency,
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runErr := make(chan error, 1)
		go func() { runErr <- streamer.Run(ctx) }()
		for _, tbl := range tables {
			deadline := time.Now().Add(2 * time.Minute)
			for {
				db, _ := sql.Open("mysql", dsn)
				var n int
				err := db.QueryRow("SELECT COUNT(*) FROM `" + tbl + "`").Scan(&n)
				_ = db.Close()
				if err == nil {
					break
				}
				select {
				case e := <-runErr:
					t.Fatalf("sync %s: stream ended during cold start: %v", tag, e)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatalf("sync %s: cold start never created %s", tag, tbl)
				}
				time.Sleep(300 * time.Millisecond)
			}
		}
		for i, c := range cells {
			applyPGDDL(t, pgSrc, fmt.Sprintf("INSERT INTO %s VALUES (1, %s)", tables[i], c.val))
		}
		lane := fmt.Sprintf("sync(conc=%d)", concurrency)
		if len(cells) == 1 && cells[0].refuse != "" {
			var err error
			select {
			case err = <-runErr:
			case <-time.After(90 * time.Second):
				err = nil
			}
			dsGrade(t, lane, cells[0], dsSourceText(t, pgSrc, tables[0]), err, dsn, tables[0])
			return
		}
		deadline := time.Now().Add(90 * time.Second)
		for i, c := range cells {
			for {
				if _, err := dsTargetText(dsn, tables[i]); err == nil {
					break
				}
				select {
				case e := <-runErr:
					t.Fatalf("sync %s: stream ended before %s landed: %v", tag, c.name, e)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatalf("sync %s: %s never landed", tag, c.name)
				}
				time.Sleep(300 * time.Millisecond)
			}
			dsGrade(t, lane, c, dsSourceText(t, pgSrc, tables[i]), nil, dsn, tables[i])
		}
	}

	var exact []dsCell
	for _, c := range dsCells {
		if c.refuse == "" {
			exact = append(exact, c)
		}
	}
	run("exact", exact)
	for _, c := range dsCells {
		if c.refuse == "DECIMAL-SCALE-EXCEEDED" {
			run(c.name, []dsCell{c})
		}
	}
}

// TestSync_DecimalScale_PostgresToMySQL_Serial / _Lanes: the CDC applier was
// the SILENT arm (measured: s31 landed ...890, nines landed -1.000...0).
func TestSync_DecimalScale_PostgresToMySQL_Serial(t *testing.T) { dsSyncLane(t, 1) }
func TestSync_DecimalScale_PostgresToMySQL_Lanes(t *testing.T)  { dsSyncLane(t, 0) }

// TestMigrate_DecimalScale_DefaultLiteral_PostgresToMySQL: a DEFAULT literal
// past the target scale was stored ROUNDED at CREATE TABLE (Note 1265), so
// every row inserted on the target without the column took the rounded
// value. It is now refused at the emit; an at-scale DEFAULT still lands.
func TestMigrate_DecimalScale_DefaultLiteral_PostgresToMySQL(t *testing.T) {
	pgSrc, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	root, _, myCleanup := startMySQL(t)
	defer myCleanup()
	applyPGDDL(t, pgSrc, `
		CREATE TABLE dd_over (id INT PRIMARY KEY, v NUMERIC DEFAULT 0.1234567890123456789012345678901);
		CREATE TABLE dd_ok   (id INT PRIMARY KEY, v NUMERIC DEFAULT 0.123456789012345678901234567890);
	`)
	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	for _, tc := range []struct {
		table  string
		refuse bool
	}{{"dd_over", true}, {"dd_ok", false}} {
		dsn := dsNewDatabase(t, root, "d_"+tc.table)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		err := (&Migrator{Source: pgEng, Target: myEng, SourceDSN: pgSrc, TargetDSN: dsn, Filter: migcore.TableFilter{Include: []string{tc.table}}}).Run(ctx)
		cancel()
		if tc.refuse {
			if err == nil || !strings.Contains(err.Error(), "DECIMAL-SCALE-EXCEEDED") {
				t.Errorf("%s: over-scale DEFAULT: migrate returned %v; want the DECIMAL-SCALE-EXCEEDED refusal", tc.table, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: at-scale DEFAULT refused: %v", tc.table, err)
		}
		db, _ := sql.Open("mysql", dsn)
		var d sql.NullString
		qerr := db.QueryRow("SELECT COLUMN_DEFAULT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'v'", tc.table).Scan(&d)
		_ = db.Close()
		if qerr != nil || dsTrimFrac(d.String) != "0.12345678901234567890123456789" {
			t.Errorf("%s: target DEFAULT = [%s] (%v); want the source's 0.123456789012345678901234567890", tc.table, d.String, qerr)
		}
	}
}
