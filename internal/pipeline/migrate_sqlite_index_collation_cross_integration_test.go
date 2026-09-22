//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-5 end to end: a SQLite source key whose column compares under NOCASE
// refuses 'A@X' after 'a@x'; every target must either refuse the same row
// or the run must have refused BEFORE any data moved. Before the fix the
// reader carried no index-column collation, so GC-22's now-completing
// SQLite→SQLite migrate (and the PG/MySQL ones that always completed) landed
// a BINARY unique that admits both rows, at exit 0.
//
// Three uniqueness-enforcing shapes — inline UNIQUE, composite table-level
// UNIQUE, and a NOCASE TEXT PRIMARY KEY — each in its own source file so a
// refusal names one shape; plus a NON-unique NOCASE index, which every
// target must accept (SQLite enforcing it verbatim, PG/MySQL dropping it
// with the INDEX-COLLATION-DROPPED WARN).
//
// The independent expected value is the source's own measured behaviour on
// the probe pair, re-measured on the target.

package pipeline

import (
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// collationShape is one seeded source: its DDL, the INSERT template the
// case-variant probe uses on the SOURCE (SQLite spelling) and on the TARGET
// (per-target spelling, `?` replaced by the literal), and whether the key
// enforces uniqueness (the shapes PG/MySQL must refuse).
type collationShape struct {
	name       string
	ddl        string
	table      string
	sqliteIns  string
	pgIns      string
	mysqlIns   string
	enforcing  bool
	wantRefuse []string // substrings the PG/MySQL refusal must carry
	// probeA / probeB are the two literals the probe inserts; empty means
	// the case-variant pair 'a@x' / 'A@X'.
	probeA, probeB string
}

// probes returns the shape's two probe literals.
func (s collationShape) probes() (a, b string) {
	if s.probeA == "" {
		return "'a@x'", "'A@X'"
	}
	return s.probeA, s.probeB
}

var collationShapes = []collationShape{
	{
		name:       "inline_unique",
		ddl:        `CREATE TABLE ci_inline (id INTEGER PRIMARY KEY, email TEXT COLLATE NOCASE UNIQUE, v TEXT NOT NULL)`,
		table:      "ci_inline",
		sqliteIns:  `INSERT INTO ci_inline (email, v) VALUES (?, 'x')`,
		pgIns:      `INSERT INTO public.ci_inline (email, v) VALUES (?, 'x')`,
		mysqlIns:   `INSERT INTO ci_inline (email, v) VALUES (?, 'x')`,
		enforcing:  true,
		wantRefuse: []string{"email", "NOCASE", "uniqueness"},
	},
	{
		name:       "composite_unique",
		ddl:        `CREATE TABLE ci_comp (id INTEGER PRIMARY KEY, email TEXT NOT NULL, v TEXT NOT NULL, UNIQUE (email COLLATE NOCASE, v))`,
		table:      "ci_comp",
		sqliteIns:  `INSERT INTO ci_comp (email, v) VALUES (?, 'same')`,
		pgIns:      `INSERT INTO public.ci_comp (email, v) VALUES (?, 'same')`,
		mysqlIns:   `INSERT INTO ci_comp (email, v) VALUES (?, 'same')`,
		enforcing:  true,
		wantRefuse: []string{"email", "NOCASE", "uniqueness"},
	},
	{
		name:       "nocase_text_pk",
		ddl:        `CREATE TABLE ci_pk (email TEXT COLLATE NOCASE PRIMARY KEY, v TEXT NOT NULL)`,
		table:      "ci_pk",
		sqliteIns:  `INSERT INTO ci_pk (email, v) VALUES (?, 'x')`,
		pgIns:      `INSERT INTO public.ci_pk (email, v) VALUES (?, 'x')`,
		mysqlIns:   `INSERT INTO ci_pk (email, v) VALUES (?, 'x')`,
		enforcing:  true,
		wantRefuse: []string{"email", "NOCASE", "primary key"},
	},
	{
		// The non-unique shape indexes an INTEGER column and probes with
		// numbers: the property pinned is "no target refuses anything", and
		// a TEXT column under any MySQL key hits the pre-existing errno-1170
		// gap (a SQLite TEXT carries no length; MySQL cannot index LONGTEXT),
		// which is filed separately and would mask this pin.
		name:      "nonunique_index",
		ddl:       `CREATE TABLE ci_idx (id INTEGER PRIMARY KEY, n INTEGER NOT NULL, v TEXT NOT NULL); CREATE INDEX ci_idx_n ON ci_idx (n COLLATE NOCASE)`,
		table:     "ci_idx",
		sqliteIns: `INSERT INTO ci_idx (n, v) VALUES (?, 'x')`,
		pgIns:     `INSERT INTO public.ci_idx (n, v) VALUES (?, 'x')`,
		mysqlIns:  `INSERT INTO ci_idx (n, v) VALUES (?, 'x')`,
		probeA:    "1",
		probeB:    "2",
	},
}

// seedCollationShape writes one shape to a fresh SQLite file and MEASURES
// whether the source refuses the case-variant on a scratch copy.
func seedCollationShape(t *testing.T, s collationShape) (path string, sourceRefuses bool) {
	t.Helper()
	path = filepath.Join(t.TempDir(), s.name+".db")
	for _, p := range []string{path, filepath.Join(t.TempDir(), s.name+"-probe.db")} {
		db, err := sql.Open("sqlite", p)
		if err != nil {
			t.Fatal(err)
		}
		for _, stmt := range strings.Split(s.ddl, ";") {
			if strings.TrimSpace(stmt) == "" {
				continue
			}
			if _, err := db.ExecContext(context.Background(), stmt); err != nil {
				t.Fatalf("seed %s: %v", s.name, err)
			}
		}
		if p != path {
			sourceRefuses = caseVariantRefused(t, db, s.sqliteIns, s)
		}
		_ = db.Close()
	}
	return path, sourceRefuses
}

// caseVariantRefused inserts the shape's two probe literals (by default
// 'a@x' then 'A@X') through insTmpl and reports whether the second was
// refused as a uniqueness violation.
func caseVariantRefused(t *testing.T, db *sql.DB, insTmpl string, s collationShape) bool {
	t.Helper()
	ctx := ctx2min(t)
	a, b := s.probes()
	if _, err := db.ExecContext(ctx, strings.Replace(insTmpl, "?", a, 1)); err != nil {
		t.Fatalf("first probe insert: %v", err)
	}
	_, err := db.ExecContext(ctx, strings.Replace(insTmpl, "?", b, 1))
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "unique") && !strings.Contains(msg, "duplicate") {
		t.Fatalf("second probe insert refused for an unrelated reason: %v", err)
	}
	return true
}

// TestMigrate_SQLiteIndexCollation_ToSQLite: the SQLite target enforces the
// source's collation verbatim — every shape migrates and measures the same.
func TestMigrate_SQLiteIndexCollation_ToSQLite(t *testing.T) {
	sqliteEng, _ := engines.Get("sqlite")
	for _, s := range collationShapes {
		t.Run(s.name, func(t *testing.T) {
			src, sourceRefuses := seedCollationShape(t, s)
			if s.enforcing && !sourceRefuses {
				t.Fatal("premise: the source did not refuse the case-variant")
			}
			dst := filepath.Join(t.TempDir(), "target.db")
			mig := &Migrator{Source: sqliteEng, Target: sqliteEng, SourceDSN: src, TargetDSN: dst}
			if err := mig.Run(ctx2min(t)); err != nil {
				t.Fatalf("Migrator.Run: %v", err)
			}
			db, err := sql.Open("sqlite", dst)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			if got := caseVariantRefused(t, db, s.sqliteIns, s); got != sourceRefuses {
				t.Errorf("target refuses case-variant = %v; source = %v — the collation was not carried", got, sourceRefuses)
			}
			// The non-unique index's collation is emitted too: re-read it.
			if !s.enforcing {
				sr, err := sqliteEng.OpenSchemaReader(ctx2min(t), dst)
				if err != nil {
					t.Fatal(err)
				}
				defer migcore.CloseIf(sr)
				back, err := sr.ReadSchema(ctx2min(t))
				if err != nil {
					t.Fatal(err)
				}
				tbl := findTable(back, s.table)
				if tbl == nil || len(tbl.Indexes) != 1 || tbl.Indexes[0].Columns[0].Collation != "NOCASE" {
					t.Errorf("target index lost its NOCASE collation: %+v", tbl)
				}
			}
		})
	}
}

// runCollationRefusalLeg migrates every shape to a PG or MySQL target: the
// uniqueness-enforcing ones must be REFUSED before the table exists, naming
// the column and collation; the non-unique one must land with the WARN
// marker and, having dropped the collation, ACCEPT the case-variant (the
// documented, warned consequence).
func runCollationRefusalLeg(t *testing.T, targetName, targetDSN, driver string, tableExists func(string) bool, insFor func(collationShape) string) {
	t.Helper()
	sqliteEng, _ := engines.Get("sqlite")
	tgtEng, _ := engines.Get(targetName)
	for _, s := range collationShapes {
		t.Run(s.name, func(t *testing.T) {
			src, sourceRefuses := seedCollationShape(t, s)
			logBuf := &logcapture.Buffer{}
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
			defer slog.SetDefault(prev)

			mig := &Migrator{Source: sqliteEng, Target: tgtEng, SourceDSN: src, TargetDSN: targetDSN}
			err := mig.Run(ctx2min(t))
			if s.enforcing {
				if !sourceRefuses {
					t.Fatal("premise: the source did not refuse the case-variant")
				}
				if err == nil {
					t.Fatalf("%s: the run completed although the target cannot enforce NOCASE on a %s", targetName, s.name)
				}
				for _, want := range s.wantRefuse {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("refusal does not name %q: %v", want, err)
					}
				}
				if tableExists(s.table) {
					t.Errorf("table %s exists on the target: the refusal arrived AFTER data moved", s.table)
				}
				return
			}
			if err != nil {
				t.Fatalf("non-unique NOCASE index must migrate with a WARN, got refusal: %v", err)
			}
			if !strings.Contains(logBuf.String(), "INDEX-COLLATION-DROPPED") {
				t.Errorf("no INDEX-COLLATION-DROPPED WARN in the log:\n%s", logBuf.String())
			}
			db, err := sql.Open(driver, targetDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			if caseVariantRefused(t, db, insFor(s), s) {
				t.Errorf("a NON-unique index must not refuse anything on the target")
			}
		})
	}
}

func TestMigrate_SQLiteIndexCollation_ToPostgres(t *testing.T) {
	_, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	exists := func(table string) bool {
		return pgQueryOne[bool](t, pgTarget, `SELECT to_regclass('public.'||quote_ident($1)) IS NOT NULL`, table)
	}
	runCollationRefusalLeg(t, "postgres", pgTarget, "pgx", exists, func(s collationShape) string { return s.pgIns })
}

func TestMigrate_SQLiteIndexCollation_ToMySQL(t *testing.T) {
	_, myTarget, cleanup := startMySQL(t)
	defer cleanup()
	exists := func(table string) bool {
		db, err := sql.Open("mysql", myTarget)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		var n int
		if err := db.QueryRowContext(
			ctx2min(t),
			`SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, table,
		).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}
	runCollationRefusalLeg(t, "mysql", myTarget, "mysql", exists, func(s collationShape) string { return s.mysqlIns })
}
