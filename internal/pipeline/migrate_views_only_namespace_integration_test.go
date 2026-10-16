//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 317: a namespace (MySQL database / Postgres schema) that holds views
// but no tables used to return from the per-namespace migrate on "source
// schema has no tables; nothing to migrate" before its views phase, so its
// views were never created and the run exited 0. These pins grade the
// TARGET: each view must exist there and return exactly the rows the
// SOURCE view returns. The independent expected value is the source
// server's own evaluation of the same view, never sluice's view list.
//
// The fan-out pins deliberately include a views-only namespace that sorts
// BEFORE the namespace holding the tables its views read (a_rpt over
// m_app), a mixed namespace in the same position (b_mix), and one that
// sorts after (z_rpt), so the cross-namespace creation order is pinned
// too, not only the early return.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// viewRows returns the view's single integer column as a sorted,
// comma-joined string, or the query error. Used against BOTH sides, so a
// missing target view is an error and a wrong binding is a row mismatch.
func viewRows(ctx context.Context, db *sql.DB, qualified string) (string, error) {
	rows, err := db.QueryContext(ctx, "SELECT id FROM "+qualified+" ORDER BY id")
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return "", err
		}
		out = append(out, fmt.Sprint(id))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return strings.Join(out, ","), nil
}

// assertViewsMatchSource checks every named view on the target returns
// what the same view returns on the source. An empty source result fails
// the test (anti-vacuity: a view over an empty table would match a
// target view over anything empty).
func assertViewsMatchSource(t *testing.T, ctx context.Context, src, dst *sql.DB, views []string) {
	t.Helper()
	for _, v := range views {
		want, err := viewRows(ctx, src, v)
		if err != nil {
			t.Fatalf("source view %s: %v", v, err)
		}
		if want == "" {
			t.Fatalf("source view %s returned no rows; the pin would be vacuous", v)
		}
		got, err := viewRows(ctx, dst, v)
		if err != nil {
			t.Errorf("Bug 317: target view %s is missing or unreadable after an rc-0 migrate: %v", v, err)
			continue
		}
		if got != want {
			t.Errorf("target view %s returned %q; the source view returns %q", v, got, want)
		}
	}
}

// seedMySQLViewsOnlyFanout lays out the fan-out shape on a MySQL server:
// m_app holds the table, a_rpt and z_rpt hold only views over it, and
// b_mix holds its own table plus a view over m_app.
func seedMySQLViewsOnlyFanout(t *testing.T, server string) {
	t.Helper()
	applyMySQLDDL(t, server, `
		CREATE DATABASE m_app;
		CREATE TABLE m_app.t (id INT PRIMARY KEY, k INT NOT NULL);
		INSERT INTO m_app.t VALUES (1, 10), (2, 20), (3, 30);
		CREATE DATABASE a_rpt;
		CREATE VIEW a_rpt.v AS SELECT id FROM m_app.t WHERE k >= 20;
		CREATE DATABASE b_mix;
		CREATE TABLE b_mix.own (id INT PRIMARY KEY);
		INSERT INTO b_mix.own VALUES (7);
		CREATE VIEW b_mix.vm AS SELECT id FROM m_app.t WHERE k <= 20;
		CREATE DATABASE z_rpt;
		CREATE VIEW z_rpt.v AS SELECT id FROM m_app.t;
	`)
}

// TestMigrate_ViewsOnlyNamespace_MySQLToMySQL_Fanout is the Bug 317
// repro on MySQL → MySQL --include-database.
func TestMigrate_ViewsOnlyNamespace_MySQLToMySQL_Fanout(t *testing.T) {
	srcServer, _, srcCleanup := startMySQL(t)
	defer srcCleanup()
	tgtServer, _, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	seedMySQLViewsOnlyFanout(t, srcServer)

	mysqlEng, _ := engines.Get("mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	mig := &Migrator{
		Source:         mysqlEng,
		Target:         mysqlEng,
		SourceDSN:      serverDSN(t, srcServer),
		TargetDSN:      serverDSN(t, tgtServer),
		DatabaseFilter: DatabaseFilter{Include: []string{"a_rpt", "b_mix", "m_app", "z_rpt"}},
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("multi-database Migrator.Run: %v", err)
	}

	src, err := sql.Open("mysql", serverDSN(t, srcServer))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	dst, err := sql.Open("mysql", serverDSN(t, tgtServer))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	assertViewsMatchSource(t, ctx, src, dst, []string{"a_rpt.v", "b_mix.vm", "z_rpt.v"})
}

// TestMigrate_ViewsOnlyNamespace_MySQLToMySQL_Single is the
// single-database arm: m_app is migrated first, then the views-only
// a_rpt on its own.
func TestMigrate_ViewsOnlyNamespace_MySQLToMySQL_Single(t *testing.T) {
	srcServer, _, srcCleanup := startMySQL(t)
	defer srcCleanup()
	tgtServer, _, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	seedMySQLViewsOnlyFanout(t, srcServer)
	applyMySQLDDL(t, tgtServer, `CREATE DATABASE m_app; CREATE DATABASE a_rpt;`)

	mysqlEng, _ := engines.Get("mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	for _, db := range []string{"m_app", "a_rpt"} {
		srcDSN, err := buildMySQLDSN(srcServer, db)
		if err != nil {
			t.Fatal(err)
		}
		tgtDSN, err := buildMySQLDSN(tgtServer, db)
		if err != nil {
			t.Fatal(err)
		}
		mig := &Migrator{Source: mysqlEng, Target: mysqlEng, SourceDSN: srcDSN, TargetDSN: tgtDSN}
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("Migrator.Run %s: %v", db, err)
		}
	}

	src, err := sql.Open("mysql", serverDSN(t, srcServer))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	dst, err := sql.Open("mysql", serverDSN(t, tgtServer))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	assertViewsMatchSource(t, ctx, src, dst, []string{"a_rpt.v"})
}

// TestMigrate_ViewsOnlyNamespace_MySQLToMySQL_UnresolvableIsLoud pins the
// other half of the contract: a views-only namespace whose view reads a
// database the run does not copy (and the target does not hold) must fail
// the run, never be skipped at exit 0.
func TestMigrate_ViewsOnlyNamespace_MySQLToMySQL_UnresolvableIsLoud(t *testing.T) {
	srcServer, _, srcCleanup := startMySQL(t)
	defer srcCleanup()
	tgtServer, _, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	seedMySQLViewsOnlyFanout(t, srcServer)

	mysqlEng, _ := engines.Get("mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	mig := &Migrator{
		Source:         mysqlEng,
		Target:         mysqlEng,
		SourceDSN:      serverDSN(t, srcServer),
		TargetDSN:      serverDSN(t, tgtServer),
		DatabaseFilter: DatabaseFilter{Include: []string{"a_rpt"}},
	}
	err := mig.Run(ctx)
	if err == nil {
		t.Fatal("Bug 317: migrating a views-only database whose view reads an uncopied database exited 0; want a loud views-phase failure")
	}
	if !strings.Contains(err.Error(), `view "v"`) {
		t.Fatalf("error does not name the view that could not be created: %v", err)
	}

	// The fan-out's views pass runs AFTER each namespace's migrate-state row
	// is marked complete, so a --resume finds a_rpt "already complete". That
	// short-circuit must still hand the views to the final pass: a resume
	// that exits 0 here would turn the loud failure above into the silent
	// skip this bug is about.
	mig.Resume = true
	if err := mig.Run(ctx); err == nil {
		t.Fatal("Bug 317: --resume after the views pass failed exited 0 without creating the view; want the same loud failure")
	} else if !strings.Contains(err.Error(), `view "v"`) {
		t.Fatalf("--resume error does not name the view that could not be created: %v", err)
	}
}

// seedPGViewsOnlyFanout is the Postgres twin of seedMySQLViewsOnlyFanout,
// as schemas of one database.
func seedPGViewsOnlyFanout(t *testing.T, dsn string) {
	t.Helper()
	applyPGDDL(t, dsn, `
		CREATE SCHEMA m_app;
		CREATE TABLE m_app.t (id INT PRIMARY KEY, k INT NOT NULL);
		INSERT INTO m_app.t VALUES (1, 10), (2, 20), (3, 30);
		CREATE SCHEMA a_rpt;
		CREATE VIEW a_rpt.v AS SELECT id FROM m_app.t WHERE k >= 20;
		CREATE SCHEMA b_mix;
		CREATE TABLE b_mix.own (id INT PRIMARY KEY);
		INSERT INTO b_mix.own VALUES (7);
		CREATE VIEW b_mix.vm AS SELECT id FROM m_app.t WHERE k <= 20;
		CREATE SCHEMA z_rpt;
		CREATE VIEW z_rpt.v AS SELECT id FROM m_app.t;
	`)
}

// TestMigrate_ViewsOnlyNamespace_PostgresToPostgres_Fanout is the Bug 317
// repro on PG → PG --include-schema.
func TestMigrate_ViewsOnlyNamespace_PostgresToPostgres_Fanout(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	seedPGViewsOnlyFanout(t, pgSource)

	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	mig := &Migrator{
		Source:         pgEng,
		Target:         pgEng,
		SourceDSN:      pgSource,
		TargetDSN:      pgTarget,
		DatabaseFilter: DatabaseFilter{Include: []string{"a_rpt", "b_mix", "m_app", "z_rpt"}},
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("multi-schema Migrator.Run: %v", err)
	}

	src, err := sql.Open("pgx", pgSource)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	dst, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	assertViewsMatchSource(t, ctx, src, dst, []string{"a_rpt.v", "b_mix.vm", "z_rpt.v"})
}

// TestMigrate_ViewsOnlyNamespace_PostgresToPostgres_Single is the
// single-schema arm: m_app into target schema m_app, then the views-only
// a_rpt into a_rpt.
func TestMigrate_ViewsOnlyNamespace_PostgresToPostgres_Single(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	seedPGViewsOnlyFanout(t, pgSource)

	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	for _, schema := range []string{"m_app", "a_rpt"} {
		mig := &Migrator{
			Source:       pgEng,
			Target:       pgEng,
			SourceDSN:    pgSource + "&schema=" + schema,
			TargetDSN:    pgTarget,
			TargetSchema: schema,
		}
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("Migrator.Run schema=%s: %v", schema, err)
		}
	}

	src, err := sql.Open("pgx", pgSource)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	dst, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	assertViewsMatchSource(t, ctx, src, dst, []string{"a_rpt.v"})
}
