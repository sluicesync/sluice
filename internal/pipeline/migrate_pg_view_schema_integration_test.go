//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// TestMigrate_PostgresView_ReadsTheTargetSchema is Bug 314's Postgres arm
// on real servers. pg_views.definition qualifies a relation exactly when its
// schema is not on the reading session's search_path, and the writer used to
// run a view body under its own default path, so a view could resolve
// against a schema other than the one its tables were copied into:
//
//   - app → app2: the body named app.t, and the target's own app.t served.
//   - public → app3: the body named nothing, and the writer's public.p
//     served instead of app3.p.
//   - app → MySQL: the body named `app`.`t`, and a MySQL database app served.
//
// Each target holds a decoy under the name the old body resolved to, so the
// cell is graded by the rows the view returns, never by its text.
func TestMigrate_PostgresView_ReadsTheTargetSchema(t *testing.T) {
	pgSource, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()
	mySource, myTarget, myCleanup := startMySQL(t)
	defer myCleanup()

	applyPGDDL(t, pgSource, `
		CREATE SCHEMA app;
		CREATE TABLE app.t (id INT PRIMARY KEY, v TEXT);
		INSERT INTO app.t VALUES (1, 'src-a'), (2, 'src-b');
		CREATE VIEW app.v AS SELECT id, v FROM app.t WHERE id > 0;
		CREATE MATERIALIZED VIEW app.mv AS SELECT id, v FROM app.t;
		CREATE TABLE public.p (id INT PRIMARY KEY, v TEXT);
		INSERT INTO public.p VALUES (1, 'pub');
		CREATE VIEW public.pv AS SELECT id, v FROM public.p;
	`)
	applyPGDDL(t, pgTarget, `
		CREATE SCHEMA app;
		CREATE TABLE app.t (id INT PRIMARY KEY, v TEXT);
		INSERT INTO app.t VALUES (99, 'DECOY');
		CREATE TABLE public.p (id INT PRIMARY KEY, v TEXT);
		INSERT INTO public.p VALUES (77, 'PUBLIC-DECOY');
	`)
	applyMySQLDDL(t, mySource, `
		CREATE DATABASE IF NOT EXISTS app;
		CREATE TABLE app.t (id INT NOT NULL PRIMARY KEY, v VARCHAR(20)) ENGINE=InnoDB;
		INSERT INTO app.t VALUES (99, 'DECOY');
	`)

	pg, _ := engines.Get("postgres")
	my, _ := engines.Get("mysql")
	cases := []struct {
		name         string
		target       ir.Engine
		srcDSN       string
		tgtDSN       string
		targetSchema string
		driver       string
		views        []string
		exclude      []string
		want         string
	}{
		{"app to app2", pg, withPGSchema(pgSource, "app"), pgTarget, "app2", "pgx", []string{"app2.v", "app2.mv"}, nil, "1:src-a,2:src-b"},
		{"public to app3", pg, pgSource, pgTarget, "app3", "pgx", []string{"app3.pv"}, nil, "1:pub"},
		{"app to mysql", my, withPGSchema(pgSource, "app"), myTarget, "", "mysql", []string{"v"}, []string{"mv"}, "1:src-a,2:src-b"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			mig := &Migrator{
				Source: pg, Target: c.target, SourceDSN: c.srcDSN, TargetDSN: c.tgtDSN,
				TargetSchema: c.targetSchema, MigrationID: "bug314-" + strings.ReplaceAll(c.name, " ", "-"),
				// MySQL has no materialized views and refuses one loudly.
				ViewFilter: ViewFilter{Exclude: c.exclude},
			}
			if err := mig.Run(ctx); err != nil {
				t.Fatalf("migrate: %v", err)
			}
			db, err := sql.Open(c.driver, c.tgtDSN)
			if err != nil {
				t.Fatalf("open target: %v", err)
			}
			defer func() { _ = db.Close() }()
			for _, view := range c.views {
				if g := readViewRows(ctx, t, db, view); g != c.want {
					t.Errorf("target view %s returns %s, want %s: it resolves against a schema other than the one its tables were copied into", view, g, c.want)
				}
			}
		})
	}

	// A view over a table the run leaves out: on a non-public source its
	// body names that table unqualified, meaning the source schema's own.
	// The target's public holds an unrelated table by that name, and the
	// view must not bind to it: the create fails loudly instead.
	t.Run("excluded table does not bind to public", func(t *testing.T) {
		applyPGDDL(t, pgSource, `
			CREATE SCHEMA ex;
			CREATE TABLE ex.keep (id INT PRIMARY KEY, v TEXT);
			CREATE TABLE ex.x (id INT PRIMARY KEY, v TEXT);
			INSERT INTO ex.x VALUES (1, 'src-x');
			CREATE VIEW ex.vx AS SELECT id, v FROM ex.x;
		`)
		applyPGDDL(t, pgTarget, `
			CREATE TABLE public.x (id INT PRIMARY KEY, v TEXT);
			INSERT INTO public.x VALUES (42, 'PUBLIC-DECOY');
		`)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		mig := &Migrator{
			Source: pg, Target: pg, SourceDSN: withPGSchema(pgSource, "ex"), TargetDSN: pgTarget,
			TargetSchema: "ex4", MigrationID: "bug314-excluded",
			Filter: migcore.TableFilter{Exclude: []string{"x"}},
		}
		err := mig.Run(ctx)
		if err == nil {
			db, oerr := sql.Open("pgx", pgTarget)
			if oerr != nil {
				t.Fatalf("open target: %v", oerr)
			}
			defer func() { _ = db.Close() }()
			t.Fatalf("migrate succeeded; ex4.vx returns %s, but the table it reads was excluded, so it can only be reading an unrelated target table", readViewRows(ctx, t, db, "ex4.vx"))
		}
		if !strings.Contains(err.Error(), "vx") {
			t.Errorf("the failure does not name the view: %v", err)
		}
	})
}

// readViewRows renders a view's (id, v) rows as "id:v,..." in id order.
func readViewRows(ctx context.Context, t *testing.T, db *sql.DB, view string) string {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT id, v FROM "+view+" ORDER BY id")
	if err != nil {
		t.Fatalf("read %s: %v", view, err)
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var id int
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, strconv.Itoa(id)+":"+v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return strings.Join(got, ",")
}

// withPGSchema adds sluice's ?schema= selector to a postgres:// DSN.
func withPGSchema(dsn, schema string) string {
	if strings.Contains(dsn, "?") {
		return dsn + "&schema=" + schema
	}
	return dsn + "?schema=" + schema
}
