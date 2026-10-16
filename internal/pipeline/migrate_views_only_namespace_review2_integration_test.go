//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 317 second-review pins.
//
//   - A typo'd --include-table on a Postgres source (the scope push-down
//     engine) must refuse on every entry point that filters: `backup full`,
//     `schema diff`, `schema preview`, single-namespace `migrate` and `sync
//     start`. The first review's fix narrowed migcore.ApplyTableFilter's
//     refusal to "emptied a non-empty set", and the push-down hands it an
//     already-empty set — so `backup full --include-table=typo` wrote an
//     empty backup at exit 0. The independent expected value is the
//     refusal text migcore owns, on a source that holds a table.
//   - A materialized view that already exists with the same definition is
//     REFRESHED, not skipped: under a pre-created schema (ADR-0166) it was
//     built over empty tables, and skipping left it empty after the copy at
//     exit 0. Graded by the target matview's rows against the source
//     matview's own.

package pipeline

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

const filterRefusal = "table filter excluded every source table"

func TestMigrate_ViewsOnlyNamespace_PostgresTypoIncludeTableRefusesEverywhere(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyPGDDL(t, pgSource, `
		CREATE TABLE public.orders (id INT PRIMARY KEY);
		INSERT INTO public.orders VALUES (1), (2);
	`)
	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	typo := mustTableFilter(t, []string{"ordres"}, nil)

	want := func(what string, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), filterRefusal) {
			t.Errorf("%s with a typo'd --include-table on Postgres: want %q; got %v", what, filterRefusal, err)
		}
	}

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	want("backup full", (&backup.Backup{Source: pgEng, SourceDSN: pgSource, Store: store, SluiceVersion: "test", Filter: typo}).Run(ctx))

	_, err = (&Differ{Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget, Format: "text", Out: &bytes.Buffer{}, Filter: typo}).Run(ctx)
	want("schema diff", err)

	want("schema preview", (&Previewer{Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget, Format: "text", Out: &bytes.Buffer{}, Filter: typo}).Run(ctx))

	want("migrate", (&Migrator{Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget, Filter: typo}).Run(ctx))

	want("sync start", (&Streamer{Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget, StreamID: "typo-include", Filter: typo}).Run(ctx))
}

// TestMigrate_ViewsOnlyNamespace_PostgresPreCreatedMatviewIsRefreshed: the
// operator pre-created the whole target schema (ADR-0166) — the table AND
// the materialized view, both empty. migrate skips the matching table,
// copies its rows, and must leave the matview holding the source's rows.
func TestMigrate_ViewsOnlyNamespace_PostgresPreCreatedMatviewIsRefreshed(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	const ddl = `
		CREATE SCHEMA app;
		CREATE TABLE app.t (id INT PRIMARY KEY, k INT NOT NULL);
		CREATE MATERIALIZED VIEW app.mv AS SELECT id FROM app.t WHERE k >= 20;
	`
	applyPGDDL(t, pgSource, ddl+`
		INSERT INTO app.t VALUES (1, 10), (2, 20), (3, 30);
		REFRESH MATERIALIZED VIEW app.mv;
	`)
	applyPGDDL(t, pgTarget, ddl)

	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	mig := &Migrator{
		Source: pgEng, Target: pgEng,
		SourceDSN: pgSource + "&schema=app", TargetDSN: pgTarget, TargetSchema: "app",
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("migrate into a pre-created schema with a matview: %v", err)
	}
	src, dst := openBoth(t, "pgx", pgSource, pgTarget)
	assertViewsMatchSource(t, ctx, src, dst, []string{"app.mv"})
}
