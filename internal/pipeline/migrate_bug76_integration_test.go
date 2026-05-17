//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for Bug 76 (schema reader validates ALL columns
// before the --include-table / --exclude-table filter):
//
// Pre-fix the PG reader's populateColumns type-validated every column
// of every table, so an unsupported type (`money`) in a table the
// operator EXCLUDED still aborted the whole migration at schema-read
// with an error naming the unrelated table. Phase-A: code-read —
// loud + deterministic; the read→validate→filter ordering put the
// per-column validation (populateColumns) before the pipeline's
// post-read applyTableFilter, with no push-down. Fix: ir.TableScoper
// push-down so readTables drops out-of-scope tables before their
// columns are read/validated.
//
// This pin: a `money` column (unsupported by the cross-engine
// translator) in a NON-included table must NOT block an
// --include-table run scoped to a clean table; the clean table must
// migrate faithfully.

package pipeline

import (
	"database/sql"
	"testing"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

func TestMigrate_PostgresToPostgres_Bug76FilterBeforeColumnValidate(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	// `good` is clean; `bad` has an unsupported `money` column AND an
	// `interval` column (the catalog's two-deep isolation: dropping one
	// unmasks the next). With the Bug-76 fix, scoping to `good` means
	// neither bad-table column is ever validated.
	applyPGDDL(t, sourceDSN, `
		CREATE TABLE good (id int PRIMARY KEY, n int, label text);
		CREATE TABLE bad  (id int PRIMARY KEY, m money, span interval);
		INSERT INTO good VALUES (1, 42, 'alpha'), (2, 7, 'beta');
		INSERT INTO bad  VALUES (1, 9.99, interval '1 day');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	t.Run("include-only good migrates despite money/interval in bad", func(t *testing.T) {
		filter, err := NewTableFilter([]string{"good"}, nil)
		if err != nil {
			t.Fatalf("NewTableFilter: %v", err)
		}
		mig := &Migrator{
			Source:    pgEng,
			Target:    pgEng,
			SourceDSN: sourceDSN,
			TargetDSN: targetDSN,
			Filter:    filter,
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			// Pre-fix: "read source schema: ... table \"bad\" column \"m\":
			// unsupported data_type \"money\"".
			t.Fatalf("Migrator.Run (Bug 76 — scoped run must not validate excluded-table columns): %v", err)
		}

		dstDB, err := sql.Open("pgx", targetDSN)
		if err != nil {
			t.Fatalf("open pg target: %v", err)
		}
		defer func() { _ = dstDB.Close() }()

		var n int
		if err := dstDB.QueryRow(`SELECT count(*) FROM good`).Scan(&n); err != nil {
			t.Fatalf("count good on target: %v", err)
		}
		if n != 2 {
			t.Errorf("good row count on target = %d; want 2", n)
		}
		// `bad` must NOT have been created (it was out of scope).
		var exists bool
		if err := dstDB.QueryRow(
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			 WHERE table_name='bad')`).Scan(&exists); err != nil {
			t.Fatalf("probe bad existence: %v", err)
		}
		if exists {
			t.Error("table 'bad' was created on target; it was excluded from the scoped run")
		}
	})

	t.Run("exclude bad migrates good", func(t *testing.T) {
		_, targetDSN2, cleanup2 := startPostgres(t)
		defer cleanup2()

		filter, err := NewTableFilter(nil, []string{"bad"})
		if err != nil {
			t.Fatalf("NewTableFilter: %v", err)
		}
		mig := &Migrator{
			Source:    pgEng,
			Target:    pgEng,
			SourceDSN: sourceDSN,
			TargetDSN: targetDSN2,
			Filter:    filter,
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("Migrator.Run (Bug 76 — --exclude-table=bad must not validate bad's columns): %v", err)
		}
		dstDB, err := sql.Open("pgx", targetDSN2)
		if err != nil {
			t.Fatalf("open pg target2: %v", err)
		}
		defer func() { _ = dstDB.Close() }()
		var n int
		if err := dstDB.QueryRow(`SELECT count(*) FROM good`).Scan(&n); err != nil {
			t.Fatalf("count good on target2: %v", err)
		}
		if n != 2 {
			t.Errorf("good row count on target2 = %d; want 2", n)
		}
	})
}
