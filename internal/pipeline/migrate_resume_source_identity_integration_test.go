//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The foreign-source resume door, against real servers (audit
// 2026-09-15 A0915-STATE-MEDIUM-1).
//
// Every cell here shares one shape: a target that already carries a
// completed (or partial) migration's state, and a SECOND run that
// adopts it by id. Before the door, the second run exited 0 having
// copied nothing.
//
// The independent expected value, named per the 2026-08-01 rule, is the
// TARGET'S OWN ROW COUNT, read out of band with a plain *sql.DB before
// the second run starts and again after it. It does not derive from the
// control table, the migration state, or anything the code under test
// writes — which matters because the defect being pinned is precisely
// that the bookkeeping says "done" while the target says otherwise.

package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"

	// Register both engines so engines.Get works and the pgx / mysql
	// database/sql drivers are available to the out-of-band probes.
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestMigrate_ResumeRefusesForeignSource(t *testing.T) {
	t.Run("typed migration id, foreign source engine", func(t *testing.T) {
		pgSrc, pgTgt, stopPG := startPostgres(t)
		defer stopPG()
		mySrc, _, stopMySQL := startMySQL(t)
		defer stopMySQL()

		pgExec(t, pgSrc, `CREATE TABLE t (id INT PRIMARY KEY, note TEXT)`)
		pgExec(t, pgSrc, `INSERT INTO t VALUES (1, 'PG_ROW_A'), (2, 'PG_ROW_B')`)

		// A DIFFERENT source, with different data, under the SAME id.
		mysqlExec(t, mySrc, `CREATE TABLE t (id INT PRIMARY KEY, note VARCHAR(32))`)
		mysqlExec(t, mySrc, `INSERT INTO t VALUES (1,'MY_A'),(2,'MY_B'),(3,'MY_C')`)

		const sharedID = "shared-migration-id"
		runMigrateOK(t, migratorFor(t, "postgres", pgSrc, "postgres", pgTgt, sharedID, false))

		// The independent expected value: what the TARGET holds, read
		// out of band, before the foreign resume runs.
		before := pgCount(t, pgTgt, "t")
		if before != 2 {
			t.Fatalf("setup: target holds %d rows; want the 2 the PG source copied", before)
		}

		err := migratorFor(t, "mysql", mySrc, "postgres", pgTgt, sharedID, true).Run(context.Background())
		requireSourceMismatch(t, err)

		if after := pgCount(t, pgTgt, "t"); after != before {
			t.Errorf("the refusal did not fire before the copy: target went from %d to %d rows", before, after)
		}
		// And the refusal must NAME both sides, or the operator cannot act.
		for _, want := range []string{"postgres", "mysql", "source_db"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal does not name %q; got: %v", want, err)
			}
		}
	})

	// Scenario D: no --migration-id anywhere. Two databases on ONE host
	// collide on the auto-derived id, which hashes the source and target
	// HOSTS and not the database.
	t.Run("auto-derived id collides across two databases of one host", func(t *testing.T) {
		pgSrc, pgTgt, stopPG := startPostgres(t)
		defer stopPG()

		pgExec(t, pgSrc, `CREATE DATABASE other_src`)
		otherSrc, err := buildPGDSN(pgSrc, "other_src")
		if err != nil {
			t.Fatalf("build second-source DSN: %v", err)
		}

		pgExec(t, pgSrc, `CREATE TABLE t (id INT PRIMARY KEY, note TEXT)`)
		pgExec(t, pgSrc, `INSERT INTO t VALUES (1, 'FIRST_DB')`)
		pgExec(t, otherSrc, `CREATE TABLE t (id INT PRIMARY KEY, note TEXT)`)
		pgExec(t, otherSrc, `INSERT INTO t VALUES (10, 'SECOND_DB_A'), (11, 'SECOND_DB_B')`)

		// Empty MigrationID on both runs: the id is derived, and both
		// sources sit on the same host, so the two runs collide.
		runMigrateOK(t, migratorFor(t, "postgres", pgSrc, "postgres", pgTgt, "", false))

		before := pgCount(t, pgTgt, "t")
		if before != 1 {
			t.Fatalf("setup: target holds %d rows; want 1", before)
		}

		err = migratorFor(t, "postgres", otherSrc, "postgres", pgTgt, "", true).Run(context.Background())
		requireSourceMismatch(t, err)

		if after := pgCount(t, pgTgt, "t"); after != before {
			t.Errorf("the refusal did not fire before the copy: target went from %d to %d rows", before, after)
		}
		for _, want := range []string{"source_db", "other_src"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal does not name %q; got: %v", want, err)
			}
		}
	})

	// The door must not break the thing it guards: a resume against the
	// SAME source still resumes and copies what the failed run did not.
	t.Run("matching identity resumes and copies the remainder", func(t *testing.T) {
		pgSrc, pgTgt, stopPG := startPostgres(t)
		defer stopPG()

		pgExec(t, pgSrc, `CREATE TABLE a (id INT PRIMARY KEY, note TEXT)`)
		pgExec(t, pgSrc, `INSERT INTO a VALUES (1,'a1'), (2,'a2'), (3,'a3')`)
		pgExec(t, pgSrc, `CREATE TABLE b (id INT PRIMARY KEY, note TEXT)`)
		pgExec(t, pgSrc, `INSERT INTO b VALUES (1,'b1'), (2,'b2'), (3,'b3')`)

		const id = "resumable"
		real := engineFor(t, "postgres")
		failing := &failingRowWriterEngine{Engine: real, failTable: "b", failAfterRows: 1}

		first := &Migrator{
			Source: real, SourceDSN: pgSrc,
			Target: failing, TargetDSN: pgTgt,
			MigrationID: id,
		}
		if err := first.Run(context.Background()); err == nil {
			t.Fatal("setup: the injected mid-copy failure did not fail the run")
		}

		// Same source, same id, --resume: the identity matches, so the
		// door is silent and the resume finishes the job.
		runMigrateOK(t, migratorFor(t, "postgres", pgSrc, "postgres", pgTgt, id, true))

		if got := pgCount(t, pgTgt, "a"); got != 3 {
			t.Errorf("table a holds %d rows after the resume; want 3", got)
		}
		if got := pgCount(t, pgTgt, "b"); got != 3 {
			t.Errorf("table b holds %d rows after the resume; want 3 — the resume did not copy the remainder", got)
		}
	})

	// (d) State written by a binary older than the column carries no
	// identity. Refusing it would strand every migration in flight at
	// upgrade time, so it WARNs and proceeds. NULLing the column is
	// exactly what such a row looks like: it is added NULLable and
	// defaultless.
	t.Run("state written before the column resumes with a warning", func(t *testing.T) {
		pgSrc, pgTgt, stopPG := startPostgres(t)
		defer stopPG()

		pgExec(t, pgSrc, `CREATE TABLE t (id INT PRIMARY KEY, note TEXT)`)
		pgExec(t, pgSrc, `INSERT INTO t VALUES (1, 'row')`)

		const id = "legacy-state"
		runMigrateOK(t, migratorFor(t, "postgres", pgSrc, "postgres", pgTgt, id, false))

		// Confirm this binary DID record an identity before we erase it —
		// otherwise the cell below would pass vacuously, proving only
		// that a column nobody writes is empty.
		if got := identityPGText(t, pgTgt, `SELECT COALESCE(source_identity,'') FROM sluice_migrate_state WHERE migration_id = 'legacy-state'`); got == "" {
			t.Fatal("this binary recorded NO source identity on a fresh run; behaviour (a) is broken and the " +
				"legacy cell below would be vacuous")
		}
		pgExec(t, pgTgt, `UPDATE sluice_migrate_state SET source_identity = NULL WHERE migration_id = 'legacy-state'`)

		logs := captureDefaultLogger(t)
		if err := migratorFor(t, "postgres", pgSrc, "postgres", pgTgt, id, true).Run(context.Background()); err != nil {
			t.Fatalf("a resume of pre-column state was refused; every in-flight migration would be stranded "+
				"on upgrade: %v", err)
		}
		if !strings.Contains(logs.String(), sourceIdentityUnrecordedMarker) {
			t.Errorf("the resume proceeded without the %s marker, so an operator has no grep-stable signal "+
				"that it could not prove the source; log was:\n%s", sourceIdentityUnrecordedMarker, logs.String())
		}
	})
}

// --- helpers -------------------------------------------------------

func engineFor(t *testing.T, name string) ir.Engine {
	t.Helper()
	eng, ok := engines.Get(name)
	if !ok {
		t.Fatalf("engine %q is not registered", name)
	}
	return eng
}

func migratorFor(t *testing.T, srcEngine, srcDSN, tgtEngine, tgtDSN, migrationID string, resume bool) *Migrator {
	t.Helper()
	return &Migrator{
		Source:      engineFor(t, srcEngine),
		SourceDSN:   srcDSN,
		Target:      engineFor(t, tgtEngine),
		TargetDSN:   tgtDSN,
		MigrationID: migrationID,
		Resume:      resume,
	}
}

func runMigrateOK(t *testing.T, m *Migrator) {
	t.Helper()
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

func requireSourceMismatch(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("the resume was ALLOWED against a foreign source: it exits 0 having copied nothing")
	}
	var coded *sluicecode.CodedError
	if !errors.As(err, &coded) {
		t.Fatalf("the refusal carries no SLUICE-E code, so nothing can branch on it: %v", err)
	}
	if coded.Code != sluicecode.CodeResumeSourceMismatch {
		t.Fatalf("refused with %s, want %s: %v", coded.Code, sluicecode.CodeResumeSourceMismatch, err)
	}
}

// pgExec is the package's existing integration helper
// (streamer_trigger_prune_integration_test.go); its variadic signature
// accepts these statement-only calls unchanged, so it is reused rather
// than shadowed.

func mysqlExec(t *testing.T, dsn, stmt string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", stmt, err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(context.Background(), stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

// pgCount is the INDEPENDENT expected value: the target's own row count,
// read with a plain connection that shares no code with the copy path or
// the migrate-state store.
func pgCount(t *testing.T, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// identityPGText is the string-returning sibling of the package's
// int64-returning pgScalar.
func identityPGText(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var s string
	if err := db.QueryRowContext(context.Background(), query).Scan(&s); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return s
}

// captureDefaultLogger redirects slog's default logger into a buffer for
// the duration of one test and restores it afterwards. The subtest using
// it is deliberately NOT parallel — the default logger is global.
func captureDefaultLogger(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}
