//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the roadmap-item-140 pre-copy foreign-key gate on
// real MySQL and Postgres targets — the field report's exact shape: a
// target branched from an existing database, so its foreign keys were
// already there when the copy started.
//
// Each engine gets the same three-part story, and the parts are stated
// as a set because any one alone would be misleading:
//
//  1. the REFUSAL — a pre-existing constraint whose parent this run also
//     copies is refused (coded) with ZERO rows on the target, replacing
//     the ~20-second Error 1452 / SQLSTATE 23503 the reporter got;
//  2. the NON-refusal — the identical run against a target WITHOUT the
//     constraint completes end to end, which is what keeps this gate
//     from becoming a false positive that blocks working migrations;
//  3. the PREMISE — a run whose constraint the gate only WARNs about
//     (parent out of scope) is allowed through, and the SERVER then
//     rejects the orphan child. That is the environmental fact the whole
//     refusal rests on ("a MySQL/Postgres cold copy enforces the
//     target's foreign keys"), asserted rather than assumed, and it is
//     what internal/docsync's roster cites for
//     BulkCopyBypassesForeignKeys = false.
//
// The MySQL leg additionally pins the deliberate NON-exemption:
// --skip-foreign-keys does not clear a constraint the target already
// has (it only strips foreign keys from the IR, issuing no target DDL),
// so a run passing it is still refused — and the constraint is read back
// off the target afterwards to prove the flag left it enforcing.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// preexistingFKMySQLDDL is the parent/child pair, foreign key included —
// applied verbatim to BOTH sides so the target arrives carrying the
// constraint exactly as a branched database does.
const preexistingFKMySQLDDL = `
	CREATE TABLE fkg_users (
		id    BIGINT       NOT NULL,
		email VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

	CREATE TABLE fkg_orders (
		id      BIGINT NOT NULL,
		user_id BIGINT NOT NULL,
		PRIMARY KEY (id),
		KEY fkg_orders_user_idx (user_id),
		CONSTRAINT fkg_orders_user_fk FOREIGN KEY (user_id) REFERENCES fkg_users (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// assertPreexistingFKRefusalIntegration pins the coded refusal and
// returns its HINT (not the *CodedError, which errcheck would require
// every caller to consume).
func assertPreexistingFKRefusalIntegration(t *testing.T, err error, want ...string) string {
	t.Helper()
	if err == nil {
		t.Fatal("want the coded pre-existing-foreign-key refusal; got nil")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeTargetPreexistingForeignKey {
		t.Fatalf("err = %v; want %s", err, sluicecode.CodeTargetPreexistingForeignKey)
	}
	for _, w := range want {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("refusal %q does not name %q", err.Error(), w)
		}
	}
	return coded.Hint
}

func TestMigrate_PreExistingTargetForeignKeys_MySQL(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	applyMySQLDDL(t, sourceDSN, preexistingFKMySQLDDL+`
		INSERT INTO fkg_users (id, email) VALUES (1, 'alice@example.com'), (2, 'bob@example.com');
		INSERT INTO fkg_orders (id, user_id) VALUES (10, 1), (11, 2), (12, 1);
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	resetTarget := func(ddl string) {
		t.Helper()
		// Drop from the SOURCE connection: dropping the database you are
		// connected to leaves the session schemaless.
		applyMySQLDDL(t, sourceDSN, `DROP DATABASE target_db; CREATE DATABASE target_db;`)
		if ddl != "" {
			applyMySQLDDL(t, targetDSN, ddl)
		}
	}

	t.Run("pre-existing constraint refuses with zero rows copied", func(t *testing.T) {
		resetTarget(preexistingFKMySQLDDL)

		mig := &Migrator{
			Source: mysqlEng, Target: mysqlEng,
			SourceDSN: sourceDSN, TargetDSN: targetDSN,
			MigrationID: "fkgate-refuse",
		}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		hint := assertPreexistingFKRefusalIntegration(t, mig.Run(ctx),
			"fkg_orders", "fkg_orders_user_fk", "fkg_users")
		for _, want := range []string{"--skip-foreign-keys", "--exclude-table", "--reset-target-data"} {
			if !strings.Contains(hint, want) {
				t.Errorf("hint %q missing the remedy %q", hint, want)
			}
		}

		db := openSQLDB(t, "mysql", targetDSN)
		assertRowCount(t, db, "fkg_users", 0)
		assertRowCount(t, db, "fkg_orders", 0)
	})

	t.Run("--skip-foreign-keys is not an exemption and leaves the constraint enforcing", func(t *testing.T) {
		// The roadmap filing named --skip-foreign-keys as THE remedy. It
		// is not, and this is where that claim is ground-truthed: the flag
		// strips foreign keys from the IR so the constraints phase creates
		// none — it issues no DDL against the target, so the constraint
		// the target already carries is still there afterwards and would
		// still have rejected the copy. Exempting the flag would have
		// waved this run through to the identical 20-second failure.
		resetTarget(preexistingFKMySQLDDL)

		mig := &Migrator{
			Source: mysqlEng, Target: mysqlEng,
			SourceDSN: sourceDSN, TargetDSN: targetDSN,
			MigrationID:     "fkgate-skipflag",
			SkipForeignKeys: true,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		assertPreexistingFKRefusalIntegration(t, mig.Run(ctx), "fkg_orders", "fkg_orders_user_fk")

		db := openSQLDB(t, "mysql", targetDSN)
		var n int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM information_schema.referential_constraints
			 WHERE constraint_schema = DATABASE() AND constraint_name = 'fkg_orders_user_fk'`,
		).Scan(&n); err != nil {
			t.Fatalf("read back the target constraint: %v", err)
		}
		if n != 1 {
			t.Errorf("the target's pre-existing foreign key is gone (count=%d) — if --skip-foreign-keys "+
				"has learned to DROP a pre-existing target constraint, this gate's non-exemption "+
				"reasoning (and its refusal hint) are now wrong", n)
		}
	})

	t.Run("target without the constraint migrates end to end", func(t *testing.T) {
		// The other half of the gate, and the one that keeps it from
		// blocking working migrations: same source, same tables, no
		// pre-existing constraint on the target.
		resetTarget("")

		mig := &Migrator{
			Source: mysqlEng, Target: mysqlEng,
			SourceDSN: sourceDSN, TargetDSN: targetDSN,
			MigrationID: "fkgate-clean",
		}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("a target with no pre-existing foreign keys must migrate cleanly: %v", err)
		}

		db := openSQLDB(t, "mysql", targetDSN)
		assertRowCount(t, db, "fkg_users", 2)
		assertRowCount(t, db, "fkg_orders", 3)
		// sluice created the source's foreign key itself, AFTER the copy —
		// the deferred-constraint discipline working as designed.
		var n int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM information_schema.referential_constraints
			 WHERE constraint_schema = DATABASE() AND constraint_name = 'fkg_orders_user_fk'`,
		).Scan(&n); err != nil || n != 1 {
			t.Errorf("sluice's own deferred foreign key missing after a clean migrate (count=%d, err=%v)", n, err)
		}
	})
}

// TestMigrate_PreExistingTargetForeignKeys_MySQLOutOfScopeParentThenTheServerRejects
// is BOTH halves of one property, which is why they share a run.
//
// The gate only WARNs when a pre-existing constraint's parent is out of
// scope, because the parent rows already on the target may satisfy every
// child — refusing there would block a working configuration. This run
// takes that branch (fkg_users is excluded) and proves it proceeds.
//
// And then the target's own server rejects the orphan child, which is the
// environmental fact the REFUSAL branch rests on: "a MySQL cold copy
// enforces the target's foreign keys". sluice's MySQL cold-copy pool sets
// no foreign_key_checks=0 — only the CDC applier does (Bug 164) — and
// that is the premise internal/docsync's roster cites for
// BulkCopyBypassesForeignKeys = false. If MySQL's copy path ever starts
// bypassing enforcement, this test goes green in the wrong way and the
// capability (plus the refusal it drives) needs revisiting.
func TestMigrate_PreExistingTargetForeignKeys_MySQLOutOfScopeParentThenTheServerRejects(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	applyMySQLDDL(t, sourceDSN, preexistingFKMySQLDDL+`
		INSERT INTO fkg_users (id, email) VALUES (1, 'alice@example.com');
		INSERT INTO fkg_orders (id, user_id) VALUES (10, 1);
	`)
	// The target carries the constraint AND an empty parent table.
	applyMySQLDDL(t, targetDSN, preexistingFKMySQLDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	filter, err := migcore.NewTableFilter(nil, []string{"fkg_users"})
	if err != nil {
		t.Fatalf("build the exclude filter: %v", err)
	}
	mig := &Migrator{
		Source: mysqlEng, Target: mysqlEng,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		MigrationID: "fkgate-outofscope",
		Filter:      filter,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	runErr := mig.Run(ctx)

	if runErr == nil {
		t.Fatal("the copy of an orphan child into a target that carries the constraint must fail; Run " +
			"returned nil — a MySQL cold copy that no longer enforces the target's foreign keys would " +
			"invalidate both the item-140 refusal and the docsync roster's declared premise")
	}
	if coded, ok := sluicecode.FromError(runErr); ok && coded.Code == sluicecode.CodeTargetPreexistingForeignKey {
		t.Fatalf("an OUT-OF-SCOPE parent must not be refused by the gate (it is a WARN): %v", runErr)
	}
	// MySQL's own child-row rejection, not a sluice refusal.
	if !strings.Contains(runErr.Error(), "1452") {
		t.Errorf("run failed with %v; want MySQL Error 1452 (the server enforcing the pre-existing "+
			"foreign key during the copy)", runErr)
	}
}

// preexistingFKPGDDL mirrors the MySQL pair on Postgres.
const preexistingFKPGDDL = `
	CREATE TABLE fkg_users (
		id    BIGINT PRIMARY KEY,
		email VARCHAR(255) NOT NULL
	);

	CREATE TABLE fkg_orders (
		id      BIGINT PRIMARY KEY,
		user_id BIGINT NOT NULL,
		CONSTRAINT fkg_orders_user_fk FOREIGN KEY (user_id) REFERENCES fkg_users (id)
	);
`

func TestMigrate_PreExistingTargetForeignKeys_Postgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, preexistingFKPGDDL+`
		INSERT INTO fkg_users (id, email) VALUES (1, 'alice@example.com'), (2, 'bob@example.com');
		INSERT INTO fkg_orders (id, user_id) VALUES (10, 1), (11, 2), (12, 1);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	t.Run("pre-existing constraint refuses with zero rows copied", func(t *testing.T) {
		applyPGDDL(t, targetDSN, preexistingFKPGDDL)

		mig := &Migrator{
			Source: pgEng, Target: pgEng,
			SourceDSN: sourceDSN, TargetDSN: targetDSN,
			MigrationID: "fkgate-pg-refuse",
		}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		assertPreexistingFKRefusalIntegration(t, mig.Run(ctx),
			"fkg_orders", "fkg_orders_user_fk", "fkg_users")

		db := openSQLDB(t, "pgx", targetDSN)
		assertRowCount(t, db, "fkg_users", 0)
		assertRowCount(t, db, "fkg_orders", 0)
	})

	t.Run("target without the constraint migrates end to end", func(t *testing.T) {
		applyPGDDL(t, targetDSN, `DROP TABLE fkg_orders, fkg_users;`)

		mig := &Migrator{
			Source: pgEng, Target: pgEng,
			SourceDSN: sourceDSN, TargetDSN: targetDSN,
			MigrationID: "fkgate-pg-clean",
		}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("a target with no pre-existing foreign keys must migrate cleanly: %v", err)
		}

		db := openSQLDB(t, "pgx", targetDSN)
		assertRowCount(t, db, "fkg_users", 2)
		assertRowCount(t, db, "fkg_orders", 3)
	})
}

// TestMigrate_PreExistingTargetForeignKeys_PostgresOutOfScopeParentThenTheServerRejects
// is the Postgres half of the premise the docsync roster cites: no
// `session_replication_role = replica` is set anywhere on the copy path,
// so COPY fires the constraint's triggers and an orphan child is rejected
// with SQLSTATE 23503.
func TestMigrate_PreExistingTargetForeignKeys_PostgresOutOfScopeParentThenTheServerRejects(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, preexistingFKPGDDL+`
		INSERT INTO fkg_users (id, email) VALUES (1, 'alice@example.com');
		INSERT INTO fkg_orders (id, user_id) VALUES (10, 1);
	`)
	applyPGDDL(t, targetDSN, preexistingFKPGDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	filter, err := migcore.NewTableFilter(nil, []string{"fkg_users"})
	if err != nil {
		t.Fatalf("build the exclude filter: %v", err)
	}
	mig := &Migrator{
		Source: pgEng, Target: pgEng,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		MigrationID: "fkgate-pg-outofscope",
		Filter:      filter,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	runErr := mig.Run(ctx)

	if runErr == nil {
		t.Fatal("the copy of an orphan child into a target that carries the constraint must fail; Run " +
			"returned nil — a Postgres cold copy that no longer enforces the target's foreign keys would " +
			"invalidate both the item-140 refusal and the docsync roster's declared premise")
	}
	if coded, ok := sluicecode.FromError(runErr); ok && coded.Code == sluicecode.CodeTargetPreexistingForeignKey {
		t.Fatalf("an OUT-OF-SCOPE parent must not be refused by the gate (it is a WARN): %v", runErr)
	}
	if !strings.Contains(runErr.Error(), "23503") && !strings.Contains(runErr.Error(), "foreign key") {
		t.Errorf("run failed with %v; want the Postgres foreign-key violation (SQLSTATE 23503) from the "+
			"server enforcing the pre-existing constraint during the copy", runErr)
	}
}
