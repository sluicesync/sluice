//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration coverage for the --upfront-indexes migrate mode
// (Migrator.UpfrontIndexes): secondary indexes are created BEFORE the bulk
// copy — right after CreateTablesWithoutConstraints — instead of the default
// deferred post-copy phase, so the bulk INSERTs maintain them during load.
// The mode reuses the engine's SchemaWriter.CreateIndexes, so it is
// engine-neutral; these pins run MySQL→MySQL because that is the motivating
// target (a large PlanetScale-MySQL deferred ADD INDEX exceeding the
// statement-time limit).

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	// Register the mysql engine so engines.Get("mysql") works.
	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestMigrate_UpfrontIndexes_MySQLToMySQL runs a full MySQL→MySQL migrate with
// UpfrontIndexes=true and asserts the target carries the correct secondary
// indexes AND foreign key AND byte-exact data — the same schema/data
// expectations TestMigrate_MySQLToMySQL asserts for the DEFAULT deferred mode,
// so an upfront run lands identically to a deferred run.
func TestMigrate_UpfrontIndexes_MySQLToMySQL(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			email      VARCHAR(255)    NOT NULL,
			active     TINYINT(1)      NOT NULL DEFAULT 1,
			created_at TIMESTAMP(0)    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id),
			UNIQUE KEY users_email_unique (email),
			KEY users_active_idx (active)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE TABLE posts (
			id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			user_id BIGINT UNSIGNED NOT NULL,
			body    TEXT            NOT NULL,
			PRIMARY KEY (id),
			KEY posts_user_id_idx (user_id),
			CONSTRAINT posts_user_id_fk FOREIGN KEY (user_id)
				REFERENCES users (id) ON DELETE CASCADE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO users (email, active) VALUES
			('alice@example.com', 1),
			('bob@example.com',   0);

		INSERT INTO posts (user_id, body) VALUES
			(1, 'first post'),
			(1, 'second post'),
			(2, 'a post by bob');
	`
	applyMySQLDDL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	mig := &Migrator{
		Source:         mysqlEng,
		Target:         mysqlEng,
		SourceDSN:      sourceDSN,
		TargetDSN:      targetDSN,
		UpfrontIndexes: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (upfront): %v", err)
	}

	// ---- Schema: secondary indexes + FK land exactly as in deferred mode ----
	sr, err := mysqlEng.OpenSchemaReader(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer migcore.CloseIf(sr)

	got, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	users := findTable(got, "users")
	posts := findTable(got, "posts")
	if users == nil || posts == nil {
		t.Fatalf("missing target tables; have %v", targetTableNames(got))
	}

	// users: a UNIQUE index on email AND a non-unique index on active — both
	// built upfront, before the copy.
	if !hasIndex(users, "users_email_unique", true) {
		t.Errorf("users missing UNIQUE index users_email_unique; indexes=%#v", users.Indexes)
	}
	if !hasIndex(users, "users_active_idx", false) {
		t.Errorf("users missing index users_active_idx; indexes=%#v", users.Indexes)
	}
	// posts: the FK survives (FKs are still created LAST — upfront reorders
	// indexes only).
	if len(posts.ForeignKeys) != 1 || posts.ForeignKeys[0].ReferencedTable != "users" ||
		posts.ForeignKeys[0].OnDelete != ir.FKActionCascade {
		t.Errorf("posts FK = %+v; want one users on-delete-cascade FK", posts.ForeignKeys)
	}

	// ---- Data: byte-exact row counts + a sample value ----
	if n := countMySQLRows(t, targetDSN, "users"); n != 2 {
		t.Errorf("target users rows = %d; want 2", n)
	}
	if n := countMySQLRows(t, targetDSN, "posts"); n != 3 {
		t.Errorf("target posts rows = %d; want 3", n)
	}

	rr, err := mysqlEng.OpenRowReader(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer migcore.CloseIf(rr)
	usersRows := readAll(t, ctx, rr, users)
	if len(usersRows) != 2 {
		t.Fatalf("target users rows via reader = %d; want 2", len(usersRows))
	}
	if email, _ := usersRows[0]["email"].(string); email != "alice@example.com" {
		t.Errorf("users[0].email = %#v; want 'alice@example.com'", usersRows[0]["email"])
	}
}

// TestMigrate_UpfrontIndexes_ResumeUnderLiveUniqueIndex_MySQLToMySQL pins the
// resume interaction the deferred mode never faces: with --upfront-indexes the
// UNIQUE secondary index exists DURING the copy (not built after it), so a
// --resume re-copy runs against a LIVE unique index. It must complete without a
// spurious duplicate-key failure and land byte-exact data.
//
// Mechanism (mirrors TestMigrate_ResumeFromBulkCopyFailure): attempt 1 uses the
// engine-neutral failingRowWriterEngine to abort the copy partway; attempt 2
// re-runs with Resume=true. BulkParallelism=1 pins the deterministic
// single-reader resume path — truncate-and-redo — so the re-copy re-INSERTs
// every row into the (emptied but still-unique-indexed) target. The idempotent
// per-batch / chunked resume paths (larger tables) are ALSO dup-safe under a
// live unique index by construction: MySQL's RowWriter implements
// ir.IdempotentRowWriter (ON DUPLICATE KEY UPDATE), which absorbs the
// batch-commit→cursor-write overlap re-insert — that engine contract is pinned
// in the mysql engine's own resume tests.
func TestMigrate_UpfrontIndexes_ResumeUnderLiveUniqueIndex_MySQLToMySQL(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE orders (
			id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			order_no VARCHAR(64)     NOT NULL,
			amount   BIGINT          NOT NULL,
			PRIMARY KEY (id),
			UNIQUE KEY orders_order_no_unique (order_no)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO orders (order_no, amount)
			SELECT CONCAT('ord-', seq), seq
			FROM (
				SELECT (a.n + b.n * 10 + 1) AS seq
				FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
				      UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a
				CROSS JOIN (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) b
			) seqs;
	`
	applyMySQLDDL(t, sourceDSN, seedDDL) // 40 rows: ord-1 .. ord-40

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Attempt 1: --upfront-indexes, copy aborts after 15 rows of orders.
	failEng := &failingRowWriterEngine{Engine: mysqlEng, failTable: "orders", failAfterRows: 15}
	mig := &Migrator{
		Source:          mysqlEng,
		Target:          failEng,
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		MigrationID:     "test-upfront-resume",
		UpfrontIndexes:  true,
		BulkParallelism: 1, // deterministic single-reader path
	}
	if err := mig.Run(ctx); err == nil {
		t.Fatal("expected first attempt to fail mid-copy; got nil")
	} else if !strings.Contains(err.Error(), "simulated mid-bulk-copy failure") {
		t.Fatalf("first-attempt err = %v; want the simulated mid-copy failure", err)
	}

	// The UNIQUE index must ALREADY exist on the target even though the copy
	// failed — proof the index was built UPFRONT (before the copy), not in a
	// deferred post-copy phase that never ran.
	sr, err := mysqlEng.OpenSchemaReader(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader (after failure): %v", err)
	}
	afterFail, err := sr.ReadSchema(ctx)
	migcore.CloseIf(sr)
	if err != nil {
		t.Fatalf("ReadSchema (after failure): %v", err)
	}
	if orders := findTable(afterFail, "orders"); orders == nil || !hasIndex(orders, "orders_order_no_unique", true) {
		t.Fatalf("after mid-copy failure the upfront UNIQUE index is absent; orders=%#v", orders)
	}

	// Attempt 2: real engine, --resume, --upfront-indexes. The re-copy runs
	// against the live unique index and must not spuriously fail.
	mig2 := &Migrator{
		Source:          mysqlEng,
		Target:          mysqlEng,
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		MigrationID:     "test-upfront-resume",
		Resume:          true,
		UpfrontIndexes:  true,
		BulkParallelism: 1,
	}
	if err := mig2.Run(ctx); err != nil {
		t.Fatalf("resume Run under live unique index: %v", err)
	}

	// Byte-exact: all 40 rows present, unique index intact, no dup order_no.
	if n := countMySQLRows(t, targetDSN, "orders"); n != 40 {
		t.Errorf("orders row count after resume = %d; want 40", n)
	}
	if n := countMySQLDistinct(t, targetDSN, "orders", "order_no"); n != 40 {
		t.Errorf("distinct order_no after resume = %d; want 40 (unique index maintained)", n)
	}
	sr2, err := mysqlEng.OpenSchemaReader(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader (final): %v", err)
	}
	defer migcore.CloseIf(sr2)
	final, err := sr2.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema (final): %v", err)
	}
	if orders := findTable(final, "orders"); orders == nil || !hasIndex(orders, "orders_order_no_unique", true) {
		t.Errorf("final UNIQUE index missing after resume; orders=%#v", orders)
	}
}

// hasIndex reports whether table carries an index of the given name and
// uniqueness. Small local helper so the assertions read declaratively.
func hasIndex(table *ir.Table, name string, unique bool) bool {
	for _, idx := range table.Indexes {
		if idx.Name == name && idx.Unique == unique {
			return true
		}
	}
	return false
}

// countMySQLRows returns COUNT(*) for a table via a direct query.
func countMySQLRows(t *testing.T, dsn, table string) int {
	t.Helper()
	return queryMySQLInt(t, dsn, "SELECT COUNT(*) FROM `"+table+"`")
}

// countMySQLDistinct returns COUNT(DISTINCT col) for a table.
func countMySQLDistinct(t *testing.T, dsn, table, col string) int {
	t.Helper()
	return queryMySQLInt(t, dsn, "SELECT COUNT(DISTINCT `"+col+"`) FROM `"+table+"`")
}

func queryMySQLInt(t *testing.T, dsn, query string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}
