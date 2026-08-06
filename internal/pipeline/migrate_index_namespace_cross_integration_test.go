//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Two source indexes that resolve to ONE target index name must be refused
// BEFORE any data moves (roadmap item 134, the SQLite sibling of item 120),
// end to end, on a real MySQL source and a real SQLite target.
//
// # Why this needs a live source
//
// The whole reason the collision is routine rather than exotic is a fact about
// MySQL, not about sluice: index names there are TABLE-scoped, and MySQL
// auto-names a single-column index after its column. So `KEY (user_id)` on two
// tables is what a real schema looks like, and both arrive at a SQLite target
// as the identical name `user_id`. A hand-built ir.Schema can assert the
// refusal but cannot assert that a real server hands sluice that pair — which
// is the premise the finding rests on, and the reason the test creates the
// tables with no index name at all and lets MySQL name them.
//
// # What is asserted, and why it is the TARGET's state
//
// `emitCreateIndex` renders `CREATE INDEX IF NOT EXISTS`, so a collision that
// reached the index phase would not error — it would return OK and create
// nothing (ground-truthed against the real driver in
// engines/sqlite's TestSQLiteIndexNamespace_CaseFoldIsGroundTruth). A late
// refusal is therefore not the failure mode: NO refusal is. The assertion here
// is that the run refuses AND the target table does not exist, so the check is
// running beside the other pre-DDL gates rather than after the copy.
//
// The control matters at least as much: the same two tables with DISTINCT
// index names must migrate, rows and indexes and all. An over-refusing check
// would break every MySQL→SQLite migration whose tables share a column name.

package pipeline

import (
	"database/sql"
	"path/filepath"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

func TestMigrate_IndexNamespace_MySQLToSQLite_CollidingIndexNames(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}

	sqliteHasTable := func(dst, name string) func() bool {
		return func() bool {
			db, err := sql.Open("sqlite", dst)
			if err != nil {
				t.Fatalf("open sqlite target: %v", err)
			}
			defer func() { _ = db.Close() }()
			var n int
			if err := db.QueryRow(
				`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name,
			).Scan(&n); err != nil {
				t.Fatalf("probe sqlite table %q: %v", name, err)
			}
			return n > 0
		}
	}

	t.Run("MySQL auto-names both indexes after the column, and the collision is refused", func(t *testing.T) {
		// No index NAME is given: MySQL names each `user_id` after its
		// column. That is the premise, established by the server rather than
		// asserted in a comment.
		applyMySQLDDL(t, mysqlSource, `
			DROP TABLE IF EXISTS ns_posts, ns_comments;
			CREATE TABLE ns_posts (
				id      BIGINT NOT NULL AUTO_INCREMENT,
				user_id BIGINT NOT NULL,
				PRIMARY KEY (id),
				KEY (user_id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			CREATE TABLE ns_comments (
				id      BIGINT NOT NULL AUTO_INCREMENT,
				user_id BIGINT NOT NULL,
				PRIMARY KEY (id),
				UNIQUE KEY (user_id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			INSERT INTO ns_posts (user_id) VALUES (1), (2);
			INSERT INTO ns_comments (user_id) VALUES (1), (2);
		`)

		dst := filepath.Join(t.TempDir(), "item134-refused.db")
		mig := &Migrator{
			Source: mysqlEng, Target: sqliteEng,
			SourceDSN: mysqlSource, TargetDSN: dst,
			MigrationID: "item134-mysql-to-sqlite-index-name-collision",
		}
		err := mig.Run(ctx2min(t))
		// The message must name both tables and the colliding identifier —
		// the operator's only route to the fix is renaming one of them.
		requireRefusalBeforeAnyDataMoved(t, err,
			[]string{"user_id", "ns_posts", "ns_comments", "SILENTLY no-op"},
			sqliteHasTable(dst, "ns_posts"))
	})

	// THE CONTROL. Distinct index names on the same two tables must migrate,
	// and BOTH indexes must exist on the target — which is the independent
	// value the refused case is measured against: without it, "refused" and
	// "this pair never worked" are the same observation.
	t.Run("distinct index names still migrate, both indexes and all rows", func(t *testing.T) {
		applyMySQLDDL(t, mysqlSource, `
			DROP TABLE IF EXISTS ns_posts, ns_comments;
			CREATE TABLE ns_posts (
				id      BIGINT NOT NULL AUTO_INCREMENT,
				user_id BIGINT NOT NULL,
				PRIMARY KEY (id),
				KEY ns_posts_user_id (user_id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			CREATE TABLE ns_comments (
				id      BIGINT NOT NULL AUTO_INCREMENT,
				user_id BIGINT NOT NULL,
				PRIMARY KEY (id),
				UNIQUE KEY ns_comments_user_id (user_id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			INSERT INTO ns_posts (user_id) VALUES (1), (2);
			INSERT INTO ns_comments (user_id) VALUES (1), (2);
		`)

		dst := filepath.Join(t.TempDir(), "item134-allowed.db")
		mig := &Migrator{
			Source: mysqlEng, Target: sqliteEng,
			SourceDSN: mysqlSource, TargetDSN: dst,
			MigrationID: "item134-mysql-to-sqlite-distinct-index-names",
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("two tables with DISTINCT index names must still migrate to SQLite: %v", err)
		}

		db, err := sql.Open("sqlite", dst)
		if err != nil {
			t.Fatalf("open sqlite target: %v", err)
		}
		defer func() { _ = db.Close() }()

		for _, tbl := range []string{"ns_posts", "ns_comments"} {
			var n int
			if err := db.QueryRow(`SELECT COUNT(*) FROM ` + tbl).Scan(&n); err != nil {
				t.Fatalf("count %s on the target: %v", tbl, err)
			}
			if n != 2 {
				t.Errorf("%s row count on the target = %d; want 2", tbl, n)
			}
		}
		for _, idx := range []string{"ns_posts_user_id", "ns_comments_user_id"} {
			var n int
			if err := db.QueryRow(
				`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?`, idx,
			).Scan(&n); err != nil {
				t.Fatalf("probe index %q: %v", idx, err)
			}
			if n != 1 {
				t.Errorf("index %q is missing from the SQLite target (found %d); the whole point of the "+
					"collision refusal is that BOTH declared indexes survive when their names differ",
					idx, n)
			}
		}
	})
}
