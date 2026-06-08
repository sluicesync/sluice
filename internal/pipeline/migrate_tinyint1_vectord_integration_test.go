//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"

	_ "github.com/go-sql-driver/mysql"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// vectorDSeedDDL creates a MySQL source table whose TINYINT(1) column holds
// real integers outside {0,1} — the Vector D shape. TINYINT(1) is only a
// display width; the column stores the full signed 8-bit range, so no relaxed
// sql_mode is needed to seed 2 / 127 / -1.
const vectorDSeedDDL = `
	CREATE TABLE flags (
		id   INT PRIMARY KEY,
		flag TINYINT(1) NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	INSERT INTO flags (id, flag) VALUES
		(1, 0),
		(2, 1),
		(3, 2),
		(4, 127),
		(5, -1);
`

// TestMigrate_TinyInt1OutOfRange_VectorD_DefaultWarnsAndCollapses pins the
// DEFAULT behavior: MySQL TINYINT(1) maps to boolean per the documented
// convention, so non-{0,1} values are collapsed to true — but the read path
// now WARNs loudly (naming the column + the --type-override remedy) instead of
// doing it silently. MySQL→Postgres.
func TestMigrate_TinyInt1OutOfRange_VectorD_DefaultWarnsAndCollapses(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, vectorDSeedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	logs := captureSlog(t)

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSource,
		TargetDSN: pgTarget,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (default): %v", err)
	}

	// The WARN must have fired, naming the column and pointing at the remedy.
	out := logs.String()
	if !strings.Contains(out, "column=flags.flag") {
		t.Errorf("no Vector D WARN naming flags.flag; got:\n%s", out)
	}
	if !strings.Contains(out, "--type-override flags.flag=smallint") {
		t.Errorf("WARN did not point at the --type-override remedy; got:\n%s", out)
	}

	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Target column is boolean (the default mapping)...
	var dataType string
	if err := db.QueryRowContext(ctx,
		"SELECT data_type FROM information_schema.columns WHERE table_name='flags' AND column_name='flag'").
		Scan(&dataType); err != nil {
		t.Fatalf("read target column type: %v", err)
	}
	if dataType != "boolean" {
		t.Errorf("target flags.flag data_type = %q; want boolean (default mapping)", dataType)
	}

	// ...and every non-zero value collapsed to true (the documented, now-loud
	// lossy behavior): id 1 (0) -> false; ids 2/3/4/5 (1/2/127/-1) -> true.
	want := map[int]bool{1: false, 2: true, 3: true, 4: true, 5: true}
	rows, err := db.QueryContext(ctx, "SELECT id, flag FROM flags ORDER BY id")
	if err != nil {
		t.Fatalf("query target: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := map[int]bool{}
	for rows.Next() {
		var id int
		var b bool
		if err := rows.Scan(&id, &b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = b
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("flags id=%d: target flag = %v; want %v", id, got[id], w)
		}
	}
}

// TestMigrate_TinyInt1OutOfRange_VectorD_IntegerOverridePreserves pins the
// remedy: --type-override <col>=smallint (and =int) rewrites the IR type the
// reader decodes with, so the TINYINT(1) cell is read as an integer and the
// real value (2, 127, -1) is preserved end-to-end — both cross-engine
// (MySQL→PG) and same-engine (MySQL→MySQL, the direction Vector D corrupts
// even though both sides are MySQL).
func TestMigrate_TinyInt1OutOfRange_VectorD_IntegerOverridePreserves(t *testing.T) {
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	wantVals := map[int]int64{1: 0, 2: 1, 3: 2, 4: 127, 5: -1}

	t.Run("MySQL_to_PG_smallint", func(t *testing.T) {
		mysqlSource, _, mysqlCleanup := startMySQL(t)
		defer mysqlCleanup()
		_, pgTarget, pgCleanup := startPostgres(t)
		defer pgCleanup()
		applyMySQLDDL(t, mysqlSource, vectorDSeedDDL)

		mig := &Migrator{
			Source: mysqlEng, Target: pgEng,
			SourceDSN: mysqlSource, TargetDSN: pgTarget,
			Mappings: []config.Mapping{{Table: "flags", Column: "flag", TargetType: "smallint"}},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("Migrator.Run: %v", err)
		}

		db, err := sql.Open("pgx", pgTarget)
		if err != nil {
			t.Fatalf("open target: %v", err)
		}
		defer func() { _ = db.Close() }()

		var dataType string
		if err := db.QueryRowContext(ctx,
			"SELECT data_type FROM information_schema.columns WHERE table_name='flags' AND column_name='flag'").
			Scan(&dataType); err != nil {
			t.Fatalf("read target column type: %v", err)
		}
		if dataType != "smallint" {
			t.Errorf("target flags.flag data_type = %q; want smallint (override)", dataType)
		}
		assertIntColumn(t, ctx, db, wantVals)
	})

	t.Run("MySQL_to_MySQL_smallint", func(t *testing.T) {
		mysqlSource, mysqlTarget, mysqlCleanup := startMySQL(t)
		defer mysqlCleanup()
		applyMySQLDDL(t, mysqlSource, vectorDSeedDDL)

		mig := &Migrator{
			Source: mysqlEng, Target: mysqlEng,
			SourceDSN: mysqlSource, TargetDSN: mysqlTarget,
			Mappings: []config.Mapping{{Table: "flags", Column: "flag", TargetType: "smallint"}},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("Migrator.Run: %v", err)
		}

		db, err := sql.Open("mysql", mysqlTarget)
		if err != nil {
			t.Fatalf("open target: %v", err)
		}
		defer func() { _ = db.Close() }()

		// Target column must be SMALLINT, NOT a re-derived TINYINT(1)
		// (which would re-trigger the boolean mapping on a round-trip).
		// Scope to the target database — source and target share the MySQL
		// container, so an unscoped information_schema query would also match
		// the source's still-tinyint(1) `flags` table.
		var colType string
		if err := db.QueryRowContext(ctx,
			"SELECT COLUMN_TYPE FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='flags' AND column_name='flag'").
			Scan(&colType); err != nil {
			t.Fatalf("read target column type: %v", err)
		}
		if !strings.HasPrefix(strings.ToLower(colType), "smallint") {
			t.Errorf("target flags.flag column_type = %q; want smallint*", colType)
		}
		assertIntColumn(t, ctx, db, wantVals)
	})
}

// assertIntColumn reads id->flag from the migrated `flags` table and checks
// each integer value was preserved exactly (no collapse to 0/1).
func assertIntColumn(t *testing.T, ctx context.Context, db *sql.DB, want map[int]int64) {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT id, flag FROM flags ORDER BY id")
	if err != nil {
		t.Fatalf("query target: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := map[int]int64{}
	for rows.Next() {
		var id int
		var v int64
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("flags id=%d: target flag = %d; want %d (integer preserved via override)", id, got[id], w)
		}
	}
}
