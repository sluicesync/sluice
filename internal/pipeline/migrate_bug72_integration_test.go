//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for Bug 72 (a wide bounded PG `varchar(N)` is not
// down-mapped to a MySQL TEXT family type; PG→MySQL create-tables
// hard-fails):
//
//   - Pre-fix: the MySQL DDL emitter wrote `VARCHAR(N)` literally.
//     varchar(16384) → Error 1074 (column length too big);
//     varchar(16383) → Error 1118 (InnoDB 65535-byte row limit);
//     varchar(65535) → Error 1074. Exit 1 at create-tables, no table.
//
//   - Post-fix: a varchar(N) over MySQL's representable cap is
//     down-mapped to the smallest TEXT tier that still holds N chars
//     (worst-case N*4 bytes), mirroring the unbounded text→LONGTEXT
//     policy, plus a loud operator-actionable advisory at migrate
//     preflight (and schema preview). Narrow varchar(255) unchanged.
//
// This is the verbatim BUG-CATALOG section 72 minimal repro:
// varchar(16383)/(16384)/(70000) plus a varchar(255) regression guard,
// create-tables must succeed and data must round-trip.

package pipeline

import (
	"database/sql"
	"log/slog"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

const bug72SeedDDL = `
	CREATE TABLE wide (
	  id    int PRIMARY KEY,
	  small varchar(255),     -- regression guard: stays VARCHAR(255)
	  v1    varchar(16383),   -- pre-fix Error 1118 (row size)
	  v2    varchar(16384),   -- pre-fix Error 1074 (column length)
	  v3    varchar(70000)    -- pre-fix Error 1074
	);
	INSERT INTO wide (id, small, v1, v2, v3) VALUES
	  (1, 'hello', repeat('a',100), repeat('b',200), repeat('c',300));
`

// TestMigrate_PostgresToMySQL_Bug72WideVarcharDownmap pins the closure:
// pre-fix create-tables died with a raw MySQL Error 1074/1118. Post-fix
// migrate exits 0, each wide column lands as a TEXT-family type sized to
// hold N chars, the narrow varchar(255) is unchanged, data round-trips,
// and the loud advisory FIRES at migrate preflight.
func TestMigrate_PostgresToMySQL_Bug72WideVarcharDownmap(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, bug72SeedDDL)

	// Capture the slog stream so the loud advisory can be asserted.
	logBuf := &lockedBuffer{}
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	defer slog.SetDefault(prevDefault)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    mysqlEng,
		SourceDSN: pgSource,
		TargetDSN: mysqlTarget,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		// Pre-fix this failed at create-tables with Error 1074/1118.
		t.Fatalf("Migrator.Run (PG→MySQL wide varchar must migrate, no 1074/1118): %v", err)
	}

	// The loud advisory MUST have fired.
	logs := string(logBuf.Bytes())
	for _, want := range []string{
		"migrate",            // preflight contextID
		"varchar",            // the source type named
		"TEXT",               // the target family named
		"Migration proceeds", // it's a NOTICE, not a refusal
		"--type-override",    // the escape hatch
		"wide.v1",            // an affected column named
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("migrate log missing Bug 72 advisory fragment %q\n--- captured logs ---\n%s", want, logs)
		}
	}

	mysqlDB, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = mysqlDB.Close() }()

	colType := func(col string) string {
		var ct string
		if err := mysqlDB.QueryRow(`
			SELECT DATA_TYPE FROM information_schema.columns
			WHERE table_name = 'wide' AND column_name = ?`, col).Scan(&ct); err != nil {
			t.Fatalf("read wide.%s DATA_TYPE: %v", col, err)
		}
		return strings.ToLower(ct)
	}
	// small stays VARCHAR; v1 (16383*4=65532 ≤ 65535) → text;
	// v2 (16384*4=65536 > 65535) and v3 (70000*4) → mediumtext.
	if got := colType("small"); got != "varchar" {
		t.Errorf("wide.small DATA_TYPE = %q; want varchar (narrow must be unchanged)", got)
	}
	if got := colType("v1"); got != "text" {
		t.Errorf("wide.v1 DATA_TYPE = %q; want text (varchar(16383) down-map)", got)
	}
	if got := colType("v2"); got != "mediumtext" {
		t.Errorf("wide.v2 DATA_TYPE = %q; want mediumtext (varchar(16384) down-map)", got)
	}
	if got := colType("v3"); got != "mediumtext" {
		t.Errorf("wide.v3 DATA_TYPE = %q; want mediumtext (varchar(70000) down-map)", got)
	}

	var small, v1, v2, v3 string
	if err := mysqlDB.QueryRow(`SELECT small, v1, v2, v3 FROM wide WHERE id = 1`).
		Scan(&small, &v1, &v2, &v3); err != nil {
		t.Fatalf("query mysql target: %v", err)
	}
	if small != "hello" {
		t.Errorf("wide.small = %q; want \"hello\"", small)
	}
	if v1 != strings.Repeat("a", 100) {
		t.Errorf("wide.v1 length = %d; want 100 'a's", len(v1))
	}
	if v2 != strings.Repeat("b", 200) {
		t.Errorf("wide.v2 length = %d; want 200 'b's", len(v2))
	}
	if v3 != strings.Repeat("c", 300) {
		t.Errorf("wide.v3 length = %d; want 300 'c's", len(v3))
	}
}

// TestMigrate_PostgresToPostgres_Bug72VarcharUnchanged guards that the
// wide-varchar down-map is cross-engine only: PG→PG must keep
// varchar(N) as varchar(N) at every width (PG has no such limit).
func TestMigrate_PostgresToPostgres_Bug72VarcharUnchanged(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, bug72SeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (PG→PG wide varchar must migrate unchanged): %v", err)
	}

	pgDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = pgDB.Close() }()

	rows, err := pgDB.Query(`
		SELECT column_name, character_maximum_length
		FROM information_schema.columns
		WHERE table_name = 'wide' AND column_name IN ('small','v1','v2','v3')
		ORDER BY column_name`)
	if err != nil {
		t.Fatalf("query pg target schema: %v", err)
	}
	defer func() { _ = rows.Close() }()

	want := map[string]int64{"small": 255, "v1": 16383, "v2": 16384, "v3": 70000}
	seen := map[string]int64{}
	for rows.Next() {
		var name string
		var n sql.NullInt64
		if err := rows.Scan(&name, &n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !n.Valid {
			t.Errorf("wide.%s character_maximum_length is NULL; want a bounded varchar (PG→PG must not down-map)", name)
			continue
		}
		seen[name] = n.Int64
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	for col, wantLen := range want {
		if seen[col] != wantLen {
			t.Errorf("wide.%s length = %d; want %d (PG→PG varchar must round-trip unchanged)", col, seen[col], wantLen)
		}
	}
}
