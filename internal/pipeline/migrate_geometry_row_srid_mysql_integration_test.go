//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-08-05 C-14, the MySQL half: a row whose geometry carries an
// SRID the COLUMN does not declare must not migrate silently.
//
// MySQL accepts a different SRID on every row of a spatial column declared
// WITHOUT the `SRID n` attribute, and reports srs_id 0 for that column. So
// `loc POINT` holding `ST_GeomFromText('POINT(1 2)', 4326)` is ordinary,
// supported MySQL — and pre-fix sluice read it as bare WKB, created the
// target column with no SRID, and re-stamped every row to SRID 0. The
// coordinates survived; what they meant did not.
//
// The oracle is the TARGET's own ST_SRID(), compared against the SOURCE's
// own ST_SRID() — two independent servers answering the same question,
// never sluice's report of what it did.
package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// geomRowSRIDSeedDDL declares the two column shapes side by side: one
// constrained to 4326 (where sluice's per-COLUMN carriage is sufficient)
// and one unconstrained (where it is not). Both hold 4326 rows.
const geomRowSRIDSeedDDL = `
	CREATE TABLE geo_declared (
		id  INT NOT NULL PRIMARY KEY,
		loc POINT SRID 4326 NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

	CREATE TABLE geo_undeclared (
		id  INT NOT NULL PRIMARY KEY,
		loc POINT NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

	INSERT INTO geo_declared  (id, loc) VALUES (1, ST_GeomFromText('POINT(1 2)', 4326));
	INSERT INTO geo_undeclared (id, loc) VALUES (1, ST_GeomFromText('POINT(1 2)', 4326));
`

// TestMigrate_MySQLGeometryRowSRID_DeclaredCarriesUndeclaredRefuses is the
// two-cell pin: the declared column round-trips its SRID, the undeclared
// one refuses instead of losing it.
func TestMigrate_MySQLGeometryRowSRID_DeclaredCarriesUndeclaredRefuses(t *testing.T) {
	source, target, cleanup := startMySQL(t)
	defer cleanup()
	applyMySQLDDL(t, source, geomRowSRIDSeedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Cell 1 — the column DECLARES 4326, so the row's SRID and the
	// column's agree and sluice's per-column carriage is enough.
	mig := &Migrator{
		Source: mysqlEng, Target: mysqlEng,
		SourceDSN: source, TargetDSN: target,
		Filter: migcore.TableFilter{Include: []string{"geo_declared"}},
		// A distinct id per cell: the two runs share a target, and the
		// auto-derived id would make the second look like a completed
		// re-run of the first.
		MigrationID: "geom-srid-declared",
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (declared SRID): %v", err)
	}
	srcSRID := readMySQLSRID(t, ctx, source, "geo_declared")
	tgtSRID := readMySQLSRID(t, ctx, target, "geo_declared")
	if srcSRID != 4326 {
		t.Fatalf("premise check: source geo_declared ST_SRID = %d; want 4326 "+
			"(the fixture is not exercising what this test claims)", srcSRID)
	}
	if tgtSRID != srcSRID {
		t.Errorf("target ST_SRID = %d; source ST_SRID = %d — the SRID did not survive the migration",
			tgtSRID, srcSRID)
	}

	// Cell 2 — the column declares NOTHING, the row carries 4326. sluice
	// cannot carry that, so it must refuse by name rather than land the
	// point at SRID 0.
	mig2 := &Migrator{
		Source: mysqlEng, Target: mysqlEng,
		SourceDSN: source, TargetDSN: target,
		Filter:      migcore.TableFilter{Include: []string{"geo_undeclared"}},
		MigrationID: "geom-srid-undeclared",
	}
	err := mig2.Run(ctx)
	if err == nil {
		landed := readMySQLSRID(t, ctx, target, "geo_undeclared")
		t.Fatalf("C-14: migrating an undeclared-SRID column SUCCEEDED; the row's SRID 4326 "+
			"landed on the target as %d", landed)
	}
	if !errors.Is(err, ir.ErrGeometryRowSRIDMismatch) {
		t.Errorf("refusal %v does not wrap ir.ErrGeometryRowSRIDMismatch", err)
	}
	if !strings.Contains(err.Error(), "4326") {
		t.Errorf("refusal does not name the row's SRID: %v", err)
	}
}

// readMySQLSRID asks the server itself what SRID the stored geometry
// carries. ST_SRID reads the VALUE's spatial reference, not the column's
// declaration, which is exactly the quantity in question.
func readMySQLSRID(t *testing.T, ctx context.Context, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var srid int
	if err := db.QueryRowContext(ctx, "SELECT ST_SRID(loc) FROM "+table+" WHERE id = 1").Scan(&srid); err != nil {
		t.Fatalf("ST_SRID(%s.loc): %v", table, err)
	}
	return srid
}
