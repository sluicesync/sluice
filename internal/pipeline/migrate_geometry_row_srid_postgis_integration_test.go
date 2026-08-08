//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-08-05 C-14, the Postgres half. See the MySQL sibling
// (migrate_geometry_row_srid_mysql_integration_test.go) for the class;
// this file pins the same two cells on PostGIS, where the unconstrained
// type is spelled `geometry` and the constrained one `geometry(Point,4326)`.
//
// PostGIS's `geometry_columns` view reports srid 0 for the unconstrained
// column, so sluice created the target column unconstrained too and
// re-stamped every 4326 row to SRID 0 — a point that is now off the coast
// of Africa rather than where it was. The oracle is the TARGET's own
// ST_SRID(), compared against the SOURCE's own.
//
// NOTE the CI convention this file obeys: the PostGIS job runs
// `-run 'PostGIS_'`, so the test name must contain that substring or it
// silently never runs.
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

// pgGeomRowSRIDSeedDDL puts the constrained and unconstrained shapes side
// by side, both holding a 4326 point.
const pgGeomRowSRIDSeedDDL = `
	CREATE EXTENSION IF NOT EXISTS postgis;

	CREATE TABLE geo_declared (
		id  INT PRIMARY KEY,
		loc geometry(Point, 4326) NOT NULL
	);

	CREATE TABLE geo_undeclared (
		id  INT PRIMARY KEY,
		loc geometry NOT NULL
	);

	INSERT INTO geo_declared   (id, loc) VALUES (1, ST_SetSRID(ST_MakePoint(1, 2), 4326));
	INSERT INTO geo_undeclared (id, loc) VALUES (1, ST_SetSRID(ST_MakePoint(1, 2), 4326));
`

// TestMigrate_PostGIS_GeometryRowSRID_DeclaredCarriesUndeclaredRefuses is
// the two-cell pin: the constrained column round-trips its SRID, the
// unconstrained one refuses instead of losing it.
func TestMigrate_PostGIS_GeometryRowSRID_DeclaredCarriesUndeclaredRefuses(t *testing.T) {
	source, target, cleanup := startPostgresWithPostGIS(t)
	defer cleanup()
	applyPGDDL(t, source, pgGeomRowSRIDSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Cell 1 — column and row agree at 4326; the per-COLUMN carriage is
	// sufficient and the SRID must survive.
	mig := &Migrator{
		Source: pgEng, Target: pgEng,
		SourceDSN: source, TargetDSN: target,
		Filter:      migcore.TableFilter{Include: []string{"geo_declared"}},
		MigrationID: "pg-geom-srid-declared",
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (declared SRID): %v", err)
	}
	srcSRID := readPGSRID(t, ctx, source, "geo_declared")
	tgtSRID := readPGSRID(t, ctx, target, "geo_declared")
	if srcSRID != 4326 {
		t.Fatalf("premise check: source geo_declared ST_SRID = %d; want 4326 "+
			"(the fixture is not exercising what this test claims)", srcSRID)
	}
	if tgtSRID != srcSRID {
		t.Errorf("target ST_SRID = %d; source ST_SRID = %d — the SRID did not survive the migration",
			tgtSRID, srcSRID)
	}

	// Cell 2 — the column is unconstrained, the row carries 4326.
	mig2 := &Migrator{
		Source: pgEng, Target: pgEng,
		SourceDSN: source, TargetDSN: target,
		Filter:      migcore.TableFilter{Include: []string{"geo_undeclared"}},
		MigrationID: "pg-geom-srid-undeclared",
	}
	err := mig2.Run(ctx)
	if err == nil {
		landed := readPGSRID(t, ctx, target, "geo_undeclared")
		t.Fatalf("C-14: migrating an unconstrained geometry column SUCCEEDED; the row's "+
			"SRID 4326 landed on the target as %d", landed)
	}
	if !errors.Is(err, ir.ErrGeometryRowSRIDMismatch) {
		t.Errorf("refusal %v does not wrap ir.ErrGeometryRowSRIDMismatch", err)
	}
	if !strings.Contains(err.Error(), "4326") {
		t.Errorf("refusal does not name the row's SRID: %v", err)
	}
}

// readPGSRID asks PostGIS itself what SRID the stored value carries.
// ST_SRID reads the VALUE, not the column's typmod, which is the quantity
// in question.
func readPGSRID(t *testing.T, ctx context.Context, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
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
