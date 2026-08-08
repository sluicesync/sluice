//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 153's GEOMETRY cell.
//
// It lives here rather than in the plain-integration sibling for one
// reason: geometry needs PostGIS, and PostGIS needs a different image
// and its own CI job. Splitting it is what the roster gate in
// migrate_pg_retarget_shape_mysql_integration_test.go's header refuses
// to let stay implied — that file states geometry is NOT in it, and this
// is where the claim is discharged.
//
// It grades exactly the same comparison, through the same
// [retargetShapeCase], against the same oracle (MySQL's own
// information_schema, read back through the MySQL SchemaReader).
package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// retargetShapeGeometryFamilies is the geometry roster. Subtype and SRID
// are both in [ir.Geometry.String], so both are part of the comparison
// the shape gate makes — a retarget that lost either would false-refuse
// exactly as `json` did.
var retargetShapeGeometryFamilies = []string{"g_pt", "g_line", "g_poly", "g_any", "g_nosrid"}

// retargetShapeGeometrySeedDDL mirrors the plain sibling's shape: one
// bare table, one whose every column is reached through a DOMAIN.
const retargetShapeGeometrySeedDDL = `
	CREATE EXTENSION IF NOT EXISTS postgis;

	CREATE DOMAIN rsg_pt     AS geometry(POINT, 4326);
	CREATE DOMAIN rsg_line   AS geometry(LINESTRING, 4326);
	CREATE DOMAIN rsg_poly   AS geometry(POLYGON, 4326);
	CREATE DOMAIN rsg_any    AS geometry;
	CREATE DOMAIN rsg_nosrid AS geometry(POINT);

	CREATE TABLE rsgplain (
		id       INT PRIMARY KEY,
		g_pt     geometry(POINT, 4326),
		g_line   geometry(LINESTRING, 4326),
		g_poly   geometry(POLYGON, 4326),
		g_any    geometry,
		g_nosrid geometry(POINT)
	);

	CREATE TABLE rsgdom (
		id       INT PRIMARY KEY,
		g_pt     rsg_pt,
		g_line   rsg_line,
		g_poly   rsg_poly,
		g_any    rsg_any,
		g_nosrid rsg_nosrid
	);

	INSERT INTO rsgplain VALUES (
		1,
		ST_SetSRID(ST_MakePoint(1, 2), 4326),
		ST_SetSRID(ST_MakeLine(ST_MakePoint(0, 0), ST_MakePoint(1, 1)), 4326),
		ST_SetSRID(ST_MakePolygon(ST_MakeLine(ARRAY[
			ST_MakePoint(0, 0), ST_MakePoint(1, 0), ST_MakePoint(1, 1), ST_MakePoint(0, 0)
		])), 4326),
		ST_MakePoint(3, 4),
		ST_MakePoint(5, 6)
	);

	INSERT INTO rsgdom SELECT * FROM rsgplain;
`

// TestMigrate_PGToMySQL_RetargetedGeometryShapeMatchesTheCatalogReadBack
// is the geometry × {bare, DOMAIN} cell of item 153's gate, plus the
// re-run assertion the operator actually hits.
func TestMigrate_PGToMySQL_RetargetedGeometryShapeMatchesTheCatalogReadBack(t *testing.T) {
	_, pgSource, pgCleanup := startPostgresWithPostGIS(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, retargetShapeGeometrySeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	newMigrator := func(id string) *Migrator {
		return &Migrator{
			Source: pgEng, Target: myEng,
			SourceDSN: pgSource, TargetDSN: mysqlTarget,
			MigrationID: id,
			// PostGIS installs geometry_columns / geography_columns
			// views whose bodies are PG-specific; the sibling
			// TestMigrate_PostGIS_PGToMySQL skips them for the same
			// reason. This test is about column shapes.
			SkipViews: true,
		}
	}

	if err := newMigrator("retarget-geom-first").Run(ctx); err != nil {
		t.Fatalf("first Migrator.Run PG→MySQL (geometry): %v", err)
	}

	t.Run("retargeted expected equals the catalog read-back", func(t *testing.T) {
		retargetShapeCase{
			sourceEngine: pgEng, sourceDSN: pgSource,
			targetEngine: myEng, targetDSN: mysqlTarget,
			bareTable: "rsgplain", domainTable: "rsgdom",
			families: retargetShapeGeometryFamilies,
		}.assertParity(t, ctx)
	})

	t.Run("a second migrate over the same target does not refuse", func(t *testing.T) {
		db, err := sql.Open("mysql", mysqlTarget)
		if err != nil {
			t.Fatalf("open mysql target: %v", err)
		}
		defer func() { _ = db.Close() }()
		for _, tbl := range []string{"rsgplain", "rsgdom"} {
			if _, err := db.ExecContext(ctx, "TRUNCATE TABLE "+tbl); err != nil {
				t.Fatalf("truncate %s: %v", tbl, err)
			}
		}
		if err := newMigrator("retarget-geom-second").Run(ctx); err != nil {
			t.Fatalf("second Migrator.Run over the SAME target (geometry): %v\n\n"+
				"Item 153's shape if it names SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH on a geometry column",
				err)
		}
		for _, tbl := range []string{"rsgplain", "rsgdom"} {
			var n int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&n); err != nil {
				t.Fatalf("count %s: %v", tbl, err)
			}
			if n != 1 {
				t.Errorf("after the second migrate, %s holds %d row(s); want 1 — the re-run was accepted "+
					"but copied nothing", tbl, n)
			}
		}
	})
}
