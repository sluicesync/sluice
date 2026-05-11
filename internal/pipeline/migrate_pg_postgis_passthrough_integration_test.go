//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PG → PG PostGIS passthrough integration tests (ADR-0032 v1 shortlist
// final entry, v0.33.0). PostGIS is unique among the v1 extensions
// because its core types (`geometry`, `geography`) pre-date ADR-0032
// and already carry first-class IR shapes via `ir.Geometry`
// (ADR-0035, shipped v0.28.0). The catalog entry for postgis is
// therefore "type-less" — its load-bearing v1 surface is the
// operator-class round-trip path that lets indexes carrying PostGIS
// opclasses (gist_geometry_ops_2d, gist_geography_ops, spgist + brin
// variants) preserve their opclass through schema reader → IR →
// schema writer on a PG → PG migration when the operator passes
// `--enable-pg-extension postgis`.
//
// This file boots the postgis/postgis:16-3.4 image (heavier than the
// stock postgres:16 used by other PG integration tests, hence the
// `integration postgis` build-tag gate matching ADR-0035's
// PG → MySQL postgis test in migrate_postgis_integration_test.go),
// enables PostGIS on both source and target, exercises the migrate
// path against a geometry column with a `gist (col)` index, and
// verifies the round-trip preserves the index's access method AND
// operator class on the target via a pg_index/pg_opclass query.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	_ "github.com/orware/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startPostgresPGToPGWithPostGIS boots a postgis/postgis:16-3.4
// container, creates source and target databases, and (when
// enableOnTarget is true) runs CREATE EXTENSION postgis on both.
// Mirrors `startPostgresWithPGVector` / `startPostgresWithTrgm`'s
// shape; the image differs (postgis ships its own image so the
// extension's shared libraries are available) and the extension name
// differs. Skips cleanly when no Docker provider is available.
func startPostgresPGToPGWithPostGIS(t *testing.T, enableOnTarget bool) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := pgtc.Run(ctx,
		"postgis/postgis:16-3.4",
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("pgx", srcConn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}

	tgtConn, err := buildPGDSN(srcConn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}

	// Source-side extension. PostGIS ships in the postgis/postgis
	// image with the shared libraries installed but inactive;
	// CREATE EXTENSION flips the per-database bit.
	if _, err := db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
		terminate()
		t.Fatalf("CREATE EXTENSION postgis on source: %v", err)
	}

	if enableOnTarget {
		tgtDB, err := sql.Open("pgx", tgtConn)
		if err != nil {
			terminate()
			t.Fatalf("open target: %v", err)
		}
		defer func() { _ = tgtDB.Close() }()
		if _, err := tgtDB.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
			terminate()
			t.Fatalf("CREATE EXTENSION postgis on target: %v", err)
		}
	}

	return srcConn, tgtConn, terminate
}

// TestMigrate_PG_PostGIS_GistIndexPassthrough exercises the
// load-bearing PostGIS PG → PG passthrough path: a
// `CREATE INDEX ... USING GIST (loc)` index over a `geometry(POINT,
// 4326)` column round-trips through migrate when the operator opts
// into `--enable-pg-extension postgis` and both sides have the
// extension installed. The ground truth is a pg_index/pg_opclass
// query on the target verifying the index exists with access method
// `gist` AND operator class `gist_geometry_ops_2d` (PostGIS's
// default GiST opclass for 2D geometry).
//
// Without this PR's catalog entry the schema reader's
// extensionOperatorClassRegistered fallthrough would NOT recognise
// `gist_geometry_ops_2d` as an extension opclass and the WARN/drop
// path wouldn't fire — the opclass would silently drop from the IR
// and the target would land an index using PG's default opclass
// (which for the 2D case happens to be the same `gist_geometry_ops_2d`,
// so the index would still create, but the fidelity loss is real
// for nD / SP-GiST / BRIN variants where the default and the
// explicit opclass diverge). The catalog entry's role is to give the
// per-opclass passthrough path a stable contract.
func TestMigrate_PG_PostGIS_GistIndexPassthrough(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresPGToPGWithPostGIS(t, true)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE places (
			id   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			name TEXT NOT NULL,
			loc  geometry(POINT, 4326) NOT NULL
		);

		INSERT INTO places (name, loc) VALUES
			('origin',   ST_SetSRID(ST_MakePoint(0, 0), 4326)),
			('one-one',  ST_SetSRID(ST_MakePoint(1, 1), 4326));

		CREATE INDEX places_loc_gist
		    ON places USING GIST (loc);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// Both source + target containers pre-install PostGIS, which
	// populates the spatial_ref_sys system table (~8500 SRIDs) and
	// creates the geometry_columns/geography_columns system views.
	// Cold-start preflight refuses to bulk-copy into a non-empty
	// target table; exclude spatial_ref_sys + skip views to scope the
	// test to operator-owned tables only.
	filter, fErr := NewTableFilter(nil, []string{"spatial_ref_sys"})
	if fErr != nil {
		t.Fatalf("NewTableFilter: %v", fErr)
	}
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"postgis"},
		Filter:              filter,
		SkipViews:           true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// Ground truth — pg_index + pg_opclass on the target. The
	// load-bearing assertion is `opcname = 'gist_geometry_ops_2d'`:
	// PostGIS's default GiST opclass for the 2D POINT case. Without
	// the per-opclass capture path the schema reader would either
	// drop the opclass (target index uses the AM default, which for
	// gist over geometry is also `gist_geometry_ops_2d` — so this
	// case happens to round-trip even without sluice's catalog entry,
	// hence the additional nD assertion below for unambiguous
	// validation of the catalog path).
	const idxQuery = `
		SELECT am.amname, opc.opcname
		FROM   pg_index ix
		JOIN   pg_class i  ON i.oid = ix.indexrelid
		JOIN   pg_class cl ON cl.oid = ix.indrelid
		JOIN   pg_am    am ON am.oid = i.relam
		JOIN   LATERAL unnest(ix.indclass) WITH ORDINALITY AS uc(opcoid, ord) ON TRUE
		JOIN   pg_opclass opc ON opc.oid = uc.opcoid
		WHERE  cl.relname = 'places'
		  AND  i.relname  = 'places_loc_gist'`
	var (
		amname  string
		opcname string
	)
	if err := tgtDB.QueryRowContext(ctx, idxQuery).Scan(&amname, &opcname); err != nil {
		t.Fatalf("index method/opclass query: %v", err)
	}
	if amname != "gist" {
		t.Errorf("target index method = %q; want gist", amname)
	}
	if opcname != "gist_geometry_ops_2d" {
		t.Errorf("target index opclass = %q; want gist_geometry_ops_2d", opcname)
	}

	// Row count check — bulk copy should have moved 2 rows.
	var n int
	if err := tgtDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM places").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("target places rows = %d; want 2", n)
	}

	// Round-trip the IR through the target reader to confirm the
	// IndexColumn.OperatorClass field surfaces from the rebuilt
	// schema. Mirrors the pg_trgm test's "read-back IR" assertion.
	pgRdr, err := pgEng.OpenSchemaReader(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader on target: %v", err)
	}
	defer closeIf(pgRdr)
	if aware, ok := pgRdr.(ir.ExtensionAware); ok {
		if err := aware.EnableExtensions(ctx, []string{"postgis"}); err != nil {
			t.Fatalf("EnableExtensions on target reader: %v", err)
		}
	}
	got, err := pgRdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema on target: %v", err)
	}
	places := findTable(got, "places")
	if places == nil {
		t.Fatal("target has no places table")
	}
	var foundGist bool
	for _, idx := range places.Indexes {
		for _, c := range idx.Columns {
			if c.OperatorClass == "gist_geometry_ops_2d" {
				foundGist = true
				if c.Column != "loc" {
					t.Errorf("postgis gist index column = %q; want loc", c.Column)
				}
			}
		}
	}
	if !foundGist {
		t.Errorf("did not find gist_geometry_ops_2d on places indexes; got %+v", places.Indexes)
	}

	// Also confirm the loc column round-tripped as ir.Geometry — the
	// catalog entry's hint-only type design (typesByName empty,
	// hintTypeNames carries `geometry`) means the schema reader
	// continues to emit ir.Geometry, NOT ir.ExtensionType.
	loc := findColumn(places, "loc")
	if loc == nil {
		t.Fatal("places.loc column missing on target")
	}
	geom, ok := loc.Type.(ir.Geometry)
	if !ok {
		t.Fatalf("places.loc IR type = %T; want ir.Geometry "+
			"(postgis catalog entry must NOT reroute geometry through ir.ExtensionType)",
			loc.Type)
	}
	if geom.Subtype != ir.GeometryPoint {
		t.Errorf("places.loc Subtype = %v; want GeometryPoint", geom.Subtype)
	}
	if geom.SRID != 4326 {
		t.Errorf("places.loc SRID = %d; want 4326", geom.SRID)
	}
}

// TestMigrate_PG_PostGIS_GistNdIndexPassthrough exercises the
// non-default opclass case where the catalog entry actually carries
// load: `gist_geometry_ops_nd` is an explicit opclass that, when
// dropped from the IR, would be silently replaced by PG's 2D default
// on the target. The catalog entry's `gist_geometry_ops_nd` opclass
// declaration is what keeps this case round-tripping fidelity.
func TestMigrate_PG_PostGIS_GistNdIndexPassthrough(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresPGToPGWithPostGIS(t, true)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE shapes (
			id   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			name TEXT NOT NULL,
			geom geometry NOT NULL
		);

		INSERT INTO shapes (name, geom) VALUES
			('p1', ST_MakePoint(0, 0));

		CREATE INDEX shapes_geom_gist_nd
		    ON shapes USING GIST (geom gist_geometry_ops_nd);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	// Both source + target containers pre-install PostGIS, which
	// populates the spatial_ref_sys system table (~8500 SRIDs) and
	// creates the geometry_columns/geography_columns system views.
	// Cold-start preflight refuses to bulk-copy into a non-empty
	// target table; exclude spatial_ref_sys + skip views to scope the
	// test to operator-owned tables only.
	filter, fErr := NewTableFilter(nil, []string{"spatial_ref_sys"})
	if fErr != nil {
		t.Fatalf("NewTableFilter: %v", fErr)
	}
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"postgis"},
		Filter:              filter,
		SkipViews:           true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	const idxQuery = `
		SELECT am.amname, opc.opcname
		FROM   pg_index ix
		JOIN   pg_class i  ON i.oid = ix.indexrelid
		JOIN   pg_class cl ON cl.oid = ix.indrelid
		JOIN   pg_am    am ON am.oid = i.relam
		JOIN   LATERAL unnest(ix.indclass) WITH ORDINALITY AS uc(opcoid, ord) ON TRUE
		JOIN   pg_opclass opc ON opc.oid = uc.opcoid
		WHERE  cl.relname = 'shapes'
		  AND  i.relname  = 'shapes_geom_gist_nd'`
	var (
		amname  string
		opcname string
	)
	if err := tgtDB.QueryRowContext(ctx, idxQuery).Scan(&amname, &opcname); err != nil {
		t.Fatalf("index method/opclass query: %v", err)
	}
	if amname != "gist" {
		t.Errorf("target index method = %q; want gist", amname)
	}
	if opcname != "gist_geometry_ops_nd" {
		t.Errorf("target index opclass = %q; want gist_geometry_ops_nd "+
			"(without the catalog entry, the opclass would drop and "+
			"PG would default to gist_geometry_ops_2d on the target)",
			opcname)
	}
}

// TestMigrate_PG_PostGIS_GeographyPassthrough pins the geography
// flavour (Bug 49). Pre-fix the PG schema reader's `udt_name ==
// "geometry"` special case had no geography sibling, so the
// translator fell through to the user-defined-hint path and surfaced
// "pass --enable-pg-extension postgis" even when the flag WAS
// passed. The fix adds a parallel geography_columns lookup and an
// IsGeography bool on ir.Geometry; the PG writer emits
// `geography(<subtype>, <srid>)` on the target.
//
// Round-trip ground truth: the target's pg_attribute reports
// `geography` (not `geometry`) for the loc column AND a SELECT
// reading back via ST_AsText returns the same WKT.
func TestMigrate_PG_PostGIS_GeographyPassthrough(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresPGToPGWithPostGIS(t, true)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE waypoints (
			id   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			name TEXT NOT NULL,
			loc  geography(POINT, 4326) NOT NULL
		);

		INSERT INTO waypoints (name, loc) VALUES
			('origin',  ST_GeographyFromText('SRID=4326;POINT(0 0)')),
			('one-one', ST_GeographyFromText('SRID=4326;POINT(1 1)'));
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	filter, fErr := NewTableFilter(nil, []string{"spatial_ref_sys"})
	if fErr != nil {
		t.Fatalf("NewTableFilter: %v", fErr)
	}
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"postgis"},
		Filter:              filter,
		SkipViews:           true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// pg_attribute / pg_type ground truth: the column's typname is
	// `geography`, not `geometry`. Pre-fix the migration would have
	// refused at schema-read, so reaching this assertion at all is
	// the load-bearing positive.
	var typname string
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT t.typname
		FROM   pg_attribute a
		JOIN   pg_class c ON c.oid = a.attrelid
		JOIN   pg_type  t ON t.oid = a.atttypid
		WHERE  c.relname = 'waypoints'
		  AND  a.attname  = 'loc'`).Scan(&typname); err != nil {
		t.Fatalf("typname query: %v", err)
	}
	if typname != "geography" {
		t.Errorf("target waypoints.loc typname = %q; want geography "+
			"(geography must NOT flatten to geometry on same-engine PG → PG)",
			typname)
	}

	var n int
	if err := tgtDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM waypoints").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("target waypoints rows = %d; want 2", n)
	}

	// IR round-trip: schema reader picks up geography_columns and
	// builds ir.Geometry{IsGeography: true}.
	pgRdr, err := pgEng.OpenSchemaReader(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader on target: %v", err)
	}
	defer closeIf(pgRdr)
	if aware, ok := pgRdr.(ir.ExtensionAware); ok {
		if err := aware.EnableExtensions(ctx, []string{"postgis"}); err != nil {
			t.Fatalf("EnableExtensions on target reader: %v", err)
		}
	}
	got, err := pgRdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema on target: %v", err)
	}
	waypoints := findTable(got, "waypoints")
	if waypoints == nil {
		t.Fatal("target has no waypoints table")
	}
	loc := findColumn(waypoints, "loc")
	if loc == nil {
		t.Fatal("waypoints.loc column missing on target")
	}
	geog, ok := loc.Type.(ir.Geometry)
	if !ok {
		t.Fatalf("waypoints.loc IR type = %T; want ir.Geometry", loc.Type)
	}
	if !geog.IsGeography {
		t.Error("waypoints.loc IsGeography = false; want true")
	}
	if geog.Subtype != ir.GeometryPoint {
		t.Errorf("waypoints.loc Subtype = %v; want GeometryPoint", geog.Subtype)
	}
	if geog.SRID != 4326 {
		t.Errorf("waypoints.loc SRID = %d; want 4326", geog.SRID)
	}
}

// TestMigrate_PG_PostGIS_SPGistIndexPassthrough exercises the
// SP-GiST opclass passthrough (Bug 50). Pre-fix the IR's IndexKind
// enum had no SP-GiST value, so the schema reader stored
// IndexKindUnspecified and the writer fell back to btree — failing
// the CREATE INDEX on the target because spgist_geometry_ops_2d
// isn't a btree opclass. With the enum entries added, the round-trip
// preserves `USING spgist (geom spgist_geometry_ops_2d)`.
func TestMigrate_PG_PostGIS_SPGistIndexPassthrough(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresPGToPGWithPostGIS(t, true)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE shapes (
			id   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			geom geometry(POINT, 4326) NOT NULL
		);

		INSERT INTO shapes (geom) VALUES (ST_SetSRID(ST_MakePoint(0, 0), 4326));

		CREATE INDEX shapes_geom_spgist
		    ON shapes USING SPGIST (geom spgist_geometry_ops_2d);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	filter, fErr := NewTableFilter(nil, []string{"spatial_ref_sys"})
	if fErr != nil {
		t.Fatalf("NewTableFilter: %v", fErr)
	}
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"postgis"},
		Filter:              filter,
		SkipViews:           true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	const idxQuery = `
		SELECT am.amname, opc.opcname
		FROM   pg_index ix
		JOIN   pg_class i  ON i.oid = ix.indexrelid
		JOIN   pg_class cl ON cl.oid = ix.indrelid
		JOIN   pg_am    am ON am.oid = i.relam
		JOIN   LATERAL unnest(ix.indclass) WITH ORDINALITY AS uc(opcoid, ord) ON TRUE
		JOIN   pg_opclass opc ON opc.oid = uc.opcoid
		WHERE  cl.relname = 'shapes'
		  AND  i.relname  = 'shapes_geom_spgist'`
	var (
		amname  string
		opcname string
	)
	if err := tgtDB.QueryRowContext(ctx, idxQuery).Scan(&amname, &opcname); err != nil {
		t.Fatalf("index method/opclass query: %v", err)
	}
	if amname != "spgist" {
		t.Errorf("target index method = %q; want spgist", amname)
	}
	if opcname != "spgist_geometry_ops_2d" {
		t.Errorf("target index opclass = %q; want spgist_geometry_ops_2d", opcname)
	}
}

// TestMigrate_PG_PostGIS_BRINIndexPassthrough — parallel of the
// SP-GiST test for the BRIN access method (Bug 50). PostGIS exposes
// brin_geometry_inclusion_ops_2d / _4d / _nd. Without the IndexKind
// enum entry the writer would emit btree, which BRIN's geometry
// opclasses can't be applied to — CREATE INDEX would fail.
func TestMigrate_PG_PostGIS_BRINIndexPassthrough(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresPGToPGWithPostGIS(t, true)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE blocks (
			id   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			geom geometry(POLYGON, 4326) NOT NULL
		);

		INSERT INTO blocks (geom) VALUES
		    (ST_SetSRID(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), 4326));

		CREATE INDEX blocks_geom_brin
		    ON blocks USING BRIN (geom brin_geometry_inclusion_ops_2d);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	filter, fErr := NewTableFilter(nil, []string{"spatial_ref_sys"})
	if fErr != nil {
		t.Fatalf("NewTableFilter: %v", fErr)
	}
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"postgis"},
		Filter:              filter,
		SkipViews:           true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	const idxQuery = `
		SELECT am.amname, opc.opcname
		FROM   pg_index ix
		JOIN   pg_class i  ON i.oid = ix.indexrelid
		JOIN   pg_class cl ON cl.oid = ix.indrelid
		JOIN   pg_am    am ON am.oid = i.relam
		JOIN   LATERAL unnest(ix.indclass) WITH ORDINALITY AS uc(opcoid, ord) ON TRUE
		JOIN   pg_opclass opc ON opc.oid = uc.opcoid
		WHERE  cl.relname = 'blocks'
		  AND  i.relname  = 'blocks_geom_brin'`
	var (
		amname  string
		opcname string
	)
	if err := tgtDB.QueryRowContext(ctx, idxQuery).Scan(&amname, &opcname); err != nil {
		t.Fatalf("index method/opclass query: %v", err)
	}
	if amname != "brin" {
		t.Errorf("target index method = %q; want brin", amname)
	}
	if opcname != "brin_geometry_inclusion_ops_2d" {
		t.Errorf("target index opclass = %q; want brin_geometry_inclusion_ops_2d", opcname)
	}
}

// TestMigrate_PG_PostGIS_GeographySubtypePreserved pins the Bug 51
// closure: PostGIS's `geography_columns.type` view returns Mixed
// Case ("Point") rather than the ALL-CAPS form `geometry_columns`
// emits. Pre-fix `parseGeometrySubtype` did a literal switch and
// `"Point"` fell through to `GeometryUnspecified`, silently widening
// `geography(Point, 4326)` on the source to `geography(Geometry,
// 4326)` on the target. The fix upper-cases the input before
// dispatching.
func TestMigrate_PG_PostGIS_GeographySubtypePreserved(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresPGToPGWithPostGIS(t, true)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE pins (
			id  BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			loc geography(POINT, 4326) NOT NULL
		);
		INSERT INTO pins (loc) VALUES
			(ST_GeographyFromText('SRID=4326;POINT(0 0)'));
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	filter, _ := NewTableFilter(nil, []string{"spatial_ref_sys"})
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"postgis"},
		Filter:              filter,
		SkipViews:           true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// Pin: target geography_columns reports subtype `Point` (the
	// PostGIS Mixed-Case shape), NOT the wildcard `Geometry`. Pre-fix
	// the column would have widened to `geography(Geometry, 4326)`.
	var gtype string
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT type
		FROM   geography_columns
		WHERE  f_table_name        = 'pins'
		  AND  f_geography_column  = 'loc'`).Scan(&gtype); err != nil {
		t.Fatalf("geography_columns query: %v", err)
	}
	if !strings.EqualFold(gtype, "Point") {
		t.Errorf("target geography subtype = %q; want \"Point\" (case-insensitive)", gtype)
	}
}

// TestMigrate_PG_PostGIS_PointZPassthrough pins the Bug 52 closure:
// PostGIS extends each 2D subtype with Z (3D), M (measure), and ZM
// (4D) variants — `geometry(POINTZ, 4326)`. Pre-fix the IR's
// `GeometrySubtype` enum had no Z/M flag, parseGeometrySubtype
// fell through to `GeometryUnspecified`, the writer emitted
// `geometry(GEOMETRY, 4326)`, and bulk copy failed with SQLSTATE
// 22023 ("Geometry has Z dimension but column does not") because
// the row's WKB carried Z but the typmod-constrained column didn't
// accept it. The fix adds HasZ / HasM bools on ir.Geometry that the
// writer reconstructs into the type modifier.
func TestMigrate_PG_PostGIS_PointZPassthrough(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresPGToPGWithPostGIS(t, true)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE landmarks (
			id   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			geom geometry(POINTZ, 4326) NOT NULL
		);
		INSERT INTO landmarks (geom) VALUES
			(ST_SetSRID(ST_MakePoint(0, 0, 100), 4326)),
			(ST_SetSRID(ST_MakePoint(1, 1, 200), 4326));
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	filter, _ := NewTableFilter(nil, []string{"spatial_ref_sys"})
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"postgis"},
		Filter:              filter,
		SkipViews:           true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// Pin: target geometry_columns reports `POINTZ` (NOT `POINT`).
	// Pre-fix the column landed as `geometry(GEOMETRY, 4326)` and
	// bulk copy failed upstream.
	var gtype string
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT type
		FROM   geometry_columns
		WHERE  f_table_name      = 'landmarks'
		  AND  f_geometry_column = 'geom'`).Scan(&gtype); err != nil {
		t.Fatalf("geometry_columns query: %v", err)
	}
	if !strings.EqualFold(gtype, "POINTZ") {
		t.Errorf("target geometry subtype = %q; want POINTZ", gtype)
	}

	// Pin: rows round-tripped (count + ST_AsText preserves Z coord).
	var n int
	if err := tgtDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM landmarks").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("target landmarks rows = %d; want 2", n)
	}
	var astext string
	if err := tgtDB.QueryRowContext(ctx, "SELECT ST_AsText(geom) FROM landmarks ORDER BY id LIMIT 1").Scan(&astext); err != nil {
		t.Fatalf("ST_AsText: %v", err)
	}
	if !strings.Contains(astext, "100") {
		t.Errorf("target ST_AsText = %q; want a literal with Z=100", astext)
	}
}

// TestMigrate_PG_PostGIS_TargetMissing_RefusedAtPreflight pins the
// loud-failure default on a target that doesn't have postgis
// installed. Mirrors the pgvector / pg_trgm preflight refusals.
func TestMigrate_PG_PostGIS_TargetMissing_RefusedAtPreflight(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresPGToPGWithPostGIS(t, false)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE places (
			id   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			name TEXT NOT NULL,
			loc  geometry(POINT, 4326) NOT NULL
		);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	// Both source + target containers pre-install PostGIS, which
	// populates the spatial_ref_sys system table (~8500 SRIDs) and
	// creates the geometry_columns/geography_columns system views.
	// Cold-start preflight refuses to bulk-copy into a non-empty
	// target table; exclude spatial_ref_sys + skip views to scope the
	// test to operator-owned tables only.
	filter, fErr := NewTableFilter(nil, []string{"spatial_ref_sys"})
	if fErr != nil {
		t.Fatalf("NewTableFilter: %v", fErr)
	}
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"postgis"},
		Filter:              filter,
		SkipViews:           true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	err := mig.Run(ctx)
	if err == nil {
		t.Fatal("Migrator.Run = nil; want preflight refusal")
	}
	if !strings.Contains(err.Error(), "postgis") {
		t.Errorf("err = %v; want mention of \"postgis\"", err)
	}
	if !strings.Contains(err.Error(), "target") {
		t.Errorf("err = %v; want mention of \"target\"", err)
	}
}
