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

	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"postgis"},
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
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"postgis"},
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
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		EnabledPGExtensions: []string{"postgis"},
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
