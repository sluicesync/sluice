//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The GC-44 anti-phantom family matrix's spatial family, on a PostGIS
// target or source (schema_forward_witness_family_integration_test.go has
// the rest; it needs the postgis job's image). The first-boundary witness
// compares geometry at geometry-vs-geography only — the change streams
// carry nothing finer (pgoutput sends a bare type OID) — so every subtype,
// SRID and dimension variant a stream creates must resume onto a MATCH.
//
// `geography` is absent from the Postgres-source tables: the pgoutput
// reader refuses the type outright at the table's first relation message
// ("unsupported column type OID", loud, pre-existing — filed as GC-44
// F10), so no stream can reach the witness with one.

package pipeline

import (
	"testing"
)

const twfbPostGISGeometryPGDDL = `
	CREATE EXTENSION IF NOT EXISTS postgis;
	CREATE TABLE fam_geo (
		id        integer PRIMARY KEY,
		g_bare    geometry,
		g_point   geometry(Point, 4326),
		g_line    geometry(LineString, 4326),
		g_poly0   geometry(Polygon, 0),
		g_multi   geometry(MultiPolygon, 3857)
	);
	INSERT INTO fam_geo (id) VALUES (1);
`

const twfbPostGISGeometryPGProbe = `
	INSERT INTO fam_geo VALUES (2,
		ST_GeomFromText('POINT(1 2)'),
		ST_GeomFromText('POINT(1 2)', 4326),
		ST_GeomFromText('LINESTRING(0 0, 1 1)', 4326),
		ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))', 0),
		ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))', 3857));
`

const twfbPostGISGeometryMySQLDDL = `
	CREATE TABLE fam_geo (
		id      INT NOT NULL PRIMARY KEY,
		g_geom  GEOMETRY NULL,
		g_point POINT SRID 4326 NULL,
		g_line  LINESTRING NULL,
		g_poly  POLYGON NULL,
		g_mpt   MULTIPOINT NULL,
		g_coll  GEOMETRYCOLLECTION NULL
	) ENGINE=InnoDB;
	INSERT INTO fam_geo (id) VALUES (1);
`

const twfbPostGISGeometryMySQLProbe = `
	INSERT INTO fam_geo VALUES (2,
		ST_GeomFromText('POINT(1 2)'),
		ST_GeomFromText('POINT(1 2)', 4326),
		ST_GeomFromText('LINESTRING(0 0, 1 1)'),
		ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))'),
		ST_GeomFromText('MULTIPOINT((0 0), (1 1))'),
		ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))'));
`

func TestTWFBFamilyMatrix_PostGIS_MySQLToPostgres(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgresWithPostGIS(t)
	defer tgtCleanup()
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"mysql", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-geo-m2p",
	}, []twfbFamilyTable{{"fam_geo", twfbPostGISGeometryMySQLDDL, twfbPostGISGeometryMySQLProbe}})
}

func TestTWFBFamilyMatrix_PostGIS_PostgresToPostgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogicalImage(t, postgisPrebakedImage, 8)
	defer cleanup()
	twfbDB{"postgres", tgtDSN}.exec(t, "CREATE EXTENSION IF NOT EXISTS postgis")
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-geo-p2p",
	}, []twfbFamilyTable{{"fam_geo", twfbPostGISGeometryPGDDL, twfbPostGISGeometryPGProbe}})
}

func TestTWFBFamilyMatrix_PostGIS_PostgresToMySQL(t *testing.T) {
	srcDSN, _, srcCleanup := startPostgresLogicalImage(t, postgisPrebakedImage, 8)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"mysql", tgtDSN}, streamID: "twfb-geo-p2m",
	}, []twfbFamilyTable{{"fam_geo", twfbPostGISGeometryPGDDL, twfbPostGISGeometryPGProbe}})
}

// The --schema-changes=refuse arm (GC-44 F5): geometry through the
// unforwarded-stream check, which takes every first boundary too.

func TestTWFBFamilyMatrix_PostGIS_RefuseMySQLToPostgres(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgresWithPostGIS(t)
	defer tgtCleanup()
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"mysql", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "refuse-geo-m2p", schemaChanges: "refuse",
	}, []twfbFamilyTable{{"fam_geo", twfbPostGISGeometryMySQLDDL, twfbPostGISGeometryMySQLProbe}})
}

func TestTWFBFamilyMatrix_PostGIS_RefusePostgresToPostgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogicalImage(t, postgisPrebakedImage, 8)
	defer cleanup()
	twfbDB{"postgres", tgtDSN}.exec(t, "CREATE EXTENSION IF NOT EXISTS postgis")
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "refuse-geo-p2p", schemaChanges: "refuse",
	}, []twfbFamilyTable{{"fam_geo", twfbPostGISGeometryPGDDL, twfbPostGISGeometryPGProbe}})
}

func TestTWFBFamilyMatrix_PostGIS_RefusePostgresToMySQL(t *testing.T) {
	srcDSN, _, srcCleanup := startPostgresLogicalImage(t, postgisPrebakedImage, 8)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	runTWFBFamilyMatrix(t, twfbCell{
		src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"mysql", tgtDSN}, streamID: "refuse-geo-p2m", schemaChanges: "refuse",
	}, []twfbFamilyTable{{"fam_geo", twfbPostGISGeometryPGDDL, twfbPostGISGeometryPGProbe}})
}
