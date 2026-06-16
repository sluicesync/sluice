//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 147 CDC geometry pin. Before this, the pgoutput path's oidToType had
// no case for PostGIS `geometry` (its OID is dynamic, assigned at CREATE
// EXTENSION time), so a PG source table with a geometry column wedged the
// sync stream on the first geometry DML ("unsupported column type OID <dyn>
// (geometry)") — geometry cold-starts (COPY) fine but could not be
// continuous-synced PG→PG. This pin resolves the runtime geometry OID
// (ensureExtensionTypeOIDs / buildRelationCacheEntry) and drives real
// pgoutput INSERT/UPDATE/DELETE through the live replication stream,
// asserting the decoded ir.Row value reconstructs to the source geometry.
//
// It ALSO guards the Bug-144-class value-decode trap: pgoutput delivers the
// value as TEXT-format []byte (hex-EWKB ASCII), which decodePGGeometry must
// hex-decode — NOT treat as raw EWKB. A regression there corrupts geometry
// silently, so the round-trip ST_AsText assertion is load-bearing.
//
// Coverage (the decode is family-uniform — pgoutput ships hex-EWKB text for
// every shape — but each is a distinct EWKB byte-layout ewkbToWKB walks):
// subtype point/polygon/multipolygon/geometrycollection, dimension 2D/Z/M/ZM,
// SRID 4326 AND 0 (the latter exercises ewkbToWKB's already-raw passthrough
// branch vs the SRID-strip branch), POINT EMPTY, a NULL value, the UPDATE
// after-image, and a DELETE before-image on a NO-PK REPLICA IDENTITY FULL
// table (where the before-image carries the full row incl. geometry, unlike
// a PK table whose before-image narrows to the key).
//
// geography is intentionally NOT covered: the reader loud-refuses it (its OID
// is unresolved) and the #20 applier has no geography codec — geography
// end-to-end is a tracked follow-up.
//
// Tagged `integration && postgis` and named `*PostGIS_*` so the required
// "Integration (PostGIS)" CI job (-run 'PostGIS_') executes it.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestCDCReader_PostGIS_GeometryFamily(t *testing.T) {
	dsn, cleanup := startPostgresForCDCImage(t, pipelinedPostGISImage)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE EXTENSION IF NOT EXISTS postgis;`)
	applyPGSQL(t, dsn, `
		CREATE TABLE geo (
			id     BIGINT PRIMARY KEY,
			g_pt   geometry(Point,4326),
			g_pt0  geometry(Point,0),
			g_poly geometry(Polygon,4326),
			g_ptz  geometry(PointZ,4326),
			g_ptm  geometry(PointM,4326),
			g_ptzm geometry(PointZM,4326),
			g_mp   geometry(MultiPolygon,4326),
			g_gc   geometry(GeometryCollection,4326),
			g_emp  geometry(Point,4326),
			g_null geometry(Point,4326)
		);
		ALTER TABLE geo REPLICA IDENTITY FULL;
		CREATE TABLE geo_nopk (
			tag TEXT,
			g   geometry(Point,4326)
		);
		ALTER TABLE geo_nopk REPLICA IDENTITY FULL;
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// geo: INSERT (the family matrix) + UPDATE (after-image) + DELETE
	// (PK before-image narrows to {id}). geo_nopk: INSERT + DELETE (FULL,
	// no PK → before-image carries the full row incl. geometry). 5 row
	// events total; their arrival proves geometry doesn't wedge (Bug 147).
	applyPGSQL(t, dsn, `
		INSERT INTO geo VALUES (
			1,
			ST_GeomFromText('POINT(1 2)',4326),
			ST_GeomFromText('POINT(3 4)',0),
			ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4326),
			ST_GeomFromText('POINT Z (1 2 3)',4326),
			ST_GeomFromText('POINT M (1 2 3)',4326),
			ST_GeomFromText('POINT ZM (1 2 3 4)',4326),
			ST_GeomFromText('MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))',4326),
			ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,1 1))',4326),
			ST_GeomFromText('POINT EMPTY',4326),
			NULL);
		UPDATE geo SET g_pt = ST_GeomFromText('POINT(9 9)',4326) WHERE id = 1;
		DELETE FROM geo WHERE id = 1;
		INSERT INTO geo_nopk VALUES ('a', ST_GeomFromText('POINT(7 8)',4326));
		DELETE FROM geo_nopk WHERE tag = 'a';
	`)

	got := drainChanges(t, ctx, changes, 5, 60*time.Second)
	if len(got) != 5 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 5 (geometry must not wedge the stream — Bug 147; stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 5 (geometry must not wedge the stream — Bug 147)", len(got))
	}

	verify, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = verify.Close() }()

	// asText reconstructs the decoded WKB on the server and returns its WKT.
	// The reader strips SRID to WKB (ADR-0035); ST_GeomFromEWKB accepts the
	// EWKB-flag dimension encoding decodePGGeometry preserves, and ST_AsText
	// is SRID-independent, so this is a faithful round-trip check.
	asText := func(label string, v any) string {
		t.Helper()
		b, ok := v.([]byte)
		if !ok || len(b) == 0 {
			t.Fatalf("%s: decoded geometry = %T(%v); want non-empty []byte WKB", label, v, v)
		}
		var wkt string
		if err := verify.QueryRowContext(ctx, `SELECT ST_AsText(ST_GeomFromEWKB($1))`, b).Scan(&wkt); err != nil {
			t.Fatalf("%s: reconstruct WKB: %v", label, err)
		}
		return wkt
	}

	var geoIns ir.Insert
	var geoUpd ir.Update
	var geoDel ir.Delete
	var nopkIns ir.Insert
	var nopkDel ir.Delete
	for _, c := range got {
		switch v := c.(type) {
		case ir.Insert:
			if v.Table == "geo_nopk" {
				nopkIns = v
			} else {
				geoIns = v
			}
		case ir.Update:
			geoUpd = v
		case ir.Delete:
			if v.Table == "geo_nopk" {
				nopkDel = v
			} else {
				geoDel = v
			}
		default:
			t.Fatalf("unexpected change type %T", c)
		}
	}

	// --- INSERT family matrix (every subtype × dimension × SRID shape) ---
	if geoIns.Row == nil {
		t.Fatal("geo INSERT missing")
	}
	for _, tc := range []struct {
		col, want string
	}{
		{"g_pt", "POINT(1 2)"},
		{"g_pt0", "POINT(3 4)"}, // SRID 0 → ewkbToWKB already-raw passthrough branch
		{"g_poly", "POLYGON((0 0,0 1,1 1,1 0,0 0))"},
		{"g_ptz", "POINT Z (1 2 3)"},
		{"g_ptm", "POINT M (1 2 3)"},
		{"g_ptzm", "POINT ZM (1 2 3 4)"},
		{"g_mp", "MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))"},
		{"g_gc", "GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,1 1))"},
		{"g_emp", "POINT EMPTY"},
	} {
		if w := asText("ins."+tc.col, geoIns.Row[tc.col]); w != tc.want {
			t.Errorf("ins.%s = %q; want %q", tc.col, w, tc.want)
		}
	}
	// NULL geometry: decodeTuple maps 'n' → nil before decodeValue, so the
	// column lands as a present nil (not absent, not garbage).
	if v, present := geoIns.Row["g_null"]; !present || v != nil {
		t.Errorf("ins.g_null = (present=%v, %v); want present nil", present, v)
	}

	// --- UPDATE after-image geometry decodes to the new value ---
	if geoUpd.After == nil {
		t.Fatal("geo UPDATE after-image missing")
	}
	if w := asText("upd.g_pt", geoUpd.After["g_pt"]); w != "POINT(9 9)" {
		t.Errorf("upd.after.g_pt = %q; want POINT(9 9)", w)
	}

	// --- DELETE on a PK table: before-image narrows to the identity key
	// {id} (REPLICA IDENTITY FULL + PK), so geometry is absent — pin the
	// narrowing reality rather than a geometry that cannot be there. ---
	if geoDel.Before == nil {
		t.Fatal("geo DELETE before-image missing")
	}
	if id, _ := geoDel.Before["id"].(int64); id != 1 {
		t.Errorf("del.before.id = %v; want 1", geoDel.Before["id"])
	}
	if _, present := geoDel.Before["g_pt"]; present {
		t.Errorf("del.before.g_pt present; want narrowed away (FULL+PK before-image is key-only)")
	}

	// --- DELETE on a NO-PK FULL table: the before-image carries the full
	// row, so a geometry IN the before-image must decode (the real
	// before-image-geometry pin). ---
	if nopkIns.Row == nil || nopkDel.Before == nil {
		t.Fatalf("geo_nopk INSERT/DELETE missing (ins=%v del=%v)", nopkIns.Row != nil, nopkDel.Before != nil)
	}
	if w := asText("nopk.ins.g", nopkIns.Row["g"]); w != "POINT(7 8)" {
		t.Errorf("nopk.ins.g = %q; want POINT(7 8)", w)
	}
	if w := asText("nopk.del.before.g", nopkDel.Before["g"]); w != "POINT(7 8)" {
		t.Errorf("nopk.del.before.g = %q; want POINT(7 8) (geometry in a FULL before-image must decode)", w)
	}
}
