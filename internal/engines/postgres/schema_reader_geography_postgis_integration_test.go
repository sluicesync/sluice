//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 240 — a BARE `geography` column (no typmod) killed EVERY
// schema-reading command for the whole database.
//
// PostGIS's geography_columns view exposes the raw postgis_typmod_*
// accessors with no null-handling (unlike geometry_columns, which
// coalesces internally), so `CREATE TABLE t (g geography)` — typmod -1 —
// reports coord_dimension = NULL. sluice scanned that into a Go int and
// the whole ReadSchema call died with a raw driver scan error: no
// SLUICE-E- code, and whole-database blast radius — migrate / sync /
// schema diff / backup all failed even when `--include-table` scoped the
// run to a table that does not carry the column, because the schema read
// precedes scoping.
//
// The fix COALESCEs the three typmod-derived columns in every query that
// reads the spatial registration views. This pin exercises all three
// production readers of geography_columns against a real bare column:
//
//  1. [SchemaReader.readGeographyColumnInfo] via a full ReadSchema —
//     the Bug 240 repro path, plus the blast-radius claim (the plain
//     sibling table must come back readable);
//  2. [loadGeometryColumnInfo] — the CDC change applier's per-table
//     lookup, same view, same bare-int scan pre-fix;
//  3. [geometryColumnSRIDsFromCatalog] — the CDC SRID guard's lookup
//     (SRID only; measured non-NULL for typmod -1 on PostGIS 3.4, but
//     coalesced for the same reason).
//
// The declared SRID additionally gets [effectiveGeographySRID]: a bare
// geography column's effective SRID is 4326 (PostGIS's geography
// default), not the raw 0 the view exposes — recording 0 made the
// row-level SRID guard ([ir.CheckGeometryRowSRID]) refuse every value
// the column actually holds, since geography stamps 4326 on ingest. The
// premise leg pins those environmental facts against the server itself.
//
// The independent expected value: PostGIS's own documented answer for an
// untyped geography column — subtype GEOMETRY/unspecified, SRID 4326
// (the geography default), 2-D.
//
// Runs in the required "Integration (PostGIS)" CI job (the `PostGIS_`
// name segment is what that job's -run filter selects).

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestSchemaReader_PostGIS_BareGeographyColumn(t *testing.T) {
	dsn, cleanup := startPGForPipelinedPostGIS(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	applyDDL(t, dsn, `
		CREATE TABLE plain_240 (id INT PRIMARY KEY, v TEXT);
		CREATE TABLE bareg_240 (id INT PRIMARY KEY, g geography);
		CREATE TABLE typedg_240 (id INT PRIMARY KEY, g geography(PointZ, 4326));
	`)

	t.Run("schema_reader", func(t *testing.T) {
		r, err := Engine{}.OpenSchemaReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenSchemaReader: %v", err)
		}
		defer closeIf(r)

		schema, err := r.ReadSchema(ctx)
		if err != nil {
			t.Fatalf("ReadSchema: %v\n"+
				"  (Bug 240: geography_columns reports coord_dimension = NULL for a bare\n"+
				"  geography column; a scan error here means the COALESCE guard is gone,\n"+
				"  and it takes every schema-reading command down with it)", err)
		}

		tables := map[string]*ir.Table{}
		for _, tt := range schema.Tables {
			tables[tt.Name] = tt
		}
		// Blast-radius pin: the defect killed the WHOLE read, so the table
		// that does not carry the column is part of the claim.
		if tables["plain_240"] == nil {
			t.Error("plain_240 missing from the read schema — the bare geography column took an unrelated table down with it")
		}

		wantG := map[string]ir.Geometry{
			"bareg_240":  {Subtype: ir.GeometryUnspecified, SRID: 4326, IsGeography: true},
			"typedg_240": {Subtype: ir.GeometryPoint, SRID: 4326, IsGeography: true, HasZ: true},
		}
		for tblName, want := range wantG {
			tbl := tables[tblName]
			if tbl == nil {
				t.Errorf("table %s missing from the read schema", tblName)
				continue
			}
			var got ir.Type
			for _, c := range tbl.Columns {
				if c.Name == "g" {
					got = c.Type
				}
			}
			if got != want {
				t.Errorf("%s.g read as %#v, want %#v", tblName, got, want)
			}
		}
	})

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	t.Run("change_applier_lookup", func(t *testing.T) {
		info, err := loadGeometryColumnInfo(ctx, db, "public", "bareg_240")
		if err != nil {
			t.Fatalf("loadGeometryColumnInfo over a bare geography column: %v (Bug 240 sibling: same view, same bare-int scan)", err)
		}
		g, found := info["g"]
		if !found {
			t.Fatal("bareg_240.g missing from the applier's spatial-column lookup")
		}
		if !g.IsGeography || g.SRID != 4326 || g.HasZ || g.HasM {
			t.Errorf("bareg_240.g applier info = %#v; want IsGeography=true, SRID=4326 (the geography default), no dimension flags", g)
		}
	})

	t.Run("cdc_srid_guard_lookup", func(t *testing.T) {
		srids, err := geometryColumnSRIDsFromCatalog(ctx, db, "public", "bareg_240")
		if err != nil {
			t.Fatalf("geometryColumnSRIDsFromCatalog over a bare geography column: %v", err)
		}
		srid, found := srids["g"]
		if !found {
			t.Fatal("bareg_240.g missing from the SRID-guard lookup")
		}
		if srid != 4326 {
			t.Errorf("bareg_240.g declared SRID = %d; want 4326 — the guard compares this against row values, "+
				"which geography physically stamps with 4326; anything else refuses every row the column holds", srid)
		}
	})

	// The premise leg: [effectiveGeographySRID]'s safety argument cites two
	// facts about PostGIS, not about sluice — assert both against the server.
	t.Run("premise_geography_defaults_to_4326", func(t *testing.T) {
		// 1. Geography stamps 4326 on an SRID-less value at ingest, so the
		//    rows a bare column holds all physically carry 4326.
		var stamped int
		if err := db.QueryRowContext(ctx,
			`SELECT ST_SRID('POINT(1 2)'::geography)`).Scan(&stamped); err != nil {
			t.Fatalf("ST_SRID of an SRID-less geography value: %v", err)
		}
		if stamped != 4326 {
			t.Fatalf("geography stamped an SRID-less value with %d; want 4326 — effectiveGeographySRID's premise is false on this server", stamped)
		}
		// 2. PostGIS normalizes an explicit `geography(X, 0)` typmod — the
		//    DDL sluice's emitter produces for IR SRID 0 — to 4326, so a
		//    target created that way declares 4326 too.
		if _, err := db.ExecContext(ctx,
			`CREATE TABLE geog_typmod0_240 (id INT PRIMARY KEY, g geography(Geometry, 0))`); err != nil {
			t.Fatalf("create geography(Geometry, 0): %v", err)
		}
		var normalized int
		if err := db.QueryRowContext(ctx,
			`SELECT srid FROM geography_columns WHERE f_table_name = 'geog_typmod0_240'`).Scan(&normalized); err != nil {
			t.Fatalf("read back geography(Geometry, 0) typmod SRID: %v", err)
		}
		if normalized != 4326 {
			t.Fatalf("geography(Geometry, 0) column declares SRID %d; want 4326 — the emitter-side premise is false on this server", normalized)
		}
	})
}
