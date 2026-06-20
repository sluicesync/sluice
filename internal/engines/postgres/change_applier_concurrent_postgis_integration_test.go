//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PostGIS geometry pin for the ADR-0105 concurrent key-hash CDC apply path.
//
// The standard concurrent differential (change_applier_concurrent_integration_test.go)
// runs on the stock postgres image, which has no PostGIS — so it cannot
// exercise the geometry family. This pin closes that gap: it applies a
// geometry-family matrix through the W-LANE path and ground-truths each value
// against ST_AsEWKB on the real target. The load-bearing claim it protects is
// the ADR-0105 requirement that the DEDICATED LANE POOL registers the same
// afterConnectRegisterGeometry codec the serial/pipelined pools do — without
// it, geometry hits pgx's TEXT fallback and PostGIS rejects the raw EWKB
// LOUDLY (XX000 "parse error - invalid geometry"), a Bug-74-class
// codec-coverage trap that would pass on the serial path but fail on lanes.
//
// Gated behind `integration && postgis`; the "Integration (PostGIS)" CI job
// runs `-run 'PostGIS_'` over this package.

package postgres

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestConcurrentApply_PostGIS_GeometryFamilyMatrix applies one geometry value
// per subtype × dimension × SRID through the W-lane concurrent apply path and
// asserts each lands byte-identically to the PostGIS ground truth. A missing
// lane-pool geometry codec fails the whole run loudly (the ApplyBatch error),
// so even one passing case proves the codec is registered; the matrix proves
// the family is covered (Bug-74 corollary).
func TestConcurrentApply_PostGIS_GeometryFamilyMatrix(t *testing.T) {
	dsn, cleanup := startPGForPipelinedPostGIS(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	verifyDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = verifyDB.Close() }()

	cases := []struct {
		name string
		wkt  string
		srid int
		mod  string
	}{
		{"point_2d_srid0", "POINT(1 2)", 0, "Point"},
		{"point_2d_srid4326", "POINT(1 2)", 4326, "Point"},
		{"point_z", "POINT Z (1 2 3)", 4326, "PointZ"},
		{"point_zm", "POINT ZM (1 2 3 4)", 4326, "PointZM"},
		{"linestring", "LINESTRING(0 0, 1 1, 2 2)", 4326, "LineString"},
		{"polygon_hole", "POLYGON((0 0,0 5,5 5,5 0,0 0),(1 1,1 2,2 2,2 1,1 1))", 4326, "Polygon"},
		{"multipolygon", "MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))", 4326, "MultiPolygon"},
		{"geomcollection", "GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,1 1))", 4326, "GeometryCollection"},
	}

	// One SRID-constrained (or SRID-0 unconstrained) table per case, mirroring
	// the cold-start path's dimension+SRID-qualified column. Distinct tables
	// hash to different lanes, so this fans across the W lanes; per-case tables
	// are required because the applier recovers a column's SRID from
	// geometry_columns (ADR-0035: SRID is per-COLUMN, not per-row — an
	// unconstrained `geometry` column legitimately reports SRID 0, the
	// existing serial/pipelined matrix uses the same per-case constrained
	// columns for the non-zero SRID cases).
	a := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer func() { _ = a.Close() }()
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	type want struct {
		table string
		ewkb  string
		srid  int
		name  string
	}
	var events []ir.Change
	var wants []want
	for _, tc := range cases {
		// id is 1 in every (distinct) table; the lane routing fans on the
		// QUALIFIED TABLE NAME hash, so distinct tables still distribute across
		// the W lanes even with the same PK value.
		const id = int64(1)
		table := "cg_" + tc.name
		colType := "geometry"
		if tc.srid != 0 {
			colType = fmt.Sprintf("geometry(%s,%d)", tc.mod, tc.srid)
		}
		applyPGApplier(t, dsn, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, geom "+colType+");")

		// Ground truth EWKB (LE, as ST_AsEWKB always emits).
		var wantHex string
		if err := verifyDB.QueryRowContext(ctx,
			`SELECT encode(ST_AsEWKB(ST_GeomFromText($1, $2)), 'hex')`, tc.wkt, tc.srid).Scan(&wantHex); err != nil {
			t.Fatalf("ground-truth EWKB for %q: %v", tc.wkt, err)
		}
		// Raw WKB as the CDC reader hands the applier (ewkbToWKB mirrors the
		// reader's decode), so the fed bytes match production byte-for-byte.
		var srcEWKBHex string
		if err := verifyDB.QueryRowContext(ctx,
			`SELECT encode(ST_AsEWKB(ST_GeomFromText($1, $2), 'NDR'), 'hex')`, tc.wkt, tc.srid).Scan(&srcEWKBHex); err != nil {
			t.Fatalf("source EWKB for %q: %v", tc.wkt, err)
		}
		srcEWKB, derr := hex.DecodeString(srcEWKBHex)
		if derr != nil {
			t.Fatalf("decode source EWKB hex: %v", derr)
		}
		rawWKB, werr := ewkbToWKB(srcEWKB)
		if werr != nil {
			t.Fatalf("ewkbToWKB for %q: %v", tc.wkt, werr)
		}
		events = append(events, ir.Insert{
			Position: cpos("geom-" + tc.name), Schema: "public", Table: table,
			Row: ir.Row{"id": id, "geom": rawWKB},
		})
		wants = append(wants, want{table: table, ewkb: wantHex, srid: tc.srid, name: tc.name})
	}

	ch := make(chan ir.Change, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	// If the lane pool lacked the geometry codec, this errors loudly here
	// (XX000 "parse error - invalid geometry" on the TEXT fallback).
	if err := a.ApplyBatch(ctx, testStreamID, ch, 2); err != nil {
		t.Fatalf("concurrent ApplyBatch of geometry (lane pool codec?): %v", err)
	}

	for _, w := range wants {
		var gotHex string
		var gotSRID int
		if err := verifyDB.QueryRowContext(ctx,
			"SELECT encode(ST_AsEWKB(geom), 'hex'), ST_SRID(geom) FROM "+w.table+" WHERE id = 1").Scan(&gotHex, &gotSRID); err != nil {
			t.Fatalf("read back geometry %s: %v", w.name, err)
		}
		if gotHex != w.ewkb {
			t.Errorf("geometry %s round-trip diverged on lane path:\n got  %s\n want %s", w.name, gotHex, w.ewkb)
		}
		if gotSRID != w.srid {
			t.Errorf("geometry %s SRID = %d; want %d (applier must recover the column SRID — #20 — on the lane path)", w.name, gotSRID, w.srid)
		}
	}
}
