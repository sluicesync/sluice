//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PostGIS geometry pins for the CDC apply path (the geometry codec, #20).
//
// # What these cover
//
// 1. [TestPipelined_PostGIS_GeometryEquivalence] — the ADR-0092
//    "pipelining changes only WHEN statements are sent, never HOW a value
//    is encoded" invariant for the geometry family: a geometry value
//    applied through the pipelined (DescribeExec) batch must produce the
//    IDENTICAL on-target result as the serial (CacheStatement) path.
// 2. [TestPipelined_PostGIS_GeometryFamilyMatrix] — the Bug-74
//    family-coverage requirement for a family-dispatched codec: every
//    geometry SUBTYPE (point/line/polygon/multi*/collection) × DIMENSION
//    (2D/Z/M/ZM) × SRID (0 and set) × NULL round-trips byte-identically
//    (ground-truthed via ST_AsEWKB src==dst on the real PostGIS target).
//
// # Background (#20: the geometry codec)
//
// sluice carries geometry as EWKB bytes (prepareValue). The COPY
// cold-start path ships them in COPY-BINARY, which `geometry_recv`
// accepts. The CDC applier binds them as a query PARAMETER; before #20 no
// codec was registered for PostGIS's dynamic `geometry` OID, so pgx fell
// back to TEXT format and `geometry_in` rejected the raw EWKB bytes
// ("parse error - invalid geometry", XX000) on BOTH apply paths —
// geometry was un-appliable over CDC (loud refusal, never silent loss).
// [pgGeometryBinaryCodec], registered per-conn on both applier pools via
// [afterConnectRegisterGeometry], closes that gap by shipping EWKB in
// BINARY to `geometry_recv` — identical bytes, identical stored value, as
// the COPY path. These pins are what catch a pipelined-only divergence or
// a per-subtype encoding bug.
//
// Out of scope (still LOUD-refused over CDC apply, never silent loss):
//   - `geography` columns — the codec registers only the `geometry` OID, so
//     a geography value's EWKB hits the text fallback and PostGIS rejects it
//     loudly (a dedicated geography codec + matrix is separate work).
//   - `geometry[]` (array-of-geometry) — convertArray / oidToType have no
//     geometry element branch, so it fails loudly at relation/array build.
// Per-row SRID in an UNconstrained `geometry` column is dropped by design
// (ADR-0035; pinned by generic_column_drops_per_row_srid below).
//
// Gated behind `integration && postgis` (the postgis image is heavier than
// stock postgres:16). The required "Integration (PostGIS)" CI job runs
// `-run 'PostGIS_'` over this package too (wired in PR #225), so these
// pins execute in CI.

package postgres

import (
	"context"
	"database/sql"
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"

	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// pipelinedPostGISImage is the pre-baked PostGIS image (Task #68) the CI
// "Integration (PostGIS)" job pre-pulls — byte-equivalent to upstream
// postgis/postgis:16-3.4 except its datadir is pre-initialised, so it boots
// without the initdb cold-start path AND avoids a Docker Hub pull (rate-limit
// flake) on the runner pool. Mirrors the sibling pgvector pin's
// pipelinedExtImage. runPGWithRetry appends the single-occurrence wait the
// pre-baked image needs.
const pipelinedPostGISImage = "ghcr.io/sluicesync/sluice-postgis:16-3.4-prebaked"

// startPGForPipelinedPostGIS boots a postgis container, enables PostGIS,
// and returns a DSN + cleanup. runPGWithRetry handles the Docker-provider
// skip, boot retry, and the wait strategy (the appended single-occurrence
// log+port wait is correct for the pre-baked image, which logs "ready" once).
// Skips cleanly when no Docker provider is available.
func startPGForPipelinedPostGIS(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()

	container := runPGWithRetry(
		t, pipelinedPostGISImage,
		// The pre-baked image's datadir is already initialised, so
		// pgtc.WithDatabase is a no-op (initdb never runs) — the bake seeds
		// "source_db" (same as the sibling pgvector/postgres pre-baked
		// images). Ask for source_db so ConnectionString targets a DB that
		// exists; the test creates its own tables inside it.
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	conn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("pgx", conn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
		terminate()
		t.Fatalf("CREATE EXTENSION postgis: %v", err)
	}
	return conn, terminate
}

// applyGeomThroughBatch applies one geometry Insert through the applier at
// the given batch size (>1 → pipelined DescribeExec batch; 1 → serial
// per-change *sql.Tx exec) on its own fresh table, and returns the
// ApplyBatch error (nil on success) plus the resulting ST_AsText (empty
// when nothing landed). It is the differential probe both arms of the pin
// call so the two paths are compared on identical input.
func applyGeomThroughBatch(t *testing.T, dsn, table string, batchSize int) (applyErr error, asText string) {
	t.Helper()
	applyPGApplier(t, dsn, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, geom geometry);")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	ch := make(chan ir.Change, 1)
	ch <- ir.Insert{Position: ir.Position{Engine: engineNamePostgres, Token: "g_" + table}, Schema: "public", Table: table, Row: ir.Row{"id": int64(1), "geom": wkbPointLE()}}
	close(ch)
	applyErr = applier.ApplyBatch(ctx, testStreamID, ch, batchSize)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()
	var got sql.NullString
	_ = db.QueryRowContext(ctx, "SELECT ST_AsText(geom) FROM "+table+" WHERE id = 1").Scan(&got)
	return applyErr, got.String
}

// TestPipelined_PostGIS_GeometryEquivalence pins that a PostGIS geometry
// value applies SUCCESSFULLY through BOTH the pipelined (batch>1,
// DescribeExec) and serial (batch=1, CacheStatement) paths, and lands the
// IDENTICAL on-target value (the ADR-0092 "only WHEN, never HOW" invariant
// for the geometry family). Before the #20 codec both paths loudly refused
// the value; now both must succeed with byte-identical results.
func TestPipelined_PostGIS_GeometryEquivalence(t *testing.T) {
	dsn, cleanup := startPGForPipelinedPostGIS(t)
	defer cleanup()

	pipeErr, pipeText := applyGeomThroughBatch(t, dsn, "g_pipe", 100)
	serErr, serText := applyGeomThroughBatch(t, dsn, "g_serial", 1)

	// Both paths must now SUCCEED (the #20 geometry codec ships EWKB binary).
	if pipeErr != nil || serErr != nil {
		t.Fatalf("geometry apply must succeed on both paths now (#20 codec): pipelined err=%v, serial err=%v", pipeErr, serErr)
	}
	// Same resulting on-target value on both paths.
	if pipeText != serText {
		t.Errorf("pipelined geometry result %q != serial result %q (encoding diverged under DescribeExec)", pipeText, serText)
	}
	// And it must be the correct geometry, not an empty/garbage value.
	// wkbPointLE() is WKB POINT(2 3); prepareValue wraps it SRID 0.
	if want := "POINT(2 3)"; pipeText != want {
		t.Errorf("geometry ST_AsText = %q; want %q", pipeText, want)
	}
}

// TestPipelined_PostGIS_GeometryFamilyMatrix is the Bug-74 family pin for
// the geometry codec: every subtype × dimension × SRID variant (+ byte
// order + NULL) applied through the pipelined batch must round-trip
// byte-identically, ground-truthed via ST_AsEWKB / ST_SRID src==dst on the
// real PostGIS target.
//
// CRITICAL: the value fed to the applier is RAW WKB — the SRID-stripped
// shape the CDC readers actually produce (PG→PG decodePGGeometry→ewkbToWKB;
// MySQL→PG decodeVStreamCell drops the SRID prefix). Per ADR-0035 the IR
// treats SRID as a per-COLUMN property; the applier recovers it from
// geometry_columns (#20 Fix 1). A test that fed pre-framed EWKB would
// silently bypass that recovery (wkbToEWKB passthrough) and MASK an SRID
// loss — so we mirror the reader exactly via ewkbToWKB and declare the
// target column with the real SRID (geometry(Geometry,<srid>)), which also
// means a regression (SRID defaulted to 0) surfaces LOUDLY as a PostGIS
// SRID-mismatch insert failure.
func TestPipelined_PostGIS_GeometryFamilyMatrix(t *testing.T) {
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
		// mod is the PostGIS subtype+dimension type modifier for the target
		// column (geometry(<mod>,<srid>)). It MUST match the value's subtype
		// and dimension: a `geometry(Geometry,...)` (2D) column rejects a Z/M
		// value with SQLSTATE 22023, exactly as a real sluice-translated
		// target column would if the schema writer dropped the dimension —
		// so the modifier here mirrors the dimension-qualified column the
		// cold-start path emits for the source type.
		mod string
		be  bool // feed big-endian raw WKB (exercises wkbToEWKB's BE branch)
	}{
		{"point_2d_srid0", "POINT(1 2)", 0, "Point", false},
		{"point_2d_srid4326", "POINT(1 2)", 4326, "Point", false},
		{"point_z", "POINT Z (1 2 3)", 4326, "PointZ", false},
		{"point_m", "POINT M (1 2 3)", 4326, "PointM", false},
		{"point_zm", "POINT ZM (1 2 3 4)", 4326, "PointZM", false},
		{"linestring", "LINESTRING(0 0, 1 1, 2 2)", 4326, "LineString", false},
		{"linestring_z", "LINESTRING Z (0 0 0, 1 1 1)", 4326, "LineStringZ", false},
		{"polygon", "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))", 4326, "Polygon", false},
		{"polygon_hole", "POLYGON((0 0,0 5,5 5,5 0,0 0),(1 1,1 2,2 2,2 1,1 1))", 4326, "Polygon", false},
		{"multipoint", "MULTIPOINT((0 0),(1 1))", 4326, "MultiPoint", false},
		{"multilinestring", "MULTILINESTRING((0 0,1 1),(2 2,3 3))", 4326, "MultiLineString", false},
		{"multipolygon", "MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))", 4326, "MultiPolygon", false},
		{"geomcollection", "GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,1 1))", 4326, "GeometryCollection", false},
		{"point_3857", "POINT(500000 6000000)", 3857, "Point", false},
		{"point_be", "POINT(1 2)", 4326, "Point", true},
		{"polygon_be", "POLYGON((0 0,0 1,1 1,1 0,0 0))", 4326, "Polygon", true},
		{"point_z_be", "POINT Z (1 2 3)", 4326, "PointZ", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Ground truth: the canonical (LE) EWKB PostGIS stores for this
			// geometry. ST_AsEWKB always emits LE, so it is the expected
			// on-target value regardless of the byte order we FEED.
			var wantHex string
			if err := verifyDB.QueryRowContext(
				ctx,
				`SELECT encode(ST_AsEWKB(ST_GeomFromText($1, $2)), 'hex')`, tc.wkt, tc.srid,
			).Scan(&wantHex); err != nil {
				t.Fatalf("ground-truth EWKB for %q: %v", tc.wkt, err)
			}

			// Raw WKB EXACTLY as the CDC reader hands the applier: take the
			// source EWKB (in the requested byte order) and run it through
			// sluice's own ewkbToWKB — the literal transform decodePGGeometry
			// applies. Using ewkbToWKB (not PostGIS ST_AsBinary) guarantees
			// the fed bytes match production byte-for-byte, including the
			// EWKB-flag dimension encoding for Z/M/ZM that ST_AsBinary's OGC
			// output would not reproduce.
			ewkbEndian := "NDR"
			if tc.be {
				ewkbEndian = "XDR"
			}
			var srcEWKBHex string
			if err := verifyDB.QueryRowContext(
				ctx,
				`SELECT encode(ST_AsEWKB(ST_GeomFromText($1, $2), $3), 'hex')`, tc.wkt, tc.srid, ewkbEndian,
			).Scan(&srcEWKBHex); err != nil {
				t.Fatalf("source EWKB for %q: %v", tc.wkt, err)
			}
			srcEWKB, err := hex.DecodeString(srcEWKBHex)
			if err != nil {
				t.Fatalf("decode source EWKB hex: %v", err)
			}
			rawWKB, err := ewkbToWKB(srcEWKB)
			if err != nil {
				t.Fatalf("ewkbToWKB (mirror the reader) for %q: %v", tc.wkt, err)
			}

			// SRID-0 cases use an unconstrained column; non-zero cases use a
			// SRID-constrained column so a recovery regression fails LOUDLY.
			colType := "geometry"
			if tc.srid != 0 {
				colType = "geometry(" + tc.mod + "," + itoa(tc.srid) + ")"
			}

			gotHex, gotSRID := applyRawWKBPipelined(t, ctx, dsn, "gm_"+tc.name, colType, rawWKB)
			if gotSRID != tc.srid {
				t.Errorf("geometry %s SRID = %d; want %d (applier must recover the column SRID — #20 Fix 1)", tc.name, gotSRID, tc.srid)
			}
			if gotHex != wantHex {
				t.Errorf("geometry %s round-trip diverged:\n got EWKB %s\nwant EWKB %s", tc.name, gotHex, wantHex)
			}
		})
	}

	// ADR-0035 documented limitation (pinned, not a bug): a value whose
	// SOURCE SRID was non-zero, applied into an UN-constrained `geometry`
	// column, lands with SRID 0 — the IR carries SRID per column, and an
	// unconstrained column reports SRID 0 in geometry_columns. This is a
	// known, documented loss (not a silent surprise); pin it so a future
	// change that "fixes" it is a conscious decision, not an accident.
	t.Run("generic_column_drops_per_row_srid", func(t *testing.T) {
		var rawHex string
		if err := verifyDB.QueryRowContext(
			ctx,
			`SELECT encode(ST_AsBinary(ST_GeomFromText('POINT(1 2)', 4326)), 'hex')`,
		).Scan(&rawHex); err != nil {
			t.Fatalf("raw WKB: %v", err)
		}
		rawWKB, _ := hex.DecodeString(rawHex)
		_, gotSRID := applyRawWKBPipelined(t, ctx, dsn, "gm_generic", "geometry", rawWKB)
		if gotSRID != 0 {
			t.Errorf("unconstrained geometry column SRID = %d; want 0 (ADR-0035 per-column-SRID limitation)", gotSRID)
		}
	})

	// NULL geometry: must land as SQL NULL, never empty/garbage.
	t.Run("null", func(t *testing.T) {
		applyPGApplier(t, dsn, "CREATE TABLE gm_null (id BIGINT PRIMARY KEY, geom geometry);")
		applier := openPipelinedApplier(t, ctx, dsn)
		defer func() { _ = applier.Close() }()
		if err := applier.EnsureControlTable(ctx); err != nil {
			t.Fatalf("EnsureControlTable: %v", err)
		}
		ch := make(chan ir.Change, 1)
		ch <- ir.Insert{Position: ir.Position{Engine: engineNamePostgres, Token: "gm_null"}, Schema: "public", Table: "gm_null", Row: ir.Row{"id": int64(1), "geom": nil}}
		close(ch)
		if err := applier.ApplyBatch(ctx, testStreamID, ch, 100); err != nil {
			t.Fatalf("apply NULL geometry: %v", err)
		}
		var isNull bool
		if err := verifyDB.QueryRowContext(ctx, "SELECT geom IS NULL FROM gm_null WHERE id = 1").Scan(&isNull); err != nil {
			t.Fatalf("verify NULL geometry: %v", err)
		}
		if !isNull {
			t.Error("NULL geometry did not land as SQL NULL")
		}
	})
}

// itoa is a tiny strconv.Itoa alias to keep the matrix table terse.
func itoa(n int) string { return strconv.Itoa(n) }

// applyRawWKBPipelined applies one geometry Insert carrying RAW WKB (the
// production reader shape) through the pipelined batch into a column of the
// given type, and returns the target's ST_AsEWKB hex + ST_SRID.
func applyRawWKBPipelined(t *testing.T, ctx context.Context, dsn, table, colType string, rawWKB []byte) (gotHex string, gotSRID int) {
	t.Helper()
	applyPGApplier(t, dsn, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, geom "+colType+");")

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	ch := make(chan ir.Change, 1)
	ch <- ir.Insert{Position: ir.Position{Engine: engineNamePostgres, Token: "g_" + table}, Schema: "public", Table: table, Row: ir.Row{"id": int64(1), "geom": rawWKB}}
	close(ch)
	if err := applier.ApplyBatch(ctx, testStreamID, ch, 100); err != nil {
		t.Fatalf("apply geometry into %s: %v", table, err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.QueryRowContext(
		ctx,
		"SELECT encode(ST_AsEWKB(geom), 'hex'), ST_SRID(geom) FROM "+table+" WHERE id = 1",
	).Scan(&gotHex, &gotSRID); err != nil {
		t.Fatalf("read back EWKB/SRID from %s: %v", table, err)
	}
	return gotHex, gotSRID
}
