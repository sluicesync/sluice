//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 239 end-to-end: a Vitess/PlanetScale (VStream) source with a
// geometry column could not `sync` at all.
//
// # The reported shape
//
// `sluice sync start` over a real vtgate died on cold start with
// PostgreSQL's own `parse error - invalid geometry (SQLSTATE XX000)` under
// SLUICE-E-BULKCOPY-TABLE-FAILED and 0 rows, while `sluice migrate` over
// the IDENTICAL source and table landed the row. Two candidate axes were
// visible in the report and it did not separate them: VStream-vs-binlog
// (the decoder), and sync's idempotent cold-start writer vs migrate's COPY
// path (the writer).
//
// # Which axis it was
//
// The WRITER. The VStream decoder was innocent: it hands downstream the
// same OGC WKB the SQL path does. What differed is that the PG RowWriter's
// pool had no PostGIS geometry codec, so its two PARAMETER-binding cores
// TEXT-encoded EWKB and `geometry_in` refused it — and a VStream source is
// the one source that ALWAYS demands the idempotent core
// ([ir.IdempotentCopyReader]). The fix registers the codec on that pool;
// the writer-level family matrix lives in the postgres engine package
// (TestRowWriter_PostGIS_GeometryFamiliesAcrossWriteCores).
//
// This file is the leg neither of those can make: the real product path —
// real vtgate, real VStream COPY phase, real PostGIS target, `sync`'s own
// cold start — across every WKB geometry family, ground-truthed on the
// TARGET with the target's own functions rather than a row count.
//
// # Coverage this pin does NOT have
//
// The vstream-tagged PIPELINE suite runs weekly in extended-suites.yml's
// `vstream-pipeline` leg, not on every PR. The per-PR guard for this bug
// is the postgis-tagged writer matrix in the required "Integration
// (PostGIS)" job; this test is the end-to-end confirmation, not the gate.
//
// Name shape: the `vstream-pipeline` leg's -run filter
// (^(TestMigrate_VStream|TestStreamer_.*VStream|TestSpikeShapeA_)) is
// enforced by scripts/check-run-filter-coverage.sh.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// geometryVStreamCase is one WKB family. The column type is the MySQL
// spatial type; wkt is both the seed and the independent expected value.
type geometryVStreamCase struct {
	column   string
	mysqlDDL string
	wkt      string
	wantType string // PostGIS ST_GeometryType on the target
}

// geometryVStreamFamilies is all seven OGC WKB geometry families. The
// decoder dispatches on query.Type_GEOMETRY for every one of them and the
// target's `geometry_recv` parses each body differently, so one family
// proves nothing about the rest (the Bug 74 lesson).
//
// Every value is at SRID 0 deliberately: a non-zero per-row SRID is
// REFUSED on this path by design (audit 2026-08-05 C-14 — VStream's
// FieldEvent carries no per-column SRID, so the target column is created
// without one and stripping would lose it twice over). SRID 0 is the case
// that has nothing left to refuse, and is exactly the case Bug 239 could
// not copy.
func geometryVStreamFamilies() []geometryVStreamCase {
	return []geometryVStreamCase{
		{"g_point", "POINT", "POINT(1 2)", "ST_Point"},
		{"g_line", "LINESTRING", "LINESTRING(0 0,1 1,2 2)", "ST_LineString"},
		{"g_poly", "POLYGON", "POLYGON((0 0,4 0,4 4,0 4,0 0))", "ST_Polygon"},
		{"g_mpoint", "MULTIPOINT", "MULTIPOINT(0 0,1 1)", "ST_MultiPoint"},
		{"g_mline", "MULTILINESTRING", "MULTILINESTRING((0 0,1 1),(2 2,3 3))", "ST_MultiLineString"},
		{"g_mpoly", "MULTIPOLYGON", "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))", "ST_MultiPolygon"},
		{"g_coll", "GEOMETRYCOLLECTION", "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))", "ST_GeometryCollection"},
	}
}

// startPostGISTargetForVStream boots the pre-baked PostGIS image and
// enables the extension, mirroring startPGTarget but with PostGIS — the
// vstream-tagged bucket cannot see the postgis-tagged helpers.
func startPostGISTargetForVStream(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c, err := pgtc.Run(
		ctx, "ghcr.io/sluicesync/sluice-postgis:16-3.4-prebaked",
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
	)
	if err != nil {
		t.Fatalf("start postgis target: %v", err)
	}
	term := func() {
		sd, cc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cc()
		_ = c.Terminate(sd)
	}
	conn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		term()
		t.Fatalf("postgis conn string: %v", err)
	}
	db, err := sql.Open("pgx", conn)
	if err != nil {
		term()
		t.Fatalf("open postgis target: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
		term()
		t.Fatalf("CREATE EXTENSION postgis: %v", err)
	}
	return conn, term
}

// TestStreamer_Bug239Geometry_VStream_PostGIS_ColdStartAndCDC drives a real
// `sync` — cold-start COPY through VStream's copy phase, then a live CDC
// insert — for every geometry family, and asserts the landed value on the
// TARGET with PostGIS's own functions.
func TestStreamer_Bug239Geometry_VStream_PostGIS_ColdStartAndCDC(t *testing.T) {
	families := geometryVStreamFamilies()
	if len(families) != 7 {
		t.Fatalf("family list has %d entries; want all 7 WKB families (anti-vacuity floor)", len(families))
	}

	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, "commerce", 1)
	defer cleanupSrc()
	targetDSN, cleanupTgt := startPostGISTargetForVStream(t)
	defer cleanupTgt()

	// One table, one column per family — a single cold start covers the
	// whole matrix and one vttestserver boot pays for all seven.
	ddl := "CREATE TABLE shapes (\n\tid BIGINT NOT NULL AUTO_INCREMENT"
	for _, f := range families {
		ddl += fmt.Sprintf(",\n\t%s %s NULL", f.column, f.mysqlDDL)
	}
	ddl += ",\n\tPRIMARY KEY (id)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	applySQL(t, mysqlDSN, ddl)
	applySQL(t, mysqlDSN, geometryInsertSQL(families, 1))
	// Row 2 leaves every geometry NULL — the NULL shape of the matrix.
	applySQL(t, mysqlDSN, "INSERT INTO shapes (id) VALUES (2)")
	// Let vttestserver's async schema tracker see the table before COPY
	// enumerates it.
	time.Sleep(3 * time.Second)

	srcEng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("planetscale engine not registered")
	}
	tgtEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	streamer := &Streamer{
		Source: srcEng,
		Target: tgtEng,
		SourceDSN: fmt.Sprintf(
			"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
			mysqlDSN, grpcEndpoint,
		),
		TargetDSN: targetDSN,
		StreamID:  "bug239-geometry",
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// --- cold start (VStream COPY phase → the idempotent writer) ---
	if !waitForShapesRow(t, tgtDB, 1, 3*time.Minute, runErr) {
		select {
		case e := <-runErr:
			t.Fatalf("cold-start row never landed; Run returned: %v\n"+
				"  (Bug 239: a 'parse error - invalid geometry' here is the regression)", e)
		default:
		}
		t.Fatal("cold-start row never landed on the target within 3m")
	}
	assertShapeFamilies(shortQueryCtx(t), t, tgtDB, families, 1, "cold start")
	assertShapesAllNull(shortQueryCtx(t), t, tgtDB, families, 2)

	// --- CDC (VStream streaming phase → the change applier) ---
	applySQL(t, mysqlDSN, geometryInsertSQL(families, 3))
	if !waitForShapesRow(t, tgtDB, 3, 2*time.Minute, runErr) {
		select {
		case e := <-runErr:
			t.Fatalf("CDC row never landed; Run returned: %v", e)
		default:
		}
		t.Fatal("CDC row never landed on the target within 2m")
	}
	assertShapeFamilies(shortQueryCtx(t), t, tgtDB, families, 3, "CDC")

	cancel()
	select {
	case <-runErr:
	case <-time.After(30 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// geometryInsertSQL builds one INSERT carrying every family's WKT at the
// given id, through MySQL's own ST_GeomFromText (SRID 0).
func geometryInsertSQL(families []geometryVStreamCase, id int) string {
	cols, vals := "id", fmt.Sprintf("%d", id)
	for _, f := range families {
		cols += ", " + f.column
		vals += fmt.Sprintf(", ST_GeomFromText('%s')", f.wkt)
	}
	return fmt.Sprintf("INSERT INTO shapes (%s) VALUES (%s)", cols, vals)
}

// shortQueryCtx is a short-lived query context for the read-back assertions.
func shortQueryCtx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

// waitForShapesRow polls until the target has the row, failing early if
// the Streamer has already returned.
func waitForShapesRow(t *testing.T, db *sql.DB, id int, timeout time.Duration, runErr <-chan error) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int
		if err := db.QueryRow("SELECT count(*) FROM shapes WHERE id = $1", id).Scan(&n); err == nil && n == 1 {
			return true
		}
		select {
		case e := <-runErr:
			t.Fatalf("Streamer.Run returned before row %d landed: %v", id, e)
		default:
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// assertShapeFamilies ground-truths every family on the TARGET: the
// geometry type PostGIS reports, and equality against the WKT literal from
// this file — neither derived from the bytes sluice wrote.
func assertShapeFamilies(
	c context.Context,
	t *testing.T,
	db *sql.DB,
	families []geometryVStreamCase,
	id int,
	phase string,
) {
	t.Helper()
	for _, f := range families {
		var gotType string
		var equal bool
		q := fmt.Sprintf(
			`SELECT ST_GeometryType(%s), ST_OrderingEquals(%s, ST_GeomFromText($1)) FROM shapes WHERE id = $2`,
			f.column, f.column,
		)
		if err := db.QueryRowContext(c, q, f.wkt, id).Scan(&gotType, &equal); err != nil {
			t.Errorf("%s: %s: read back: %v", phase, f.column, err)
			continue
		}
		if gotType != f.wantType {
			t.Errorf("%s: %s: ST_GeometryType = %q; want %q", phase, f.column, gotType, f.wantType)
		}
		if !equal {
			t.Errorf("%s: %s: landed geometry != %q", phase, f.column, f.wkt)
		}
	}
}

// assertShapesAllNull pins the NULL shape of the matrix through the same
// cold-start path.
func assertShapesAllNull(c context.Context, t *testing.T, db *sql.DB, families []geometryVStreamCase, id int) {
	t.Helper()
	for _, f := range families {
		var isNull bool
		q := fmt.Sprintf(`SELECT %s IS NULL FROM shapes WHERE id = $1`, f.column)
		if err := db.QueryRowContext(c, q, id).Scan(&isNull); err != nil {
			t.Errorf("NULL row: %s: read back: %v", f.column, err)
			continue
		}
		if !isNull {
			t.Errorf("NULL row: %s did not land as NULL", f.column)
		}
	}
}
