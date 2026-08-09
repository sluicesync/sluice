//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MySQL/MariaDB geometry axis-order PREMISE, pinned against both real
// servers — and the disproof of Bug 238.
//
// # What Bug 238 claimed
//
// That MySQL 8 stores a GEOGRAPHIC spatial reference system's coordinates as
// (latitude, longitude) while MariaDB and PostGIS store (longitude, latitude),
// so sluice's byte-verbatim carriage of geometry preserved the numbers and
// destroyed their meaning — a MariaDB `POINT(12.5 41.9)` (Rome) landing on
// MySQL off the Somali coast. A fix was designed on that premise: swap every
// coordinate pair at the MySQL engine boundary.
//
// # What both servers actually do
//
// Measured on `mysql:8` and `mariadb:11.4`, and asserted below:
//
//	mysql>   INSERT … ST_GeomFromText('POINT(12.5 41.9)', 4326, 'axis-order=long-lat')
//	mysql>   SELECT HEX(g) → E6100000 01010000 00 <12.5> <41.9>
//	mariadb> INSERT … ST_GeomFromText('POINT(12.5 41.9)', 4326)
//	mariadb> SELECT HEX(g) → E6100000 01010000 00 <12.5> <41.9>
//
// **Byte-identical.** MySQL's per-SRS axis order is a property of the
// CONVERSION FUNCTIONS — `ST_GeomFromText`, `ST_AsText`, `ST_AsBinary`,
// `ST_GeomFromWKB`, whose default for a geographic SRS is latitude-first — and
// NOT of the stored form. The stored form, which is what `HEX(col)` returns
// and what sluice's reader and writer carry, is (longitude, latitude) on both
// servers, exactly as PostGIS stores it.
//
// So byte-verbatim carriage is CORRECT, and a swap at the engine boundary
// would introduce precisely the relocation it was designed to prevent. The
// original report compared `ST_AsText` on MariaDB (which renders lon-first)
// against `ST_AsText` on MySQL (which renders lat-first for a 4326 value):
// the RENDERINGS differ, the stored place does not. The `ERROR 3617 … Latitude
// -122.400000 is out of range` in the same report is the same artifact on the
// input side — `ST_GeomFromText('POINT(-122.4 37.8)', 4326)` on MySQL reads
// -122.4 as a latitude — and never fires on the path sluice actually uses,
// which is pinned below with that exact value.
//
// # Why this is a test and not a note
//
// CLAUDE.md's premise-naming rule: a safety argument that cites a fact about
// the world outside the code owes that fact a runtime check or a named test.
// Bug 238 was filed, graded HIGH, designed, and specified as turnkey work on a
// premise nobody had bound to a server. This is that binding. It fails if
// either server ever changes its stored form — which is the only condition
// under which the swap would become necessary.
package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// geometryAxisPlaces is the corpus: real places whose coordinates make an
// axis mix-up unmistakable, over every geometry family.
//
// Every family is present because a swap, had one been needed, would have been
// a WKB walker dispatching on the geometry type code — where one representative
// proves nothing about the others (MultiPoint nests complete Point geometries
// with their own headers). The premise is asserted family by family so a future
// "but maybe only POINT is affected" cannot be answered by assumption.
//
// The WKT is written in the GIS convention (longitude first).
var geometryAxisPlaces = []struct {
	col string
	typ string
	wkt string
}{
	// Rome. |lon| <= 90, so a mix-up here is SILENT — the shape the report
	// measured as a successful migration to the wrong place.
	{"g_point", "POINT", "POINT(12.5 41.9)"},
	// San Francisco. |lon| > 90, so a mix-up is LOUD (MySQL Error 3617,
	// "Latitude -122.400000 is out of range") — the shape the report measured
	// as an occasional error. Neither fires on the carriage path.
	{"g_sf", "POINT", "POINT(-122.4 37.8)"},
	{"g_line", "LINESTRING", "LINESTRING(12.5 41.9,2.35 48.86)"},
	{"g_poly", "POLYGON", "POLYGON((0 0,0 1,1 1,1 0,0 0))"},
	// The paren-less element form: MariaDB's WKT parser rejects the
	// `MULTIPOINT((x y),(x y))` spelling MySQL renders, and both accept this.
	{"g_mpoint", "MULTIPOINT", "MULTIPOINT(12.5 41.9,2.35 48.86)"},
	{"g_mline", "MULTILINESTRING", "MULTILINESTRING((12.5 41.9,2.35 48.86),(0 1,1 0))"},
	{"g_mpoly", "MULTIPOLYGON", "MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))"},
	{"g_coll", "GEOMETRYCOLLECTION", "GEOMETRYCOLLECTION(POINT(12.5 41.9),LINESTRING(2.35 48.86,0 1))"},
}

// geometryAxisSeedDDL renders the corpus for one MySQL-family server.
//
// Each server is seeded through ITS OWN conversion convention, so both hold
// the same PLACE: MariaDB's WKT parser is axis-agnostic, MySQL's defaults to
// latitude-first for a geographic SRS and is told otherwise explicitly. That
// asymmetry in the SEED is the whole point — if the two servers' stored bytes
// then match, the axis order lives in the conversion functions and not in the
// storage.
//
// Every column DECLARES its SRID, because an undeclared column holding a 4326
// row is refused up front by the per-row SRID guard (audit 2026-08-05 C-14)
// and could never reach a carriage question at all.
func geometryAxisSeedDDL(mariadb bool) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE places (\n  id INT NOT NULL PRIMARY KEY")
	for _, p := range geometryAxisPlaces {
		if mariadb {
			// MariaDB spells the per-column SRID as REF_SYSTEM_ID.
			fmt.Fprintf(&b, ",\n  %s %s REF_SYSTEM_ID=4326 NOT NULL", p.col, p.typ)
			continue
		}
		fmt.Fprintf(&b, ",\n  %s %s SRID 4326 NOT NULL", p.col, p.typ)
	}
	b.WriteString("\n);\n")

	b.WriteString("INSERT INTO places (id")
	for _, p := range geometryAxisPlaces {
		fmt.Fprintf(&b, ", %s", p.col)
	}
	b.WriteString(") VALUES (1")
	for _, p := range geometryAxisPlaces {
		if mariadb {
			fmt.Fprintf(&b, ", ST_GeomFromText('%s', 4326)", p.wkt)
			continue
		}
		fmt.Fprintf(&b, ", ST_GeomFromText('%s', 4326, 'axis-order=long-lat')", p.wkt)
	}
	b.WriteString(");\n")
	return b.String()
}

// TestGeometryAxisPremise_MySQLAndMariaDBStoreTheSameBytes is the premise
// itself: the same PLACE has the same STORED form on both servers.
//
// If this ever fails, the two servers have genuinely diverged in their stored
// representation and sluice's byte-verbatim carriage becomes unsafe — which is
// the condition under which a WKB coordinate-pair swapper would be needed.
//
// One was built for Bug 238 — a full 2D walker over all seven WKB families,
// pinned against MySQL's own ST_SwapXY — and REMOVED once the premise was
// disproved, rather than left in the tree as dead code that reads as
// load-bearing and invites someone to wire it. It is `geometry_axis.go` in the
// mysql engine package at commit 71cb943d; recover it from that SHA if this
// test ever fails.
func TestGeometryAxisPremise_MySQLAndMariaDBStoreTheSameBytes(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	mariaSource, _, mariaCleanup := startMariaDB(t)
	defer mariaCleanup()

	applyMySQLDDL(t, mysqlSource, geometryAxisSeedDDL(false))
	applyMySQLDDL(t, mariaSource, geometryAxisSeedDDL(true))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for _, p := range geometryAxisPlaces {
		my := queryMySQLString(t, ctx, mysqlSource, "SELECT HEX("+p.col+") FROM places WHERE id = 1")
		md := queryMySQLString(t, ctx, mariaSource, "SELECT HEX("+p.col+") FROM places WHERE id = 1")
		if my == "" {
			t.Fatalf("premise check: MySQL %s stored nothing; the fixture is not exercising what this test claims", p.col)
		}
		if my != md {
			t.Errorf("%s: MySQL and MariaDB no longer store the same PLACE the same way — sluice's "+
				"byte-verbatim geometry carriage is unsafe and the Bug 238 axis swap becomes necessary\n"+
				"  MySQL   HEX = %s\n  MariaDB HEX = %s", p.col, my, md)
		}
	}

	// The two servers also agree about where the point IS, each read through
	// its own accessor. This is the half that says the identical bytes are not
	// identically WRONG.
	assertMySQLPlace(t, ctx, mysqlSource, "g_point", 41.9, 12.5, "the MySQL seed")
	assertMySQLPlace(t, ctx, mysqlSource, "g_sf", 37.8, -122.4, "the MySQL seed")
	assertMariaDBPlace(t, ctx, mariaSource, "g_point", 41.9, 12.5, "the MariaDB seed")
	assertMariaDBPlace(t, ctx, mariaSource, "g_sf", 37.8, -122.4, "the MariaDB seed")
}

// TestGeometryAxisPremise_MariaDBToMySQLCarriesThePlace is the behavioural
// half: sluice's actual copy, MariaDB → MySQL, lands the place the source
// held — with no axis normalisation anywhere.
//
// This is the exact scenario Bug 238 reported as a silent relocation, and the
// exact scenario the proposed fix would have BROKEN: swapping on the MySQL
// writer moves Rome to the Somali coast. The g_sf column carries the |lon| > 90
// value the report attributed to `ERROR 3617`; it lands without complaint,
// because the error came from a WKT conversion on the seeding side and not
// from carriage.
func TestGeometryAxisPremise_MariaDBToMySQLCarriesThePlace(t *testing.T) {
	mariaSource, _, mariaCleanup := startMariaDB(t)
	defer mariaCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyMySQLDDL(t, mariaSource, geometryAxisSeedDDL(true))

	mariaEng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	mig := &Migrator{
		Source: mariaEng, Target: mysqlEng,
		SourceDSN: mariaSource, TargetDSN: mysqlTarget,
		Filter:      migcore.TableFilter{Include: []string{"places"}},
		MigrationID: "geom-axis-premise-mariadb-mysql",
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// The oracle is the TARGET server's own reading of the stored value —
	// never sluice's report of what it copied.
	assertMySQLPlace(t, ctx, mysqlTarget, "g_point", 41.9, 12.5, "the MariaDB → MySQL copy")
	assertMySQLPlace(t, ctx, mysqlTarget, "g_sf", 37.8, -122.4, "the MariaDB → MySQL copy")

	// And every family carries byte-for-byte, which is what "verbatim" means.
	for _, p := range geometryAxisPlaces {
		src := queryMySQLString(t, ctx, mariaSource, "SELECT HEX("+p.col+") FROM places WHERE id = 1")
		tgt := queryMySQLString(t, ctx, mysqlTarget, "SELECT HEX("+p.col+") FROM places WHERE id = 1")
		if src == "" {
			t.Fatalf("premise check: MariaDB source %s stored nothing", p.col)
		}
		if tgt != src {
			t.Errorf("%s: MariaDB → MySQL is no longer byte-verbatim\n  source = %s\n  target = %s", p.col, src, tgt)
		}
	}
}

// assertMySQLPlace reads a point through MySQL's own geographic accessors.
func assertMySQLPlace(t *testing.T, ctx context.Context, dsn, col string, wantLat, wantLon float64, lane string) {
	t.Helper()
	lat := queryMySQLFloat(t, ctx, dsn, "SELECT ST_Latitude("+col+") FROM places WHERE id = 1")
	lon := queryMySQLFloat(t, ctx, dsn, "SELECT ST_Longitude("+col+") FROM places WHERE id = 1")
	if lat != wantLat || lon != wantLon {
		t.Errorf("%s: %s is at latitude %v, longitude %v; want latitude %v, longitude %v",
			lane, col, lat, lon, wantLat, wantLon)
	}
}

// assertMariaDBPlace reads the same point through MariaDB's accessors, where
// ST_X is longitude and ST_Y is latitude (MariaDB has no ST_Latitude).
func assertMariaDBPlace(t *testing.T, ctx context.Context, dsn, col string, wantLat, wantLon float64, lane string) {
	t.Helper()
	lon := queryMySQLFloat(t, ctx, dsn, "SELECT ST_X("+col+") FROM places WHERE id = 1")
	lat := queryMySQLFloat(t, ctx, dsn, "SELECT ST_Y("+col+") FROM places WHERE id = 1")
	if lat != wantLat || lon != wantLon {
		t.Errorf("%s: %s is at latitude %v, longitude %v; want latitude %v, longitude %v",
			lane, col, lat, lon, wantLat, wantLon)
	}
}

// queryMySQLString runs a single-value query against a MySQL-protocol server.
func queryMySQLString(t *testing.T, ctx context.Context, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()

	var out sql.NullString
	if err := db.QueryRowContext(ctx, query).Scan(&out); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return out.String
}

// queryMySQLFloat is queryMySQLString for a coordinate.
func queryMySQLFloat(t *testing.T, ctx context.Context, dsn, query string) float64 {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()

	var out float64
	if err := db.QueryRowContext(ctx, query).Scan(&out); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return out
}
