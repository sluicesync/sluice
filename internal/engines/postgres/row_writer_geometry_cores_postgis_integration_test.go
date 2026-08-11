//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 239 — the RowWriter has THREE write cores, and only one of them
// could carry a geometry value.
//
// # What went wrong
//
// [prepareValue] hands a geometry column's value to pgx as EWKB `[]byte`.
// The COPY core ships that in COPY-BINARY, which PostGIS `geometry_recv`
// accepts with no codec at all — so COPY worked and everything downstream
// of it looked fine. The other two cores ([writeViaBatch] and
// [writeViaBatchIdempotent]) bind the same bytes as query PARAMETERS, and
// PostGIS's `geometry` type has a DYNAMIC per-database OID for which pgx
// has no codec: it fell back to TEXT format, `geometry_in` tried to parse
// raw EWKB as text, and every row died with
//
//	ERROR: parse error - invalid geometry (SQLSTATE XX000)
//
// The geometry codec that fixes exactly this ([pgGeometryBinaryCodec],
// v0.32.1-era task #20) already existed — it was registered on the CDC
// applier's three pools and on none of the writer's. A VStream (Vitess /
// PlanetScale) source ALWAYS demands the idempotent core, so a `sync` of
// any spatial column died on cold start with 0 rows while `migrate` over
// the identical source landed it. That asymmetry is the whole bug.
//
// # What this pins, and over which paths (the gate owes its own scope)
//
// [TestRowWriter_PostGIS_GeometryFamiliesAcrossWriteCores] drives ALL
// THREE cores — COPY, plain batch, idempotent batch — over the seven WKB
// geometry families × {2-D, 3-D (Z), NULL} × BOTH spatial column types,
// `geometry` and `geography`. One family is not a representative here
// (Bug 74): `geometry_recv` parses each family's WKB body differently,
// and the pre-fix failure was a whole-format rejection that any single
// family would have caught only for itself. Geography is not a
// representative of geometry either (audit GAP 1, 2026-08-10): it is a
// DIFFERENT dynamic OID routed to a different receive function
// (`geography_recv`), and it spent Bug 239's whole lifetime failing on
// the batch cores with the identical error text after geometry was
// fixed — because only the geometry OID had the codec registered.
//
// Reaching the plain-batch core uses the PRODUCTION route rather than
// poking unexported state: an `interval` column on the table, which
// [WriteRows] dispatches away from COPY ([tableHasIntervalColumn]). That
// is a real shape — a table with both a spatial and an interval column —
// and it means this pin covers the batch core the way an operator reaches
// it.
//
// The independent expected value: the WKT literal written in this file,
// plus the geometry-type name and dimension count, all asserted with the
// TARGET's own functions (ST_GeometryType / ST_NDims / ST_Equals), never
// re-derived from the bytes sluice wrote. The byte-exactness check
// (ST_AsBinary(target) == the WKB handed to the writer) is the second,
// stricter leg — it fails on any silent re-shaping the WKT comparison
// would tolerate.
//
// Runs in the required "Integration (PostGIS)" CI job (the `PostGIS_`
// name segment is what that job's -run filter selects).

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// geometryFamilyCase is one row of the Bug-74 family matrix: a WKB
// geometry family in a given dimensional shape.
type geometryFamilyCase struct {
	name string
	// wkt is BOTH the seed (converted to WKB on the server, which is what
	// a MySQL/VStream source hands the writer) and the independent
	// expected value the target is checked against.
	wkt string
	// wantType is PostGIS's own ST_GeometryType spelling.
	wantType string
	// wantNDims is 2 for planar, 3 for Z.
	wantNDims int
	// subtype is the IR column subtype the target column is declared with.
	subtype ir.GeometrySubtype
}

// geometryFamilyMatrix is every WKB geometry family × {2-D, Z}. NULL is a
// third shape, handled per-case by the driver (each family also writes a
// NULL row into its nullable column).
func geometryFamilyMatrix() []geometryFamilyCase {
	return []geometryFamilyCase{
		{"point", "POINT(1 2)", "ST_Point", 2, ir.GeometryPoint},
		{"point_z", "POINT Z (1 2 3)", "ST_Point", 3, ir.GeometryPoint},
		{"linestring", "LINESTRING(0 0,1 1,2 2)", "ST_LineString", 2, ir.GeometryLineString},
		{"linestring_z", "LINESTRING Z (0 0 0,1 1 1)", "ST_LineString", 3, ir.GeometryLineString},
		{"polygon", "POLYGON((0 0,4 0,4 4,0 4,0 0))", "ST_Polygon", 2, ir.GeometryPolygon},
		{"polygon_z", "POLYGON Z ((0 0 1,4 0 1,4 4 1,0 4 1,0 0 1))", "ST_Polygon", 3, ir.GeometryPolygon},
		{"multipoint", "MULTIPOINT((0 0),(1 1))", "ST_MultiPoint", 2, ir.GeometryMultiPoint},
		{"multipoint_z", "MULTIPOINT Z ((0 0 1),(1 1 2))", "ST_MultiPoint", 3, ir.GeometryMultiPoint},
		{"multilinestring", "MULTILINESTRING((0 0,1 1),(2 2,3 3))", "ST_MultiLineString", 2, ir.GeometryMultiLineString},
		{"multilinestring_z", "MULTILINESTRING Z ((0 0 1,1 1 1))", "ST_MultiLineString", 3, ir.GeometryMultiLineString},
		{"multipolygon", "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))", "ST_MultiPolygon", 2, ir.GeometryMultiPolygon},
		{"multipolygon_z", "MULTIPOLYGON Z (((0 0 1,1 0 1,1 1 1,0 1 1,0 0 1)))", "ST_MultiPolygon", 3, ir.GeometryMultiPolygon},
		{"geometrycollection", "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))", "ST_GeometryCollection", 2, ir.GeometryCollection},
		{"geometrycollection_z", "GEOMETRYCOLLECTION Z (POINT Z (1 2 3))", "ST_GeometryCollection", 3, ir.GeometryCollection},
	}
}

// writeCore names one of the RowWriter's three value-binding cores and
// how a caller actually reaches it.
type writeCore struct {
	name string
	// intervalColumn forces [WriteRows] off COPY and onto the plain batch
	// core (tableHasIntervalColumn) — the production route, not a poke at
	// unexported state.
	intervalColumn bool
	// idempotent selects WriteRowsIdempotent (what a VStream cold start
	// demands) over WriteRows.
	idempotent bool
}

// spatialColumnType is the {geometry, geography} axis: the target column's
// declared type, which selects the receive function (and, pre-GAP-1-fix,
// decided whether the batch cores had a codec at all).
type spatialColumnType struct {
	name        string
	isGeography bool
}

// TestRowWriter_PostGIS_GeometryFamiliesAcrossWriteCores is the Bug 239 +
// GAP 1 pin. Every geometry family × dimensional shape × NULL must land
// byte-identically through EVERY write core, not just COPY — into BOTH
// spatial column types, not just `geometry`.
func TestRowWriter_PostGIS_GeometryFamiliesAcrossWriteCores(t *testing.T) {
	dsn, cleanup := startPGForPipelinedPostGIS(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	rw, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer func() {
		if c, ok := rw.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	writer, ok := rw.(*RowWriter)
	if !ok {
		t.Fatalf("OpenRowWriter returned %T; want *RowWriter", rw)
	}
	if !writer.hasPostGIS {
		t.Fatal("target reports no PostGIS; the whole matrix would be vacuous")
	}
	idem, ok := rw.(ir.IdempotentRowWriter)
	if !ok {
		t.Fatalf("PG RowWriter %T must implement ir.IdempotentRowWriter", rw)
	}

	matrix := geometryFamilyMatrix()
	if len(matrix) < 14 {
		t.Fatalf("family matrix has %d cases; want all 7 WKB families × {2-D, Z} (anti-vacuity floor)", len(matrix))
	}

	cores := []writeCore{
		{name: "copy"},
		{name: "batch", intervalColumn: true},
		{name: "idempotent", idempotent: true},
	}
	colTypes := []spatialColumnType{
		{name: "geometry"},
		{name: "geography", isGeography: true},
	}

	for _, ct := range colTypes {
		for _, core := range cores {
			for _, tc := range matrix {
				t.Run(ct.name+"/"+core.name+"/"+tc.name, func(t *testing.T) {
					runGeometryCoreCase(ctx, t, db, writer, idem, ct, core, tc)
				})
			}
		}
	}
}

// runGeometryCoreCase creates a one-off target table, writes the family's
// value (plus a NULL sibling row) through the named core, and
// ground-truths the landed geometry against this file's WKT literal using
// the target's own spatial functions.
func runGeometryCoreCase(
	ctx context.Context,
	t *testing.T,
	db *sql.DB,
	writer *RowWriter,
	idem ir.IdempotentRowWriter,
	ct spatialColumnType,
	core writeCore,
	tc geometryFamilyCase,
) {
	t.Helper()

	table := fmt.Sprintf("geo_%s_%s_%s", ct.name, core.name, tc.name)
	ddl := fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, g %s NULL`, quoteIdent(table), ct.name)
	if core.intervalColumn {
		ddl += `, span interval NULL`
	}
	ddl += `)`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}

	// The seed WKB comes from the server's own ST_AsBinary — the same OGC
	// WKB shape a MySQL/VStream source hands the writer (docs/value-types.md).
	var wkbHex string
	if err := db.QueryRowContext(ctx,
		`SELECT encode(ST_AsBinary(ST_GeomFromText($1)), 'hex')`, tc.wkt).Scan(&wkbHex); err != nil {
		t.Fatalf("seed WKB for %q: %v", tc.wkt, err)
	}
	wkb, err := hex.DecodeString(wkbHex)
	if err != nil {
		t.Fatalf("decode seed WKB: %v", err)
	}

	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
		{Name: "g", Type: ir.Geometry{Subtype: tc.subtype, IsGeography: ct.isGeography}, Nullable: true},
	}
	row1 := ir.Row{"id": int64(1), "g": wkb}
	row2 := ir.Row{"id": int64(2), "g": nil}
	if core.intervalColumn {
		cols = append(cols, &ir.Column{Name: "span", Type: ir.Interval{}, Nullable: true})
		row1["span"] = "1 day"
		row2["span"] = nil
	}
	irTable := &ir.Table{
		Name:       table,
		Columns:    cols,
		PrimaryKey: &ir.Index{Name: "pk_" + table, Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	rows := make(chan ir.Row, 2)
	rows <- row1
	rows <- row2
	close(rows)

	if core.idempotent {
		err = idem.WriteRowsIdempotent(ctx, irTable, rows)
	} else {
		err = writer.WriteRows(ctx, irTable, rows)
	}
	if err != nil {
		t.Fatalf("write %s family %s via %s core: %v\n"+
			"  (Bug 239: 'parse error - invalid geometry' here means the pool lost the PostGIS geometry codec)",
			table, tc.name, core.name, err)
	}

	// Leg 1 — the target's own functions against this file's literals. The
	// `::geometry` cast is exact (same serialization) and is what lets one
	// query serve both arms: geography-typed columns lack ST_OrderingEquals,
	// and a bare geography column stamps SRID 4326 on ingest, which
	// ST_OrderingEquals would refuse against the literal's SRID 0 — so the
	// comparison normalizes the SRID away (the coordinate bytes are the
	// fidelity claim here; SRID handling has its own pins).
	var gotType string
	var gotNDims int
	var gotEqual bool
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT ST_GeometryType(g::geometry), ST_NDims(g::geometry),
		        ST_OrderingEquals(ST_SetSRID(g::geometry, 0), ST_GeomFromText($1)) FROM %s WHERE id = 1`,
		quoteIdent(table),
	), tc.wkt).Scan(&gotType, &gotNDims, &gotEqual); err != nil {
		t.Fatalf("read back %s: %v", table, err)
	}
	if gotType != tc.wantType {
		t.Errorf("ST_GeometryType = %q; want %q", gotType, tc.wantType)
	}
	if gotNDims != tc.wantNDims {
		t.Errorf("ST_NDims = %d; want %d (a dropped Z is silent value loss)", gotNDims, tc.wantNDims)
	}
	if !gotEqual {
		t.Errorf("ST_OrderingEquals(target, %q) = false; the landed geometry is not the seeded one", tc.wkt)
	}

	// Leg 2 — byte-exactness against what the writer was handed. Catches a
	// re-shaping (dimension drop, ring reorder) the WKT comparison tolerates.
	var backHex string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT encode(ST_AsBinary(g::geometry), 'hex') FROM %s WHERE id = 1`, quoteIdent(table),
	)).Scan(&backHex); err != nil {
		t.Fatalf("read back WKB %s: %v", table, err)
	}
	back, err := hex.DecodeString(backHex)
	if err != nil {
		t.Fatalf("decode target WKB: %v", err)
	}
	if !bytes.Equal(back, wkb) {
		t.Errorf("target WKB = %s; want %s (src==dst byte-exact)", backHex, wkbHex)
	}

	// Leg 3 — the NULL shape survives the same core.
	var isNull bool
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT g IS NULL FROM %s WHERE id = 2`, quoteIdent(table),
	)).Scan(&isNull); err != nil {
		t.Fatalf("read back NULL row %s: %v", table, err)
	}
	if !isNull {
		t.Error("NULL geometry cell did not land as NULL")
	}
}

// TestRowWriter_PostGIS_BatchCoresAreTheOnesBug239Broke is the discriminator
// that keeps the sibling pin above honest: it asserts the two PARAMETER-
// binding cores are genuinely exercised by the matrix, by checking that the
// dispatch it relies on still holds. If [tableHasIntervalColumn] ever stops
// routing away from COPY, the "batch" arm silently becomes a second COPY run
// and the matrix would keep passing while covering one core.
func TestRowWriter_PostGIS_BatchCoresAreTheOnesBug239Broke(t *testing.T) {
	withInterval := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "g", Type: ir.Geometry{Subtype: ir.GeometryPoint}, Nullable: true},
			{Name: "span", Type: ir.Interval{}, Nullable: true},
		},
	}
	if !tableHasIntervalColumn(withInterval) {
		t.Fatal("tableHasIntervalColumn = false for a table with an interval column; " +
			"the family matrix's `batch` arm would silently run COPY twice")
	}
	plain := &ir.Table{
		Name:    "t",
		Columns: []*ir.Column{{Name: "g", Type: ir.Geometry{Subtype: ir.GeometryPoint}, Nullable: true}},
	}
	if tableHasIntervalColumn(plain) {
		t.Fatal("tableHasIntervalColumn = true for a geometry-only table; the `copy` arm is not COPY")
	}
}
