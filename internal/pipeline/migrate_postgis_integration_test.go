//go:build integration && postgis

// End-to-end integration test for the PostGIS-aware GEOMETRY
// translation. Boots the postgis/postgis:16-3.4 image (heavier than
// the postgres:16 used by other integration tests, which is why this
// file is gated on a separate `postgis` build tag), enables PostGIS
// on the target, and migrates a MySQL source containing a POINT
// column into it. The verification reads ST_AsText on the target so
// the assertion compares the human-readable geometry rather than
// raw EWKB bytes.

package pipeline

import (
	"context"
	"database/sql"
	"io"
	"testing"
	"time"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startPostgresWithPostGIS boots the postgis/postgis:16-3.4 image,
// creates source and target databases, and runs CREATE EXTENSION
// postgis on the target so the engine's open-time detection
// reports PostGIS available.
func startPostgresWithPostGIS(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
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

	// Enable PostGIS on the target database. The image ships with
	// the extension files installed but inactive; CREATE EXTENSION
	// flips the bit per-database.
	tgtDB, err := sql.Open("pgx", tgtConn)
	if err != nil {
		terminate()
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	if _, err := tgtDB.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
		terminate()
		t.Fatalf("CREATE EXTENSION postgis: %v", err)
	}

	return srcConn, tgtConn, terminate
}

// TestMigrate_PostGIS_MySQLToPG covers the cross-engine path with
// a POINT column. The MySQL source's geometry value (SRID-prefixed
// WKB) gets stripped to pure WKB by the value decoder, then wrapped
// in PostGIS EWKB framing by the PG row writer using the column's
// SRID from the mappings entry.
func TestMigrate_PostGIS_MySQLToPG(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	_, pgTarget, pgCleanup := startPostgresWithPostGIS(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE places (
			id   BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			loc  POINT SRID 4326 NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO places (name, loc) VALUES
			('origin',   ST_GeomFromText('POINT(0 0)', 4326)),
			('one-one',  ST_GeomFromText('POINT(1 1)', 4326));
	`
	applyMySQLDDL(t, mysqlSource, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSource,
		TargetDSN: pgTarget,
		Stdout:    io.Discard,
		// MySQL POINT comes through as ir.Geometry{Subtype: GeometryPoint}
		// without a SRID. The mapping override sets SRID=4326 so the PG
		// column emerges as geometry(POINT, 4326) and the EWKB framing
		// uses the same SRID — without this, the points would land in
		// a SRID-0 column and queries like ST_AsText would still work
		// but spatial joins against other 4326 data would fail.
		Mappings: []config.Mapping{
			{
				Table: "places", Column: "loc",
				TargetType:        "postgis_point",
				TargetTypeOptions: map[string]any{"srid": 4326},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// Schema-side: PG sees the column as geometry(POINT, 4326). The
	// schema reader returns ir.Geometry; PostGIS-specific subtype +
	// SRID introspection isn't part of the v1 schema reader (would be
	// a future enhancement to reconstruct SRID from PG's pg_geometry_columns
	// metadata). For now we verify the column DDL via PG's
	// geometry_columns view directly — the canonical source of truth
	// for PostGIS-typed columns.
	subtype, srid := readPostGISColumnType(t, ctx, pgTarget, "places", "loc")
	if subtype != "POINT" {
		t.Errorf("places.loc subtype = %q; want POINT", subtype)
	}
	if srid != 4326 {
		t.Errorf("places.loc srid = %d; want 4326", srid)
	}

	// Value-side: ST_AsText round-trip. The migrated rows should
	// produce identical text representations to the source.
	want := map[string]string{
		"origin":  "POINT(0 0)",
		"one-one": "POINT(1 1)",
	}
	got := readPlaceTextGeometries(t, ctx, pgTarget)
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for name, wantText := range want {
		if got[name] != wantText {
			t.Errorf("places[%q] ST_AsText = %q; want %q", name, got[name], wantText)
		}
	}

	// Belt-and-braces: the IR shape on the read-back side. The PG
	// schema reader queries PostGIS's geometry_columns view to
	// reconstruct the precise subtype + SRID; we should see them
	// matching what the writer emitted.
	sr, err := pgEng.OpenSchemaReader(ctx, pgTarget)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	got2, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := findTable(got2, "places")
	if tbl == nil {
		t.Fatalf("places table not found after migration")
	}
	loc := findColumn(tbl, "loc")
	if loc == nil {
		t.Fatalf("places.loc column missing")
	}
	geom, ok := loc.Type.(ir.Geometry)
	if !ok {
		t.Fatalf("places.loc IR type = %T; want ir.Geometry", loc.Type)
	}
	if geom.Subtype != ir.GeometryPoint {
		t.Errorf("places.loc Subtype = %v; want GeometryPoint", geom.Subtype)
	}
	if geom.SRID != 4326 {
		t.Errorf("places.loc SRID = %d; want 4326", geom.SRID)
	}
}

// readPostGISColumnType reads the geometry subtype and SRID from
// PG's geometry_columns view (the canonical PostGIS metadata
// source). Returns ("", 0) when no row matches — the caller's test
// failure surfaces that.
func readPostGISColumnType(t *testing.T, ctx context.Context, dsn, table, column string) (subtype string, srid int) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = db.QueryRowContext(queryCtx,
		`SELECT type, srid
		   FROM geometry_columns
		  WHERE f_table_name = $1 AND f_geometry_column = $2`,
		table, column,
	).Scan(&subtype, &srid)
	if err != nil {
		t.Logf("geometry_columns lookup for %s.%s: %v", table, column, err)
		return "", 0
	}
	return subtype, srid
}

// readPlaceTextGeometries returns name → ST_AsText(loc) for every
// row in the places table. Used by the round-trip assertion.
func readPlaceTextGeometries(t *testing.T, ctx context.Context, dsn string) map[string]string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(queryCtx, "SELECT name, ST_AsText(loc) FROM places ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var name, geom string
		if err := rows.Scan(&name, &geom); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[name] = geom
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration: %v", err)
	}
	return out
}
