//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// postgisPrebakedImage is the task-#68 pre-baked postgis container
// image — built nightly from upstream postgis/postgis:16-3.4 by
// .github/workflows/build-prebaked-images.yml. Cold-start drops from
// ~30-60s (postgis initdb is heavier than vanilla postgres initdb) to
// ~5s. Byte-equivalent to the upstream image except
// /var/lib/postgresql/data is pre-populated. See docs/dev/ci-images.md.
//
// Defined here (not in shared_container_integration_test.go) because
// the postgis-tagged tests live in their own build-tag bucket; the
// shared TestMain in the postgres engine package never boots a postgis
// container, so threading the constant through there isn't needed.
const postgisPrebakedImage = "ghcr.io/sluicesync/sluice-postgis:16-3.4-prebaked"

// startPostgresWithPostGIS boots the pre-baked postgis image,
// creates source and target databases, and runs CREATE EXTENSION
// postgis on the target so the engine's open-time detection
// reports PostGIS available.
func startPostgresWithPostGIS(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := pgtc.Run(
		ctx,
		// Task #68: pre-baked postgis image — already has initdb done so
		// the boot avoids the disk-I/O contention path that the
		// upstream postgis/postgis:16-3.4 hits on the self-hosted runner
		// pool. Byte-equivalent to upstream postgis/postgis:16-3.4
		// except /var/lib/postgresql/data is pre-populated; the PostGIS
		// extension's shared libraries are present the same way they
		// are in the upstream image. See docs/dev/ci-images.md.
		postgisPrebakedImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		// Pre-baked image's datadir is pre-initialized so postgres
		// only logs "ready to accept connections" once — single-
		// occurrence wait replaces BasicWaitStrategies' 2-occurrence
		// inner strategy. See pg_prebaked_integration_test.go.
		pgPrebakedWaitStrategy(),
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
	defer migcore.CloseIf(sr)
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

	err = db.QueryRowContext(
		queryCtx,
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

// TestMigrate_PostGIS_PGToMySQL covers the reverse cross-engine path
// (Bug 26 closure on the PG-source side). PG's `geometry(POINT, 4326)`
// schema is read with SRID populated from PostGIS's geometry_columns
// view; the MySQL writer emits `POINT NOT NULL SRID 4326` so the
// target's ST_SRID(loc) returns 4326 instead of 0. Row values
// round-trip via the EWKB → WKB → MySQL-prefixed-WKB path.
func TestMigrate_PostGIS_PGToMySQL(t *testing.T) {
	_, pgSource, pgCleanup := startPostgresWithPostGIS(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	// Need to enable postgis on the source database to define the
	// geometry column. startPostgresWithPostGIS enables it on
	// target_db only.
	srcDB, err := sql.Open("pgx", pgSource)
	if err != nil {
		t.Fatalf("open pg source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	srcCtx, srcCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer srcCancel()
	if _, err := srcDB.ExecContext(srcCtx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
		t.Fatalf("CREATE EXTENSION postgis (source): %v", err)
	}
	if _, err := srcDB.ExecContext(srcCtx, `
		CREATE TABLE places (
			id   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			name TEXT NOT NULL,
			loc  geometry(POINT, 4326) NOT NULL
		);
		INSERT INTO places (name, loc) VALUES
			('origin',  ST_SetSRID(ST_MakePoint(0, 0), 4326)),
			('one-one', ST_SetSRID(ST_MakePoint(1, 1), 4326));
	`); err != nil {
		t.Fatalf("seed pg source: %v", err)
	}

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    mysqlEng,
		SourceDSN: pgSource,
		TargetDSN: mysqlTarget,
		// PostGIS auto-installs `geography_columns` + `geometry_columns`
		// views into the public schema as part of `CREATE EXTENSION
		// postgis`. They reference PG-specific functions that don't
		// translate to MySQL; the test's focus is GEOMETRY column
		// translation, not view-body translation. SkipViews keeps the
		// test scoped to what Bug 26's fix actually addresses.
		SkipViews: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// Schema-side: MySQL columns table reports srs_id=4326 for the
	// loc column.
	wantSRID := 4326
	gotSRID := readMySQLGeometrySRID(t, ctx, mysqlTarget, "places", "loc")
	if gotSRID != wantSRID {
		t.Errorf("places.loc srs_id on MySQL = %d; want %d", gotSRID, wantSRID)
	}

	// Value-side: ST_AsText round-trip on MySQL.
	want := map[string]string{
		"origin":  "POINT(0 0)",
		"one-one": "POINT(1 1)",
	}
	got := readMySQLPlaceTextGeometries(t, ctx, mysqlTarget)
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for name, wantText := range want {
		if got[name] != wantText {
			t.Errorf("places[%q] ST_AsText = %q; want %q", name, got[name], wantText)
		}
	}

	// Also belt-and-braces ST_SRID on the value bytes — confirms the
	// MySQL row writer prefixed the bytes with SRID 4326.
	gotValSRIDs := readMySQLPlaceValueSRIDs(t, ctx, mysqlTarget)
	for name, srid := range gotValSRIDs {
		if srid != wantSRID {
			t.Errorf("places[%q] ST_SRID(loc) = %d; want %d", name, srid, wantSRID)
		}
	}
}

// readMySQLGeometrySRID returns the srs_id from
// information_schema.columns for a given (table, column). 0 means
// "no row", which the test assertion catches.
func readMySQLGeometrySRID(t *testing.T, ctx context.Context, dsn, table, column string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer func() { _ = db.Close() }()
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var srid sql.NullInt64
	err = db.QueryRowContext(
		queryCtx,
		`SELECT srs_id FROM information_schema.columns
		  WHERE table_schema = DATABASE()
		    AND table_name   = ?
		    AND column_name  = ?`,
		table, column,
	).Scan(&srid)
	if err != nil {
		t.Logf("srs_id lookup for %s.%s: %v", table, column, err)
		return 0
	}
	if !srid.Valid {
		return 0
	}
	return int(srid.Int64)
}

// readMySQLPlaceTextGeometries returns name → ST_AsText(loc) for every
// row in the MySQL places table.
func readMySQLPlaceTextGeometries(t *testing.T, ctx context.Context, dsn string) map[string]string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
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
		t.Fatalf("rows: %v", err)
	}
	return out
}

// readMySQLPlaceValueSRIDs returns name → ST_SRID(loc) — the SRID
// MySQL extracts from each row value's prefix bytes. Distinct from
// the column-level srs_id; together they confirm the EWKB → WKB →
// MySQL-prefixed-WKB conversion preserved the SRID at every step.
func readMySQLPlaceValueSRIDs(t *testing.T, ctx context.Context, dsn string) map[string]int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer func() { _ = db.Close() }()
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(queryCtx, "SELECT name, ST_SRID(loc) FROM places ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]int{}
	for rows.Next() {
		var name string
		var srid int
		if err := rows.Scan(&name, &srid); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[name] = srid
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// TestMigrate_PostGIS_PGToMariaDB is the item-73 Phase-2 F3 pin on the
// PG→mariadb direction: a PG `geometry(POINT, 4326)` column lands on a
// MariaDB target via the `REF_SYSTEM_ID=4326` TYPE attribute (MariaDB
// rejects MySQL 8's `SRID 4326`), so the column's GEOMETRY_COLUMNS.SRID
// AND each value's ST_SRID both round-trip as 4326 — closing the geometry
// round-trip the Phase-2 read side opened. Mirrors PGToMySQL.
func TestMigrate_PostGIS_PGToMariaDB(t *testing.T) {
	_, pgSource, pgCleanup := startPostgresWithPostGIS(t)
	defer pgCleanup()

	_, mariadbTarget, mariadbCleanup := startMariaDB(t)
	defer mariadbCleanup()

	srcDB, err := sql.Open("pgx", pgSource)
	if err != nil {
		t.Fatalf("open pg source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	srcCtx, srcCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer srcCancel()
	if _, err := srcDB.ExecContext(srcCtx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
		t.Fatalf("CREATE EXTENSION postgis (source): %v", err)
	}
	if _, err := srcDB.ExecContext(srcCtx, `
		CREATE TABLE places (
			id   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			name TEXT NOT NULL,
			loc  geometry(POINT, 4326) NOT NULL
		);
		INSERT INTO places (name, loc) VALUES
			('origin',  ST_SetSRID(ST_MakePoint(0, 0), 4326)),
			('one-one', ST_SetSRID(ST_MakePoint(1, 1), 4326));
	`); err != nil {
		t.Fatalf("seed pg source: %v", err)
	}

	mariadbEng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    mariadbEng,
		SourceDSN: pgSource,
		TargetDSN: mariadbTarget,
		SkipViews: true, // PostGIS auto-installs geometry_columns views (PG-only bodies)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (PG → mariadb): %v", err)
	}

	// Column SRID via GEOMETRY_COLUMNS (MariaDB has no srs_id column) —
	// this is the REF_SYSTEM_ID=4326 emit landing.
	if gotSRID := readMariaDBGeometrySRID(t, ctx, mariadbTarget, "places", "loc"); gotSRID != 4326 {
		t.Errorf("places.loc GEOMETRY_COLUMNS srid on MariaDB = %d; want 4326 (REF_SYSTEM_ID emit)", gotSRID)
	}
	// Value round-trip: text + per-value SRID.
	wantText := map[string]string{"origin": "POINT(0 0)", "one-one": "POINT(1 1)"}
	if got := readMySQLPlaceTextGeometries(t, ctx, mariadbTarget); len(got) != len(wantText) {
		t.Fatalf("got %d rows; want %d", len(got), len(wantText))
	} else {
		for name, want := range wantText {
			if got[name] != want {
				t.Errorf("places[%q] ST_AsText = %q; want %q", name, got[name], want)
			}
		}
	}
	for name, srid := range readMySQLPlaceValueSRIDs(t, ctx, mariadbTarget) {
		if srid != 4326 {
			t.Errorf("places[%q] ST_SRID(loc) = %d; want 4326", name, srid)
		}
	}
}

// TestMigrate_PostGIS_MariaDBToPG is the item-73 Phase-2 F3 pin on the
// mariadb→PG direction: a MariaDB geometry column declared with
// REF_SYSTEM_ID=4326 is READ back with SRID 4326 (from GEOMETRY_COLUMNS)
// and migrated to a PostGIS target as geometry(POINT, 4326), value + SRID
// surviving. Together with PGToMariaDB this proves the read+write SRID
// round-trip both ways.
func TestMigrate_PostGIS_MariaDBToPG(t *testing.T) {
	mariadbSource, _, mariadbCleanup := startMariaDB(t)
	defer mariadbCleanup()

	_, pgTarget, pgCleanup := startPostgresWithPostGIS(t)
	defer pgCleanup()

	applyMySQLDDL(t, mariadbSource, `
		CREATE TABLE places (
			id   BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			loc  POINT REF_SYSTEM_ID=4326 NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO places (name, loc) VALUES
			('origin',  ST_GeomFromText('POINT(0 0)', 4326)),
			('one-one', ST_GeomFromText('POINT(1 1)', 4326));`)

	mariadbEng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    mariadbEng,
		Target:    pgEng,
		SourceDSN: mariadbSource,
		TargetDSN: pgTarget,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (mariadb → PG): %v", err)
	}

	// PG column: geometry(POINT, 4326) — the mariadb read SRID (from
	// GEOMETRY_COLUMNS) fed the cross-engine emit.
	subtype, srid := readPostGISColumnType(t, ctx, pgTarget, "places", "loc")
	if subtype != "POINT" {
		t.Errorf("places.loc PG subtype = %q; want POINT", subtype)
	}
	if srid != 4326 {
		t.Errorf("places.loc PG srid = %d; want 4326 (SRID read from MariaDB GEOMETRY_COLUMNS)", srid)
	}
	wantText := map[string]string{"origin": "POINT(0 0)", "one-one": "POINT(1 1)"}
	got := readPlaceTextGeometries(t, ctx, pgTarget)
	if len(got) != len(wantText) {
		t.Fatalf("got %d rows; want %d", len(got), len(wantText))
	}
	for name, want := range wantText {
		if got[name] != want {
			t.Errorf("places[%q] ST_AsText = %q; want %q", name, got[name], want)
		}
	}
}

// readMariaDBGeometrySRID returns the per-column SRID from MariaDB's
// information_schema.GEOMETRY_COLUMNS view (MariaDB has no srs_id column
// on information_schema.columns, unlike MySQL 8). 0 means "no row".
func readMariaDBGeometrySRID(t *testing.T, ctx context.Context, dsn, table, column string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mariadb: %v", err)
	}
	defer func() { _ = db.Close() }()
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var srid sql.NullInt64
	err = db.QueryRowContext(
		queryCtx,
		`SELECT srid FROM information_schema.geometry_columns
		  WHERE g_table_schema = DATABASE()
		    AND g_table_name    = ?
		    AND g_geometry_column = ?`,
		table, column,
	).Scan(&srid)
	if err != nil {
		t.Logf("GEOMETRY_COLUMNS srid lookup for %s.%s: %v", table, column, err)
		return 0
	}
	if !srid.Valid {
		return 0
	}
	return int(srid.Int64)
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
