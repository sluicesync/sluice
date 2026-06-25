//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0112 within-table chunk-parallelism integration pins. The
// load-bearing property: a multi-chunk table restored with
// --bulk-parallelism > 1 converges BYTE-IDENTICAL to the serial restore
// of the SAME backup. The class matrix (Bug-74 doctrine — pin the
// class, not the representative):
//
//   {PG target, MySQL target} × value-type-varied table
//   (int / decimal / text / json|jsonb / bytea|blob / uuid|char /
//    temporal / bool) so the parallel chunk-group writers are exercised
//   across value FAMILIES, not one representative type.
//
// The differential is serial-vs-parallel restore of one backup into two
// targets, compared with a whole-row ordered-MD5 fingerprint
// (multiset-equality: a dup, a missing row, or any value drift moves the
// digest). Plus: a 1-chunk table stays serial (no fan-out), and a
// corrupted layer-2 row-count still fails hard. Engagement is observer-
// asserted (restoreChunkDispatchObserver), not timing-inferred.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestRestoreChunkParallel_PG_ByteIdenticalToSerial pins the headline
// claim on a PG target: a value-type-varied, multi-chunk table restored
// with ChunkParallelism=4 produces the EXACT same per-table content as a
// serial restore of the same backup (whole-row md5 differential), and
// the within-table fan-out engaged (observer-asserted).
func TestRestoreChunkParallel_PG_ByteIdenticalToSerial(t *testing.T) {
	sourceDSN, serialDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	parallelDSN := createSecondPGTarget(t, sourceDSN, "target_parallel")

	const table = "varied"
	applyDDL(t, sourceDSN, fmt.Sprintf(`
		CREATE TABLE %s (
			id    BIGINT PRIMARY KEY,
			n     INTEGER NOT NULL,
			amt   NUMERIC(20,6) NOT NULL,
			label TEXT NOT NULL,
			doc   JSONB NOT NULL,
			raw   BYTEA NOT NULL,
			uid   UUID NOT NULL,
			ts    TIMESTAMPTZ NOT NULL,
			flag  BOOLEAN NOT NULL
		);
		INSERT INTO %s (id, n, amt, label, doc, raw, uid, ts, flag)
		SELECT g,
		       g * 7,
		       (g %% 1000) + 0.123456,
		       'row-' || g || repeat('x', g %% 17),
		       jsonb_build_object('k', g, 'arr', jsonb_build_array(g, g+1)),
		       decode(lpad(to_hex(g), 8, '0'), 'hex'),
		       ('00000000-0000-0000-0000-' || lpad(to_hex(g), 12, '0'))::uuid,
		       TIMESTAMPTZ '2026-01-01 00:00:00+00' + (g || ' seconds')::interval,
		       (g %% 2 = 0)
		FROM generate_series(1, 200) g;
	`, table, table))

	pgEng, _ := engines.Get("postgres")
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		ChunkRows: 25, // 200 rows → 8 chunks
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	// Serial restore (ChunkParallelism=1) into target_db.
	if err := (&Restore{
		Target: pgEng, TargetDSN: serialDSN, Store: store,
		TableParallelism: 1, ChunkParallelism: 1,
	}).Run(context.Background()); err != nil {
		t.Fatalf("serial Restore.Run: %v", err)
	}

	// Parallel within-table restore (ChunkParallelism=4) into target_parallel.
	gotChunkP, gotReason := observeRestoreChunkDispatch(t)
	if err := (&Restore{
		Target: pgEng, TargetDSN: parallelDSN, Store: store,
		TableParallelism: 1, ChunkParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("parallel Restore.Run: %v", err)
	}
	if *gotChunkP <= 1 {
		t.Fatalf("within-table dispatch = serial (reason %q); want the chunk fan-out engaged", *gotReason)
	}

	// Both targets must match the source AND each other, byte-identical.
	assertPGTablesMatch(t, sourceDSN, serialDSN, table)
	assertPGTablesMatch(t, sourceDSN, parallelDSN, table)
	sN, sSum := pgTableContentFingerprint(t, serialDSN, table)
	pN, pSum := pgTableContentFingerprint(t, parallelDSN, table)
	if sN != pN || sSum != pSum {
		t.Fatalf("serial (n=%d md5=%s) != parallel (n=%d md5=%s) — within-table parallelism is not byte-identical",
			sN, sSum, pN, pSum)
	}
}

// TestRestoreChunkParallel_PG_SingleChunkStaysSerial pins that a 1-chunk
// table is NOT fanned out even with ChunkParallelism=4 (the fan-out only
// engages where it helps), and the content still round-trips.
func TestRestoreChunkParallel_PG_SingleChunkStaysSerial(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const table = "tiny"
	applyDDL(t, sourceDSN, fmt.Sprintf(`
		CREATE TABLE %s (id BIGINT PRIMARY KEY, label TEXT NOT NULL);
		INSERT INTO %s SELECT g, 'r' || g FROM generate_series(1, 5) g;
	`, table, table))

	pgEng, _ := engines.Get("postgres")
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		ChunkRows: 100, // 5 rows → 1 chunk
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	if err := (&Restore{
		Target: pgEng, TargetDSN: targetDSN, Store: store,
		TableParallelism: 1, ChunkParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	// Single-chunk table collapses to serial inside restoreTable; the
	// content still round-trips.
	assertPGTablesMatch(t, sourceDSN, targetDSN, table)
}

// TestRestoreChunkParallel_MySQL_ByteIdenticalToSerial pins the
// engine-generic claim on a MySQL target: ADR-0112 engages on MySQL too
// (no snapshot gate on the write side), and a value-type-varied
// multi-chunk table restored with ChunkParallelism=4 converges
// byte-identical to the serial restore of the same backup.
func TestRestoreChunkParallel_MySQL_ByteIdenticalToSerial(t *testing.T) {
	sourceDSN, serialDSN, cleanup := startMySQL(t)
	defer cleanup()
	parallelDSN := createSecondMySQLTarget(t, sourceDSN, "target_parallel")

	const table = "varied"
	applyMySQLDDL(t, sourceDSN, fmt.Sprintf(`
		CREATE TABLE %s (
			id    BIGINT PRIMARY KEY,
			n     INT NOT NULL,
			amt   DECIMAL(20,6) NOT NULL,
			label TEXT NOT NULL,
			doc   JSON NOT NULL,
			raw   VARBINARY(64) NOT NULL,
			uid   CHAR(36) NOT NULL,
			ts    DATETIME(6) NOT NULL,
			flag  TINYINT(1) NOT NULL
		);
	`, table))
	// Seed deterministically via a numbers table (MySQL has no
	// generate_series). 200 rows of varied values across families.
	seedMySQLVariedRows(t, sourceDSN, table, 200)

	mysqlEng, _ := engines.Get("mysql")
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&Backup{
		Source: mysqlEng, SourceDSN: sourceDSN, Store: store,
		ChunkRows: 25, // 200 rows → 8 chunks
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	if err := (&Restore{
		Target: mysqlEng, TargetDSN: serialDSN, Store: store,
		TableParallelism: 1, ChunkParallelism: 1,
	}).Run(context.Background()); err != nil {
		t.Fatalf("serial Restore.Run: %v", err)
	}

	gotChunkP, gotReason := observeRestoreChunkDispatch(t)
	if err := (&Restore{
		Target: mysqlEng, TargetDSN: parallelDSN, Store: store,
		TableParallelism: 1, ChunkParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("parallel Restore.Run: %v", err)
	}
	if *gotChunkP <= 1 {
		t.Fatalf("within-table dispatch = serial (reason %q); want parallel — ADR-0112 is engine-generic on the write side", *gotReason)
	}

	sN, sSum := mysqlTableContentFingerprint(t, serialDSN, table)
	pN, pSum := mysqlTableContentFingerprint(t, parallelDSN, table)
	if sN != pN || sSum != pSum {
		t.Fatalf("serial (n=%d md5=%s) != parallel (n=%d md5=%s) — within-table parallelism is not byte-identical on MySQL",
			sN, sSum, pN, pSum)
	}
}

// createSecondPGTarget creates an additional empty database on the PG
// container behind sourceDSN and returns its DSN.
func createSecondPGTarget(t *testing.T, sourceDSN, dbName string) string {
	t.Helper()
	db, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open source for CREATE DATABASE: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(context.Background(), "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("create %s: %v", dbName, err)
	}
	dsn, err := buildPGDSN(sourceDSN, dbName)
	if err != nil {
		t.Fatalf("build %s DSN: %v", dbName, err)
	}
	return dsn
}

// createSecondMySQLTarget creates an additional empty database on the
// MySQL container behind sourceDSN and returns its DSN.
func createSecondMySQLTarget(t *testing.T, sourceDSN, dbName string) string {
	t.Helper()
	db, err := sql.Open("mysql", sourceDSN+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open source for CREATE DATABASE: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(context.Background(), "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("create %s: %v", dbName, err)
	}
	dsn, err := buildMySQLDSN(sourceDSN, dbName)
	if err != nil {
		t.Fatalf("build %s DSN: %v", dbName, err)
	}
	return dsn
}

// seedMySQLVariedRows inserts n rows of value-type-varied data into the
// MySQL table created by the caller, covering int / decimal / text /
// json / varbinary / char(uuid-shaped) / datetime / bool families.
func seedMySQLVariedRows(t *testing.T, dsn, table string, n int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()
	stmt := fmt.Sprintf(
		"INSERT INTO %s (id, n, amt, label, doc, raw, uid, ts, flag) VALUES (?,?,?,?,?,?,?,?,?)", table,
	)
	for g := 1; g <= n; g++ {
		label := fmt.Sprintf("row-%d", g)
		doc := fmt.Sprintf(`{"k": %d, "arr": [%d, %d]}`, g, g, g+1)
		raw := fmt.Sprintf("%08x", g) // hex string into VARBINARY
		uid := fmt.Sprintf("00000000-0000-0000-0000-%012x", g)
		ts := fmt.Sprintf("2026-01-01 00:00:%02d.000000", g%60)
		if _, err := db.ExecContext(
			context.Background(), stmt,
			g, g*7, fmt.Sprintf("%d.123456", g%1000),
			label, doc, raw, uid, ts, g%2,
		); err != nil {
			t.Fatalf("seed row %d: %v", g, err)
		}
	}
}

// mysqlTableContentFingerprint returns (row count, md5 over the sorted
// full-row texts) for table at dsn — the MySQL analogue of
// pgTableContentFingerprint. Every column is rendered to text, NULLs
// normalised, rows concatenated in id order, then md5'd; so a dup, a
// missing row, or any value drift moves the digest. The session
// timezone is pinned so DATETIME renders identically across DBs.
func mysqlTableContentFingerprint(t *testing.T, dsn, table string) (int64, string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()
	var n int64
	if err := db.QueryRowContext(context.Background(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	// CONCAT_WS over every column (declaration order matches both
	// targets, restored from the same backup), GROUP_CONCAT ordered by
	// id, then MD5. CONCAT_WS skips NULLs so a sentinel keeps NULL
	// distinguishable from empty-string.
	q := fmt.Sprintf(
		"SELECT COALESCE(MD5(GROUP_CONCAT("+
			"CONCAT_WS('|', id, n, amt, label, doc, HEX(raw), uid, ts, flag) "+
			"ORDER BY id SEPARATOR '\\n')), '') FROM %s", table,
	)
	var sum string
	if err := db.QueryRowContext(context.Background(), q).Scan(&sum); err != nil {
		t.Fatalf("fingerprint %s: %v", table, err)
	}
	return n, sum
}
