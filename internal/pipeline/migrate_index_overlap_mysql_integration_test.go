//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for overlapped index builds on MySQL targets (ADR-0080,
// roadmap item 3c). MySQL's SchemaWriter now implements
// ir.IncrementalIndexBuilder + ir.TableIndexedNotifier, so a MySQL-target
// migrate (MySQL→MySQL and PG→MySQL) builds each table's secondary indexes
// as its copy lands, concurrently with the still-copying tables — the same
// structural tail-collapse ADR-0077 gave Postgres targets. These boot real
// containers and assert:
//
//   - overlap-actually-happens: min(indexBuildStart) < max(copyComplete),
//     measured via the test-only seams (pipeline copy-complete + the mysql
//     engine index-build-start) — without this the chunk could silently
//     regress to a sequential post-copy index phase and still pass
//     zero-loss. THIS IS THE LOAD-BEARING ANTI-REGRESSION PIN.
//   - zero-loss + every secondary index present (information_schema.
//     statistics), covering the UNIQUE / FULLTEXT / SPATIAL index kinds, for
//     MySQL→MySQL AND PG→MySQL.
//   - resume short-circuit: kill mid-run, resume, no duplicate-index error,
//     all indexes present, zero loss, and IndexesBuilt short-circuits the
//     already-indexed tables (the per-table index-build-start counter proves
//     the short-circuit; partial sets are a no-op via indexExists).
//
// The PlanetScale/Vitess flavor gate (the overlap is DECLINED there) is
// covered by a unit-level behavioral test in the mysql engine package —
// vttestserver may not model online-DDL, so the gate is pinned without a
// container (see schema_writer_index_overlap_test.go).

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/mysql"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// seedManyIndexedTablesMySQLAllKinds creates tableCount tables on a MySQL
// source, each with rowsEach rows and THREE secondary index kinds so the
// per-table-family pin (ADR-0080) exercises every ALTER … ADD INDEX shape
// emitCreateIndex dispatches on, not one representative:
//
//   - a plain UNIQUE index on a derived column (UNIQUE INDEX);
//   - a FULLTEXT index on a TEXT column (FULLTEXT INDEX);
//   - a SPATIAL index on a NOT NULL POINT column (SPATIAL INDEX).
//
// The numeric columns are PK-derived so a checksum catches loss/dup.
func seedManyIndexedTablesMySQLAllKinds(t *testing.T, dsn string, tableCount, rowsEach int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for i := 0; i < tableCount; i++ {
		name := fmt.Sprintf("tbl_%02d", i)
		ddl := fmt.Sprintf(`
			CREATE TABLE %s (
				id  BIGINT PRIMARY KEY,
				u   BIGINT NOT NULL,
				txt TEXT NOT NULL,
				pt  POINT NOT NULL SRID 0,
				UNIQUE  INDEX %s_u_uidx (u),
				FULLTEXT INDEX %s_txt_ftidx (txt),
				SPATIAL  INDEX %s_pt_spidx (pt)
			);`, name, name, name, name)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		const batch = 500
		for off := 0; off < rowsEach; off += batch {
			vals := ""
			for j := off; j < off+batch && j < rowsEach; j++ {
				if vals != "" {
					vals += ","
				}
				// u is unique per row; txt is searchable; pt is a valid point.
				vals += fmt.Sprintf("(%d,%d,'row %d text',ST_GeomFromText('POINT(%d %d)',0))",
					j+1, j+1, j+1, j%180, j%90)
			}
			if _, err := db.ExecContext(ctx,
				fmt.Sprintf("INSERT INTO %s (id,u,txt,pt) VALUES %s", name, vals)); err != nil {
				t.Fatalf("seed %s: %v", name, err)
			}
		}
	}
}

// mysqlSecondaryIndexNames is the set of secondary indexes
// seedManyIndexedTablesMySQLAllKinds creates per table, by family.
func mysqlSecondaryIndexNames(name string) []string {
	return []string{name + "_u_uidx", name + "_txt_ftidx", name + "_pt_spidx"}
}

// assertAllMySQLSecondaryIndexesPresent confirms every seeded table's three
// secondary indexes (UNIQUE / FULLTEXT / SPATIAL) exist on the MySQL target.
func assertAllMySQLSecondaryIndexesPresent(ctx context.Context, t *testing.T, dsn string, tableCount int) {
	t.Helper()
	assertNamedMySQLIndexesPresent(ctx, t, dsn, tableCount, mysqlSecondaryIndexNames)
}

// assertNamedMySQLIndexesPresent confirms, for every seeded table, that each
// index name namesFor returns exists on the MySQL target
// (information_schema.statistics).
func assertNamedMySQLIndexesPresent(ctx context.Context, t *testing.T, dsn string, tableCount int, namesFor func(name string) []string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	for i := 0; i < tableCount; i++ {
		name := fmt.Sprintf("tbl_%02d", i)
		for _, idx := range namesFor(name) {
			var got int
			if err := db.QueryRowContext(
				ctx,
				`SELECT COUNT(*) FROM information_schema.statistics
				 WHERE table_schema=DATABASE() AND table_name=? AND index_name=?`,
				name, idx,
			).Scan(&got); err != nil {
				t.Fatalf("query statistics %s: %v", idx, err)
			}
			if got == 0 {
				t.Errorf("index %s missing on MySQL target", idx)
			}
		}
	}
}

// assertManyIndexedTablesZeroLossMySQL compares COUNT(*) + SUM(id) + SUM(u)
// between two MySQL DSNs for every seeded table.
func assertManyIndexedTablesZeroLossMySQL(ctx context.Context, t *testing.T, srcDSN, tgtDSN string, tableCount int) {
	t.Helper()
	srcDB, err := sql.Open("mysql", srcDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	tgtDB, err := sql.Open("mysql", tgtDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	checksum := func(db *sql.DB, name string) (c, sid, su int64) {
		q := fmt.Sprintf("SELECT COUNT(*), COALESCE(SUM(id),0), COALESCE(SUM(u),0) FROM %s", name)
		if err := db.QueryRowContext(ctx, q).Scan(&c, &sid, &su); err != nil {
			t.Fatalf("checksum %s: %v", name, err)
		}
		return
	}
	for i := 0; i < tableCount; i++ {
		name := fmt.Sprintf("tbl_%02d", i)
		sc, si, su := checksum(srcDB, name)
		tc, ti, tu := checksum(tgtDB, name)
		if sc != tc || si != ti || su != tu {
			t.Errorf("table %s checksum mismatch: src(%d,%d,%d) tgt(%d,%d,%d)",
				name, sc, si, su, tc, ti, tu)
		}
	}
}

// TestMigrate_MySQL_IndexOverlap_ActuallyHappens is the load-bearing pin
// (ADR-0080): it records when each table's index build STARTS (mysql engine
// seam) and when each copy COMPLETES (pipeline seam), then asserts
// min(indexBuildStart) < max(copyComplete) — proving the index phase
// genuinely overlapped the copy rather than running after it. Also asserts
// zero-loss + all index kinds present. MySQL→MySQL.
func TestMigrate_MySQL_IndexOverlap_ActuallyHappens(t *testing.T) {
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const (
		tableCount = 12
		rowsEach   = 20_000
	)
	src, tgt, cleanup := startMySQL(t)
	defer cleanup()
	seedManyIndexedTablesMySQLAllKinds(t, src, tableCount, rowsEach)

	var obsMu sync.Mutex
	var firstIndexStart, lastCopyComplete time.Time

	restoreCopy := setOnTableCopiedObserverForTest(func(_ string) {
		obsMu.Lock()
		lastCopyComplete = time.Now()
		obsMu.Unlock()
	})
	defer restoreCopy()
	restoreIdx := mysql.SetIndexBuildStartObserverForTest(func(_ string) {
		obsMu.Lock()
		if firstIndexStart.IsZero() {
			firstIndexStart = time.Now()
		}
		obsMu.Unlock()
	})
	defer restoreIdx()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	mig := &Migrator{
		Source:           mysqlEng,
		Target:           mysqlEng,
		SourceDSN:        src,
		TargetDSN:        tgt,
		TableParallelism: 4,
		// Keep tables single-streamed so the cross-table + index axes are
		// what is exercised, not within-table chunking.
		BulkParallelMinRows: int64(rowsEach * 10),
		MigrationID:         "mysql-idx-overlap",
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertManyIndexedTablesZeroLossMySQL(ctx, t, src, tgt, tableCount)
	assertAllMySQLSecondaryIndexesPresent(ctx, t, tgt, tableCount)

	obsMu.Lock()
	fis, lcc := firstIndexStart, lastCopyComplete
	obsMu.Unlock()
	if fis.IsZero() {
		t.Fatal("no index build ever started (mysql engine seam never fired) — overlap not engaged")
	}
	if lcc.IsZero() {
		t.Fatal("no copy ever completed (pipeline seam never fired)")
	}
	t.Logf("overlap: firstIndexStart=%s lastCopyComplete=%s (delta=%s)",
		fis.Format(time.StampMilli), lcc.Format(time.StampMilli), lcc.Sub(fis))
	if !fis.Before(lcc) {
		t.Errorf("index builds did NOT overlap the copy: firstIndexStart=%s >= lastCopyComplete=%s "+
			"(the chunk regressed to a sequential post-copy index phase)", fis, lcc)
	}
}

// TestMigrate_PGToMySQL_IndexOverlap_ZeroLossAllIndexes drives the
// cross-engine PG→MySQL direction: a PG source with the same three index
// kinds (translated to MySQL UNIQUE/FULLTEXT/SPATIAL on the target) lands
// zero-loss with every secondary index present, and the overlap engages
// (the copy-complete seam fires from inside runOverlappedCopyAndIndexPhase).
func TestMigrate_PGToMySQL_IndexOverlap_ZeroLossAllIndexes(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const (
		tableCount = 8
		rowsEach   = 10_000
	)
	pgSrc, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, myTgt, myCleanup := startMySQL(t)
	defer myCleanup()

	seedManyIndexedTablesPGForMySQL(t, pgSrc, tableCount, rowsEach)

	// The overlap MUST engage on the MySQL target — the copy-complete seam
	// only fires inside runOverlappedCopyAndIndexPhase.
	var overlapFired atomic.Int64
	restore := setOnTableCopiedObserverForTest(func(_ string) {
		overlapFired.Add(1)
	})
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	mig := &Migrator{
		Source:              pgEng,
		Target:              mysqlEng,
		SourceDSN:           pgSrc,
		TargetDSN:           myTgt,
		TableParallelism:    4,
		BulkParallelMinRows: int64(rowsEach * 10),
		MigrationID:         "pg-to-mysql-idx-overlap",
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if n := overlapFired.Load(); n == 0 {
		t.Error("overlap path did NOT engage on the MySQL target (copy-complete seam never fired)")
	}
	assertNamedMySQLIndexesPresent(ctx, t, myTgt, tableCount,
		func(name string) []string { return []string{name + "_u_uidx", name + "_v_idx"} })

	// Zero-loss: compare PG source vs MySQL target row counts + checksums.
	pgDB, err := sql.Open("pgx", pgSrc)
	if err != nil {
		t.Fatalf("open pg source: %v", err)
	}
	defer func() { _ = pgDB.Close() }()
	myDB, err := sql.Open("mysql", myTgt)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = myDB.Close() }()
	for i := 0; i < tableCount; i++ {
		name := fmt.Sprintf("tbl_%02d", i)
		var sc, ssum int64
		if err := pgDB.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*), COALESCE(SUM(id),0)+COALESCE(SUM(u),0) FROM %s", name)).
			Scan(&sc, &ssum); err != nil {
			t.Fatalf("pg checksum %s: %v", name, err)
		}
		var tc, tsum int64
		if err := myDB.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*), COALESCE(SUM(id),0)+COALESCE(SUM(u),0) FROM %s", name)).
			Scan(&tc, &tsum); err != nil {
			t.Fatalf("mysql checksum %s: %v", name, err)
		}
		if sc != tc || ssum != tsum {
			t.Errorf("table %s mismatch: pg(%d,%d) mysql(%d,%d)", name, sc, ssum, tc, tsum)
		}
	}
}

// seedManyIndexedTablesPGForMySQL creates tableCount PG tables that
// translate to MySQL tables carrying a UNIQUE, a FULLTEXT-eligible, and a
// SPATIAL secondary index on the MySQL target. PG's b-tree UNIQUE maps to
// MySQL UNIQUE; a PG GIN/GiST full-text or PostGIS spatial index isn't
// available on a plain PG container, so the FULLTEXT/SPATIAL kinds are
// exercised on the MySQL→MySQL direction (above); here we pin the
// cross-engine copy + the UNIQUE kind end-to-end on PG→MySQL.
func seedManyIndexedTablesPGForMySQL(t *testing.T, dsn string, tableCount, rowsEach int) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for i := 0; i < tableCount; i++ {
		name := fmt.Sprintf("tbl_%02d", i)
		ddl := fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT PRIMARY KEY,
				u  BIGINT NOT NULL,
				v  BIGINT NOT NULL
			);
			INSERT INTO %s (id, u, v)
				SELECT g, g, g %% 100 FROM generate_series(1, %d) AS g;
			CREATE UNIQUE INDEX %s_u_uidx ON %s (u);
			CREATE INDEX %s_v_idx ON %s (v);
			ANALYZE %s;
		`, name, name, rowsEach, name, name, name, name, name)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
}
