//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Within-table parallel-copy chunking for the SQLite file source (ADR-0128
// #3): a binary `.db` SQLite source with tables large enough to chunk is
// migrated into Postgres with bulk-parallelism > 1 and a low min-rows
// threshold, so the orchestrator splits each table into N keyset/range
// chunks copied concurrently. The pin is END-TO-END exactly-once: row count
// AND content exact, no dupes, no drops — across an integer PK (MIN/MAX/
// divide), a mixed-case TEXT PK (the keyset collation silent-loss guard),
// and a composite PK (row-value comparison). A `.sql`-dump source is
// confirmed to still migrate on the single-reader path (unchanged).

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver for seeding the temp source file

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// chunkSourceRows is the per-table row count for the chunking source. Well
// above the test's BulkParallelMinRows (100) so every table chunks, but
// small enough to seed and migrate quickly.
const chunkSourceRows = 2000

// seedSQLiteChunkSource writes a temp SQLite `.db` with four PK families,
// each chunkSourceRows rows:
//
//   - wide_int: single INTEGER PK (id 1..N), v = id*2 → MIN/MAX/divide.
//   - wide_text: single TEXT PK with ALTERNATING first-letter case (so the
//     BINARY byte order differs from any case-folded order — the keyset
//     collation guard), n = the ordinal payload.
//   - wide_comp: composite (g, k) PK, payload = g*100000 + j → keyset
//     row-value comparison.
//   - wide_ts: single DATETIME PK (the temporal silent-loss guard) — this
//     table is ABOVE the chunk threshold but its PK decodes to a time.Time
//     that can't round-trip a `>` cursor, so it MUST be disqualified and
//     copied whole-table single-reader (NOT chunked); n = the ordinal.
//
// Returns the file path plus the expected SUM of each table's payload so the
// content compare catches a corrupted/duplicated/dropped row, not just a
// count mismatch.
func seedSQLiteChunkSource(t *testing.T) (path string, wantIntSum, wantTextSum, wantCompSum, wantTsSum int64) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "chunk.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()

	ddl := []string{
		`CREATE TABLE wide_int (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)`,
		`CREATE TABLE wide_text (k TEXT PRIMARY KEY, n INTEGER NOT NULL)`,
		`CREATE TABLE wide_comp (g INTEGER NOT NULL, k TEXT NOT NULL, payload INTEGER NOT NULL, PRIMARY KEY (g, k))`,
		`CREATE TABLE wide_ts (k DATETIME PRIMARY KEY, n INTEGER NOT NULL)`,
	}
	for _, s := range ddl {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed ddl: %v", err)
		}
	}

	// Batched multi-row INSERTs (literal values, no binds) for speed.
	const batch = 200
	flush := func(table, cols string, vals []string) {
		if len(vals) == 0 {
			return
		}
		stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, cols, strings.Join(vals, ","))
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("seed insert %s: %v", table, err)
		}
	}

	// wide_int.
	var vals []string
	for i := 1; i <= chunkSourceRows; i++ {
		vals = append(vals, fmt.Sprintf("(%d,%d)", i, i*2))
		wantIntSum += int64(i)
		if len(vals) == batch {
			flush("wide_int", "id,v", vals)
			vals = vals[:0]
		}
	}
	flush("wide_int", "id,v", vals)
	vals = vals[:0]

	// wide_text: alternating case prefix; keys distinct under BINARY.
	for i := 0; i < chunkSourceRows; i++ {
		prefix := "key"
		if i%2 == 0 {
			prefix = "KEY"
		}
		vals = append(vals, fmt.Sprintf("('%s_%06d',%d)", prefix, i, i))
		wantTextSum += int64(i)
		if len(vals) == batch {
			flush("wide_text", "k,n", vals)
			vals = vals[:0]
		}
	}
	flush("wide_text", "k,n", vals)
	vals = vals[:0]

	// wide_comp: g in 1..G, j in 0..rows/G-1.
	const groups = 10
	per := chunkSourceRows / groups
	for g := 1; g <= groups; g++ {
		for j := 0; j < per; j++ {
			payload := int64(g*100000 + j)
			vals = append(vals, fmt.Sprintf("(%d,'tag_%06d',%d)", g, j, payload))
			wantCompSum += payload
			if len(vals) == batch {
				flush("wide_comp", "g,k,payload", vals)
				vals = vals[:0]
			}
		}
	}
	flush("wide_comp", "g,k,payload", vals)
	vals = vals[:0]

	// wide_ts: a DATETIME PK stored as INTEGER unix-epoch seconds (the source
	// DSN selects sqlite_date_encoding=unixepoch). This is the CATASTROPHIC
	// case the value-fidelity review flagged: had the table chunked, the
	// intra-chunk cursor would bind the decoded time.Time as TEXT, SQLite ranks
	// numeric < text so `k > <text-cursor>` is FALSE for every numeric row, the
	// 2nd page of each chunk comes back EMPTY, and the chunk silently truncates
	// to one page. The fix disqualifies the temporal PK so the table is copied
	// whole-table single-reader instead — this fixture would FAIL the
	// exactly-once assertion below if that disqualification regressed.
	tsBase := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	for i := 0; i < chunkSourceRows; i++ {
		vals = append(vals, fmt.Sprintf("(%d,%d)", tsBase+int64(i)*60, i))
		wantTsSum += int64(i)
		if len(vals) == batch {
			flush("wide_ts", "k,n", vals)
			vals = vals[:0]
		}
	}
	flush("wide_ts", "k,n", vals)

	return path, wantIntSum, wantTextSum, wantCompSum, wantTsSum
}

// TestMigrate_SQLiteChunkedToPostgres is the end-to-end exactly-once pin for
// within-table chunking. The config forces MULTI-PAGE chunks (chunks larger
// than the per-page batch limit) so the intra-chunk cursor advance — the
// silent-loss surface the value-fidelity review flagged — is actually
// exercised, not just a single page per chunk. It also asserts (via the
// test-only chunk lifecycle observer) that the round-trippable tables chunked
// while the temporal-PK table did NOT (it must fall back to single-reader).
func TestMigrate_SQLiteChunkedToPostgres(t *testing.T) {
	src, wantIntSum, wantTextSum, wantCompSum, wantTsSum := seedSQLiteChunkSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	// Observe chunk lifecycle: record the max chunk index seen per table so
	// we can prove chunking fired (chunkIndex >= 1 means > 1 chunk). The
	// observer is a global test seam; reset it on exit.
	var (
		obsMu    sync.Mutex
		maxChunk = map[string]int{}
		prevObs  = copyChunkLifecycleObserver
	)
	copyChunkLifecycleObserver = func(table string, chunkIndex int, active bool) {
		if !active {
			return
		}
		obsMu.Lock()
		if chunkIndex > maxChunk[table] {
			maxChunk[table] = chunkIndex
		}
		obsMu.Unlock()
	}
	defer func() { copyChunkLifecycleObserver = prevObs }()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// BulkParallelMinRows=600 with 2000-row tables → ceil(2000/600) floored at
	// parallelism = 4 chunks of ~500 rows; BulkBatchSize=200 → ~3 pages per
	// chunk, so the intra-chunk cursor advance is exercised across pages. The
	// 2000-row wide_ts is also above the 600 threshold, so the ONLY reason it
	// can stay single-reader is the temporal-PK disqualification — exactly
	// what we assert below.
	mig := &Migrator{
		Source:    sqliteEng,
		Target:    pgEng,
		SourceDSN: src + "?sqlite_date_encoding=unixepoch", // wide_ts stores unix-epoch ints
		TargetDSN: pgTarget,

		BulkParallelism:     4,
		BulkParallelMinRows: 600,
		BulkBatchSize:       200,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite chunked → PG): %v", err)
	}

	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := ctx2min(t)

	// Exactly-once = count == distinct-PK count == expected, AND the payload
	// SUM matches (so a corrupted/duplicated value, not just a count slip,
	// fails). distinct-PK count guards against a boundary-straddling dup;
	// count==expected guards against a drop. wide_ts proves a temporal-PK
	// table copies exactly-once on the single-reader fallback (the multi-page
	// cursor bug would have truncated/duped it had it taken the chunk path).
	assertChunkExactlyOnce(t, ctx, db, "wide_int", "id", "id", chunkSourceRows, wantIntSum)
	assertChunkExactlyOnce(t, ctx, db, "wide_text", "k", "n", chunkSourceRows, wantTextSum)
	assertChunkExactlyOnce(t, ctx, db, "wide_comp", "g||':'||k", "payload", chunkSourceRows, wantCompSum)
	assertChunkExactlyOnce(t, ctx, db, "wide_ts", "k", "n", chunkSourceRows, wantTsSum)

	obsMu.Lock()
	defer obsMu.Unlock()
	// Round-trippable tables chunked (proves the multi-page chunk path ran).
	for _, tbl := range []string{"wide_int", "wide_text", "wide_comp"} {
		if maxChunk[tbl] < 1 {
			t.Errorf("table %q did not chunk (max chunk index = %d); the within-table chunk path did not run", tbl, maxChunk[tbl])
		}
	}
	// The temporal-PK table must NOT have chunked (copyChunk never ran for it).
	if mx, seen := maxChunk["wide_ts"]; seen {
		t.Errorf("temporal-PK table wide_ts chunked (max chunk index = %d); it must fall back to single-reader (the decoded time.Time cursor can't round-trip)", mx)
	}
}

// assertExactlyOnce checks, on the TARGET, that table holds exactly wantCount
// rows, that the distinct count of pkExpr equals wantCount (no dup landed on
// a chunk boundary), and that SUM(sumCol) equals wantSum (content exact — a
// corrupted/duplicated value shifts the sum).
func assertChunkExactlyOnce(t *testing.T, ctx context.Context, db *sql.DB, table, pkExpr, sumCol string, wantCount int, wantSum int64) {
	t.Helper()
	var n, distinct int
	var sum int64
	if err := db.QueryRowContext(
		ctx,
		fmt.Sprintf("SELECT count(*), count(DISTINCT %s), COALESCE(SUM(%s),0) FROM %s", pkExpr, sumCol, table),
	).Scan(&n, &distinct, &sum); err != nil {
		t.Fatalf("aggregate %s: %v", table, err)
	}
	if n != wantCount {
		t.Errorf("%s count = %d; want %d (drop)", table, n, wantCount)
	}
	if distinct != wantCount {
		t.Errorf("%s distinct-PK = %d; want %d (boundary dup)", table, distinct, wantCount)
	}
	if sum != wantSum {
		t.Errorf("%s SUM(%s) = %d; want %d (content corruption)", table, sumCol, sum, wantSum)
	}
}

// TestMigrate_SQLiteDumpStaysSingleReader confirms a `.sql`-dump source still
// migrates correctly under bulk-parallelism > 1: the dump path is routed to
// the single-reader copy (per-chunk readers would re-materialize the dump),
// and NO table chunks. Reuses the D1-shaped dump from the sibling test.
func TestMigrate_SQLiteDumpStaysSingleReader(t *testing.T) {
	src := seedSQLiteDumpSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	var (
		obsMu    sync.Mutex
		anyChunk bool
		prevObs  = copyChunkLifecycleObserver
	)
	copyChunkLifecycleObserver = func(_ string, chunkIndex int, active bool) {
		if active && chunkIndex >= 1 {
			obsMu.Lock()
			anyChunk = true
			obsMu.Unlock()
		}
	}
	defer func() { copyChunkLifecycleObserver = prevObs }()

	sqliteEng, _ := engines.Get("sqlite")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{
		Source:              sqliteEng,
		Target:              pgEng,
		SourceDSN:           src,
		TargetDSN:           pgTarget,
		BulkParallelism:     4,
		BulkParallelMinRows: 1, // would chunk if the dump path allowed it
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite dump → PG, parallel): %v", err)
	}

	// Content still correct (the same round-trip the non-parallel dump test
	// asserts).
	assertSQLiteRoundTrip(t, pgEng, pgTarget)

	obsMu.Lock()
	defer obsMu.Unlock()
	if anyChunk {
		t.Error("a `.sql`-dump table chunked; dump sources must stay single-reader (per-chunk readers would re-materialize the dump)")
	}
}
