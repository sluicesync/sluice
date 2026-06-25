//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0119 end-to-end pin: native-MySQL → {MySQL, PG} `sync start` cold-start
// with copy_table_parallelism=2 and intra-table PK-range work-stealing engaged.
// A SKEWED corpus (a few LARGE tables above the within-table threshold + several
// small) is copied; the test asserts:
//
//   - the LARGE tables were split into MULTIPLE chunks (the tail reclaimed —
//     observed via the [intraTableChunkObserver] seam, not timing), across the
//     PK-family matrix (integer / single-keyset varchar / composite);
//   - the small tables stayed WHOLE (one work item);
//   - every table lands at its EXACT source count with a matching value-sensitive
//     checksum (no gap/dup across the chunked + W×D writers — the exactly-once
//     silent-loss surface, ADR-0119 Decision 5);
//   - --no-intra-table-stealing forces every table whole (the opt-out);
//   - the same holds native-MySQL → Postgres (cross-engine write side).
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags=integration -v -count=1 -timeout=25m \
//	  -run 'TestStreamer_ChunkedWorkStealing' ./internal/pipeline/...

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// chunkObserverRecorder installs the intra-table chunk observer and records the
// max chunk count seen per table (build-time, single-threaded). Returns a
// snapshot getter and a restore func.
func installChunkObserver() (get func(string) int, restore func()) {
	var mu sync.Mutex
	counts := map[string]int{}
	prev := intraTableChunkObserver
	intraTableChunkObserver = func(table string, chunks int) {
		mu.Lock()
		if chunks > counts[table] {
			counts[table] = chunks
		}
		mu.Unlock()
	}
	return func(tbl string) int {
			mu.Lock()
			defer mu.Unlock()
			return counts[tbl]
		}, func() {
			intraTableChunkObserver = prev
		}
}

// seedRowsMySQL bulk-inserts n rows via batched multi-row INSERTs (fast enough
// to cross the within-table chunk threshold without a slow per-row loop).
func seedRowsMySQL(t *testing.T, dsn, insertPrefix string, n, batch int, tuple func(i int) string) {
	t.Helper()
	for start := 0; start < n; start += batch {
		end := start + batch
		if end > n {
			end = n
		}
		var sb strings.Builder
		sb.WriteString(insertPrefix)
		sb.WriteString(" VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteByte(',')
			}
			sb.WriteString(tuple(i))
		}
		sb.WriteByte(';')
		applyMySQLDDL(t, dsn, sb.String())
	}
}

// crcChecksumMySQL is an order-independent value-sensitive checksum over a
// table: SUM of CRC32 of a per-row CONCAT expression.
func crcChecksumMySQL(t *testing.T, dsn, table, concatExpr string) int64 {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("mysql open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var n int64
	q := fmt.Sprintf("SELECT COALESCE(SUM(CRC32(%s)), 0) FROM %s", concatExpr, table)
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("mysql checksum %q: %v", table, err)
	}
	return n
}

// TestStreamer_ChunkedWorkStealing_MySQL is the headline ADR-0119 pin:
// native-MySQL → MySQL cold-copy of a skewed corpus with intra-table stealing,
// across the integer / varchar-keyset / composite-keyset PK families.
func TestStreamer_ChunkedWorkStealing_MySQL(t *testing.T) {
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	src, tgt, cleanup := startMySQL(t)
	defer cleanup()

	// Three LARGE chunk-eligible tables (one per PK family) + five small tables.
	// Eight tables → the auto threshold floors at 10_000; 30_000 rows clears it
	// with comfortable margin against the information_schema estimate undershoot.
	const bigN = 30_000
	applyMySQLDDL(t, src,
		"CREATE TABLE ws_int  (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id));"+
			"CREATE TABLE ws_str  (id VARCHAR(32) NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id));"+
			"CREATE TABLE ws_comp (a BIGINT NOT NULL, b VARCHAR(32) NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (a, b));")
	for _, s := range []string{"sm_a", "sm_b", "sm_c", "sm_d", "sm_e"} {
		applyMySQLDDL(t, src, fmt.Sprintf("CREATE TABLE %s (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id));", s))
	}

	seedRowsMySQL(t, src, "INSERT INTO ws_int (id, v)", bigN, 1000, func(i int) string {
		return fmt.Sprintf("(%d, 'i-%d')", i, i)
	})
	seedRowsMySQL(t, src, "INSERT INTO ws_str (id, v)", bigN, 1000, func(i int) string {
		return fmt.Sprintf("('k%07d', 's-%d')", i, i)
	})
	seedRowsMySQL(t, src, "INSERT INTO ws_comp (a, b, v)", bigN, 1000, func(i int) string {
		return fmt.Sprintf("(%d, 'b%07d', 'c-%d')", i%100, i, i)
	})
	const smallN = 25
	for _, s := range []string{"sm_a", "sm_b", "sm_c", "sm_d", "sm_e"} {
		seedRowsMySQL(t, src, fmt.Sprintf("INSERT INTO %s (id, v)", s), smallN, 100, func(i int) string {
			return fmt.Sprintf("(%d, '%s-%d')", i, s, i)
		})
	}
	// Refresh the information_schema row estimate so CountRows clears the
	// threshold for the large tables (InnoDB updates TABLE_ROWS lazily).
	applyMySQLDDL(t, src, "ANALYZE TABLE ws_int, ws_str, ws_comp, sm_a, sm_b, sm_c, sm_d, sm_e;")

	getChunks, restoreObs := installChunkObserver()
	defer restoreObs()

	streamer := &Streamer{
		Source:           mysqlEng,
		Target:           mysqlEng,
		SourceDSN:        src + "&copy_table_parallelism=2",
		TargetDSN:        tgt,
		StreamID:         "chunked-ws-mysql",
		CopyFanoutDegree: 4,
		// NoIntraTableStealing defaults false → intra-table stealing ON.
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	bigTables := map[string]int{"ws_int": bigN, "ws_str": bigN, "ws_comp": bigN}
	for tbl, want := range bigTables {
		if !waitForExactRowCountMySQL(tgt, tbl, want, 180*time.Second) {
			t.Fatalf("large table %q never reached %d rows (got %d) — gap/dup across chunks?",
				tbl, want, pollRowCountMySQL(tgt, tbl))
		}
	}
	for _, s := range []string{"sm_a", "sm_b", "sm_c", "sm_d", "sm_e"} {
		if !waitForExactRowCountMySQL(tgt, s, smallN, 60*time.Second) {
			t.Fatalf("small table %q never reached %d rows (got %d)", s, smallN, pollRowCountMySQL(tgt, s))
		}
	}

	// Value-sensitive checksums (a silent swap/overwrite that preserves count
	// would slip a count-only check).
	checks := map[string]string{
		"ws_int":  "CONCAT(id,'|',v)",
		"ws_str":  "CONCAT(id,'|',v)",
		"ws_comp": "CONCAT(a,'|',b,'|',v)",
	}
	for tbl, expr := range checks {
		if s, d := crcChecksumMySQL(t, src, tbl, expr), crcChecksumMySQL(t, tgt, tbl, expr); s != d {
			t.Errorf("table %q checksum mismatch: src=%d tgt=%d (chunked exactly-once violated)", tbl, s, d)
		}
	}

	// Chunking ENGAGED on every large table (the tail reclaimed); small tables
	// stayed whole.
	for _, tbl := range []string{"ws_int", "ws_str", "ws_comp"} {
		if got := getChunks(tbl); got < 2 {
			t.Errorf("large table %q split into %d chunks; want ≥2 (intra-table stealing did not engage for this PK family)", tbl, got)
		}
	}
	for _, s := range []string{"sm_a", "sm_b", "sm_c", "sm_d", "sm_e"} {
		if got := getChunks(s); got != 1 {
			t.Errorf("small table %q split into %d chunks; want 1 (sub-threshold tables must stay whole)", s, got)
		}
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_ChunkedWorkStealing_OptOut_MySQL pins the --no-intra-table-stealing
// opt-out: a large chunk-eligible table is copied WHOLE (one work item) and still
// lands exactly once.
func TestStreamer_ChunkedWorkStealing_OptOut_MySQL(t *testing.T) {
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	src, tgt, cleanup := startMySQL(t)
	defer cleanup()

	const bigN = 30_000
	applyMySQLDDL(t, src,
		"CREATE TABLE oo_big (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id));"+
			"CREATE TABLE oo_x (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id));")
	seedRowsMySQL(t, src, "INSERT INTO oo_big (id, v)", bigN, 1000, func(i int) string {
		return fmt.Sprintf("(%d, 'o-%d')", i, i)
	})
	seedRowsMySQL(t, src, "INSERT INTO oo_x (id, v)", 10, 10, func(i int) string {
		return fmt.Sprintf("(%d, 'x-%d')", i, i)
	})
	applyMySQLDDL(t, src, "ANALYZE TABLE oo_big, oo_x;")

	getChunks, restoreObs := installChunkObserver()
	defer restoreObs()

	streamer := &Streamer{
		Source:               mysqlEng,
		Target:               mysqlEng,
		SourceDSN:            src + "&copy_table_parallelism=2",
		TargetDSN:            tgt,
		StreamID:             "chunked-ws-optout",
		CopyFanoutDegree:     4,
		NoIntraTableStealing: true, // the opt-out: everything stays whole.
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForExactRowCountMySQL(tgt, "oo_big", bigN, 180*time.Second) {
		t.Fatalf("oo_big never reached %d rows (got %d) under opt-out", bigN, pollRowCountMySQL(tgt, "oo_big"))
	}
	if got := crcChecksumMySQL(t, src, "oo_big", "CONCAT(id,'|',v)"); got != crcChecksumMySQL(t, tgt, "oo_big", "CONCAT(id,'|',v)") {
		t.Errorf("oo_big checksum mismatch under opt-out (exactly-once violated)")
	}
	// The opt-out forces ONE whole-table item even for the large eligible table.
	if got := getChunks("oo_big"); got != 1 {
		t.Errorf("oo_big split into %d items under --no-intra-table-stealing; want 1 (opt-out ignored)", got)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_ChunkedWorkStealing_MySQLToPostgres pins the cross-engine write
// side: native-MySQL → Postgres cold-copy of a large integer-PK table that
// chunks, landing exactly once on PG (count + SUM(id)/MAX(id) congruent).
func TestStreamer_ChunkedWorkStealing_MySQLToPostgres(t *testing.T) {
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	srcDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const bigN = 30_000
	applyMySQLDDL(t, srcDSN,
		"CREATE TABLE wp_int (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id));"+
			"CREATE TABLE wp_a (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id));"+
			"CREATE TABLE wp_b (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id));")
	seedRowsMySQL(t, srcDSN, "INSERT INTO wp_int (id, v)", bigN, 1000, func(i int) string {
		return fmt.Sprintf("(%d, 'p-%d')", i, i)
	})
	for _, s := range []string{"wp_a", "wp_b"} {
		seedRowsMySQL(t, srcDSN, fmt.Sprintf("INSERT INTO %s (id, v)", s), 15, 15, func(i int) string {
			return fmt.Sprintf("(%d, '%s-%d')", i, s, i)
		})
	}
	applyMySQLDDL(t, srcDSN, "ANALYZE TABLE wp_int, wp_a, wp_b;")

	getChunks, restoreObs := installChunkObserver()
	defer restoreObs()

	streamer := &Streamer{
		Source:           mysqlEng,
		Target:           pgEng,
		SourceDSN:        srcDSN + "&copy_table_parallelism=2",
		TargetDSN:        pgDSN,
		StreamID:         "chunked-ws-mysql-pg",
		CopyFanoutDegree: 4,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForExactRowCountPGChunk(pgDSN, "wp_int", bigN, 180*time.Second) {
		t.Fatalf("wp_int never reached %d rows on PG (got %d) — gap/dup across chunks?", bigN, pollRowCountPGQuoted(pgDSN, "wp_int"))
	}
	for _, s := range []string{"wp_a", "wp_b"} {
		if !waitForExactRowCountPGChunk(pgDSN, s, 15, 60*time.Second) {
			t.Fatalf("small table %q never reached 15 rows on PG (got %d)", s, pollRowCountPGQuoted(pgDSN, s))
		}
	}

	// Congruence on the chunked integer table: SUM(id) + MAX(id) must match the
	// source exactly (a dropped/duped row shifts SUM, a truncated chunk shifts
	// MAX). A family-portable value-sensitive check across the engine boundary.
	srcSum, srcMax := mysqlIntAgg(t, srcDSN, "wp_int")
	dstSum, dstMax := pgIntAgg(t, pgDSN, "wp_int")
	if srcSum != dstSum || srcMax != dstMax {
		t.Errorf("wp_int aggregate mismatch: src(sum=%d,max=%d) dst(sum=%d,max=%d) (chunked exactly-once violated cross-engine)",
			srcSum, srcMax, dstSum, dstMax)
	}
	if got := getChunks("wp_int"); got < 2 {
		t.Errorf("wp_int split into %d chunks; want ≥2 (intra-table stealing did not engage)", got)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

func waitForExactRowCountPGChunk(dsn, table string, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollRowCountPGQuoted(dsn, table) == want {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return pollRowCountPGQuoted(dsn, table) == want
}

func mysqlIntAgg(t *testing.T, dsn, table string) (sumID, maxID int64) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("mysql open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COALESCE(SUM(id),0), COALESCE(MAX(id),0) FROM %s", table)).Scan(&sumID, &maxID); err != nil {
		t.Fatalf("mysql agg %q: %v", table, err)
	}
	return sumID, maxID
}

func pgIntAgg(t *testing.T, dsn, table string) (sumID, maxID int64) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("pg open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COALESCE(SUM(id),0), COALESCE(MAX(id),0) FROM %q`, table)).Scan(&sumID, &maxID); err != nil {
		t.Fatalf("pg agg %q: %v", table, err)
	}
	return sumID, maxID
}
