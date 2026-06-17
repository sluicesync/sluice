//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the sampled-keyset within-table parallel-copy
// strategy (ADR-0096): non-integer and composite primary keys now get
// the same N-way fan-out integer PKs got in v0.5.0 (ADR-0019).
//
// These boot a real Postgres container, seed a source table above the
// parallelism threshold with a UUID PK (single non-integer) and a
// (tenant_id, seq) composite PK, run migration with --bulk-parallelism>1,
// and assert the EXACTLY-ONCE coverage property end-to-end:
//
//   - target COUNT(*) == source COUNT(*) (no missing rows), AND
//   - an order-independent content checksum matches source==target
//     (no duplicated or corrupted rows),
//   - the parallel path actually fanned out (>1 distinct chunk in logs),
//   - a non-orderable / no-PK table still copies correctly via the
//     single-reader fallback.
//
// This is the silent-loss-class surface the chunk-boundary math guards;
// the unit tests pin the partition arithmetic, these pin it against a
// real engine's ORDER BY / window-function semantics.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// sourceTargetChecksum returns an order-independent content checksum of
// a table: SUM over per-row md5 hashes of the row's text form. Equal
// checksums on source and target ⇒ identical row sets (modulo an
// astronomically unlikely md5 collision), independent of row order or
// which chunk copied which row.
func sourceTargetChecksum(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open for checksum: %v", err)
	}
	defer func() { _ = db.Close() }()
	var sum sql.NullString
	if err := db.QueryRow(query).Scan(&sum); err != nil {
		t.Fatalf("checksum query: %v", err)
	}
	return sum.String
}

func assertCountAndChecksum(t *testing.T, sourceDSN, targetDSN, table, checksumExpr string) {
	t.Helper()
	countQ := "SELECT COUNT(*)::text FROM " + table
	srcCount := sourceTargetChecksum(t, sourceDSN, countQ)
	tgtCount := sourceTargetChecksum(t, targetDSN, countQ)
	if srcCount != tgtCount {
		t.Errorf("%s: count source=%s target=%s", table, srcCount, tgtCount)
	}
	sumQ := fmt.Sprintf(
		"SELECT COALESCE(SUM(('x'||substr(md5(%s),1,16))::bit(64)::bigint),0)::text FROM %s",
		checksumExpr, table,
	)
	srcSum := sourceTargetChecksum(t, sourceDSN, sumQ)
	tgtSum := sourceTargetChecksum(t, targetDSN, sumQ)
	if srcSum != tgtSum {
		t.Errorf("%s: checksum source=%s target=%s (rows missing/duplicated/corrupted)", table, srcSum, tgtSum)
	}
}

func assertChunkFanout(t *testing.T, logs string, minChunks int) {
	t.Helper()
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		if strings.Contains(logs, fmt.Sprintf("chunk=%d", i)) {
			seen[fmt.Sprintf("chunk=%d", i)] = true
		}
	}
	if len(seen) < minChunks {
		t.Errorf("expected >= %d distinct chunks in logs; saw %d (%v)", minChunks, len(seen), seen)
	}
}

// TestMigrate_PG_KeysetCopy_UUIDPK seeds a UUID-PK table above the
// threshold and verifies parallel copy fans out and lands every row
// exactly once.
func TestMigrate_PG_KeysetCopy_UUIDPK(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 40_000
	// gen_random_uuid() needs pgcrypto on older PG; PG13+ has it built in
	// via the pgcrypto-free gen_random_uuid in core (PG13+). The test
	// images are PG16, so it is available.
	ddl := fmt.Sprintf(`
		CREATE TABLE tokens (
			id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			label TEXT NOT NULL
		);
		INSERT INTO tokens (label)
			SELECT 'row-' || g FROM generate_series(1, %d) AS g;
		ANALYZE tokens;
	`, rowCount)
	applyPGDDL(t, sourceDSN, ddl)

	pgEng, _ := engines.Get("postgres")
	logs := captureSlog(t)
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 10_000,
		MigrationID:         "test-keyset-uuid",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	assertChunkFanout(t, logs.String(), 2)
	assertCountAndChecksum(t, sourceDSN, targetDSN, "tokens", "id::text || '|' || label")
}

// TestMigrate_PG_KeysetCopy_CompositePK seeds a composite-PK table and
// verifies the keyset strategy's tuple boundaries partition it exactly
// once across parallel chunks.
func TestMigrate_PG_KeysetCopy_CompositePK(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 40_000
	// Skewed tenant distribution so equal-row-count keyset boundaries
	// land mid-tenant — exercises the second PK column in the boundary
	// tuple, not just the first.
	ddl := fmt.Sprintf(`
		CREATE TABLE memberships (
			tenant_id BIGINT NOT NULL,
			seq       BIGINT NOT NULL,
			payload   TEXT NOT NULL,
			PRIMARY KEY (tenant_id, seq)
		);
		INSERT INTO memberships (tenant_id, seq, payload)
			SELECT (g %% 5), g, 'p-' || g FROM generate_series(1, %d) AS g;
		ANALYZE memberships;
	`, rowCount)
	applyPGDDL(t, sourceDSN, ddl)

	pgEng, _ := engines.Get("postgres")
	logs := captureSlog(t)
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 10_000,
		MigrationID:         "test-keyset-composite",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	assertChunkFanout(t, logs.String(), 2)
	assertCountAndChecksum(t, sourceDSN, targetDSN, "memberships",
		"tenant_id::text || '|' || seq::text || '|' || payload")
}

// makeCollatedStringValues builds rowCount distinct string PK values that
// SPAN case, punctuation, and non-ASCII so that the DB's default collation
// (en_US.utf8 — case- and accent-aware, NOT byte order) DISAGREES with a
// naive bytewise comparison. Under the old Go-side bytewise upper-bound
// clip, boundary-straddling rows (e.g. a lowercase value just under an
// uppercase boundary) were excluded by BOTH the chunk above (Go: "past
// upper") and the chunk below (DB: "<= lower"), landing in NO chunk —
// silent permanent loss. The fix pushes the upper bound into SQL so it
// uses the SAME collation as ORDER BY; this generator makes the gap
// reproducible if the fix regresses.
func makeCollatedStringValues(n int) []string {
	// A small alphabet of code points whose collation order differs from
	// their byte order under en_US.utf8: mixed case (a<A is collation but
	// 'A'=0x41<'a'=0x61 byte), underscore/punctuation, and accented Latin.
	alphabet := []rune{'a', 'A', 'b', 'B', '_', '-', 'z', 'Z', 'é', 'É', 'ñ', '0', '9'}
	out := make([]string, 0, n)
	seen := map[string]bool{}
	// Deterministic enumeration of length-3 strings over the alphabet,
	// taken in order until we have n distinct values.
	la := len(alphabet)
	for i := 0; len(out) < n; i++ {
		s := string([]rune{
			alphabet[(i/(la*la))%la],
			alphabet[(i/la)%la],
			alphabet[i%la],
		}) + fmt.Sprintf("%05d", i) // suffix guarantees distinctness
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func insertStringPKRows(t *testing.T, dsn, table string, vals []string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open for seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO "+table+" (id, label) VALUES ($1, $2)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	for i, v := range vals {
		if _, err := stmt.ExecContext(ctx, v, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("insert %q: %v", v, err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ANALYZE "+table); err != nil {
		t.Fatalf("analyze: %v", err)
	}
}

// TestMigrate_PG_KeysetCopy_StringPK_DefaultCollation is the load-bearing
// pin the original suite MISSED by using UUID (byte-monotonic lowercase
// hex) as the "string" representative. It seeds a text-PK table > the
// chunk threshold with values whose en_US.utf8 collation order differs
// from byte order, runs parallel keyset copy, and asserts every row lands
// EXACTLY ONCE (count + order-independent checksum). A coverage gap from a
// bytewise-vs-collation upper-bound mismatch fails this loudly.
func TestMigrate_PG_KeysetCopy_StringPK_DefaultCollation(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 40_000
	applyPGDDL(t, sourceDSN, `
		CREATE TABLE names (
			id    TEXT PRIMARY KEY,
			label TEXT NOT NULL
		);`)
	insertStringPKRows(t, sourceDSN, "names", makeCollatedStringValues(rowCount))

	pgEng, _ := engines.Get("postgres")
	logs := captureSlog(t)
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 10_000,
		MigrationID:         "test-keyset-string-default-collation",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	assertChunkFanout(t, logs.String(), 2)
	assertCountAndChecksum(t, sourceDSN, targetDSN, "names", "id || '|' || label")
}

// TestMigrate_PG_KeysetCopy_StringPK_ExplicitCollation is the same
// coverage assertion but with an EXPLICIT per-column COLLATE "en_US.utf8"
// on the PK, confirming the SQL upper-bound predicate honours the column's
// declared collation (not the database default) — the order ORDER BY and
// the `<=` row comparison both use.
func TestMigrate_PG_KeysetCopy_StringPK_ExplicitCollation(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 40_000
	applyPGDDL(t, sourceDSN, `
		CREATE TABLE names_coll (
			id    TEXT COLLATE "en_US.utf8" PRIMARY KEY,
			label TEXT NOT NULL
		);`)
	insertStringPKRows(t, sourceDSN, "names_coll", makeCollatedStringValues(rowCount))

	pgEng, _ := engines.Get("postgres")
	logs := captureSlog(t)
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 10_000,
		MigrationID:         "test-keyset-string-explicit-collation",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	assertChunkFanout(t, logs.String(), 2)
	assertCountAndChecksum(t, sourceDSN, targetDSN, "names_coll", "id || '|' || label")
}

// TestMigrate_PG_KeysetCopy_NumericPK pins the decimal/numeric PK family
// where LEXICAL order != NUMERIC order ("10" < "9" as text). The bytewise
// Go clip compared decimal-as-text lexically while the DB orders
// numerically, so boundary rows could fall into no chunk. With the SQL
// upper bound the DB does the comparison numerically on both bounds.
func TestMigrate_PG_KeysetCopy_NumericPK(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 40_000
	// Values where lexical and numeric order diverge across magnitudes:
	// generate_series gives 1..N; as NUMERIC the DB orders them 1,2,..,N
	// but their text forms order 1,10,100,...,2,20,... — the exact gap.
	ddl := fmt.Sprintf(`
		CREATE TABLE ledger (
			id    NUMERIC(20,4) PRIMARY KEY,
			label TEXT NOT NULL
		);
		INSERT INTO ledger (id, label)
			SELECT g + 0.5, 'r-' || g FROM generate_series(1, %d) AS g;
		ANALYZE ledger;
	`, rowCount)
	applyPGDDL(t, sourceDSN, ddl)

	pgEng, _ := engines.Get("postgres")
	logs := captureSlog(t)
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 10_000,
		MigrationID:         "test-keyset-numeric",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	assertChunkFanout(t, logs.String(), 2)
	assertCountAndChecksum(t, sourceDSN, targetDSN, "ledger", "id::text || '|' || label")
}

// TestMigrate_PG_KeysetCopy_CharPK pins the CHAR(n) PK family
// (blank-padded comparison semantics) under the default collation.
func TestMigrate_PG_KeysetCopy_CharPK(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 40_000
	applyPGDDL(t, sourceDSN, `
		CREATE TABLE codes (
			id    CHAR(8) PRIMARY KEY,
			label TEXT NOT NULL
		);`)
	// CHAR(8) values that span case + punctuation + accent within 8 chars
	// so the collation-vs-byte-order gap is exercised under blank padding.
	insertStringPKRows(t, sourceDSN, "codes", distinctTruncated(rowCount))

	pgEng, _ := engines.Get("postgres")
	logs := captureSlog(t)
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 10_000,
		MigrationID:         "test-keyset-char",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	assertChunkFanout(t, logs.String(), 2)
	// CHAR comparison is blank-padded; compare on rtrim'd value so source
	// and target agree regardless of how each surfaces trailing spaces.
	assertCountAndChecksum(t, sourceDSN, targetDSN, "codes", "rtrim(id) || '|' || label")
}

// distinctTruncated builds n distinct CHAR(8)-fitting values that still
// span case + punctuation + accent so the collation gap is exercised
// within 8 chars. Two collated lead chars + a base-36 5-char counter fits
// in 7-8 chars and stays distinct for well past 40k rows.
func distinctTruncated(n int) []string {
	lead := []rune{'a', 'A', 'b', 'B', '_', 'z', 'Z', 'é', 'ñ'}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		c1 := lead[i%len(lead)]
		c2 := lead[(i/len(lead))%len(lead)]
		out = append(out, string([]rune{c1, c2})+fmt.Sprintf("%05d", i))
	}
	return out
}

// TestKeysetPartition_MatchesDBOrder_StringPK is the DIRECT invariant pin
// the original suite lacked (it only self-checked the Go comparator). It
// ground-truths the partition against the REAL database: it samples actual
// keyset boundaries from a collated-string table, assembles the half-open
// (lower, upper] chunks via computeKeysetChunkBoundaries, drains EACH chunk
// through ReadRowsBatchBounded (the same SQL bounded read the orchestrator
// uses), and asserts the chunks together cover EVERY source PK EXACTLY
// ONCE. Under the old bytewise Go clip on a non-C collation this fails
// (boundary-straddling rows land in zero chunks); with the SQL upper bound
// it holds by construction because both bounds use the column's collation.
func TestKeysetPartition_MatchesDBOrder_StringPK(t *testing.T) {
	sourceDSN, _, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 20_000
	applyPGDDL(t, sourceDSN, `
		CREATE TABLE part_probe (
			id    TEXT PRIMARY KEY,
			label TEXT NOT NULL
		);`)
	insertStringPKRows(t, sourceDSN, "part_probe", makeCollatedStringValues(rowCount))

	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	sr, err := pgEng.OpenSchemaReader(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	var table *ir.Table
	for _, tb := range schema.Tables {
		if tb.Name == "part_probe" {
			table = tb
		}
	}
	if table == nil {
		t.Fatal("part_probe not found in schema")
	}

	rr, err := pgEng.OpenRowReader(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	if c, ok := rr.(interface{ Close() error }); ok {
		defer func() { _ = c.Close() }()
	}

	sampler, ok := rr.(ir.KeysetSampler)
	if !ok {
		t.Fatal("PG reader does not implement KeysetSampler")
	}
	bounded, ok := rr.(ir.BoundedBatchedRowReader)
	if !ok {
		t.Fatal("PG reader does not implement BoundedBatchedRowReader")
	}

	const n = 4
	bounds, err := computeKeysetChunkBoundaries(ctx, sampler, table, n)
	if err != nil {
		t.Fatalf("computeKeysetChunkBoundaries: %v", err)
	}
	if len(bounds) < 2 {
		t.Fatalf("expected >= 2 chunks for ground-truth partition; got %d", len(bounds))
	}

	// Drain every chunk and tally how many chunks each PK appears in.
	seen := map[string]int{}
	for _, b := range bounds {
		cursor := b.lowerPK
		for {
			ch, err := bounded.ReadRowsBatchBounded(ctx, table, cursor, b.upperPK, 2000)
			if err != nil {
				t.Fatalf("ReadRowsBatchBounded chunk %d: %v", b.chunkIndex, err)
			}
			var last []any
			batch := 0
			for row := range ch {
				id := fmt.Sprintf("%v", row["id"])
				seen[id]++
				last = []any{row["id"]}
				batch++
			}
			if rerr := rr.Err(); rerr != nil {
				t.Fatalf("reader err on chunk %d: %v", b.chunkIndex, rerr)
			}
			if batch == 0 {
				break
			}
			cursor = last
		}
	}

	// Every source PK must be seen exactly once across all chunks.
	missing, dup := 0, 0
	for id, c := range seen {
		switch {
		case c == 0:
			missing++
		case c > 1:
			dup++
			if dup <= 5 {
				t.Errorf("pk %q appeared in %d chunks (disjointness violated)", id, c)
			}
		}
	}
	if len(seen) != rowCount {
		t.Errorf("partition covered %d distinct PKs; want %d (coverage gap = silent loss)", len(seen), rowCount)
	}
	if missing != 0 || dup != 0 {
		t.Errorf("partition not exactly-once: missing=%d duplicated=%d", missing, dup)
	}
}

// TestMigrate_PG_KeysetCopy_NoPKFallback confirms a table with no usable
// chunk key (no PK) still copies correctly via the single-reader
// fallback under --bulk-parallelism>1 — the un-chunkable case must never
// lose or duplicate rows.
func TestMigrate_PG_KeysetCopy_NoPKFallback(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 20_000
	ddl := fmt.Sprintf(`
		CREATE TABLE events_nopk (
			val TEXT NOT NULL
		);
		INSERT INTO events_nopk (val)
			SELECT 'v-' || g FROM generate_series(1, %d) AS g;
		ANALYZE events_nopk;
	`, rowCount)
	applyPGDDL(t, sourceDSN, ddl)

	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 1_000,
		MigrationID:         "test-keyset-nopk",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	assertCountAndChecksum(t, sourceDSN, targetDSN, "events_nopk", "val")
}
