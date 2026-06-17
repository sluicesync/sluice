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
