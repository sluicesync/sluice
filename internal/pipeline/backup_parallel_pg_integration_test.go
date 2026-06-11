//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0084 integration pins: parallel per-table backup reads on a real
// Postgres source. The class matrix (Bug-74 doctrine — pin the class,
// not the representative):
//
//   {plaintext, per-chain encrypted, per-chunk encrypted}
//     × {parallel (PG, eligible), serial (MySQL, gate refuses)}
//     × {fresh run, cancel-mid-run + resume}
//
// plus manifest determinism (parallel completion order must not leak
// into the manifest) — all ground-truthed with content checksums
// (COUNT + SUM(hashtext(row::text))) against the live source, never
// row counts alone. The MySQL serial leg lives in
// backup_parallel_mysql_integration_test.go.

package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// seedParallelBackupTables creates n tables (pbk_00 … pbk_<n-1>) with
// mixed-family columns and rowsPerTable rows each, so the sweep has
// real cross-table work and the checksum covers more than integers.
func seedParallelBackupTables(t *testing.T, dsn string, n, rowsPerTable int) []string {
	t.Helper()
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("pbk_%02d", i)
		names = append(names, name)
		applyDDL(t, dsn, fmt.Sprintf(`
			CREATE TABLE %s (
				id      BIGINT PRIMARY KEY,
				label   TEXT NOT NULL,
				amount  NUMERIC(12,4) NOT NULL,
				created TIMESTAMPTZ NOT NULL
			);
			INSERT INTO %s (id, label, amount, created)
			SELECT g,
			       'tbl-%02d-row-' || g,
			       (g %% 1000) + 0.25,
			       TIMESTAMPTZ '2026-01-01 00:00:00+00' + (g || ' seconds')::interval
			FROM generate_series(1, %d) g;
		`, name, name, i, rowsPerTable))
	}
	return names
}

// pgTableChecksum returns (row count, content checksum) for one table:
// SUM over hashtext of every row's text rendering. Column order is the
// table's declaration order on both source and restored target, so
// equal contents hash equal.
func pgTableChecksum(t *testing.T, dsn, table string) (count, sum int64) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf(`SELECT COUNT(*), COALESCE(SUM(hashtext(t::text)::bigint), 0) FROM %s t`, table)
	if err := db.QueryRow(q).Scan(&count, &sum); err != nil {
		t.Fatalf("checksum %s: %v", table, err)
	}
	return count, sum
}

// assertTablesMatchPG fails the test for every table whose (count,
// checksum) differs between source and target.
func assertTablesMatchPG(t *testing.T, sourceDSN, targetDSN string, tables []string) {
	t.Helper()
	for _, table := range tables {
		srcN, srcSum := pgTableChecksum(t, sourceDSN, table)
		dstN, dstSum := pgTableChecksum(t, targetDSN, table)
		if srcN != dstN || srcSum != dstSum {
			t.Errorf("table %s: source (n=%d sum=%d) != target (n=%d sum=%d)", table, srcN, srcSum, dstN, dstSum)
		}
	}
}

// observeBackupDispatch installs the test-only dispatch observer and
// returns pointers to the captured decision. Restores the seam via
// t.Cleanup. Tests using it must not run in t.Parallel (package
// precedent: coldStartDispatchObserver).
func observeBackupDispatch(t *testing.T) (gotParallelism *int, gotReason *string) {
	t.Helper()
	p, r := 0, ""
	backupDispatchObserver = func(tableParallelism int, reason string) {
		p, r = tableParallelism, reason
	}
	t.Cleanup(func() { backupDispatchObserver = nil })
	return &p, &r
}

// TestBackupParallel_PG_RoundTripChecksums pins the headline path: a
// TableParallelism=4 full backup on a PG source engages the parallel
// sweep (observer-asserted, not timing-inferred), restores into a
// fresh target, and every table's content checksum matches the source.
func TestBackupParallel_PG_RoundTripChecksums(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	tables := seedParallelBackupTables(t, sourceDSN, 6, 150)
	pgEng, _ := engines.Get("postgres")
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	gotP, gotReason := observeBackupDispatch(t)
	if err := (&Backup{
		Source:           pgEng,
		SourceDSN:        sourceDSN,
		Store:            store,
		ChunkRows:        40, // 150 rows → 4 chunks per table
		TableParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	if *gotP <= 1 {
		t.Fatalf("dispatch = serial (reason %q); want the parallel sweep engaged on a PG source", *gotReason)
	}

	m, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if m.PartialState != irbackup.BackupStateComplete {
		t.Fatalf("PartialState = %q; want complete", m.PartialState)
	}
	// Manifest table order must equal schema order (pre-staging
	// invariant) even though completion order was concurrent.
	if len(m.Tables) != len(tables) {
		t.Fatalf("manifest tables = %d; want %d", len(m.Tables), len(tables))
	}
	for i, table := range m.Tables {
		if table.Name != tables[i] {
			t.Errorf("manifest.Tables[%d] = %s; want %s (schema order)", i, table.Name, tables[i])
		}
	}

	if err := (&Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	assertTablesMatchPG(t, sourceDSN, targetDSN, tables)
}

// cancelAfterChunkPutsStore wraps a store and cancels the supplied
// CancelFunc once n chunk Puts (keys under chunks/) have landed —
// a deterministic stand-in for "the process died mid-sweep".
type cancelAfterChunkPutsStore struct {
	*LocalStore
	n      int64
	puts   atomic.Int64
	cancel context.CancelFunc
}

func (s *cancelAfterChunkPutsStore) Put(ctx context.Context, path string, r io.Reader) error {
	if len(path) > 7 && path[:7] == "chunks/" {
		if s.puts.Add(1) == s.n {
			s.cancel()
		}
	}
	return s.LocalStore.Put(ctx, path, r)
}

// TestBackupParallel_PG_CancelBoundsPartialsAndResumeCompletes pins
// the ADR-0084 crash contract: a run cancelled with N tables in flight
// leaves a manifest where (a) every table is staged (schema order),
// (b) at most tableParallelism entries are Partial WITH chunks (the
// in-flight workers; untouched tables are Partial with zero chunks),
// and (c) a plain re-run resumes to a complete backup whose restore
// checksums match the source.
func TestBackupParallel_PG_CancelBoundsPartialsAndResumeCompletes(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const nTables, tableParallelism = 8, 4
	tables := seedParallelBackupTables(t, sourceDSN, nTables, 200)
	pgEng, _ := engines.Get("postgres")
	inner, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// 200 rows / 50-row chunks = 4 chunks per table, 32 total; cancel
	// once the 10th chunk lands so several tables are mid-stream.
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &cancelAfterChunkPutsStore{LocalStore: inner, n: 10, cancel: cancel}
	err = (&Backup{
		Source:           pgEng,
		SourceDSN:        sourceDSN,
		Store:            store,
		ChunkRows:        50,
		TableParallelism: tableParallelism,
	}).Run(runCtx)
	if err == nil {
		t.Fatal("cancelled Backup.Run returned nil; want a context error")
	}

	m, err := readManifest(context.Background(), inner)
	if err != nil {
		t.Fatalf("readManifest after cancel: %v", err)
	}
	if m.PartialState != irbackup.BackupStateInProgress {
		t.Fatalf("PartialState = %q; want in_progress", m.PartialState)
	}
	if len(m.Tables) != nTables {
		t.Fatalf("staged tables = %d; want all %d (pre-staging invariant)", len(m.Tables), nTables)
	}
	inFlight := 0
	for i, table := range m.Tables {
		if table.Name != tables[i] {
			t.Errorf("manifest.Tables[%d] = %s; want %s (schema order)", i, table.Name, tables[i])
		}
		if table.Partial && len(table.Chunks) > 0 {
			inFlight++
		}
	}
	if inFlight > tableParallelism {
		t.Errorf("crashed manifest has %d in-flight partial tables; want <= tableParallelism (%d)", inFlight, tableParallelism)
	}
	t.Logf("crashed manifest: %d in-flight partials (bound %d)", inFlight, tableParallelism)

	// Resume: a plain re-run against the same store completes the
	// backup (whole-table skips + per-chunk resume + fresh streams).
	if err := (&Backup{
		Source:           pgEng,
		SourceDSN:        sourceDSN,
		Store:            inner,
		ChunkRows:        50,
		TableParallelism: tableParallelism,
	}).Run(context.Background()); err != nil {
		t.Fatalf("resume Backup.Run: %v", err)
	}
	m2, err := readManifest(context.Background(), inner)
	if err != nil {
		t.Fatalf("readManifest after resume: %v", err)
	}
	if m2.PartialState != irbackup.BackupStateComplete {
		t.Fatalf("resumed PartialState = %q; want complete", m2.PartialState)
	}

	if err := (&Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     inner,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	assertTablesMatchPG(t, sourceDSN, targetDSN, tables)
}

// TestBackupParallel_PG_EncryptedRoundTrip pins the parallel sweep ×
// encryption-mode matrix: per-chain (one CEK, wrapped once) AND
// per-chunk (fresh CEK + envelope wrap per chunk, exercised
// CONCURRENTLY by peer tables — the WrapCEK-under-concurrency leg).
func TestBackupParallel_PG_EncryptedRoundTrip(t *testing.T) {
	for _, mode := range []string{crypto.EncryptModePerChain, crypto.EncryptModePerChunk} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
			defer cleanup()

			tables := seedParallelBackupTables(t, sourceDSN, 4, 120)
			pgEng, _ := engines.Get("postgres")
			store, err := NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatalf("NewLocalStore: %v", err)
			}

			const passphrase = "parallel-backup-test-passphrase"
			params, err := crypto.DefaultArgon2idParams()
			if err != nil {
				t.Fatalf("DefaultArgon2idParams: %v", err)
			}
			env, err := crypto.NewPassphraseEnvelope(passphrase, params)
			if err != nil {
				t.Fatalf("NewPassphraseEnvelope: %v", err)
			}

			gotP, gotReason := observeBackupDispatch(t)
			if err := (&Backup{
				Source:           pgEng,
				SourceDSN:        sourceDSN,
				Store:            store,
				ChunkRows:        40,
				TableParallelism: 4,
				Encryption:       &BackupEncryption{Envelope: env, Mode: mode},
			}).Run(context.Background()); err != nil {
				t.Fatalf("Backup.Run (%s): %v", mode, err)
			}
			if *gotP <= 1 {
				t.Fatalf("dispatch = serial (reason %q); want parallel", *gotReason)
			}

			envRestore := envelopeFromManifest(t, store, passphrase)
			if err := (&Restore{
				Target:    pgEng,
				TargetDSN: targetDSN,
				Store:     store,
				Envelope:  envRestore,
			}).Run(context.Background()); err != nil {
				t.Fatalf("Restore.Run (%s): %v", mode, err)
			}
			assertTablesMatchPG(t, sourceDSN, targetDSN, tables)
		})
	}
}

// TestBackupParallel_PG_ManifestDeterminism pins that the manifests a
// serial (TableParallelism=1) and a parallel (4) run produce over the
// same idle source are equal modulo the volatile fields (CreatedAt,
// BackupID, EndPosition — each run anchors its own slot/LSN): same
// table order, same chunk files, same per-chunk row counts and
// SHA-256s. Completion order must not leak into the artifact.
func TestBackupParallel_PG_ManifestDeterminism(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()

	seedParallelBackupTables(t, sourceDSN, 5, 130)
	pgEng, _ := engines.Get("postgres")

	runOne := func(tableParallelism int) *irbackup.Manifest {
		t.Helper()
		store, err := NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		if err := (&Backup{
			Source:           pgEng,
			SourceDSN:        sourceDSN,
			Store:            store,
			ChunkRows:        40,
			TableParallelism: tableParallelism,
		}).Run(context.Background()); err != nil {
			t.Fatalf("Backup.Run(parallelism=%d): %v", tableParallelism, err)
		}
		m, err := readManifest(context.Background(), store)
		if err != nil {
			t.Fatalf("readManifest: %v", err)
		}
		return m
	}

	serial := runOne(1)
	parallel := runOne(4)

	normalize := func(m *irbackup.Manifest) string {
		m.CreatedAt = time.Time{}
		m.BackupID = ""
		m.EndPosition = ir.Position{}
		b, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			t.Fatalf("marshal normalized manifest: %v", err)
		}
		return string(b)
	}
	if s, p := normalize(serial), normalize(parallel); s != p {
		t.Errorf("serial and parallel manifests differ modulo volatile fields:\n--- serial ---\n%s\n--- parallel ---\n%s", s, p)
	}
}
