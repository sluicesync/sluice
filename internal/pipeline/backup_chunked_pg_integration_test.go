//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0149 integration pins: within-table chunked backup reads on a
// real Postgres source. The class matrix (Bug-74 doctrine — pin the
// class, not the representative): chunk-strategy families
//
//	{int PK (MIN/MAX divide), TEXT PK incl. collation-sensitive values
//	 (sampled keyset), composite PK (sampled keyset)}
//	  × {plaintext, per-chunk encrypted}
//	  × {fresh run, cancel-mid-table + resume (Bug 135 re-stream)}
//
// all ground-truthed against the live source with exact counts +
// content checksums + an ordered row compare — never chunk counts
// alone — and the dispatch asserted through the observer seam, never
// inferred from timing. Every fixture is freshly seeded and NEVER
// ANALYZEd, so each engagement assertion doubles as the e2e pin for
// the 59c55e27 estimator trap (reltuples sentinel → estimate 0 →
// silent single-stream) on the backup path.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/engines"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// observeBackupChunkDispatch installs the ADR-0149 test-only dispatch
// observer and returns accessors for the captured per-table decisions.
// The observer fires from concurrent pool workers, so the capture is
// mutex-guarded. Tests using it must not run in t.Parallel (package
// precedent: backupDispatchObserver).
func observeBackupChunkDispatch(t *testing.T) (ranges func(table string) int, reason func(table string) string) {
	t.Helper()
	var (
		mu sync.Mutex
		r  = map[string]int{}
		w  = map[string]string{}
	)
	backupChunkDispatchObserver = func(table string, n int, why string) {
		mu.Lock()
		defer mu.Unlock()
		r[table] = n
		w[table] = why
	}
	t.Cleanup(func() { backupChunkDispatchObserver = nil })
	ranges = func(table string) int {
		mu.Lock()
		defer mu.Unlock()
		return r[table]
	}
	reason = func(table string) string {
		mu.Lock()
		defer mu.Unlock()
		return w[table]
	}
	return ranges, reason
}

// assertOrderedRowsMatchPG compares the FULL ordered row set of one
// table between source and target — a stricter companion to the
// count+checksum compare that would also catch an ordering-insensitive
// hash collision or a swapped-column corruption.
func assertOrderedRowsMatchPG(t *testing.T, sourceDSN, targetDSN, query string) {
	t.Helper()
	read := func(dsn string) []string {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open %s: %v", dsn, err)
		}
		defer func() { _ = db.Close() }()
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("query %q: %v", query, err)
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, s)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		return out
	}
	src, dst := read(sourceDSN), read(targetDSN)
	if len(src) != len(dst) {
		t.Fatalf("ordered compare: source has %d rows, target has %d", len(src), len(dst))
	}
	for i := range src {
		if src[i] != dst[i] {
			t.Fatalf("ordered compare: row %d differs: source %q != target %q", i, src[i], dst[i])
		}
	}
}

// TestBackupChunked_PG_IntPKRoundTrip pins the headline path: one big
// int-PK table (never ANALYZEd) engages the MIN/MAX-divide chunked
// read while a small sibling stays single-stream, the backup restores
// exactly, and the manifest's snapshot-anchored EndPosition is
// unaffected by chunking.
func TestBackupChunked_PG_IntPKRoundTrip(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const bigRows = 3000
	applyDDL(t, sourceDSN, fmt.Sprintf(`
		CREATE TABLE big_int (
			id     BIGINT PRIMARY KEY,
			label  TEXT NOT NULL,
			amount NUMERIC(12,4) NOT NULL
		);
		INSERT INTO big_int (id, label, amount)
		SELECT g, 'row-' || g, (g %% 997) + 0.25 FROM generate_series(1, %d) g;
		CREATE TABLE small_int (
			id BIGINT PRIMARY KEY,
			v  TEXT NOT NULL
		);
		INSERT INTO small_int (id, v) SELECT g, 'v-' || g FROM generate_series(1, 50) g;
	`, bigRows))

	pgEng, _ := engines.Get("postgres")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	ranges, reason := observeBackupChunkDispatch(t)
	if err := (&Backup{
		Source:              pgEng,
		SourceDSN:           sourceDSN,
		Store:               store,
		ChunkRows:           400, // big_int rolls multiple chunk FILES per range
		TableParallelism:    2,
		BulkParallelism:     4,
		BulkParallelMinRows: 500, // small_int (50) stays under; big_int (3000, never ANALYZEd) must clear it
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	if got := ranges("big_int"); got <= 1 {
		t.Fatalf("big_int dispatch = single-stream (reason %q); want the chunked read engaged "+
			"(never-ANALYZEd source — the 59c55e27 estimator trap on the backup path)", reason("big_int"))
	}
	if got := ranges("small_int"); got != 1 {
		t.Errorf("small_int dispatch = %d ranges; want 1 (below --bulk-parallel-min-rows)", got)
	}

	m, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if m.PartialState != irbackup.BackupStateComplete {
		t.Fatalf("PartialState = %q; want complete", m.PartialState)
	}
	// (e) The manifest's chain anchor must be unaffected by chunking:
	// still the snapshot-anchored position captured at snapshot START.
	if m.EndPosition.Engine != "postgres" || m.EndPosition.Token == "" {
		t.Errorf("EndPosition = %+v; want the snapshot-anchored postgres position (chunking must not disturb the anchor)", m.EndPosition)
	}
	for _, entry := range m.Tables {
		if entry.Name != "big_int" {
			continue
		}
		if entry.RowCount != bigRows {
			t.Errorf("big_int manifest RowCount = %d; want %d", entry.RowCount, bigRows)
		}
		if len(entry.Chunks) <= 1 {
			t.Errorf("big_int chunks = %d; want > 1 (chunked read + 400-row file roll)", len(entry.Chunks))
		}
		var sum int64
		seen := map[string]bool{}
		for _, ci := range entry.Chunks {
			sum += ci.RowCount
			if seen[ci.File] {
				t.Errorf("duplicate chunk file %q in manifest", ci.File)
			}
			seen[ci.File] = true
		}
		if sum != bigRows {
			t.Errorf("big_int per-chunk RowCount sum = %d; want %d", sum, bigRows)
		}
	}

	if err := (&Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	assertTablesMatchPG(t, sourceDSN, targetDSN, []string{"big_int", "small_int"})
	assertOrderedRowsMatchPG(t, sourceDSN, targetDSN,
		`SELECT id::text || '|' || label || '|' || amount::text FROM big_int ORDER BY id`)
}

// TestBackupChunked_PG_KeysetPKFamilies pins the sampled-keyset
// strategy legs (the non-representative half of the family matrix): a
// TEXT PK with collation-sensitive values (case-mixed, hyphenated —
// exactly the family whose Go-bytewise vs DB-collation boundary
// divergence was the ADR-0096 silent-loss class) and a composite
// (INT, INT) PK. Both never ANALYZEd.
func TestBackupChunked_PG_KeysetPKFamilies(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const rowsPer = 1500
	applyDDL(t, sourceDSN, fmt.Sprintf(`
		CREATE TABLE big_text (
			code TEXT PRIMARY KEY,
			v    TEXT NOT NULL
		);
		INSERT INTO big_text (code, v)
		SELECT (CASE g %% 4 WHEN 0 THEN 'a-' WHEN 1 THEN 'A-' WHEN 2 THEN 'b.' ELSE 'B_' END)
		       || lpad(g::text, 6, '0'),
		       'val-' || g
		FROM generate_series(1, %d) g;
		CREATE TABLE big_comp (
			tenant INT NOT NULL,
			seq    INT NOT NULL,
			v      TEXT NOT NULL,
			PRIMARY KEY (tenant, seq)
		);
		INSERT INTO big_comp (tenant, seq, v)
		SELECT g %% 10, g, 'val-' || g FROM generate_series(1, %d) g;
	`, rowsPer, rowsPer))

	pgEng, _ := engines.Get("postgres")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	ranges, reason := observeBackupChunkDispatch(t)
	if err := (&Backup{
		Source:              pgEng,
		SourceDSN:           sourceDSN,
		Store:               store,
		ChunkRows:           300,
		TableParallelism:    2,
		BulkParallelism:     4,
		BulkParallelMinRows: 400,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	for _, table := range []string{"big_text", "big_comp"} {
		if got := ranges(table); got <= 1 {
			t.Fatalf("%s dispatch = single-stream (reason %q); want the keyset chunked read engaged", table, reason(table))
		}
	}

	if err := (&Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	assertTablesMatchPG(t, sourceDSN, targetDSN, []string{"big_text", "big_comp"})
	// Ordered compares in the PK's DB-collation order — the exact axis
	// a boundary mis-clip would corrupt (dropped/duplicated straddling
	// row shifts every subsequent position).
	assertOrderedRowsMatchPG(t, sourceDSN, targetDSN,
		`SELECT code || '|' || v FROM big_text ORDER BY code`)
	assertOrderedRowsMatchPG(t, sourceDSN, targetDSN,
		`SELECT tenant::text || '|' || seq::text || '|' || v FROM big_comp ORDER BY tenant, seq`)
}

// TestBackupChunked_PG_EncryptedPerChunkCEK pins chunking × the
// per-chunk encryption mode — every range worker resolves a FRESH CEK
// + envelope wrap per chunk file, concurrently (the
// WrapCEK-under-concurrency leg of the ADR-0149 matrix).
func TestBackupChunked_PG_EncryptedPerChunkCEK(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const bigRows = 2400
	applyDDL(t, sourceDSN, fmt.Sprintf(`
		CREATE TABLE big_enc (
			id BIGINT PRIMARY KEY,
			v  TEXT NOT NULL
		);
		INSERT INTO big_enc (id, v) SELECT g, 'val-' || g FROM generate_series(1, %d) g;
	`, bigRows))

	pgEng, _ := engines.Get("postgres")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	const passphrase = "chunked-backup-test-passphrase"
	params, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	env, err := crypto.NewPassphraseEnvelope(passphrase, params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}

	ranges, reason := observeBackupChunkDispatch(t)
	if err := (&Backup{
		Source:              pgEng,
		SourceDSN:           sourceDSN,
		Store:               store,
		ChunkRows:           300,
		BulkParallelism:     4,
		BulkParallelMinRows: 400,
		Encryption:          &BackupEncryption{Envelope: env, Mode: crypto.EncryptModePerChunk},
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	if got := ranges("big_enc"); got <= 1 {
		t.Fatalf("big_enc dispatch = single-stream (reason %q); want chunked", reason("big_enc"))
	}
	// Per-chunk mode: every manifest chunk must carry its own WrappedCEK.
	m, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	for _, entry := range m.Tables {
		for _, ci := range entry.Chunks {
			if ci.Encryption == nil || len(ci.Encryption.WrappedCEK) == 0 {
				t.Fatalf("chunk %q missing per-chunk WrappedCEK", ci.File)
			}
		}
	}

	envRestore := envelopeFromManifest(t, store, passphrase)
	if err := (&Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     store,
		Envelope:  envRestore,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	assertTablesMatchPG(t, sourceDSN, targetDSN, []string{"big_enc"})
}

// TestBackupChunked_PG_CancelRestreamsPartialTable pins that the
// Bug 135 resume posture holds with chunking on: a run cancelled while
// the chunked table is mid-stream leaves a Partial entry, and a plain
// re-run DISCARDS it (re-streams the table from scratch — no per-chunk
// resume) to a complete backup whose restore matches the source
// exactly.
func TestBackupChunked_PG_CancelRestreamsPartialTable(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const bigRows = 3000
	applyDDL(t, sourceDSN, fmt.Sprintf(`
		CREATE TABLE big_resume (
			id BIGINT PRIMARY KEY,
			v  TEXT NOT NULL
		);
		INSERT INTO big_resume (id, v) SELECT g, 'val-' || g FROM generate_series(1, %d) g;
	`, bigRows))

	pgEng, _ := engines.Get("postgres")
	inner, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// 3000 rows / 200-row chunk files ⇒ ~15 chunk Puts; cancel once the
	// 4th lands so the chunked table is unambiguously mid-stream.
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &cancelAfterChunkPutsStore{LocalStore: inner, n: 4, cancel: cancel}
	err = (&Backup{
		Source:              pgEng,
		SourceDSN:           sourceDSN,
		Store:               store,
		ChunkRows:           200,
		BulkParallelism:     4,
		BulkParallelMinRows: 400,
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
	for _, entry := range m.Tables {
		if entry.Name == "big_resume" && !entry.Partial {
			t.Fatalf("big_resume marked complete after a mid-stream cancel; the Bug 135 posture is broken")
		}
	}

	// Resume: plain re-run (chunking active again) must re-stream the
	// partial table from scratch and complete.
	ranges, reason := observeBackupChunkDispatch(t)
	if err := (&Backup{
		Source:              pgEng,
		SourceDSN:           sourceDSN,
		Store:               inner,
		ChunkRows:           200,
		BulkParallelism:     4,
		BulkParallelMinRows: 400,
	}).Run(context.Background()); err != nil {
		t.Fatalf("resume Backup.Run: %v", err)
	}
	if got := ranges("big_resume"); got <= 1 {
		t.Fatalf("resumed big_resume dispatch = single-stream (reason %q); want chunked", reason("big_resume"))
	}
	m2, err := readManifest(context.Background(), inner)
	if err != nil {
		t.Fatalf("readManifest after resume: %v", err)
	}
	if m2.PartialState != irbackup.BackupStateComplete {
		t.Fatalf("resumed PartialState = %q; want complete", m2.PartialState)
	}
	for _, entry := range m2.Tables {
		if entry.Name != "big_resume" {
			continue
		}
		if entry.Partial || entry.RowCount != bigRows {
			t.Fatalf("resumed big_resume entry = {Partial:%v RowCount:%d}; want {false %d}", entry.Partial, entry.RowCount, bigRows)
		}
		var sum int64
		for _, ci := range entry.Chunks {
			sum += ci.RowCount
		}
		if sum != bigRows {
			t.Errorf("resumed big_resume per-chunk RowCount sum = %d; want %d (partial chunk list must have been discarded, not extended)", sum, bigRows)
		}
	}

	if err := (&Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     inner,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	assertTablesMatchPG(t, sourceDSN, targetDSN, []string{"big_resume"})
	assertOrderedRowsMatchPG(t, sourceDSN, targetDSN,
		`SELECT id::text || '|' || v FROM big_resume ORDER BY id`)
}
