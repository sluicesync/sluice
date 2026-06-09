//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the FAST parallel sync cold-start (ADR-0079,
// roadmap item 3d). `sluice sync start`'s PG→PG cold-start now reuses
// migrate's fast machinery — cross-table pool (ADR-0076) + index-build
// overlap (ADR-0077) + same-engine raw passthrough (ADR-0078) — with all N
// parallel readers pinned to the ONE exported snapshot via
// [ir.SnapshotImporter]. These boot a real Postgres container (logical
// replication enabled, source + target) and assert:
//
//   - many-table cold-start: zero-loss + the copy actually parallelised
//     (index-overlap seam fires, only reachable on the parallel path) + CDC
//     continues correctly after (post-snapshot writes arrive via CDC, never
//     double-copied);
//   - the raw-copy lane is TAKEN on the sync path (rawCopyTakenObserver);
//   - per-transform negative fallback: redaction / --type-override /
//     --expr-override / --inject-shard-column each force the IR path (raw
//     NOT taken) AND the transform IS applied on the target;
//   - THE LOAD-BEARING PIN — snapshot consistency: rows written to the
//     source AFTER the snapshot opens but DURING the parallel copy are NOT
//     in the COPY output (every reader saw the pre-snapshot view) AND those
//     writes arrive exactly once via CDC.
//
// The MySQL serial-fallback counterpart lives in
// streamer_coldstart_serial_fallback_integration_test.go.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/postgres" // named: SetIndexBuildStartObserverForTest + self-registers via init
	"sluicesync.dev/sluice/internal/redact"
)

// TestStreamer_ColdStartParallel_PG_ManyTableZeroLossAndOverlap is the
// headline pin: a PG→PG `sync start` cold-start over many medium tables
// copies fast (parallel + overlapped), is zero-loss, and then continues
// CDC — a post-cold-start INSERT lands on the target via CDC.
//
// The index-overlap observability seam (min(indexBuildStart) <
// max(copyComplete)) doubles as the "fast path engaged" proof: the
// overlapped phase is ONLY reachable through runColdStartParallel →
// runBulkCopyPhases, never through the serial runBulkCopyWithOpts. If the
// gate silently routed to serial, the seam would never fire.
func TestStreamer_ColdStartParallel_PG_ManyTableZeroLossAndOverlap(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const (
		tableCount = 12
		rowsEach   = 40_000
	)
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	seedManyIndexedTables(t, src, tableCount, rowsEach)

	// Observability: first index-build start vs last copy completion. Both
	// seams fire only on the parallel path, so a non-zero firstIndexStart is
	// also proof the fast path engaged.
	var obsMu sync.Mutex
	var firstIndexStart, lastCopyComplete time.Time
	restoreCopy := setOnTableCopiedObserverForTest(func(_ string) {
		obsMu.Lock()
		lastCopyComplete = time.Now()
		obsMu.Unlock()
	})
	defer restoreCopy()
	restoreIdx := postgres.SetIndexBuildStartObserverForTest(func(_ string) {
		obsMu.Lock()
		if firstIndexStart.IsZero() {
			firstIndexStart = time.Now()
		}
		obsMu.Unlock()
	})
	defer restoreIdx()

	streamer := &Streamer{
		Source:           pgEng,
		Target:           pgEng,
		SourceDSN:        src,
		TargetDSN:        tgt,
		StreamID:         "coldstart-parallel-manytable",
		TableParallelism: 4,
		// Keep tables single-streamed so the cross-table + index axes are
		// what is exercised (not within-table chunking).
		BulkParallelMinRows: int64(rowsEach * 10),
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Bulk-copy delivered every seed row.
	for i := 0; i < tableCount; i++ {
		name := fmt.Sprintf("tbl_%02d", i)
		if !waitForExactRowCount(tgt, name, rowsEach, 3*time.Minute) {
			t.Fatalf("cold-start copy never delivered %d rows to %s (got %d)",
				rowsEach, name, pollRowCount(tgt, name))
		}
	}

	// CDC continues: an INSERT after cold-start lands via CDC.
	applyDDL(t, src, fmt.Sprintf("INSERT INTO tbl_00 (id, v, b) VALUES (%d, 0, 0);", rowsEach+1))
	if !waitForExactRowCount(tgt, "tbl_00", rowsEach+1, 60*time.Second) {
		t.Fatalf("post-cold-start CDC INSERT never propagated; tbl_00 rows = %d, want %d",
			pollRowCount(tgt, "tbl_00"), rowsEach+1)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}

	obsMu.Lock()
	fis, lcc := firstIndexStart, lastCopyComplete
	obsMu.Unlock()
	if fis.IsZero() {
		t.Fatal("no index build started — fast parallel cold-start did NOT engage (gate regressed to serial?)")
	}
	if lcc.IsZero() {
		t.Fatal("no copy completion observed — parallel pool never ran")
	}
	if !fis.Before(lcc) {
		t.Errorf("index builds did NOT overlap the copy on the sync cold-start: firstIndexStart=%s >= lastCopyComplete=%s",
			fis, lcc)
	}
}

// TestStreamer_ColdStartParallel_PG_RawLaneTaken asserts the same-engine
// raw-copy passthrough (ADR-0078) engages on the SYNC cold-start path, via
// the rawCopyTakenObserver seam — a green zero-loss test alone can't tell
// the raw lane from the IR fallback.
func TestStreamer_ColdStartParallel_PG_RawLaneTaken(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()

	const rows = 5_000
	applyDDL(t, src, fmt.Sprintf(`
		CREATE TABLE widgets (id BIGINT PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO widgets (id, payload)
			SELECT g, 'p' || g FROM generate_series(1, %d) AS g;
		ANALYZE widgets;
	`, rows))

	rec := installRawCopyRecorder(t)

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  "coldstart-parallel-rawlane",
		// Force the whole table down the within-table chunk path so the raw
		// lane (which lives on the fast-loader/chunk branch) is exercised.
		BulkParallelism:     4,
		BulkParallelMinRows: 100,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForExactRowCount(tgt, "widgets", rows, 2*time.Minute) {
		t.Fatalf("cold-start copy never delivered %d rows (got %d)", rows, pollRowCount(tgt, "widgets"))
	}
	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}

	if !rec.any() {
		t.Error("raw-copy lane was NOT taken on the sync cold-start (no rawCopyTakenObserver fire) — the fast same-engine passthrough did not engage")
	}
}

// TestStreamer_ColdStartParallel_PG_NegativeFallbackPerTransform is the
// value-fidelity pin: each transform the byte-pipe would silently skip
// MUST force the IR path (raw NOT taken) AND the transform MUST be applied
// on the target. One case per transform — the gate is family-dispatched
// (redaction / type-override / expr-override / shard), so the matrix
// exercises every cell, not one representative.
func TestStreamer_ColdStartParallel_PG_NegativeFallbackPerTransform(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	tests := []struct {
		name string
		// configure mutates the streamer to add exactly one transform.
		configure func(s *Streamer)
		// verify asserts the transform was applied on the target.
		verify func(t *testing.T, tgt string)
	}{
		{
			name: "redaction",
			configure: func(s *Streamer) {
				r := redact.New()
				r.Set("public", "widgets", "secret", redact.Hash{Algo: "sha256"})
				s.Redactor = r
			},
			verify: func(t *testing.T, tgt string) {
				// The redacted column must NOT equal the source plaintext.
				var got string
				db, _ := sql.Open("pgx", tgt)
				defer func() { _ = db.Close() }()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := db.QueryRowContext(ctx, "SELECT secret FROM widgets WHERE id = 1").Scan(&got); err != nil {
					t.Fatalf("read target secret: %v", err)
				}
				if got == "plaintext-1" {
					t.Errorf("redaction NOT applied on target: secret == source plaintext %q (raw lane silently skipped the redaction?)", got)
				}
			},
		},
		{
			name: "type override",
			configure: func(s *Streamer) {
				// amount NUMERIC(12,2) -> text on the target. numeric decodes
				// cleanly as a string on the IR read path (unlike e.g. BIGINT
				// -> text, which mis-decodes); this mirrors the proven migrate
				// negative-fallback shape.
				s.Mappings = []config.Mapping{{Table: "widgets", Column: "amount", TargetType: "text"}}
			},
			verify: func(t *testing.T, tgt string) {
				var dataType string
				db, _ := sql.Open("pgx", tgt)
				defer func() { _ = db.Close() }()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := db.QueryRowContext(
					ctx,
					"SELECT data_type FROM information_schema.columns WHERE table_name='widgets' AND column_name='amount'",
				).Scan(&dataType); err != nil {
					t.Fatalf("read target column type: %v", err)
				}
				if !strings.Contains(strings.ToLower(dataType), "text") {
					t.Errorf("type override NOT applied: target amount column is %q, want text (raw lane silently skipped the override?)", dataType)
				}
			},
		},
		{
			name: "expression override",
			configure: func(s *Streamer) {
				// Rewrite the generated column body from upper() to lower() so
				// the target value diverges from the byte-pipe-copied source
				// value — the proven migrate expr-override shape (an override on
				// a generated column).
				s.ExpressionMappings = []config.ExpressionMapping{
					{Table: "widgets", Column: "secret_upper", Expression: "lower(secret)"},
				}
			},
			verify: func(t *testing.T, tgt string) {
				var mismatches int64
				db, _ := sql.Open("pgx", tgt)
				defer func() { _ = db.Close() }()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := db.QueryRowContext(
					ctx,
					"SELECT count(*) FROM widgets WHERE secret_upper <> lower(secret)",
				).Scan(&mismatches); err != nil {
					t.Fatalf("read target secret_upper: %v", err)
				}
				if mismatches != 0 {
					t.Errorf("expression override NOT applied on %d rows (raw lane silently skipped the override?)", mismatches)
				}
			},
		},
		{
			name: "shard injection",
			configure: func(s *Streamer) {
				s.InjectShardColumn = ShardColumnSpec{Name: "shard_id", Value: "us-east-1"}
			},
			verify: func(t *testing.T, tgt string) {
				var got string
				db, _ := sql.Open("pgx", tgt)
				defer func() { _ = db.Close() }()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := db.QueryRowContext(ctx, "SELECT shard_id FROM widgets WHERE id = 1").Scan(&got); err != nil {
					t.Fatalf("read injected shard column: %v", err)
				}
				if got != "us-east-1" {
					t.Errorf("shard column NOT injected: shard_id = %q, want us-east-1 (raw lane silently skipped the stamp?)", got)
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			src, tgt, cleanup := startPostgresLogical(t)
			defer cleanup()

			const rows = 2_000
			applyDDL(t, src, fmt.Sprintf(`
				CREATE TABLE widgets (
					id           BIGINT        PRIMARY KEY,
					amount       NUMERIC(12,2) NOT NULL,
					secret       TEXT          NOT NULL,
					secret_upper TEXT GENERATED ALWAYS AS (upper(secret)) STORED
				);
				INSERT INTO widgets (id, amount, secret)
					SELECT g, (g * 1.5)::numeric(12,2), 'plaintext-' || g FROM generate_series(1, %d) AS g;
				ANALYZE widgets;
			`, rows))

			rec := installRawCopyRecorder(t)

			streamer := &Streamer{
				Source:              pgEng,
				Target:              pgEng,
				SourceDSN:           src,
				TargetDSN:           tgt,
				StreamID:            "coldstart-parallel-fallback-" + tc.name,
				BulkParallelism:     4,
				BulkParallelMinRows: 100,
			}
			tc.configure(streamer)

			streamCtx, streamCancel := context.WithCancel(context.Background())
			defer streamCancel()
			runErr := make(chan error, 1)
			go func() { runErr <- streamer.Run(streamCtx) }()

			if !waitForExactRowCount(tgt, "widgets", rows, 2*time.Minute) {
				t.Fatalf("cold-start copy never delivered %d rows (got %d)", rows, pollRowCount(tgt, "widgets"))
			}
			streamCancel()
			select {
			case <-runErr:
			case <-time.After(20 * time.Second):
				t.Fatal("Streamer.Run did not return after ctx cancel")
			}

			if rec.any() {
				t.Errorf("raw-copy lane was TAKEN despite %s configured — the byte-pipe would silently skip the transform (silent-loss)", tc.name)
			}
			tc.verify(t, tgt)
		})
	}
}

// TestStreamer_ColdStartParallel_PG_SnapshotConsistency is THE load-bearing
// pin (ADR-0079): it proves every parallel reader is pinned to the SAME
// exported snapshot, and that the snapshot/CDC boundary is gap-free. We
// write NEW rows to the source AFTER the snapshot opens but BEFORE the
// parallel copy drains — then assert:
//
//   - those post-snapshot rows are NOT in the COPY output (every parallel
//     reader saw the consistent_point view, not its own per-connection
//     now() — if a reader had opened its own snapshot after the write, the
//     fast NON-upsert loader would have COPY'd the post rows and the CDC
//     re-insert would have hit a duplicate-key error, NOT silently
//     collapsed: the fast loader is non-idempotent on a fresh cold-start);
//   - the post-snapshot writes arrive EXACTLY ONCE via CDC (gap-free: not
//     dropped, not double-applied) — proven by the closed-form checksum.
//
// The write is fired from the [coldStartDispatchObserver] seam, which runs
// the instant the cold-start chooses the fast path — AFTER the exported
// snapshot was captured, BEFORE the copy pool drains it. That makes the
// "committed strictly after the consistent_point, during the copy"
// interleaving DETERMINISTIC regardless of how fast the copy runs (the
// raw-copy lane drains 200k rows in ~3s, far too fast to catch by polling
// the target row count).
func TestStreamer_ColdStartParallel_PG_SnapshotConsistency(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()

	// preSnapshot rows are 1..N. postSnapshot rows N+1..N+M are written
	// AFTER the snapshot opens but during the copy.
	const (
		preSnapshot  = 50_000
		postSnapshot = 50
	)
	applyDDL(t, src, fmt.Sprintf(`
		CREATE TABLE events (id BIGINT PRIMARY KEY, v BIGINT NOT NULL);
		INSERT INTO events (id, v)
			SELECT g, g FROM generate_series(1, %d) AS g;
		ANALYZE events;
	`, preSnapshot))

	// Fire the post-snapshot write from the dispatch seam: it runs after the
	// snapshot was captured (openSnapshotStreamScoped, earlier in coldStart)
	// and before runColdStartParallel drains it, so the INSERT commits
	// strictly after the consistent_point and during the copy window. Once.
	var writeOnce sync.Once
	restore := setColdStartDispatchObserverForTest(func(fast bool) {
		if !fast {
			return
		}
		writeOnce.Do(func() {
			applyDDL(t, src, fmt.Sprintf(
				"INSERT INTO events (id, v) SELECT g, g FROM generate_series(%d, %d) AS g;",
				preSnapshot+1, preSnapshot+postSnapshot,
			))
		})
	})
	defer restore()

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  "coldstart-parallel-snapshot-consistency",
		// Multi-chunk within-table so several snapshot-pinned readers run.
		BulkParallelism:     4,
		BulkParallelMinRows: 1000,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Final state must be ALL rows (pre + post), each exactly once. The post
	// rows can ONLY arrive via CDC (they committed after the snapshot, so the
	// snapshot-pinned COPY can't have seen them); their arrival proves the
	// CDC tail picked up from the consistent_point with no gap.
	total := preSnapshot + postSnapshot
	if !waitForExactRowCount(tgt, "events", total, 3*time.Minute) {
		t.Fatalf("final row count never reached %d (got %d); the post-snapshot CDC writes did not all arrive",
			total, pollRowCount(tgt, "events"))
	}

	// Zero-loss / no-dup checksum over the full range: SUM(v) == the closed
	// form for 1..total. A dropped row OR a double-applied row breaks this.
	// Combined with the non-upsert-fast-loader argument in the doc comment,
	// this is the gap-free-AND-no-leak proof.
	wantSum := int64(total) * int64(total+1) / 2
	gotSum := pgScalar(t, tgt, "SELECT COALESCE(SUM(v),0) FROM events")
	if gotSum != wantSum {
		t.Errorf("checksum mismatch: SUM(v) = %d, want %d (snapshot boundary not gap-free / a row double-copied or dropped)", gotSum, wantSum)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_ColdStartParallel_PG_WithinTableChunkingEngaged is the
// ADR-0079 v1.1 pin: a large, ANALYZEd single table on the PG-source fast
// sync cold-start must engage WITHIN-table PK-range chunking — not just the
// cross-table pool's single-stream-per-table. It proves the pinned-reader
// CountRows/RangeBounds relaxation (v1.1) actually re-enabled chunking AND
// did not reintroduce the a8d065d self-deadlock (a regression would HANG
// here, failing via the row-count timeout). The raw-copy observer fires once
// per copied UNIT, so >1 fire for the single table == multiple chunks ran.
//
// ANALYZE is load-bearing: a pinned reader takes the reltuples estimate and
// declines the exact-COUNT(*) seq scan, so without populated stats the table
// stays single-stream (the documented v1.1 limitation) and this would not
// chunk.
func TestStreamer_ColdStartParallel_PG_WithinTableChunkingEngaged(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()

	const rows = 60_000
	applyDDL(t, src, fmt.Sprintf(`
		CREATE TABLE big (id BIGINT PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO big (id, payload)
			SELECT g, 'p' || g FROM generate_series(1, %d) AS g;
		ANALYZE big;
	`, rows))

	rec := installRawCopyRecorder(t)

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  "coldstart-within-table-chunk",
		// 60k rows >> the 1000-row threshold → within-table chunking eligible;
		// up to 4 PK-range chunks, each a raw-copied unit.
		BulkParallelism:     4,
		BulkParallelMinRows: 1000,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForExactRowCount(tgt, "big", rows, 2*time.Minute) {
		t.Fatalf("cold-start copy never delivered %d rows (got %d) — a hang here would indicate the pinned-reader CountRows deadlock regressed",
			rows, pollRowCount(tgt, "big"))
	}
	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}

	if n := rec.count("big"); n < 2 {
		t.Errorf("within-table chunking did NOT engage on the sync fast cold-start: raw-copy fired %d time(s) for \"big\" (want >=2 chunks). The ADR-0079 v1.1 pinned-reader CountRows/RangeBounds fix may have regressed to single-stream.", n)
	}
}
