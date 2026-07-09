// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

// Regression pins for Bug 167: under sustained sqlite-trigger CDC the source
// SQLite WAL (and modernc's mmap of it, hence process RSS) grew without bound
// because (1) the poller's idle pooled read connection retained a stale WAL
// read-mark that pinned the checkpoint from ever resetting the WAL, and (2)
// nothing issued an explicit checkpoint, so an app that disabled
// wal_autocheckpoint had no reset path at all.
//
// These tests are pure-Go (modernc, no CGO/Docker) and deterministic — they pin
// the MECHANISM (idle release + checkpoint truncates + cadence wiring) rather
// than a timing-dependent size-under-load, so they are CI-stable.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func walBytes(t *testing.T, dbPath string) int64 {
	t.Helper()
	fi, err := os.Stat(dbPath + "-wal")
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("stat -wal: %v", err)
	}
	return fi.Size()
}

// newWALSourceWithChangeLog creates a WAL-mode SQLite file holding a
// sluice_change_log table, with the app writer's auto-checkpoint DISABLED so the
// ONLY thing that can reset the WAL is sluice's own wal_checkpoint(TRUNCATE) —
// this isolates the fix. It returns the path and the open writer (caller closes).
func newWALSourceWithChangeLog(t *testing.T) (dbPath string, writer *sql.DB) {
	t.Helper()
	dbPath = filepath.Join(t.TempDir(), "src.db")
	w, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	w.SetMaxOpenConns(1)
	ctx := context.Background()
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA wal_autocheckpoint=0", // app auto-checkpoint OFF: sluice must reset the WAL
		`CREATE TABLE "` + ChangeLogTable + `" (` +
			"id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, tbl TEXT, " +
			"before TEXT, after TEXT, captured_at TEXT)",
	} {
		if _, err := w.ExecContext(ctx, stmt); err != nil {
			_ = w.Close()
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}
	return dbPath, w
}

// growChangeLogWAL appends n change-log rows with a ~fatBytes `after` image so
// the WAL accumulates real frames.
func growChangeLogWAL(t *testing.T, w *sql.DB, n, fatBytes int) {
	t.Helper()
	ctx := context.Background()
	blob := make([]byte, fatBytes)
	for i := range blob {
		blob[i] = byte('a' + i%26)
	}
	after := `{"v":{"t":"text","v":"` + string(blob) + `"}}`
	tx, err := w.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO "`+ChangeLogTable+`"(op, tbl, before, after, captured_at) VALUES ('I','t',NULL,?, '2026-01-01 00:00:00.000')`,
			after); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// TestLocalPollerCheckpointBoundsWAL is the core Bug 167 regression pin. With
// the app writer's auto-checkpoint disabled, it (1) opens the REAL poller
// executor (localBackend, which releases the idle connection), (2) polls once so
// a read connection has been used and returned to the pool, (3) grows the WAL
// further, then (4) issues checkpointWAL and asserts the WAL was truncated to
// near zero.
//
// This discriminates the regression on BOTH axes:
//   - revert SetMaxIdleConns(0): the idle pooled read-mark from step (2) pins
//     the TRUNCATE (busy), so the WAL is NOT reclaimed and the assert fails.
//   - remove the explicit checkpoint: the WAL is never reset (auto-checkpoint is
//     off), so it stays large and the assert fails.
func TestLocalPollerCheckpointBoundsWAL(t *testing.T) {
	dbPath, w := newWALSourceWithChangeLog(t)
	defer func() { _ = w.Close() }()
	ctx := context.Background()

	growChangeLogWAL(t, w, 4000, 500)
	walSeeded := walBytes(t, dbPath)
	if walSeeded < 1<<20 { // sanity: there must be a fat WAL to reclaim
		t.Fatalf("expected a multi-MB WAL to reclaim, got %d bytes", walSeeded)
	}

	exec, err := localBackend(dbPath).openExec(ctx, true)
	if err != nil {
		t.Fatalf("open poller executor: %v", err)
	}
	defer func() { _ = exec.close() }()

	// (2) Poll once — a read connection is used and returned to the pool.
	if _, err := exec.pollChangeLog(ctx, 0, 100); err != nil {
		t.Fatalf("poll: %v", err)
	}
	// (3) Grow the WAL beyond that read's snapshot.
	growChangeLogWAL(t, w, 4000, 500)
	walGrown := walBytes(t, dbPath)

	// (4) Checkpoint must truncate the WAL to ~zero.
	if err := exec.checkpointWAL(ctx); err != nil {
		t.Fatalf("checkpointWAL: %v", err)
	}
	walAfter := walBytes(t, dbPath)

	t.Logf("WAL bytes: seeded=%d grown=%d afterCheckpoint=%d", walSeeded, walGrown, walAfter)
	// TRUNCATE drops the -wal to 0 (or a tiny header). Assert it collapsed well
	// below the grown size — a pinned checkpoint would leave it ~unchanged.
	if walAfter > walGrown/4 {
		t.Fatalf("WAL not reclaimed: after=%d is not << grown=%d "+
			"(idle read-mark pinned the checkpoint, or the checkpoint did not run)",
			walAfter, walGrown)
	}
}

// TestLocalPollerReleasesIdleConn pins that the poller's read pool does not
// retain idle connections (the load-bearing pin release). After a poll the pool
// must report zero idle connections.
func TestLocalPollerReleasesIdleConn(t *testing.T) {
	dbPath, w := newWALSourceWithChangeLog(t)
	defer func() { _ = w.Close() }()
	ctx := context.Background()

	exec, err := localBackend(dbPath).openExec(ctx, true)
	if err != nil {
		t.Fatalf("open poller executor: %v", err)
	}
	defer func() { _ = exec.close() }()

	le, ok := exec.(*localExecutor)
	if !ok {
		t.Fatalf("expected *localExecutor, got %T", exec)
	}
	if _, err := le.pollChangeLog(ctx, 0, 100); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if idle := le.db.Stats().Idle; idle != 0 {
		t.Fatalf("poller pool retained %d idle connection(s); Bug 167 requires SetMaxIdleConns(0) "+
			"so no idle read-mark pins the WAL checkpoint", idle)
	}
}

// spyExecutor counts checkpointWAL calls and is otherwise an empty no-op poller,
// to pin that the pump fires the checkpoint on its cadence.
type spyExecutor struct {
	checkpoints int64
}

func (s *spyExecutor) execDDL(context.Context, string) error { return nil }
func (s *spyExecutor) pollChangeLog(context.Context, int64, int) ([]rawChangeRow, error) {
	return nil, nil
}

func (s *spyExecutor) readFingerprints(context.Context) ([]fingerprintRow, error) { return nil, nil }

func (s *spyExecutor) changeLogExists(context.Context) (bool, error)      { return true, nil }
func (s *spyExecutor) maxChangeLogID(context.Context) (int64, error)      { return 0, nil }
func (s *spyExecutor) discoverTriggers(context.Context) ([]string, error) { return nil, nil }
func (s *spyExecutor) pruneChangeLogBatch(context.Context, int64, int64) (int64, error) {
	return 0, nil
}
func (s *spyExecutor) minChangeLogID(context.Context) (int64, error) { return 0, nil }
func (s *spyExecutor) pruneBatchSize() int64                         { return localPruneBatchSize }
func (s *spyExecutor) maxPollBatch() int                             { return 0 }
func (s *spyExecutor) checkpointWAL(context.Context) error {
	atomic.AddInt64(&s.checkpoints, 1)
	return nil
}
func (s *spyExecutor) vacuum(context.Context) error { return nil }
func (s *spyExecutor) changeLogStats(context.Context) (minID, count int64, err error) {
	return 0, 0, nil
}
func (s *spyExecutor) close() error { return nil }

// TestPumpFiresCheckpointOnCadence pins that the polling loop invokes
// checkpointWAL on the configured cadence (the wiring of the fix into pump).
func TestPumpFiresCheckpointOnCadence(t *testing.T) {
	spy := &spyExecutor{}
	r := &CDCReader{
		exec:               spy,
		pollInterval:       5 * time.Millisecond,
		batchSize:          defaultBatchSize,
		checkpointInterval: 20 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan ir.Change, cdcChannelBuffer)
	done := make(chan struct{})
	go func() {
		r.pump(ctx, 0, out)
		close(done)
	}()
	// Drain (the spy emits nothing, but be safe).
	go func() {
		for range out {
		}
	}()

	deadline := time.After(2 * time.Second)
	for atomic.LoadInt64(&spy.checkpoints) < 2 {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("pump did not fire checkpointWAL on cadence; got %d calls",
				atomic.LoadInt64(&spy.checkpoints))
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	cancel()
	<-done
}

// TestPumpCheckpointDisabled pins that a zero/negative cadence disables the
// periodic checkpoint (so the idle-release alone governs the WAL).
func TestPumpCheckpointDisabled(t *testing.T) {
	spy := &spyExecutor{}
	r := &CDCReader{
		exec:               spy,
		pollInterval:       5 * time.Millisecond,
		batchSize:          defaultBatchSize,
		checkpointInterval: 0,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	out := make(chan ir.Change, cdcChannelBuffer)
	go func() {
		for range out {
		}
	}()
	r.pump(ctx, 0, out)
	if n := atomic.LoadInt64(&spy.checkpoints); n != 0 {
		t.Fatalf("checkpoint cadence disabled but pump fired it %d time(s)", n)
	}
}

// failingCheckpointExecutor is a spyExecutor whose checkpointWAL fails
// while fail is true — drives the DEBUG→WARN escalation pin.
type failingCheckpointExecutor struct {
	spyExecutor
	fail bool
}

func (f *failingCheckpointExecutor) checkpointWAL(context.Context) error {
	if f.fail {
		return errors.New("database is locked")
	}
	return nil
}

// TestWALCheckpointFailureEscalatesToWarn pins the audit-2026-07-08
// §4.4 escalation boundary: consecutive checkpointWAL failures log at
// DEBUG below walCheckpointWarnThreshold, at WARN from the threshold
// on, and one success resets the streak so the next failure is DEBUG
// again.
func TestWALCheckpointFailureEscalatesToWarn(t *testing.T) {
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	exec := &failingCheckpointExecutor{fail: true}
	r := &CDCReader{exec: exec}
	ctx := context.Background()

	// Failures below the threshold stay quiet at WARN level.
	for i := 1; i < walCheckpointWarnThreshold; i++ {
		r.runWALCheckpoint(ctx)
		if out := buf.String(); strings.Contains(out, "WAL checkpoint failing repeatedly") {
			t.Fatalf("failure %d escalated to WARN before the threshold (%d): %q", i, walCheckpointWarnThreshold, out)
		}
	}

	// The threshold-th consecutive failure escalates, naming the streak.
	r.runWALCheckpoint(ctx)
	out := buf.String()
	if !strings.Contains(out, "WAL checkpoint failing repeatedly") {
		t.Fatalf("failure %d did not escalate to WARN: %q", walCheckpointWarnThreshold, out)
	}
	if !strings.Contains(out, `"consecutive_failures":3`) {
		t.Errorf("WARN does not carry the streak count: %q", out)
	}

	// One success resets the streak: the next failure is DEBUG again.
	buf.Reset()
	exec.fail = false
	r.runWALCheckpoint(ctx)
	exec.fail = true
	r.runWALCheckpoint(ctx)
	if out := buf.String(); strings.Contains(out, "WAL checkpoint failing repeatedly") {
		t.Fatalf("streak did not reset on success; post-success failure escalated: %q", out)
	}
	if r.walCheckpointFailures != 1 {
		t.Fatalf("walCheckpointFailures after reset+1 failure = %d; want 1", r.walCheckpointFailures)
	}
}
