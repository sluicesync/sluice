//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end integration test for the snapshot+CDC orchestrator
// (pipeline.Streamer). Same-engine Postgres → Postgres: the source
// is seeded, the streamer captures a snapshot, runs bulk-copy on the
// target, then streams ongoing changes through a stub ChangeApplier
// that records every event received.

package pipeline

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startPostgresLogical boots a Postgres container with wal_level=
// logical. Mirrors the snapshot-CDC integration helper in the
// postgres engine package (kept local so this test file is
// self-contained).
func startPostgresLogical(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := pgtc.Run(
		ctx,
		// Task #68: pre-baked PG image. See
		// pg_prebaked_integration_test.go for the full rationale.
		pgPrebakedImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_wal_senders=8",
					"-c", "max_replication_slots=8",
				},
			},
		}),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("pgx", srcConn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}

	tgtConn, err := buildPGDSN(srcConn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}
	return srcConn, tgtConn, terminate
}

// recordingApplier is the stub [ir.ChangeApplier] for this test. It
// drains the change channel into a slice the test inspects, with
// per-change synchronisation so tests can wait for "N events seen".
type recordingApplier struct {
	mu      sync.Mutex
	changes []ir.Change

	// sig fires once per received change. Buffered to a generous
	// size so the test never blocks the applier goroutine.
	sig chan struct{}
}

func newRecordingApplier() *recordingApplier {
	return &recordingApplier{sig: make(chan struct{}, 64)}
}

// EnsureControlTable is a no-op for the stub — the recording
// applier doesn't talk to a database. The Streamer still calls
// this method as part of its startup flow.
func (a *recordingApplier) EnsureControlTable(_ context.Context) error { return nil }

// ReadPosition always reports "no row" so the Streamer takes the
// cold-start path. Tests covering warm resume use a real engine
// applier rather than this stub.
func (a *recordingApplier) ReadPosition(_ context.Context, _ string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

// ListStreams returns an empty slice — the recording applier
// doesn't talk to a real control table, and tests covering the
// CLI's `sync status` command go through the engine appliers.
func (a *recordingApplier) ListStreams(_ context.Context) ([]ir.StreamStatus, error) {
	return []ir.StreamStatus{}, nil
}

// RequestStop is a no-op for the stub. Tests that exercise the
// stop-signal flow use a real engine applier so the polling loop
// has a real control table to read.
func (a *recordingApplier) RequestStop(_ context.Context, _ string) error        { return nil }
func (a *recordingApplier) ClearStopRequested(_ context.Context, _ string) error { return nil }

func (a *recordingApplier) Apply(ctx context.Context, _ string, changes <-chan ir.Change) error {
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				return nil
			}
			// Skip orthogonal infra events — the recording applier
			// captures row events for shape assertions. TxBegin/
			// TxCommit are tx-boundary bookkeeping (ADR-0027);
			// ir.SchemaSnapshot is the ADR-0049 schema-history
			// boundary event (a reader emits one at first-touch +
			// each true DDL delta). Neither is DML. Without this
			// skip, the leading first-touch SchemaSnapshot races the
			// data inserts: waitFor(1) returns after recording just
			// the snapshot and the test asserts on a stream that
			// never had a chance to dispatch the row events yet
			// (CI run 26127255685 surfaced this).
			switch c.(type) {
			case ir.TxBegin, ir.TxCommit, ir.SchemaSnapshot:
				continue
			}
			a.mu.Lock()
			a.changes = append(a.changes, c)
			a.mu.Unlock()
			select {
			case a.sig <- struct{}{}:
			default:
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *recordingApplier) snapshot() []ir.Change {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]ir.Change, len(a.changes))
	copy(out, a.changes)
	return out
}

// waitFor waits for the recording applier to accumulate at least
// `n` total changes, or returns false on timeout / ctx cancellation.
func (a *recordingApplier) waitFor(ctx context.Context, n int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		a.mu.Lock()
		got := len(a.changes)
		a.mu.Unlock()
		if got >= n {
			return true
		}
		select {
		case <-a.sig:
		case <-deadline.C:
			return false
		case <-ctx.Done():
			return false
		}
	}
}

// TestStreamer_PostgresToPostgres exercises Streamer.Run end-to-end:
// snapshot capture → bulk-copy of seed rows → CDC of post-snapshot
// inserts. Asserts that bulk-copied rows reach the target table and
// that post-snapshot changes flow through the recording applier.
func TestStreamer_PostgresToPostgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;

		INSERT INTO users (email) VALUES
			('alice@example.com'),
			('bob@example.com');
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	applier := newRecordingApplier()
	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		Applier:   applier,
	}

	// Run the streamer in a goroutine; cancel via ctx when we're done.
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// The old blind 2s start-up sleep served two purposes; both are now
	// waited for explicitly. First, the replication slot must exist
	// before the finite CDC insert below commits — a commit that lands
	// BEFORE the slot is created is captured by neither the snapshot nor
	// CDC (the AIMD "0/250" flake class; see [waitForSourceSlot]).
	waitForSourceSlot(t, sourceDSN, 60*time.Second)

	// Second, verify bulk copy reached the target. (Streamer doesn't
	// expose a "bulk-copy done" signal; poll for the seed rows, then
	// pin the exact count — bulk-copy must not duplicate.)
	if !waitForRowCount(t, targetDSN, "users", 2, 60*time.Second) {
		t.Fatalf("target users rows = %d after bulk copy; want 2", pollRowCount(targetDSN, "users"))
	}
	if targetRows := countRows(t, targetDSN, "users"); targetRows != 2 {
		t.Errorf("target users rows = %d after bulk copy; want 2", targetRows)
	}

	// Insert a new row on the source — should flow through CDC.
	applyDDL(t, sourceDSN, "INSERT INTO users (email) VALUES ('carol@example.com');")

	// Wait for the recording applier to see one Insert.
	if !applier.waitFor(streamCtx, 1, 30*time.Second) {
		t.Fatalf("applier did not receive any change events within timeout")
	}

	// Stop the streamer and let it return cleanly.
	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Streamer.Run returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}

	// Inspect the recorded changes: at least one ir.Insert with
	// email = carol@example.com.
	got := applier.snapshot()
	if len(got) < 1 {
		t.Fatalf("recording applier received %d changes; want ≥1", len(got))
	}
	var carolSeen bool
	for _, c := range got {
		ins, ok := c.(ir.Insert)
		if !ok {
			continue
		}
		if email, _ := ins.Row["email"].(string); email == "carol@example.com" {
			carolSeen = true
			break
		}
	}
	if !carolSeen {
		t.Errorf("expected an ir.Insert for carol@example.com; got %d events without it", len(got))
	}
}

func applyDDL(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("apply sql: %v", err)
	}
}

func countRows(t *testing.T, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}
