//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the MySQL snapshot+CDC handoff. Boots a MySQL
// container with binlog enabled, seeds rows R1..R5, opens a snapshot
// stream, inserts R6 on a separate connection (so it commits *after*
// the snapshot's logical clock), and asserts:
//
//   - bulk-copy via stream.Rows yields exactly R1..R5 (no overlap),
//   - CDC via stream.Changes yields exactly the R6 insert (no gap).
//
// This is the canonical no-gap, no-overlap proof for the §4 chunk.

package mysql

import (
	"context"
	"database/sql"
	"sort"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// startMySQLForSnapshotCDC returns a DSN pointed at a freshly-reset
// `source_db` database on the shard's shared mysqld container (the
// same one startMySQLForCDC uses — binlog ROW + full row-image is
// the shared default). See shared_container_integration_test.go.
// The (dsn, cleanup) shape is preserved; cleanup is a no-op because
// TestMain owns teardown.
func startMySQLForSnapshotCDC(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	return newSharedDB(t, "source_db")
}

func applyMySQLSnap(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
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

// TestSnapshotStream_NoGapNoOverlap is the load-bearing test for §4.
//
// Sequence:
//
//  1. Seed R1..R5 (committed before snapshot).
//  2. Open SnapshotStream — captures binlog position P.
//  3. On a separate connection (NOT the snapshot conn), INSERT R6.
//     This commits at a position > P, AFTER the snapshot's logical clock.
//  4. Drain stream.Rows → assert exactly {R1..R5}.
//  5. Drain stream.Changes → assert exactly the R6 insert.
//
// The properties that make this load-bearing:
//
//   - If R6 appears in step 4, there's overlap (snapshot wasn't pinned
//     to the captured position).
//   - If R6 doesn't appear in step 5, there's a gap (CDC missed
//     events between snapshot and stream start).
func TestSnapshotStream_NoGapNoOverlap(t *testing.T) {
	dsn, cleanup := startMySQLForSnapshotCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO users (email) VALUES
			('r1@example.com'),
			('r2@example.com'),
			('r3@example.com'),
			('r4@example.com'),
			('r5@example.com');
	`
	applyMySQLSnap(t, dsn, seedDDL)

	eng := Engine{Flavor: FlavorVanilla}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Step 3 — concurrent insert on a SEPARATE connection. Same DSN
	// so it's the same database; different *sql.DB pool so the
	// snapshot tx doesn't see this connection's changes.
	applyMySQLSnap(t, dsn, "INSERT INTO users (email) VALUES ('r6@example.com');")

	// Step 4 — drain stream.Rows. Build the schema for the read.
	usersTable := schemaForUsers()
	bulkRows := drainAllRows(t, ctx, stream.Rows, usersTable)
	bulkEmails := emailsOf(bulkRows)
	want := []string{"r1@example.com", "r2@example.com", "r3@example.com", "r4@example.com", "r5@example.com"}
	if !equalStringSlices(bulkEmails, want) {
		t.Fatalf("bulk rows = %v; want exactly %v (overlap or missing rows)", bulkEmails, want)
	}

	// Step 5 — start CDC from the captured position. Should yield
	// exactly the R6 insert. Block until it shows up; cdc receive
	// loop runs in its own goroutine.
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	got := drainSnapshotChanges(t, ctx, changes, 1, 30*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d changes; want 1 (R6 insert)", len(got))
	}
	insR6, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}
	if email, _ := insR6.Row["email"].(string); email != "r6@example.com" {
		t.Errorf("R6 insert email = %#v; want r6@example.com", insR6.Row["email"])
	}
}

// TestSnapshotStream_ReleaseRowsClosesSnapshotTx exercises the MySQL
// counterpart of PG's Bug 21 fix. The corresponding PG test in
// internal/engines/postgres/cdc_snapshot_integration_test.go pinned
// the property that ReleaseRowsFn commits the snapshot tx and
// releases the AccessShareLock so the source is free to take DDL.
// On MySQL the equivalent is the MDL_SHARED_READ that
// START TRANSACTION WITH CONSISTENT SNAPSHOT acquires with
// dur=TRANSACTION on every table the snapshot reads — held until
// COMMIT. Until task #34 (this commit) wired the MySQL engine's
// ReleaseRowsFn, that MDL stayed alive for the entire streamer
// lifetime, blocking operator ALTERs even with ALGORITHM=INSTANT
// (the brief MDL upgrade INSTANT still needs would queue forever
// behind the snapshot's never-released SHARED_READ). This was the
// root cause of the long-deferred Chunk E pin in
// streamer_schema_history_cross_integration_test.go (task #28); see
// that file's docstring for the diagnostic journey.
//
// Asserts:
//
//   - ReleaseRows releases the SHARED_READ MDL — performance_schema.
//     metadata_locks shows zero TABLE SHARED_READ dur=TRANSACTION
//     entries on `users` after release.
//   - CDC continues working after ReleaseRows — a post-release INSERT
//     still surfaces on the change stream.
//   - ALTER TABLE on the source after ReleaseRows succeeds promptly
//     (pre-fix it would block on the unreleased MDL).
//   - Calling ReleaseRows twice is a no-op; Close is idempotent with
//     ReleaseRows.
func TestSnapshotStream_ReleaseRowsClosesSnapshotTx(t *testing.T) {
	dsn, cleanup := startMySQLForSnapshotCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('a@example.com'), ('b@example.com');
	`
	applyMySQLSnap(t, dsn, seedDDL)

	eng := Engine{Flavor: FlavorVanilla}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Drain the bulk rows so we're at the post-bulk-copy state the
	// orchestrator would be at when it calls ReleaseRows.
	usersTable := schemaForUsers()
	_ = drainAllRows(t, ctx, stream.Rows, usersTable)

	// Pre-release: the snapshot tx holds MDL_SHARED_READ on users.
	// We don't assert pre-state strictly (its presence depends on
	// performance_schema's flush cadence after the snapshot tx
	// started); the load-bearing assertion is post-release.

	if err := stream.ReleaseRows(); err != nil {
		t.Fatalf("ReleaseRows: %v", err)
	}

	// Post-release: zero TABLE SHARED_READ MDL on users. Pre-fix this
	// would be 1 (the snapshot tx's hold).
	if n := countSnapshotMDLOnUsers(t, ctx, dsn); n != 0 {
		t.Errorf("expected 0 SHARED_READ MDL entries on users after ReleaseRows; got %d", n)
	}

	// ALTER on the source must succeed promptly — pre-fix this would
	// block on the unreleased MDL. We give it 30s; the actual
	// expectation is sub-second.
	alterCtx, alterCancel := context.WithTimeout(ctx, 30*time.Second)
	defer alterCancel()
	alterDB, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open for ALTER: %v", err)
	}
	defer func() { _ = alterDB.Close() }()
	alterStart := time.Now()
	if _, err := alterDB.ExecContext(alterCtx, "ALTER TABLE users ADD COLUMN nickname VARCHAR(64), ALGORITHM=INSTANT"); err != nil {
		t.Fatalf("post-release ALTER failed: %v (pre-fix this would have hung on MDL)", err)
	}
	if elapsed := time.Since(alterStart); elapsed > 5*time.Second {
		t.Errorf("post-release ALTER took %v; expected sub-second (suspect MDL still held)", elapsed)
	}

	// CDC still works: open StreamChanges after release, commit a row,
	// watch it surface. Need to issue a multi-statement DDL+INSERT or
	// the post-ALTER schema is invisible to the prior position; the
	// existing CDC reader was opened with the pre-ALTER schema in
	// mind but go-mysql binlog reader re-discovers it from the binlog
	// QUERY event.
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges after ReleaseRows: %v", err)
	}
	applyMySQLSnap(t, dsn, "INSERT INTO users (email, nickname) VALUES ('c@example.com', 'cc')")
	got := drainSnapshotChanges(t, ctx, changes, 1, 30*time.Second)
	if len(got) < 1 {
		t.Fatalf("got %d post-release changes; want >=1", len(got))
	}

	// Idempotency: a second ReleaseRows is a no-op.
	if err := stream.ReleaseRows(); err != nil {
		t.Errorf("second ReleaseRows: %v; want nil", err)
	}
}

// countSnapshotMDLOnUsers returns the count of TABLE SHARED_READ MDL
// entries on `users` with dur=TRANSACTION (the lock-shape that a
// REPEATABLE-READ + WITH CONSISTENT SNAPSHOT transaction holds).
// Used by the task #34 pin to assert the snapshot tx's MDL has been
// released.
func countSnapshotMDLOnUsers(t *testing.T, ctx context.Context, dsn string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const q = `
		SELECT COUNT(*) FROM performance_schema.metadata_locks
		WHERE OBJECT_TYPE     = 'TABLE'
		  AND OBJECT_NAME     = 'users'
		  AND LOCK_TYPE       = 'SHARED_READ'
		  AND LOCK_DURATION   = 'TRANSACTION'`
	var n int
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("count snapshot MDL: %v", err)
	}
	return n
}

// schemaForUsers returns an [ir.Table] matching the seed DDL above —
// just enough for the RowReader to issue its SELECT and decode rows.
func schemaForUsers() *ir.Table {
	return &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
	}
}

// drainAllRows reads every row that ReadRows produces for the given
// table. Returns the slice in arrival order.
func drainAllRows(t *testing.T, ctx context.Context, rr ir.RowReader, table *ir.Table) []ir.Row {
	t.Helper()
	ch, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var out []ir.Row
	for row := range ch {
		out = append(out, row)
	}
	return out
}

// drainSnapshotChanges is the same drainer pattern used by the
// standalone CDC integration test: take up to want events with a
// timeout. Returns whatever it has if the timeout fires or the
// channel closes.
func drainSnapshotChanges(
	t *testing.T,
	ctx context.Context,
	changes <-chan ir.Change,
	want int,
	timeout time.Duration,
) []ir.Change {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	got := make([]ir.Change, 0, want)
	for len(got) < want {
		select {
		case c, ok := <-changes:
			if !ok {
				return got
			}
			// Skip orthogonal infra events — this helper captures
			// row events for shape assertions. TxBegin/TxCommit are
			// tx-boundary bookkeeping (ADR-0027); ir.SchemaSnapshot is
			// the ADR-0049 schema-history boundary event (emitted at
			// first-touch + each true DDL delta). Neither is DML.
			switch c.(type) {
			case ir.TxBegin, ir.TxCommit, ir.SchemaSnapshot:
				continue
			}
			got = append(got, c)
		case <-deadline.C:
			return got
		case <-ctx.Done():
			return got
		}
	}
	return got
}

func emailsOf(rows []ir.Row) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		s, _ := r["email"].(string)
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
