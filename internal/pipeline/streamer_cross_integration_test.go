//go:build integration

// Cross-engine continuous-sync integration test for pipeline.Streamer:
// MySQL source → Postgres target. This is the streaming counterpart of
// migrate_cross_integration_test.go (which exercises the snapshot-only
// orchestrator). Together they validate that the IR contract holds for
// both bulk-copy and CDC paths across the engine pair.
//
// The schema deliberately includes a TINYINT(1) column so the test
// exercises the binlog int8 → cross-engine bool path end-to-end —
// that path isn't covered by any same-engine test, since same-engine
// CDC keeps the value in its native int8 form on both sides.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestStreamer_MySQLToPostgres is the cross-engine streaming spine
// test. Phases:
//
//	A. Boot MySQL (binlog) and Postgres (wal_level=logical).
//	B. Seed MySQL with users(id, email, active) + R1.
//	C. Streamer{Source: mysql, Target: pg} runs.
//	D. Wait for bulk-copy to land R1 on PG.
//	E. INSERT R2 on MySQL → flows to PG via CDC.
//	F. UPDATE R1's active flag on MySQL → verify on PG (binlog
//	   int8 → ir.Boolean → PG bool conversion path).
//	G. DELETE R2 on MySQL → verify gone on PG.
//	H. Cancel ctx, verify clean shutdown.
func TestStreamer_MySQLToPostgres(t *testing.T) {
	// Two containers — MySQL is the source (we only need binlog),
	// Postgres is the target (no wal_level=logical needed: PG is
	// the *target*, not the CDC source). startPostgresLogical's
	// wal_level=logical is harmless extra config but irrelevant for
	// this test.
	mysqlSourceDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()

	_, pgTargetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE users (
			id     BIGINT       NOT NULL AUTO_INCREMENT,
			email  VARCHAR(255) NOT NULL,
			active TINYINT(1)   NOT NULL DEFAULT 1,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email, active) VALUES ('r1@example.com', 1);
	`
	applyMySQLDDL(t, mysqlSourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSourceDSN,
		TargetDSN: pgTargetDSN,
		StreamID:  "test-cross-mysql-pg",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// ---- Phase D: bulk-copy lands R1 on PG ----
	if !waitForRowCount(t, pgTargetDSN, "users", 1, 60*time.Second) {
		t.Fatalf("phase D: bulk copy never delivered R1 to PG target")
	}
	// Sanity: active=true on PG (TINYINT(1) → bool through bulk copy).
	if got := readActiveByEmail(t, pgTargetDSN, "r1@example.com"); got == nil || !*got {
		t.Errorf("phase D: r1.active on PG = %v; want true (bulk-copy TINYINT(1)→bool)", got)
	}

	// ---- Phase E: INSERT R2 on MySQL flows through CDC ----
	applyMySQLDDL(t, mysqlSourceDSN,
		"INSERT INTO users (email, active) VALUES ('r2@example.com', 0);")
	if !waitForRowCount(t, pgTargetDSN, "users", 2, 30*time.Second) {
		t.Fatalf("phase E: CDC INSERT never delivered R2 to PG target")
	}
	// active=false on the new row — exercises binlog int8(0) → bool path.
	if got := readActiveByEmail(t, pgTargetDSN, "r2@example.com"); got == nil || *got {
		t.Errorf("phase E: r2.active on PG = %v; want false (CDC INSERT TINYINT(1)=0 → bool)", got)
	}

	// ---- Phase F: UPDATE R1 flips active flag ----
	applyMySQLDDL(t, mysqlSourceDSN,
		"UPDATE users SET active = 0 WHERE email = 'r1@example.com';")
	if !waitForActiveByEmail(t, pgTargetDSN, "r1@example.com", false, 30*time.Second) {
		got := readActiveByEmail(t, pgTargetDSN, "r1@example.com")
		t.Fatalf("phase F: r1.active on PG = %v; want false within 30s (CDC UPDATE never propagated)", got)
	}

	// ---- Phase G: DELETE R2 ----
	applyMySQLDDL(t, mysqlSourceDSN,
		"DELETE FROM users WHERE email = 'r2@example.com';")
	if !waitForRowAbsentByEmail(t, pgTargetDSN, "r2@example.com", 30*time.Second) {
		t.Fatalf("phase G: CDC DELETE never removed r2 from PG target")
	}
	// And R1 should still be there with active=false.
	if n := countRows(t, pgTargetDSN, "users"); n != 1 {
		t.Errorf("phase G: PG users row count = %d; want 1", n)
	}

	// ---- Phase H: clean shutdown ----
	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("phase H: Streamer.Run did not return after ctx cancel")
	}
}

// readActiveByEmail returns the bool value of the active column for
// the row with the given email, or nil if no such row exists. Returns
// nil on any query error too — callers treat absent and erroring as
// the same "not yet" state for polling helpers.
func readActiveByEmail(t *testing.T, dsn, email string) *bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var active bool
	err = db.QueryRowContext(ctx,
		`SELECT active FROM users WHERE email = $1`, email,
	).Scan(&active)
	if err != nil {
		return nil
	}
	return &active
}

// waitForActiveByEmail polls the PG target until users(email).active
// equals want, or the timeout fires. Used by the CDC-UPDATE phase
// where the change is async.
func waitForActiveByEmail(t *testing.T, dsn, email string, want bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := readActiveByEmail(t, dsn, email); got != nil && *got == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// waitForRowAbsentByEmail polls the PG target until no row matches
// the email, or the timeout fires. Used by the CDC-DELETE phase.
func waitForRowAbsentByEmail(t *testing.T, dsn, email string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var n int
		queryErr := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM users WHERE email = $1`, email,
		).Scan(&n)
		cancel()
		_ = db.Close()
		if queryErr == nil && n == 0 {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
