//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Track 1c — Phase B item 1 (reader level): GTID-mode position-loss
// detection via verifyGTIDSetReachable.
//
// Phase-A ground-truth: the streamer's snapshot→CDC handoff always
// persists a file/pos position even on a gtid_mode=ON source (the
// snapshot path captures SHOW MASTER STATUS → positionModeFilePos).
// So the GTID branch of verifyPositionResumable —
// verifyGTIDSetReachable, which runs GTID_SUBSET(@@global.gtid_purged,
// resume) — is reached ONLY when a caller hands the reader a GTID
// position directly (position-from-manifest, or a reader opened at a
// GTID bookmark). The streamer-level chaos tests
// (streamer_mysql_position_loss_chaos_integration_test.go) cover the
// file/pos fall-through; THIS test pins the GTID branch at the unit
// of code that actually owns it: the CDC reader.
//
// Oracle (loud-failure tenet floor): a resume against a GTID position
// whose GTIDs have been purged past (the PlanetScale "down past
// 3-day binlog retention; gtid_purged advanced" mechanism) must be
// detected LOUDLY — StreamChanges returns an error wrapping
// ir.ErrPositionInvalid — never a silent skip. errors.Is(err,
// ir.ErrPositionInvalid) is the exact contract the streamer's
// ADR-0022 fall-through keys on (streamer.go:1037), so proving the
// reader sets it proves the recovery chain fires.
//
// Reuses startMySQLForCDC's shape but adds gtid_mode=ON (the reader's
// resolveStartPosition auto-detects GTID mode and uses the GTID
// position primitive). applyMySQL / drainChanges are reused verbatim
// from cdc_reader_integration_test.go.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// startMySQLGTIDForCDC boots a MySQL container with binlog + GTID
// mode. Sibling of startMySQLForCDC; the gtid-mode flags make the
// reader's resolveStartPosition auto-detect GTID and emit GTID-mode
// positions (the ones verifyGTIDSetReachable validates on resume).
//
// Why this helper keeps booting its own container (unlike the rest
// of the engine's integration tests, which now share one mysqld via
// TestMain — see shared_container_integration_test.go): this test's
// PURGE BINARY LOGS mutates *global* binlog state on the server, so
// running it against the shared container would truncate every
// other CDC test's binlog history mid-shard. One extra container
// boot per shard run is the price of test isolation here.
func startMySQLGTIDForCDC(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	// Task #12 Phase B: this per-test helper bypasses the shared
	// TestMain container (the test's PURGE BINARY LOGS would truncate
	// other tests' binlog history), so it doesn't get task #60's
	// retry path automatically. The same wait-until-ready flake that
	// hits the shared container hits this boot too — instrumented by
	// PR #59's CI runs where this test failed with the same shape.
	// Apply the same retry schedule as ensureSharedMySQL.
	var (
		container *mysqltc.MySQLContainer
		lastErr   error
	)
	for attempt := 1; attempt <= sharedMySQLBootAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), sharedMySQLBootTimeout)
		c, err := mysqltc.Run(
			ctx,
			"mysql:8.0",
			mysqltc.WithDatabase("source_db"),
			mysqltc.WithUsername("root"),
			mysqltc.WithPassword("rootpw"),
			testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Cmd: []string{
						"mysqld",
						"--server-id=1",
						"--log-bin=mysql-bin",
						"--binlog-format=ROW",
						"--binlog-row-image=FULL",
						"--gtid-mode=ON",
						"--enforce-gtid-consistency=ON",
					},
				},
			}),
		)
		cancel()
		if err == nil {
			container = c
			if attempt > 1 {
				log.Printf("startMySQLGTIDForCDC boot attempt %d/%d succeeded",
					attempt, sharedMySQLBootAttempts)
			}
			break
		}
		if c != nil {
			_ = c.Terminate(context.Background())
		}
		lastErr = err
		if attempt < sharedMySQLBootAttempts {
			backoff := sharedMySQLBootBackoff(attempt)
			log.Printf("startMySQLGTIDForCDC boot attempt %d/%d failed: %v; retrying in %s",
				attempt, sharedMySQLBootAttempts, err, backoff)
			time.Sleep(backoff)
			continue
		}
		log.Printf("startMySQLGTIDForCDC boot attempt %d/%d failed: %v; giving up",
			attempt, sharedMySQLBootAttempts, err)
	}
	if container == nil {
		t.Fatalf("start container: %d attempts exhausted: %v", sharedMySQLBootAttempts, lastErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	conn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return conn, terminate
}

// TestCDCReader_GTIDPositionLoss_DetectedLoud is the reader-level
// Phase-B GTID oracle.
//
//  1. Boot gtid_mode=ON MySQL, seed users.
//  2. Open reader at "current"; INSERT a row; drain it; capture the
//     emitted GTID-mode position (proves auto-detect picked GTID).
//  3. Close the reader.
//  4. Generate more txns + FLUSH + PURGE BINARY LOGS so
//     @@global.gtid_purged advances PAST the captured resume set
//     (retention-exceeded).
//  5. Pre-assert the purge actually moved gtid_purged past the
//     resume set (GTID_SUBSET == 0) — keeps the test honest; a no-op
//     purge would otherwise pass for the wrong reason.
//  6. Open a NEW reader and StreamChanges from the captured GTID
//     position. Assert the call returns an error wrapping
//     ir.ErrPositionInvalid (loud detection; the streamer's ADR-0022
//     fall-through trigger).
func TestCDCReader_GTIDPositionLoss_DetectedLoud(t *testing.T) {
	dsn, cleanup := startMySQLGTIDForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQL(t, dsn, seedDDL)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges (initial): %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	applyMySQL(t, dsn, "INSERT INTO users (email) VALUES ('capture@example.com')")
	got := drainChanges(t, ctx, changes, 1, 30*time.Second)
	if len(got) != 1 {
		t.Fatalf("initial: got %d changes; want 1", len(got))
	}
	capturedPos := got[0].Pos()
	if capturedPos.Token == "" {
		t.Fatal("captured position token is empty")
	}

	// Confirm auto-detect picked GTID mode (the branch under test).
	decoded, ok, derr := decodeBinlogPos(capturedPos)
	if derr != nil || !ok {
		t.Fatalf("decode captured position: ok=%v err=%v", ok, derr)
	}
	if decoded.Mode != positionModeGTID {
		t.Fatalf("captured position mode = %q; want %q (gtid_mode=ON source should "+
			"yield a GTID-mode reader position — if this fails the GTID resume-"+
			"validation branch is not being exercised)", decoded.Mode, positionModeGTID)
	}
	if decoded.GTIDSet == "" {
		t.Fatal("captured GTID position has empty gtid_set")
	}
	t.Logf("captured GTID resume set = %q", decoded.GTIDSet)

	// Close the reader before mutating source binlog state.
	if c, ok := rdr.(interface{ Close() error }); ok {
		_ = c.Close()
	}

	// ---- Advance gtid_purged past the captured resume set. ----
	applyMySQL(t, dsn, "INSERT INTO users (email) VALUES ('post-1@example.com')")
	applyMySQL(t, dsn, "FLUSH BINARY LOGS")
	applyMySQL(t, dsn, "INSERT INTO users (email) VALUES ('post-2@example.com')")
	applyMySQL(t, dsn, "FLUSH BINARY LOGS")
	purgeAllButLatestBinlogMySQL(t, dsn)

	// Honest-test guard: GTID_SUBSET(@@global.gtid_purged, resume)
	// must now be 0. If it's still 1 the purge didn't advance
	// gtid_purged past the resume set and the test would validate
	// warm-resume rather than the ErrPositionInvalid path.
	assertGTIDPurgedPastResume(t, dsn, decoded.GTIDSet)

	// ---- Resume from the now-unreachable GTID position. ----
	rdr2, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader (resume): %v", err)
	}
	defer func() {
		if c, ok := rdr2.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	_, streamErr := rdr2.StreamChanges(ctx, capturedPos)
	if streamErr == nil {
		t.Fatal("PHASE-B VERDICT (GTID position-loss): StreamChanges returned nil error " +
			"resuming from a GTID position whose GTIDs were purged past — SILENT skip risk; " +
			"verifyGTIDSetReachable did not refuse. This violates the loud-failure floor.")
	}
	if !errors.Is(streamErr, ir.ErrPositionInvalid) {
		t.Fatalf("PHASE-B VERDICT (GTID position-loss): StreamChanges errored but NOT with "+
			"ir.ErrPositionInvalid (got %v). The streamer's ADR-0022 fall-through keys on "+
			"errors.Is(err, ir.ErrPositionInvalid) (streamer.go:1037); without the wrap the "+
			"cold-start recovery would not fire and the operator would get a fatal stall.", streamErr)
	}
	t.Logf("PHASE-B VERDICT (GTID position-loss): LOUD + ACTIONABLE — StreamChanges refused with "+
		"%v (wraps ir.ErrPositionInvalid → streamer ADR-0022 cold-start fall-through fires). "+
		"Oracle satisfied.", streamErr)
}

// purgeAllButLatestBinlogMySQL is the engine-package-local twin of
// the pipeline package's purgeAllButLatestBinlog. Re-declared here
// (not shared) because the helper is package-private to the pipeline
// test package and this test lives in the mysql engine package — the
// same two-engines-two-packages rationale the resume helpers document.
func purgeAllButLatestBinlogMySQL(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}
	cols, err := rows.Columns()
	if err != nil {
		_ = rows.Close()
		t.Fatalf("SHOW BINARY LOGS columns: %v", err)
	}
	holders := make([]any, len(cols))
	dest := make([]any, len(cols))
	for i := range dest {
		holders[i] = &dest[i]
	}
	var latest string
	for rows.Next() {
		if err := rows.Scan(holders...); err != nil {
			_ = rows.Close()
			t.Fatalf("scan: %v", err)
		}
		switch v := dest[0].(type) {
		case string:
			latest = v
		case []byte:
			latest = string(v)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("rows.Err: %v", err)
	}
	_ = rows.Close()
	if latest == "" {
		t.Fatal("SHOW BINARY LOGS returned no rows")
	}
	if _, err := db.ExecContext(ctx, "PURGE BINARY LOGS TO '"+latest+"'"); err != nil {
		t.Fatalf("PURGE BINARY LOGS TO %q: %v", latest, err)
	}
}

// assertGTIDPurgedPastResume fails the test unless
// GTID_SUBSET(@@global.gtid_purged, resumeSet) == 0 — the exact
// predicate verifyGTIDSetReachable evaluates. Mirroring it here keeps
// the test honest: the ErrPositionInvalid assertion is only
// meaningful if the source genuinely purged past the resume set.
func assertGTIDPurgedPastResume(t *testing.T, dsn, resumeSet string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var purged string
	if err := db.QueryRowContext(ctx, "SELECT @@global.gtid_purged").Scan(&purged); err != nil {
		t.Fatalf("read gtid_purged: %v", err)
	}
	var subset int
	if err := db.QueryRowContext(
		ctx,
		"SELECT GTID_SUBSET(@@global.gtid_purged, ?)", resumeSet,
	).Scan(&subset); err != nil {
		t.Fatalf("GTID_SUBSET: %v", err)
	}
	if subset == 1 {
		t.Fatalf("retention NOT exceeded: GTID_SUBSET(gtid_purged=%q, resume=%q)=1 — "+
			"purge did not advance gtid_purged past the resume set; the test would "+
			"validate warm-resume, not the ErrPositionInvalid path", purged, resumeSet)
	}
	t.Logf("retention-exceeded confirmed: gtid_purged=%q is NOT a subset of resume=%q (GTID_SUBSET=0)",
		purged, resumeSet)
}
