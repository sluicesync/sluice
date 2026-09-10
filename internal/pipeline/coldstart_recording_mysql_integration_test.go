//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit A0909-P2b, on a real server: a MySQL-source `sync start` cold
// start leaves its progress recorded on the target.
//
// MySQL is the family the motivating operator report used — an AWS→GCP
// move of a PlanetScale MySQL database
// (docs/operator/cross-region-migration.md) — and it is precisely the
// family v0.148.0's cold-start visibility could not reach: the ADR-0079
// fast lane that owned the recording admits only a source exporting a
// SHAREABLE snapshot, which MySQL does not. Every MySQL cold start took
// the serial `runBulkCopyWithOpts` path and wrote nothing at all, so
// `sync status` reported the stream as absent for the whole copy.
//
// # What makes this evidence rather than a re-read of our own bookkeeping
//
// The check does not go through the pipeline's recorder, the migrate
// state store, or `ir.TableProgress`'s decoder. It queries the two
// control TABLES with raw SQL and compares against an INDEPENDENT
// expected value: the row count of the source table itself, taken with
// its own COUNT(*). If the recorder wrote a number nothing measured,
// that comparison is what fails.

package pipeline

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

func TestColdStartRecording_MySQLSerialLane_LeavesReadableState(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE recorded_users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE recorded_orders (
			id     BIGINT NOT NULL AUTO_INCREMENT,
			amount INT    NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO recorded_users (email) VALUES ('a@example.com'), ('b@example.com'), ('c@example.com');
		INSERT INTO recorded_orders (amount) VALUES (10), (20);
	`
	applyDDLMySQL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const streamID = "a0909-p2b-recording"
	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Errorf("streamer did not exit within 20s of cancel")
		}
	}()

	// The copy itself landing is the precondition, not the assertion.
	if !waitForRowCountMySQL(t, targetDSN, "recorded_users", 3, 90*time.Second) {
		t.Fatalf("bulk copy never delivered recorded_users")
	}
	if !waitForRowCountMySQL(t, targetDSN, "recorded_orders", 2, 60*time.Second) {
		t.Fatalf("bulk copy never delivered recorded_orders")
	}

	migrationID := "sync-" + streamID
	control := openMySQLControlReader(t, targetDSN)

	// The header row. Before A0909-P2b there was no row here AT ALL for a
	// MySQL cold start, which is the shape `sync status` cannot tell from
	// a dead process.
	//
	// SCOPE, stated so this check cannot read broader than it is: reaching
	// `complete` proves the header exists and was marked terminal, and
	// NOTHING about the phases in between — coldStartRunCopy's own
	// markRecordedColdStartComplete writes that value whatever the copy
	// recorded. The mutation run confirmed it: deleting the serial lane's
	// Recording wiring left this loop green and was caught only by the
	// per-table rows below. The phase LADDER (tables → bulk_copy →
	// identity_sync → indexes → constraints → views) is pinned against the
	// real runBulkCopyWithOpts by
	// TestSerialColdStartRecordsPhasesAndPerTableProgress.
	var (
		phase          string
		snapshotAnchor sql.NullString
	)
	deadline := time.Now().Add(60 * time.Second)
	for {
		qctx, qcancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := control.QueryRowContext(
			qctx,
			"SELECT phase, snapshot_anchor FROM sluice_migrate_state WHERE migration_id = ?", migrationID,
		).Scan(&phase, &snapshotAnchor)
		qcancel()
		if err == nil && phase == "complete" {
			break
		}
		if time.Now().After(deadline) {
			if err != nil {
				t.Fatalf("A0909-P2b: no recorded cold start for %q on the target after the copy landed "+
					"(`sync status` would report this stream as absent for the whole run): %v", migrationID, err)
			}
			t.Fatalf("A0909-P2b: the recorded cold start never reached phase `complete` (stuck at %q); "+
				"`sync status` keeps listing a finished run as in progress, with an age that only climbs — "+
				"the signal it tells operators means the run is DEAD", phase)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// The deliberate asymmetry, ground-truthed on a real server rather
	// than reasoned about: the SERIAL lane records no snapshot anchor, so
	// resumeStoppedColdStart keeps declining it exactly as it did before
	// this lane recorded anything. See coldstart_recording.go.
	if snapshotAnchor.Valid && snapshotAnchor.String != "" {
		t.Errorf("the MySQL serial lane recorded a snapshot anchor (%q); the stopped-cold-start resume ladder "+
			"would then offer to SKIP a copy on a lane it was never designed or measured against",
			snapshotAnchor.String)
	}

	// The per-table rows, compared against the SOURCE's own COUNT(*) —
	// an expected value neither the recorder nor the copy produced.
	for _, table := range []string{"recorded_users", "recorded_orders"} {
		wantRows := countRowsMySQL(t, sourceDSN, table)
		qctx, qcancel := context.WithTimeout(context.Background(), 10*time.Second)
		var progress string
		err := control.QueryRowContext(
			qctx,
			"SELECT progress FROM sluice_migrate_table_progress WHERE migration_id = ? AND table_name = ?",
			migrationID, table,
		).Scan(&progress)
		qcancel()
		if err != nil {
			t.Errorf("no recorded progress row for table %q: %v", table, err)
			continue
		}
		if !strings.Contains(progress, `"state":"complete"`) {
			t.Errorf("table %q recorded %s, want a complete state", table, progress)
		}
		// The compact wire form omits rows_copied when it is zero
		// (omitempty), so an absent count and a recorded zero look the
		// same here — which is exactly why the source's own number is
		// the thing being matched.
		wantCount := `"rows_copied":` + strconv.Itoa(wantRows)
		if !strings.Contains(progress, wantCount) {
			t.Errorf("table %q recorded %s, want it to carry %s (the source's own COUNT(*))", table, progress, wantCount)
		}
	}
}

// The multi-database fan-out, on a real server: N namespaces copied
// under ONE stream id record N distinct progress rows.
//
// This is the collision the ProgressNamespace qualifier exists for, and
// it is not hypothetical — the existing fan-out coverage
// (TestStreamer_MultiDatabase_MySQLToMySQL) uses two databases whose
// table is called `widgets` in both, which is what real fan-outs look
// like. Unqualified, the second database's copy upserts the first's row
// and the recorded run is short by every collision, silently, because
// nothing reads these rows back on this path today.
func TestColdStartRecording_MySQLMultiDatabase_KeysDoNotCollide(t *testing.T) {
	srcServer, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtHomeDSN, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()

	applyDDLMySQL(t, srcServer, `
		CREATE TABLE widgets (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (name) VALUES ('a-one'), ('a-two');
	`)
	shopDSN := mustCreateMySQLDatabase(t, srcServer, "shop_db")
	applyDDLMySQL(t, shopDSN, `
		CREATE TABLE widgets (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (name) VALUES ('b-one'), ('b-two'), ('b-three');
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const streamID = "a0909-p2b-fanout"
	streamer := &Streamer{
		Source:         mysqlEng,
		Target:         mysqlEng,
		SourceDSN:      serverDSN(t, srcServer),
		TargetDSN:      tgtHomeDSN,
		StreamID:       streamID,
		DatabaseFilter: DatabaseFilter{Include: []string{"source_db", "shop_db"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Errorf("streamer did not exit within 20s of cancel")
		}
	}()

	// The control tables live in the target DSN's home database.
	migrationID := "sync-" + streamID
	control := openMySQLControlReader(t, tgtHomeDSN)

	// Wait for the fan-out to finish — its terminal mark lands after the
	// LAST database, so `complete` means every namespace was copied.
	deadline := time.Now().Add(120 * time.Second)
	for {
		qctx, qcancel := context.WithTimeout(context.Background(), 10*time.Second)
		var phase string
		err := control.QueryRowContext(qctx,
			"SELECT phase FROM sluice_migrate_state WHERE migration_id = ?", migrationID).Scan(&phase)
		qcancel()
		if err == nil && phase == "complete" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A0909-P2b: the multi-database fan-out never recorded a complete cold start for %q "+
				"(last error: %v)", migrationID, err)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// The independent expected value: each SOURCE database's own COUNT(*).
	for _, tc := range []struct {
		key string
		dsn string
	}{
		{"source_db.widgets", srcServer},
		{"shop_db.widgets", shopDSN},
	} {
		wantRows := countRowsMySQL(t, tc.dsn, "widgets")
		qctx, qcancel := context.WithTimeout(context.Background(), 10*time.Second)
		var progress string
		err := control.QueryRowContext(
			qctx,
			"SELECT progress FROM sluice_migrate_table_progress WHERE migration_id = ? AND table_name = ?",
			migrationID, tc.key,
		).Scan(&progress)
		qcancel()
		if err != nil {
			t.Errorf("no recorded progress row under %q: %v — two namespaces sharing a table name collapsed "+
				"onto one key, so one database's copy was attributed to the other", tc.key, err)
			continue
		}
		if !strings.Contains(progress, `"state":"complete"`) {
			t.Errorf("%q recorded %s, want a complete state", tc.key, progress)
		}
		wantCount := `"rows_copied":` + strconv.Itoa(wantRows)
		if !strings.Contains(progress, wantCount) {
			t.Errorf("%q recorded %s, want it to carry %s (that source database's own COUNT(*))", tc.key, progress, wantCount)
		}
	}
}

// openMySQLControlReader opens ONE connection to the target for the raw
// control-table reads. Deliberately raw SQL rather than the engine's own
// MigrationStateStore: a check that reads back through the writer's
// decoder can only prove the writer agrees with itself.
func openMySQLControlReader(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
