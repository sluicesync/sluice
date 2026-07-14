//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0157 — schema-drift notification, end-to-end on a real MySQL-source
// stream. This drives the same unprovable-RENAME refusal as
// TestStreamer_SchemaForward_RenameColumn_MySQL_Refuses (a MySQL source has
// no stable column id, so a RENAME cannot be proven and refuses loudly),
// but wires a capturing notifier into the streamer and asserts the operator
// is paged EXACTLY ONCE with the recovery hint in the body — while the sync
// still stalls with no data loss (the intercept refuses before issuing the
// target DDL, exactly as before).

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/notify"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

func TestStreamer_SchemaDriftNotify_MySQLRename_FiresOnce(t *testing.T) {
	mysqlDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, mysqlDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL,
			old_label VARCHAR(64)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, old_label) VALUES (1, 'alpha', 'x'), (2, 'beta', 'y');")

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	// A capturing notifier wired via the test seam stands in for the
	// webhook/Slack/SMTP sink set — no live endpoint needed.
	captured := &capturingNotifier{}
	streamer := &Streamer{
		Source:                     myEng,
		Target:                     myEng,
		SourceDSN:                  mysqlDSN,
		TargetDSN:                  targetDSN,
		StreamID:                   "test-schema-drift-notify",
		schemaDriftNotifierForTest: captured,
		// SuppressSchemaDriftNotify defaults false ⇒ ENABLED (zero-value-safe).
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	// PRIME to a CDC→CDC boundary (a rename against the seed would be SKIPPED
	// by the seed-guard, not refused).
	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets ADD COLUMN _prime_col INT;")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, _prime_col) VALUES (100, 'prime', 1);")
	if !waitForRowCountMySQL(t, targetDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("prime: prime row never landed — seed→CDC boundary not processed")
	}

	// The unprovable MySQL RENAME on a CDC→CDC boundary → refuse loudly + stall.
	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets RENAME COLUMN old_label TO new_label;")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, new_label) VALUES (3, 'gamma', 'z');")

	var streamErr error
	select {
	case streamErr = <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatal("streamer did not surface refuse-loudly on MySQL RENAME within timeout")
	}
	if streamErr == nil {
		t.Fatal("streamer returned nil; expected refuse on unprovable MySQL RENAME (the stall)")
	}
	if !strings.Contains(streamErr.Error(), "RENAME COLUMN") {
		t.Fatalf("unexpected error (want ambiguous-rename refusal): %v", streamErr)
	}

	// EXACTLY ONE schema-drift notification fired (edge-once).
	if got := captured.count(); got != 1 {
		t.Fatalf("schema-drift notifications fired = %d; want exactly 1", got)
	}
	n := captured.got[0]
	if n.Category != notify.CategorySchemaDrift {
		t.Errorf("notification category = %q; want schema-drift", n.Category)
	}
	if n.Level != notify.LevelCritical {
		t.Errorf("notification level = %q; want critical", n.Level)
	}
	if !strings.Contains(n.StreamID, "test-schema-drift-notify") {
		t.Errorf("notification stream id = %q; want the stream", n.StreamID)
	}
	// The body carries the offending shape AND the recovery hint (actionable).
	for _, want := range []string{"RENAME COLUMN", "cannot be auto-forwarded", "recovery: drained model", "sluice sync stop --wait"} {
		if !strings.Contains(n.Body, want) {
			t.Errorf("notification body missing %q:\n%s", want, n.Body)
		}
	}

	// No data loss: the intercept refused BEFORE issuing the target DDL, so
	// new_label never appears on the target (the stall preserved fidelity).
	tgtDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	if waitForMySQLColumn(t, tgtDB, "widgets", "new_label", true, 5*time.Second) {
		t.Errorf("MySQL target widgets.new_label exists — intercept did NOT refuse (data-loss risk)")
	}
}
