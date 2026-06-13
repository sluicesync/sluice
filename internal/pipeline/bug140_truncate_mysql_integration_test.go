//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 140 regression pin: a source-side TRUNCATE TABLE in a live
// MySQL -> MySQL sync stream must reach the target even when the
// statement carries a leading SQL comment. MySQL preserves leading
// comments verbatim in the binlog QUERY_EVENT, and the pre-fix
// parseTruncateTable required the body to START with "TRUNCATE", so a
// commented truncate (a hand-written migration note, an APM/ORM query
// tag) was silently routed to generic DDL handling and never emitted
// as ir.Truncate — the target kept the rows the source truncated and
// the stream never converged. The sync-convergence property found it
// (its renderTx prepends `-- tx N (pattern)\n` to every statement);
// this is the focused, deterministic pin. The bug catalog also noted
// the only truncate integration test was PG-only — this adds the
// MySQL coverage.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

func TestBug140_MySQLToMySQL_CommentedTruncatePropagates(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE trunc_t (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO trunc_t (email) VALUES ('seed1@example.com'), ('seed2@example.com');
	`
	applyDDLMySQL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "bug140-trunc",
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

	// Bulk copy delivers the 2 seed rows.
	if !waitForRowCountMySQL(t, targetDSN, "trunc_t", 2, 60*time.Second) {
		t.Fatalf("bulk copy never delivered the 2 seed rows")
	}

	// A live CDC insert replicates (proves the stream is healthy).
	applyDDLMySQL(t, sourceDSN, "INSERT INTO trunc_t (email) VALUES ('live3@example.com');")
	if !waitForRowCountMySQL(t, targetDSN, "trunc_t", 3, 30*time.Second) {
		t.Fatalf("CDC never delivered the live insert (count=%d)", countRowsMySQL(t, targetDSN, "trunc_t"))
	}

	// The truncate — with a leading comment, the exact shape that
	// reproduced Bug 140. The binlog records the comment verbatim; the
	// reader must still recognise it as a TRUNCATE and empty the target.
	applyDDLMySQL(t, sourceDSN, "-- operator note: clear the table\nTRUNCATE TABLE trunc_t;")

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if countRowsMySQL(t, targetDSN, "trunc_t") == 0 {
			return // converged
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("BUG 140: target still has %d rows after a comment-led TRUNCATE; source-side truncate never applied",
		countRowsMySQL(t, targetDSN, "trunc_t"))
}
