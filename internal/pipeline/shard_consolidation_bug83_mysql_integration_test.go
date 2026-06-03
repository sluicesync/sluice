//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0054 Bug 83 end-to-end pin — MySQL → MySQL.
//
// Sibling to shard_consolidation_bug83_pg_integration_test.go. The
// failure shape on MySQL is the same root cause — the intercept's
// table cache started empty and treated the first CDC SchemaSnapshot
// as the cold-start anchor — but the applier-side crash text differs
// (`Unknown column '<new>' in 'field list'`, Error 1054). Mirroring
// the pin on both engines closes the "validate end-to-end before
// building more" tenet for the cross-engine surface.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

func TestStreamer_Bug83_MySQL_LiveCoordination_AddColumn(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');
	`)

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const streamID = "test-bug83-mysql"
	streamer := &Streamer{
		Source:    myEng,
		Target:    myEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
		InjectShardColumn: ShardColumnSpec{
			Name:  "source_shard_id",
			Value: "shard_a",
		},
		CoordinateLiveDDL: true,
		ShardCoordinationLease: LeaseConfig{
			LeaseDuration: 30 * time.Second,
			RenewDeadline: 20 * time.Second,
			RetryPeriod:   5 * time.Second,
		},
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows (cold-start path stalled before CDC)")
	}

	// Bug 83 timing: source DDL + INSERT between cold-start completion
	// and the first CDC row event. Pre-fix the next CDC row crashes the
	// applier with Error 1054 (`Unknown column 'price' in 'field list'`)
	// because the lease was never acquired and the target schema is
	// stale.
	applyDDLMySQL(t, sourceDSN, "ALTER TABLE widgets ADD COLUMN price DECIMAL(10,2);")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, price) VALUES (3, 'gamma', 3.75);")

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-DDL row never landed — Bug 83 regression " +
			"(intercept treated the first CDC SchemaSnapshot as the cold-start anchor " +
			"instead of as a real boundary; lease never recorded, applier crashed on " +
			"the INSERT referencing the new column with MySQL Error 1054)")
	}

	tgtDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Target schema reflects the added column.
	var hasPrice int
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'widgets' AND column_name = 'price'
	`).Scan(&hasPrice); err != nil {
		t.Fatalf("check column: %v", err)
	}
	if hasPrice != 1 {
		t.Errorf("target widgets.price column missing — boundary apply didn't fire")
	}

	// Lease row reflects the applied state.
	const leaseQ = `SELECT applied_at IS NOT NULL, applied_schema_version
		FROM sluice_shard_consolidation_lease
		WHERE target_table_full_name = ?`
	var (
		applied bool
		version int64
	)
	if err := tgtDB.QueryRowContext(ctx, leaseQ, "source_db.widgets").Scan(&applied, &version); err != nil {
		t.Fatalf("scan lease row: %v (a missing row is the load-bearing Bug 83 symptom — "+
			"the intercept never routed the boundary, so the lease table stayed empty)", err)
	}
	if !applied {
		t.Error("lease.applied_at should be set after the routed boundary")
	}
	if version < 1 {
		t.Errorf("lease.applied_schema_version = %d; want >= 1", version)
	}

	// Confirm gamma row landed with price.
	var (
		gotName  string
		gotPrice sql.NullString
	)
	if err := tgtDB.QueryRowContext(ctx,
		"SELECT name, CAST(price AS CHAR) FROM widgets WHERE id = 3").Scan(&gotName, &gotPrice); err != nil {
		t.Fatalf("scan gamma: %v", err)
	}
	if gotName != "gamma" {
		t.Errorf("widgets.name = %q; want gamma", gotName)
	}
	if !gotPrice.Valid || gotPrice.String != "3.75" {
		t.Errorf("widgets.price = %v; want 3.75", gotPrice)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
