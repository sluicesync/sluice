//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestStreamer_AddColumnForward_MySQL_IndexedTable_ForwardsALTER is the
// end-to-end pin for GC-1 (audit backlog 2026-09-22): on a MySQL BINLOG
// source, a cold-started sync whose table carries a named SECONDARY
// INDEX must forward the first post-cold-start `ALTER TABLE ADD COLUMN`
// and keep streaming.
//
// Why this test exists beside migrate_add_column_forward_mysql_integration_test.go:
// every pre-existing binlog schema-forward integration test creates an
// index-less table (`id PK, name`), and that is exactly why GC-1 was
// never caught. The cold-start seed is a full SchemaReader read and
// carried Table.Indexes; the binlog boundary projection (projectTableIR)
// is {Schema, Name, Columns, PrimaryKey} and never did. With no
// secondary index the two agreed by accident; with one, the seed's index
// diffed as a phantom DropIndex, joined the real AddColumn as a
// two-class combo, and ClassifyShape's multi-shape refusal killed the
// stream BEFORE the seed-guard that exists to absorb seed-vs-CDC
// phantoms. The row inserted after the ALTER then never landed.
//
// Shard: the TestStreamer_ prefix routes it to the pipeline-rest-streamer
// shard (ci.yml `-run ^TestStreamer_`); the package is on
// scripts/check-shard-coverage.sh's COVERED_PACKAGES list. Runs the
// vanilla flavor; the MariaDB flavor rides the identical binlog reader
// and the same normalizer branch, pinned at the unit tier by
// TestNormalizeForCDCComparison_Binlog_* over every binlog flavor.
func TestStreamer_AddColumnForward_MySQL_IndexedTable_ForwardsALTER(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL,
			INDEX ix_widgets_name (name)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');")

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:                 myEng,
		Target:                 myEng,
		SourceDSN:              sourceDSN,
		TargetDSN:              targetDSN,
		StreamID:               "test-addcol-fwd-mysql-indexed",
		ForwardSchemaAddColumn: true,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	applyDDLMySQL(t, sourceDSN, "ALTER TABLE widgets ADD COLUMN price DECIMAL(10,2);")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, price) VALUES (3, 'gamma', 3.75);")

	// Phase B watches the streamer alongside the row count: the GC-1
	// failure shape is Run returning the multi-shape refusal, which a
	// bare row-count wait would only report as a 60s timeout.
	deadline := time.Now().Add(60 * time.Second)
	for pollRowCountMySQL(targetDSN, "widgets") < 3 {
		select {
		case err := <-runErr:
			t.Fatalf("phase B: streamer halted at the first post-cold-start boundary instead of forwarding the ADD COLUMN (GC-1 phantom index-drop): %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("phase B: post-ALTER row never landed — forwarding broken")
		}
		time.Sleep(200 * time.Millisecond)
	}

	tgtDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var hasPrice int
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'widgets' AND column_name = 'price'
	`).Scan(&hasPrice); err != nil {
		t.Fatalf("check column: %v", err)
	}
	if hasPrice != 1 {
		t.Errorf("target widgets.price column missing — intercept didn't forward the ALTER")
	}

	var gammaPrice sql.NullString
	if err := tgtDB.QueryRowContext(ctx, "SELECT CAST(price AS CHAR) FROM widgets WHERE id=3").Scan(&gammaPrice); err != nil {
		t.Fatalf("scan gamma price: %v", err)
	}
	if !gammaPrice.Valid || gammaPrice.String != "3.75" {
		t.Errorf("widgets.price for id=3 = %v; want 3.75", gammaPrice)
	}

	// The independent expected value for "the phantom was absorbed, not
	// forwarded": the target's own catalog still carries the secondary
	// index the cold copy created. A forwarded phantom DropIndex would
	// have removed it.
	var hasIndex int
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = 'widgets' AND index_name = 'ix_widgets_name'
	`).Scan(&hasIndex); err != nil {
		t.Fatalf("check index: %v", err)
	}
	if hasIndex != 1 {
		t.Errorf("target ix_widgets_name missing — a phantom DropIndex was forwarded")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
