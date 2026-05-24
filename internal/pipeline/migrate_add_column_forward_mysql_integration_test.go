//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0058 — Online ADD COLUMN forwarding for single-stream (non-Shape-A)
// CDC apply. MySQL → MySQL live CDC.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
)

// TestAddColumnForward_MySQL_FlagOn_ForwardsALTER pins the
// MySQL → MySQL load-bearing happy path.
func TestAddColumnForward_MySQL_FlagOn_ForwardsALTER(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL
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
		StreamID:               "test-addcol-fwd-mysql",
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

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed — forwarding broken")
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

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestAddColumnForward_MySQL_Backfill verifies the backfill loop on
// MySQL → MySQL. Source post-ALTER UPDATEs assign per-row values; the
// final target state reflects them.
func TestAddColumnForward_MySQL_Backfill(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL
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
		StreamID:               "test-addcol-backfill-mysql",
		ForwardSchemaAddColumn: true,
		BackfillAddedColumn:    true,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	applyDDLMySQL(t, sourceDSN, "ALTER TABLE widgets ADD COLUMN price DECIMAL(10,2);")
	applyDDLMySQL(t, sourceDSN, "UPDATE widgets SET price = 1.25 WHERE id = 1;")
	applyDDLMySQL(t, sourceDSN, "UPDATE widgets SET price = 2.50 WHERE id = 2;")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, price) VALUES (3, 'gamma', 3.75);")

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed")
	}

	tgtDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Poll until all three target rows have their expected price.
	deadline := time.Now().Add(30 * time.Second)
	want := map[int64]string{1: "1.25", 2: "2.50", 3: "3.75"}
	for time.Now().Before(deadline) {
		allMatch := true
		for id, expected := range want {
			var got sql.NullString
			if err := tgtDB.QueryRowContext(ctx, "SELECT CAST(price AS CHAR) FROM widgets WHERE id=?", id).Scan(&got); err != nil {
				allMatch = false
				break
			}
			if !got.Valid || got.String != expected {
				allMatch = false
				break
			}
		}
		if allMatch {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	for id, expected := range want {
		var got sql.NullString
		if err := tgtDB.QueryRowContext(ctx, "SELECT CAST(price AS CHAR) FROM widgets WHERE id=?", id).Scan(&got); err != nil {
			t.Fatalf("scan id=%d: %v", id, err)
		}
		if !got.Valid || got.String != expected {
			t.Errorf("widgets.price for id=%d = %v; want %s", id, got, expected)
		}
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
