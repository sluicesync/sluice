//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0058 — Online ADD COLUMN forwarding for single-stream (non-Shape-A)
// CDC apply. MySQL → MySQL live CDC.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
)

// TestStreamer_AddColumnForward_MySQL_FlagOn_ForwardsALTER pins the
// MySQL → MySQL load-bearing happy path.
func TestStreamer_AddColumnForward_MySQL_FlagOn_ForwardsALTER(t *testing.T) {
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

// TestStreamer_AddColumnForward_MySQL_Backfill verifies the backfill loop on
// MySQL → MySQL. Source post-ALTER UPDATEs assign per-row values; the
// final target state reflects them.
func TestStreamer_AddColumnForward_MySQL_Backfill(t *testing.T) {
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

// TestStreamer_AddColumnForward_MySQL_RefusesComputedDefault pins Bug 90's fix
// (v0.79.1) on MySQL → MySQL. MySQL's TableMapEvent doesn't carry
// COLUMN_DEFAULT either, so the production CDC path arrives at the
// intercept with Default=nil; the fix's source-side SchemaReader
// probe surfaces the canonical text and the text-scan classifies
// the volatility class.
//
// Class-pin (Bug 74) — why one scenario, not many (Bug 91 closure):
// the only volatile DEFAULT shape MySQL itself accepts on ADD COLUMN
// is CURRENT_TIMESTAMP / NOW() on TIMESTAMP|DATETIME columns. Every
// other volatile function (UUID(), RAND(), UTC_TIMESTAMP(),
// LAST_INSERT_ID(), …) is rejected at MySQL DDL parse time with
// Error 1674 ("Statement is unsafe because it uses a system function
// that may return a different value on the replica") REGARDLESS of
// binlog_format — even with --binlog-format=ROW the server refuses
// to compile the ALTER (the safety check is unconditional in MySQL
// 8.0; only `SET SESSION sql_log_bin = 0` bypasses it, which then
// hides the DDL from the binlog and defeats the test). The
// integration tier therefore carries the one scenario MySQL permits
// (now-default) — sufficient to prove the production plumbing
// (TableMapEvent → SchemaSnapshot → intercept → prober → classifier
// → refuse-loudly). Class coverage for UUID / RAND / UTC_TIMESTAMP /
// LAST_INSERT_ID and every other MySQL volatile family lives in the
// unit-test matrix (TestClassifyDefaultVolatility_Class), which
// exercises the classifier directly with no MySQL-DDL gating.
func TestStreamer_AddColumnForward_MySQL_RefusesComputedDefault(t *testing.T) {
	scenarios := []struct {
		name   string
		ddl    string
		col    string
		expect string // substring expected in error (lower-cased)
	}{
		{
			name:   "now-default",
			ddl:    "ALTER TABLE widgets ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;",
			col:    "created_at",
			expect: "current_timestamp",
		},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
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
				StreamID:               "test-addcol-refuse-mysql-" + sc.name,
				ForwardSchemaAddColumn: true,
			}

			streamCtx, streamCancel := context.WithCancel(context.Background())
			defer streamCancel()

			runErr := make(chan error, 1)
			go func() { runErr <- streamer.Run(streamCtx) }()

			if !waitForRowCountMySQL(t, targetDSN, "widgets", 2, 30*time.Second) {
				t.Fatalf("phase A: bulk-copy never landed seed rows")
			}

			applyDDLMySQL(t, sourceDSN, sc.ddl)
			applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name) VALUES (3, 'gamma');")

			var err error
			select {
			case err = <-runErr:
			case <-time.After(60 * time.Second):
				t.Fatal("streamer did not surface refuse-loudly error within timeout")
			}
			if err == nil {
				t.Fatal("streamer returned nil error; expected refuse-loudly on computed DEFAULT")
			}
			errStr := strings.ToLower(err.Error())
			if !strings.Contains(errStr, "computed default") {
				t.Errorf("error %q does not mention 'computed default'", err)
			}
			if !strings.Contains(errStr, sc.expect) {
				t.Errorf("error %q does not mention %q", err, sc.expect)
			}
			if !strings.Contains(err.Error(), "ADR-0058 §2a") {
				t.Errorf("error %q does not cite ADR-0058 §2a", err)
			}

			// The target widgets table must NOT have the new column
			// (the intercept refused BEFORE issuing the target ALTER).
			tgtDB, openErr := sql.Open("mysql", targetDSN)
			if openErr != nil {
				t.Fatalf("open target: %v", openErr)
			}
			defer func() { _ = tgtDB.Close() }()

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			var newColCount int
			if err := tgtDB.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM information_schema.columns
				WHERE table_schema = DATABASE() AND table_name='widgets' AND column_name=?
			`, sc.col).Scan(&newColCount); err != nil {
				t.Fatalf("check target column: %v", err)
			}
			if newColCount != 0 {
				t.Errorf("target widgets.%s exists — intercept did NOT refuse the volatile DEFAULT (silent forwarding regression)", sc.col)
			}
		})
	}
}
