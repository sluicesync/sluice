//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SLM-1 (audit 2026-09-01), end to end on a real MySQL source whose host
// zone is not UTC: the zone-sibling swap as the FIRST DDL after a cold
// start — no priming ADD COLUMN — must refuse loudly in BOTH modes that
// re-apply schema deltas to the target, and leave the target's
// pre-existing rows and column type untouched.
//
// The two observed shapes this pins (silent-loss-mysql.md §SLM-1):
//
//   - Shape A (--inject-shard-column): the first `MODIFY c TIMESTAMP` was
//     FORWARDED through the boundary router and every pre-existing target
//     row read 9 h off at exit 0.
//   - default --schema-changes=forward: the same swap was seed-skipped at
//     INFO and every later row landed in a zone-mismatched column.
//
// The container boots with --default-time-zone=+09:00 so the operator's
// ALTER session inherits the non-UTC zone without any SET — the shipped
// MySQL default is time_zone=SYSTEM, i.e. the host's zone, which is what
// makes this the DEFAULT outcome on a non-UTC source rather than a
// deliberately mismatched configuration.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// startMySQLBinlogAtTokyo mirrors startMySQLBinlog with the server's
// default time_zone pinned to +09:00 — the SLM-1 reproduction shape.
func startMySQLBinlogAtTokyo(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	container := runMySQLWithRetry(
		t,
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
					"--default-time-zone=+09:00",
					"--net-write-timeout=600",
					"--net-read-timeout=600",
				},
			},
		}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	srcConn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	db, err := sql.Open("mysql", srcConn+"&multiStatements=true")
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}
	tgtConn, err := buildMySQLDSN(srcConn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}
	return srcConn, tgtConn, terminate
}

func TestStreamer_SessionTZSwapAtTheFirstBoundary_RefusesInBothModes(t *testing.T) {
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	modes := []struct {
		name      string
		configure func(s *Streamer)
	}{
		{
			// Scenario A: the boundary router forwards ALTER COLUMN TYPE
			// regardless of --schema-changes.
			name: "shape-a",
			configure: func(s *Streamer) {
				s.InjectShardColumn = ShardColumnSpec{Name: "source_shard_id", Value: "shard_a"}
				s.ShardCoordinationLease = LeaseConfig{
					LeaseDuration: 30 * time.Second,
					RenewDeadline: 20 * time.Second,
					RetryPeriod:   5 * time.Second,
				}
			},
		},
		{
			// Scenario B: the ADR-0091 single-stream intercept (zero-value
			// SchemaChanges is the shipping "forward" default).
			name:      "default-forward",
			configure: func(*Streamer) {},
		},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startMySQLBinlogAtTokyo(t)
			defer cleanup()

			// DATETIME rows at Tokyo wall clock 21:00 (= 12:00 UTC). A
			// forwarded `MODIFY c TIMESTAMP` on the target — which runs
			// under sluice's pinned +00:00 — would store them as 21:00
			// UTC, 9 h off the source's 12:00 UTC.
			applyDDLMySQL(t, sourceDSN, `
				CREATE TABLE events (
					id BIGINT NOT NULL PRIMARY KEY,
					c  DATETIME NOT NULL
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
				INSERT INTO events (id, c) VALUES
					(1, '2020-01-01 21:00:00'),
					(2, '2020-01-01 21:00:00'),
					(3, '2020-01-01 21:00:00');
			`)

			streamer := &Streamer{
				Source:    myEng,
				Target:    myEng,
				SourceDSN: sourceDSN,
				TargetDSN: targetDSN,
				StreamID:  "test-slm1-" + mode.name,
			}
			mode.configure(streamer)

			streamCtx, streamCancel := context.WithCancel(context.Background())
			defer streamCancel()
			runErr := make(chan error, 1)
			go func() { runErr <- streamer.Run(streamCtx) }()

			if !waitForRowCountMySQL(t, targetDSN, "events", 3, 60*time.Second) {
				t.Fatalf("bulk-copy never landed the seed rows")
			}

			// THE SWAP, as the FIRST DDL after cold start, from a session
			// that inherits the server's +09:00 default — no SET, no
			// priming ADD COLUMN. The post-ALTER INSERT is a row the
			// stream would apply if it stayed alive.
			applyDDLMySQL(t, sourceDSN, "ALTER TABLE events MODIFY c TIMESTAMP NOT NULL;")
			applyDDLMySQL(t, sourceDSN, "INSERT INTO events (id, c) VALUES (4, '2020-01-01 21:00:00');")

			var streamErr error
			select {
			case streamErr = <-runErr:
			case <-time.After(90 * time.Second):
				t.Fatal("streamer did not surface the session-time_zone refusal within 90s — the first-boundary swap was forwarded or skipped (SLM-1)")
			}
			if streamErr == nil {
				t.Fatal("streamer returned nil on a DATETIME→TIMESTAMP swap at the first boundary; want the session-time_zone cast refusal")
			}
			for _, want := range []string{"cannot be forwarded", `column "c"`, "time_zone", "drained model"} {
				if !strings.Contains(streamErr.Error(), want) {
					t.Errorf("stream error missing %q; got: %v", want, streamErr)
				}
			}

			// The target is untouched: column type still DATETIME, every
			// pre-existing row still the wall clock it was copied as, and
			// the post-ALTER row never applied. The independent expected
			// value is what the source itself now holds: read under +00:00
			// the swapped source rows are 12:00:00 (the ALTER resolved
			// 21:00 Tokyo as 12:00 UTC), which is exactly the value a
			// forwarded ALTER could not have reproduced on the target.
			assertTargetUntouchedBySwap(t, sourceDSN, targetDSN)

			streamCancel()
		})
	}
}

// assertTargetUntouchedBySwap reads both sides under an explicit +00:00
// session and asserts the target column is still DATETIME holding the
// copied wall clock, while the source's swap actually happened.
func assertTargetUntouchedBySwap(t *testing.T, sourceDSN, targetDSN string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := func(dsn, q string, args ...any) *sql.Row {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("conn: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		if _, err := conn.ExecContext(ctx, "SET SESSION time_zone = '+00:00'"); err != nil {
			t.Fatalf("set time_zone: %v", err)
		}
		return conn.QueryRowContext(ctx, q, args...)
	}

	var srcType, tgtType string
	if err := query(sourceDSN, "SELECT DATA_TYPE FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'events' AND column_name = 'c'").Scan(&srcType); err != nil {
		t.Fatalf("source column type: %v", err)
	}
	if err := query(targetDSN, "SELECT DATA_TYPE FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'events' AND column_name = 'c'").Scan(&tgtType); err != nil {
		t.Fatalf("target column type: %v", err)
	}
	if !strings.EqualFold(srcType, "timestamp") {
		t.Fatalf("source events.c is %q; the test's own ALTER did not land", srcType)
	}
	if !strings.EqualFold(tgtType, "datetime") {
		t.Errorf("target events.c is %q; want datetime — the swap was forwarded to the target (SLM-1 Scenario A)", tgtType)
	}

	var srcAtUTC, tgtWallClock string
	if err := query(sourceDSN, "SELECT CAST(c AS CHAR) FROM events WHERE id = 1").Scan(&srcAtUTC); err != nil {
		t.Fatalf("source row: %v", err)
	}
	if err := query(targetDSN, "SELECT CAST(c AS CHAR) FROM events WHERE id = 1").Scan(&tgtWallClock); err != nil {
		t.Fatalf("target row: %v", err)
	}
	if srcAtUTC != "2020-01-01 12:00:00" {
		t.Fatalf("source row 1 reads %q under +00:00; want 12:00:00 — the premise (the ALTER re-zoned the stored value against the +09:00 session) did not hold on this server", srcAtUTC)
	}
	if tgtWallClock != "2020-01-01 21:00:00" {
		t.Errorf("target row 1 reads %q; want the copied DATETIME wall clock 21:00:00 unchanged", tgtWallClock)
	}

	var n int
	if err := query(targetDSN, "SELECT COUNT(*) FROM events").Scan(&n); err != nil {
		t.Fatalf("target count: %v", err)
	}
	if n != 3 {
		t.Errorf("target holds %d rows; want 3 — the post-ALTER row must not have been applied into a zone-mismatched column (SLM-1 Scenario B)", n)
	}
}
