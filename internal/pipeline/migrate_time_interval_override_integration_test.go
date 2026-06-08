//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"

	_ "github.com/go-sql-driver/mysql"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_MySQLTimeDuration_IntervalOverride pins the Vector C
// data-preserving escape hatch for a MySQL TIME column used as a DURATION
// rather than a time-of-day. MySQL TIME spans -838:59:59…838:59:59, which
// exceeds PG `time`'s 00:00–24:00 range, so the default TIME → PG `time`
// mapping can't hold a >24h or negative value. `--type-override
// COL=interval` maps the column to PG `interval`, which holds the full
// range; the value is carried as its textual form and PG parses it.
func TestMigrate_MySQLTimeDuration_IntervalOverride(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, `
		CREATE TABLE durations (
			id  INT PRIMARY KEY,
			dur TIME(6) NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO durations (id, dur) VALUES
			(1, '838:59:59'),          -- max MySQL TIME; far beyond PG time's 24h
			(2, '-12:30:00'),          -- negative; impossible in PG time
			(3, '00:00:05'),           -- ordinary small duration
			(4, NULL),                 -- NULL carries through
			(5, '12:34:56.789012'),    -- fractional seconds (TIME(6))
			(6, '00:00:00');           -- zero duration
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source: mysqlEng, Target: pgEng,
		SourceDSN: mysqlSource, TargetDSN: pgTarget,
		Mappings: []config.Mapping{{Table: "durations", Column: "dur", TargetType: "interval"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (interval override): %v", err)
	}

	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Target column must be `interval`, not `time`.
	var dataType string
	if err := db.QueryRowContext(ctx,
		"SELECT data_type FROM information_schema.columns WHERE table_name='durations' AND column_name='dur'").
		Scan(&dataType); err != nil {
		t.Fatalf("read target column type: %v", err)
	}
	if dataType != "interval" {
		t.Errorf("target durations.dur data_type = %q; want interval (the override)", dataType)
	}

	// Each duration must round-trip exactly — including the >24h and the
	// negative value PG `time` could not hold. PG renders an hour-bearing
	// interval as HH:MM:SS (no day rollover), so the text matches MySQL's.
	// NULL sentinel: a SQL NULL renders as the empty string via the
	// NullString below; every other id is a non-empty interval text.
	want := map[int]string{
		1: "838:59:59",
		2: "-12:30:00",
		3: "00:00:05",
		4: "", // NULL
		5: "12:34:56.789012",
		6: "00:00:00",
	}
	rows, err := db.QueryContext(ctx, "SELECT id, dur::text FROM durations ORDER BY id")
	if err != nil {
		t.Fatalf("query target: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := map[int]string{}
	for rows.Next() {
		var id int
		var dur sql.NullString
		if err := rows.Scan(&id, &dur); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = dur.String // "" when NULL
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("durations id=%d: target dur = %q; want %q (exact duration via interval override)", id, got[id], w)
		}
	}
}

// TestMigrate_IntervalOverride_MySQLTargetRefuses pins that the `interval`
// override is refused loudly for a MySQL target — MySQL has no INTERVAL
// type, and silently degrading to TIME would re-lose the range the
// override exists to preserve.
func TestMigrate_IntervalOverride_MySQLTargetRefuses(t *testing.T) {
	mysqlSource, mysqlTarget, cleanup := startMySQL(t)
	defer cleanup()

	applyMySQLDDL(t, mysqlSource, `
		CREATE TABLE durations (
			id  INT PRIMARY KEY,
			dur TIME NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO durations (id, dur) VALUES (1, '01:02:03');
	`)

	mysqlEng, _ := engines.Get("mysql")
	mig := &Migrator{
		Source: mysqlEng, Target: mysqlEng,
		SourceDSN: mysqlSource, TargetDSN: mysqlTarget,
		Mappings: []config.Mapping{{Table: "durations", Column: "dur", TargetType: "interval"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err := mig.Run(ctx)
	if err == nil {
		t.Fatal("Migrator.Run = nil; want a loud refusal of interval on a MySQL target")
	}
	if !strings.Contains(strings.ToUpper(err.Error()), "INTERVAL") {
		t.Errorf("err = %v; want it to name INTERVAL as unsupported on MySQL", err)
	}
}

// TestStreamer_MySQLToPostgres_IntervalOverride pins the SYNC/CDC path for
// the `interval` override (the gap the value-fidelity review flagged): a
// MySQL TIME column overridden to PG `interval` must round-trip through
// cold-start AND continuous CDC. The CDC applier reads the target catalog
// (loadColumnTypes → translateType), which now resolves `interval` instead
// of stopping the stream, and binds the textual duration via prepareValue.
func TestStreamer_MySQLToPostgres_IntervalOverride(t *testing.T) {
	mysqlSourceDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgTargetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSourceDSN, `
		CREATE TABLE spans (
			id  BIGINT NOT NULL AUTO_INCREMENT,
			dur TIME(6) NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO spans (id, dur) VALUES (1, '100:00:00');
	`)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")
	streamer := &Streamer{
		Source: mysqlEng, Target: pgEng,
		SourceDSN: mysqlSourceDSN, TargetDSN: pgTargetDSN,
		StreamID: "test-cross-mysql-pg-interval",
		Mappings: []config.Mapping{{Table: "spans", Column: "dur", TargetType: "interval"}},
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	pollDur := func(id int, want string, timeout time.Duration) bool {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			db, err := sql.Open("pgx", pgTargetDSN)
			if err == nil {
				var dur sql.NullString
				qctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				e := db.QueryRowContext(qctx, "SELECT dur::text FROM spans WHERE id=$1", id).Scan(&dur)
				cancel()
				_ = db.Close()
				if e == nil && dur.String == want {
					return true
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
		return false
	}

	// Cold-start: the >24h duration lands on a PG interval column.
	if !waitForRowCount(t, pgTargetDSN, "spans", 1, 60*time.Second) {
		t.Fatal("cold-start never delivered span 1")
	}
	if !pollDur(1, "100:00:00", 15*time.Second) {
		t.Fatal("cold-start: span 1 dur != 100:00:00 on PG interval column")
	}

	// CDC INSERT: a new >24h duration.
	applyMySQLDDL(t, mysqlSourceDSN, "INSERT INTO spans (id, dur) VALUES (2, '500:30:00');")
	if !pollDur(2, "500:30:00", 30*time.Second) {
		t.Fatal("CDC INSERT: span 2 dur never reached PG as 500:30:00")
	}

	// CDC UPDATE: flip span 1 to a NEGATIVE duration (impossible in PG time).
	applyMySQLDDL(t, mysqlSourceDSN, "UPDATE spans SET dur='-99:00:00' WHERE id=1;")
	if !pollDur(1, "-99:00:00", 30*time.Second) {
		t.Fatal("CDC UPDATE: span 1 dur never updated to -99:00:00 on PG")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
