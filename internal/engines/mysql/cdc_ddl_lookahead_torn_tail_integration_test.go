//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDDLLookahead_TornActiveTail_RealServer pins the look-ahead against a
// real server's torn tail (cdc_ddl_lookahead.go, "SHOW BINLOG EVENTS reads
// past the position it was asked about"): with writers flushing large
// transactions, the look-ahead's SHOW BINLOG EVENTS on the active file runs
// into an event still being written and MySQL fails the read with 1220. The
// scan must complete over every such read — the CI failure of 2026-10-13
// stopped a stream on one.
//
// The independent expected value is the server's own: each scan's end is a
// SHOW BINARY LOG STATUS position, and the scan must mark exactly that as
// read. Anti-vacuity: the cell runs until the scan has itself been handed
// tornWant torn reads, and fails if the server never tore one — a run that
// saw none proved nothing. (MariaDB serves only whole events and never
// tears, so on a MariaDB image the cell skips.)
func TestDDLLookahead_TornActiveTail_RealServer(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "lookahead_torn")
	defer cleanup()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(16)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(version), "mariadb") {
		t.Skipf("%s serves only whole events from SHOW BINLOG EVENTS; there is no torn tail to read", version)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE big (id BIGINT AUTO_INCREMENT PRIMARY KEY, b LONGBLOB)"); err != nil {
		t.Fatal(err)
	}

	writeCtx, stopWriters := context.WithCancel(ctx)
	var writers sync.WaitGroup
	payload := strings.Repeat("x", 40000) // larger than the binlog cache: flushed in pieces
	for range 8 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for writeCtx.Err() == nil {
				tx, err := db.BeginTx(writeCtx, nil)
				if err != nil {
					continue
				}
				_, _ = tx.ExecContext(writeCtx, "INSERT INTO big (b) VALUES (?),(?),(?)", payload, payload, payload)
				_ = tx.Commit()
			}
		}()
	}
	defer func() { stopWriters(); writers.Wait() }()

	const tornWant = 2
	var torn atomic.Int64
	l := ddlLookahead{listEvents: func(ctx context.Context, file string, from uint32, limit int) ([]binlogEventRow, error) {
		rows, err := showBinlogEvents(ctx, db, file, from, limit)
		if isActiveTailTruncation(err) {
			torn.Add(1)
		}
		return rows, err
	}}
	file, pos, err := masterStatus(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	from := binlogCoord{file, pos}
	scans := 0
	deadline := time.Now().Add(2 * time.Minute)
	for torn.Load() < tornWant {
		if time.Now().After(deadline) {
			t.Fatalf("the server tore %d of %d look-ahead reads in two minutes (want %d): the cell exercised nothing",
				torn.Load(), scans, tornWant)
		}
		file, pos, err := masterStatus(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		to := binlogCoord{file, pos}
		if to.file != from.file {
			from = binlogCoord{to.file, 4} // rotated: scan the new active file
		}
		if err := l.scan(ctx, db, from, to); err != nil {
			t.Fatalf("scan %d [%s, %s) after %d torn reads: %v", scans, from, to, torn.Load(), err)
		}
		if to.after(from) && l.through != to {
			t.Fatalf("scan %d: marker = %s; want the status position %s", scans, l.through, to)
		}
		from = to
		scans++
	}
	t.Logf("%d scans; the server tore %d of their reads and every scan completed", scans, torn.Load())
}
