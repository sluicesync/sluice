//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The BEHAVIOURAL pin for the binlog dispatch-path classification — the
// second instance of the Bug 207 class, found by auditing setErr sites after
// the first one shipped inert.
//
// TestSetErrSitesClassify already guards this line, but it is a SYNTAX gate:
// it proves the call is wrapped in classifyReaderError, not that a transient
// raised on this path actually ends up retriable. Trusting a syntactic check
// to stand in for behaviour is a smaller version of the exact mistake that
// produced Bug 207 (a unit test that called the classifier directly and so
// pinned the FUNCTION rather than the PATH). This test drives a REAL binlog
// stream and asserts on what the streamer actually collects.
//
// Why a seam instead of a real fault: the failure being reproduced is a
// connection blip during the LIVE information_schema read that dispatchRows
// performs at a TABLE_MAP boundary for an uncached table. Landing a real
// failure inside that specific window is not reliably reproducible — worse,
// database/sql transparently retries a killed pooled connection on ErrBadConn,
// so the obvious "KILL the connection" approach usually recovers underneath
// the code under test and proves nothing. Everything else here is real: real
// MySQL, real binlog syncer, real TABLE_MAP, real dispatch, real setErr.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestCDCReader_DispatchPathSchemaLoadTransient_IsRetriable(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE dispatch_probe (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			label VARCHAR(64)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rdr, err := Engine{Flavor: FlavorVanilla}.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	cdc, ok := rdr.(*CDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T, not *CDCReader", rdr)
	}

	// The injected fault: the transport error shape a source blip produces
	// during the live schema read. Deliberately NOT pre-wrapped as retriable —
	// the whole point is that classifyReaderError on the dispatch path is what
	// makes it retriable, so an unclassified path leaves it terminal.
	var loads int
	cdc.schemaLoader = func(context.Context, *sql.DB, string, string, Flavor) (*tableSchema, error) {
		loads++
		return nil, errors.New("invalid connection")
	}

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// The syncer registers asynchronously; let it reach "now" so the INSERT
	// below is inside the stream (mirrors the sibling CDC tests).
	time.Sleep(200 * time.Millisecond)

	// An INSERT on a table with no schemaCache entry forces the TABLE_MAP
	// boundary to take the live-schema-read path, where the fault fires.
	applyMySQL(t, dsn, `INSERT INTO dispatch_probe (label) VALUES ('trigger');`)

	// The stream must die (the fault is unconditional) — we care about HOW.
	deadline := time.After(30 * time.Second)
	for cdc.Err() == nil {
		select {
		case _, open := <-changes:
			if !open && cdc.Err() == nil {
				t.Fatal("change stream closed with no parked error after an injected schema-load failure")
			}
		case <-deadline:
			t.Fatalf("no error parked within 30s (schema loader called %d times) — the dispatch path may no longer perform a live schema read, in which case this pin needs re-pointing", loads)
		}
	}

	streamErr := cdc.Err()
	if loads == 0 {
		t.Fatal("schema loader was never called — the injected fault never reached the dispatch path, so this run proves nothing")
	}

	var re ir.RetriableError
	if !errors.As(streamErr, &re) || !re.Retriable() {
		t.Fatalf("a transient raised on the DISPATCH path was parked NON-retriable, so the stream dies on a blip that "+
			"nettransient exists to ride out — the Bug 207 class, second instance.\n"+
			"  parked: %v\n"+
			"Fix: classify at the r.dispatch(...) error return in the binlog run loop.", streamErr)
	}
	// The original cause must stay reachable for diagnostics (%w chain).
	if !strings.Contains(streamErr.Error(), "invalid connection") {
		t.Errorf("classified error lost the underlying cause; got %v", streamErr)
	}
}
