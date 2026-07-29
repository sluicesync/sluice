// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins [CDCReader.Close]'s join of the pump goroutine (roadmap item 103).
//
// The defect this exists for is the MySQL instance of the class the
// Postgres reader was fixed for: Close cancelled the pump and returned
// immediately, so `r.db = nil` landed underneath a pump that was still
// running a live information_schema query on that same pool — every
// TABLE_MAP boundary for an uncached table sends the pump into
// [CDCReader.tableFor] → loadTableSchema(ctx, r.db, …). The Postgres
// version at least tested the field for nil first; this one reads it
// straight, so the losing race is a method call on a nil *sql.DB and a
// panic on a goroutine nothing recovers.
//
// The pin is at the Close/pump boundary rather than on the db field,
// because the class is "Close tears down state the pump still owns", not
// "this one field is unguarded".

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

// stubJoinConnector backs a [sql.OpenDB] handle that is real enough to be
// held, read and closed, but never dials: the pin is about teardown
// ordering, not about talking to a server.
type stubJoinConnector struct{}

func (stubJoinConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("mysql: stub connector never connects")
}

func (stubJoinConnector) Driver() driver.Driver { return nil }

func TestCDCReaderCloseJoinsPumpBeforeReleasingSchemaPool(t *testing.T) {
	r := &CDCReader{db: sql.OpenDB(stubJoinConnector{})}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.streamerCancel = cancel
	r.pumpDone = done

	var (
		parked  = make(chan struct{}) // the stand-in pump saw the cancel
		release = make(chan struct{}) // the test decides when it unwinds
		exited  = make(chan struct{})
	)
	// Stands in for the real pump: notices the cancellation, then does one
	// more schema-lookup-shaped read of r.db on its way out — which is
	// what tableFor does on a TABLE_MAP boundary. Without the join that
	// read races Close's teardown, so this goroutine is what makes the pin
	// bite under -race as well as deterministically below.
	go func() {
		defer close(done)
		<-ctx.Done()
		close(parked)
		<-release
		if r.db != nil {
			_ = r.db.Stats()
		}
		close(exited)
	}()

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- r.Close() }()

	<-parked // Close has cancelled the pump

	select {
	case <-closeReturned:
		t.Fatal("Close returned while the pump goroutine was still running: it must join the pump before releasing the schema pool, or the pump's live information_schema lookup reads a closed/nil r.db")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	<-exited

	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the pump exited")
	}

	if r.db != nil {
		t.Error("Close left the schema pool attached")
	}
	if r.pumpDone != nil {
		t.Error("Close left pumpDone set; a second Close would block on an already-closed stream")
	}

	// "Safe to call multiple times" is part of Close's contract and the
	// join must not break it.
	if err := r.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
