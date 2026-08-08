// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins [CDCReader.Close]'s join of the polling goroutine.
//
// The regression this exists for is the sqlite-trigger defect one engine over.
// That one was found by accident (a door matrix opened and closed a reader per
// cell and panicked on the first run), fixed, and recorded as closing the
// class — without enumerating the siblings. This reader, the other trigger
// engine with the identical polling shape, still had it: Close cancelled the
// pump and then immediately closed AND nil'd r.db, while the pump's very next
// act after any select is a query on r.db (holes.refresh,
// captureTxidUpperBound, or the batch QueryContext). Cancellation is
// asynchronous, so a Close landing in that window nil-dereferenced inside the
// goroutine — an unrecovered panic that takes the process down rather than
// failing the sync — and, either way, an unsynchronised cross-goroutine
// read/write of the field that `-race` grades.
//
// The pin is at the Close/pump boundary rather than on the db field, because
// the class is "Close tears down state the pump still owns".

package pgtrigger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

// stubConnector backs a [sql.OpenDB] handle that is real enough to be
// nil-checked, read and closed, but never dials.
type stubConnector struct{}

func (stubConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("pgtrigger: stub connector never connects")
}

func (stubConnector) Driver() driver.Driver { return nil }

func TestCDCReaderClose_JoinsThePumpBeforeClosingThePool(t *testing.T) {
	r := &CDCReader{db: sql.OpenDB(stubConnector{})}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.pumpCancel = cancel
	r.pumpDone = done

	var (
		parked  = make(chan struct{}) // the stand-in pump saw the cancel
		release = make(chan struct{}) // the test decides when it unwinds
		exited  = make(chan struct{})
	)
	// Stands in for the real pump: notices the cancellation, then does one
	// more poll-shaped read of r.db on its way out — which is exactly what the
	// real pump does at holes.refresh / captureTxidUpperBound / QueryContext.
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
		t.Fatal("Close returned while the polling goroutine was still running: it must join the pump " +
			"before closing the pool, or the pump's next poll dereferences a closed/nil r.db")
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
		t.Error("Close left the pool attached")
	}
	if r.pumpDone != nil {
		t.Error("Close left pumpDone set; a second Close would block on an already-closed stream")
	}

	// Close is documented as safe to call multiple times; the join must not
	// break that.
	if err := r.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
