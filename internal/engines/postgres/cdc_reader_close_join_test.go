// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins [CDCReader.Close]'s join of the pump goroutine.
//
// The regression this exists for: Close cancelled the pump and returned
// immediately, so `r.db = nil` landed underneath a pump that was still
// running live catalog queries on that same pool (every pgoutput
// RelationMessage runs resolveColumnStableIDs / resolveIdentityKeyCols /
// ensureEnumTypeOIDs against r.db). CI caught it as a data race between
// Close and ensureEnumTypeOIDs; the worse reachable outcome is a pump
// that passes its `r.db == nil` guard and then dereferences a nil
// *sql.DB in QueryContext — a panic on a goroutine nothing recovers.
//
// The pin is deliberately at the Close/pump boundary rather than on the
// db field, because the defect class is "Close tears down state the pump
// still owns", not "this one field is unguarded".

package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

// stubConnector backs a [sql.OpenDB] handle that is real enough to be
// nil-checked, read and closed, but never dials: the pin is about
// teardown ordering, not about talking to a server.
type stubConnector struct{}

func (stubConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("postgres: stub connector never connects")
}

func (stubConnector) Driver() driver.Driver { return nil }

func TestCDCReaderCloseJoinsPumpBeforeReleasingCatalogPool(t *testing.T) {
	r := &CDCReader{db: sql.OpenDB(stubConnector{})}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.streamerCancel = cancel
	r.pumpDone = done

	var (
		parked  = make(chan struct{}) // the stand-in pump saw the cancel
		release = make(chan struct{}) // the test decides when it unwinds
		exited  = make(chan struct{})
	)
	// Stands in for the real pump: notices the cancellation, then does
	// one more catalog-shaped read of r.db on its way out. Without the
	// join that read races (and, for a genuine *sql.DB method call,
	// nil-derefs) Close's teardown — so this goroutine is what makes the
	// pin bite under -race as well as deterministically below.
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
		t.Fatal("Close returned while the pump goroutine was still running: it must join the pump before releasing the catalog pool, or the pump's live catalog queries read a closed/nil r.db")
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
		t.Error("Close left the catalog pool attached")
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
