// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Roadmap item 139's Postgres sibling — the raw-COPY import's connection
// ACQUIRE must reach the grow gate, not just the COPY that follows it.
//
// The MySQL half of item 139 is a retry (a lost acquire is replayed). This
// half cannot be: `ImportRawCopy` streams a one-shot reader straight into
// COPY FROM STDIN and has no resume point. What it CAN do — and did not —
// is TRIP the gate, which is the only contribution an unretryable lane can
// make to riding out a grow window. The COPY error path already did; the
// acquire returned raw, so a drop arriving one statement earlier told the
// sibling lanes nothing.

// refusingConnectorDriver is a driver whose Open always fails with the
// supplied error, so `db.Conn(ctx)` fails without a server.
type refusingConnectorDriver struct{ err error }

func (d refusingConnectorDriver) Open(string) (driver.Conn, error) { return nil, d.err }

func newRefusingDB(t *testing.T, openErr error) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("pg-refusing-acquire-%p", &openErr)
	sql.Register(name, refusingConnectorDriver{err: openErr})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func rawCopyPinTable() *ir.Table {
	return &ir.Table{
		Name: "raw_pin",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 8}},
			{Name: "v", Type: ir.Text{}},
		},
	}
}

// TestImportRawCopy_AcquireTripsTheGrowGate: a CLASSIFIED transient at the
// acquire must trip the gate so the sibling lanes park.
func TestImportRawCopy_AcquireTripsTheGrowGate(t *testing.T) {
	db := newRefusingDB(t, errors.New("dial tcp 10.0.0.9:5432: read: connection reset by peer"))
	gate := &recordingGrowGate{}
	w := &RowWriter{db: db, schema: "public", growGate: gate}

	_, err := w.ImportRawCopy(context.Background(), rawCopyPinTable(), ir.RawCopyText,
		strings.NewReader("1\tx\n"))
	if err == nil {
		t.Fatal("ImportRawCopy: want the acquire failure, got nil")
	}
	if !strings.Contains(err.Error(), "acquire conn") {
		t.Errorf("acquire error lost its wording: %v", err)
	}
	if got := gate.trips.Load(); got != 1 {
		t.Errorf("gate.Trip calls = %d; want 1 — an unretryable lane's only contribution to a grow window "+
			"is telling the siblings it hit one, and the acquire used to skip that", got)
	}
}

// TestImportRawCopy_AcquireDoesNotTripOnTerminal is the other direction:
// a wrong password is not a grow window, and quiescing every sibling lane
// for it would be the item-143 inference defect made worse.
func TestImportRawCopy_AcquireDoesNotTripOnTerminal(t *testing.T) {
	db := newRefusingDB(t, errors.New(`pq: password authentication failed for user "nope"`))
	gate := &recordingGrowGate{}
	w := &RowWriter{db: db, schema: "public", growGate: gate}

	if _, err := w.ImportRawCopy(context.Background(), rawCopyPinTable(), ir.RawCopyText,
		strings.NewReader("1\tx\n")); err == nil {
		t.Fatal("ImportRawCopy: want the acquire failure, got nil")
	}
	if got := gate.trips.Load(); got != 0 {
		t.Errorf("gate.Trip calls = %d; want 0 (a terminal acquire failure must not open a quiesce window)", got)
	}
}
