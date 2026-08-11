//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-server half of the [CDCReader.Close] pump-join pin (the unit half
// is cdc_reader_close_join_test.go). This one closes a reader whose pump
// is genuinely live against a real mysqld, parked exactly where the
// defect bites: inside [CDCReader.tableFor]'s schema lookup, holding the
// very pool Close is about to release.
//
// The parking is deliberate and the test is worse without it. The first
// draft simply closed a live reader and asserted the change channel was
// already closed — and it PASSED with the join deleted, because a
// cancelled pump usually unwinds faster than Close returns. A gate whose
// green survives the defect it names is the thing this project keeps
// paying for, so the pin now holds the pump inside the lookup and
// asserts Close BLOCKS there. Deleting the join fails it deterministically.
//
// Why a real server rather than the unit stand-in: two of the claims
// under the fix are about go-mysql, not about sluice. Close joins a pump
// whose event source is a real BinlogSyncer, and it needs that pump to
// observe its own cancellation — the Postgres sibling of this fix found
// exactly that half broken (pgconn dressed an already-done context as a
// timeout, so a cancelled pump spun forever and the join hung). Regress
// that here and this test does not fail an assertion, it HANGS at Close
// and dies on the package timeout.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestCDCReader_CloseJoinsLivePump(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE join_probe (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	eng := Engine{Flavor: FlavorVanilla}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	opened, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	rdr, ok := opened.(*CDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T, want *CDCReader — this pin reaches the schema-loader seam", opened)
	}

	var (
		parked   = make(chan struct{}) // the pump is inside the schema lookup
		release  = make(chan struct{}) // the test decides when it may finish
		poolOpen = make(chan error, 1) // was r.db still usable in there?
	)
	// The seam tableFor loads through, parked where the real
	// information_schema query would be. Holding the pump HERE is the
	// whole point: this is the one place the pump touches the pool Close
	// releases, so a Close that does not join lands `r.db.Close()` on a
	// live reader — and on this engine, unlike Postgres, there is not even
	// a nil check between that and a panicking method call.
	var once bool
	rdr.schemaLoader = func(ctx context.Context, db *sql.DB, schema, table string, flavor Flavor) (*tableSchema, error) {
		if !once {
			once = true
			close(parked)
			<-release
			// The pool must still be usable at this instant. Without the
			// join it has already been closed underneath us, which is the
			// defect stated as an assertion rather than as a race the
			// scheduler may or may not lose.
			poolOpen <- db.PingContext(context.Background())
		}
		return loadTableSchema(ctx, db, schema, table, flavor)
	}

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// The syncer registers asynchronously; the house pattern in this
	// package is a short settle before generating events. Nothing below
	// depends on timing — the test blocks on `parked` — so this cannot
	// make the pin flaky in either direction.
	time.Sleep(200 * time.Millisecond)
	applyMySQL(t, dsn, `INSERT INTO join_probe (email) VALUES ('a@example.com'), ('b@example.com');`)

	select {
	case <-parked:
	case <-time.After(60 * time.Second):
		t.Fatal("the pump never reached the schema lookup — no TABLE_MAP for join_probe arrived, so this pin never got to the state it exists to test")
	}

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- rdr.Close() }()

	select {
	case err := <-closeReturned:
		t.Fatalf("Close returned (%v) while the pump was inside its information_schema lookup: it must join the pump before releasing the pool that lookup is running on", err)
	case <-time.After(500 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-poolOpen:
		if err != nil {
			t.Errorf("the schema pool was already closed while the pump was still using it: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the parked loader never reported")
	}

	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Close did not return after the pump was released — the pump could not observe its own cancellation, and the join is now a hang rather than a leak (this is the shape the Postgres sibling of this fix had)")
	}

	// The pump closes the change channel as its last act before signalling
	// done, so a Close that joined has ALREADY closed it. Non-blocking on
	// purpose: no sleep, no tolerance.
	drained := 0
	for {
		select {
		case _, ok := <-changes:
			if !ok {
				return
			}
			drained++
		default:
			t.Fatalf("Close returned but the change channel is neither closed nor drained (%d buffered events) — the pump outlived the join", drained)
		}
	}
}
