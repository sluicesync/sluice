//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStream_VTTestServer_BlockedConsumerIsNotAHungStream pins the VStream
// arm of perf-parity gap 37. The CDC pump's Phase-2 watchdog reads total
// silence as a hung stream (the post-failover wedge) — but a pump parked
// handing a change to a consumer that is not taking it is silent too, and
// before [sendHeld] the watchdog could not tell the two apart. Measured on
// vttestserver: a consumer stalled 75s ended the stream at the default 45s
// window, a retryable error the pipeline answers by reconnecting; during a
// forwarded ADD COLUMN's backfill (which holds the consumer for a pass over
// the table) that ends the attempt with the backfill unconfirmed — a
// terminal ADD-COLUMN-BACKFILL-INCOMPLETE.
//
// The window is lowered per-DSN (vstream_progress_timeout) so the stall is
// seconds, not minutes. The grading is against the source's own rows: every
// row written before the stall arrives exactly once, a row written after it
// arrives, and the reader records no error.
func TestVStream_VTTestServer_BlockedConsumerIsNotAHungStream(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	const (
		progressWindow = 5 * time.Second
		rows           = 2000 // far more changes than the reader's channel buffers
	)
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE bc (id BIGINT NOT NULL PRIMARY KEY, pad VARCHAR(64) NOT NULL) ENGINE=InnoDB`)
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE d10 (d INT NOT NULL PRIMARY KEY) ENGINE=InnoDB`)
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO d10 VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)`)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_progress_timeout=%s",
		mysqlDSN, grpcEndpoint, progressWindow,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	rdr, err := Engine{Flavor: FlavorPlanetScale}.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	readerErr := func() error {
		if r, ok := rdr.(interface{ Err() error }); ok {
			return r.Err()
		}
		return nil
	}
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(2 * time.Second) // vtgate registers the stream at "current"

	// One statement, 2,000 row changes: the pump fills the channel and
	// parks on the consumer below.
	applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(
		`INSERT INTO bc (id, pad) SELECT a.d*1000 + b.d*100 + c.d*10 + e.d + 1, 'r' FROM d10 a, d10 b, d10 c, d10 e WHERE a.d < %d`, rows/1000,
	))

	// Stall the consumer for three progress windows.
	time.Sleep(3 * progressWindow)
	if err := readerErr(); err != nil {
		t.Fatalf("the reader recorded an error while only its consumer was stalled (%s, progress window %s): %v — a pump parked on its consumer was read as a hung stream", 3*progressWindow, progressWindow, err)
	}

	seen := make(map[int64]int, rows)
	for len(seen) < rows {
		select {
		case c, ok := <-changes:
			if !ok {
				t.Fatalf("the change channel closed after %d of %d rows: %v", len(seen), rows, readerErr())
			}
			if ins, isIns := c.(ir.Insert); isIns && ins.Table == "bc" {
				id, _ := ins.Row["id"].(int64)
				seen[id]++
			}
		case <-ctx.Done():
			t.Fatalf("timed out with %d of %d rows delivered (reader err: %v)", len(seen), rows, readerErr())
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("row id=%d delivered %d times; want exactly once", id, n)
		}
	}

	// The stream carries on past the stall.
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO bc (id, pad) VALUES (999999, 'after')`)
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				t.Fatalf("the change channel closed before the post-stall row: %v", readerErr())
			}
			if ins, isIns := c.(ir.Insert); isIns && ins.Table == "bc" {
				if id, _ := ins.Row["id"].(int64); id == 999999 {
					if err := readerErr(); err != nil {
						t.Fatalf("the reader recorded an error after the stall: %v", err)
					}
					return
				}
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for the post-stall row (reader err: %v)", readerErr())
		}
	}
}
