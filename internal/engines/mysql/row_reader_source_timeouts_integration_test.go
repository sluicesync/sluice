//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"
)

// TestOpenRowReader_RaisesSourceReadSessionTimeouts is the ADR-0109 §A
// live pin: a source RowReader's pooled session has net_write_timeout /
// net_read_timeout raised to the bounded default, so a transient
// target-stall-induced backpressure (the source read idling while the
// writer can't drain) does NOT trip the source server's default 60s
// net_write_timeout and drop the cold-copy read.
//
// Asserted against a REAL source read session — the @@-variable readback is
// the ground truth that the DSN-param SET actually took at handshake (a unit
// test on cfg.Params can't prove the driver emitted the SET and the server
// accepted it).
func TestOpenRowReader_RaisesSourceReadSessionTimeouts(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rr, err := (Engine{Flavor: FlavorVanilla}).OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() { _ = rr.(*RowReader).Close() }()

	q := rr.(*RowReader).q
	for _, v := range []string{"net_write_timeout", "net_read_timeout"} {
		var got int
		rows, err := q.QueryContext(ctx, "SELECT @@SESSION."+v)
		if err != nil {
			t.Fatalf("query @@SESSION.%s: %v", v, err)
		}
		if !rows.Next() {
			rows.Close()
			t.Fatalf("@@SESSION.%s returned no rows", v)
		}
		if err := rows.Scan(&got); err != nil {
			rows.Close()
			t.Fatalf("scan @@SESSION.%s: %v", v, err)
		}
		rows.Close()
		if got != sourceReadSessionTimeoutSeconds {
			t.Errorf("@@SESSION.%s = %d; want %d (ADR-0109 §A bounded source-read timeout)",
				v, got, sourceReadSessionTimeoutSeconds)
		}
	}
}
