//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_ColdStart_PG_TargetSchemaReachesTheParallelLanes is the
// SYNC half of audit 2026-09-09 A0909-PDD-P1's pin.
//
// The fix threads `--target-schema` onto the writer `openOneChunkConn`
// mints, and that opener serves BOTH lanes: migrate's copy and the sync
// cold start's. The pin that shipped with the fix covers migrate only
// (`TestMigrate_PG_TargetSchema`, which gained an above-threshold table
// for exactly this reason). The pre-tag value-fidelity review filed the
// gap as VF0909C-4: the fix reaches both lanes, the pin reached one —
// which is the one-lane-pinned shape the roster around it was built to
// prevent, one level down.
//
// The defect this refuses to let back in: with the override missing from
// the chunk opener, the primary writer wrote to the named schema and
// every peer wrote to `public`. Loud when `public.<table>` is absent,
// SILENT when it exists and accepts the rows.
//
// The table is seeded ABOVE the within-table parallel threshold and the
// stream is configured with a lowered `BulkParallelMinRows`, because a
// small fixture reaches only the primary writer and cannot see this at
// all. That is not incidental — it is the entire reason the original
// migrate pin missed a defect present since v0.25.0.
func TestStreamer_ColdStart_PG_TargetSchemaReachesTheParallelLanes(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const (
		schemaName = "svc_analytics"
		rows       = 2_500
	)
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, src, `
		CREATE TABLE orders (id BIGINT PRIMARY KEY, note TEXT NOT NULL);
		INSERT INTO orders (id, note) SELECT g, 'order-' || g FROM generate_series(1, 2500) AS g;
		CREATE TABLE customers (id BIGINT PRIMARY KEY, email TEXT NOT NULL);
		INSERT INTO customers (id, email) SELECT g, 'c' || g || '@example.com' FROM generate_series(1, 50) AS g;
	`)

	streamer := &Streamer{
		Source:       pgEng,
		Target:       pgEng,
		SourceDSN:    src,
		TargetDSN:    tgt,
		StreamID:     "coldstart-target-schema",
		TargetSchema: schemaName,
		// Force `orders` onto the within-table chunk axis. Without this the
		// fixture would have to be enormous to cross the adaptive
		// threshold, and a cold start that only reaches the primary writer
		// cannot observe the defect at all.
		BulkParallelMinRows: 1000,
		BulkParallelism:     4,
	}

	ctx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()

	for _, tc := range []struct {
		table string
		want  int
	}{
		{"orders", rows},
		{"customers", 50},
	} {
		if !waitForQualifiedRowCount(tgt, schemaName, tc.table, tc.want, 3*time.Minute) {
			streamCancel()
			t.Fatalf("cold start never delivered %d rows to %s.%s (got %d) — the copy went somewhere else, "+
				"which is audit A0909-PDD-P1 on the sync lane",
				tc.want, schemaName, tc.table, qualifiedRowCount(tgt, schemaName, tc.table))
		}
	}

	// The load-bearing assertion. Under the defect the peer chunks landed
	// in `public`, so a target that ALSO holds these tables in `public` is
	// the silent arm. Neither may exist there.
	for _, table := range []string{"orders", "customers"} {
		if publicTableExists(tgt, table) {
			streamCancel()
			t.Errorf("public.%s exists after a --target-schema cold start; the parallel copy lanes wrote "+
				"outside the named schema (audit A0909-PDD-P1)", table)
		}
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(30 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// qualifiedRowCount counts rows in an explicitly schema-qualified table,
// returning -1 when the table is absent. The unqualified helpers in this
// package resolve through search_path, which is exactly the ambiguity
// this test exists to remove.
func qualifiedRowCount(dsn, schema, table string) int {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return -1
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %q.%q`, schema, table)).Scan(&n); err != nil {
		return -1
	}
	return n
}

func waitForQualifiedRowCount(dsn, schema, table string, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if qualifiedRowCount(dsn, schema, table) == want {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

// publicTableExists asks the catalog rather than counting rows: under the
// defect the table would exist in `public` and hold the peer chunks'
// rows, but a zero-row `public` table is equally wrong here and a count
// alone would not distinguish "absent" from "empty".
func publicTableExists(dsn, table string) bool {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	var exists bool
	if err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)`, table).Scan(&exists); err != nil {
		return false
	}
	return exists
}
