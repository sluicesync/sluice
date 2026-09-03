//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-09-01 A2-1 — the drained-model recovery a mid-stream DDL
// refusal prescribes must actually run. End-to-end PG → PG through the
// real Streamer and the real applier: DML → DDL on the source → the
// refusal → the operator applies the same DDL on the target → a restart
// with the SAME --stream-id lands the next row under the new shape, and
// `sluice_cdc_skipped_tables` stays empty.
//
// Before the fix the Postgres reader's TxCommit carried CommitLSN, the
// commit record's START, which logical decoding re-delivers; every warm
// resume replayed the last applied transaction, its historic
// RelationMessage re-seeded the relation cache in the pre-DDL shape, and
// the refusal fired again — and the replayed RENAME TABLE rows were
// routed to the old name and counted in the skipped-table ledger,
// tripping `sync health`. The reader-level pins are in
// internal/engines/postgres (TestPGCDC_DrainedModelRecoveryResumes,
// TestPGCDC_TxCommitPositionIsPostCommit); this is the half with a real
// target and a real ledger.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// readSkippedTableLedger returns "table_name=skip_count" for every
// ledger row of streamID, tolerant of the ledger not existing.
func readSkippedTableLedger(t *testing.T, dsn, streamID string) []string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx,
		`SELECT table_name, skip_count FROM "public"."sluice_cdc_skipped_tables" WHERE stream_id = $1 ORDER BY table_name`,
		streamID)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return nil
		}
		t.Fatalf("read skipped-table ledger: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name string
		var n int64
		if err := rows.Scan(&name, &n); err != nil {
			t.Fatalf("scan ledger row: %v", err)
		}
		out = append(out, fmt.Sprintf("%s=%d", name, n))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("ledger rows: %v", err)
	}
	return out
}

// selectText returns the single TEXT value of `SELECT col FROM table
// WHERE id = id`.
func selectText(t *testing.T, dsn, table, col string, id int) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var v string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT %s FROM %s WHERE id = $1`, col, table), id).Scan(&v); err != nil {
		t.Fatalf("select %s.%s id=%d: %v", table, col, id, err)
	}
	return v
}

// TestStreamer_PGToPG_DrainedModelRecovery is the A2-1 gate with a real
// target. Every scenario: the last applied transaction touches the
// table (the audit's boundary condition for the wedge), the source DDL
// plus one post-DDL row makes the stream refuse (the anti-vacuity
// floor), the same DDL is applied on the target by hand, and a restart
// with the same stream-id must land the post-DDL row and leave the
// skipped-table ledger empty.
//
// Shapes: RENAME TABLE (refuses in every mode at the reader — the
// observed ledger-tripping shape), RENAME COLUMN and DROP COLUMN under
// --schema-changes=refuse (the reader refusals whose replayed rows would
// also have failed against the reshaped target), and the
// projection-invisible interval typmod gate under forward mode.
func TestStreamer_PGToPG_DrainedModelRecovery(t *testing.T) {
	for _, sc := range []struct {
		name   string
		mode   string
		create string
		// sourceDDL is the mid-stream source change plus the post-DDL
		// row (id 3).
		sourceDDL string
		// targetDDL is what the operator applies on the target by hand.
		targetDDL string
		// want are substrings the FIRST run's refusal must carry.
		want []string
		// finalTable / col / value locate the post-DDL row on the target.
		finalTable string
		col        string
		value      string
	}{
		{
			name:       "rename-table/forward",
			mode:       "forward",
			create:     `CREATE TABLE t8 (id INT PRIMARY KEY, v TEXT); ALTER TABLE t8 REPLICA IDENTITY FULL; INSERT INTO t8 VALUES (1, 'seed');`,
			sourceDDL:  `ALTER TABLE t8 RENAME TO t8b; INSERT INTO t8b VALUES (3, 'after');`,
			targetDDL:  `ALTER TABLE t8 RENAME TO t8b;`,
			want:       []string{"RENAME public.t8 → public.t8b", "Drained-model recovery"},
			finalTable: "t8b",
			col:        "v",
			value:      "after",
		},
		{
			name:       "rename-column/refuse",
			mode:       "refuse",
			create:     `CREATE TABLE t (id INT PRIMARY KEY, v TEXT); ALTER TABLE t REPLICA IDENTITY FULL; INSERT INTO t VALUES (1, 'seed');`,
			sourceDDL:  `ALTER TABLE t RENAME COLUMN v TO w; INSERT INTO t VALUES (3, 'after');`,
			targetDDL:  `ALTER TABLE t RENAME COLUMN v TO w;`,
			want:       []string{"RENAME COLUMN v → w", "Drained-model recovery"},
			finalTable: "t",
			col:        "w",
			value:      "after",
		},
		{
			name:       "drop-column/refuse",
			mode:       "refuse",
			create:     `CREATE TABLE t (id INT PRIMARY KEY, v TEXT, c INT); ALTER TABLE t REPLICA IDENTITY FULL; INSERT INTO t VALUES (1, 'seed', 0);`,
			sourceDDL:  `ALTER TABLE t DROP COLUMN c; INSERT INTO t VALUES (3, 'after');`,
			targetDDL:  `ALTER TABLE t DROP COLUMN c;`,
			want:       []string{"DROP COLUMN", "Drained-model recovery"},
			finalTable: "t",
			col:        "v",
			value:      "after",
		},
		{
			name:       "interval-typmod/forward",
			mode:       "forward",
			create:     `CREATE TABLE t (id INT PRIMARY KEY, v TEXT, iv INTERVAL(6)); ALTER TABLE t REPLICA IDENTITY FULL; INSERT INTO t VALUES (1, 'seed', '1 second');`,
			sourceDDL:  `ALTER TABLE t ALTER COLUMN iv TYPE INTERVAL(3); INSERT INTO t VALUES (3, 'after', '1.5 seconds');`,
			targetDDL:  `ALTER TABLE t ALTER COLUMN iv TYPE INTERVAL(3);`,
			want:       []string{"cannot be forwarded", `column "iv"`, "Drained-model recovery"},
			finalTable: "t",
			col:        "v",
			value:      "after",
		},
		// SLM-1c: the live session-TimeZone cast refusal, then the
		// recovery. After the operator's target ALTER the resumed
		// process's first RelationMessage is checked against the SEEDED
		// prior (the target's post-ALTER family), which now equals the
		// wire shape — a seed taken from the history instead would refuse
		// again, forever (the SLM-1b loop, on this lane).
		{
			name:       "timestamp-zone-swap/forward",
			mode:       "forward",
			create:     `CREATE TABLE t (id INT PRIMARY KEY, v TEXT, c TIMESTAMP); ALTER TABLE t REPLICA IDENTITY FULL; INSERT INTO t VALUES (1, 'seed', '2020-01-01 12:00:00');`,
			sourceDDL:  `ALTER TABLE t ALTER COLUMN c TYPE TIMESTAMPTZ; INSERT INTO t VALUES (3, 'after', '2020-01-01 12:00:00+00');`,
			targetDDL:  `ALTER TABLE t ALTER COLUMN c TYPE TIMESTAMPTZ;`,
			want:       []string{"cannot be forwarded", `column "c"`, "TimeZone", "Drained-model recovery"},
			finalTable: "t",
			col:        "v",
			value:      "after",
		},
	} {
		t.Run(sc.name, func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
			defer cleanup()
			applyDDL(t, sourceDSN, sc.create)

			pgEng, ok := engines.Get("postgres")
			if !ok {
				t.Fatal("postgres engine not registered")
			}
			streamID := "a2-1-" + strings.ReplaceAll(sc.name, "/", "-")
			newStreamer := func() *Streamer {
				return &Streamer{
					Source:        pgEng,
					Target:        pgEng,
					SourceDSN:     sourceDSN,
					TargetDSN:     targetDSN,
					StreamID:      streamID,
					SchemaChanges: sc.mode,
				}
			}
			// The table the first run streams into; the rename scenario
			// moves it afterwards.
			firstTable := sc.finalTable
			if sc.name == "rename-table/forward" {
				firstTable = "t8"
			}
			insertTwo := fmt.Sprintf(`INSERT INTO %s (id, v) VALUES (2, 'two');`, firstTable)

			// ---- Run 1: cold start, one CDC transaction on the table
			// (its position is what the restart resumes from), then the
			// refusal.
			ctx1, cancel1 := context.WithCancel(context.Background())
			defer cancel1()
			runErr1 := make(chan error, 1)
			go func() { runErr1 <- newStreamer().Run(ctx1) }()
			if !waitForRowCount(t, targetDSN, firstTable, 1, 60*time.Second) {
				t.Fatal("run 1: cold start never landed the seed row")
			}
			applyDDL(t, sourceDSN, insertTwo)
			if !waitForRowCount(t, targetDSN, firstTable, 2, 30*time.Second) {
				t.Fatal("run 1: CDC never landed id=2")
			}
			applyDDL(t, sourceDSN, sc.sourceDDL)
			var err error
			select {
			case err = <-runErr1:
			case <-time.After(90 * time.Second):
				t.Fatal("run 1 did not refuse within 90s — the floor: the first run must refuse")
			}
			if err == nil {
				t.Fatal("run 1 returned nil; the floor requires the refusal")
			}
			for _, want := range sc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("run 1 refusal missing %q; got: %v", want, err)
				}
			}
			cancel1()

			// ---- The operator follows the hint: apply the DDL on the
			// target, restart with the same stream-id.
			applyDDL(t, targetDSN, sc.targetDDL)
			ctx2, cancel2 := context.WithCancel(context.Background())
			defer cancel2()
			runErr2 := make(chan error, 1)
			go func() { runErr2 <- newStreamer().Run(ctx2) }()
			landed := make(chan bool, 1)
			go func() { landed <- waitForRowCount(t, targetDSN, sc.finalTable, 3, 60*time.Second) }()
			select {
			case err := <-runErr2:
				t.Fatalf("run 2 died on the warm resume (the recovery the hint prescribes cannot run): %v", err)
			case ok := <-landed:
				if !ok {
					t.Fatalf("run 2 never landed the post-DDL row on %s", sc.finalTable)
				}
			}
			if got := selectText(t, targetDSN, sc.finalTable, sc.col, 3); got != sc.value {
				t.Errorf("post-DDL row on the target: %s.%s = %q; want %q", sc.finalTable, sc.col, got, sc.value)
			}
			if ledger := readSkippedTableLedger(t, targetDSN, streamID); len(ledger) != 0 {
				t.Errorf("sluice_cdc_skipped_tables is not empty after the recovery: %v — replayed rows were routed to a shape the target no longer has", ledger)
			}
			cancel2()
			select {
			case <-runErr2:
			case <-time.After(30 * time.Second):
				t.Fatal("run 2 did not return after ctx cancel")
			}
		})
	}
}
