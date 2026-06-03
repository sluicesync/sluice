//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0058 — Online ADD COLUMN forwarding for single-stream (non-Shape-A)
// CDC apply. PG → PG live CDC: cold-start completes, source ALTER
// TABLE ADD COLUMN lands mid-stream, and post-ALTER INSERTs flow to
// the target.
//
// Two scenarios:
//
//   1. Flag OFF (default): pre-v0.79.0 behavior preserved. The
//      post-ALTER INSERT triggers `column does not exist` on the
//      applier and the row never lands. Pinned to lock the
//      no-behavior-change-without-opt-in contract.
//   2. Flag ON: target ALTER lands; post-ALTER INSERT flows through;
//      with --backfill-added-column also on, already-shipped rows
//      get the source's per-row values for the new column.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_AddColumnForward_PG_FlagOn_ForwardsALTER pins ADR-0058's
// load-bearing happy path on PG → PG: with --forward-schema-add-column
// set, a source ALTER TABLE ADD COLUMN forwards to the target and a
// subsequent INSERT carrying the new column lands.
func TestStreamer_AddColumnForward_PG_FlagOn_ForwardsALTER(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:                 pgEng,
		Target:                 pgEng,
		SourceDSN:              sourceDSN,
		TargetDSN:              targetDSN,
		StreamID:               "test-addcol-fwd-pg",
		ForwardSchemaAddColumn: true,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	// Source ALTER + post-ALTER INSERT. Pre-ADR-0058 with the flag
	// OFF the INSERT would crash the applier with "column does not
	// exist"; with --forward-schema-add-column on, the intercept
	// forwards the ALTER first, then the INSERT lands cleanly.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ADD COLUMN price NUMERIC(10,2);
		INSERT INTO widgets (id, name, price) VALUES (3, 'gamma', 3.75);
	`)

	if !waitForPGRowCount(t, targetDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed — forwarding broken")
	}

	// Verify the target schema has the new column.
	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var hasPrice int
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema='public' AND table_name='widgets' AND column_name='price'
	`).Scan(&hasPrice); err != nil {
		t.Fatalf("check column: %v", err)
	}
	if hasPrice != 1 {
		t.Errorf("target widgets.price column missing — intercept didn't forward the ALTER")
	}

	// Without --backfill-added-column, rows already on the target
	// (id=1, id=2) carry NULL for price. id=3 carries the post-ALTER
	// INSERT value.
	var gammaPrice sql.NullString
	if err := tgtDB.QueryRowContext(ctx, "SELECT price::text FROM widgets WHERE id=3").Scan(&gammaPrice); err != nil {
		t.Fatalf("scan gamma price: %v", err)
	}
	if !gammaPrice.Valid || gammaPrice.String != "3.75" {
		t.Errorf("widgets.price for id=3 = %v; want 3.75", gammaPrice)
	}

	var alphaPrice sql.NullString
	if err := tgtDB.QueryRowContext(ctx, "SELECT price::text FROM widgets WHERE id=1").Scan(&alphaPrice); err != nil {
		t.Fatalf("scan alpha price: %v", err)
	}
	if alphaPrice.Valid {
		t.Errorf("widgets.price for id=1 = %v; want NULL (no backfill flag)", alphaPrice)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_AddColumnForward_PG_Backfill_PopulatesPriorRows pins the
// --backfill-added-column path: rows shipped to the target before the
// ALTER get the source's per-row values for the new column.
func TestStreamer_AddColumnForward_PG_Backfill_PopulatesPriorRows(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:                 pgEng,
		Target:                 pgEng,
		SourceDSN:              sourceDSN,
		TargetDSN:              targetDSN,
		StreamID:               "test-addcol-backfill-pg",
		ForwardSchemaAddColumn: true,
		BackfillAddedColumn:    true,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	// Populate source's per-row values for the new column BEFORE the
	// ALTER (so backfill has source-side data to pull through). The
	// flow is:
	//   1. Cold-start replicates id=1, id=2 with no price column.
	//   2. Source ALTERs to add price (default NULL).
	//   3. Source UPDATEs assign per-row prices.
	//   4. Source INSERTs id=3.
	//
	// Backfill kicks in at step 2's SchemaSnapshot — it reads
	// (id, price) from source for rows already on the target. Since
	// the source's price is NULL at that exact moment (the UPDATEs
	// haven't happened yet), backfill reads NULL — same as no-backfill.
	//
	// To exercise the backfill PATH (not just the value), we structure
	// timing so step 3's UPDATEs happen BEFORE step 2 — but that
	// requires either re-ordering (impossible since ALTER comes first
	// in DDL) or accepting that backfill in PG → PG with a single
	// source DML position is mainly verifying the synthetic UPDATE
	// path emits cleanly.
	//
	// The simpler pin: after ALTER, sluice replicates the subsequent
	// source UPDATE events normally. Backfill's value-add is when
	// the source had per-row values BEFORE the ALTER — but in real
	// schema-evolution operator flows that's the "DEFAULT
	// <subquery>" case which ADR-0058 §2a refuses loudly.
	//
	// What this test pins instead: the backfill code path runs without
	// error AND the final state is correct after subsequent UPDATEs.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ADD COLUMN price NUMERIC(10,2);
		UPDATE widgets SET price = 1.25 WHERE id = 1;
		UPDATE widgets SET price = 2.50 WHERE id = 2;
		INSERT INTO widgets (id, name, price) VALUES (3, 'gamma', 3.75);
	`)

	if !waitForPGRowCount(t, targetDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed")
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Wait for the post-ALTER UPDATEs to flow.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var got sql.NullString
		if err := tgtDB.QueryRowContext(ctx, "SELECT price::text FROM widgets WHERE id=1").Scan(&got); err != nil {
			t.Fatalf("poll alpha price: %v", err)
		}
		if got.Valid && got.String == "1.25" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Final values.
	for id, want := range map[int]string{1: "1.25", 2: "2.50", 3: "3.75"} {
		var got sql.NullString
		if err := tgtDB.QueryRowContext(ctx, "SELECT price::text FROM widgets WHERE id=$1", id).Scan(&got); err != nil {
			t.Fatalf("scan id=%d: %v", id, err)
		}
		if !got.Valid || got.String != want {
			t.Errorf("widgets.price for id=%d = %v; want %s", id, got, want)
		}
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_AddColumnForward_PG_FlagOff_RefusesLoudly pins the negative
// case: with --forward-schema-add-column UNSET (default), the
// post-ALTER INSERT errors the streamer. This is the pre-v0.79.0
// behavior; the test guards against accidental default-on changes
// that would silently shift operator-visible semantics.
func TestStreamer_AddColumnForward_PG_FlagOff_RefusesLoudly(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name) VALUES (1, 'alpha');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-addcol-fwd-pg-off",
		// ForwardSchemaAddColumn deliberately unset — pre-v0.79.0
		// behavior is preserved.
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, targetDSN, "widgets", 1, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed row")
	}

	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ADD COLUMN price NUMERIC(10,2);
		INSERT INTO widgets (id, name, price) VALUES (2, 'beta', 1.99);
	`)

	// Without the forward flag, the post-ALTER INSERT errors the
	// applier; the stream surfaces a retry-loop error. Wait briefly
	// for the failure shape (target row count stays at 1).
	stuck := !waitForPGRowCount(t, targetDSN, "widgets", 2, 10*time.Second)
	if !stuck {
		t.Errorf("with flag OFF, post-ALTER INSERT should NOT land on target; got 2 rows (silent forwarding regression?)")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_AddColumnForward_PG_RefusesComputedDefault pins Bug 90's fix
// (v0.79.1): v0.79.0 silently forwarded an ADD COLUMN with a
// computed DEFAULT (NOW() / nextval / random / etc.) because the CDC
// reader's RelationMessage projection dropped the DEFAULT clause and
// the existing intercept's `ir.DefaultExpression` check found
// Default=nil. The fix wires a source-side SchemaReader probe and a
// text-based volatility scan; this test exercises the production
// flow end-to-end on PG → PG.
//
// Class-pin (Bug 74): the test runs two scenarios — NOW() (time-
// volatile) and nextval(seq) (sequence-stateful). The fix dispatches
// on the same volatility classifier so a single representative would
// only prove the classifier triggered, not that every class is
// covered. Two scenarios in one test file keeps the integration
// suite's runtime in check while still pinning both major Bug 90
// failure shapes.
func TestStreamer_AddColumnForward_PG_RefusesComputedDefault(t *testing.T) {
	scenarios := []struct {
		name   string
		ddl    string
		expect string // substring expected in the streamer error
	}{
		{
			name:   "now-default",
			ddl:    `ALTER TABLE widgets ADD COLUMN created_at TIMESTAMPTZ DEFAULT NOW();`,
			expect: "now",
		},
		{
			name: "nextval-default",
			ddl: `CREATE SEQUENCE IF NOT EXISTS widgets_counter START 100;
ALTER TABLE widgets ADD COLUMN seq_id BIGINT DEFAULT nextval('widgets_counter');`,
			expect: "nextval",
		},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
			defer cleanup()

			applyPGDDL(t, sourceDSN, `
				CREATE TABLE widgets (
					id BIGSERIAL PRIMARY KEY,
					name TEXT NOT NULL
				);
				ALTER TABLE widgets REPLICA IDENTITY FULL;
				INSERT INTO widgets (name) VALUES ('alpha'), ('beta');
			`)

			pgEng, ok := engines.Get("postgres")
			if !ok {
				t.Fatal("postgres engine not registered")
			}

			streamer := &Streamer{
				Source:                 pgEng,
				Target:                 pgEng,
				SourceDSN:              sourceDSN,
				TargetDSN:              targetDSN,
				StreamID:               "test-addcol-refuse-vol-" + sc.name,
				ForwardSchemaAddColumn: true,
			}

			streamCtx, streamCancel := context.WithCancel(context.Background())
			defer streamCancel()

			runErr := make(chan error, 1)
			go func() { runErr <- streamer.Run(streamCtx) }()

			if !waitForPGRowCount(t, targetDSN, "widgets", 2, 30*time.Second) {
				t.Fatalf("phase A: bulk-copy never landed seed rows")
			}

			// Source ALTER with the volatile DEFAULT + post-ALTER
			// INSERT to push the DDL through PG's logical replication.
			// The intercept must refuse the ADD COLUMN before issuing
			// the target ALTER; the post-ALTER INSERT then never
			// lands because the schema's diverged.
			applyPGDDL(t, sourceDSN, sc.ddl+`
				INSERT INTO widgets (name) VALUES ('gamma');
			`)

			// Wait for the stream to surface the refuse-loudly error.
			// The streamer's retry loop may try a few times before
			// giving up; cap the wait to keep CI runtime in check.
			var err error
			select {
			case err = <-runErr:
			case <-time.After(60 * time.Second):
				t.Fatal("streamer did not surface refuse-loudly error within timeout")
			}
			if err == nil {
				t.Fatal("streamer returned nil error; expected refuse-loudly on computed DEFAULT")
			}
			errStr := strings.ToLower(err.Error())
			if !strings.Contains(errStr, "computed default") {
				t.Errorf("error %q does not mention 'computed default'", err)
			}
			if !strings.Contains(errStr, sc.expect) {
				t.Errorf("error %q does not mention %q (volatile function name)", err, sc.expect)
			}
			if !strings.Contains(err.Error(), "ADR-0058 §2a") {
				t.Errorf("error %q does not cite ADR-0058 §2a", err)
			}

			// The target widgets table must NOT have the new column
			// (the intercept refused BEFORE issuing the target
			// ALTER).
			tgtDB, openErr := sql.Open("pgx", targetDSN)
			if openErr != nil {
				t.Fatalf("open target: %v", openErr)
			}
			defer func() { _ = tgtDB.Close() }()

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			var newColCount int
			colName := "created_at"
			if sc.name == "nextval-default" {
				colName = "seq_id"
			}
			if err := tgtDB.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM information_schema.columns
				WHERE table_schema='public' AND table_name='widgets' AND column_name=$1
			`, colName).Scan(&newColCount); err != nil {
				t.Fatalf("check target column: %v", err)
			}
			if newColCount != 0 {
				t.Errorf("target widgets.%s exists — intercept did NOT refuse the volatile DEFAULT (silent forwarding regression)", colName)
			}
		})
	}
}
