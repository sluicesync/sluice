//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Phase 1 of task #62 (ADR-0066): load-bearing end-to-end test for
// the postgres-trigger engine, same-engine round-trip. The test:
//
//  1. Boots a postgres:16 container WITHOUT wal_level=logical (the
//     trigger engine's whole point is to work on tiers that lock
//     down logical replication; the upstream image's default
//     wal_level=replica matches what Heroku Essential / Render
//     Basic-class tiers expose).
//  2. Seeds the source with N rows.
//  3. Runs `pgtrigger.Setup` to install the change-log table +
//     capture function + per-table triggers.
//  4. Runs `pipeline.Migrator` to bulk-copy the seed rows to a
//     target database (same engine, vanilla `postgres-trigger` →
//     `postgres-trigger` round-trip). The bulk-copy path doesn't
//     need CDC; the trigger engine inherits the row-reader / row-
//     writer surfaces from the composed postgres engine.
//  5. Opens the trigger engine's CDC reader against the source +
//     applies post-migrate INSERT / UPDATE / DELETE; asserts each
//     change reaches the target via the change-applier.
//
// Same-engine sanity per ADR-0001 / CLAUDE.md "validate end-to-end
// before building more". Cross-engine (postgres-trigger → mysql /
// planetscale) is Phase 2 work; the full §15 family-matrix pin is
// Phase 3.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	"sluicesync.dev/sluice/internal/ir"

	// Side-effect imports register both engines.
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startPostgresPlain boots a PG container with the upstream image's
// default settings (no wal_level=logical override). Returns DSNs for
// source_db and target_db plus a cleanup callback. This models the
// most restricted PG tier shape the trigger engine targets.
func startPostgresPlain(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := pgtc.Run(
		ctx,
		"postgres:16",
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("pgx", srcConn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}

	tgtConn, err := swapDSNDatabase(srcConn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}
	return srcConn, tgtConn, terminate
}

// swapDSNDatabase replaces the database-name component of a Postgres
// URI DSN. Local helper so this test file doesn't depend on the
// existing pipeline buildPGDSN helper (build-tag isolation; the
// helper here is read-only).
func swapDSNDatabase(orig, newDB string) (string, error) {
	u, err := url.Parse(orig)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	if u.Path == "" || u.Path == "/" {
		return "", fmt.Errorf("DSN has no db-name path: %q", orig)
	}
	u.Path = "/" + strings.TrimPrefix(newDB, "/")
	return u.String(), nil
}

// applyPGTrigDDL runs a possibly-multi-statement DDL block.
func applyPGTrigDDL(t *testing.T, dsn, ddl string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}
}

// recordingAppliedCount returns the row count for table on dsn.
// Used to confirm the target reflects CDC-driven INSERT / UPDATE /
// DELETE.
func recordingAppliedCount(t *testing.T, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// TestMigratePGTrigger_BulkCopyPlusCDCRoundTrip is the load-bearing
// end-to-end test for Phase 1 of ADR-0066. Bulk-copy + CDC tail of
// postgres-trigger → postgres-trigger, N rows seeded + M rows added
// via CDC, asserting that the target reflects the source after a
// stable-state window. Per task #62: "this is the §15 sanity test
// that gates the rest of the engine work."
func TestMigratePGTrigger_BulkCopyPlusCDCRoundTrip(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresPlain(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE orders (
			id      BIGINT PRIMARY KEY,
			amount  NUMERIC(20,10) NOT NULL,
			memo    TEXT
		);
		ALTER TABLE orders REPLICA IDENTITY FULL;

		INSERT INTO orders (id, amount, memo) VALUES
			(1, 100.10, 'seed-1'),
			(2, 200.20, 'seed-2'),
			(3, 300.30, 'seed-3');
	`
	applyPGTrigDDL(t, sourceDSN, seedDDL)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Step 1: install the trigger-engine state on the source.
	if _, err := pgtrigger.Setup(ctx, sourceDSN, pgtrigger.SetupOptions{
		Tables: []string{"orders"},
		Schema: "public",
	}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}

	// Step 2: bulk-copy via pipeline.Migrator using
	// postgres-trigger on both sides. The trigger engine's row-
	// reader / row-writer surfaces come from the composed
	// postgres.Engine; the migrator never touches CDC.
	trigEng, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	mig := &Migrator{
		Source:    trigEng,
		Target:    trigEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		// Exclude the sluice_change_log table from the migration —
		// the change-log is sluice-managed source-side state, not
		// user data, and the target doesn't need it.
		Filter: mustNewFilter(t, nil, []string{
			pgtrigger.ChangeLogTable,
			pgtrigger.ChangeLogMetaTable,
		}),
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	if got := recordingAppliedCount(t, targetDSN, "orders"); got != 3 {
		t.Fatalf("target rows after bulk-copy = %d; want 3", got)
	}

	// Step 3: open the trigger-engine CDC reader on the source.
	// The reader anchors to MAX(id) ("from now"); INSERTs issued
	// AFTER the StreamChanges call must reach the channel.
	reader, err := trigEng.OpenCDCReader(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	out, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Step 4: open the target-side change applier and tail the
	// channel into it. The applier comes from the composed
	// postgres.Engine — same INSERT / UPDATE / DELETE SQL the
	// vanilla PG path uses.
	applier, err := trigEng.OpenChangeApplier(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	// Apply the channel in a goroutine. The streamID is opaque to
	// the trigger reader; we pick a stable value so the target's
	// sluice_cdc_state row carries something readable.
	applyCtx, applyCancel := context.WithCancel(ctx)
	defer applyCancel()

	const streamID = "pgtrigger-roundtrip-test"
	applyDone := make(chan error, 1)
	var applyWG sync.WaitGroup
	applyWG.Add(1)
	go func() {
		defer applyWG.Done()
		applyDone <- applier.Apply(applyCtx, streamID, out)
	}()

	// Step 5: drive CDC-time changes on the source. INSERT + UPDATE +
	// DELETE on order id=4 — every shape, with a JSONB-encoded
	// NUMERIC value to flex the §4 round-trip path.
	applyPGTrigDDL(t, sourceDSN, `
		INSERT INTO orders (id, amount, memo) VALUES (4, 400.40, 'cdc-insert');
		UPDATE orders SET memo = 'cdc-updated' WHERE id = 4;
		INSERT INTO orders (id, amount, memo) VALUES (5, 500.50, 'cdc-second');
		DELETE FROM orders WHERE id = 5;
	`)

	// Step 6: wait for the apply path to catch up. We poll the
	// target's row count + memo state. Expected stable state:
	//   id=1,2,3 from bulk copy
	//   id=4 with memo='cdc-updated' (insert then update)
	//   id=5 absent (insert then delete)
	deadline := time.Now().Add(45 * time.Second)
	var lastCount int
	var lastMemo string
	for time.Now().Before(deadline) {
		lastCount = recordingAppliedCount(t, targetDSN, "orders")
		lastMemo = readOrderMemo(t, targetDSN, 4)
		if lastCount == 4 && lastMemo == "cdc-updated" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastCount != 4 {
		t.Errorf("target row count = %d; want 4 (3 seeded + 1 CDC-inserted-then-update; id=5 cancelled by delete)", lastCount)
	}
	if lastMemo != "cdc-updated" {
		t.Errorf("orders[4].memo = %q; want 'cdc-updated' (insert then update via CDC)", lastMemo)
	}

	// Confirm id=5 is absent (CDC delete landed).
	if exists := orderExists(t, targetDSN, 5); exists {
		t.Errorf("orders[5] exists; want absent (CDC delete should have removed it)")
	}

	// Confirm the NUMERIC value round-tripped without precision
	// loss. The seeded id=1 has 100.10; the CDC-inserted id=4 has
	// 400.40. Both must match what we wrote.
	if got := readOrderAmount(t, targetDSN, 4); got != "400.4000000000" && got != "400.40" {
		t.Errorf("orders[4].amount = %q; want a precision-preserving round-trip of 400.40", got)
	}

	// Step 7: clean shutdown — cancel the applier so the channel
	// drain exits, then wait for the goroutine.
	applyCancel()
	applyWG.Wait()
	select {
	case err := <-applyDone:
		// context.Canceled is the expected exit shape.
		if err != nil && !strings.Contains(err.Error(), "context canceled") {
			t.Errorf("applier.Apply returned non-cancel error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("applier did not exit after ctx cancel")
	}
}

// mustNewFilter wraps NewTableFilter so the test code stays compact.
func mustNewFilter(t *testing.T, include, exclude []string) TableFilter {
	t.Helper()
	f, err := NewTableFilter(include, exclude)
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	return f
}

// readOrderMemo returns the memo column for the row with id=id, or
// the empty string when no row matches.
func readOrderMemo(t *testing.T, dsn string, id int) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var memo sql.NullString
	row := db.QueryRowContext(ctx, "SELECT memo FROM orders WHERE id = $1", id)
	if err := row.Scan(&memo); err != nil {
		if err == sql.ErrNoRows {
			return ""
		}
		t.Fatalf("scan memo: %v", err)
	}
	return memo.String
}

// readOrderAmount returns the amount column for the row with id=id
// in its TEXT representation so the test can pin precision without
// reaching for big.Rat. Returns "" on missing row.
func readOrderAmount(t *testing.T, dsn string, id int) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var amount sql.NullString
	row := db.QueryRowContext(ctx, "SELECT amount::text FROM orders WHERE id = $1", id)
	if err := row.Scan(&amount); err != nil {
		if err == sql.ErrNoRows {
			return ""
		}
		t.Fatalf("scan amount: %v", err)
	}
	return amount.String
}

// orderExists reports whether a row with id=id is present.
func orderExists(t *testing.T, dsn string, id int) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders WHERE id = $1", id).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n > 0
}
