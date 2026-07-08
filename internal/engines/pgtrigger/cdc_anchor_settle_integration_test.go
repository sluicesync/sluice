//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Live pins for the anchor settle-wait helpers (cdc_anchor_settle.go).
// The through-the-handoff pin (the invisible in-flight low-id repro via
// the real OpenSnapshotStream) lives in
// cdc_snapshot_settle_integration_test.go.

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestSettleWait_TimeoutRefusesLoudly pins the bounded loud refusal: a
// wedged writer transaction must not hang cold-start forever. The wait
// helper is exercised against a REAL stuck txn with a short budget; the
// refusal must name the stuck txid(s) and the operator action.
// (OpenSnapshotStream passes the production anchorSettleTimeout to this
// same helper — the refusal path from there is straight error
// propagation.) The stuck txn deliberately reproduces the live-probed
// at-or-above-xmax shape (the ONLY txn running, so it is absent from
// every snapshot's xip list) — the shape a pg_snapshot_xip-based wait
// silently misses.
func TestSettleWait_TimeoutRefusesLoudly(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT);`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"items"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// A genuinely stuck writer: open, one write (assigns a txid), never
	// settles within the wait budget.
	dbA, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	defer func() { _ = dbA.Close() }()
	txA, err := dbA.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	defer func() { _ = txA.Rollback() }()
	if _, err := txA.ExecContext(ctx, `INSERT INTO items (id, label) VALUES (1, 'stuck')`); err != nil {
		t.Fatalf("insert A: %v", err)
	}
	var txidA int64
	if err := txA.QueryRowContext(ctx, `SELECT pg_current_xact_id()::text::bigint`).Scan(&txidA); err != nil {
		t.Fatalf("read tx-A txid: %v", err)
	}

	// The copy-snapshot horizon (A invisible in it) and the settle
	// bound, captured in the production order: horizon first, bound
	// after.
	snapText, err := captureSnapshotText(ctx, db)
	if err != nil {
		t.Fatalf("captureSnapshotText: %v", err)
	}
	upperBound, err := captureTxidUpperBound(ctx, db)
	if err != nil {
		t.Fatalf("captureTxidUpperBound: %v", err)
	}
	if upperBound <= txidA {
		t.Fatalf("upper bound %d does not exceed tx-A's txid %d", upperBound, txidA)
	}

	err = waitForPreSnapshotTxnsToSettle(ctx, db, upperBound, 2*time.Second)
	if err == nil {
		t.Fatal("waitForPreSnapshotTxnsToSettle: expected a loud timeout refusal; got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, fmt.Sprint(txidA)) {
		t.Errorf("refusal does not name the stuck txid %d: %v", txidA, err)
	}
	if !strings.Contains(msg, "pg_stat_activity") {
		t.Errorf("refusal missing the operator action (pg_stat_activity hint): %v", err)
	}
	if !strings.Contains(msg, "still in flight") {
		t.Errorf("refusal missing the still-in-flight diagnosis: %v", err)
	}

	// After the writer settles, the same wait succeeds — the refusal is
	// budget-bounded, not sticky.
	if err := txA.Commit(); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	if err := waitForPreSnapshotTxnsToSettle(ctx, db, upperBound, 10*time.Second); err != nil {
		t.Errorf("waitForPreSnapshotTxnsToSettle after commit: %v", err)
	}

	// And the clamp sees the formerly-invisible row: A's change-log row
	// is not visible in the captured horizon, so MIN lands on its id.
	minID, found, err := minChangeLogIDForInvisibleTxns(ctx, db, "public", snapText, upperBound)
	if err != nil {
		t.Fatalf("minChangeLogIDForInvisibleTxns: %v", err)
	}
	if !found {
		t.Fatal("minChangeLogIDForInvisibleTxns: found=false; want tx-A's change-log row visible post-settle")
	}
	if minID <= 0 {
		t.Errorf("minChangeLogIDForInvisibleTxns = %d; want a positive change-log id", minID)
	}
}

// TestSettleWait_QuiescentSourceFirstPollExits pins the no-overlap fast
// path on a REAL quiescent source: nothing in flight at snapshot time →
// the wait exits on its FIRST poll, without sleeping a poll interval
// (the interval is 1 s; the bound below fails if any sleep sneaks in),
// and the clamp finds nothing to clamp.
func TestSettleWait_QuiescentSourceFirstPollExits(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	upperBound, err := captureTxidUpperBound(ctx, db)
	if err != nil {
		t.Fatalf("captureTxidUpperBound: %v", err)
	}
	start := time.Now()
	if err := waitForPreSnapshotTxnsToSettle(ctx, db, upperBound, anchorSettleTimeout); err != nil {
		t.Fatalf("waitForPreSnapshotTxnsToSettle on quiescent source: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("quiescent settle wait took %s; want first-poll exit (no %s interval sleep)", elapsed, anchorSettlePollInterval)
	}
}
