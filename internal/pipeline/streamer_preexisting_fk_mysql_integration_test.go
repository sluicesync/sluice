//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The roadmap-item-140 pre-copy foreign-key gate on the SYNC cold-start
// leg, on real MySQL.
//
// This is the leg the field report actually ran: the reporter branched
// their target from an existing database and started a `sync`, and the
// cold copy died ~20 seconds in on Error 1452. The check rides
// [existingTablesGate], which the sync cold start already calls, so
// there is no second implementation — but "the shared helper covers
// both" is exactly the claim this project keeps paying for when nobody
// asserts it, so the sync leg gets its own end-to-end pin.
//
// Both directions are here: refused before a row moves when the target
// carries the constraint, and untouched (cold copy + live CDC converge)
// when it does not.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/sluicecode"
)

const preexistingFKSyncDDL = `
	CREATE TABLE fks_users (
		id    BIGINT       NOT NULL,
		email VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

	CREATE TABLE fks_orders (
		id      BIGINT NOT NULL,
		user_id BIGINT NOT NULL,
		PRIMARY KEY (id),
		KEY fks_orders_user_idx (user_id),
		CONSTRAINT fks_orders_user_fk FOREIGN KEY (user_id) REFERENCES fks_users (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

const preexistingFKSyncSeed = `
	INSERT INTO fks_users (id, email) VALUES (1, 'alice@example.com'), (2, 'bob@example.com');
	INSERT INTO fks_orders (id, user_id) VALUES (10, 1), (11, 2), (12, 1);
`

func TestStreamer_PreExistingTargetForeignKeys_RefusesColdStart(t *testing.T) {
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyMySQLDDL(t, src, preexistingFKSyncDDL+preexistingFKSyncSeed)
	// The branched-target shape: empty tables that already carry the
	// constraint. Empty, so the Bug-9 populated-target preflight passes
	// and this gate is the only thing standing between the operator and
	// the 20-second Error 1452.
	applyMySQLDDL(t, tgt, preexistingFKSyncDDL)

	streamer := &Streamer{
		Source:    myEng,
		Target:    myEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  "fk-gate-refuse",
	}
	err := streamer.Run(context.Background())
	if err == nil {
		t.Fatal("expected the coded pre-existing-foreign-key refusal; Run returned nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeTargetPreexistingForeignKey {
		t.Fatalf("expected coded %s; got %v (err=%v)", sluicecode.CodeTargetPreexistingForeignKey, ce, err)
	}
	for _, want := range []string{"fks_orders", "fks_orders_user_fk", "fks_users"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err.Error(), want)
		}
	}
	if !strings.Contains(ce.Hint, "--skip-foreign-keys") {
		t.Errorf("hint %q does not name --skip-foreign-keys", ce.Hint)
	}
	// Refused BEFORE any row moved.
	if got := pollRowCountMySQL(tgt, "fks_users"); got != 0 {
		t.Fatalf("target fks_users rows after refusal = %d; want 0 (refusal must precede the copy)", got)
	}
	if got := pollRowCountMySQL(tgt, "fks_orders"); got != 0 {
		t.Fatalf("target fks_orders rows after refusal = %d; want 0", got)
	}
}

func TestStreamer_PreExistingTargetForeignKeys_CleanTargetSyncsNormally(t *testing.T) {
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyMySQLDDL(t, src, preexistingFKSyncDDL+preexistingFKSyncSeed)
	// No target DDL at all: the ordinary cold start, which must be
	// completely untouched by the new gate.

	streamer := &Streamer{
		Source:    myEng,
		Target:    myEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  "fk-gate-clean",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- streamer.Run(ctx) }()

	if !waitForExactRowCountMySQL(tgt, "fks_orders", 3, 2*time.Minute) {
		select {
		case err := <-errCh:
			t.Fatalf("clean-target sync exited before copying: %v", err)
		default:
			t.Fatalf("cold copy never delivered 3 rows (got %d)", pollRowCountMySQL(tgt, "fks_orders"))
		}
	}
	// The CDC leg still runs — the gate has not disturbed the handoff.
	applyMySQLDDL(t, src, `INSERT INTO fks_users (id, email) VALUES (3, 'carol@example.com');
		INSERT INTO fks_orders (id, user_id) VALUES (13, 3);`)
	if !waitForExactRowCountMySQL(tgt, "fks_orders", 4, 2*time.Minute) {
		t.Fatalf("CDC never applied the live insert (got %d rows)", pollRowCountMySQL(tgt, "fks_orders"))
	}
	cancel()
	select {
	case <-errCh:
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
