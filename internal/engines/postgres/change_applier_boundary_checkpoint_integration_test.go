//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 132, Postgres half: the applier now sets
// CheckpointOnlyAtTxBoundary=true, which makes two shared-loop paths reachable
// on this engine for the first time — commitBatch's position WITHHOLDING and
// writeBoundaryOnly's dedicated position-only transaction.
//
// The shared loop already has unit pins for both, but they drive a test-local
// BatchConfig whose closures satisfy the seam BY CONSTRUCTION. That proves the
// loop calls what it declared and says nothing about whether PG's real
// closures — the ADR-0092 pipelined *pgxBatchTx, whose WritePosition QUEUES
// the upsert onto a batch that only flushes at Commit — behave correctly when
// a batch commits with no position write, or when a position writes with no
// data. This test runs the real applier against a real Postgres.

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_ApplyBatch_WithholdsMidTransactionPosition pins the
// item-132 contract on the Postgres applier:
//
//   - a batch that flushes MID source transaction commits its DATA but does
//     NOT advance the persisted position;
//   - the trailing TxCommit — which arrives on an EMPTY batch, because the
//     transaction's rows already went out in earlier batches — advances the
//     position to the boundary via writeBoundaryOnly.
//
// The independent expected value is the source token stream itself: the
// persisted position must be the transaction's COMMIT token, never any of the
// row tokens inside it. Both halves are asserted, because a fix that never
// advanced the position at all would satisfy the first alone — that is exactly
// the audit-2026-08-01 S3 regression (a marker-less source pointed at a
// flagged applier never checkpointed for the life of the stream).
func TestChangeApplier_ApplyBatch_WithholdsMidTransactionPosition(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// ONE source transaction of six rows, applied with maxBatchSize=2: the
	// loop flushes three times, twice of them strictly inside the source
	// transaction, and the closing TxCommit lands on an empty batch.
	const commitToken = "tx1-commit"
	events := []ir.Change{
		ir.TxBegin{Position: ir.Position{Token: "tx1-begin"}},
	}
	for i := int64(1); i <= 6; i++ {
		events = append(events, ir.Insert{
			Position: ir.Position{Token: tokenForInt(i)},
			Schema:   "public",
			Table:    "users",
			Row:      ir.Row{"id": i, "email": emailForInt(i)},
		})
	}
	events = append(events, ir.TxCommit{Position: ir.Position{Token: commitToken}})

	pumpBatchedChanges(t, ctx, applier, events, 2)

	if got := countAllRows(t, dsn, "users"); got != 6 {
		t.Fatalf("rows = %d; want 6 — the DATA must still commit on every flush; "+
			"withholding is only about the POSITION write", got)
	}

	pos, found, err := applier.ReadPosition(ctx, testStreamID)
	if err != nil {
		t.Fatalf("ReadPosition: %v", err)
	}
	if !found {
		t.Fatal("ReadPosition: no row found — the trailing TxCommit arrived on an EMPTY batch, so " +
			"writeBoundaryOnly is the only thing that can advance the position; the stream would " +
			"never checkpoint (the audit-2026-08-01 S3 shape)")
	}
	if pos.Token != commitToken {
		// Naming which mid-transaction row it stopped at makes the failure
		// legible: any row token means the position landed inside the source
		// transaction, which on a MySQL GTID source resumes PAST that
		// transaction and silently loses its tail (item 132).
		t.Errorf("persisted position token = %q; want %q (the source transaction's COMMIT). A row "+
			"token here means the applier checkpointed MID-SOURCE-TRANSACTION", pos.Token, commitToken)
	}
}
