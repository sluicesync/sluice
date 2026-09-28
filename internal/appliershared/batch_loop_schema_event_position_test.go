// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRunBatchLoop_SchemaEventInsideSourceTx_NeverPersistsItsPosition pins
// [schemaEventAtBoundary] over the whole matrix the loop dispatches on: both
// DDL transactionality modes (the MySQL target applies a schema event alone
// via ApplyOne; the Postgres target flushes it on the batch tx) × both schema
// events × the event arriving first in a batch or mid-batch × inside or
// outside a source transaction. Inside a transaction the event's own position
// must never be persisted — it is its DDL's position (a MySQL DDL anchor, a
// Postgres RelationMessage's 0/0), which can lie before transactions already
// applied, and persisting it regressed the stream behind them while the
// current transaction's apply marks were durable (the 2026-09-28 CRITICAL).
// The transaction's TxCommit persists the next position instead. Outside a
// transaction the event is a statement boundary and still persists.
func TestRunBatchLoop_SchemaEventInsideSourceTx_NeverPersistsItsPosition(t *testing.T) {
	for _, transactionalDDL := range []bool{false, true} {
		for evName, ev := range schemaEvents("pX") {
			for _, midBatch := range []bool{false, true} {
				for _, inTx := range []bool{false, true} {
					name := fmt.Sprintf("transactionalDDL=%v/%s/midBatch=%v/inSourceTx=%v", transactionalDDL, evName, midBatch, inTx)
					t.Run(name, func(t *testing.T) {
						rec := &recorder{}
						cfg := testConfig(t, rec, transactionalDDL)
						cfg.CheckpointOnlyAtTxBoundary = true
						cfg.CacheSchemaSnapshot = func(ir.SchemaSnapshot) {}
						var changes []ir.Change
						if inTx {
							changes = append(changes, txBegin("tb"))
						}
						if midBatch {
							changes = append(changes, insertAt("p1"))
						}
						changes = append(changes, ev)
						if inTx {
							changes = append(changes, txCommit("tc"))
						}
						if err := RunBatchLoop(context.Background(), cfg, "stream", feed(true, changes...), 100); err != nil {
							t.Fatalf("RunBatchLoop: %v", err)
						}
						got := rec.list()
						persisted := slices.Contains(got, "writePosition:pX") || slices.Contains(got, "applyOne:pX")
						if persisted == inTx {
							t.Fatalf("the schema event's own position persisted = %v; want %v (inside a source transaction it is never a boundary)\nevents: %v",
								persisted, !inTx, got)
						}
						if inTx && !slices.Contains(got, "writePosition:tc") {
							t.Fatalf("the transaction's commit position was not persisted after the schema event\nevents: %v", got)
						}
					})
				}
			}
		}
	}
}
