// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRunBatchLoop_KeyScopedRefusalNamesTheSplit pins Bug 294 on the shared
// batch loop: a GC-42 refusal says its source transaction WAS split exactly
// when an earlier batch committed part of that transaction, and keeps its own
// "may" otherwise. The refusal is raised from both places it reaches the loop
// — a serial dispatch, and the commit a pipelined engine grades it at — and
// the loop's Classify flattens the error to a string first, so a note that
// only flipped a field on the refusal would never reach the message.
//
// The expected value is the batch boundaries the cell's batch size and
// transaction markers force, not anything the loop reports.
func TestRunBatchLoop_KeyScopedRefusalNamesTheSplit(t *testing.T) {
	refusal := func() error {
		return RefuseKeyScopedMultiMatch("fake", "delete", "s", "t", ir.Row{"id": int64(2)}, 2)
	}
	snapshotAt := func(token string) ir.Change {
		return ir.SchemaSnapshot{Position: pos(token), Schema: "s", Table: "t"}
	}
	cases := []struct {
		name             string
		changes          []ir.Change
		batchSize        int
		transactionalDDL bool   // the Postgres shape: a schema event rides its own batch tx
		refuseAt         string // dispatch of this token refuses
		skip             string // dispatch of this token reports an absent target
		atCommit         int    // > 0: the Nth commit refuses instead
		wantSplit        bool
	}{
		// Bug 295: a committed batch that held no row of the transaction is
		// not part of it. The filed repro is the first cell: the Postgres
		// reader's lazily emitted SchemaSnapshot, flushed alone at the
		// table's first row, ahead of the statement that is refused.
		// Pin the class: both schema-event shapes (TransactionalDDL true and
		// false), a skipped row, and a Truncate — each against a sibling
		// whose batch did carry a row, which must still name the split.
		{
			name:             "schema snapshot flushed alone, then the transaction's first row refused (Bug 295)",
			changes:          []ir.Change{txBegin("tb"), snapshotAt("s1"), insertAt("p1")},
			batchSize:        100,
			transactionalDDL: true, refuseAt: "p1", wantSplit: false,
		},
		{
			name:             "schema snapshot flushed with an earlier row of the transaction",
			changes:          []ir.Change{txBegin("tb"), insertAt("p1"), snapshotAt("s1"), insertAt("p2")},
			batchSize:        100,
			transactionalDDL: true, refuseAt: "p2", wantSplit: true,
		},
		{
			name:      "schema snapshot applied alone without transactional DDL, then refused",
			changes:   []ir.Change{txBegin("tb"), snapshotAt("s1"), insertAt("p1")},
			batchSize: 100, refuseAt: "p1", wantSplit: false,
		},
		{
			name:      "rows flushed ahead of a schema snapshot without transactional DDL",
			changes:   []ir.Change{txBegin("tb"), insertAt("p1"), snapshotAt("s1"), insertAt("p2")},
			batchSize: 100, refuseAt: "p2", wantSplit: true,
		},
		{
			name:      "the only committed row was skipped for an absent target",
			changes:   []ir.Change{txBegin("tb"), insertAt("p1"), insertAt("p2")},
			batchSize: 1, skip: "p1", refuseAt: "p2", wantSplit: false,
		},
		{
			// A committed TRUNCATE leaves the target in a state the source
			// never had, as the lane and per-change paths also say.
			name:             "a Truncate committed inside the transaction is a split",
			changes:          []ir.Change{txBegin("tb"), truncateAt("x1"), insertAt("p1")},
			batchSize:        100,
			transactionalDDL: true, refuseAt: "p1", wantSplit: true,
		},
		{
			name:      "a Truncate applied alone without transactional DDL is a split",
			changes:   []ir.Change{txBegin("tb"), truncateAt("x1"), insertAt("p1")},
			batchSize: 100, refuseAt: "p1", wantSplit: true,
		},
		{
			name:             "a Truncate skipped for an absent target is not",
			changes:          []ir.Change{txBegin("tb"), truncateAt("x1"), insertAt("p1")},
			batchSize:        100,
			transactionalDDL: true, skip: "x1", refuseAt: "p1", wantSplit: false,
		},
		{
			name:      "row cap split, refused at dispatch",
			changes:   []ir.Change{txBegin("tb"), insertAt("p1"), insertAt("p2")},
			batchSize: 1, refuseAt: "p2", wantSplit: true,
		},
		{
			name:      "row cap split, refused at the TxCommit flush's commit",
			changes:   []ir.Change{txBegin("tb"), insertAt("p1"), insertAt("p2"), insertAt("p3"), txCommit("tc")},
			batchSize: 2, atCommit: 2, wantSplit: true,
		},
		{
			name:      "whole transaction in one batch",
			changes:   []ir.Change{txBegin("tb"), insertAt("p1"), insertAt("p2")},
			batchSize: 100, refuseAt: "p2", wantSplit: false,
		},
		{
			name: "the previous transaction was split, this one is not",
			changes: []ir.Change{
				txBegin("ta"), insertAt("p1"), insertAt("p2"), txCommit("tc"),
				txBegin("tb"), insertAt("q1"),
			},
			batchSize: 1, refuseAt: "q1", wantSplit: false,
		},
		{
			name:      "marker-less stream: every change is its own transaction",
			changes:   []ir.Change{insertAt("p1"), insertAt("p2")},
			batchSize: 1, refuseAt: "p2", wantSplit: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			cfg := testConfig(t, rec, tc.transactionalDDL)
			cfg.CacheSchemaSnapshot = func(ir.SchemaSnapshot) {}
			cfg.Dispatch = func(_ context.Context, _ BatchTx, _ string, c ir.Change) (bool, error) {
				if tc.refuseAt != "" && c.Pos().Token == tc.refuseAt {
					return false, refusal()
				}
				return c.Pos().Token == tc.skip, nil
			}
			if tc.atCommit > 0 {
				commits := 0
				cfg.Commit = func(tx BatchTx) error {
					commits++
					if commits == tc.atCommit {
						_ = tx.Rollback()
						return refusal()
					}
					return tx.(*sql.Tx).Commit()
				}
			}
			err := RunBatchLoop(context.Background(), cfg, "stream", feed(true, tc.changes...), tc.batchSize)
			if !errors.Is(err, ErrKeyScopedWriteMatchedMultipleRows) || !ir.IsTerminal(err) {
				t.Fatalf("want the terminal GC-42 refusal; got %v", err)
			}
			msg := err.Error()
			if !strings.Contains(msg, keyScopedMultiMatchRolledBack) {
				t.Errorf("the refusal lost its rollback account: %s", msg)
			}
			if got := strings.Contains(msg, keyScopedMultiMatchSplitNote); got != tc.wantSplit {
				t.Errorf("names the split = %v, want %v: %s", got, tc.wantSplit, msg)
			}
		})
	}
}
