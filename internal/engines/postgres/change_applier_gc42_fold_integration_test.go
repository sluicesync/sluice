//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// TestGC42_RefusalInsideAFold pins that GC-42's multi-row refusal, raised by
// the FIRST marked change of a transaction under --exactly-once-lanes — the
// lane batch that carries ADR-0190 amendment D's fold (the transaction's
// anchor position, its rows_applied increment and the closed transactions'
// mark deletions) — rolls ALL of the fold back, on both Postgres lane write
// cores: the refusal must not leave the position advanced past a change that
// never applied, a rows_applied that counts it, a mark vouching for it, or
// the closed transaction's mark deleted. The independent evidence is the
// target's own tables on a separate connection.
//
// The multi-row match is a target a pre-fix sluice already corrupted: two
// rows share a DEFERRABLE primary key (seeded under replica mode), and a
// DELETE by that key — routed to a lane, since it does not move the key —
// matches both.
func TestGC42_RefusalInsideAFold(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	for _, core := range laneFoldCores {
		t.Run(core.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			f := newFoldFixture(ctx, t, dsn, `CREATE TABLE fold_items (id BIGINT, code TEXT NOT NULL UNIQUE,
				CONSTRAINT fold_items_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED)`)
			if _, err := f.db.ExecContext(ctx, `BEGIN; SET LOCAL session_replication_role = replica;
				INSERT INTO fold_items VALUES (7, 'a'), (7, 'b'); COMMIT;`); err != nil {
				t.Fatalf("seed the corrupted target: %v", err)
			}
			batch := []ir.Change{ir.Delete{
				Schema: "public", Table: "fold_items", Before: ir.Row{"id": int64(7)},
				ApplyID: ir.ApplyID{TxID: "fold-tx", Seq: 1},
			}}
			ticket := &laneapply.FoldTicket{Tx: "fold-tx", Pos: f.pos(1), RowsApplied: 1, ClosedTxs: []string{"closed-tx"}}
			f.la.fence.Open("fold-tx", false)

			err := core.apply(ctx, f.la, batch, ticket)
			if !errors.Is(err, appliershared.ErrKeyScopedWriteMatchedMultipleRows) {
				t.Fatalf("want the GC-42 multi-row refusal from the fold batch; got %v", err)
			}
			pos, rows, marks, data := f.state(ctx, t)
			if pos != f.pos(0).Token || rows != 0 || marks["fold-tx"] != 0 || marks["closed-tx"] != 1 || data != 2 {
				t.Fatalf("the refused fold left something durable: position %s (want %s), rows_applied %d (want 0), "+
					"marks %v (want no fold-tx mark, closed-tx kept), %d data rows (want both of the 2)",
					pos, f.pos(0).Token, rows, marks, data)
			}
			if f.la.fence.Admits([]applymarks.Mark{{TxID: "fold-tx"}}, "") {
				t.Fatal("a refused fold anchored its transaction")
			}
		})
	}
}
