//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0007's whole guarantee is that a change and the position that
// records it move together, so a crash between them is impossible. Until
// 2026-08-07 the only pin on that was TestPipelined_AtomicityMidBatchError,
// which proves the ROLLBACK direction on ONE of the three write cores: both
// disappear when a batch fails. Absence-on-failure is weaker than
// same-transaction — two sequential transactions that both fail also leave
// nothing behind — and the serial per-change core, which is what every
// --apply-concurrency 1 stream and every degrade-to-serial path uses, had
// no pin at all.
//
// This file asserts the positive direction, from the server rather than
// from sluice: PostgreSQL stamps every tuple with the xid that wrote it, and
// two transactions cannot share an xid, so xmin(data row) == xmin(control
// row) IS same-transaction. That is the independent expected value the
// 2026-08-01 rule asks for — nothing sluice recorded, no re-reading of the
// artifact under test.

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// tupleXmin returns the transaction id that produced the live tuple
// matching where, as text. Text rather than a numeric type because xid is
// not a Go-scannable integer and equality is all this test needs; a
// wraparound between two statements of one test is not a thing.
func tupleXmin(t *testing.T, dsn, query string, args ...any) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var xmin string
	if err := db.QueryRow(query, args...).Scan(&xmin); err != nil {
		t.Fatalf("read xmin (%s): %v", query, err)
	}
	if xmin == "" || xmin == "0" {
		t.Fatalf("read xmin (%s): got %q — a frozen or absent tuple cannot witness anything", query, xmin)
	}
	return xmin
}

// atomicityCase is one position-write core.
type atomicityCase struct {
	name string
	// prepare returns an applier already configured for this core, plus the
	// batch size to drive it with (<=1 routes to the serial per-change path).
	prepare   func(t *testing.T, ctx context.Context, dsn string) (*ChangeApplier, int)
	wantSameT bool
	// why documents a wantSameT=false cell — an architectural exemption,
	// never a defect left unstated.
	why string
}

// TestPositionAndDataLandInOneTransaction is the ADR-0007 positive pin, and
// it grades every Postgres position-write core rather than the fast one.
//
// THE ROSTER, and what each cell means:
//
//   - serial per-change (`Apply`, and `ApplyBatch` with maxBatchSize<=1) —
//     writePositionTx on the same *sql.Tx as the dispatch. SAME TX.
//   - batched pipelined (ADR-0092) — the position upsert is queued onto the
//     same pgx batch as the data and flushed in one SendBatch. SAME TX.
//   - batched serial fallback — the path taken when the raw-conn escape is
//     unavailable and beginPipelinedTx returns errPipelineUnavailable. It is
//     reached here by clearing pipelineCfg, which is exactly what the
//     fallback branch keys on. SAME TX.
//   - concurrent lanes (ADR-0104/0105) — DELIBERATELY NOT same-tx, and the
//     cell asserts the difference rather than omitting it. Each lane commits
//     its data alone; the resume position is owned by the frontier
//     checkpoint, which persists a MERGED position in its own transaction
//     and only up to a fully-durable source-tx boundary. Writing it per-lane
//     would regress the position to a metadata anchor (Bug 158). An
//     exemption asserted is an exemption that cannot silently become a
//     defect.
func TestPositionAndDataLandInOneTransaction(t *testing.T) {
	cases := []atomicityCase{
		{
			name: "serial per-change",
			prepare: func(t *testing.T, ctx context.Context, dsn string) (*ChangeApplier, int) {
				return openPipelinedApplier(t, ctx, dsn), 1
			},
			wantSameT: true,
		},
		{
			name: "batched pipelined (ADR-0092)",
			prepare: func(t *testing.T, ctx context.Context, dsn string) (*ChangeApplier, int) {
				return openPipelinedApplier(t, ctx, dsn), 100
			},
			wantSameT: true,
		},
		{
			name: "batched serial fallback (pipeline unavailable)",
			prepare: func(t *testing.T, ctx context.Context, dsn string) (*ChangeApplier, int) {
				a := openPipelinedApplier(t, ctx, dsn)
				// The branch under test: beginPipelinedTx returns
				// errPipelineUnavailable on a nil cfg and the batch loop
				// falls back to a plain *sql.Tx.
				a.pipelineCfg = nil
				return a, 100
			},
			wantSameT: true,
		},
		{
			name: "concurrent lanes (ADR-0104/0105)",
			prepare: func(t *testing.T, ctx context.Context, dsn string) (*ChangeApplier, int) {
				a := openPipelinedApplier(t, ctx, dsn)
				a.SetApplyConcurrency(2)
				return a, 100
			},
			wantSameT: false,
			why: "the lane commits data alone; the resume position is persisted by the frontier checkpoint in its own " +
				"transaction, only up to a fully-durable source-tx boundary (the position relaxation, Bug 158)",
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := startPostgresForApplier(t)
			defer cleanup()
			applyPGApplier(t, dsn, `CREATE TABLE atom (id BIGINT PRIMARY KEY, n INT NOT NULL);`)

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			applier, batchSize := tc.prepare(t, ctx, dsn)
			defer func() { _ = applier.Close() }()
			if err := applier.EnsureControlTable(ctx); err != nil {
				t.Fatalf("EnsureControlTable: %v", err)
			}

			id := int64(i + 1)
			tok := fmt.Sprintf(`{"slot":"sluice_slot","lsn":"0/%d00"}`, i+2)
			events := []ir.Change{
				ir.TxBegin{Position: ir.Position{Engine: engineNamePostgres, Token: tok}},
				ir.Insert{
					Position: ir.Position{Engine: engineNamePostgres, Token: tok},
					Schema:   "public", Table: "atom",
					Row: ir.Row{"id": id, "n": int64(7)},
				},
				ir.TxCommit{Position: ir.Position{Engine: engineNamePostgres, Token: tok}},
			}
			ch := make(chan ir.Change, len(events))
			for _, e := range events {
				ch <- e
			}
			close(ch)
			if err := applier.ApplyBatch(ctx, testStreamID, ch, batchSize); err != nil {
				t.Fatalf("ApplyBatch(batchSize=%d): %v", batchSize, err)
			}

			// Anti-vacuity: both rows must exist, or "the xids match" is a
			// statement about nothing. tupleXmin fails loudly on absence.
			dataXmin := tupleXmin(t, dsn, `SELECT xmin::text FROM public.atom WHERE id = $1`, id)
			posXmin := tupleXmin(t, dsn,
				`SELECT xmin::text FROM public.sluice_cdc_state WHERE stream_id = $1`, testStreamID)

			same := dataXmin == posXmin
			switch {
			case tc.wantSameT && !same:
				t.Errorf("ADR-0007 violated on the %s core: the row was written by xid %s and its position by xid %s — "+
					"two transactions, so a crash between them leaves the target holding data the recorded position does not cover "+
					"(or a position covering data that rolled back)", tc.name, dataXmin, posXmin)
			case !tc.wantSameT && same:
				t.Errorf("the %s core wrote its position in the SAME transaction as the data (xid %s). That is an "+
					"architectural change, not an improvement: %s. Update this cell deliberately or revert.",
					tc.name, dataXmin, tc.why)
			default:
				t.Logf("PROVEN %s: data xid=%s position xid=%s (same=%v, expected)", tc.name, dataXmin, posXmin, same)
			}
		})
	}
}
