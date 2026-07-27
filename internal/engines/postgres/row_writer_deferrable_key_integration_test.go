//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 211, the SECOND arbiter site: the idempotent bulk-copy writer.
//
// The CDC applier is where Bug 211 was observed, but it is not the only
// place sluice builds an `INSERT … ON CONFLICT (key) DO UPDATE`. The
// idempotent RowWriter does too, and it is reached by `restore
// --data-only` / chain replay, `schema add-table`'s bulk copy, and the
// VStream COPY catchup absorb. Against a target carrying a DEFERRABLE
// primary key it produced the same raw SQLSTATE 55000, with no code and
// no remedy.
//
// The key here comes from the SOURCE IR while the arbiter has to satisfy
// the TARGET, and those can legitimately disagree: pre-creating the
// target with an immediate primary key is the documented workaround for
// this whole class. So the check reads the live catalog, and the two
// cross cases below — deferrable in the IR / immediate on the target,
// and the reverse — are what prove it.

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// deferrableCopyTable is the IR the writer copies; ConstraintDeferrable
// is set per-case to model what the SOURCE declared.
func deferrableCopyTable(deferrable bool) *ir.Table {
	return &ir.Table{
		Name: "dpk",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Text{}, Nullable: true},
		},
		PrimaryKey: &ir.Index{
			Name:                 "dpk_pk",
			Columns:              []ir.IndexColumn{{Column: "id"}},
			Unique:               true,
			ConstraintBacked:     true,
			ConstraintDeferrable: deferrable,
		},
	}
}

// TestWriteRowsIdempotent_DeferrableTargetKey drives the real idempotent
// writer against a real Postgres for all four (source IR × target
// catalog) deferrability combinations. Only the two with a DEFERRABLE
// TARGET key may refuse — the target catalog is what PG's arbiter
// inference reads.
func TestWriteRowsIdempotent_DeferrableTargetKey(t *testing.T) {
	cases := []struct {
		name         string
		targetDDL    string
		irDeferrable bool
		wantRefusal  bool
	}{
		{
			name:      "immediate everywhere — the ordinary copy, unaffected",
			targetDDL: `CREATE TABLE dpk (id BIGINT PRIMARY KEY, v TEXT);`,
		},
		{
			name:         "deferrable source key carried onto the target — refuse, coded",
			targetDDL:    `CREATE TABLE dpk (id BIGINT, v TEXT, CONSTRAINT dpk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);`,
			irDeferrable: true,
			wantRefusal:  true,
		},
		{
			// The documented workaround for this class. An IR-driven
			// check would refuse this and break the escape hatch.
			name:         "deferrable in the IR, target pre-created immediate — must copy",
			targetDDL:    `CREATE TABLE dpk (id BIGINT PRIMARY KEY, v TEXT);`,
			irDeferrable: true,
		},
		{
			// The reverse: an IR-driven check would MISS this one.
			name:        "immediate in the IR, target actually deferrable — refuse, coded",
			targetDDL:   `CREATE TABLE dpk (id BIGINT, v TEXT, CONSTRAINT dpk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY IMMEDIATE);`,
			wantRefusal: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := startPostgresForApplier(t)
			defer cleanup()
			applyPGApplier(t, dsn, tc.targetDDL)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			rw, err := Engine{}.OpenRowWriter(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenRowWriter: %v", err)
			}
			defer closeIf(rw)
			iw, ok := rw.(ir.IdempotentRowWriter)
			if !ok {
				t.Fatal("postgres RowWriter does not implement ir.IdempotentRowWriter")
			}

			rows := make(chan ir.Row, 2)
			rows <- ir.Row{"id": int64(1), "v": "a"}
			rows <- ir.Row{"id": int64(2), "v": "b"}
			close(rows)

			err = iw.WriteRowsIdempotent(ctx, deferrableCopyTable(tc.irDeferrable), rows)
			if tc.wantRefusal {
				assertDeferrableKeyRefusal(t, err, "dpk")
				return
			}
			if err != nil {
				t.Fatalf("WriteRowsIdempotent: %v", err)
			}
			if got := pgScalarInt(t, dsn, "SELECT COUNT(*) FROM dpk"); got != 2 {
				t.Fatalf("row count = %d; want 2", got)
			}

			// Re-copy: the whole point of the idempotent writer is that a
			// replayed chunk converges rather than duplicating.
			replay := make(chan ir.Row, 2)
			replay <- ir.Row{"id": int64(1), "v": "a2"}
			replay <- ir.Row{"id": int64(2), "v": "b"}
			close(replay)
			if err := iw.WriteRowsIdempotent(ctx, deferrableCopyTable(tc.irDeferrable), replay); err != nil {
				t.Fatalf("WriteRowsIdempotent (replay): %v", err)
			}
			if got := pgScalarInt(t, dsn, "SELECT COUNT(*) FROM dpk"); got != 2 {
				t.Fatalf("row count after replay = %d; want 2 (the upsert did not converge)", got)
			}
			if got := pgScalarInt(t, dsn, "SELECT COUNT(*) FROM dpk WHERE v = 'a2'"); got != 1 {
				t.Fatalf("replay did not update the row: %d rows with v='a2'", got)
			}
		})
	}
}
