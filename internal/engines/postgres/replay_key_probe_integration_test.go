//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// replayOutcome is what re-applying one INSERT through the real applier
// does to a table: the ground truth the probe's verdict is graded against.
type replayOutcome string

const (
	replayConverges  replayOutcome = "converges"  // still one row: the upsert collided
	replayDuplicates replayOutcome = "duplicates" // two rows, at no error: the F-E1 silent class
	replayLoud       replayOutcome = "loud"       // the second apply errors
)

// TestRowWriter_ProbeReplayKey_ShapeMatrix pins the Postgres half of the
// F-E1 target judgment against a real server, over every key shape the
// applier's arbiter selection distinguishes — not one representative.
//
// The independent expected value per shape is the APPLIER'S OWN behaviour,
// measured: one INSERT is applied twice through the real change applier and
// the outcome recorded. keyed=true must coincide with "converges"; every
// silent "duplicates" shape must be keyed=false. A keyed=false shape whose
// re-apply is merely LOUD (a non-arbiter unique index that still collides)
// is refused by the door too, which is the conservative direction: the
// applier would have failed on it anyway. Rows are chosen to reach the
// silent outcome wherever one exists (a NULL in a nullable key part, a value
// outside a partial index's predicate).
func TestRowWriter_ProbeReplayKey_ShapeMatrix(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cases := []struct {
		table, ddl  string
		row         ir.Row
		wantExists  bool
		wantKeyed   bool
		wantRefusal bool // the applier's own deferrable-key refusal
		outcome     replayOutcome
	}{
		{table: "absent"},
		{"no_key", "CREATE TABLE no_key (id int NOT NULL, v text)", ir.Row{"id": int64(1)}, true, false, false, replayDuplicates},
		{"pk", "CREATE TABLE pk (id int PRIMARY KEY, v text)", ir.Row{"id": int64(1)}, true, true, false, replayConverges},
		{"nn_unique", "CREATE TABLE nn_unique (id int NOT NULL UNIQUE, v text)", ir.Row{"id": int64(1)}, true, true, false, replayConverges},
		{"nn_unique_composite", "CREATE TABLE nn_unique_composite (a int NOT NULL, b int NOT NULL, v text, UNIQUE (a, b))", ir.Row{"a": int64(1), "b": int64(1)}, true, true, false, replayConverges},
		{"nullable_unique", "CREATE TABLE nullable_unique (id int UNIQUE, v text)", ir.Row{"id": nil}, true, false, false, replayDuplicates},
		{"composite_one_nullable", "CREATE TABLE composite_one_nullable (a int NOT NULL, b int, v text, UNIQUE (a, b))", ir.Row{"a": int64(1), "b": nil}, true, false, false, replayDuplicates},
		{"partial_unique", "CREATE TABLE partial_unique (id int NOT NULL, v text); CREATE UNIQUE INDEX partial_unique_ix ON partial_unique (id) WHERE id > 0", ir.Row{"id": int64(-1)}, true, false, false, replayDuplicates},
		{"expression_unique", "CREATE TABLE expression_unique (id int NOT NULL, v text); CREATE UNIQUE INDEX expression_unique_ix ON expression_unique ((id * 2))", ir.Row{"id": int64(1)}, true, false, false, replayLoud},
		{table: "deferrable_pk_only", ddl: "CREATE TABLE deferrable_pk_only (id int PRIMARY KEY DEFERRABLE, v text)", wantExists: true, wantRefusal: true},
		{"deferrable_pk_plus_nn_unique", "CREATE TABLE deferrable_pk_plus_nn_unique (id int PRIMARY KEY DEFERRABLE, u int NOT NULL UNIQUE, v text)", ir.Row{"id": int64(1), "u": int64(1)}, true, true, false, replayConverges},
	}
	for _, c := range cases {
		if c.ddl != "" {
			applyPGApplier(t, dsn, c.ddl)
		}
	}

	rw, err := (Engine{}).OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer func() {
		if c, ok := rw.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	prober, ok := rw.(ir.ReplayKeyProber)
	if !ok {
		t.Fatal("the Postgres row writer does not implement ir.ReplayKeyProber — the F-E1 target judgment is inert")
	}

	sawKeyed, sawSilent := false, false
	for _, c := range cases {
		t.Run(c.table, func(t *testing.T) {
			exists, keyed, err := prober.ProbeReplayKey(ctx, &ir.Table{Name: c.table})
			if c.wantRefusal {
				if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeTargetDeferrableKey {
					t.Fatalf("ProbeReplayKey = (%v, %v, %v); want the applier's %s refusal",
						exists, keyed, err, sluicecode.CodeTargetDeferrableKey)
				}
				return
			}
			if err != nil {
				t.Fatalf("ProbeReplayKey: %v", err)
			}
			if exists != c.wantExists || keyed != c.wantKeyed {
				t.Fatalf("ProbeReplayKey = (exists=%v, keyed=%v); want (%v, %v)", exists, keyed, c.wantExists, c.wantKeyed)
			}
			if !exists {
				return
			}

			row := ir.Row{"v": "x"}
			for k, v := range c.row {
				row[k] = v
			}
			ins := ir.Insert{Schema: "public", Table: c.table, Row: row}
			if err := applyDeferrableEvents(t, dsn, []ir.Change{ins}); err != nil {
				t.Fatalf("first apply: %v", err)
			}
			got := replayConverges
			if err := applyDeferrableEvents(t, dsn, []ir.Change{ins}); err != nil {
				got = replayLoud
			} else if n := pgScalarInt(t, dsn, "SELECT count(*) FROM public."+c.table); n == 2 {
				got = replayDuplicates
			}
			if got != c.outcome {
				t.Fatalf("re-applying one INSERT %s; the case declares %s — the ground truth moved", got, c.outcome)
			}
			if keyed != (got == replayConverges) {
				t.Errorf("probe says keyed=%v but a re-apply %s", keyed, got)
			}
			sawKeyed = sawKeyed || keyed
			sawSilent = sawSilent || got == replayDuplicates
		})
	}
	if !sawKeyed || !sawSilent {
		t.Fatalf("anti-vacuity: the matrix must reach a keyed table and a silently duplicating one (keyed=%v, silent=%v)", sawKeyed, sawSilent)
	}
}
