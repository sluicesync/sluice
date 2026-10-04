//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRowWriter_ProbeReplayKey_ShapeMatrix pins the MySQL half of the F-E1
// target judgment against a real server, over every key shape that matters —
// not one representative.
//
// The independent expected value per shape is what the applier actually does,
// measured: one INSERT is applied twice and the outcome recorded. keyed=true
// must coincide with a converged re-apply, and every silently duplicating
// shape must be keyed=false. Rows carry a NULL in every nullable key part,
// which is exactly when ON DUPLICATE KEY UPDATE does not collide — so the
// nullable_unique row is "keyed" to the applier's batching probe
// (tableIsKeyless counts any UNIQUE index) and still duplicates, which is
// why the door's probe is the strict one.
//
// One shape is refused CONSERVATIVELY and says so: a functional UNIQUE key
// part. ON DUPLICATE KEY UPDATE collides on it, so the re-apply converges,
// but the engine's upsert-key picker (and the recorded-schema predicate,
// irbackup.TableReplayIdempotent) never treat an expression as a key, and the
// door follows the picker rather than widening it for one rare shape.
func TestRowWriter_ProbeReplayKey_ShapeMatrix(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		converges  = "converges"
		duplicates = "duplicates"
		loud       = "loud"
	)
	cases := []struct {
		table, ddl   string
		row          ir.Row
		wantExists   bool
		wantKeyed    bool
		outcome      string
		conservative bool // keyed=false although the re-apply converges
	}{
		{table: "absent"},
		{"no_key", "CREATE TABLE no_key (id INT NOT NULL, v TEXT)", ir.Row{"id": int64(1)}, true, false, duplicates, false},
		{"pk", "CREATE TABLE pk (id INT PRIMARY KEY, v TEXT)", ir.Row{"id": int64(1)}, true, true, converges, false},
		{"nn_unique", "CREATE TABLE nn_unique (id INT NOT NULL UNIQUE, v TEXT)", ir.Row{"id": int64(1)}, true, true, converges, false},
		{"nn_unique_composite", "CREATE TABLE nn_unique_composite (a INT NOT NULL, b INT NOT NULL, v TEXT, UNIQUE KEY (a, b))", ir.Row{"a": int64(1), "b": int64(1)}, true, true, converges, false},
		{"nullable_unique", "CREATE TABLE nullable_unique (id INT NULL UNIQUE, v TEXT)", ir.Row{"id": nil}, true, false, duplicates, false},
		{"composite_one_nullable", "CREATE TABLE composite_one_nullable (a INT NOT NULL, b INT NULL, v TEXT, UNIQUE KEY (a, b))", ir.Row{"a": int64(1), "b": nil}, true, false, duplicates, false},
		{"functional_unique", "CREATE TABLE functional_unique (id INT NOT NULL, v TEXT, UNIQUE KEY fu ((id * 2)))", ir.Row{"id": int64(1)}, true, false, converges, true},
	}
	for _, c := range cases {
		if c.ddl != "" {
			applyMySQLApplier(t, dsn, c.ddl)
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
		t.Fatal("the MySQL row writer does not implement ir.ReplayKeyProber — the F-E1 target judgment is inert")
	}
	applier, err := (Engine{}).OpenChangeApplier(ctx, dsn)
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
	apply := func(c ir.Change) error {
		ch := make(chan ir.Change, 1)
		ch <- c
		close(ch)
		return applier.Apply(ctx, testStreamID, ch)
	}

	sawKeyed, sawSilent := false, false
	for _, c := range cases {
		t.Run(c.table, func(t *testing.T) {
			exists, keyed, err := prober.ProbeReplayKey(ctx, &ir.Table{Name: c.table})
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
			ins := ir.Insert{Table: c.table, Row: row}
			if err := apply(ins); err != nil {
				t.Fatalf("first apply: %v", err)
			}
			got := converges
			if err := apply(ins); err != nil {
				got = loud
			} else if countAllRows(t, dsn, "", c.table) == 2 {
				got = duplicates
			}
			if got != c.outcome {
				t.Fatalf("re-applying one INSERT %s; the case declares %s — the ground truth moved", got, c.outcome)
			}
			if !c.conservative && keyed != (got == converges) {
				t.Errorf("probe says keyed=%v but a re-apply %s", keyed, got)
			}
			if got == duplicates && keyed {
				t.Errorf("a silently duplicating table was judged keyed")
			}
			sawKeyed = sawKeyed || keyed
			sawSilent = sawSilent || got == duplicates
		})
	}
	if !sawKeyed || !sawSilent {
		t.Fatalf("anti-vacuity: the matrix must reach a keyed table and a silently duplicating one (keyed=%v, silent=%v)", sawKeyed, sawSilent)
	}
}
