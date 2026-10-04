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
// Two shapes are refused CONSERVATIVELY and say so: a functional UNIQUE key
// part, and a STORED generated primary key. ON DUPLICATE KEY UPDATE collides
// on both, so the re-apply converges, but whether the expression reads only
// columns the replayed rows supply is not visible to the probe.
//
// The probe is handed the RECORDED table — the row's own columns plus "v" —
// because whether a key is SUPPLIED by the replayed rows is half of the
// judgment (audit F-E1 review, HIGH 1). The surrogate rows are that class;
// the two-keys rows pin that ODKU collides on ANY unique key, so "some fully
// supplied NOT NULL unique key" is this engine's predicate, not "the key the
// upsert picker would choose".
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
		{table: "absent", row: ir.Row{"id": int64(1)}},
		{"no_key", "CREATE TABLE no_key (id INT NOT NULL, v TEXT)", ir.Row{"id": int64(1)}, true, false, duplicates, false},
		{"pk", "CREATE TABLE pk (id INT PRIMARY KEY, v TEXT)", ir.Row{"id": int64(1)}, true, true, converges, false},
		{"nn_unique", "CREATE TABLE nn_unique (id INT NOT NULL UNIQUE, v TEXT)", ir.Row{"id": int64(1)}, true, true, converges, false},
		{"nn_unique_composite", "CREATE TABLE nn_unique_composite (a INT NOT NULL, b INT NOT NULL, v TEXT, UNIQUE KEY (a, b))", ir.Row{"a": int64(1), "b": int64(1)}, true, true, converges, false},
		{"nullable_unique", "CREATE TABLE nullable_unique (id INT NULL UNIQUE, v TEXT)", ir.Row{"id": nil}, true, false, duplicates, false},
		{"composite_one_nullable", "CREATE TABLE composite_one_nullable (a INT NOT NULL, b INT NULL, v TEXT, UNIQUE KEY (a, b))", ir.Row{"a": int64(1), "b": nil}, true, false, duplicates, false},
		{"functional_unique", "CREATE TABLE functional_unique (id INT NOT NULL, v TEXT, UNIQUE KEY fu ((id * 2)))", ir.Row{"id": int64(1)}, true, false, converges, true},
		// HIGH 1: keyed only on a surrogate the replayed rows never carry.
		// ODKU has nothing to collide on; every re-applied row draws a fresh
		// key value.
		{"surrogate_auto_increment_pk", "CREATE TABLE surrogate_auto_increment_pk (sid BIGINT AUTO_INCREMENT PRIMARY KEY, id INT NOT NULL, v TEXT)", ir.Row{"id": int64(1)}, true, false, duplicates, false},
		{"surrogate_default_expr_pk", "CREATE TABLE surrogate_default_expr_pk (sid BINARY(16) NOT NULL DEFAULT (UUID_TO_BIN(UUID())) PRIMARY KEY, id INT NOT NULL, v TEXT)", ir.Row{"id": int64(1)}, true, false, duplicates, false},
		// Two keys, one supplied: ODKU collides on ANY unique key, so the
		// supplied one is enough whichever is the PRIMARY KEY.
		{"two_keys_pk_unsupplied", "CREATE TABLE two_keys_pk_unsupplied (sid BIGINT AUTO_INCREMENT PRIMARY KEY, id INT NOT NULL UNIQUE, v TEXT)", ir.Row{"id": int64(1)}, true, true, converges, false},
		{"two_keys_pk_supplied", "CREATE TABLE two_keys_pk_supplied (id INT PRIMARY KEY, u BINARY(16) NOT NULL DEFAULT (UUID_TO_BIN(UUID())) UNIQUE, v TEXT)", ir.Row{"id": int64(1)}, true, true, converges, false},
		// MySQL resolves column names case-insensitively; so does the probe.
		{"case_folded_pk", "CREATE TABLE case_folded_pk (ID INT PRIMARY KEY, v TEXT)", ir.Row{"id": int64(1)}, true, true, converges, false},
		// A generated key derived only from supplied columns converges, but
		// the probe cannot see what its expression reads: conservative.
		{"generated_pk", "CREATE TABLE generated_pk (id INT NOT NULL, k INT AS (id * 2) STORED PRIMARY KEY, v TEXT)", ir.Row{"id": int64(1)}, true, false, converges, true},
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
			exists, keyed, err := prober.ProbeReplayKey(ctx, replayRecordedTable(c.table, c.row))
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

// replayRecordedTable is the recorded (backup) definition a case's replayed
// rows come from: exactly the row's columns plus "v", each nullable when the
// case's value is NULL. The probe's supplied-column set is derived from it.
func replayRecordedTable(name string, row ir.Row) *ir.Table {
	t := &ir.Table{Name: name, Columns: []*ir.Column{{Name: "v", Type: ir.Text{}, Nullable: true}}}
	for col, val := range row {
		t.Columns = append(t.Columns, &ir.Column{Name: col, Type: ir.Integer{Width: 64}, Nullable: val == nil})
	}
	return t
}
