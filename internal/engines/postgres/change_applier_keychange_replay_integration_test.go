//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The behaviour behind the BROKER-INCREMENTAL-PARTIAL recovery text (audit
// F-E1, second review). A broker re-run re-applies a whole incremental
// through this applier, so what this applier does with a change sequence
// applied TWICE is what the operator gets. Inserts and same-key updates
// converge; an incremental that changed a key value does not, in two ways,
// and the message names both. These pins bind that text
// (TestBrokerIncrementalPartialError_RecoveryTextIsScoped) to behaviour.
//
// The before-images are key-only, which is what a Postgres source with the
// default REPLICA IDENTITY sends for an UPDATE that changes the key and for
// a DELETE.

func keyChangeApplier(t *testing.T, ctx context.Context, dsn string) func(...ir.Change) error {
	t.Helper()
	applier, err := (Engine{}).OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeIf(applier) })
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatal(err)
	}
	return func(cs ...ir.Change) error {
		ch := make(chan ir.Change, len(cs))
		for _, c := range cs {
			ch <- c
		}
		close(ch)
		return applier.(ir.BatchedChangeApplier).ApplyBatch(ctx, "keychange", ch, 100)
	}
}

func keyChangeRows(t *testing.T, ctx context.Context, dsn, table string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var s sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT string_agg(id||':'||v, ',' ORDER BY id) FROM `+table).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s.String
}

// TestApplier_KeyChangingReplay_RefusesLoudly: [INSERT id=1, UPDATE id
// 1→2] applied, then re-applied twice. Every re-apply must fail on the key
// (23505) and leave the target as the first apply left it — a refusal on
// every re-run, which is why the broker's recovery for it is
// --reset-target-data.
func TestApplier_KeyChangingReplay_RefusesLoudly(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	applyPGApplier(t, dsn, `CREATE TABLE kc (id INT PRIMARY KEY, v TEXT)`)
	apply := keyChangeApplier(t, ctx, dsn)

	ins := ir.Insert{Table: "kc", Row: ir.Row{"id": int64(1), "v": "a"}}
	upd := ir.Update{Table: "kc", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2), "v": "a"}}
	if err := apply(ins, upd); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	for rerun := 1; rerun <= 2; rerun++ {
		err := apply(ins, upd)
		if err == nil || !strings.Contains(err.Error(), "23505") {
			t.Fatalf("re-run %d of a key-changing sequence returned %v; want a loud 23505", rerun, err)
		}
		if got := keyChangeRows(t, ctx, dsn, "kc"); got != "2:a" {
			t.Errorf("re-run %d left the target at %q; want the first apply's %q", rerun, got, "2:a")
		}
	}
}

// TestApplier_KeyChangingReplay_KeyReuseIsSilent is a CHARACTERIZATION pin
// of a known defect, filed in docs/dev/audit-backlog.md as
// F-E1-KEY-REUSE-REPLAY: a key value moved off one row and onto another
// inside one incremental. The source ends with {1:c} (row 3 renamed to 1);
// re-applying [UPDATE 1→2, DELETE 2, UPDATE 3→1] onto that state moves the
// CURRENT row 1 (the old row 3) to 2, deletes it, and finds no row 3 — the
// target ends EMPTY, at a nil error. The BROKER-INCREMENTAL-PARTIAL text
// warns of exactly this. When the exactly-once follow-up makes the re-run
// converge, this test fails: update the message, the docs and the backlog
// entry with it.
func TestApplier_KeyChangingReplay_KeyReuseIsSilent(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	applyPGApplier(t, dsn, `CREATE TABLE kr (id INT PRIMARY KEY, v TEXT); INSERT INTO kr VALUES (1, 'a'), (3, 'c')`)
	apply := keyChangeApplier(t, ctx, dsn)

	seq := []ir.Change{
		ir.Update{Table: "kr", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2), "v": "a"}},
		ir.Delete{Table: "kr", Before: ir.Row{"id": int64(2)}},
		ir.Update{Table: "kr", Before: ir.Row{"id": int64(3)}, After: ir.Row{"id": int64(1), "v": "c"}},
	}
	if err := apply(seq...); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if got := keyChangeRows(t, ctx, dsn, "kr"); got != "1:c" {
		t.Fatalf("first apply left %q; want the source's %q", got, "1:c")
	}
	err := apply(seq...)
	got := keyChangeRows(t, ctx, dsn, "kr")
	if err != nil || got != "" {
		t.Fatalf("the key-reuse re-apply now returns (%v) and leaves %q; it used to return nil and leave the table "+
			"empty. If it converges or refuses now, the defect is fixed: update BROKER-INCREMENTAL-PARTIAL's text, "+
			"the docs and the F-E1-KEY-REUSE-REPLAY backlog entry, and invert this pin", err, got)
	}
}
