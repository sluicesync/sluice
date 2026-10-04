//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The MySQL half of the behaviour behind the BROKER-INCREMENTAL-PARTIAL
// recovery text (audit F-E1, second review); the Postgres file of the same
// name carries the full rationale. A broker re-run re-applies a whole
// incremental through this applier; an incremental that changed a key value
// either refuses on every re-run or, when a key value moved onto another
// row, re-applies to the wrong row silently.
//
// The before-images are FULL, as a binlog_row_image=FULL source sends them,
// so the silent key-reuse case needs the reused row's whole image to match
// the earlier before-image; a MINIMAL-image source sends the key alone and
// matches on it, as on Postgres.

func keyChangeApplierMySQL(t *testing.T, ctx context.Context, dsn string) func(...ir.Change) error {
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
		return applier.(*ChangeApplier).ApplyBatch(ctx, testStreamID, ch, 100)
	}
}

func keyChangeRowsMySQL(t *testing.T, ctx context.Context, dsn, table string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var s sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id) FROM "+table).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s.String
}

// TestApplier_KeyChangingReplay_RefusesLoudly: [INSERT id=1, UPDATE id
// 1→2] re-applied fails on the key (1062) on every re-run and leaves the
// target as the first apply left it.
func TestApplier_KeyChangingReplay_RefusesLoudly(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	applyMySQLApplier(t, dsn, "CREATE TABLE kc (id INT PRIMARY KEY, v VARCHAR(8))")
	apply := keyChangeApplierMySQL(t, ctx, dsn)

	ins := ir.Insert{Table: "kc", Row: ir.Row{"id": int64(1), "v": "a"}}
	upd := ir.Update{Table: "kc", Before: ir.Row{"id": int64(1), "v": "a"}, After: ir.Row{"id": int64(2), "v": "a"}}
	if err := apply(ins, upd); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	for rerun := 1; rerun <= 2; rerun++ {
		err := apply(ins, upd)
		if err == nil || !strings.Contains(err.Error(), "1062") {
			t.Fatalf("re-run %d of a key-changing sequence returned %v; want a loud 1062", rerun, err)
		}
		if got := keyChangeRowsMySQL(t, ctx, dsn, "kc"); got != "2:a" {
			t.Errorf("re-run %d left the target at %q; want the first apply's %q", rerun, got, "2:a")
		}
	}
}

// TestApplier_KeyChangingReplay_KeyReuseIsSilent is the MySQL twin of the
// Postgres CHARACTERIZATION pin of F-E1-KEY-REUSE-REPLAY (see there): the
// re-apply of [UPDATE 1→2, DELETE 2, UPDATE 3→1] onto the source's end
// state {1:a} empties the table at a nil error.
func TestApplier_KeyChangingReplay_KeyReuseIsSilent(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	applyMySQLApplier(t, dsn, "CREATE TABLE kr (id INT PRIMARY KEY, v VARCHAR(8))")
	applyMySQLApplier(t, dsn, "INSERT INTO kr VALUES (1, 'a'), (3, 'a')")
	apply := keyChangeApplierMySQL(t, ctx, dsn)

	seq := []ir.Change{
		ir.Update{Table: "kr", Before: ir.Row{"id": int64(1), "v": "a"}, After: ir.Row{"id": int64(2), "v": "a"}},
		ir.Delete{Table: "kr", Before: ir.Row{"id": int64(2), "v": "a"}},
		ir.Update{Table: "kr", Before: ir.Row{"id": int64(3), "v": "a"}, After: ir.Row{"id": int64(1), "v": "a"}},
	}
	if err := apply(seq...); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if got := keyChangeRowsMySQL(t, ctx, dsn, "kr"); got != "1:a" {
		t.Fatalf("first apply left %q; want the source's %q", got, "1:a")
	}
	err := apply(seq...)
	got := keyChangeRowsMySQL(t, ctx, dsn, "kr")
	if err != nil || got != "" {
		t.Fatalf("the key-reuse re-apply now returns (%v) and leaves %q; it used to return nil and leave the table "+
			"empty. If it converges or refuses now, the defect is fixed: update BROKER-INCREMENTAL-PARTIAL's text, "+
			"the docs and the F-E1-KEY-REUSE-REPLAY backlog entry, and invert this pin", err, got)
	}
}
