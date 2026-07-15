//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end integration tests for `sluice backfill` (ADR-0159) on
// BOTH shipping engines. One container per engine; each engine runs
// the same scenario set against real tables:
//
//   - int-PK backfill with a self-describing --where guard, done-state
//     rows untouched, bounded batches (>1 chunk at --batch-size 10);
//   - idempotent re-run (completed-state short-circuit) AND a
//     --restart re-walk that the guard turns into 0 updated rows;
//   - resume-after-kill with a non-idempotent-detectable expression
//     (COALESCE(new_col, 0) + old_col + 1): a double-apply would land
//     2*old+2 and the assertions would catch it;
//   - --dry-run writes nothing and reports the estimate;
//   - the Phase-2 completion gate: --verify reports 0/clean after a
//     guarded walk, --verify-only over fresh rows is the coded
//     SLUICE-E-BACKFILL-INCOMPLETE with the right count and writes
//     nothing, and a --restart catch-up walk turns the gate clean;
//   - composite-PK walk;
//   - NULL propagation through the expression + a multi-column --set.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/progress"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func TestBackfill_MySQL_EndToEnd(t *testing.T) {
	srcDSN, _, cleanup := startMySQL(t)
	defer cleanup()
	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	db, err := sql.Open("mysql", srcDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	runBackfillScenarios(t, db, eng, srcDSN)
}

func TestBackfill_PG_EndToEnd(t *testing.T) {
	srcDSN, _, cleanup := startPostgres(t)
	defer cleanup()
	eng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	db, err := sql.Open("pgx", srcDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	runBackfillScenarios(t, db, eng, srcDSN)
}

// mustExecBF runs one DDL/DML statement or fails the test.
func mustExecBF(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

// seedBackfillRows inserts n rows (id 1..n, old_col = id, new_col
// NULL) into table using literal INSERTs so one spelling serves both
// engines.
func seedBackfillRows(t *testing.T, db *sql.DB, table string, n int) {
	t.Helper()
	values := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		values = append(values, fmt.Sprintf("(%d, %d, NULL)", i, i))
	}
	mustExecBF(t, db, fmt.Sprintf("INSERT INTO %s (id, old_col, new_col) VALUES %s", table, strings.Join(values, ", ")))
}

// newIntgBackfiller builds a Backfiller for the standard int-PK
// scenario shape.
func newIntgBackfiller(eng ir.Engine, dsn, table, expr, where string, batch int) *Backfiller {
	return &Backfiller{
		Engine:    eng,
		DSN:       dsn,
		Table:     table,
		Sets:      []ir.BackfillSet{{Column: "new_col", Expr: expr}},
		Where:     where,
		BatchSize: batch,
	}
}

// cancelAfterNChunksSink cancels the run's context once n chunks have
// reported progress — the "kill mid-run" lever for the resume pin.
type cancelAfterNChunksSink struct {
	progress.Nop
	seen   atomic.Int32
	after  int32
	cancel context.CancelFunc
}

func (s *cancelAfterNChunksSink) TableProgress(string, int64, int64) {
	if s.seen.Add(1) == s.after {
		s.cancel()
	}
}

func runBackfillScenarios(t *testing.T, db *sql.DB, eng ir.Engine, dsn string) {
	ctx := context.Background()

	t.Run("int_pk_guarded_backfill_bounded_batches", func(t *testing.T) {
		mustExecBF(t, db, "CREATE TABLE bf_items (id INT PRIMARY KEY, old_col INT NOT NULL, new_col INT NULL)")
		seedBackfillRows(t, db, "bf_items", 100)
		// Rows 1..5 are already "done" with a sentinel value that the
		// expression would NOT produce — any touch is visible.
		mustExecBF(t, db, "UPDATE bf_items SET new_col = old_col + 100 WHERE id <= 5")

		b := newIntgBackfiller(eng, dsn, "bf_items", "old_col + 1", "new_col IS NULL", 10)
		res, err := b.Run(ctx)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if res.RowsUpdated != 95 {
			t.Errorf("RowsUpdated = %d; want 95 (5 rows were already done)", res.RowsUpdated)
		}
		if res.Remaining != 95 {
			t.Errorf("Remaining estimate = %d; want 95", res.Remaining)
		}
		// Bounded batches: 100 PK-walk rows at batch 10 ⇒ 10 chunks.
		if res.Chunks != 10 {
			t.Errorf("Chunks = %d; want 10 (batch bound must hold)", res.Chunks)
		}
		assertBackfillColumn(t, db, "bf_items", func(id, old int64, newV sql.NullInt64) error {
			switch {
			case id <= 5 && (!newV.Valid || newV.Int64 != old+100):
				return fmt.Errorf("pre-done row touched: new_col = %v; want %d", newV, old+100)
			case id > 5 && (!newV.Valid || newV.Int64 != old+1):
				return fmt.Errorf("not backfilled: new_col = %v; want %d", newV, old+1)
			}
			return nil
		})

		t.Run("idempotent_rerun_is_completed_noop", func(t *testing.T) {
			res2, err := newIntgBackfiller(eng, dsn, "bf_items", "old_col + 1", "new_col IS NULL", 10).Run(ctx)
			if err != nil {
				t.Fatalf("re-run: %v", err)
			}
			if !res2.AlreadyComplete || res2.RowsUpdated != 0 || res2.Chunks != 0 {
				t.Errorf("re-run = %+v; want AlreadyComplete with 0 rows and 0 chunks", res2)
			}
		})

		t.Run("restart_rewalks_but_guard_updates_nothing", func(t *testing.T) {
			b3 := newIntgBackfiller(eng, dsn, "bf_items", "old_col + 1", "new_col IS NULL", 10)
			b3.Restart = true
			res3, err := b3.Run(ctx)
			if err != nil {
				t.Fatalf("--restart run: %v", err)
			}
			if res3.AlreadyComplete {
				t.Error("--restart short-circuited on completed state; must start over")
			}
			if res3.Chunks == 0 {
				t.Error("--restart executed no chunks; the walk must re-run")
			}
			if res3.RowsUpdated != 0 {
				t.Errorf("--restart updated %d rows; the self-describing guard must match none", res3.RowsUpdated)
			}
			assertBackfillColumn(t, db, "bf_items", func(id, old int64, newV sql.NullInt64) error {
				want := old + 1
				if id <= 5 {
					want = old + 100
				}
				if !newV.Valid || newV.Int64 != want {
					return fmt.Errorf("row changed by guarded re-walk: new_col = %v; want %d", newV, want)
				}
				return nil
			})
		})
	})

	t.Run("resume_after_kill_lands_exact_remaining_set", func(t *testing.T) {
		mustExecBF(t, db, "CREATE TABLE bf_resume (id INT PRIMARY KEY, old_col INT NOT NULL, new_col INT NULL)")
		seedBackfillRows(t, db, "bf_resume", 100)

		// COALESCE makes a double-apply VISIBLE: applied once a row is
		// old+1; a second application (guard or cursor bug) would land
		// (old+1) + old + 1 = 2*old+2.
		const expr = "COALESCE(new_col, 0) + old_col + 1"
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		b1 := newIntgBackfiller(eng, dsn, "bf_resume", expr, "new_col IS NULL", 10)
		b1.Progress = &cancelAfterNChunksSink{after: 3, cancel: cancel}
		if _, err := b1.Run(runCtx); err == nil {
			t.Fatal("cancelled run returned nil error; want context cancellation")
		}

		var done int64
		if err := db.QueryRow("SELECT COUNT(*) FROM bf_resume WHERE new_col IS NOT NULL").Scan(&done); err != nil {
			t.Fatalf("count done: %v", err)
		}
		if done == 0 || done == 100 {
			t.Fatalf("done after kill = %d; want a partial run (the cancel lever failed)", done)
		}

		b2 := newIntgBackfiller(eng, dsn, "bf_resume", expr, "new_col IS NULL", 10)
		res2, err := b2.Run(ctx)
		if err != nil {
			t.Fatalf("resume run: %v", err)
		}
		if !res2.Resumed {
			t.Error("resume run did not pick up the persisted cursor")
		}
		if res2.RowsUpdated != 100 {
			t.Errorf("RowsUpdated after resume = %d; want the cumulative 100", res2.RowsUpdated)
		}
		assertBackfillColumn(t, db, "bf_resume", func(id, old int64, newV sql.NullInt64) error {
			if !newV.Valid || newV.Int64 != old+1 {
				return fmt.Errorf("new_col = %v; want exactly %d (2*old+2 = %d would mean a double-apply)", newV, old+1, 2*old+2)
			}
			return nil
		})
	})

	t.Run("dry_run_writes_nothing", func(t *testing.T) {
		mustExecBF(t, db, "CREATE TABLE bf_dry (id INT PRIMARY KEY, old_col INT NOT NULL, new_col INT NULL)")
		seedBackfillRows(t, db, "bf_dry", 20)

		var out strings.Builder
		b := newIntgBackfiller(eng, dsn, "bf_dry", "old_col + 1", "new_col IS NULL", 10)
		b.DryRun = true
		b.Out = &out
		res, err := b.Run(ctx)
		if err != nil {
			t.Fatalf("dry-run: %v", err)
		}
		if res.Remaining != 20 {
			t.Errorf("Remaining = %d; want 20", res.Remaining)
		}
		if !strings.Contains(out.String(), "UPDATE") {
			t.Errorf("dry-run output %q missing the UPDATE statement", out.String())
		}
		var touched int64
		if err := db.QueryRow("SELECT COUNT(*) FROM bf_dry WHERE new_col IS NOT NULL").Scan(&touched); err != nil {
			t.Fatalf("count: %v", err)
		}
		if touched != 0 {
			t.Errorf("dry-run modified %d rows; must modify none", touched)
		}
	})

	t.Run("composite_pk_walk", func(t *testing.T) {
		mustExecBF(t, db, "CREATE TABLE bf_comp (a INT, b VARCHAR(16), old_col INT NOT NULL, new_col INT NULL, PRIMARY KEY (a, b))")
		values := make([]string, 0, 60)
		i := 0
		for a := 1; a <= 6; a++ {
			for bIdx := 0; bIdx < 10; bIdx++ {
				i++
				values = append(values, fmt.Sprintf("(%d, 'k%02d', %d, NULL)", a, bIdx, i))
			}
		}
		mustExecBF(t, db, "INSERT INTO bf_comp (a, b, old_col, new_col) VALUES "+strings.Join(values, ", "))

		b := &Backfiller{
			Engine:    eng,
			DSN:       dsn,
			Table:     "bf_comp",
			Sets:      []ir.BackfillSet{{Column: "new_col", Expr: "old_col + 1"}},
			Where:     "new_col IS NULL",
			BatchSize: 7,
		}
		res, err := b.Run(ctx)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if res.RowsUpdated != 60 {
			t.Errorf("RowsUpdated = %d; want 60", res.RowsUpdated)
		}
		// 60 rows at batch 7: 8 full chunks + 1 partial tail = 9.
		if res.Chunks != 9 {
			t.Errorf("Chunks = %d; want 9", res.Chunks)
		}
		var wrong int64
		if err := db.QueryRow("SELECT COUNT(*) FROM bf_comp WHERE new_col IS NULL OR new_col <> old_col + 1").Scan(&wrong); err != nil {
			t.Fatalf("verify: %v", err)
		}
		if wrong != 0 {
			t.Errorf("%d composite-PK rows not backfilled correctly", wrong)
		}
	})

	t.Run("verify_gate_and_verify_only", func(t *testing.T) {
		mustExecBF(t, db, "CREATE TABLE bf_verify (id INT PRIMARY KEY, old_col INT NOT NULL, new_col INT NULL)")
		seedBackfillRows(t, db, "bf_verify", 30)

		// A guarded run with --verify: the post-pass reports 0 and the
		// safe-to-contract signal, exit clean.
		var out strings.Builder
		b := newIntgBackfiller(eng, dsn, "bf_verify", "old_col + 1", "new_col IS NULL", 10)
		b.Verify = true
		b.Out = &out
		res, err := b.Run(ctx)
		if err != nil {
			t.Fatalf("verified run: %v", err)
		}
		if !res.Verified || res.RowsUpdated != 30 {
			t.Errorf("Verified=%v RowsUpdated=%d; want true, 30", res.Verified, res.RowsUpdated)
		}
		if !strings.Contains(out.String(), "safe to run the contract step") {
			t.Errorf("verify report %q missing the safe-to-contract signal", out.String())
		}

		// Fresh rows behind the completed run: the scriptable gate must
		// return the coded incomplete error with the right count and
		// write nothing.
		mustExecBF(t, db, "INSERT INTO bf_verify (id, old_col, new_col) VALUES (31, 31, NULL), (32, 32, NULL), (33, 33, NULL)")
		vo := &Backfiller{Engine: eng, DSN: dsn, Table: "bf_verify", Where: "new_col IS NULL", VerifyOnly: true}
		_, err = vo.Run(ctx)
		if err == nil {
			t.Fatal("--verify-only over fresh rows returned nil; want SLUICE-E-BACKFILL-INCOMPLETE")
		}
		coded, ok := sluicecode.FromError(err)
		if !ok || coded.Code != sluicecode.CodeBackfillIncomplete {
			t.Fatalf("err = %v; want code %s", err, sluicecode.CodeBackfillIncomplete)
		}
		if !strings.Contains(err.Error(), "3 row(s)") {
			t.Errorf("err %q should carry the remaining count 3", err)
		}
		var still int64
		if err := db.QueryRow("SELECT COUNT(*) FROM bf_verify WHERE new_col IS NULL").Scan(&still); err != nil {
			t.Fatalf("count: %v", err)
		}
		if still != 3 {
			t.Errorf("rows matching the guard after --verify-only = %d; want 3 (the gate must write nothing)", still)
		}

		// --restart re-walks the completed spec to pick up the
		// stragglers; the gate then passes clean.
		b2 := newIntgBackfiller(eng, dsn, "bf_verify", "old_col + 1", "new_col IS NULL", 10)
		b2.Restart = true
		res2, err := b2.Run(ctx)
		if err != nil {
			t.Fatalf("--restart catch-up run: %v", err)
		}
		if res2.RowsUpdated != 3 {
			t.Errorf("catch-up RowsUpdated = %d; want 3", res2.RowsUpdated)
		}
		res3, err := vo.Run(ctx)
		if err != nil {
			t.Fatalf("--verify-only after catch-up: %v", err)
		}
		if !res3.Verified {
			t.Error("Verified = false after the catch-up walk; want true")
		}
	})

	t.Run("null_propagation_and_multi_set", func(t *testing.T) {
		mustExecBF(t, db, "CREATE TABLE bf_multi (id INT PRIMARY KEY, old_col INT NULL, label VARCHAR(32) NOT NULL, new_a INT NULL, new_b VARCHAR(32) NULL)")
		values := make([]string, 0, 10)
		for i := 1; i <= 10; i++ {
			oldVal := fmt.Sprintf("%d", i)
			if i%2 == 0 {
				oldVal = "NULL" // NULL source values must propagate, not error
			}
			values = append(values, fmt.Sprintf("(%d, %s, 'name%d', NULL, NULL)", i, oldVal, i))
		}
		mustExecBF(t, db, "INSERT INTO bf_multi (id, old_col, label, new_a, new_b) VALUES "+strings.Join(values, ", "))

		b := &Backfiller{
			Engine: eng,
			DSN:    dsn,
			Table:  "bf_multi",
			Sets: []ir.BackfillSet{
				{Column: "new_a", Expr: "old_col * 2"},
				{Column: "new_b", Expr: "UPPER(label)"},
			},
			Where:     "new_b IS NULL",
			BatchSize: 3,
		}
		res, err := b.Run(ctx)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if res.RowsUpdated != 10 {
			t.Errorf("RowsUpdated = %d; want 10", res.RowsUpdated)
		}
		rows, err := db.Query("SELECT id, old_col, new_a, new_b FROM bf_multi ORDER BY id")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id int64
			var oldV, newA sql.NullInt64
			var newB sql.NullString
			if err := rows.Scan(&id, &oldV, &newA, &newB); err != nil {
				t.Fatalf("scan: %v", err)
			}
			wantB := fmt.Sprintf("NAME%d", id)
			if !newB.Valid || newB.String != wantB {
				t.Errorf("id=%d new_b = %v; want %q", id, newB, wantB)
			}
			switch {
			case oldV.Valid && (!newA.Valid || newA.Int64 != oldV.Int64*2):
				t.Errorf("id=%d new_a = %v; want %d", id, newA, oldV.Int64*2)
			case !oldV.Valid && newA.Valid:
				t.Errorf("id=%d new_a = %v; want NULL (NULL old_col must propagate)", id, newA)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
	})
}

// assertBackfillColumn walks (id, old_col, new_col) of table and
// applies check per row.
func assertBackfillColumn(t *testing.T, db *sql.DB, table string, check func(id, old int64, newV sql.NullInt64) error) {
	t.Helper()
	rows, err := db.Query("SELECT id, old_col, new_col FROM " + table + " ORDER BY id")
	if err != nil {
		t.Fatalf("query %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	n := 0
	for rows.Next() {
		var id, old int64
		var newV sql.NullInt64
		if err := rows.Scan(&id, &old, &newV); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if err := check(id, old, newV); err != nil {
			t.Errorf("%s id=%d: %v", table, id, err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if n == 0 {
		t.Fatalf("%s: no rows scanned", table)
	}
}
