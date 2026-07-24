//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Vector B on the CDC APPLY path (v0.100 readiness C2). The three bulk
// writers grew the SHOW WARNINGS silent-clamp closure in v0.99.28
// (connect_sqlmode_integration_test.go pins them); the ChangeApplier —
// the steady-state sync path — did not, so under the operator's
// --mysql-sql-mode='' relaxed opt-in a CDC UPDATE writing an
// out-of-range value silently clamped (300 → 127 on TINYINT) with no
// signal. These tests pin the apply-path twin across its two write
// shapes (the Bug-74 "pin the class" matrix for this surface):
//
//   - serial per-change dispatch (Apply → dispatch → single-row exec)
//   - the ADR-0139 coalesced multi-row upsert (ApplyBatch →
//     mysqlBatchTx.flushUpserts) — also the exact statement shape the
//     ADR-0104 concurrent lanes emit (same accumulator + builder)
//
// plus the strict-mode control (the default errors loudly at the exec,
// exactly as today — and the probe never runs, so the hot path pays
// nothing) and the once-per-table WARN dedup.

package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// relaxSharedGlobalSQLMode relaxes the shared container's GLOBAL
// sql_mode so freshly-pooled connections coerce instead of erroring,
// restoring the original mode on cleanup (other tests on the shared
// container expect strict). Mirrors the bulk-path Vector B pins.
func relaxSharedGlobalSQLMode(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	admin, err := openDB(ctx, mustParseDSN(t, dsn), nil)
	if err != nil {
		t.Fatalf("admin openDB: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	var origGlobal string
	if err := admin.QueryRowContext(ctx, "SELECT @@GLOBAL.sql_mode").Scan(&origGlobal); err != nil {
		t.Fatalf("read GLOBAL sql_mode: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "SET GLOBAL sql_mode = ''"); err != nil {
		t.Skipf("cannot relax GLOBAL sql_mode for the apply-path probe: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "SET GLOBAL sql_mode = ?", origGlobal) })
}

// openRelaxedApplier opens a ChangeApplier through the real engine +
// option path (Engine{}.WithSQLMode("") — the same construction the
// CLI's --mysql-sql-mode=” resolves to), so the relaxed-mode gate is
// pinned through the layer that sets it, not via a hand-built struct.
func openRelaxedApplier(t *testing.T, ctx context.Context, dsn string) ir.ChangeApplier {
	t.Helper()
	eng := Engine{}.WithSQLMode("")
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier (relaxed): %v", err)
	}
	t.Cleanup(func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	return applier
}

// TestChangeApplier_RelaxedModeWarnsOnClamp_Serial pins the serial
// per-change path: a CDC UPDATE writing 300 into TINYINT under relaxed
// mode clamps to 127 AND emits the one-time-per-table Vector B WARN
// (RED pre-fix: the clamp was silent), and a second clamping change on
// the same table does NOT warn again.
func TestChangeApplier_RelaxedModeWarnsOnClamp_Serial(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "vectorb_apply")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	relaxSharedGlobalSQLMode(t, ctx, dsn)

	applyMySQLApplier(t, dsn, `
		CREATE TABLE gauges (
			id    INT     NOT NULL,
			small TINYINT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	applier := openRelaxedApplier(t, ctx, dsn)
	buf := captureSlog(t)

	events := []ir.Change{
		ir.Insert{Schema: "vectorb_apply", Table: "gauges", Row: ir.Row{"id": int64(1), "small": int64(5)}},
		ir.Update{
			Schema: "vectorb_apply", Table: "gauges",
			Before: ir.Row{"id": int64(1), "small": int64(5)},
			After:  ir.Row{"id": int64(1), "small": int64(300)}, // 300 > TINYINT max 127 → server clamps
		},
	}
	pumpChanges(t, ctx, applier, events)

	// The value was silently clamped by the server (documents the
	// Vector B shape the WARN exists to name)...
	var got int
	db, err := openDB(ctx, mustParseDSN(t, dsn), nil)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	if err := db.QueryRowContext(ctx, "SELECT small FROM gauges WHERE id=1").Scan(&got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got != 127 {
		t.Skipf("server did not clamp 300→127 (got %d); GLOBAL sql_mode relax may not have taken on the pooled conn", got)
	}
	// ...and the clamp was reported as a loud WARN naming the table.
	out := buf.String()
	if !strings.Contains(out, "gauges") || !strings.Contains(out, "SILENTLY coerced") {
		t.Errorf("serial apply relaxed-mode WARN missing/incorrect:\n%s", out)
	}
	if !strings.Contains(out, "--mysql-sql-mode") {
		t.Errorf("apply-path WARN should name the strict-mode remedy:\n%s", out)
	}

	// Dedup: a second clamping UPDATE on the same table must NOT warn again.
	buf.Reset()
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Update{
			Schema: "vectorb_apply", Table: "gauges",
			Before: ir.Row{"id": int64(1), "small": int64(127)},
			After:  ir.Row{"id": int64(1), "small": int64(400)},
		},
	})
	if strings.Contains(buf.String(), "SILENTLY coerced") {
		t.Errorf("warned twice for the same table; want once-per-table:\n%s", buf.String())
	}
}

// TestChangeApplier_RelaxedModeWarnsOnClamp_CoalescedBatch pins the
// ADR-0139 multi-row upsert shape: a batched run of clamping INSERTs
// coalesces into ONE multi-row statement (asserted via the
// multiRowFlushHookForTest seam so the pin cannot go vacuous by
// silently falling back to per-row apply) and the clamp is WARNed once.
func TestChangeApplier_RelaxedModeWarnsOnClamp_CoalescedBatch(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "vectorb_apply_batch")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	relaxSharedGlobalSQLMode(t, ctx, dsn)

	applyMySQLApplier(t, dsn, `
		CREATE TABLE gauges (
			id    INT     NOT NULL,
			small TINYINT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	applier := openRelaxedApplier(t, ctx, dsn)

	maxCoalesced := 0
	multiRowFlushHookForTest = func(rows int) {
		if rows > maxCoalesced {
			maxCoalesced = rows
		}
	}
	defer func() { multiRowFlushHookForTest = nil }()

	buf := captureSlog(t)
	events := make([]ir.Change, 0, 4)
	for i := int64(1); i <= 4; i++ {
		events = append(events, ir.Insert{
			Position: ir.Position{Engine: engineNameMySQL, Token: "tok"},
			Schema:   "vectorb_apply_batch",
			Table:    "gauges",
			Row:      ir.Row{"id": i, "small": int64(300)}, // every row clamps to 127
		})
	}
	pumpBatchedChanges(t, ctx, applier, events, 8)

	if maxCoalesced < 2 {
		t.Fatalf("coalesced flush never fired with >1 row (max=%d) — the multi-row upsert path was not exercised", maxCoalesced)
	}

	db, err := openDB(ctx, mustParseDSN(t, dsn), nil)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	var got int
	if err := db.QueryRowContext(ctx, "SELECT small FROM gauges WHERE id=1").Scan(&got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got != 127 {
		t.Skipf("server did not clamp 300→127 (got %d); GLOBAL sql_mode relax may not have taken", got)
	}

	out := buf.String()
	if !strings.Contains(out, "gauges") || !strings.Contains(out, "SILENTLY coerced") {
		t.Errorf("coalesced-batch relaxed-mode WARN missing/incorrect:\n%s", out)
	}
	if n := strings.Count(out, "SILENTLY coerced"); n > 1 {
		t.Errorf("WARN emitted %d times for one table; want once-per-table:\n%s", n, out)
	}
}

// TestChangeApplier_StrictModeErrorsOnOutOfRange_Control is the
// strict-mode control: with the default strict session (nil sqlMode —
// openDB injects STRICT_TRANS_TABLES regardless of the relaxed GLOBAL),
// the same out-of-range UPDATE errors LOUDLY at the exec, exactly as
// today. Without this control a future regression in the session
// sql_mode plumbing would make the relaxed-mode WARN tests green for
// the wrong reason.
func TestChangeApplier_StrictModeErrorsOnOutOfRange_Control(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "vectorb_apply_strict")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	relaxSharedGlobalSQLMode(t, ctx, dsn)

	applyMySQLApplier(t, dsn, `
		CREATE TABLE gauges (
			id    INT     NOT NULL,
			small TINYINT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier (strict): %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	ch := make(chan ir.Change, 1)
	ch <- ir.Insert{Schema: "vectorb_apply_strict", Table: "gauges", Row: ir.Row{"id": int64(1), "small": int64(300)}}
	close(ch)
	applyErr := applier.Apply(ctx, testStreamID, ch)
	if applyErr == nil {
		t.Fatal("strict-mode apply of out-of-range TINYINT succeeded; want the loud MySQL 1264 refusal")
	}
	if !strings.Contains(strings.ToLower(applyErr.Error()), "out of range") {
		t.Errorf("strict-mode refusal should surface the out-of-range error; got: %v", applyErr)
	}
}
