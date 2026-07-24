// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// Unit pins for the SQLSTATE 55006 slot-active reap-window retry
// (retryWhileSlotActive), the shared core of START_REPLICATION's
// disconnect-is-not-release wait. The budget/backoff are package vars so
// these run in milliseconds instead of sleeping out the real 5-minute
// wall-clock budget; no real walsender is needed — `attempt` is scripted.

// slotActive is the SQLSTATE 55006 error a just-disconnected walsender's
// lingering slot produces, in the same shape pglogrepl surfaces.
func slotActive() error {
	return &pgconn.PgError{Code: "55006", Message: `replication slot "sluice_slot" is active for PID 42`}
}

// shrinkSlotActiveRetry compresses the budget + backoff so the retry
// pins run fast, restoring the real values on cleanup.
func shrinkSlotActiveRetry(t *testing.T, budget time.Duration) {
	t.Helper()
	b, base, mx := slotActiveRetryBudget, slotActiveRetryBaseBackoff, slotActiveRetryMaxBackoff
	slotActiveRetryBudget = budget
	slotActiveRetryBaseBackoff = time.Millisecond
	slotActiveRetryMaxBackoff = 4 * time.Millisecond
	t.Cleanup(func() {
		slotActiveRetryBudget = b
		slotActiveRetryBaseBackoff = base
		slotActiveRetryMaxBackoff = mx
	})
}

// TestRetryWhileSlotActive_ClearsAfterManyRetries is the RED-before-
// GREEN pin for the class change (fixed attempt COUNT → wall-clock
// BUDGET). The scripted walsender stays "active" for FIFTEEN attempts —
// far past the old fixed 8-attempt cap — then releases. The old count-
// bounded loop returned the loud 55006 at attempt 8 (RED, verified by
// re-adding the cap); the budget loop keeps retrying as long as the
// wall-clock budget permits and succeeds on the 16th (GREEN). This is
// the exact managed-PG shape: PS-PG PG18 held the slot active >90s, far
// past the old ~31.5s attempt budget but well within the new 5-minute
// one.
func TestRetryWhileSlotActive_ClearsAfterManyRetries(t *testing.T) {
	shrinkSlotActiveRetry(t, 5*time.Second) // generous relative to 15 ~ms backoffs

	calls := 0
	err := retryWhileSlotActive(context.Background(), "sluice_slot", func() error {
		calls++
		if calls <= 15 {
			return slotActive()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryWhileSlotActive: want success once the slot releases within budget; got %v", err)
	}
	if calls != 16 {
		t.Errorf("attempts = %d; want 16 (15 active failures past the old 8-attempt cap, then success)", calls)
	}
}

// TestRetryWhileSlotActive_TooShortBudgetStillFails is the other half of
// the RED-before-GREEN pair: with a budget SHORTER than the reap window
// (modelling the pre-fix ~31.5s bound against PS-PG's ~90s hold) the same
// still-active slot exhausts the budget and surfaces the loud 55006
// unchanged. It proves the budget is the deciding factor — widening it is
// what fixes the managed-PG resume, and a too-short budget is what broke
// it.
func TestRetryWhileSlotActive_TooShortBudgetStillFails(t *testing.T) {
	shrinkSlotActiveRetry(t, 20*time.Millisecond)

	// Scripted slot that only releases after 500ms — well past the tiny
	// budget, so the loop must give up first.
	deadline := time.Now().Add(500 * time.Millisecond)
	err := retryWhileSlotActive(context.Background(), "sluice_slot", func() error {
		if time.Now().Before(deadline) {
			return slotActive()
		}
		return nil
	})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55006" {
		t.Fatalf("want the original loud 55006 refusal after the budget is exhausted; got %v", err)
	}
}

// TestRetryWhileSlotActive_NeverClearsFailsAtBudget pins the floor: a
// slot that NEVER releases fails loudly at ~the budget, not forever. The
// returned error is the original 55006 (the second-writer refusal),
// unchanged.
func TestRetryWhileSlotActive_NeverClearsFailsAtBudget(t *testing.T) {
	shrinkSlotActiveRetry(t, 40*time.Millisecond)

	start := time.Now()
	err := retryWhileSlotActive(context.Background(), "sluice_slot", slotActive)
	elapsed := time.Since(start)

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55006" {
		t.Fatalf("want the original loud 55006 after budget exhaustion; got %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Errorf("returned in %v, before the budget elapsed — the retry didn't wait", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("returned in %v — far past the budget; the loop is not bounded", elapsed)
	}
}

// TestRetryWhileSlotActive_NonRetryableReturnsImmediately pins that a
// non-55006 error (and a success) short-circuit with a single attempt —
// no backoff, no budget wait.
func TestRetryWhileSlotActive_NonRetryableReturnsImmediately(t *testing.T) {
	shrinkSlotActiveRetry(t, 5*time.Minute) // long budget; a non-retryable error must not wait on it

	t.Run("non-55006 error", func(t *testing.T) {
		other := &pgconn.PgError{Code: "42704", Message: `replication slot "sluice_slot" does not exist`}
		calls := 0
		start := time.Now()
		err := retryWhileSlotActive(context.Background(), "sluice_slot", func() error {
			calls++
			return other
		})
		if !errors.Is(err, other) {
			t.Fatalf("want the non-55006 error returned unchanged; got %v", err)
		}
		if calls != 1 {
			t.Errorf("attempts = %d; want 1 (no retry on a non-55006 error)", calls)
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("returned in %v; a non-retryable error must not wait on the budget", d)
		}
	})

	t.Run("success", func(t *testing.T) {
		calls := 0
		if err := retryWhileSlotActive(context.Background(), "sluice_slot", func() error {
			calls++
			return nil
		}); err != nil {
			t.Fatalf("want nil on immediate success; got %v", err)
		}
		if calls != 1 {
			t.Errorf("attempts = %d; want 1", calls)
		}
	})
}

// TestRetryWhileSlotActive_CtxCancelReturnsPromptly pins that cancelling
// ctx mid-wait returns ctx.Err() promptly — a real shutdown exits fast
// regardless of the (here, long) budget.
func TestRetryWhileSlotActive_CtxCancelReturnsPromptly(t *testing.T) {
	shrinkSlotActiveRetry(t, 5*time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := retryWhileSlotActive(ctx, "sluice_slot", slotActive)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled from the cancelled wait; got %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("returned in %v after cancel; want prompt (< 1s)", d)
	}
}

// TestSlotActiveRetryBudget_SharesTheEngineNeutralHome pins that the
// engine's budget derives from the single [ir.SlotActiveReapBudget]
// home so it can't drift from the decommission slot-drop path.
func TestSlotActiveRetryBudget_SharesTheEngineNeutralHome(t *testing.T) {
	if slotActiveRetryBudget != ir.SlotActiveReapBudget {
		t.Errorf("slotActiveRetryBudget = %v; want ir.SlotActiveReapBudget (%v) — the shared home", slotActiveRetryBudget, ir.SlotActiveReapBudget)
	}
	if ir.SlotActiveReapBudget != 5*time.Minute {
		t.Errorf("ir.SlotActiveReapBudget = %v; want 5m", ir.SlotActiveReapBudget)
	}
}
