// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// shrinkOverlapReparentEnvelope makes the shared reparent-retry envelope fast
// for tests and restores it on cleanup. Production never mutates these.
func shrinkOverlapReparentEnvelope(t *testing.T) {
	t.Helper()
	ob, oc, ow, oa := pgCopyReparentBackoffBaseVar, pgCopyReparentBackoffCapVar, pgCopyReparentMaxWallVar, pgCopyReparentRetryAttemptsVar
	pgCopyReparentBackoffBaseVar = time.Millisecond
	pgCopyReparentBackoffCapVar = 2 * time.Millisecond
	pgCopyReparentMaxWallVar = 5 * time.Second
	pgCopyReparentRetryAttemptsVar = 100000
	t.Cleanup(func() {
		pgCopyReparentBackoffBaseVar, pgCopyReparentBackoffCapVar, pgCopyReparentMaxWallVar, pgCopyReparentRetryAttemptsVar = ob, oc, ow, oa
	})
}

// reparentErr is a classified storage-grow/reparent transient (57P01) the
// overlap retry must ride; classifyApplierError wraps it as ir.RetriableError.
func reparentErr() error {
	return &pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"}
}

// TestRetryIndexBuildWithReparent_RidesTransientThenSucceeds: a build that hits
// N reparent transients then succeeds converges, re-acquiring a fresh conn
// before each retry (ADR-0114 overlap residual / item 42).
func TestRetryIndexBuildWithReparent_RidesTransientThenSucceeds(t *testing.T) {
	shrinkOverlapReparentEnvelope(t)
	attempts, reacquires := 0, 0
	attempt := func(context.Context) error {
		attempts++
		if attempts <= 3 {
			return reparentErr()
		}
		return nil
	}
	reacquire := func(context.Context) error { reacquires++; return nil }

	if err := retryIndexBuildWithReparent(context.Background(), `"idx_x" on "t"`, attempt, reacquire); err != nil {
		t.Fatalf("expected success after riding transients, got %v", err)
	}
	if attempts != 4 {
		t.Fatalf("attempts = %d; want 4 (3 transient + 1 success)", attempts)
	}
	if reacquires != 3 {
		t.Fatalf("reacquires = %d; want 3 (one fresh conn before each retry)", reacquires)
	}
}

// TestRetryIndexBuildWithReparent_TerminalNoRetry: a real DDL fault (23505
// unique_violation — NOT in the retriable set) returns unchanged after one
// attempt, with NO reacquire. The build phase must still fail loudly on a
// genuine error.
func TestRetryIndexBuildWithReparent_TerminalNoRetry(t *testing.T) {
	shrinkOverlapReparentEnvelope(t)
	fault := &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"}
	attempts, reacquires := 0, 0
	attempt := func(context.Context) error { attempts++; return fault }
	reacquire := func(context.Context) error { reacquires++; return nil }

	err := retryIndexBuildWithReparent(context.Background(), `"idx" on "t"`, attempt, reacquire)
	if !errors.Is(err, fault) {
		t.Fatalf("expected the terminal fault unchanged, got %v", err)
	}
	if attempts != 1 || reacquires != 0 {
		t.Fatalf("attempts=%d reacquires=%d; want 1/0 (no retry on a real DDL fault)", attempts, reacquires)
	}
}

// TestRetryIndexBuildWithReparent_ReacquireErrorRidesBudget: a reacquire that
// itself fails with a transient (the target still unreachable) is classified
// on the next iteration and ridden, then succeeds.
func TestRetryIndexBuildWithReparent_ReacquireErrorRidesBudget(t *testing.T) {
	shrinkOverlapReparentEnvelope(t)
	attempts, reacquires := 0, 0
	attempt := func(context.Context) error {
		attempts++
		// First attempt fails (reparent); after a successful reacquire, succeed.
		if attempts == 1 {
			return reparentErr()
		}
		return nil
	}
	reacquire := func(context.Context) error {
		reacquires++
		if reacquires == 1 {
			return reparentErr() // target still mid-reparent — must ride this too
		}
		return nil
	}
	if err := retryIndexBuildWithReparent(context.Background(), `"idx" on "t"`, attempt, reacquire); err != nil {
		t.Fatalf("expected success after riding a reacquire transient, got %v", err)
	}
	if reacquires != 2 {
		t.Fatalf("reacquires = %d; want 2 (first failed-transient, second ok)", reacquires)
	}
}

// TestRetryIndexBuildWithReparent_ExhaustionIsLoud: an always-transient build
// surfaces a LOUD terminal error wrapping the last transient after the
// wall-clock bound (never silent, never infinite).
func TestRetryIndexBuildWithReparent_ExhaustionIsLoud(t *testing.T) {
	shrinkOverlapReparentEnvelope(t)
	pgCopyReparentMaxWallVar = 40 * time.Millisecond
	last := reparentErr()
	attempt := func(context.Context) error { return last }
	reacquire := func(context.Context) error { return nil }

	err := retryIndexBuildWithReparent(context.Background(), `"idx" on "t"`, attempt, reacquire)
	if err == nil {
		t.Fatal("expected a loud terminal error on exhaustion, got nil")
	}
	if !errors.Is(err, last) {
		t.Fatalf("exhaustion error must wrap the last transient, got %v", err)
	}
}

// TestRetryIndexBuildWithReparent_CtxCancel: ctx cancel during the backoff
// unwinds promptly with ctx.Err().
func TestRetryIndexBuildWithReparent_CtxCancel(t *testing.T) {
	shrinkOverlapReparentEnvelope(t)
	ctx, cancel := context.WithCancel(context.Background())
	attempt := func(context.Context) error { cancel(); return reparentErr() }
	reacquire := func(context.Context) error { return nil }

	err := retryIndexBuildWithReparent(ctx, `"idx" on "t"`, attempt, reacquire)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on cancel during backoff, got %v", err)
	}
}
