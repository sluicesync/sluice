// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeDDLClassifier implements ir.TransientClassifier for the DDL-phase
// retry tests: transient verdict is delegated to a closure so each case
// controls exactly which errors are treated as ride-out-able.
type fakeDDLClassifier struct {
	isTransient func(error) bool
}

func (f fakeDDLClassifier) IsTransientError(err error) bool { return f.isTransient(err) }

// shrinkDDLRetryEnvelope makes the retry loop fast for tests and restores
// the production envelope on cleanup. Production NEVER mutates these — only
// tests, via this helper (the vars-not-consts reasoning in the source).
func shrinkDDLRetryEnvelope(t *testing.T) {
	t.Helper()
	origBase, origCap, origWall, origAttempts := ddlPhaseRetryBackoffBase, ddlPhaseRetryBackoffCap, ddlPhaseRetryMaxWall, ddlPhaseRetryAttempts
	ddlPhaseRetryBackoffBase = time.Millisecond
	ddlPhaseRetryBackoffCap = 2 * time.Millisecond
	ddlPhaseRetryMaxWall = 5 * time.Second
	ddlPhaseRetryAttempts = 100000
	t.Cleanup(func() {
		ddlPhaseRetryBackoffBase, ddlPhaseRetryBackoffCap, ddlPhaseRetryMaxWall, ddlPhaseRetryAttempts = origBase, origCap, origWall, origAttempts
	})
}

func TestRunDDLPhaseWithReparentRetry_RetriesTransientThenSucceeds(t *testing.T) {
	shrinkDDLRetryEnvelope(t)
	transient := errors.New("57P01: terminating connection due to administrator command")
	cl := fakeDDLClassifier{isTransient: func(err error) bool { return errors.Is(err, transient) }}

	calls := 0
	do := func(context.Context) error {
		calls++
		if calls < 4 {
			return transient
		}
		return nil
	}
	if err := RunDDLPhaseWithReparentRetry(context.Background(), "indexes", cl, do); err != nil {
		t.Fatalf("expected success after transient retries, got %v", err)
	}
	if calls != 4 {
		t.Fatalf("expected 4 attempts (3 transient + 1 success), got %d", calls)
	}
}

func TestRunDDLPhaseWithReparentRetry_TerminalErrorReturnsImmediately(t *testing.T) {
	shrinkDDLRetryEnvelope(t)
	// Classifier says NOTHING is transient — a real DDL fault must surface
	// after exactly one attempt (no retry, no backoff, byte-for-byte the
	// pre-ADR-0114 behaviour).
	cl := fakeDDLClassifier{isTransient: func(error) bool { return false }}
	fault := errors.New("42703: column \"nope\" does not exist")
	calls := 0
	do := func(context.Context) error { calls++; return fault }

	err := RunDDLPhaseWithReparentRetry(context.Background(), "constraints", cl, do)
	if !errors.Is(err, fault) {
		t.Fatalf("expected the terminal fault unchanged, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 attempt for a non-transient error, got %d", calls)
	}
}

func TestRunDDLPhaseWithReparentRetry_NoClassifierSingleAttempt(t *testing.T) {
	shrinkDDLRetryEnvelope(t)
	// A classifierSrc that does NOT implement ir.TransientClassifier ⇒ one
	// attempt, terminal on any error (pre-ADR-0114). nil works too.
	fault := errors.New("any error")
	calls := 0
	do := func(context.Context) error { calls++; return fault }

	err := RunDDLPhaseWithReparentRetry(context.Background(), "indexes", nil, do)
	if !errors.Is(err, fault) {
		t.Fatalf("expected the fault unchanged, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 attempt without a classifier, got %d", calls)
	}
}

func TestRunDDLPhaseWithReparentRetry_ExhaustionIsLoudAndWraps(t *testing.T) {
	shrinkDDLRetryEnvelope(t)
	// Always-transient + never-succeeds ⇒ the loop must surface a LOUD
	// terminal error after the wall-clock bound, wrapping the last error
	// (never silent, never infinite).
	transient := errors.New("57P03: the database system is not accepting connections")
	cl := fakeDDLClassifier{isTransient: func(error) bool { return true }}
	ddlPhaseRetryMaxWall = 50 * time.Millisecond // tighten so the test is quick
	calls := 0
	do := func(context.Context) error { calls++; return transient }

	err := RunDDLPhaseWithReparentRetry(context.Background(), "indexes", cl, do)
	if err == nil {
		t.Fatal("expected a loud terminal error on exhaustion, got nil")
	}
	if !errors.Is(err, transient) {
		t.Fatalf("exhaustion error must wrap the last transient, got %v", err)
	}
	if calls < 2 {
		t.Fatalf("expected at least 2 attempts before exhaustion, got %d", calls)
	}
}

func TestRunDDLPhaseWithReparentRetry_CtxCancelUnwinds(t *testing.T) {
	shrinkDDLRetryEnvelope(t)
	transient := errors.New("57P01")
	cl := fakeDDLClassifier{isTransient: func(error) bool { return true }}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	do := func(context.Context) error {
		calls++
		if calls == 1 {
			cancel() // cancel after the first attempt so the backoff select sees Done
		}
		return transient
	}
	err := RunDDLPhaseWithReparentRetry(ctx, "indexes", cl, do)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on cancel during backoff, got %v", err)
	}
}

func TestRunDDLPhaseWithReparentRetry_SuccessFirstTryNoClassifierCall(t *testing.T) {
	shrinkDDLRetryEnvelope(t)
	// The happy path: do succeeds first try, the classifier is never even
	// consulted (the common untroubled case adds no overhead).
	classifierCalled := false
	cl := fakeDDLClassifier{isTransient: func(error) bool { classifierCalled = true; return false }}
	calls := 0
	do := func(context.Context) error { calls++; return nil }

	if err := RunDDLPhaseWithReparentRetry(context.Background(), "views", cl, do); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 attempt, got %d", calls)
	}
	if classifierCalled {
		t.Fatal("classifier must not be consulted when the first attempt succeeds")
	}
}
