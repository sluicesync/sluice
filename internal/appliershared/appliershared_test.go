// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSchema covers the small fallback rule. The applier's configured
// schema wins; the change's source-side schema is only used when the
// applier wasn't given a default.
func TestSchema(t *testing.T) {
	if got := Schema("default_db", "source_db"); got != "default_db" {
		t.Errorf("default wins: got %q; want default_db", got)
	}
	if got := Schema("default_db", ""); got != "default_db" {
		t.Errorf("empty change schema: got %q; want default_db", got)
	}
	if got := Schema("", "source_db"); got != "source_db" {
		t.Errorf("empty default falls back to change schema: got %q; want source_db", got)
	}
}

// TestRunWithDeadline exercises the package-level watchdog that the
// engines' commitWithTimeout delegates to (Bug 56, v0.52.1). The
// watchdog race semantics need direct coverage because the production
// failure mode (tx.Commit blocked inside crypto/tls.(*Conn).Read on a
// half-closed PlanetScale destination) can't be reproduced in a unit
// test — but the race + cancel + passthrough logic is testable with
// synthetic closures.
func TestRunWithDeadline(t *testing.T) {
	t.Run("zero timeout: passthrough preserves return verbatim", func(t *testing.T) {
		sentinel := errors.New("synthetic commit failure")
		got := RunWithDeadline(0, func() error { return sentinel })
		if !errors.Is(got, sentinel) {
			t.Errorf("zero-timeout passthrough lost the original error; got %v; want %v", got, sentinel)
		}
	})

	t.Run("negative timeout: same passthrough", func(t *testing.T) {
		got := RunWithDeadline(-1*time.Second, func() error { return nil })
		if got != nil {
			t.Errorf("negative-timeout passthrough produced unexpected error: %v", got)
		}
	})

	t.Run("positive timeout: fast f returns its own value", func(t *testing.T) {
		sentinel := errors.New("fast f")
		got := RunWithDeadline(500*time.Millisecond, func() error { return sentinel })
		if !errors.Is(got, sentinel) {
			t.Errorf("fast-f race lost the original error; got %v; want %v", got, sentinel)
		}
	})

	t.Run("positive timeout: slow f trips watchdog with DeadlineExceeded", func(t *testing.T) {
		// f sleeps longer than the timeout; the watchdog must fire.
		start := time.Now()
		got := RunWithDeadline(20*time.Millisecond, func() error {
			time.Sleep(500 * time.Millisecond)
			return nil
		})
		if !errors.Is(got, context.DeadlineExceeded) {
			t.Errorf("slow-f watchdog did not return DeadlineExceeded; got %v", got)
		}
		// The wall-clock cost should match the timeout, not f's sleep.
		// Allow generous slack for scheduler jitter, especially on CI.
		elapsed := time.Since(start)
		if elapsed > 100*time.Millisecond {
			t.Errorf("watchdog took %v; expected ~20ms (cap 100ms)", elapsed)
		}
	})
}

// TestNonGeneratedRowKeys pins the moved helper directly: sorted-order
// output (the engine originals used a local insertion sort; the hoist
// switched to slices.Sorted — same lexicographic contract), the
// generated-column filter, and the nil/partial-map tolerance. The
// engine-side SQL-builder tests (TestBuildSQL_FiltersGeneratedColumns
// in both engines) keep exercising it through the real call sites.
func TestNonGeneratedRowKeys(t *testing.T) {
	row := ir.Row{"b": 1, "a": 2, "total": 3}
	colTypes := map[string]*ir.Column{
		"a":     {Name: "a", Type: ir.Integer{Width: 64}},
		"total": {Name: "total", Type: ir.Integer{Width: 64}, GeneratedExpr: "a + b"},
	}

	got := NonGeneratedRowKeys(row, colTypes)
	want := []string{"a", "b"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("generated column not filtered or order wrong: got %v; want %v", got, want)
	}

	// Nil map: every key included, still sorted.
	got = NonGeneratedRowKeys(row, nil)
	want = []string{"a", "b", "total"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("nil colTypes: got %v; want %v", got, want)
		}
	}
}

// TestTruncateToken pins the boundary: a token at exactly maxLen
// passes through verbatim; one byte over trims to maxLen-1 plus the
// ellipsis rune.
func TestTruncateToken(t *testing.T) {
	if got := TruncateToken("abcd", 4); got != "abcd" {
		t.Errorf("at-limit token modified: got %q", got)
	}
	if got := TruncateToken("abcde", 4); got != "abc…" {
		t.Errorf("over-limit token: got %q; want %q", got, "abc…")
	}
}
