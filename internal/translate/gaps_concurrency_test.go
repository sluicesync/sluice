// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// detectGaps is safe under concurrent use (audit 2026-08-01 A2).
//
// The gap matchers used to be a lazily-populated plain map whose comment said
// concurrent calls were "racy but the worst case is duplicate compilation".
// In Go that is not the worst case: the runtime detects a concurrent map read
// and map write and throws `fatal error: concurrent map read and map write`,
// which no deferred recover can catch. The process dies.
//
// The table is now built once at package initialization and never written, so
// the write that could race does not exist. This test drives the real entry
// point from many goroutines at once; under `-race` it fails on any residual
// unsynchronised write, and even without `-race` a reintroduced lazy cache
// would surface here as the fatal throw rather than as a quiet flake.

package translate

import (
	"sync"
	"testing"
)

func TestDetectGaps_ConcurrentCallersAreSafe(t *testing.T) {
	// Expressions chosen to HIT the registry — a run that matched nothing
	// would exercise the lookup but never the branch that used to write.
	exprs := []string{
		"GREATEST(a, b)",
		"LEAST(a, b)",
		"REGEXP_LIKE(col, '^x')",
		"nothing_here(col)",  // a miss, to cover the not-in-registry path
		"greatest ( a , b )", // case + whitespace variant
	}

	// Non-vacuity: at least one expression must actually MATCH, or the loop
	// below would spin over a detector that never reaches the matcher table
	// and the whole test would prove nothing.
	if got := detectGaps(exprs[0], nil); len(got) == 0 {
		t.Fatalf("%q matched no gap pattern — this test would exercise the lookup without ever "+
			"reaching the matcher, making the concurrency check vacuous", exprs[0])
	}

	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	start := make(chan struct{})
	hits := make([]int, goroutines)
	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			<-start // release together, to maximise overlap
			for range 50 {
				hits[i] += len(detectGaps(exprs[i%len(exprs)], nil))
			}
		}(i)
	}
	close(start)
	wg.Wait()

	total := 0
	for _, n := range hits {
		total += n
	}
	if total == 0 {
		t.Error("no goroutine detected any gap; the concurrent run did not exercise the matcher table")
	}
}

// TestGapMatchers_CoverTheRegistry is the anti-vacuity floor for the table:
// if it were built empty (or from the wrong slice), the concurrency test above
// would still pass — it would just be exercising the uncached fallback on
// every call and never touching the map at all.
func TestGapMatchers_CoverTheRegistry(t *testing.T) {
	if len(gapPatterns) == 0 {
		t.Fatal("gapPatterns is empty; this test and the matcher table are both vacuous")
	}
	for _, pat := range gapPatterns {
		re, ok := gapMatchers[pat.name]
		if !ok {
			t.Errorf("gapPatterns entry %q has no pre-built matcher — it would compile on every call, "+
				"and a future lazy cache would put the write back", pat.name)
			continue
		}
		if re == nil {
			t.Errorf("matcher for %q is nil", pat.name)
		}
	}
	if len(gapMatchers) != len(gapPatterns) {
		t.Errorf("gapMatchers has %d entries, gapPatterns has %d — the table and the registry have drifted",
			len(gapMatchers), len(gapPatterns))
	}
}

// TestGapMatcher_MatchesAsBefore pins that pre-building did not change what
// the regexes match. The pattern is a word-boundary call form, case-insensitive,
// tolerating whitespace before the paren.
func TestGapMatcher_MatchesAsBefore(t *testing.T) {
	re := gapMatcher("GREATEST")
	for _, want := range []string{"GREATEST(x)", "greatest(x)", "GREATEST  (x)"} {
		if !re.MatchString(want) {
			t.Errorf("%q should match", want)
		}
	}
	for _, reject := range []string{"myGREATEST(x)", "GREATESTx(y)"} {
		if re.MatchString(reject) {
			t.Errorf("%q should NOT match (word boundary)", reject)
		}
	}
}
