// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// A non-VStream flavor (vanilla MySQL) can't be sharded, so DiscoverShards
// must return (nil, nil) WITHOUT connecting — this keeps the Bug 152
// cross-shard preflight free for the common case. We pass a DSN that would
// fail to dial if it were used; the nil error proves no connection was made.
func TestEngine_DiscoverShards_VanillaNoOp(t *testing.T) {
	eng := Engine{Flavor: FlavorVanilla}
	shards, err := eng.DiscoverShards(context.Background(), "u:p@tcp(203.0.113.1:3306)/db")
	if err != nil {
		t.Fatalf("DiscoverShards(vanilla) = err %v; want nil (no connection attempted)", err)
	}
	if shards != nil {
		t.Errorf("DiscoverShards(vanilla) = %v; want nil (vanilla MySQL is not sharded)", shards)
	}
}

// Compile-time + behavioral: the Engine satisfies ir.ShardDiscoverer.
func TestEngine_ImplementsShardDiscoverer(_ *testing.T) {
	var _ ir.ShardDiscoverer = Engine{}
}

// withFastShardDiscoveryBackoff shrinks the retry-on-empty backoff to
// near-zero for the duration of a test (restored on cleanup) so the
// always-empty exhaustion path runs in microseconds, not seconds.
func withFastShardDiscoveryBackoff(t *testing.T) {
	t.Helper()
	base, cap0 := shardDiscoveryRetryBackoffBaseVar, shardDiscoveryRetryBackoffCapVar
	shardDiscoveryRetryBackoffBaseVar = time.Microsecond
	shardDiscoveryRetryBackoffCapVar = time.Microsecond
	t.Cleanup(func() {
		shardDiscoveryRetryBackoffBaseVar = base
		shardDiscoveryRetryBackoffCapVar = cap0
	})
}

// Retry-on-empty (2a): a transient empty read for the target keyspace is
// retried, and a subsequent populated read wins — the fresh-PlanetScale-
// keyspace case that used to force an operator relaunch. The non-empty result
// is returned verbatim; discovery is invoked exactly as many times as it took
// to see shards (here: twice), never the full budget.
func TestRetryShardDiscoveryOnEmpty_EmptyThenPopulated(t *testing.T) {
	withFastShardDiscoveryBackoff(t)
	const ks = "app"
	calls := 0
	discover := func(context.Context) (map[string][]string, error) {
		calls++
		if calls < 2 {
			return map[string][]string{}, nil // transient: keyspace not served yet
		}
		return map[string][]string{ks: {"-80", "80-"}}, nil
	}
	got, err := retryShardDiscoveryOnEmpty(context.Background(), ks, discover)
	if err != nil {
		t.Fatalf("retryShardDiscoveryOnEmpty = err %v; want nil", err)
	}
	if want := []string{"-80", "80-"}; len(got[ks]) != len(want) {
		t.Fatalf("shards for %q = %v; want %v", ks, got[ks], want)
	}
	if calls != 2 {
		t.Errorf("discover invoked %d times; want 2 (retried once, then populated)", calls)
	}
}

// The non-empty FIRST read is the fast path: it returns immediately with no
// retry and therefore no added latency on a healthy keyspace.
func TestRetryShardDiscoveryOnEmpty_FastPathNoRetry(t *testing.T) {
	const ks = "app"
	calls := 0
	discover := func(context.Context) (map[string][]string, error) {
		calls++
		return map[string][]string{ks: {"-"}}, nil
	}
	got, err := retryShardDiscoveryOnEmpty(context.Background(), ks, discover)
	if err != nil {
		t.Fatalf("retryShardDiscoveryOnEmpty = err %v; want nil", err)
	}
	if len(got[ks]) != 1 {
		t.Fatalf("shards for %q = %v; want [-]", ks, got[ks])
	}
	if calls != 1 {
		t.Errorf("discover invoked %d times; want 1 (fast path, no retry)", calls)
	}
}

// A genuinely-unserved keyspace stays empty after the cap: the loop exhausts
// its budget (exactly shardDiscoveryRetryAttemptsVar attempts) and hands the
// empty result back WITHOUT an error of its own, so the caller's existing loud
// failure fires. resolveVStreamShards turns this empty result into its
// "returned no shards" refusal; the control-keyspace path into a "" fallback.
func TestRetryShardDiscoveryOnEmpty_AlwaysEmptyExhaustsBudget(t *testing.T) {
	withFastShardDiscoveryBackoff(t)
	const ks = "missing"
	calls := 0
	discover := func(context.Context) (map[string][]string, error) {
		calls++
		// A served-but-different keyspace is present; the target isn't.
		return map[string][]string{"other": {"-"}}, nil
	}
	got, err := retryShardDiscoveryOnEmpty(context.Background(), ks, discover)
	if err != nil {
		t.Fatalf("retryShardDiscoveryOnEmpty = err %v; want nil (empty is not the loop's own error)", err)
	}
	if len(got[ks]) != 0 {
		t.Fatalf("shards for %q = %v; want empty", ks, got[ks])
	}
	if calls != shardDiscoveryRetryAttemptsVar {
		t.Errorf("discover invoked %d times; want the full budget of %d", calls, shardDiscoveryRetryAttemptsVar)
	}
}

// A discover ERROR (connection/parse failure) is NOT the transient-empty case:
// it is returned immediately, unretried.
func TestRetryShardDiscoveryOnEmpty_DiscoverErrorNotRetried(t *testing.T) {
	withFastShardDiscoveryBackoff(t)
	sentinel := errors.New("dial tcp: connection refused")
	calls := 0
	discover := func(context.Context) (map[string][]string, error) {
		calls++
		return nil, sentinel
	}
	_, err := retryShardDiscoveryOnEmpty(context.Background(), "app", discover)
	if !errors.Is(err, sentinel) {
		t.Fatalf("retryShardDiscoveryOnEmpty = err %v; want the discover error %v", err, sentinel)
	}
	if calls != 1 {
		t.Errorf("discover invoked %d times; want 1 (errors are not retried)", calls)
	}
}

// ctx cancellation during a backoff wait aborts promptly with ctx.Err() rather
// than riding out the remaining budget.
func TestRetryShardDiscoveryOnEmpty_CtxCancelAborts(t *testing.T) {
	// A large backoff would block indefinitely; cancellation must win.
	base, cap0 := shardDiscoveryRetryBackoffBaseVar, shardDiscoveryRetryBackoffCapVar
	shardDiscoveryRetryBackoffBaseVar = time.Hour
	shardDiscoveryRetryBackoffCapVar = time.Hour
	t.Cleanup(func() {
		shardDiscoveryRetryBackoffBaseVar = base
		shardDiscoveryRetryBackoffCapVar = cap0
	})

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	discover := func(context.Context) (map[string][]string, error) {
		calls++
		cancel() // cancel before the first backoff wait
		return map[string][]string{}, nil
	}
	done := make(chan error, 1)
	go func() {
		_, err := retryShardDiscoveryOnEmpty(ctx, "app", discover)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("retryShardDiscoveryOnEmpty = err %v; want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retryShardDiscoveryOnEmpty did not abort promptly on ctx cancel")
	}
	if calls != 1 {
		t.Errorf("discover invoked %d times; want 1 (cancel during first backoff)", calls)
	}
}

// shardDiscoveryBackoff doubles from the base and clamps at the cap.
func TestShardDiscoveryBackoff(t *testing.T) {
	base, cap0 := shardDiscoveryRetryBackoffBaseVar, shardDiscoveryRetryBackoffCapVar
	shardDiscoveryRetryBackoffBaseVar = 500 * time.Millisecond
	shardDiscoveryRetryBackoffCapVar = 2 * time.Second
	t.Cleanup(func() {
		shardDiscoveryRetryBackoffBaseVar = base
		shardDiscoveryRetryBackoffCapVar = cap0
	})
	want := []time.Duration{
		500 * time.Millisecond, // attempt 1
		time.Second,            // attempt 2
		2 * time.Second,        // attempt 3 (2s, exactly the cap)
		2 * time.Second,        // attempt 4 (clamped)
	}
	for i, w := range want {
		if got := shardDiscoveryBackoff(i + 1); got != w {
			t.Errorf("shardDiscoveryBackoff(%d) = %s; want %s", i+1, got, w)
		}
	}
}
