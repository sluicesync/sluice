// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// timeoutNetErr is a net.Error whose Timeout() is true — the shape the
// classifier must treat as a transient (covers the Windows wsarecv
// WSAETIMEDOUT case surfacing as a net timeout). Its message deliberately
// contains NO transient/permanent keyword so the test exercises the
// net.Error branch, not a substring match.
type timeoutNetErr struct{}

func (timeoutNetErr) Error() string   { return "network op timed out" }
func (timeoutNetErr) Timeout() bool   { return true }
func (timeoutNetErr) Temporary() bool { return true }

// TestIsRetriableChunkOpenError_Classification pins the full transient vs
// permanent matrix — the connection-OPEN analog of the source-read
// classifier test. Every transient shape must be retriable; every permanent
// fault must fail fast; unknown + nil must stay fatal (conservative).
func TestIsRetriableChunkOpenError_Classification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		// --- transient connection-drop class → retriable ---
		{"driver.ErrBadConn", driver.ErrBadConn, true},
		{"wrapped driver.ErrBadConn", fmt.Errorf("mysql: ping: %w", driver.ErrBadConn), true},
		{"invalid connection text", errors.New("mysql: ping: invalid connection"), true},
		{"io.EOF", io.EOF, true},
		{"wrapped io.EOF", fmt.Errorf("read: %w", io.EOF), true},
		{"net.Error timeout", timeoutNetErr{}, true},
		{"connection reset", errors.New("read tcp: connection reset by peer"), true},
		{"wsarecv did not properly respond", errors.New("wsarecv: A connection attempt failed because the connected party did not properly respond after a period of time"), true},
		{"broken pipe", errors.New("write: broken pipe"), true},
		{"connection refused", errors.New("dial tcp: connect: connection refused"), true},
		{"bad connection", errors.New("driver: bad connection"), true},
		{"unexpected EOF", errors.New("unexpected EOF"), true},
		{"wrapped ir.RetriableError", fmt.Errorf("open: %w", fakeRetriableErr{msg: "vttablet: unavailable"}), true},
		// Case-insensitive: an upper-cased transient shape still matches.
		{"upper-cased invalid connection", errors.New("MySQL: Ping: INVALID CONNECTION"), true},

		// --- permanent faults → fail fast (checked BEFORE transient text) ---
		{"access denied", errors.New("Error 1045: Access denied for user 'x'@'y'"), false},
		{"unknown database", errors.New("Error 1049: Unknown database 'nope'"), false},
		{"invalid DSN", errors.New("invalid DSN: missing host"), false},
		{"authentication failed", errors.New("pq: Authentication failed for user"), false},
		{"parseDSN", errors.New("mysql: parseDSN: invalid connection string"), false},
		{"permission denied", errors.New("permission denied for table foo"), false},
		// Permanent substring must WIN over a co-occurring transient substring.
		{"permanent beats transient text", errors.New("Access denied; connection reset by peer"), false},

		// --- unknown + nil → conservative fatal ---
		{"nil", nil, false},
		{"unknown table-not-found", errors.New("table not found: bar"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetriableChunkOpenError(tc.err); got != tc.want {
				t.Errorf("isRetriableChunkOpenError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// withFastChunkOpenRetry shrinks the chunk-open retry envelope so the suite
// never actually sleeps and a bounded give-up hits quickly, restoring the
// production values on cleanup. attempts caps the loud give-up deterministically.
func withFastChunkOpenRetry(t *testing.T, attempts int) {
	t.Helper()
	base, capDur, at, wall := chunkOpenRetryBackoffBase, chunkOpenRetryBackoffCap, chunkOpenRetryAttempts, chunkOpenRetryMaxWall
	chunkOpenRetryBackoffBase = time.Microsecond
	chunkOpenRetryBackoffCap = time.Microsecond
	chunkOpenRetryAttempts = attempts
	chunkOpenRetryMaxWall = time.Hour // let the attempt cap, not the clock, bound the test
	t.Cleanup(func() {
		chunkOpenRetryBackoffBase = base
		chunkOpenRetryBackoffCap = capDur
		chunkOpenRetryAttempts = at
		chunkOpenRetryMaxWall = wall
	})
}

// noSlot is the isSlotExhausted predicate for tests that exercise the
// transient path only — no error is ever slot exhaustion.
func noSlot(error) bool { return false }

// failSlotBackoff is a slotBackoff callback that fails the test if invoked —
// the transient-path tests must never route through the slot-exhaustion seam.
func failSlotBackoff(t *testing.T) func(context.Context, int) (time.Duration, error) {
	return func(context.Context, int) (time.Duration, error) {
		t.Fatal("slotBackoff called on a non-slot-exhaustion path")
		return 0, nil
	}
}

// TestOpenChunkConnWithRetry_TransientThenSucceeds pins the happy reconnect:
// a transient connection drop on the first N opens backs off and retries, and
// the (N+1)th open succeeds — no whole-migrate abort.
func TestOpenChunkConnWithRetry_TransientThenSucceeds(t *testing.T) {
	withFastChunkOpenRetry(t, 100)

	const failN = 3
	var opens int
	open := func(context.Context) (ir.RowReader, ir.RowWriter, error) {
		opens++
		if opens <= failN {
			return nil, nil, errors.New("mysql: ping: invalid connection")
		}
		return noopRowReader{}, noopRowWriter{}, nil
	}

	rdr, wr, err := openChunkConnWithRetry(context.Background(), 7, "bench", open, noSlot, failSlotBackoff(t))
	if err != nil {
		t.Fatalf("openChunkConnWithRetry after transient drops: %v", err)
	}
	if rdr == nil || wr == nil {
		t.Fatal("expected a non-nil reader/writer on eventual success")
	}
	if opens != failN+1 {
		t.Errorf("opens = %d, want %d (%d transient + 1 success)", opens, failN+1, failN)
	}
}

// TestOpenChunkConnWithRetry_AlwaysTransientGivesUpLoudly pins the bounded
// give-up: an endpoint that keeps dropping surfaces a LOUD terminal error
// wrapping the most recent transient after the budget — never an infinite spin.
func TestOpenChunkConnWithRetry_AlwaysTransientGivesUpLoudly(t *testing.T) {
	withFastChunkOpenRetry(t, 5)

	lastErr := errors.New("read tcp: connection reset by peer")
	var opens int
	open := func(context.Context) (ir.RowReader, ir.RowWriter, error) {
		opens++
		return nil, nil, lastErr
	}

	_, _, err := openChunkConnWithRetry(context.Background(), 7, "bench", open, noSlot, failSlotBackoff(t))
	if err == nil {
		t.Fatal("expected a loud terminal error on a permanently-dropping endpoint, got nil")
	}
	if !errors.Is(err, lastErr) {
		t.Errorf("give-up error should wrap the most recent transient; got %v", err)
	}
	// attempts=5 → the loop gives up when transientTry reaches 5 (opens: the
	// initial + 4 retries = 5). Asserts the give-up is bounded, not infinite.
	if opens != 5 {
		t.Errorf("opens = %d, want 5 (bounded give-up, no infinite loop)", opens)
	}
}

// TestOpenChunkConnWithRetry_PermanentFailsImmediately pins the safety
// property: a permanent open fault surfaces on the FIRST open with zero
// retries — never masked as a transient blip.
func TestOpenChunkConnWithRetry_PermanentFailsImmediately(t *testing.T) {
	withFastChunkOpenRetry(t, 100)

	permErr := errors.New("Error 1045: Access denied for user 'x'@'y'")
	var opens int
	open := func(context.Context) (ir.RowReader, ir.RowWriter, error) {
		opens++
		return nil, nil, permErr
	}

	_, _, err := openChunkConnWithRetry(context.Background(), 7, "bench", open, noSlot, failSlotBackoff(t))
	if err == nil {
		t.Fatal("expected the permanent error to surface, got nil")
	}
	if !errors.Is(err, permErr) {
		t.Errorf("error should wrap the permanent open fault; got %v", err)
	}
	if opens != 1 {
		t.Errorf("opens = %d, want 1 (no retry on a permanent fault)", opens)
	}
}

// TestOpenChunkConnWithRetry_ContextCancelBreaksBackoff pins that a cancelled
// context aborts the retry backoff promptly rather than sleeping out the budget.
func TestOpenChunkConnWithRetry_ContextCancelBreaksBackoff(t *testing.T) {
	// Long backoff so the cancel, not the timer, is what ends the wait.
	base, capDur, wall := chunkOpenRetryBackoffBase, chunkOpenRetryBackoffCap, chunkOpenRetryMaxWall
	chunkOpenRetryBackoffBase = time.Hour
	chunkOpenRetryBackoffCap = time.Hour
	chunkOpenRetryMaxWall = time.Hour
	t.Cleanup(func() {
		chunkOpenRetryBackoffBase = base
		chunkOpenRetryBackoffCap = capDur
		chunkOpenRetryMaxWall = wall
	})

	ctx, cancel := context.WithCancel(context.Background())
	open := func(context.Context) (ir.RowReader, ir.RowWriter, error) {
		cancel() // trip cancellation, then hand back a transient to enter backoff
		return nil, nil, errors.New("mysql: ping: invalid connection")
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := openChunkConnWithRetry(ctx, 7, "bench", open, noSlot, failSlotBackoff(t))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("openChunkConnWithRetry did not return promptly on ctx cancel")
	}
}
