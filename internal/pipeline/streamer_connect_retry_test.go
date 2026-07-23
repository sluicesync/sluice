// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the connect-phase transient fall-through in runWithRetry (the
// 2026-07-22 scale-soak incident): a transient network failure while a
// retry attempt RE-ESTABLISHES its connections must ride the existing
// budget + backoff instead of killing the stream, while every terminal
// shape — auth, DSN parse, coded refusals, unmarked errors — stays as loud
// as before.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"syscall"
	"testing"
	"time"
)

// incidentOpenErr reproduces the exact failure that killed the scale-soak
// sync: the target applier reopen dying on go-sql-driver's dead-pool-conn
// post-mortem, wrapped exactly as openApplier wraps it.
func incidentOpenErr() error {
	return connectHint(fmt.Errorf("pipeline: open target change applier: %w",
		errors.New("mysql: ping: invalid connection")))
}

func TestConnectRetry_TransientOpenFailureRetries(t *testing.T) {
	s := fastRetryStreamer(t, true /* CDC phase: durable position exists */, 8)
	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		if calls == 1 {
			return incidentOpenErr()
		}
		return nil // the next attempt's warm resume succeeds (the incident's ground truth)
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error; want the connect transient absorbed by a retry: %v", err)
	}
	if calls != 2 {
		t.Fatalf("runOnce called %d times; want 2 (failed reopen + successful warm resume)", calls)
	}
}

func TestConnectRetry_BudgetExhaustionStaysLoud(t *testing.T) {
	// The loud-failure floor: a target that can NEVER be reached exhausts
	// the consecutive budget and fails, never loops forever.
	s := fastRetryStreamer(t, true, 3)
	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		return incidentOpenErr()
	}
	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("Run succeeded; want budget exhaustion")
	}
	if !strings.Contains(err.Error(), "retry budget exhausted") {
		t.Errorf("error %q missing the budget-exhaustion prefix", err)
	}
	if !strings.Contains(err.Error(), "invalid connection") {
		t.Errorf("error %q lost the underlying cause", err)
	}
	if calls != 3 {
		t.Fatalf("runOnce called %d times; want exactly the budget (3)", calls)
	}
}

func TestConnectRetry_TerminalShapesStayTerminal(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"marked but auth-shaped", connectHint(fmt.Errorf("pipeline: open target change applier: %w",
			errors.New("mysql: ping: Error 1045: Access denied for user 'x'")))},
		{"marked but DSN parse", connectHint(errors.New("invalid DSN: missing the slash dividing the database name"))},
		{"marked but coded refusal", connectHint(errors.New("pipeline: ensure publication scope: SLUICE-E-CDC-PUBLICATION-SCOPE-CONFLICT: narrowing rescope refused"))},
		{"transient shape but UNMARKED", fmt.Errorf("pipeline: apply: %w",
			errors.New("read tcp 1.2.3.4:1->5.6.7.8:2: connection reset by peer"))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := fastRetryStreamer(t, true, 8)
			var calls int
			s.runOnceFn = func(context.Context) error {
				calls++
				return c.err
			}
			err := s.Run(context.Background())
			if !errors.Is(err, c.err) {
				t.Fatalf("Run returned %v; want the original terminal error", err)
			}
			if calls != 1 {
				t.Fatalf("runOnce called %d times; want 1 (terminal, no retry)", calls)
			}
		})
	}
}

// timeoutNetError is a minimal net.Error whose Timeout() reports true.
type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "dial tcp: lookup deadline" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

func TestIsTransientNetworkShape(t *testing.T) {
	transient := []error{
		errors.New("mysql: ping: invalid connection"),
		errors.New("net/http: TLS handshake timeout"),
		errors.New("read tcp 1.2.3.4:1->5.6.7.8:2: connection reset by peer"),
		errors.New("dial tcp 5.6.7.8:3306: connect: connection refused"),
		errors.New("write tcp 1.2.3.4:1->5.6.7.8:2: wsasend: An established connection was aborted"),
		errors.New("read tcp 1.2.3.4:1->5.6.7.8:2: wsarecv: An existing connection was forcibly closed by the remote host"),
		fmt.Errorf("open: %w", io.ErrUnexpectedEOF),
		fmt.Errorf("dial: %w", syscall.ECONNREFUSED),
		fmt.Errorf("connect: %w", timeoutNetError{}),
	}
	for _, err := range transient {
		if !isTransientNetworkShape(err) {
			t.Errorf("isTransientNetworkShape(%q) = false; want true", err)
		}
	}
	terminal := []error{
		nil,
		errors.New("dial tcp: lookup db.example.com: no such host"), // wrong endpoint: operator error
		errors.New("Error 1045: Access denied for user 'x'@'%'"),
		errors.New("invalid DSN: missing the slash dividing the database name"),
		errors.New("SLUICE-E-CDC-PUBLICATION-SCOPE-CONFLICT: narrowing rescope refused"),
		errors.New("pq: password authentication failed for user \"x\""),
	}
	for _, err := range terminal {
		if isTransientNetworkShape(err) {
			t.Errorf("isTransientNetworkShape(%v) = true; want false (terminal-by-design)", err)
		}
	}
}

func TestConnectHint_TransparentWrap(t *testing.T) {
	inner := errors.New("mysql: ping: invalid connection")
	wrapped := connectHint(fmt.Errorf("pipeline: open target change applier: %w", inner))
	// The marker must be presentation-transparent: same operator-facing
	// text discipline as WrapWithHint, and the chain stays traversable.
	if !errors.Is(wrapped, inner) {
		t.Error("connectHint broke the Unwrap chain to the underlying cause")
	}
	if !strings.Contains(wrapped.Error(), "open target change applier") {
		t.Errorf("connectHint changed the message: %q", wrapped.Error())
	}
	if connectHint(nil) != nil {
		t.Error("connectHint(nil) must be nil")
	}
}

// TestConnectRetry_BackoffWithoutEngineHint pins the re==nil guard: the
// connect fall-through reaches computeRetryBackoff with no engine
// classifier, so the policy's own schedule must be used (a nil-deref here
// was the naive implementation's failure mode).
func TestConnectRetry_BackoffWithoutEngineHint(t *testing.T) {
	s := fastRetryStreamer(t, true, 2)
	s.ApplyRetryBackoffBase = time.Nanosecond
	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		return incidentOpenErr()
	}
	// Two attempts: the first failure must survive the backoff computation
	// (no panic) and the second exhausts the budget of 2.
	if err := s.Run(context.Background()); err == nil {
		t.Fatal("want budget exhaustion after 2 attempts")
	}
	if calls != 2 {
		t.Fatalf("runOnce called %d times; want 2", calls)
	}
}
