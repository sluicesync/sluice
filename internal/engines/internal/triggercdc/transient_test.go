// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package triggercdc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"syscall"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// timeoutErr is a net.Error whose Timeout() is true — the structured shape a
// dial/response-header deadline produces.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "some dial deadline" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return false }

// TestIsTransientTransportError pins the exact transient shape set. Widening the
// retry surface (or a stdlib/driver rewording silently narrowing it back to
// terminal, which is how a routine blip killed a multi-day soak on 2026-07-22)
// fails this pin rather than slipping in.
func TestIsTransientTransportError(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		transient bool
	}{
		// --- structured ---
		{"io.EOF", io.EOF, true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, true},
		{"ECONNRESET", syscall.ECONNRESET, true},
		{"ECONNREFUSED", syscall.ECONNREFUSED, true},
		{"EPIPE", syscall.EPIPE, true},
		{"net.Error with Timeout()", timeoutErr{}, true},
		{"wrapped structured shape stays reachable", fmt.Errorf("d1: query database %q: %w", "db", io.EOF), true},

		// --- text fallback (observed / no reliable structured form) ---
		{
			"TLS handshake timeout (OBSERVED 2026-07-22)",
			fmt.Errorf(`d1: query database "x": Post "https://api": net/http: TLS handshake timeout`), true,
		},
		{"connection reset by peer", errors.New("read tcp 1.2.3.4:5: connection reset by peer"), true},
		{"broken pipe", errors.New("write tcp: broken pipe"), true},
		{"i/o timeout", errors.New("dial tcp 1.2.3.4:5: i/o timeout"), true},
		{"server closed idle connection", errors.New("http: server closed idle connection"), true},
		{"temporary DNS failure", errors.New("lookup api: temporary failure in name resolution"), true},

		// --- terminal by design ---
		{"nil", nil, false},
		{
			"no such host (operator error — fail fast)",
			errors.New(`Post "https://typo": dial tcp: lookup typo: no such host`), false,
		},
		{"auth failure", errors.New(`d1: query database "x" failed: HTTP 401: unauthorized`), false},
		{"malformed statement", errors.New(`d1: query database "x" refused: near "SELCT": syntax error`), false},
		{"decode fault", errors.New(`d1: decode response from database "x": invalid character`), false},
		{"missing change-log table", errors.New("no such table: sluice_change_log"), false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := IsTransientTransportError(c.err); got != c.transient {
				t.Errorf("IsTransientTransportError(%v) = %v, want %v", c.err, got, c.transient)
			}
		})
	}
}

// TestRetriableHTTPStatus pins the retriable status set. 500 is deliberately
// included — the observed D1 CDC-killer was a plain `HTTP 500 internal error`.
// A 4xx that means the REQUEST is wrong stays terminal so a bad token or a
// missing database fails loudly instead of burning the retry budget.
func TestRetriableHTTPStatus(t *testing.T) {
	retriable := []int{408, 429, 500, 502, 503, 504}
	terminal := []int{200, 201, 301, 400, 401, 403, 404, 405, 409, 410, 422, 501}

	for _, code := range retriable {
		if !RetriableHTTPStatus(code) {
			t.Errorf("RetriableHTTPStatus(%d) = false, want true (transient server-side shape)", code)
		}
	}
	for _, code := range terminal {
		if RetriableHTTPStatus(code) {
			t.Errorf("RetriableHTTPStatus(%d) = true, want false (retrying masks an operator error)", code)
		}
	}
}

// ClassifyTransient must produce something the pipeline's interface-only
// classifier (pipeline.classifyRetriable → errors.As for ir.RetriableError)
// actually matches — that interface match is the ENTIRE reason an unclassified
// transient used to terminate the stream.
func TestClassifyTransient_SatisfiesPipelineRetrySurface(t *testing.T) {
	t.Run("transient is wrapped and matches ir.RetriableError", func(t *testing.T) {
		in := fmt.Errorf("sqlite-trigger: poll: %w",
			fmt.Errorf(`d1: query database "x": %w`, &url.Error{Op: "Post", URL: "https://api", Err: timeoutErr{}}))
		got := ClassifyTransient(in)

		var re ir.RetriableError
		if !errors.As(got, &re) || !re.Retriable() {
			t.Fatalf("ClassifyTransient did not produce a pipeline-retriable error: %v", got)
		}
		// The original chain must stay reachable for diagnostics.
		var ue *url.Error
		if !errors.As(got, &ue) {
			t.Errorf("ClassifyTransient lost the underlying *url.Error chain")
		}
	})

	t.Run("terminal shape is returned unchanged", func(t *testing.T) {
		in := errors.New(`d1: query database "x" failed: HTTP 401: unauthorized`)
		got := ClassifyTransient(in)
		// Deliberate IDENTITY comparison, not errors.Is: the contract is that a
		// terminal error is returned UNCHANGED (same instance). errors.Is would
		// still be true if ClassifyTransient wrapped it, which is exactly the
		// regression this asserts against.
		//nolint:errorlint // identity check is the assertion; see comment above
		if got != in {
			t.Errorf("ClassifyTransient rewrapped a terminal error: got %v", got)
		}
		var re ir.RetriableError
		if errors.As(got, &re) {
			t.Errorf("terminal error must NOT satisfy ir.RetriableError")
		}
	})

	t.Run("nil in, nil out", func(t *testing.T) {
		if got := ClassifyTransient(nil); got != nil {
			t.Errorf("ClassifyTransient(nil) = %v, want nil", got)
		}
	})
}

// AsTransient wraps unconditionally — it is for callers holding a STRUCTURED
// signal (an HTTP status) the error text does not carry.
func TestAsTransient(t *testing.T) {
	base := fmt.Errorf(`d1: query database "x" failed: HTTP 500: internal error`)
	got := AsTransient(base)

	var re ir.RetriableError
	if !errors.As(got, &re) || !re.Retriable() {
		t.Fatalf("AsTransient did not produce a retriable error: %v", got)
	}
	if !errors.Is(got, base) {
		t.Errorf("AsTransient lost the wrapped cause")
	}
	if got := AsTransient(nil); got != nil {
		t.Errorf("AsTransient(nil) = %v, want nil", got)
	}
}

// Guard: net.Error is the interface the structured timeout check relies on.
var _ net.Error = timeoutErr{}
