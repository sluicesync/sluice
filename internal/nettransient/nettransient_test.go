// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package nettransient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
)

// TestTextShapes_PinDown pins the corpus BYTE-FOR-BYTE (the
// reparentRetriableSubstrings pin-down discipline): widening or
// narrowing the shared retry surface must be a deliberate act that
// updates this pin — and the per-consumer parity tests — in the same
// diff, never an incidental edit.
func TestTextShapes_PinDown(t *testing.T) {
	want := []string{
		"invalid connection",
		"tls handshake timeout",
		"connection reset by peer",
		"connection refused",
		"connection timed out",
		"broken pipe",
		"i/o timeout",
		"unexpected eof",
		"use of closed network connection",
		"forcibly closed by the remote host",
		"wsarecv:",
		"wsasend:",
		"connectex:",
		"actively refused",
		"server closed idle connection",
		"temporary failure in name resolution",
	}
	if len(TextShapes) != len(want) {
		t.Fatalf("TextShapes has %d entries; want %d — update this pin AND every consumer parity test together", len(TextShapes), len(want))
	}
	for i, w := range want {
		if TextShapes[i] != w {
			t.Errorf("TextShapes[%d] = %q; want %q", i, TextShapes[i], w)
		}
	}
}

// TestTextShapes_CorpusHygiene — the matcher lower-cases the message
// before comparing, so a mixed-case (or duplicate/empty) corpus entry
// would silently never match.
func TestTextShapes_CorpusHygiene(t *testing.T) {
	seen := make(map[string]bool, len(TextShapes))
	for _, s := range TextShapes {
		if s == "" {
			t.Error("empty corpus entry would match everything")
		}
		if s != strings.ToLower(s) {
			t.Errorf("corpus entry %q is not lower-case — it can never match the lowered message", s)
		}
		if seen[s] {
			t.Errorf("duplicate corpus entry %q", s)
		}
		seen[s] = true
	}
}

// timeoutNetError is a minimal net.Error whose Timeout() reports true —
// the structured shape a dial/response-header deadline produces.
type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "some dial deadline" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return false }

var _ net.Error = timeoutNetError{}

// TestIsTransientShape pins the matcher over the structured legs, a
// framed sample of every corpus entry, and the terminal-by-design
// negatives.
func TestIsTransientShape(t *testing.T) {
	structured := []error{
		io.EOF,
		io.ErrUnexpectedEOF,
		syscall.ECONNRESET,
		syscall.ECONNREFUSED,
		syscall.EPIPE,
		syscall.ETIMEDOUT,
		timeoutNetError{},
		fmt.Errorf("open: %w", io.ErrUnexpectedEOF),
		fmt.Errorf("dial: %w", syscall.ECONNREFUSED),
	}
	for _, err := range structured {
		if !IsTransientShape(err) {
			t.Errorf("IsTransientShape(%v) = false; want true (structured leg)", err)
		}
	}
	// Every corpus entry, framed the way drivers frame them (mixed case
	// exercises the lowering).
	for _, s := range TextShapes {
		framed := errors.New("driver: exec: " + strings.ToUpper(s[:1]) + s[1:])
		if !IsTransientShape(framed) {
			t.Errorf("IsTransientShape(%q) = false; want true (corpus entry %q)", framed, s)
		}
	}
	terminal := []error{
		nil,
		errors.New("dial tcp: lookup db.example.com: no such host"), // typo'd endpoint: operator error
		errors.New("Error 1045: Access denied for user 'x'@'%'"),
		errors.New(`FATAL: password authentication failed for user "app" (SQLSTATE 28P01)`),
		errors.New("invalid DSN: missing the slash dividing the database name"),
		errors.New("SLUICE-E-CDC-PUBLICATION-SCOPE-CONFLICT: narrowing rescope refused"),
		context.Canceled, // clean shutdown must never be absorbed
		errors.New("some random failure"),
	}
	for _, err := range terminal {
		if IsTransientShape(err) {
			t.Errorf("IsTransientShape(%v) = true; want false (terminal-by-design)", err)
		}
	}
}

// TestIsTransientShape_DeadlineExceededPosture is the ARCH-5 posture
// pin (audit 2026-07-23): context.DeadlineExceeded implements net.Error
// with Timeout() == true, so it classifies TRANSIENT — deliberately
// (per-dial/per-exec deadlines against a briefly-down peer arrive
// exactly this way; see the matcher's doc comment for why the shutdown
// cost is bounded to one log line). context.Canceled stays terminal, so
// a clean shutdown is never absorbed. Flipping either verdict is a
// posture change that must rewrite this pin and the matcher's comment
// together.
func TestIsTransientShape_DeadlineExceededPosture(t *testing.T) {
	if !IsTransientShape(context.DeadlineExceeded) {
		t.Error("IsTransientShape(context.DeadlineExceeded) = false; the documented posture is TRANSIENT (dial/exec deadlines to a restarting peer)")
	}
	if !IsTransientShape(fmt.Errorf("pipeline: open target change applier: %w", context.DeadlineExceeded)) {
		t.Error("wrapped context.DeadlineExceeded must stay reachable through the net.Error leg")
	}
	if IsTransientShape(context.Canceled) {
		t.Error("IsTransientShape(context.Canceled) = true; a clean shutdown must never classify transient")
	}
	if IsTransientShape(fmt.Errorf("apply: %w", context.Canceled)) {
		t.Error("wrapped context.Canceled must stay terminal")
	}
}
