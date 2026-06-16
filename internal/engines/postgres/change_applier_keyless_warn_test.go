// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Unit tests pinning the ADR-0089 keyless-guard WARN wording (Bug 143).
//
// The bug: the original WARN claimed single-row apply meant "crash-replay
// cannot duplicate rows". That is false — keyless CDC is at-least-once,
// because crash-resume granularity is the SOURCE TRANSACTION (the GTID/LSN
// advances only at its commit), so every keyless row in one interrupted
// source transaction replays together. These pins keep the corrected,
// honest wording from silently regressing back to the over-promise.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// captureKeylessWarn installs a WARN-level JSON slog handler into a buffer
// for the test's duration, restoring the previous default on cleanup.
func captureKeylessWarn(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// TestWarnKeylessOnce_HonestWording: the WARN must state at-least-once
// semantics and must NOT contain the old false "cannot duplicate" claim.
func TestWarnKeylessOnce_HonestWording(t *testing.T) {
	buf := captureKeylessWarn(t)
	a := &ChangeApplier{}
	a.warnKeylessOnce(context.Background(), "public.events")

	out := buf.String()
	if strings.Contains(out, "cannot duplicate") {
		t.Errorf("WARN must not claim crash-replay cannot duplicate rows (Bug 143); got %q", out)
	}
	for _, want := range []string{"at-least-once", "not idempotent", "public.events", "ADR-0089"} {
		if !strings.Contains(out, want) {
			t.Errorf("WARN should contain %q; got %q", want, out)
		}
	}
}

// TestWarnKeylessOnce_OncePerTable: the WARN fires once per keyless table,
// not once per change — flooding would mask the signal.
func TestWarnKeylessOnce_OncePerTable(t *testing.T) {
	buf := captureKeylessWarn(t)
	a := &ChangeApplier{}
	for i := 0; i < 5; i++ {
		a.warnKeylessOnce(context.Background(), "public.events")
	}
	if got := strings.Count(buf.String(), "\n"); got != 1 {
		t.Errorf("expected exactly 1 WARN across 5 calls for one table; got %d:\n%s", got, buf.String())
	}

	a.warnKeylessOnce(context.Background(), "public.audit")
	if got := strings.Count(buf.String(), "\n"); got != 2 {
		t.Errorf("expected a second WARN for a second keyless table; got %d:\n%s", got, buf.String())
	}
}
