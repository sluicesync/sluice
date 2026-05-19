// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
	"time"
)

// TestValidateRetryFlags pins ADR-0038 pin-down 3: the three
// --apply-retry-* dials carry hard ranges (attempts 1–64, base
// 10ms–10s, cap 1s–300s) enforced loudly at startup, not silently
// clamped. Every boundary (just-inside accepted, just-outside
// rejected) is asserted per dial so a future off-by-one in the
// comparison is caught.
func TestValidateRetryFlags(t *testing.T) {
	const (
		okAttempts = 8
		okBase     = 100 * time.Millisecond
		okCap      = 30 * time.Second
	)

	cases := []struct {
		name     string
		attempts int
		base     time.Duration
		capDur   time.Duration
		wantErr  string // substring expected in the error; "" = accept
	}{
		{"defaults accepted", okAttempts, okBase, okCap, ""},

		// attempts boundaries
		{"attempts min accepted (1 = no retry)", 1, okBase, okCap, ""},
		{"attempts max accepted (64)", 64, okBase, okCap, ""},
		{"attempts 0 rejected", 0, okBase, okCap, "--apply-retry-attempts=0 out of range"},
		{"attempts negative rejected", -1, okBase, okCap, "--apply-retry-attempts=-1 out of range"},
		{"attempts 65 rejected", 65, okBase, okCap, "--apply-retry-attempts=65 out of range"},

		// base boundaries
		{"base min accepted (10ms)", okAttempts, 10 * time.Millisecond, okCap, ""},
		{"base max accepted (10s)", okAttempts, 10 * time.Second, okCap, ""},
		{"base just below min rejected (9ms)", okAttempts, 9 * time.Millisecond, okCap, "--apply-retry-backoff-base"},
		{"base zero rejected", okAttempts, 0, okCap, "--apply-retry-backoff-base"},
		{"base just above max rejected (11s)", okAttempts, 11 * time.Second, okCap, "--apply-retry-backoff-base"},

		// cap boundaries
		{"cap min accepted (1s)", okAttempts, okBase, 1 * time.Second, ""},
		{"cap max accepted (300s)", okAttempts, okBase, 300 * time.Second, ""},
		{"cap just below min rejected (999ms)", okAttempts, okBase, 999 * time.Millisecond, "--apply-retry-backoff-cap"},
		{"cap just above max rejected (301s)", okAttempts, okBase, 301 * time.Second, "--apply-retry-backoff-cap"},

		// the validator checks attempts first; assert order is stable
		// (an operator fixing one error at a time gets a deterministic
		// sequence rather than a flapping message).
		{"all three bad → attempts reported first", 0, 0, 0, "--apply-retry-attempts=0 out of range"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := validateRetryFlags(c.attempts, c.base, c.capDur)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("validateRetryFlags(%d,%s,%s) = %v; want nil",
						c.attempts, c.base, c.capDur, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateRetryFlags(%d,%s,%s) = nil; want error containing %q",
					c.attempts, c.base, c.capDur, c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("error = %q; want substring %q", err.Error(), c.wantErr)
			}
			// Every range error must cite ADR-0038 so the operator
			// can find the rationale (pin-down 3 promise).
			if !strings.Contains(err.Error(), "ADR-0038") {
				t.Errorf("range error %q does not reference ADR-0038", err.Error())
			}
		})
	}
}

// TestRetryBoundConstants pins the literal ADR-0038 pin-down-3 bounds.
// A change-detector by design: if the ranges are widened/narrowed the
// ADR's Configuration table and this pin must move together, so the
// "operator can compute the worst-case envelope" property the ADR
// promises stays true.
func TestRetryBoundConstants(t *testing.T) {
	if retryAttemptsLo != 1 || retryAttemptsHi != 64 {
		t.Errorf("attempts bounds = [%d,%d]; ADR-0038 pins [1,64]", retryAttemptsLo, retryAttemptsHi)
	}
	if retryBackoffBaseLo != 10*time.Millisecond || retryBackoffBaseHi != 10*time.Second {
		t.Errorf("base bounds = [%s,%s]; ADR-0038 pins [10ms,10s]", retryBackoffBaseLo, retryBackoffBaseHi)
	}
	if retryBackoffCapLo != 1*time.Second || retryBackoffCapHi != 300*time.Second {
		t.Errorf("cap bounds = [%s,%s]; ADR-0038 pins [1s,300s]", retryBackoffCapLo, retryBackoffCapHi)
	}
}
