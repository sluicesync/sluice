// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestIsPlanetScaleEphemeralRole(t *testing.T) {
	cases := []struct {
		role string
		want bool
	}{
		{"pscale_api_abc123", true},
		{"pscale_api_", true},
		{"postgres", false},
		{"", false},
		{"pscale_apiX", false}, // no underscore boundary — not the ephemeral shape
		{"my_pscale_api_x", false},
		{"app_owner", false},
	}
	for _, c := range cases {
		if got := isPlanetScaleEphemeralRole(c.role); got != c.want {
			t.Errorf("isPlanetScaleEphemeralRole(%q) = %v; want %v", c.role, got, c.want)
		}
	}
}

type fakeRoleReporter struct {
	role string
	err  error
}

func (f fakeRoleReporter) CurrentRole(_ context.Context) (string, error) {
	return f.role, f.err
}

// captureWarn runs fn with the default slog logger swapped for a recorder
// and returns everything it logged.
func captureWarn(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	fn()
	return buf.String()
}

func TestPreflightTargetOwnershipAdvisory_WarnsOnEphemeralRole(t *testing.T) {
	out := captureWarn(t, func() {
		preflightTargetOwnershipAdvisory(context.Background(), fakeRoleReporter{role: "pscale_api_xyz"})
	})
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("expected a WARN; got: %s", out)
	}
	if !strings.Contains(out, "pscale_api_xyz") {
		t.Errorf("expected the offending role in the WARN; got: %s", out)
	}
	if !strings.Contains(out, "reset-default") {
		t.Errorf("expected the recovery hint in the WARN; got: %s", out)
	}
}

func TestPreflightTargetOwnershipAdvisory_QuietOnDefaultRole(t *testing.T) {
	out := captureWarn(t, func() {
		preflightTargetOwnershipAdvisory(context.Background(), fakeRoleReporter{role: "postgres"})
	})
	if strings.Contains(out, "OWNED by an ephemeral") {
		t.Errorf("Default role must not warn; got: %s", out)
	}
}

func TestPreflightTargetOwnershipAdvisory_SwallowsProbeError(t *testing.T) {
	out := captureWarn(t, func() {
		preflightTargetOwnershipAdvisory(context.Background(), fakeRoleReporter{err: errors.New("boom")})
	})
	if strings.Contains(out, "level=WARN") {
		t.Errorf("a probe error must not produce a WARN; got: %s", out)
	}
}

func TestPreflightTargetOwnershipAdvisory_NoOpOnNonReporter(t *testing.T) {
	// A non-PG handle (no CurrentRole method) must be a silent no-op, not a panic.
	out := captureWarn(t, func() {
		preflightTargetOwnershipAdvisory(context.Background(), struct{}{})
	})
	if out != "" {
		t.Errorf("non-reporter handle should log nothing; got: %s", out)
	}
}
