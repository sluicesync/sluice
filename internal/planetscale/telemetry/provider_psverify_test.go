//go:build psverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Credentialed smoke test for the PlanetScale telemetry provider. Gated
// behind the psverify build tag — same posture as the PS-MySQL/PG
// verification suites — so a CI run WITHOUT credentials never runs (or
// builds) it. It hits the REAL PlanetScale metrics endpoint with a
// read_metrics_endpoints service token and asserts the provider produces an
// ok Sample with at least one observed metric family.
//
// Usage from a shell with credentials available:
//
//	go test -tags=psverify -v -count=1 -timeout=2m \
//	  -run 'TestPSVerify_TelemetryProvider' ./internal/planetscale/telemetry/...
//
// Credentials are read from the environment, falling back to the
// machine-local file C:\code\PLANETSCALE_SLUICESYNC_METRICS.env:
//
//	PLANETSCALE_METRICS_SERVICE_TOKEN_ID
//	PLANETSCALE_METRICS_SERVICE_TOKEN
//	PLANETSCALE_METRICS_ORG
//	PLANETSCALE_METRICS_DATABASE   (the branch's database name to filter to)
//	PLANETSCALE_METRICS_BRANCH     (optional; defaults to "main")
//
// The token is NEVER printed; only its presence/absence is reported.
package telemetry

import (
	"bufio"
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

const psverifyMetricsEnvFile = `C:\code\PLANETSCALE_SLUICESYNC_METRICS.env`

func psverifyEnv(t *testing.T, key string) string {
	t.Helper()
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	// Fall back to the machine-local env file.
	f, err := os.Open(psverifyMetricsEnvFile)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if ok && strings.TrimSpace(k) == key {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func TestPSVerify_TelemetryProvider(t *testing.T) {
	tokenID := psverifyEnv(t, "PLANETSCALE_METRICS_SERVICE_TOKEN_ID")
	token := psverifyEnv(t, "PLANETSCALE_METRICS_SERVICE_TOKEN")
	org := psverifyEnv(t, "PLANETSCALE_METRICS_ORG")
	database := psverifyEnv(t, "PLANETSCALE_METRICS_DATABASE")
	branch := psverifyEnv(t, "PLANETSCALE_METRICS_BRANCH")

	if tokenID == "" || token == "" || org == "" {
		t.Skip("PLANETSCALE_METRICS_* credentials absent — skipping credentialed smoke test")
	}
	// Mask: report ONLY presence + a short prefix length, never the secret.
	t.Logf("psverify telemetry: org=%s token_id=%s token=<%d-char redacted> database=%s branch=%s",
		org, tokenID, len(token), database, branchOrMain(branch))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	p, err := New(ctx, Config{
		Org:          org,
		TokenID:      tokenID,
		Token:        token,
		Database:     database,
		Branch:       branch,
		PollInterval: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Wait for the first poll to land (it fires immediately in run()).
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if s, ok := p.Sample(ctx); ok {
			t.Logf("live snapshot: cpu=%.3f(known=%v) mem=%.3f(known=%v) storage=%.3f(known=%v avail=%d cap=%d) lag=%.1fs(known=%v) conns=%d/%d(known=%v)",
				s.CPUUtil, s.CPUKnown, s.MemUtil, s.MemKnown,
				s.StorageUtil, s.StorageKnown, s.StorageAvailableBytes, s.StorageCapacityBytes,
				s.ReplicaLagSeconds, s.LagKnown, s.ActiveConnections, s.MaxConnections, s.ConnKnown)
			if !s.CPUKnown && !s.MemKnown && !s.StorageKnown && !s.LagKnown && !s.ConnKnown {
				t.Fatal("live snapshot ok but no metric family observed — check the metric-name table against the live exposition")
			}
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("provider never produced an ok Sample from the live endpoint within 60s (check org/database/branch and the service-token permission)")
}

func branchOrMain(b string) string {
	if b == "" {
		return "main"
	}
	return b
}
