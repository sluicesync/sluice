// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPrintFleetPlan_RedactsDSN pins Gap B: the `sync run --dry-run` plan must
// never echo a DSN password. It covers BOTH the go-sql-driver DSN shape
// (user:pw@tcp(host)/db — the common MySQL/PlanetScale form, which the old
// dsnEndpoint fallback printed verbatim) and the URI shape. The host locator is
// still present so the plan stays useful.
func TestPrintFleetPlan_RedactsDSN(t *testing.T) {
	const (
		mysqlPassword = "supersecret_mysql_pw"
		pscalePwToken = "pscale_pw_TOPSECRET"
		pgPassword    = "supersecret_pg_pw"
	)
	fleet := &SyncFleetConfig{Syncs: []SyncSpec{
		{
			StreamID:     "driver-form",
			SourceDriver: "mysql", Source: "root:" + mysqlPassword + "@tcp(src-host:3306)/app",
			TargetDriver: "planetscale", Target: "root:" + pscalePwToken + "@tcp(dst-host:3306)/app?tls=true",
		},
		{
			StreamID:     "uri-form",
			SourceDriver: "postgres", Source: "postgres://u:" + pgPassword + "@pg-host:5432/app",
			TargetDriver: "mysql", Target: "root:" + mysqlPassword + "@tcp(dst2:3306)/app",
		},
	}}

	path := filepath.Join(t.TempDir(), "plan.txt")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	if err := printFleetPlan(f, fleet); err != nil {
		_ = f.Close()
		t.Fatalf("printFleetPlan: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close plan: %v", err)
	}
	out, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	plan := string(out)

	for _, secret := range []string{mysqlPassword, pscalePwToken, pgPassword} {
		if strings.Contains(plan, secret) {
			t.Errorf("dry-run plan LEAKED a DSN password %q:\n%s", secret, plan)
		}
	}
	// The redacted host locator is still there — a plan with no endpoint is useless.
	for _, host := range []string{"src-host:3306", "dst-host:3306", "pg-host:5432", "dst2:3306"} {
		if !strings.Contains(plan, host) {
			t.Errorf("dry-run plan dropped the host locator %q:\n%s", host, plan)
		}
	}
}
