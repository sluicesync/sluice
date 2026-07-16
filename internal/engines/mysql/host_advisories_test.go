// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSourceHostAdvisories_DigitalOceanRetention pins the item-70a
// lying-retention advisory: a CDC-anchoring run against a
// *.db.ondigitalocean.com host WARNs naming the ~13-16-minute
// out-of-band purge window (the live probe's observed bound) and the
// binlog_retention_period config-API
// remedy — the host pattern is the ONLY reliable signal because
// @@binlog_expire_logs_seconds lies (live-probed 2026-07-15).
func TestSourceHostAdvisories_DigitalOceanRetention(t *testing.T) {
	var _ ir.SourceHostAdvisor = Engine{}

	const dsn = "doadmin:pw@tcp(db-mysql-nyc3-12345-do-user-1-0.b.db.ondigitalocean.com:25060)/defaultdb?tls=true"

	got := (Engine{Flavor: FlavorVanilla}).SourceHostAdvisories(dsn, true)
	if len(got) != 1 {
		t.Fatalf("got %d advisories; want 1", len(got))
	}
	a := got[0]
	for _, want := range []string{
		"db.ondigitalocean.com",
		"binlog_expire_logs_seconds",
		"binlog_retention_period",
		"86400",
		"13-16 minutes",
	} {
		if !strings.Contains(a.Message, want) {
			t.Errorf("message should mention %q; got: %s", want, a.Message)
		}
	}
	if a.Hint == "" {
		t.Error("advisory carries no hint")
	}
}

// TestSourceHostAdvisories_DigitalOcean_MigrateSilent pins the cdc
// gate: a plain migrate never returns to the source binlog, so the
// retention advisory must NOT fire for cdc=false — a WARN about a
// hazard the run cannot hit is noise.
func TestSourceHostAdvisories_DigitalOcean_MigrateSilent(t *testing.T) {
	const dsn = "doadmin:pw@tcp(db-mysql-nyc3-12345-do-user-1-0.b.db.ondigitalocean.com:25060)/defaultdb"
	if got := (Engine{Flavor: FlavorVanilla}).SourceHostAdvisories(dsn, false); len(got) != 0 {
		t.Errorf("cdc=false: got %d advisories (%v); want none", len(got), got)
	}
}

// TestSourceHostAdvisories_NonDOHostsSilent pins the no-op for every
// other host shape: plain hosts, PlanetScale endpoints (they have
// their own ValidateDSN refusal), socket DSNs, and garbage.
func TestSourceHostAdvisories_NonDOHostsSilent(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
	}{
		{"local", "root:pw@tcp(localhost:3306)/app"},
		{"planetscale", "u:p@tcp(aws.connect.psdb.cloud:3306)/app"},
		{"suffix embedded mid-host does not match", "u:p@tcp(db.ondigitalocean.com.evil.example:3306)/app"},
		{"unix socket", "u:p@unix(/var/run/mysqld/mysqld.sock)/app"},
		{"empty", ""},
		{"garbage", "::::"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := (Engine{Flavor: FlavorVanilla}).SourceHostAdvisories(c.dsn, true); len(got) != 0 {
				t.Errorf("got %d advisories (%v); want none", len(got), got)
			}
		})
	}
}
