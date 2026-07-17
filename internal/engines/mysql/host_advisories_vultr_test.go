// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"
)

// vultrTestDSN is a realistic Vultr Managed MySQL DSN (host shape from
// the 2026-07-17 live probe: vultr-prod-<uuid>-vultr-prod-<hex>
// labels, nonstandard high port).
const vultrTestDSN = "vultradmin:pw@tcp(vultr-prod-1a2b3c4d-vultr-prod-9f8e.vultrdb.com:16751)/defaultdb?tls=true"

// TestSourceHostAdvisories_VultrNoRemedyRetention pins the Vultr
// no-remedy advisory: a CDC-anchoring run against a *.vultrdb.com host
// WARNs naming the ~10-16-minute out-of-band purge window, that
// @@binlog_expire_logs_seconds does not govern it, that NO retention
// knob exists on the platform (API/CLI/SQL all verified live), and the
// migrate-and-cut-over consequence — deliberately stronger than the
// DigitalOcean message, which can at least point at a knob.
func TestSourceHostAdvisories_VultrNoRemedyRetention(t *testing.T) {
	got := (Engine{Flavor: FlavorVanilla}).SourceHostAdvisories(vultrTestDSN, true)
	if len(got) != 1 {
		t.Fatalf("got %d advisories; want 1", len(got))
	}
	a := got[0]
	for _, want := range []string{
		"vultrdb.com",
		"~10-16 minutes",
		"@@binlog_expire_logs_seconds",
		"NO retention setting",
		"cannot be extended",
		"does NOT hold the purger back",
		"migrate-and-cut-over",
		"under ~10 minutes",
	} {
		if !strings.Contains(a.Message, want) {
			t.Errorf("message should mention %q; got: %s", want, a.Message)
		}
	}
	// The no-remedy shape: the message must NOT recommend DO's knob (a
	// copy-paste of the DO advisory would send operators hunting for a
	// setting that does not exist on this platform).
	if strings.Contains(a.Message, "binlog_retention_period") {
		t.Errorf("Vultr message must not name DO's binlog_retention_period knob (no knob exists on Vultr); got: %s", a.Message)
	}
	if a.Hint == "" {
		t.Error("advisory carries no hint")
	}
}

// TestSourceHostAdvisories_Vultr_MigrateSilent pins the cdc gate for
// the Vultr leg: a plain migrate never returns to the source binlog,
// so the retention advisory must NOT fire for cdc=false.
func TestSourceHostAdvisories_Vultr_MigrateSilent(t *testing.T) {
	if got := (Engine{Flavor: FlavorVanilla}).SourceHostAdvisories(vultrTestDSN, false); len(got) != 0 {
		t.Errorf("cdc=false: got %d advisories (%v); want none", len(got), got)
	}
}

// TestSourceHostAdvisories_ProviderExclusive pins the dispatch: each
// host class fires exactly its own advisory — a DO host never carries
// the Vultr no-remedy text, a Vultr host never carries DO's config-API
// remedy, and the suffix embedded mid-host matches neither.
func TestSourceHostAdvisories_ProviderExclusive(t *testing.T) {
	const doDSN = "doadmin:pw@tcp(db-mysql-nyc3-12345-do-user-1-0.b.db.ondigitalocean.com:25060)/defaultdb"

	doGot := (Engine{Flavor: FlavorVanilla}).SourceHostAdvisories(doDSN, true)
	if len(doGot) != 1 || !strings.Contains(doGot[0].Message, "DigitalOcean") {
		t.Fatalf("DO host: got %v; want exactly the DigitalOcean advisory", doGot)
	}
	if strings.Contains(doGot[0].Message, "Vultr") {
		t.Errorf("DO advisory must not mention Vultr; got: %s", doGot[0].Message)
	}

	vGot := (Engine{Flavor: FlavorVanilla}).SourceHostAdvisories(vultrTestDSN, true)
	if len(vGot) != 1 || !strings.Contains(vGot[0].Message, "Vultr") {
		t.Fatalf("Vultr host: got %v; want exactly the Vultr advisory", vGot)
	}
	if strings.Contains(vGot[0].Message, "DigitalOcean") {
		t.Errorf("Vultr advisory must not mention DigitalOcean; got: %s", vGot[0].Message)
	}

	const embedded = "u:p@tcp(vultrdb.com.evil.example:3306)/app"
	if got := (Engine{Flavor: FlavorVanilla}).SourceHostAdvisories(embedded, true); len(got) != 0 {
		t.Errorf("suffix embedded mid-host: got %d advisories (%v); want none", len(got), got)
	}
}
