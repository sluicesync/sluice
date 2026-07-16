// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package expandcontract

import (
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/planetscale/api"
)

// TestBranchMySQLConfig_MetacharSafeDSN is the audit 2026-07-16 M3.6
// pin: the dev-branch migrate-state DSN is built through
// gomysql.NewConfig + FormatDSN, never string splicing, so a minted
// branch password carrying DSN metacharacters ('@', '/', ':', '?', and
// '(' — all legal in PlanetScale-minted secrets) round-trips the
// driver's own parser byte-exact. The pre-fix fmt.Sprintf shape parsed
// such a password into the wrong host/db split.
func TestBranchMySQLConfig_MetacharSafeDSN(t *testing.T) {
	pw := &api.BranchPassword{
		Username:      "user-x",
		PlainText:     `pscale_pw_a@b/c:d?e(f)`,
		AccessHostURL: "aws.connect.psdb.cloud",
	}
	cfg := branchMySQLConfig(pw, "shop")
	dsn := cfg.FormatDSN()

	parsed, err := gomysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN(%q): %v", dsn, err)
	}
	if parsed.User != pw.Username || parsed.Passwd != pw.PlainText {
		t.Errorf("credentials did not round-trip: user=%q passwd=%q", parsed.User, parsed.Passwd)
	}
	if parsed.DBName != "shop" || parsed.Net != "tcp" {
		t.Errorf("net/db did not round-trip: net=%q db=%q", parsed.Net, parsed.DBName)
	}
	// ParseDSN normalizes a port-less addr to :3306 — the default the
	// pre-fix Sprintf hard-coded.
	if parsed.Addr != "aws.connect.psdb.cloud:3306" {
		t.Errorf("addr = %q; want aws.connect.psdb.cloud:3306", parsed.Addr)
	}
	if parsed.TLSConfig != "true" {
		t.Errorf("tls = %q; want \"true\" (PlanetScale hosts require TLS)", parsed.TLSConfig)
	}
}
