// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"
)

// TestParseDSN_NoDoubleInvalidPrefix is the GitHub #17 papercut
// fix: when go-sql-driver/mysql returns "invalid DSN: <reason>"
// and sluice wraps with "mysql: invalid DSN: %w", the operator
// saw a confusing "mysql: invalid DSN: invalid DSN: <reason>"
// double-prefix. Sluice now strips the redundant inner prefix.
func TestParseDSN_NoDoubleInvalidPrefix(t *testing.T) {
	// A garbage DSN the driver rejects (intentionally non-parseable
	// so the wrap path fires).
	_, err := parseDSN("garbage://not-a-valid-dsn$$$")
	if err == nil {
		t.Fatal("expected error from garbage DSN; got nil")
	}
	msg := err.Error()
	// Confirm the redundant double-prefix is gone.
	if strings.Count(strings.ToLower(msg), "invalid dsn") > 1 {
		t.Errorf("error %q still contains the doubled 'invalid DSN:' prefix that GitHub #17 papercut targets", msg)
	}
}

// TestDSNShapeHint_BranchPathDetected covers the second GitHub #17
// papercut: PlanetScale DSNs of the shape `db/branch` produce the
// driver's generic "did you forget to escape a param value?" hint
// which sends operators down the wrong rabbit hole. Sluice now
// detects the path-with-extra-slash and emits a clearer hint
// pointing at branch-scoped credentials.
func TestDSNShapeHint_BranchPathDetected(t *testing.T) {
	hint := dsnShapeHint("user:pw@tcp(aws.connect.psdb.cloud:3306)/sync-destination-mysql/safe-mig-test?tls=true")
	if hint == "" {
		t.Fatal("expected non-empty hint for /db/branch DSN; got empty")
	}
	if !strings.Contains(strings.ToLower(hint), "branch") {
		t.Errorf("hint should mention 'branch' to disambiguate; got %q", hint)
	}
	if !strings.Contains(hint, "PlanetScale") {
		t.Errorf("hint should name PlanetScale so operators recognise the pattern; got %q", hint)
	}
}

// TestParseDSN_TCPRoutesThroughKeepaliveNet pins #77: a plain-TCP DSN
// is rerouted onto the custom keep-alive network so the connection
// inherits sluice's TCP keep-alive policy. A regression here (back to
// bare "tcp") would silently drop the cloud-NAT idle-timeout hardening.
func TestParseDSN_TCPRoutesThroughKeepaliveNet(t *testing.T) {
	cfg, err := parseDSN("user:pw@tcp(host:3306)/mydb")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if cfg.Net != keepaliveNet {
		t.Errorf("cfg.Net = %q, want %q (tcp should reroute through the keep-alive dialer)", cfg.Net, keepaliveNet)
	}
}

// TestParseDSN_UnixSocketNotRerouted confirms the keep-alive rerouting
// only touches TCP — unix sockets (where TCP keep-alive is meaningless)
// are left on their original network.
func TestParseDSN_UnixSocketNotRerouted(t *testing.T) {
	cfg, err := parseDSN("root@unix(/tmp/mysql.sock)/foo")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if cfg.Net != "unix" {
		t.Errorf("cfg.Net = %q, want \"unix\" (unix sockets must not be rerouted)", cfg.Net)
	}
}

// TestParseDSN_InjectsStrictSQLMode pins the v0.92.1 sql_mode plumbing
// the cycle-time validation called into question. The post-handshake
// SET path the driver emits depends on every entry in cfg.Params,
// including this one — if the value isn't here, no SET happens, and
// Bugs 102/103 silently re-open.
func TestParseDSN_InjectsStrictSQLMode(t *testing.T) {
	cfg, err := parseDSN("user:pw@tcp(host:3306)/mydb")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	val, ok := cfg.Params["sql_mode"]
	if !ok {
		t.Fatal("cfg.Params[\"sql_mode\"] absent — driver won't issue SET sql_mode at handshake")
	}
	if !strings.Contains(val, "STRICT_TRANS_TABLES") {
		t.Errorf("cfg.Params[\"sql_mode\"] = %q; expected to contain STRICT_TRANS_TABLES", val)
	}
	if !strings.HasPrefix(val, "'") || !strings.HasSuffix(val, "'") {
		t.Errorf("cfg.Params[\"sql_mode\"] = %q; expected SQL-literal quotes for driver's verbatim `SET key = value` emission", val)
	}
	if cfg.Collation != "utf8mb4_general_ci" {
		t.Errorf("cfg.Collation = %q; expected utf8mb4_general_ci so handshake collation ID supports 4-byte UTF-8", cfg.Collation)
	}
}

// TestParseDSN_DSNSqlModeWinsOverInjected confirms an operator-supplied
// sql_mode in the DSN takes precedence over sluice's default. The two-
// tier override policy documented in connect.go depends on this.
func TestParseDSN_DSNSqlModeWinsOverInjected(t *testing.T) {
	cfg, err := parseDSN("user:pw@tcp(host:3306)/mydb?sql_mode=%27ANSI_QUOTES%27")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	val := cfg.Params["sql_mode"]
	if !strings.Contains(val, "ANSI_QUOTES") {
		t.Errorf("DSN-supplied sql_mode should win; got %q", val)
	}
	if strings.Contains(val, "STRICT_TRANS_TABLES") {
		t.Errorf("DSN-supplied sql_mode should have replaced the default; got %q", val)
	}
}

// TestParseDSN_SetSessionSQLModeEmptyDisablesInjection covers the
// legacy-data escape hatch (--mysql-sql-mode=”). Empty string means
// "don't inject anything — let the server's default apply".
func TestParseDSN_SetSessionSQLModeEmptyDisablesInjection(t *testing.T) {
	orig := sessionSQLMode
	defer func() { sessionSQLMode = orig }()
	SetSessionSQLMode("")
	cfg, err := parseDSN("user:pw@tcp(host:3306)/mydb")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if _, ok := cfg.Params["sql_mode"]; ok {
		t.Errorf("--mysql-sql-mode='' should suppress sql_mode injection; got cfg.Params[\"sql_mode\"]=%q", cfg.Params["sql_mode"])
	}
}

// TestDSNShapeHint_PlainPathNoHint confirms a well-formed DSN with
// just `db` in the path produces no hint (we don't want false
// positives noising every DSN parse error).
func TestDSNShapeHint_PlainPathNoHint(t *testing.T) {
	cases := []string{
		"user:pw@tcp(host:3306)/mydb",
		"user:pw@tcp(host:3306)/mydb?tls=true",
		"root@unix(/tmp/mysql.sock)/foo",
		"user@(localhost)/bar?parseTime=true&loc=UTC",
	}
	for _, dsn := range cases {
		dsn := dsn
		t.Run(dsn, func(t *testing.T) {
			hint := dsnShapeHint(dsn)
			if hint != "" {
				t.Errorf("expected empty hint for well-formed DSN; got %q", hint)
			}
		})
	}
}
