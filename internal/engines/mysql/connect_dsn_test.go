// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"log/slog"
	"net/url"
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

// TestParseDSN_WarnsOnNBEDisagreement pins the SEC-1 re-review follow-up:
// parseDSN WARNs once when an operator-supplied DSN `sql_mode` param disagrees
// with sluice's configured session mode ([sessionSQLMode]) on
// NO_BACKSLASH_ESCAPES — the one residual fail-open channel where the DDL
// emitters' backslash-escaping decision (made against sessionSQLMode) diverges
// from the mode the connection actually runs (the DSN param, which wins on the
// wire). Both directions of disagreement WARN; the agreeing case is silent;
// the WARN never blocks (parseDSN still succeeds). The value classifier
// [sqlModeHasNBE] is exercised directly so the substring match is pinned too.
func TestParseDSN_WarnsOnNBEDisagreement(t *testing.T) {
	// sqlModeHasNBE: quoted / mixed-case / comma-list forms all match, and no
	// other flag is a false positive.
	for mode, want := range map[string]bool{
		"'NO_BACKSLASH_ESCAPES'":                   true,
		"no_backslash_escapes":                     true,
		"STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES": true,
		"'STRICT_TRANS_TABLES,ANSI_QUOTES'":        false,
		"":                                         false,
		"NO_ZERO_DATE":                             false,
	} {
		if got := sqlModeHasNBE(mode); got != want {
			t.Errorf("sqlModeHasNBE(%q) = %v; want %v", mode, got, want)
		}
	}

	dsnWith := func(sqlMode string) string {
		return "user:pw@tcp(host:3306)/mydb?sql_mode=" + url.QueryEscape("'"+sqlMode+"'")
	}
	capture := func(t *testing.T, session, dsnMode string) string {
		t.Helper()
		orig := sessionSQLMode
		defer func() { sessionSQLMode = orig }()
		SetSessionSQLMode(session)
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		if _, err := parseDSN(dsnWith(dsnMode)); err != nil {
			t.Fatalf("parseDSN(%q session=%q): %v", dsnMode, session, err)
		}
		return buf.String()
	}

	// Direction 1: session mode has NO_BACKSLASH_ESCAPES, DSN doesn't → WARN.
	if out := capture(t, "STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES", "STRICT_TRANS_TABLES"); !strings.Contains(out, "NO_BACKSLASH_ESCAPES") || !strings.Contains(out, "disagrees") {
		t.Errorf("session-NBE vs DSN-plain: want a NO_BACKSLASH_ESCAPES disagreement WARN; got %q", out)
	}
	// Direction 2: DSN mode has NO_BACKSLASH_ESCAPES, session doesn't → WARN.
	if out := capture(t, "STRICT_TRANS_TABLES", "STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES"); !strings.Contains(out, "NO_BACKSLASH_ESCAPES") || !strings.Contains(out, "disagrees") {
		t.Errorf("DSN-NBE vs session-plain: want a NO_BACKSLASH_ESCAPES disagreement WARN; got %q", out)
	}
	// Agreeing case (both plain): no WARN.
	if out := capture(t, "STRICT_TRANS_TABLES", "STRICT_TRANS_TABLES"); strings.Contains(out, "disagrees") {
		t.Errorf("agreeing modes must not WARN; got %q", out)
	}
	// Agreeing case (both NBE): no WARN.
	if out := capture(t, "NO_BACKSLASH_ESCAPES", "NO_BACKSLASH_ESCAPES"); strings.Contains(out, "disagrees") {
		t.Errorf("both-NBE must not WARN; got %q", out)
	}
	// No DSN sql_mode param at all → sluice injects sessionSQLMode itself, so
	// there is nothing to disagree with: no WARN.
	func() {
		orig := sessionSQLMode
		defer func() { sessionSQLMode = orig }()
		SetSessionSQLMode("STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES")
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		if _, err := parseDSN("user:pw@tcp(host:3306)/mydb"); err != nil {
			t.Fatalf("parseDSN (no DSN sql_mode): %v", err)
		}
		if strings.Contains(buf.String(), "disagrees") {
			t.Errorf("no DSN sql_mode param must not WARN; got %q", buf.String())
		}
	}()
}

// TestStripVStreamParams_RemovesAllVStreamKeys pins the Bug 126
// choke-point helper that openDB runs before handing the config to
// go-sql-driver/mysql. Every vstream_* param is a sluice-internal DSN
// extension consumed only by the VStream CDC reader; if any survives
// into a MySQL connection the driver emits a `SET vstream_* = …` that
// self-hosted Vitess / vttestserver rejects (Error 1105 / VT05006),
// killing a planetscale-flavored cold-start at the schema-reader open.
// The integration counterpart (cdc_vstream_bug126_integration_test.go)
// proves the end-to-end behaviour against real Vitess; this is the
// fast unit pin so the strip can't regress silently under the default
// suite.
func TestStripVStreamParams_RemovesAllVStreamKeys(t *testing.T) {
	cfg, err := parseDSN("user:pw@tcp(host:3306)/mydb")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	// The full known set of sluice's vstream_* extensions, as a real
	// --source-driver=planetscale DSN would carry them.
	vstreamKeys := []string{
		"vstream_endpoint",
		"vstream_transport",
		"vstream_auth",
		"vstream_shards",
		"vstream_auto_discover_shards",
		"vstream_insecure_tls",
	}
	for _, k := range vstreamKeys {
		cfg.Params[k] = "x"
	}
	// A non-vstream param that MUST survive the strip (sql_mode is
	// injected by parseDSN; time_zone too) — the helper must be a
	// prefix filter, not a blanket clear.
	cfg.Params["custom_param"] = "keepme"

	stripped := stripVStreamParams(cfg)

	for _, k := range vstreamKeys {
		if _, ok := stripped.Params[k]; ok {
			t.Errorf("stripped.Params still contains %q; openDB would emit SET %s and vtgate would reject it (Bug 126)", k, k)
		}
	}
	if v := stripped.Params["custom_param"]; v != "keepme" {
		t.Errorf("stripped.Params[custom_param] = %q; non-vstream params must survive the strip", v)
	}
	if v := stripped.Params["sql_mode"]; v == "" {
		t.Error("stripped.Params[sql_mode] empty; the strict-mode injection must survive the strip")
	}

	// No-mutation guarantee: the helper Clone()s, so the caller's
	// original cfg.Params is left intact — the VStream reader reads
	// vstream_* out of its own cfg before openDB ever runs, and a
	// future caller that inspects cfg after openDB must see no change.
	for _, k := range vstreamKeys {
		if _, ok := cfg.Params[k]; !ok {
			t.Errorf("stripVStreamParams mutated the caller's cfg (lost %q); it must Clone()", k)
		}
	}
}

// TestStripVStreamParams_StripsZeroDate pins ADR-0127: the sluice-internal
// zero_date param (a per-sync zero-date policy override, NOT a MySQL system
// variable) must be stripped before the MySQL session — otherwise the
// go-sql-driver would emit `SET zero_date = …` at session init and the server
// would reject it. It is in nativeSluiceParams alongside copy_table_parallelism.
func TestStripVStreamParams_StripsZeroDate(t *testing.T) {
	cfg, err := parseDSN("user:pw@tcp(host:3306)/mydb?zero_date=null")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if _, ok := cfg.Params["zero_date"]; !ok {
		t.Fatal("precondition: parsed cfg should carry zero_date")
	}
	stripped := stripVStreamParams(cfg)
	if _, ok := stripped.Params["zero_date"]; ok {
		t.Error("stripped.Params still contains zero_date; openDB would emit SET zero_date and the server would reject it")
	}
	// No-mutation guarantee: the reader reads zero_date out of its own cfg
	// before openDB strips a CLONE, so the caller's cfg must be left intact.
	if _, ok := cfg.Params["zero_date"]; !ok {
		t.Error("stripVStreamParams mutated the caller's cfg (lost zero_date); it must Clone()")
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
