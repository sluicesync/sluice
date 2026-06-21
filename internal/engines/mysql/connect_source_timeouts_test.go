// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strconv"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// TestApplySourceReadSessionTimeouts_SetsBoundedDefault pins ADR-0109 §A:
// a source read session gets net_write_timeout / net_read_timeout set to
// the bounded 10-min default, emitted as a bare integer the go-sql-driver
// turns into `SET net_write_timeout = 600` at session init. A regression
// here re-opens the silent cold-copy-abort-on-target-stall gap (the source
// drops the idle read at its default 60s net_write_timeout).
func TestApplySourceReadSessionTimeouts_SetsBoundedDefault(t *testing.T) {
	cfg := &mysql.Config{}
	applySourceReadSessionTimeouts(cfg)

	want := strconv.Itoa(sourceReadSessionTimeoutSeconds)
	if want != "600" {
		t.Fatalf("sourceReadSessionTimeoutSeconds = %s; ADR-0109 §A specifies a ~10-min bound (600)", want)
	}
	for _, key := range []string{"net_write_timeout", "net_read_timeout"} {
		got, ok := cfg.Params[key]
		if !ok {
			t.Errorf("cfg.Params[%q] absent — the driver won't emit `SET %s = ...` on the source read session", key, key)
			continue
		}
		if got != want {
			t.Errorf("cfg.Params[%q] = %q; want %q (the bounded source-read default)", key, got, want)
		}
	}
}

// TestApplySourceReadSessionTimeouts_OperatorOverrideWins confirms the
// two-tier override shape (matching sql_mode / time_zone): a value the
// operator put in the source DSN params is NEVER clobbered, so a deliberate
// per-source tuning survives.
func TestApplySourceReadSessionTimeouts_OperatorOverrideWins(t *testing.T) {
	cfg := &mysql.Config{Params: map[string]string{
		"net_write_timeout": "120",
		// net_read_timeout deliberately left unset so the helper fills it.
	}}
	applySourceReadSessionTimeouts(cfg)

	if got := cfg.Params["net_write_timeout"]; got != "120" {
		t.Errorf("operator-supplied net_write_timeout=120 was overwritten with %q; the DSN value must win", got)
	}
	if got := cfg.Params["net_read_timeout"]; got != strconv.Itoa(sourceReadSessionTimeoutSeconds) {
		t.Errorf("net_read_timeout = %q; the unset key should get the bounded default %d", got, sourceReadSessionTimeoutSeconds)
	}
}

// TestApplySourceReadSessionTimeouts_ZeroValueSafe pins the v0.99.51 trap
// guard: the bound is a package CONSTANT, not a config field, so it is set
// even when cfg arrives with a nil Params map (the common zero-value
// construction path — every test / programmatic open that doesn't pre-seed
// Params still gets the safe large value, not 0).
func TestApplySourceReadSessionTimeouts_ZeroValueSafe(t *testing.T) {
	cfg := &mysql.Config{} // nil Params — the Go zero value
	applySourceReadSessionTimeouts(cfg)
	if cfg.Params == nil {
		t.Fatal("Params still nil after applySourceReadSessionTimeouts — the helper must allocate it")
	}
	if cfg.Params["net_write_timeout"] == "0" || cfg.Params["net_write_timeout"] == "" {
		t.Errorf("net_write_timeout = %q on the zero-value path — must be the bounded large default, never 0/empty", cfg.Params["net_write_timeout"])
	}
}
