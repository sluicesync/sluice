// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// paramParsers is the same four-entry-point family [mysqlDSNParsers] sweeps,
// projected onto cfg.Params — every one feeds openDB, so a param applied to
// fewer than all four leaves whole connection classes on the server's own
// max_error_count.
var paramParsers = []struct {
	name  string
	parse func(dsn string) (map[string]string, error)
}{
	{"parseDSN", func(dsn string) (map[string]string, error) {
		cfg, err := parseDSN(dsn)
		if err != nil {
			return nil, err
		}
		return cfg.Params, nil
	}},
	{"parseServerDSN", func(dsn string) (map[string]string, error) {
		cfg, err := parseServerDSN(dsn)
		if err != nil {
			return nil, err
		}
		return cfg.Params, nil
	}},
	{"parseDSNForFlavor/planetscale", func(dsn string) (map[string]string, error) {
		cfg, err := parseDSNForFlavor(dsn, FlavorPlanetScale)
		if err != nil {
			return nil, err
		}
		return cfg.Params, nil
	}},
	{"parseServerDSNForFlavor/vitess", func(dsn string) (map[string]string, error) {
		cfg, err := parseServerDSNForFlavor(dsn, FlavorVitess)
		if err != nil {
			return nil, err
		}
		return cfg.Params, nil
	}},
}

// TestParseDSN_PinsMaxErrorCountFloor is the COLD-1 pin (audit 2026-08-11).
// sluice's silent-clamp detection reads the SHOW WARNINGS row count, which the
// server truncates at @@max_error_count; a server (or DBA) running
// max_error_count=0 leaves @@warning_count accurate but SHOW WARNINGS empty, so
// a truncating LOAD DATA committed the clamped value and exited 0. Every DSN
// parser must inject a max_error_count floor so no sluice connection inherits a
// suppressing value.
func TestParseDSN_PinsMaxErrorCountFloor(t *testing.T) {
	for _, p := range paramParsers {
		t.Run(p.name, func(t *testing.T) {
			params, err := p.parse("user:pw@tcp(db.example:3306)/appdb")
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := params["max_error_count"]; got != "1024" {
				t.Fatalf("max_error_count = %q, want %q — without the floor a server-side "+
					"max_error_count=0 silences SHOW WARNINGS and the silent-clamp refusal never fires (COLD-1)", got, "1024")
			}
		})
	}
}

// TestParseDSN_OperatorMaxErrorCountWins pins the two-tier override: an explicit
// DSN max_error_count= param is a visible operator choice and wins, matching
// sql_mode / time_zone / writeTimeout.
func TestParseDSN_OperatorMaxErrorCountWins(t *testing.T) {
	for _, p := range paramParsers {
		t.Run(p.name, func(t *testing.T) {
			params, err := p.parse("user:pw@tcp(db.example:3306)/appdb?max_error_count=64")
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := params["max_error_count"]; got != "64" {
				t.Fatalf("max_error_count = %q, want the operator's 64 — a DSN param must win", got)
			}
		})
	}
}
