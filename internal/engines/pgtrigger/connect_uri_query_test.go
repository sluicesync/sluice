// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestParseURIDSN_KeepsOptionsSpacesForPgx is the pgtrigger copy of the
// postgres engine's TestURIRewriters_KeepOptionsSpacesForPgx: this
// engine's own parseURIDSN re-encodes the query too, and must hand pgx
// (whose URI grammar reads `+` as a literal plus from v5.11) an `options`
// value it parses back exactly.
func TestParseURIDSN_KeepsOptionsSpacesForPgx(t *testing.T) {
	cases := []struct {
		dsn, wantOptions string
	}{
		{"postgres://u:p@h/db?options=-c%20a.b%3D1&schema=s", "-c a.b=1"},
		{"postgres://u:p@h/db?options=-c%20a.b%3D1%2B2&schema=s", "-c a.b=1+2"},
		{"postgres://u:p@h/db?options=-csearch_path%3Dapp%20-cstatement_timeout%3D5&schema=s", "-csearch_path=app -cstatement_timeout=5"},
	}
	for _, tc := range cases {
		cfg, err := parseURIDSN(tc.dsn)
		if err != nil {
			t.Fatalf("parseURIDSN(%q): %v", tc.dsn, err)
		}
		if cfg.schema != "s" {
			t.Errorf("schema = %q, want s", cfg.schema)
		}
		if strings.Contains(cfg.dsn, "+") {
			t.Errorf("driver DSN %q contains '+', which pgx >= 5.11 reads as a literal plus", cfg.dsn)
		}
		pc, err := pgx.ParseConfig(cfg.dsn)
		if err != nil {
			t.Fatalf("pgx.ParseConfig(%q): %v", cfg.dsn, err)
		}
		if got := pc.RuntimeParams["options"]; got != tc.wantOptions {
			t.Errorf("pgx options = %q, want %q (DSN: %q)", got, tc.wantOptions, cfg.dsn)
		}
	}
}
