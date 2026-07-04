// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestPinStandardConformingStrings pins the SEC-1 session hardening: every
// pgx pool sluice opens forces standard_conforming_strings=on (the setting
// sluice's DDL emitters assume when quoting string literals by doubling
// quotes only), while an explicit operator DSN value wins — the same
// two-tier override shape as the MySQL engine's sql_mode injection.
func TestPinStandardConformingStrings(t *testing.T) {
	cfg, err := pgx.ParseConfig("postgres://u:p@localhost:5432/db")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	pinStandardConformingStrings(cfg)
	if got := cfg.RuntimeParams["standard_conforming_strings"]; got != "on" {
		t.Errorf("standard_conforming_strings = %q; want pinned to \"on\"", got)
	}

	// Operator DSN override is respected (they take on the hazard themselves).
	cfg, err = pgx.ParseConfig("postgres://u:p@localhost:5432/db?standard_conforming_strings=off")
	if err != nil {
		t.Fatalf("ParseConfig(with override): %v", err)
	}
	pinStandardConformingStrings(cfg)
	if got := cfg.RuntimeParams["standard_conforming_strings"]; got != "off" {
		t.Errorf("standard_conforming_strings = %q; want the operator's explicit \"off\" preserved", got)
	}
}
