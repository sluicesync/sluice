// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"strings"
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

// TestComposeAfterConnect pins the hook-chaining shape behind the F2
// engine-wide extra_float_digits pin: stdlib's OptionAfterConnect is
// LAST-WINS (it replaces, never chains), so call sites carrying their
// own hook (the applier's geometry-codec registration) must compose
// [afterConnectSessionPins] back in via composeAfterConnect — hooks run
// left to right and the first error short-circuits.
func TestComposeAfterConnect(t *testing.T) {
	var order []string
	mk := func(name string, err error) func(context.Context, *pgx.Conn) error {
		return func(context.Context, *pgx.Conn) error {
			order = append(order, name)
			return err
		}
	}
	if err := composeAfterConnect(mk("a", nil), mk("b", nil))(context.Background(), nil); err != nil {
		t.Fatalf("compose: %v", err)
	}
	if got := strings.Join(order, ","); got != "a,b" {
		t.Errorf("hook order = %q; want a,b", got)
	}

	order = nil
	boom := errors.New("boom")
	err := composeAfterConnect(mk("a", boom), mk("b", nil))(context.Background(), nil)
	if !errors.Is(err, boom) {
		t.Errorf("error = %v; want boom", err)
	}
	if got := strings.Join(order, ","); got != "a" {
		t.Errorf("short-circuit order = %q; want a", got)
	}
}
