// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0153 statement-protocol flavor default: the
// PlanetScale / Vitess flavors resolve to client-side interpolation
// (`interpolateParams=true`) at the parseDSNForFlavor choke point, vanilla
// stays on the binary protocol, and an explicit DSN setting always wins.
// Pinned through REAL DSN strings and the real mysql.ParseDSN path — the
// Bug-180 lesson: a resolution branch that only fires for an omitted/empty
// value must be pinned through the actual parser, not a hand-built config.

package mysql

import (
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// TestInterpolationDefault_FlavorDSNMatrix is the flavor × DSN-param
// resolution table: {mysql, planetscale, vitess} × {no param, explicit
// false, explicit true} through parseDSNForFlavor with real DSN strings.
func TestInterpolationDefault_FlavorDSNMatrix(t *testing.T) {
	cases := []struct {
		flavor Flavor
		param  string // appended to the DSN verbatim
		want   bool
	}{
		{FlavorVanilla, "", false},
		{FlavorVanilla, "&interpolateParams=false", false},
		{FlavorVanilla, "&interpolateParams=true", true},
		{FlavorPlanetScale, "", true},                          // the ADR-0153 default
		{FlavorPlanetScale, "&interpolateParams=false", false}, // explicit wins
		{FlavorPlanetScale, "&interpolateParams=true", true},
		{FlavorVitess, "", true},
		{FlavorVitess, "&interpolateParams=false", false},
		{FlavorVitess, "&interpolateParams=true", true},
	}
	for _, c := range cases {
		dsn := "user:pw@tcp(db.example.com:3306)/appdb?tls=true" + c.param
		cfg, err := parseDSNForFlavor(dsn, c.flavor)
		if err != nil {
			t.Errorf("%s %q: parse error: %v", c.flavor, c.param, err)
			continue
		}
		if cfg.InterpolateParams != c.want {
			t.Errorf("%s with param %q: InterpolateParams = %v; want %v", c.flavor, c.param, cfg.InterpolateParams, c.want)
		}
	}

	// The server-DSN sibling resolves identically (a database-less
	// multi-DB / EnsureDatabase probe under a VStream flavor).
	cfg, err := parseServerDSNForFlavor("user:pw@tcp(db.example.com:3306)/", FlavorPlanetScale)
	if err != nil {
		t.Fatalf("parseServerDSNForFlavor: %v", err)
	}
	if !cfg.InterpolateParams {
		t.Error("parseServerDSNForFlavor(planetscale, no param): InterpolateParams = false; want the flavor default")
	}
}

// TestDSNSetsInterpolateParams pins the explicitness detector against the
// DSN anatomy traps: the driver consumes the param at ParseDSN (losing
// explicit-false vs absent), so the detector inspects the raw string — and
// must mirror the driver's own "everything after the LAST '/'" rule so
// passwords and socket paths containing '/', '?', or the literal param name
// never count as explicit.
func TestDSNSetsInterpolateParams(t *testing.T) {
	cases := []struct {
		dsn  string
		want bool
	}{
		{"u:p@tcp(h:3306)/db", false},
		{"u:p@tcp(h:3306)/db?interpolateParams=true", true},
		{"u:p@tcp(h:3306)/db?interpolateParams=false", true},
		{"u:p@tcp(h:3306)/db?interpolateParams=0", true},
		{"u:p@tcp(h:3306)/db?tls=true&interpolateParams=False&loc=UTC", true},
		// Key-prefix / key-suffix lookalikes are NOT the driver param.
		{"u:p@tcp(h:3306)/db?xinterpolateParams=true", false},
		{"u:p@tcp(h:3306)/db?interpolateParamsX=true", false},
		// A value-less segment is ignored by ParseDSN — not explicit.
		{"u:p@tcp(h:3306)/db?interpolateParams", false},
		// Password containing '?' + the param name: the query section
		// starts after the LAST '/', so this is not explicit.
		{"u:pa?interpolateParams=true@tcp(h:3306)/db", false},
		// Unix socket path with '/'s.
		{"u:p@unix(/tmp/my.sock)/db?charset=utf8mb4", false},
		{"u:p@unix(/tmp/my.sock)/db?interpolateParams=true", true},
		// No slash at all (invalid DSN shape — never explicit).
		{"not-a-dsn", false},
	}
	for _, c := range cases {
		if got := dsnSetsInterpolateParams(c.dsn); got != c.want {
			t.Errorf("dsnSetsInterpolateParams(%q) = %v; want %v", c.dsn, got, c.want)
		}
	}
}

// TestInterpolationDefault_UnsafeCollationSkipsDefault pins the graceful
// step-aside: a VStream-flavor DSN pinning a driver-denylisted collation
// keeps the binary protocol (WARN, not refusal) — the perf default must
// never turn a previously-working configuration into a connect failure.
func TestInterpolationDefault_UnsafeCollationSkipsDefault(t *testing.T) {
	dsn := "user:pw@tcp(db.example.com:3306)/appdb?collation=gbk_chinese_ci"
	cfg, err := parseDSNForFlavor(dsn, FlavorPlanetScale)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.InterpolateParams {
		t.Error("unsafe collation: InterpolateParams = true; the flavor default must step aside (the driver would refuse the connector)")
	}
}

// TestInterpolationDefault_ExplicitTrueUnsafeCollation_RefusedByDriver pins
// the operator-explicit unsafe combination: the DRIVER refuses it at
// ParseDSN, loudly, before sluice's default logic ever runs — the loud
// failure the graceful step-aside above deliberately does not replicate.
// Pinned on EVERY flavor: the guards key on the resolved interpolation
// state, not the flavor, so a vanilla-MySQL operator taking the documented
// DSN opt-in (docs/throughput-tuning.md) gets the identical protection.
func TestInterpolationDefault_ExplicitTrueUnsafeCollation_RefusedByDriver(t *testing.T) {
	dsn := "user:pw@tcp(db.example.com:3306)/appdb?collation=sjis_bin&interpolateParams=true"
	for _, flavor := range []Flavor{FlavorVanilla, FlavorPlanetScale, FlavorVitess} {
		_, err := parseDSNForFlavor(dsn, flavor)
		if err == nil {
			t.Fatalf("%s: explicit interpolateParams=true + unsafe collation parsed clean; want the driver's loud refusal", flavor)
		}
		if !strings.Contains(err.Error(), "unsafe collations") {
			t.Errorf("%s: refusal %q; want the driver's 'unsafe collations' message", flavor, err)
		}
	}
}

// TestInterpolationUnsafeCollations_SubsetOfDriver pins sluice's copy of the
// driver's unexported denylist against the driver itself: every entry we
// step aside for must genuinely be refused by mysql.ParseDSN when combined
// with interpolateParams=true (our list ⊆ the driver's), and a safe
// collation must not be. If the driver DROPS a collation from its denylist,
// this pin fails on the stale entry and the copy gets pruned; if the driver
// ADDS one we lack, the flavor default on such a DSN fails loudly at
// connector build (never silently) — see interpolationUnsafeCollations.
func TestInterpolationUnsafeCollations_SubsetOfDriver(t *testing.T) {
	for collation := range interpolationUnsafeCollations {
		dsn := "u:p@tcp(h:3306)/db?interpolateParams=true&collation=" + collation
		if _, err := mysql.ParseDSN(dsn); err == nil {
			t.Errorf("collation %q: driver accepted interpolateParams=true; sluice's denylist copy has a stale entry", collation)
		}
	}
	if _, err := mysql.ParseDSN("u:p@tcp(h:3306)/db?interpolateParams=true&collation=utf8mb4_general_ci"); err != nil {
		t.Errorf("safe collation refused by driver: %v", err)
	}
	if interpolationUnsafeCollations["utf8mb4_general_ci"] {
		t.Error("sluice's denylist wrongly contains utf8mb4_general_ci")
	}
}

// TestWithDatabase_PreservesExplicitInterpolateFalse pins the ADR-0153
// explicit-DSN-wins contract across the WithDatabase FormatDSN round-trip:
// FormatDSN omits default-valued params, so an explicit
// interpolateParams=false must be re-materialized on the derived DSN or a
// downstream flavor-aware parse would re-apply the default the operator
// opted out of.
func TestWithDatabase_PreservesExplicitInterpolateFalse(t *testing.T) {
	derived, err := Engine{Flavor: FlavorPlanetScale}.WithDatabase(
		"user:pw@tcp(db.example.com:3306)/appdb?interpolateParams=false", "otherdb",
	)
	if err != nil {
		t.Fatalf("WithDatabase: %v", err)
	}
	if !dsnSetsInterpolateParams(derived) {
		t.Fatalf("derived DSN %q lost the explicit interpolateParams=false", derived)
	}
	cfg, err := parseDSNForFlavor(derived, FlavorPlanetScale)
	if err != nil {
		t.Fatalf("re-parse derived DSN: %v", err)
	}
	if cfg.InterpolateParams {
		t.Errorf("derived DSN %q resolved to interpolation; the operator's explicit false must survive the round-trip", derived)
	}

	// Explicit true survives via FormatDSN's own non-default emission.
	derived, err = Engine{Flavor: FlavorVanilla}.WithDatabase(
		"user:pw@tcp(db.example.com:3306)/appdb?interpolateParams=true", "otherdb",
	)
	if err != nil {
		t.Fatalf("WithDatabase(true): %v", err)
	}
	cfg, err = parseDSNForFlavor(derived, FlavorVanilla)
	if err != nil {
		t.Fatalf("re-parse derived DSN: %v", err)
	}
	if !cfg.InterpolateParams {
		t.Errorf("derived DSN %q lost the explicit interpolateParams=true", derived)
	}

	// And the common no-param case stays clean: nothing materialized, the
	// downstream flavor parse decides (vanilla → binary, VStream → interp).
	derived, err = Engine{Flavor: FlavorPlanetScale}.WithDatabase(
		"user:pw@tcp(db.example.com:3306)/appdb", "otherdb",
	)
	if err != nil {
		t.Fatalf("WithDatabase(no param): %v", err)
	}
	if dsnSetsInterpolateParams(derived) {
		t.Errorf("derived DSN %q grew an interpolateParams param the operator never set", derived)
	}
}
