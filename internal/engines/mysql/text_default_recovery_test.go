// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestReconcileTextDefault pins the premise binding of the GC-37 (h)
// recovery: the probed text must be what the catalog would have shown,
// with only supplementary characters allowed to read back as 1-4 '?'.
func TestReconcileTextDefault(t *testing.T) {
	cases := []struct {
		catalog, probed string
		ok              bool
	}{
		{"?x", "😀x", true},                   // MySQL / MariaDB VARCHAR: one '?'
		{"????z", "😀z", true},                // MariaDB TEXT: one '?' per byte
		{"??", "😀", true},                    // two '?' for one character
		{"?", "?", true},                     // a genuine '?'
		{"???", "?😀?", true},                 // genuine '?' around a lost one
		{"?????", "😀?", true},                // four for the character, then a genuine one
		{"é?", "é😀", true},                   // BMP survives verbatim
		{"?", "é", false},                    // a BMP character never reads back as '?'
		{"?x", "😀y", false},                  // disagrees outside the '?' run
		{"?x", "😀", false},                   // probe shorter than the catalog
		{"?", "😀x", false},                   // probe longer than the catalog
		{"??????", "😀", false},               // more '?' than one character can explain
		{"ab", "ab", true},                   // no '?' at all
		{"?", "", false},                     // an empty probe never explains a '?'
		{"x?", "x😀😀", false},                 // one '?' cannot stand for two characters
		{"x??", "x😀😀", true},                 // one each
		{strings.Repeat("?", 8), "😀😀", true}, // four each
	}
	for _, c := range cases {
		err := reconcileTextDefault(c.catalog, c.probed)
		if (err == nil) != c.ok {
			t.Errorf("reconcileTextDefault(%q, %q) = %v; want ok=%v", c.catalog, c.probed, err, c.ok)
		}
	}
}

// TestTextDefaultCatalogText pins the trigger: a literal default with a
// '?' on a character column, on either flavor, and nothing else.
func TestTextDefaultCatalogText(t *testing.T) {
	valid := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	cases := []struct {
		name    string
		flavor  Flavor
		charset string
		extra   string
		def     sql.NullString
		text    string
		probe   bool
	}{
		{"mysql literal with ?", FlavorVanilla, "utf8mb4", "", valid("?x"), "?x", true},
		{"mysql literal without ?", FlavorVanilla, "utf8mb4", "", valid("abc"), "abc", false},
		{"mysql expression", FlavorVanilla, "utf8mb4", "DEFAULT_GENERATED", valid("_utf8mb4\\'?\\'"), "", false},
		{"mysql binary column (no charset)", FlavorVanilla, "", "", valid("0x3F"), "", false},
		{"mysql NULL", FlavorVanilla, "utf8mb4", "", sql.NullString{}, "", false},
		{"mariadb quoted with ?", FlavorMariaDB, "utf8mb4", "", valid("'?x'"), "?x", true},
		{"mariadb quote doubling", FlavorMariaDB, "utf8mb4", "", valid("'it''s?'"), "it's?", true},
		{"mariadb NULL keyword", FlavorMariaDB, "utf8mb4", "", valid("NULL"), "", false},
		{"mariadb expression", FlavorMariaDB, "utf8mb4", "", valid("concat('?','a')"), "", false},
		{"mariadb binary column", FlavorMariaDB, "", "", valid("'?'"), "", false},
		{"mariadb malformed quote", FlavorMariaDB, "utf8mb4", "", valid("'?"), "", false},
	}
	for _, c := range cases {
		text, probe := textDefaultCatalogText(c.flavor, c.charset, c.extra, c.def)
		if probe != c.probe || (probe && text != c.text) {
			t.Errorf("%s: textDefaultCatalogText = (%q, %v); want (%q, %v)", c.name, text, probe, c.text, c.probe)
		}
	}
}

// TestCheckEnumSetDefault pins the label check: a recovered ENUM/SET
// default that the (lossy) labels do not contain is refused, because it
// means the TYPE was read lossily too (GC-37 (i)).
func TestCheckEnumSetDefault(t *testing.T) {
	if err := checkEnumSetDefault("?b", []string{"a", "?b"}, false); err != nil {
		t.Errorf("a genuine '?' label: %v", err)
	}
	if err := checkEnumSetDefault("😀b", []string{"a", "?b"}, false); err == nil ||
		!strings.Contains(err.Error(), "cannot read this column's type faithfully") {
		t.Errorf("ENUM default outside the labels as read: err = %v; want the type refusal", err)
	}
	if err := checkEnumSetDefault("a,?s", []string{"a", "?s"}, true); err != nil {
		t.Errorf("SET members all labels: %v", err)
	}
	if err := checkEnumSetDefault("a,😀s", []string{"a", "?s"}, true); err == nil {
		t.Error("SET member outside the labels as read: want a refusal")
	}
	if err := checkEnumSetDefault("", []string{"a"}, true); err != nil {
		t.Errorf("the empty SET: %v", err)
	}
}

// TestRecoverTextDefaults_RefusesLoudly pins that the recovery never
// guesses: a probe that disagrees with the catalog, an ENUM default the
// labels do not hold, or a failed probe all end the read with an error
// naming the column — none of them leaves the '?' text in place.
func TestRecoverTextDefaults_RefusesLoudly(t *testing.T) {
	ctx := context.Background()
	for i, c := range []struct {
		name  string
		row   catalogColumnRow
		want  string
		check bool
	}{
		{
			name: "probe disagrees with the catalog",
			row:  catalogColumnRow{name: "x", dataType: "varchar", columnType: "varchar(10)", probeUTF8Hex: "C3A978"}, // 'éx' for catalog '?x'
			want: "does not agree with information_schema",
		},
		{
			name:  "ENUM default outside the labels as read",
			row:   catalogColumnRow{name: "x", dataType: "enum", columnType: "enum('a','?b')", probeUTF8Hex: "F09F988062"},
			want:  "cannot read this column's type faithfully",
			check: true,
		},
		{
			name: "probe not answered",
			row:  catalogColumnRow{name: "x", dataType: "varchar", columnType: "varchar(10)"},
			want: "probe the true values",
		},
	} {
		db := newCatalogFakeDB(t, fmt.Sprintf("sluice-text-refuse-%d", i), []catalogColumnRow{c.row})
		var got string
		p := pendingTextDefault{table: "w", column: "x", catalog: "?x", set: func(s string) { got = s }}
		if c.check {
			p.catalog = "?b"
			p.check = enumSetDefaultCheck(ir.Enum{Values: []string{"a", "?b"}})
		}
		err := recoverTextDefaults(ctx, db, "src", FlavorVanilla, []pendingTextDefault{p})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v; want one containing %q", c.name, err, c.want)
		}
		if err != nil && !strings.Contains(err.Error(), "src.w.x") && c.want != "probe the true values" {
			t.Errorf("%s: err = %v; want it to name the column src.w.x", c.name, err)
		}
		if got != "" {
			t.Errorf("%s: the setter ran with %q despite the refusal", c.name, got)
		}
	}
}

// TestRecoverTextDefaults_VTGateReadsShowCreate pins the PlanetScale /
// Vitess path: vtgate rejects the DEFAULT() probe, so those flavors
// recover from SHOW CREATE TABLE, which mysqld prints as a 0x… literal of
// the column's bytes for a default utf8mb3 cannot hold. A utf8-family
// column decodes; any other charset is taken only when SHOW CREATE agrees
// with the catalog exactly, and is otherwise refused.
func TestRecoverTextDefaults_VTGateReadsShowCreate(t *testing.T) {
	ctx := context.Background()
	rows := []catalogColumnRow{
		{name: "vc", columnType: "varchar(20)", showCreate: "0xF09F988078"},
		{name: "q", columnType: "varchar(20)", showCreate: "'?'"},
		{name: "u16q", columnType: "varchar(10)", showCreate: "'?'"},
		{name: "u16", columnType: "varchar(10)", showCreate: "0xD83DDE000077"},
	}
	for _, flavor := range []Flavor{FlavorPlanetScale, FlavorVitess} {
		db := newCatalogFakeDB(t, fmt.Sprintf("sluice-text-vtgate-%d", flavor), rows)
		got := map[string]string{}
		pend := func(col, catalog, charset string) pendingTextDefault {
			return pendingTextDefault{
				table: "w", column: col, catalog: catalog, charset: charset,
				set: func(s string) { got[col] = s },
			}
		}
		ok := []pendingTextDefault{pend("vc", "?x", "utf8mb4"), pend("q", "?", "utf8mb4"), pend("u16q", "?", "utf16")}
		if err := recoverTextDefaults(ctx, db, "src", flavor, ok); err != nil {
			t.Fatalf("flavor %d: %v", flavor, err)
		}
		for col, want := range map[string]string{"vc": "😀x", "q": "?", "u16q": "?"} {
			if got[col] != want {
				t.Errorf("flavor %d: %s recovered %q; want %q", flavor, col, got[col], want)
			}
		}
		err := recoverTextDefaults(ctx, db, "src", flavor, []pendingTextDefault{pend("u16", "?w", "utf16")})
		if err == nil || !strings.Contains(err.Error(), "sluice decodes only UTF-8 there") || !strings.Contains(err.Error(), "src.w.u16") {
			t.Errorf("flavor %d: a utf16 default SHOW CREATE prints as UTF-16 bytes: err = %v; want the named refusal", flavor, err)
		}
	}
}
