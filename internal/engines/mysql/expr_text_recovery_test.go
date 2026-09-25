// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// isHex decodes a HEX(information_schema …) value measured on a real server.
func isHex(t *testing.T, h string) string {
	t.Helper()
	b, err := hex.DecodeString(h)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestRecoverExprText_MeasuredMySQL pins the MySQL arm against the exact
// bytes MySQL 8.0.46 / 8.4 returned (HEX(COLUMN_DEFAULT) and the SHOW CREATE
// TABLE lines, 2026-09-24): every family of non-ASCII — two-byte (é, ß),
// three-byte (中), four-byte (😀), a function-wrapped expression, and an
// escaped apostrophe — recovers to the UTF-8 the column was declared with,
// and the four-byte case recovers although SHOW CREATE prints it as '????'.
func TestRecoverExprText_MeasuredMySQL(t *testing.T) {
	cases := []struct {
		name, catalogHex, line, want string
	}{
		{"two-byte", "5F757466386D62345C27C383C2A95C27", "`a` varchar(40) DEFAULT (_utf8mb4'é'),", `_utf8mb4\'é\'`},
		{
			"function, two and three byte", "636F6E636174285F757466386D62345C27C383C29F5C272C5F757466386D62345C27C3A4C2B8C2AD5C2729",
			"`b` varchar(40) DEFAULT (concat(_utf8mb4'ß',_utf8mb4'中')),", `concat(_utf8mb4\'ß\',_utf8mb4\'中\')`,
		},
		{"four-byte", "5F757466386D62345C27C3B0C29FC298C280785C27", "`c` varchar(40) DEFAULT (_utf8mb4'????x'),", `_utf8mb4\'😀x\'`},
		{
			"json", "6A736F6E5F6F626A656374285F757466386D62345C276B5C272C5F757466386D62345C27C383C2A95C2729",
			"`j` json DEFAULT (json_object(_utf8mb4'k',_utf8mb4'é')),", `json_object(_utf8mb4\'k\',_utf8mb4\'é\')`,
		},
		{"apostrophe", "5F757466386D62345C2769745C5C5C277320C383C2A95C27", "`a` varchar(40) DEFAULT (_utf8mb4'it\\'s é'),", `_utf8mb4\'it\\\'s é\'`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			catalog := isHex(t, c.catalogHex)
			if !exprTextNeedsRecovery(FlavorVanilla, catalog) {
				t.Fatalf("%q does not trigger recovery", catalog)
			}
			col := strings.Trim(strings.Fields(c.line)[0], "`")
			ddl := "CREATE TABLE `t` (\n  " + c.line + "\n  PRIMARY KEY (`id`)\n)"
			got, err := recoverExprText(FlavorVanilla, ddl, pendingExprText{kind: exprSiteColumn, name: col, catalog: catalog})
			if err != nil {
				t.Fatalf("recoverExprText: %v", err)
			}
			if got != c.want {
				t.Fatalf("recovered %q; want %q", got, c.want)
			}
		})
	}
}

// TestRecoverExprText_MySQLRefusals pins the three ways the MySQL arm
// refuses rather than carrying a guess.
func TestRecoverExprText_MySQLRefusals(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `a` varchar(40) DEFAULT (_utf8mb4'é'),\n  `v` varchar(40) DEFAULT (_latin1'?')\n)"
	cases := []struct {
		name, col, catalog, want string
	}{
		// A table created from a latin1 session stores the byte E9, which
		// information_schema renders as é; undone, it is not UTF-8.
		{"not UTF-8 once undone", "v", isHex(t, "5F6C6174696E315C27C3A95C27"), "not the Latin-1 re-encoding"},
		// A character above U+00FF cannot be a Latin-1 reading of a byte.
		{"not double-encoded", "a", `_utf8mb4\'中\'`, "not the Latin-1 re-encoding"},
		// Double-encoded and valid, but SHOW CREATE says otherwise.
		{"disagrees with SHOW CREATE", "a", isHex(t, "5F757466386D62345C27C383C29F5C27"), "SHOW CREATE TABLE does not carry"},
		{"no line", "missing", isHex(t, "5F757466386D62345C27C383C2A95C27"), "no line"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := recoverExprText(FlavorVanilla, ddl, pendingExprText{kind: exprSiteColumn, name: c.col, catalog: c.catalog})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v; want one containing %q", err, c.want)
			}
		})
	}
}

// TestRecoverExprText_MeasuredMariaDB pins the MariaDB arm against MariaDB
// 11.4's measured text: information_schema writes one '?' per byte of a
// character beyond the BMP, and SHOW CREATE TABLE is faithful — for an
// expression default, a generated column and a CHECK.
func TestRecoverExprText_MeasuredMariaDB(t *testing.T) {
	ddl := strings.Join([]string{
		"CREATE TABLE `g` (",
		"  `a` varchar(40) GENERATED ALWAYS AS (concat('é😀',`id`)) VIRTUAL,",
		"  `c` varchar(40) DEFAULT concat('😀x',''),",
		"  `q` varchar(40) DEFAULT concat('?',''),",
		"  CONSTRAINT `CONSTRAINT_1` CHECK (`b` <> '😀é')",
		")",
	}, "\n")
	cases := []struct {
		name    string
		kind    exprSiteKind
		obj     string
		catalog string
		want    string
	}{
		{"generated", exprSiteColumn, "a", isHex(t, "636F6E6361742827C3A93F3F3F3F272C6069646029"), "concat('é😀',`id`)"},
		{"default", exprSiteColumn, "c", isHex(t, "636F6E63617428273F3F3F3F78272C272729"), "concat('😀x','')"},
		{"check", exprSiteCheck, "CONSTRAINT_1", isHex(t, "606260203C3E20273F3F3F3FC3A927"), "`b` <> '😀é'"},
		{"genuine question mark", exprSiteColumn, "q", "concat('?','')", "concat('?','')"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !exprTextNeedsRecovery(FlavorMariaDB, c.catalog) {
				t.Fatalf("%q does not trigger recovery", c.catalog)
			}
			got, err := recoverExprText(FlavorMariaDB, ddl, pendingExprText{kind: c.kind, name: c.obj, catalog: c.catalog})
			if err != nil {
				t.Fatalf("recoverExprText: %v", err)
			}
			if got != c.want {
				t.Fatalf("recovered %q; want %q", got, c.want)
			}
		})
	}
	// A catalog text the line does not carry is refused.
	if _, err := recoverExprText(FlavorMariaDB, ddl, pendingExprText{kind: exprSiteColumn, name: "c", catalog: "concat('????y','')"}); err == nil {
		t.Fatal("a MariaDB catalog text matching nothing on its line was accepted")
	}
}

// TestExprTextNeedsRecovery pins the trigger: MySQL on any byte ≥ 0x80 (a
// double-encoded text never needs a '?'), MariaDB on any '?'. ASCII-only
// MySQL expressions and '?'-free MariaDB ones pay nothing.
func TestExprTextNeedsRecovery(t *testing.T) {
	for _, c := range []struct {
		flavor Flavor
		text   string
		want   bool
	}{
		{FlavorVanilla, `_utf8mb4\'plain\'`, false},
		{FlavorVanilla, `_utf8mb4\'?\'`, false},
		{FlavorVanilla, "_utf8mb4\\'Ã©\\'", true},
		{FlavorPlanetScale, "_utf8mb4\\'Ã©\\'", true},
		{FlavorMariaDB, "concat('é','')", false},
		{FlavorMariaDB, "concat('????','')", true},
	} {
		if got := exprTextNeedsRecovery(c.flavor, c.text); got != c.want {
			t.Errorf("exprTextNeedsRecovery(%v, %q) = %v; want %v", c.flavor, c.text, got, c.want)
		}
	}
}

// TestAppendColumnExprPending_SetterReTranslates pins that a recovered
// expression default lands in the IR through the SAME translation the scan
// used — a portable expression with the recovered text — and that a
// literal default (text_default_recovery.go's) is not queued here.
func TestAppendColumnExprPending_SetterReTranslates(t *testing.T) {
	col := &ir.Column{Name: "a", Type: ir.Varchar{Length: 40}}
	def := sql.NullString{String: isHex(t, "5F757466386D62345C27C383C2A95C27"), Valid: true}
	col.Default = FlavorVanilla.translateColumnDefault(def, "DEFAULT_GENERATED", col.Type)
	pending := appendColumnExprPending(nil, FlavorVanilla, "t", col, def, "DEFAULT_GENERATED", "")
	if len(pending) != 1 {
		t.Fatalf("queued %d; want 1", len(pending))
	}
	pending[0].set(`_utf8mb4\'é\'`)
	got, ok := col.Default.(ir.DefaultExpression)
	if !ok || got.Expr != "'é'" {
		t.Fatalf("Default = %#v; want the portable expression 'é'", col.Default)
	}
	lit := sql.NullString{String: "é", Valid: true}
	if n := len(appendColumnExprPending(nil, FlavorVanilla, "t", col, lit, "", "")); n != 0 {
		t.Fatalf("a literal default was queued for expression recovery (%d)", n)
	}
}
