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
// bytes MySQL 8.0.46 / 8.4 returned (HEX(information_schema …) and the
// SHOW CREATE TABLE lines, 2026-09-24). utf8mb4 literals of every UTF-8
// length recover to the text they declare, the four-byte one although SHOW
// CREATE prints it as '????'. latin1 literals — a table created from a
// latin1 session — recover to their latin1 VALUE: bytes C3A9 are 'Ã©', not
// 'é' (the pre-review regression, F1), and the byte E9 is 'é' rather than a
// refusal (F3). The expected values are what the server's own INSERT stored
// (hex(a) = C383C2A9, hex(e) = 636166C3A9, measured).
func TestRecoverExprText_MeasuredMySQL(t *testing.T) {
	cases := []struct {
		name, catalogHex string
		kind             exprSiteKind
		line, want       string
	}{
		{"utf8mb4 two-byte", "5F757466386D62345C27C383C2A95C27", exprSiteDefault, "`a` varchar(40) DEFAULT (_utf8mb4'é'),", `_utf8mb4\'é\'`},
		{
			"utf8mb4 function, two and three byte",
			"636F6E636174285F757466386D62345C27C383C29F5C272C5F757466386D62345C27C3A4C2B8C2AD5C2729", exprSiteDefault,
			"`b` varchar(40) DEFAULT (concat(_utf8mb4'ß',_utf8mb4'中')),", `concat(_utf8mb4\'ß\',_utf8mb4\'中\')`,
		},
		{"utf8mb4 four-byte", "5F757466386D62345C27C3B0C29FC298C280785C27", exprSiteDefault, "`c` varchar(40) DEFAULT (_utf8mb4'????x'),", `_utf8mb4\'😀x\'`},
		{
			"json", "6A736F6E5F6F626A656374285F757466386D62345C276B5C272C5F757466386D62345C27C383C2A95C2729", exprSiteDefault,
			"`j` json DEFAULT (json_object(_utf8mb4'k',_utf8mb4'é')),", `json_object(_utf8mb4\'k\',_utf8mb4\'é\')`,
		},
		{"apostrophe", "5F757466386D62345C2769745C5C5C277320C383C2A95C27", exprSiteDefault, "`a` varchar(40) DEFAULT (_utf8mb4'it\\'s é'),", `_utf8mb4\'it\\\'s é\'`},
		{"utf8mb3 (N'')", "5F757466386D62335C27C383C2A95C27", exprSiteDefault, "`a` varchar(20) DEFAULT (_utf8mb3'é'),", `_utf8mb3\'é\'`},
		{"latin1 C3A9 is Ã©", "5F6C6174696E315C27C383C2A95C27", exprSiteDefault, "`a` varchar(20) DEFAULT (_latin1'é'),", `_latin1\'Ã©\'`},
		{"latin1 E9 is é", "5F6C6174696E315C27636166C3A95C27", exprSiteDefault, "`e` varchar(20) DEFAULT (_latin1'caf?'),", `_latin1\'café\'`},
		{
			"latin1 virtual generated (SHOW CREATE prints the bytes)", "636F6E636174285F6C6174696E315C27C383C2A95C272C6069646029", exprSiteGenerated,
			"`g` varchar(20) GENERATED ALWAYS AS (concat(_latin1'é',`id`)) VIRTUAL,", "concat(_latin1\\'Ã©\\',`id`)",
		},
		{
			"latin1 stored generated (SHOW CREATE prints the value)", "636F6E636174285F6C6174696E315C27C383C2A95C272C6069646029", exprSiteGenerated,
			"`g` varchar(20) GENERATED ALWAYS AS (concat(_utf8mb4'Ã©',`id`)) STORED,", "concat(_latin1\\'Ã©\\',`id`)",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			catalog := isHex(t, c.catalogHex)
			if !exprTextNeedsRecovery(FlavorVanilla, catalog) {
				t.Fatalf("%q does not trigger recovery", catalog)
			}
			col := strings.Trim(strings.Fields(c.line)[0], "`")
			ddl := "CREATE TABLE `t` (\n  " + c.line + "\n  PRIMARY KEY (`id`)\n)"
			got, err := recoverExprText(FlavorVanilla, ddl, pendingExprText{kind: c.kind, name: col, catalog: catalog})
			if err != nil {
				t.Fatalf("recoverExprText: %v", err)
			}
			if got != c.want {
				t.Fatalf("recovered %q; want %q", got, c.want)
			}
		})
	}
	// And a latin1 CHECK, measured: CHECK (a <> 'xÃ©') from a latin1 session.
	ddl := "CREATE TABLE `l1` (\n  `a` varchar(20),\n  CONSTRAINT `ck` CHECK ((`a` <> _latin1'xé'))\n)"
	got, err := recoverExprText(FlavorVanilla, ddl, pendingExprText{kind: exprSiteCheck, name: "ck", catalog: isHex(t, "28606160203C3E205F6C6174696E315C2778C383C2A95C2729")})
	if err != nil || got != "(`a` <> _latin1\\'xÃ©\\')" {
		t.Fatalf("latin1 CHECK recovered %q, %v; want (`a` <> _latin1\\'xÃ©\\')", got, err)
	}
}

// TestRecoverExprText_MySQLRefusals pins every way the MySQL arm refuses
// rather than carrying a guess.
func TestRecoverExprText_MySQLRefusals(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `a` varchar(40) DEFAULT (_utf8mb4'é'),\n  `k` varchar(20) DEFAULT (_gbk'é'),\n  `c1` varchar(20) DEFAULT (_latin1'?')\n)"
	cases := []struct {
		name, col, catalog, want string
	}{
		// F2: gbk bytes C3A9 are 茅 (the server's INSERT stored E88C85); the
		// bytes happen to be valid UTF-8 and SHOW CREATE agrees, so only the
		// introducer can refuse it.
		{"gbk", "k", isHex(t, "5F67626B5C27C383C2A95C27"), "character set gbk"},
		// MySQL's latin1 is cp1252: byte 0x80 is € (measured E282AC).
		{"latin1 cp1252 range", "c1", isHex(t, "5F6C6174696E315C27C2805C27"), "cp1252"},
		// A character above U+00FF cannot be a widened byte.
		{"not widened", "a", `_utf8mb4\'中\'`, "one-character-per-byte"},
		// Widened and valid, but SHOW CREATE says otherwise.
		{"disagrees with SHOW CREATE", "a", isHex(t, "5F757466386D62345C27C383C29F5C27"), "does not carry"},
		// A utf8mb4 literal whose stored bytes are not UTF-8.
		{"utf8mb4 not UTF-8", "a", "_utf8mb4\\'\u00e9\\'", "not UTF-8"},
		{"no introducer", "a", "\\'\u00c3\u00a9\\'", "no charset introducer"},
		{"no line", "missing", isHex(t, "5F757466386D62345C27C383C2A95C27"), "no line"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := recoverExprText(FlavorVanilla, ddl, pendingExprText{kind: exprSiteDefault, name: c.col, catalog: c.catalog})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v; want one containing %q", err, c.want)
			}
		})
	}
}

// TestRecoverExprText_MeasuredMariaDB pins the MariaDB arm against MariaDB
// 11.4's measured text: information_schema writes one '?' per byte of a
// character beyond the BMP, and SHOW CREATE TABLE is faithful — for an
// expression default, a generated column and a CHECK. The search is
// anchored to each clause (F5): a column carrying a genuine '?' DEFAULT and
// an inline CHECK holding an emoji resolves each to its own clause.
func TestRecoverExprText_MeasuredMariaDB(t *testing.T) {
	ddl := strings.Join([]string{
		"CREATE TABLE `g` (",
		"  `a` varchar(40) GENERATED ALWAYS AS (concat('é😀',`id`)) VIRTUAL,",
		"  `c` varchar(40) DEFAULT concat('😀x',''),",
		"  `q` varchar(40) DEFAULT concat('?','') CHECK (`q` <> concat('😀','')),",
		"  `w` varchar(40) DEFAULT concat('????','') CHECK (`w` <> concat('😀','')),",
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
		{"generated", exprSiteGenerated, "a", isHex(t, "636F6E6361742827C3A93F3F3F3F272C6069646029"), "concat('é😀',`id`)"},
		{"default", exprSiteDefault, "c", isHex(t, "636F6E63617428273F3F3F3F78272C272729"), "concat('😀x','')"},
		{"check", exprSiteCheck, "CONSTRAINT_1", isHex(t, "606260203C3E20273F3F3F3FC3A927"), "`b` <> '😀é'"},
		{"genuine question mark beside an inline CHECK", exprSiteDefault, "q", "concat('?','')", "concat('?','')"},
		{"inline CHECK beside a genuine '?' DEFAULT", exprSiteCheck, "q", "`q` <> concat('????','')", "`q` <> concat('😀','')"},
		// The DEFAULT's catalog text also matches the CHECK's emoji literal;
		// only the clause anchor tells them apart.
		{"genuine '????' DEFAULT beside an emoji CHECK", exprSiteDefault, "w", "concat('????','')", "concat('????','')"},
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
	// A catalog text matching nothing on its line is refused; so is a lost
	// character spelled with the wrong number of '?'.
	for _, bad := range []string{"concat('????y','')", "concat('??x','')"} {
		if _, err := recoverExprText(FlavorMariaDB, ddl, pendingExprText{kind: exprSiteDefault, name: "c", catalog: bad}); err == nil {
			t.Errorf("MariaDB catalog text %q was accepted", bad)
		}
	}
}

// TestExprTextNeedsRecovery pins the trigger: MySQL on any byte ≥ 0x80 (a
// widened text never needs a '?'), MariaDB on any '?'. ASCII-only MySQL
// expressions and '?'-free MariaDB ones pay nothing.
func TestExprTextNeedsRecovery(t *testing.T) {
	for _, c := range []struct {
		flavor Flavor
		text   string
		want   bool
	}{
		{FlavorVanilla, `_utf8mb4\'plain\'`, false},
		{FlavorVanilla, `_utf8mb4\'?\'`, false},
		{FlavorVanilla, "_utf8mb4\\'\u00c3\u00a9\\'", true},
		{FlavorVanilla, "_latin1\\'caf\u00e9\\'", true},
		{FlavorPlanetScale, "_utf8mb4\\'\u00c3\u00a9\\'", true},
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

// TestDecodeMySQLISExprForCompare pins the Shape A probe's decode: a MySQL
// target's widened CHECK_CLAUSE normalizes to the same portable text the
// source read recovered, and an undecodable one reports ok=false (the probe
// then compares the raw text, unequal — loud).
func TestDecodeMySQLISExprForCompare(t *testing.T) {
	got, ok := decodeMySQLISExprForCompare(isHex(t, "28606260203C3E205F757466386D62345C27C383C2A9C3B0C29FC298C2805C2729"))
	if !ok || normalizeMySQLExpressionText(got) != "(b <> 'é😀')" {
		t.Fatalf("decoded %q (%v); want one normalizing to (b <> 'é😀')", got, ok)
	}
	if _, ok := decodeMySQLISExprForCompare(isHex(t, "5F67626B5C27C383C2A95C27")); ok {
		t.Fatal("a gbk literal decoded")
	}
}
