// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"path"
	"reflect"
	"strings"
	"testing"
)

func TestStripMySQLComments(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{
			"line comment dropped",
			"-- header comment\nCREATE TABLE a (id int);",
			"\nCREATE TABLE a (id int);",
		},
		{
			"hash comment dropped",
			"# a hash comment\nCREATE TABLE a (id int);",
			"\nCREATE TABLE a (id int);",
		},
		{
			"plain block comment elided",
			"CREATE TABLE a (/* inline */ id int);",
			"CREATE TABLE a (  id int);",
		},
		{
			"conditional comment unwrapped (SET guard)",
			"/*!40101 SET NAMES utf8mb4 */;\nCREATE TABLE a (id int);",
			"  SET NAMES utf8mb4  ;\nCREATE TABLE a (id int);",
		},
		{
			"conditional CHECK constraint unwrapped",
			"CONSTRAINT `c` /*!80016 CHECK ((`x` > 0)) */",
			"CONSTRAINT `c`   CHECK ((`x` > 0))  ",
		},
		{
			"star-slash inside string literal not treated as comment end",
			"INSERT INTO t VALUES ('a */ b');",
			"INSERT INTO t VALUES ('a */ b');",
		},
		{
			"double-dash inside backtick identifier survives",
			"CREATE TABLE `we--ird` (id int);",
			"CREATE TABLE `we--ird` (id int);",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripMySQLComments(tc.in); got != tc.want {
				t.Errorf("stripMySQLComments(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSplitMySQLStatements(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{
			"plain statements",
			"CREATE TABLE a (id int);\nCREATE TABLE b (id int);",
			[]string{"CREATE TABLE a (id int)", "CREATE TABLE b (id int)"},
		},
		{
			"semicolon inside single-quoted literal",
			"INSERT INTO t VALUES ('has; a semicolon');",
			[]string{"INSERT INTO t VALUES ('has; a semicolon')"},
		},
		{
			"backslash-escaped quote does not end literal early",
			`INSERT INTO t VALUES ('it\'s; fine');`,
			[]string{`INSERT INTO t VALUES ('it\'s; fine')`},
		},
		{
			"semicolon inside backtick identifier",
			"CREATE TABLE `odd;name` (id int);",
			[]string{"CREATE TABLE `odd;name` (id int)"},
		},
		{
			"trailing statement without terminator",
			"CREATE TABLE a (id int);\nCREATE TABLE b (id int)",
			[]string{"CREATE TABLE a (id int)", "CREATE TABLE b (id int)"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitMySQLStatements(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("splitMySQLStatements(%q) = %#v; want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestSplitMySQLElements(t *testing.T) {
	inner := "`id` bigint NOT NULL, `price` decimal(10,2) DEFAULT '0.00', PRIMARY KEY (`id`), " +
		"KEY `k` (`a`,`b`), CONSTRAINT `chk` CHECK ((`price` >= 0))"
	want := []string{
		"`id` bigint NOT NULL",
		" `price` decimal(10,2) DEFAULT '0.00'",
		" PRIMARY KEY (`id`)",
		" KEY `k` (`a`,`b`)",
		" CONSTRAINT `chk` CHECK ((`price` >= 0))",
	}
	got := splitMySQLElements(inner)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("splitMySQLElements = %#v; want %#v", got, want)
	}
}

func TestMySQLElementKey(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"column", "`id` bigint unsigned NOT NULL AUTO_INCREMENT", "COLUMN id"},
		{"column with dot-free name", "`region_code` char(2)", "COLUMN region_code"},
		{"primary key", "PRIMARY KEY (`id`)", "PRIMARY KEY"},
		{"unique key", "UNIQUE KEY `email_uidx` (`email`)", "KEY email_uidx"},
		{"unique index spelling", "UNIQUE INDEX `email_uidx` (`email`)", "KEY email_uidx"},
		{"plain key", "KEY `status_idx` (`status`)", "KEY status_idx"},
		{"fulltext key", "FULLTEXT KEY `ft` (`body`)", "KEY ft"},
		{"prefix index key", "KEY `sku_prefix` (`sku`(16))", "KEY sku_prefix"},
		{"fk constraint", "CONSTRAINT `orders_fk` FOREIGN KEY (`cid`) REFERENCES `c` (`id`)", "CONSTRAINT orders_fk"},
		{"check constraint", "CONSTRAINT `chk` CHECK ((`x` >= 0))", "CONSTRAINT chk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mysqlElementKey(tc.in); got != tc.want {
				t.Errorf("mysqlElementKey(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestNormalizeMySQLElement_CosmeticStrip_FidelityPreserving pins the
// two cosmetic normalizations AND, critically (the Bug-74 discipline),
// that they strip ONLY default-equal values — a genuine non-default
// collation or a non-BTREE index method must still survive so a real
// fidelity drop surfaces as a divergence rather than being masked.
func TestNormalizeMySQLElement_CosmeticStrip_FidelityPreserving(t *testing.T) {
	const defCS, defCO = "utf8mb4", "utf8mb4_0900_ai_ci"
	cases := []struct {
		name, key, in, want string
	}{
		{
			"default charset+collation stripped from column",
			"COLUMN email",
			"`email` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL",
			"`email` varchar(255) NOT NULL",
		},
		{
			"non-default charset+collation on column KEPT",
			"COLUMN region_code",
			"`region_code` char(2) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT 'zz'",
			"`region_code` char(2) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT 'zz'",
		},
		{
			"default charset but non-default collation keeps COLLATE",
			"COLUMN body",
			"`body` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
			"`body` text COLLATE utf8mb4_bin",
		},
		{
			"USING BTREE stripped from secondary key",
			"KEY email_uidx",
			"UNIQUE KEY `email_uidx` (`email`) USING BTREE",
			"UNIQUE KEY `email_uidx` (`email`)",
		},
		{
			"USING HASH on key KEPT",
			"KEY h",
			"KEY `h` (`x`) USING HASH",
			"KEY `h` (`x`) USING HASH",
		},
		{
			"constraint body untouched",
			"CONSTRAINT chk",
			"CONSTRAINT `chk` CHECK ((`x` >= 0))",
			"CONSTRAINT `chk` CHECK ((`x` >= 0))",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeMySQLElement(tc.key, tc.in, defCS, defCO); got != tc.want {
				t.Errorf("normalizeMySQLElement(%q, %q) = %q; want %q", tc.key, tc.in, got, tc.want)
			}
		})
	}
}

func TestMySQLTableDefaults(t *testing.T) {
	cases := []struct {
		name, options, wantCS, wantCO string
	}{
		{
			"collate omitted, filled from charset default",
			"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			"utf8mb4", "utf8mb4_0900_ai_ci",
		},
		{
			"explicit collate wins",
			"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"utf8mb4", "utf8mb4_bin",
		},
		{
			"latin1 default",
			"ENGINE=InnoDB DEFAULT CHARSET=latin1",
			"latin1", "latin1_swedish_ci",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs, co := mysqlTableDefaults(tc.options)
			if cs != tc.wantCS || co != tc.wantCO {
				t.Errorf("mysqlTableDefaults(%q) = (%q,%q); want (%q,%q)", tc.options, cs, co, tc.wantCS, tc.wantCO)
			}
		})
	}
}

func TestParseMySQLSchemaDump(t *testing.T) {
	// A representative mysqldump --no-data --skip-comments fragment: the
	// standard header SET guards, a DROP TABLE, and one CREATE TABLE with
	// a column, a PK, a secondary key, an FK, and a version-guarded CHECK.
	dump := "/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n" +
		"/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;\n" +
		"DROP TABLE IF EXISTS `orders`;\n" +
		"CREATE TABLE `orders` (\n" +
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `customer_id` bigint unsigned NOT NULL,\n" +
		"  `subtotal_cents` int NOT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `orders_customer_idx` (`customer_id`),\n" +
		"  CONSTRAINT `orders_customer_fk` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`) ON DELETE CASCADE,\n" +
		"  CONSTRAINT `orders_subtotal_positive` CHECK ((`subtotal_cents` >= 0))\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;\n"

	got := ParseMySQLSchemaDump(dump)
	wantKeys := []string{
		"orders COLUMN id",
		"orders COLUMN customer_id",
		"orders COLUMN subtotal_cents",
		"orders PRIMARY KEY",
		"orders KEY orders_customer_idx",
		"orders CONSTRAINT orders_customer_fk",
		"orders CONSTRAINT orders_subtotal_positive",
		"orders OPTIONS",
	}
	if len(got) != len(wantKeys) {
		t.Fatalf("parsed %d statements; want %d\n%#v", len(got), len(wantKeys), got)
	}
	for i, k := range wantKeys {
		if got[i].Key != k {
			t.Errorf("stmt[%d].Key = %q; want %q", i, got[i].Key, k)
		}
	}

	// The CHECK survived the version-guard unwrap (the vacuous-loss class
	// stripMySQLComments exists to prevent).
	var check *dumpStatement
	for i := range got {
		if got[i].Key == "orders CONSTRAINT orders_subtotal_positive" {
			check = &got[i]
		}
	}
	if check == nil {
		t.Fatal("version-guarded CHECK constraint was eaten by the comment stripper")
	}
	if !strings.Contains(check.Body, "CHECK") {
		t.Errorf("CHECK body lost its keyword: %q", check.Body)
	}

	// AUTO_INCREMENT=<n> is stripped from the options blob (data-derived
	// counter), ENGINE/CHARSET retained.
	var opts *dumpStatement
	for i := range got {
		if got[i].Key == "orders OPTIONS" {
			opts = &got[i]
		}
	}
	if opts == nil {
		t.Fatal("no OPTIONS element")
	}
	if strings.Contains(strings.ToUpper(opts.Body), "AUTO_INCREMENT") {
		t.Errorf("OPTIONS body still carries AUTO_INCREMENT: %q", opts.Body)
	}
	if !strings.Contains(opts.Body, "ENGINE=InnoDB") || !strings.Contains(opts.Body, "CHARSET=utf8mb4") {
		t.Errorf("OPTIONS body dropped a real option: %q", opts.Body)
	}

	// Vacuous-guard counting primitives.
	if n := CountMySQLColumns(got); n != 3 {
		t.Errorf("CountMySQLColumns = %d; want 3", n)
	}
	if n := CountMySQLTables(got); n != 1 {
		t.Errorf("CountMySQLTables = %d; want 1", n)
	}
}

// TestParseMySQLSchemaDump_VacuousGuardCountsElements pins the guard's
// counting primitives: a dump reduced entirely to preamble parses to
// zero elements, which the harness floors against the seed and fails
// loudly instead of reading empty-diff as parity.
func TestParseMySQLSchemaDump_VacuousGuardCountsElements(t *testing.T) {
	empty := ParseMySQLSchemaDump(
		"/*!40101 SET NAMES utf8mb4 */;\nDROP TABLE IF EXISTS `x`;\n/*!40014 SET FOREIGN_KEY_CHECKS=0 */;",
	)
	if len(empty) != 0 {
		t.Fatalf("preamble-only dump: got %d elements; want 0\n%#v", len(empty), empty)
	}
	if CountMySQLColumns(empty) != 0 || CountMySQLTables(empty) != 0 {
		t.Fatalf("preamble-only dump: columns=%d tables=%d; want 0/0",
			CountMySQLColumns(empty), CountMySQLTables(empty))
	}
}

// TestDiffDumpStatements_MySQLElements confirms the shared diff engine
// keys MySQL element statements correctly: an identical table cancels;
// a demoted UNIQUE→plain KEY surfaces as a same-key body mismatch; a
// dropped CHECK surfaces as only-in-oracle.
func TestDiffDumpStatements_MySQLElements(t *testing.T) {
	oracle := ParseMySQLSchemaDump(
		"CREATE TABLE `t` (\n" +
			"  `id` bigint NOT NULL,\n" +
			"  `email` varchar(255) NOT NULL,\n" +
			"  PRIMARY KEY (`id`),\n" +
			"  UNIQUE KEY `email_uidx` (`email`),\n" +
			"  CONSTRAINT `chk` CHECK ((`id` > 0))\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
	)
	// sluice side: UNIQUE demoted to a plain KEY, and the CHECK dropped.
	sluice := ParseMySQLSchemaDump(
		"CREATE TABLE `t` (\n" +
			"  `id` bigint NOT NULL,\n" +
			"  `email` varchar(255) NOT NULL,\n" +
			"  PRIMARY KEY (`id`),\n" +
			"  KEY `email_uidx` (`email`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
	)

	d := DiffDumpStatements(sluice, oracle)
	if d.Empty() {
		t.Fatal("diff reported parity; want a mismatch + an only-in-oracle")
	}
	if len(d.Mismatched) != 1 || d.Mismatched[0].Key != "t KEY email_uidx" {
		t.Errorf("Mismatched = %#v; want one body mismatch on the demoted key", d.Mismatched)
	}
	if len(d.OnlyInOracle) != 1 || d.OnlyInOracle[0].Key != "t CONSTRAINT chk" {
		t.Errorf("OnlyInOracle = %#v; want the dropped CHECK", d.OnlyInOracle)
	}
	if len(d.OnlyInSluice) != 0 {
		t.Errorf("OnlyInSluice = %#v; want none", d.OnlyInSluice)
	}
}

// TestDumpParityAllowlistMySQL_Hygiene mirrors the PG allowlist hygiene
// pin: every entry carries a reason and a citation, every pattern is
// valid under path.Match, and TRIAGE entries are recognizable by the
// marker alone.
func TestDumpParityAllowlistMySQL_Hygiene(t *testing.T) {
	if len(DumpParityAllowlistMySQL) == 0 {
		t.Fatal("DumpParityAllowlistMySQL is empty; the harness depends on at least the state-table entry")
	}
	for _, e := range DumpParityAllowlistMySQL {
		if strings.TrimSpace(e.Pattern) == "" {
			t.Errorf("entry with empty pattern: %+v", e)
		}
		if _, err := path.Match(e.Pattern, ""); err != nil {
			t.Errorf("invalid pattern %q: %v", e.Pattern, err)
		}
		if strings.TrimSpace(e.Reason) == "" {
			t.Errorf("entry %q has no reason", e.Pattern)
		}
		if strings.TrimSpace(e.Citation) == "" {
			t.Errorf("entry %q has no citation; cite the doc/ADR/source or mark it %q", e.Pattern, DumpParityTriageCitation)
		}
	}

	// The state-table pattern must match a decomposed element key.
	if e := MatchDumpParityAllowlist("sluice_migrate_state COLUMN phase", DumpParityAllowlistMySQL); e == nil {
		t.Error("state-table pattern should match a migrate-state column element key")
	}
}
