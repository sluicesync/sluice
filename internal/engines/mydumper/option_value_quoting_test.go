// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Backtick-quoted table-option values are refused (audit 2026-08-01 Sec1).
//
// The lexer lets a backtick-quoted identifier carry arbitrary bytes up to its
// closing backtick, and parseOptionValue returned that text verbatim for
// CHARSET / COLLATE — positions the target emitters render as BARE
// identifiers.
//
// Why refusing is safe, ground-truthed on MySQL 8.0 rather than assumed:
// MySQL ACCEPTS a quoted collation value, but validates the NAME (a hostile
// one is rejected with errno 1273, Unknown collation), and SHOW CREATE TABLE —
// which mydumper dumps verbatim — always emits the value UNQUOTED. So no dump
// produced by a real server can contain one, and refusing costs a genuine dump
// nothing.
//
// The arbitrary-SQL escalation is separately closed downstream (audit C1's
// sqlident.Check on the MySQL emitter; the Postgres emitter drops
// foreign-dialect collations), so this is the read-boundary half of a
// defence-in-depth pair — not the only thing standing between a tampered file
// and the target.

package mydumper

import (
	"strings"
	"testing"
)

const sec1Prelude = "CREATE TABLE `t` (\n  `a` int NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB "

func TestParseOptionValue_RefusesBacktickQuotedValues(t *testing.T) {
	cases := []struct {
		name string
		opts string
	}{
		{
			name: "hostile collation carrying statement separators",
			opts: "DEFAULT CHARSET=utf8mb4 COLLATE=`utf8mb4_general_ci; CREATE ROLE attacker SUPERUSER; --`",
		},
		{
			name: "hostile charset",
			opts: "DEFAULT CHARSET=`utf8mb4; DROP TABLE users; --` COLLATE=utf8mb4_general_ci",
		},
		{
			// Even a HARMLESS-looking quoted value is refused: the point is
			// that a real dump never contains one, so its presence is the
			// signal, not the payload.
			name: "benign-looking quoted collation is still refused",
			opts: "DEFAULT CHARSET=utf8mb4 COLLATE=`utf8mb4_general_ci`",
		},
		{
			name: "quoted CHARACTER SET form",
			opts: "DEFAULT CHARACTER SET=`utf8mb4` COLLATE=utf8mb4_general_ci",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseCreateTable(sec1Prelude+tc.opts+";", "t.sql")
			if err == nil {
				t.Fatalf("a backtick-quoted table-option value was ACCEPTED. The lexer allows arbitrary "+
					"bytes inside backticks and this value flows into a CHARSET/COLLATE position the "+
					"emitters render bare (audit Sec1). input: %q", tc.opts)
			}
			if !strings.Contains(err.Error(), "backtick-quoted") {
				t.Errorf("refused, but not for the quoting reason — the guard did not fire; got: %v", err)
			}
		})
	}
}

// The controls. Every shape a REAL mydumper dump contains must still parse, or
// the guard is worse than the hole it closes.
func TestParseOptionValue_RealDumpShapesStillParse(t *testing.T) {
	cases := []struct {
		name string
		opts string
	}{
		// Exactly what SHOW CREATE TABLE emits.
		{"bare charset and collation", "DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"},
		{"charset only", "DEFAULT CHARSET=utf8mb4"},
		{"collation only", "COLLATE=utf8mb4_general_ci"},
		{"CHARACTER SET spelling", "DEFAULT CHARACTER SET=utf8mb4"},
		{"quoted STRING option value stays legal", "DEFAULT CHARSET=utf8mb4 COMMENT='a table comment'"},
		{"no options at all", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseCreateTable(sec1Prelude+tc.opts+";", "t.sql"); err != nil {
				t.Errorf("a real-dump shape was refused: %v\ninput options: %q", err, tc.opts)
			}
		})
	}
}

// Backtick quoting on NAMES is legitimate and appears on nearly every line of
// a real dump. The guard must not have leaked into expectIdent — this is the
// anti-over-fire floor, and it is the reason the fix is in parseOptionValue
// rather than in expectIdent as the audit's one-line framing suggested.
func TestQuotedIdentifiers_StillAcceptedForNames(t *testing.T) {
	const ddl = "CREATE TABLE `my table` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `other col` varchar(32) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `idx name` (`other col`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	tbl, err := parseCreateTable(ddl, "t.sql")
	if err != nil {
		t.Fatalf("backtick-quoted NAMES must still parse — every real dump quotes them: %v", err)
	}
	if tbl.Name != "my table" {
		t.Errorf("table name = %q, want %q", tbl.Name, "my table")
	}
}
