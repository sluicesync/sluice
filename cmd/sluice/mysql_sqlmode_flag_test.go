// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

// TestMySQLSQLModeFlagName pins the v0.92.2 fix for the v0.92.1
// kong-tag typo. The field name MySQLSQLMode contains three
// consecutive capital letters (SQLSQL); kong's auto-kebab-case reads
// it as `My` + `SQLSQL` + `Mode` because the SQL/SQL boundary has no
// lowercase to mark a word break, yielding the flag `--my-sqlsql-mode`.
// v0.92.1 published that broken name; v0.92.2 adds an explicit
// `name:"mysql-sql-mode"` kong tag to pin the documented public name.
//
// This test asserts the parsed --mysql-sql-mode value reaches
// Globals.MySQLSQLMode. The negative side (the broken auto-derived
// name is no longer accepted) falls out of kong's own validation.
func TestMySQLSQLModeFlagName(t *testing.T) {
	cli := &CLI{}
	parser, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	_, err = parser.Parse([]string{
		"--mysql-sql-mode=STRICT_TRANS_TABLES,ANSI_QUOTES",
		"engines",
	})
	if err != nil {
		t.Fatalf("--mysql-sql-mode flag rejected: %v", err)
	}
	if !strings.Contains(cli.MySQLSQLMode, "ANSI_QUOTES") {
		t.Errorf("MySQLSQLMode = %q; expected the explicit value to land", cli.MySQLSQLMode)
	}
}

// TestMySQLSQLModeFlagBrokenSpellingRejected pins the v0.92.1 typo as
// a fix: the old `--my-sqlsql-mode` flag-name MUST stop being accepted
// so an operator who paste-copied the broken cycle-time spelling gets
// a clear error instead of a no-op. Belt-and-suspenders with the kong
// `name:` tag above.
func TestMySQLSQLModeFlagBrokenSpellingRejected(t *testing.T) {
	cli := &CLI{}
	parser, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	_, err = parser.Parse([]string{
		"--my-sqlsql-mode=STRICT_TRANS_TABLES",
		"engines",
	})
	if err == nil {
		t.Fatalf("v0.92.1's typo flag --my-sqlsql-mode should be rejected; instead it parsed cli.MySQLSQLMode=%q", cli.MySQLSQLMode)
	}
	if !strings.Contains(err.Error(), "unknown flag") && !strings.Contains(err.Error(), "did you mean") {
		t.Errorf("err = %v; expected an 'unknown flag' rejection", err)
	}
}
