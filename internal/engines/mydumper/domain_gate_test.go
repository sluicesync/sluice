// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"testing"

	"sluicesync.dev/sluice/internal/domaingate"
)

// TestDomainTransparency_MydumperDispatchRoster is this package's instantiation
// of the Bug 233 gate (audit A-3): every column-type dispatch either reads the
// STORAGE type through ir.UnwrapDomain or carries a written, code-verified
// reason below.
//
// mydumper is a MySQL logical-dump (mydumper/myloader format) READER: it parses
// a dump's `CREATE TABLE` DDL + data chunks into the IR. MySQL has no
// `CREATE DOMAIN`, so this parser can never populate an ir.Domain — every site
// here is SOURCE-SIDE and inert to unwrap, the same argument the mysql engine's
// own SchemaReader exemptions make.
//
// See the domaingate package doc for what a pass proves and what it does not.
func TestDomainTransparency_MydumperDispatchRoster(t *testing.T) {
	domaingate.Assert(t, domaingate.Config{
		Dir:    ".",
		Engine: "mydumper",
		// 3 dispatch sites; floor a touch under so a refactor that dissolves
		// the shape is caught, not the exact count.
		MinSites: 2,
		Allowed:  mydumperDomainDispatchExemptions,
	})
}

var mydumperDomainDispatchExemptions = map[string]string{
	"row_reader.go:warnIfTimestampsWithoutTZHeader:col.Type": "SOURCE-SIDE: the *ir.Table was parsed by " +
		"this package from a mydumper `CREATE TABLE` DDL block (schema_parse.go), and MySQL has no " +
		"CREATE DOMAIN — so ir.Domain cannot appear. The dispatch only selects which TIMESTAMP columns to " +
		"name in the missing-TZ-header WARN; unwrapping would be inert.",
	"row_reader.go:warnIfSingleFloatColumns:col.Type": "SOURCE-SIDE: same mydumper-parsed *ir.Table; a MySQL " +
		"dump carries no DOMAIN. The dispatch selects single-precision FLOAT columns for the display-rounding " +
		"WARN — unwrapping a wrapper this reader cannot produce is dead code.",
	"schema_parse.go:checkCharsets:col.Type": "SOURCE-SIDE: this IS the mydumper `CREATE TABLE` parse " +
		"(tableBuilder) that produces the IR; nothing upstream of it can have wrapped a type in ir.Domain, and " +
		"MySQL has no CREATE DOMAIN regardless. The switch picks string-family columns for the charset refusal.",
}
