// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestEmitterBackslashPolicy_WholeTree pins the task-2.5 per-instance sql_mode
// backslash policy across EVERY emit-tree site that renders a SQL string literal
// — ENUM values, DEFAULT literals, column comments, table comments, and the
// DOMAIN-CHECK regex pattern — under BOTH the strict default (backslash IS an
// escape → doubled) and NO_BACKSLASH_ESCAPES (backslash is ordinary → NOT
// doubled). This is the "pin the class, not the representative" discipline: a
// single site pinned green would not prove the emitter threads its policy to the
// others, which is exactly the silent-corruption class the global removal risks.
func TestEmitterBackslashPolicy_WholeTree(t *testing.T) {
	strict := stdEmitter // the strict/factory default doubles backslashes
	noEscMode := "NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES"
	relaxed := newMySQLEmitter(&noEscMode) // NO_BACKSLASH_ESCAPES leaves them raw

	// A table exercising every quoteSQLString-reaching emit site with a
	// backslash-bearing value.
	table := &ir.Table{
		Name:    "t",
		Comment: `tbl C:\x`,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}, Nullable: false},
			{Name: "e", Type: ir.Enum{Values: []string{`a\b`}}, Nullable: false},
			{
				// Varchar (not TEXT) so MySQL permits the DEFAULT clause —
				// mysqlForbidsDefault suppresses DEFAULT on BLOB/TEXT/GEOMETRY/JSON.
				Name:    "s",
				Type:    ir.Varchar{Length: 32},
				Default: ir.DefaultLiteral{Value: `d\e`},
				Comment: `col C:\y`,
			},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	strictDDL, err := strict.emitTableDefWithDomainChecks(table, false)
	if err != nil {
		t.Fatalf("strict emitTableDef: %v", err)
	}
	relaxedDDL, err := relaxed.emitTableDefWithDomainChecks(table, false)
	if err != nil {
		t.Fatalf("relaxed emitTableDef: %v", err)
	}

	// Under the strict default every backslash is doubled at each site; under
	// NO_BACKSLASH_ESCAPES none are. Assert the SAME source value renders
	// differently per policy at EVERY site (not one representative).
	for _, want := range []string{`'a\\b'`, `'d\\e'`, `'col C:\\y'`, `'tbl C:\\x'`} {
		if !strings.Contains(strictDDL, want) {
			t.Errorf("strict DDL missing doubled literal %q\nDDL: %s", want, strictDDL)
		}
	}
	for _, want := range []string{`'a\b'`, `'d\e'`, `'col C:\y'`, `'tbl C:\x'`} {
		if !strings.Contains(relaxedDDL, want) {
			t.Errorf("relaxed (NO_BACKSLASH_ESCAPES) DDL missing un-doubled literal %q\nDDL: %s", want, relaxedDDL)
		}
	}

	// The DOMAIN-CHECK regex site: a PG DOMAIN over TEXT with a `~ 'pattern'`
	// check reaches quoteSQLString via translateRegexCheckBody.
	domTable := &ir.Table{
		Name: "d",
		Columns: []*ir.Column{
			{
				Name: "c",
				Type: ir.Domain{
					Name:     "codedom",
					BaseType: ir.Text{Size: ir.TextTiny},
					Checks:   []ir.DomainCheck{{Body: `VALUE ~ '^a\.b$'`}},
				},
				Nullable: false,
			},
		},
	}
	strictDom, err := strict.emitTableDefWithDomainChecks(domTable, true)
	if err != nil {
		t.Fatalf("strict domain emit: %v", err)
	}
	relaxedDom, err := relaxed.emitTableDefWithDomainChecks(domTable, true)
	if err != nil {
		t.Fatalf("relaxed domain emit: %v", err)
	}
	if !strings.Contains(strictDom, `'^a\\.b$'`) {
		t.Errorf("strict DOMAIN-CHECK regex missing doubled backslash\nDDL: %s", strictDom)
	}
	if !strings.Contains(relaxedDom, `'^a\.b$'`) {
		t.Errorf("relaxed DOMAIN-CHECK regex must NOT double the backslash\nDDL: %s", relaxedDom)
	}
}

// TestNewMySQLEmitter_ResolvesFromSQLMode pins the resolution: nil / a mode
// WITHOUT NO_BACKSLASH_ESCAPES → escape (double); a mode WITH it → no escape.
func TestNewMySQLEmitter_ResolvesFromSQLMode(t *testing.T) {
	if !newMySQLEmitter(nil).backslashEscapes {
		t.Error("nil sql_mode (strict default) must escape backslashes")
	}
	strict := "STRICT_TRANS_TABLES"
	if !newMySQLEmitter(&strict).backslashEscapes {
		t.Error("a strict mode without NO_BACKSLASH_ESCAPES must escape backslashes")
	}
	empty := ""
	if !newMySQLEmitter(&empty).backslashEscapes {
		t.Error("'' (server default) must assume backslash escaping (MySQL factory default)")
	}
	noEsc := "NO_BACKSLASH_ESCAPES"
	if newMySQLEmitter(&noEsc).backslashEscapes {
		t.Error("NO_BACKSLASH_ESCAPES must suppress backslash doubling")
	}
}
