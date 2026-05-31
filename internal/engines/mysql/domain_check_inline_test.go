// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// Unit tests for the v0.97.0 PG → MySQL inline DOMAIN-CHECK
// translator integration with emitTableDef and the v0.96.2 WARN.

import (
	"context"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestEmitTableDef_DomainCheck_TextRegex_InlineCheckSupported pins the
// canonical regex DOMAIN path: a column typed as an `email_address`
// DOMAIN (over text, with the standard email regex CHECK) produces a
// CREATE TABLE that carries a `CHECK (REGEXP_LIKE(...))` clause when
// the writer's inlineCheckSupported flag is true.
func TestEmitTableDef_DomainCheck_TextRegex_InlineCheckSupported(t *testing.T) {
	table := &ir.Table{
		Name: "gl_users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "email", Type: ir.Domain{
				Name:     "email_address",
				BaseType: ir.Text{},
				Checks: []ir.DomainCheck{{
					Name: "email_address_check",
					Body: `(VALUE ~ '^[^@]+@[^@]+\.[^@]+$'::text)`,
				}},
			}},
		},
	}
	stmt, err := emitTableDefWithDomainChecks(table, true)
	if err != nil {
		t.Fatalf("emitTableDefWithDomainChecks: %v", err)
	}
	if !strings.Contains(stmt, "REGEXP_LIKE(`email`,") {
		t.Errorf("CREATE TABLE should contain REGEXP_LIKE on email; got:\n%s", stmt)
	}
	// v0.97.1: backslashes in the pattern are doubled in the SQL
	// literal so MySQL's string parser produces `\.` for the regex,
	// not `.` (which would match any char).
	if !strings.Contains(stmt, `'^[^@]+@[^@]+\\.[^@]+$'`) {
		t.Errorf("CREATE TABLE should preserve the regex pattern with backslash-doubled escape; got:\n%s", stmt)
	}
}

// TestEmitTableDef_DomainCheck_NumericRange_InlineCheckSupported pins
// the canonical range DOMAIN path: `pct percentage` with the standard
// `VALUE >= 0 AND VALUE <= 100` CHECK emits inline.
func TestEmitTableDef_DomainCheck_NumericRange_InlineCheckSupported(t *testing.T) {
	table := &ir.Table{
		Name: "pct_test",
		Columns: []*ir.Column{
			{Name: "pct", Type: ir.Domain{
				Name:     "percentage",
				BaseType: ir.Decimal{Unconstrained: true},
				Checks: []ir.DomainCheck{{
					Name: "percentage_check",
					Body: `((VALUE >= (0)::numeric) AND (VALUE <= (100)::numeric))`,
				}},
			}},
		},
	}
	stmt, err := emitTableDefWithDomainChecks(table, true)
	if err != nil {
		t.Fatalf("emitTableDefWithDomainChecks: %v", err)
	}
	if !strings.Contains(stmt, "`pct` >= 0") || !strings.Contains(stmt, "`pct` <= 100") {
		t.Errorf("CREATE TABLE should contain range CHECK on pct; got:\n%s", stmt)
	}
}

// TestEmitTableDef_DomainCheck_InlineCheckUnsupported_NoCHECKEmitted
// confirms the version gate: older MySQL targets (the false branch
// from mysqlVersionSupportsInlineCheck) get the v0.96.x behavior —
// no inline CHECK, no behavior change from prior releases.
func TestEmitTableDef_DomainCheck_InlineCheckUnsupported_NoCHECKEmitted(t *testing.T) {
	table := &ir.Table{
		Name: "gl_users",
		Columns: []*ir.Column{
			{Name: "email", Type: ir.Domain{
				Name:     "email_address",
				BaseType: ir.Text{},
				Checks: []ir.DomainCheck{{
					Name: "email_address_check",
					Body: `(VALUE ~ '^[^@]+@[^@]+\.[^@]+$'::text)`,
				}},
			}},
		},
	}
	stmt, err := emitTableDefWithDomainChecks(table, false)
	if err != nil {
		t.Fatalf("emitTableDefWithDomainChecks: %v", err)
	}
	if strings.Contains(stmt, "REGEXP_LIKE") || strings.Contains(stmt, "CHECK (") {
		t.Errorf("inlineCheckSupported=false MUST NOT emit a CHECK; got:\n%s", stmt)
	}
}

// TestEmitTableDef_DomainCheck_UntranslatableDOMAINCheckDropped
// confirms that DOMAIN CHECKs the translator doesn't recognise (any
// shape outside the regex / range whitelist) are silently dropped at
// emit time — covered by the WARN at maybeWarnDomainCheckDrop instead.
// Emitting an inaccurate CHECK would silently re-introduce Bug 113.
func TestEmitTableDef_DomainCheck_UntranslatableDOMAINCheckDropped(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "x", Type: ir.Domain{
				Name:     "weird_check",
				BaseType: ir.Text{},
				Checks:   []ir.DomainCheck{{Body: `(LENGTH(VALUE) > 5)`}},
			}},
		},
	}
	stmt, err := emitTableDefWithDomainChecks(table, true)
	if err != nil {
		t.Fatalf("emitTableDefWithDomainChecks: %v", err)
	}
	if strings.Contains(stmt, "CHECK") {
		t.Errorf("untranslatable DOMAIN CHECK should be dropped (not best-effort emitted); got:\n%s", stmt)
	}
}

// TestEmitTableDef_DomainCheck_PreservesUserCHECKs confirms the
// trailing-comma logic still produces valid SQL when both inline
// DOMAIN CHECKs AND user-declared table.CheckConstraints land on the
// same CREATE TABLE.
func TestEmitTableDef_DomainCheck_PreservesUserCHECKs(t *testing.T) {
	table := &ir.Table{
		Name: "mixed",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "email", Type: ir.Domain{
				Name:     "email_address",
				BaseType: ir.Text{},
				Checks: []ir.DomainCheck{{
					Body: `(VALUE ~ '^[^@]+@[^@]+\.[^@]+$'::text)`,
				}},
			}},
		},
		CheckConstraints: []*ir.CheckConstraint{
			{Name: "ck_id_positive", Expr: "id > 0"},
		},
	}
	stmt, err := emitTableDefWithDomainChecks(table, true)
	if err != nil {
		t.Fatalf("emitTableDefWithDomainChecks: %v", err)
	}
	if !strings.Contains(stmt, "id > 0") {
		t.Errorf("user CHECK constraint should still emit; got:\n%s", stmt)
	}
	if !strings.Contains(stmt, "REGEXP_LIKE(`email`,") {
		t.Errorf("DOMAIN CHECK should still emit; got:\n%s", stmt)
	}
	// Basic syntactic check: no double-comma, no trailing comma before
	// the closing paren — emitTableDef's caller is the DB driver
	// which would reject those shapes.
	if strings.Contains(stmt, ",,") {
		t.Errorf("double-comma in emitted CREATE TABLE: %s", stmt)
	}
	if strings.Contains(stmt, ",\n)") || strings.Contains(stmt, ", )") {
		t.Errorf("trailing comma before closing paren: %s", stmt)
	}
}

// TestMaybeWarnDomainCheckDrop_v0970_SuppressesWhenAllTranslated
// confirms the symmetry-with-v0.96.2-WARN promise: a DOMAIN whose
// every CHECK translates AND the writer supports inline CHECK MUST
// NOT WARN (the inline emit covers the constraint enforcement —
// nothing was silently dropped).
func TestMaybeWarnDomainCheckDrop_v0970_SuppressesWhenAllTranslated(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{inlineCheckSupported: true}
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "gl_users",
			Columns: []*ir.Column{
				{Name: "email", Type: ir.Domain{
					Name:     "email_address",
					BaseType: ir.Text{},
					Checks: []ir.DomainCheck{{
						Body: `(VALUE ~ '^[^@]+@[^@]+\.[^@]+$'::text)`,
					}},
				}},
			},
		},
	}}
	w.maybeWarnDomainCheckDrop(context.Background(), schema)
	if buf.Len() != 0 {
		t.Errorf("expected NO WARN when all CHECKs translate; got %q", buf.String())
	}
}

// TestMaybeWarnDomainCheckDrop_v0970_FiresWhenPartialTranslate covers
// the mixed case: one DOMAIN with two CHECKs, one translates and one
// doesn't. The WARN MUST fire and the dropped-count MUST equal 1
// (NOT 2 — the translated one isn't dropped).
func TestMaybeWarnDomainCheckDrop_v0970_FiresWhenPartialTranslate(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{inlineCheckSupported: true}
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "t",
			Columns: []*ir.Column{
				{Name: "x", Type: ir.Domain{
					Name:     "two_checks",
					BaseType: ir.Text{},
					Checks: []ir.DomainCheck{
						{Body: `(VALUE ~ 'ok'::text)`}, // translates
						{Body: `(LENGTH(VALUE) > 5)`},  // doesn't
					},
				}},
			},
		},
	}}
	w.maybeWarnDomainCheckDrop(context.Background(), schema)
	out := buf.String()
	if !strings.Contains(out, "two_checks") {
		t.Errorf("WARN should fire with mixed CHECK translation; got %q", out)
	}
	if !strings.Contains(out, `"check_constraint_dropped":1`) {
		t.Errorf("WARN should report exactly 1 dropped CHECK on partial translation; got %q", out)
	}
}

// TestMaybeWarnDomainCheckDrop_v0970_FiresWhenInlineCheckUnsupported
// confirms the older-MySQL path: when the writer's inlineCheckSupported
// flag is false, every DOMAIN CHECK is counted as dropped and the WARN
// fires unchanged — v0.96.2 behavior preserved.
func TestMaybeWarnDomainCheckDrop_v0970_FiresWhenInlineCheckUnsupported(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{inlineCheckSupported: false}
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "gl_users",
			Columns: []*ir.Column{
				{Name: "email", Type: ir.Domain{
					Name:     "email_address",
					BaseType: ir.Text{},
					Checks: []ir.DomainCheck{{
						Body: `(VALUE ~ '^[^@]+@[^@]+\.[^@]+$'::text)`,
					}},
				}},
			},
		},
	}}
	w.maybeWarnDomainCheckDrop(context.Background(), schema)
	if !strings.Contains(buf.String(), "email_address") {
		t.Errorf("WARN should fire on older-MySQL target; got %q", buf.String())
	}
}

// TestMaybeWarnDomainCheckDrop_v0970_FiresWhenZeroChecks confirms
// that a DOMAIN with no CHECKs at all (`CREATE DOMAIN country_code AS
// text`) still triggers the WARN — the DOMAIN-name type alias doesn't
// carry to MySQL. v0.96.2 behavior preserved.
func TestMaybeWarnDomainCheckDrop_v0970_FiresWhenZeroChecks(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{inlineCheckSupported: true}
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "t",
			Columns: []*ir.Column{
				{Name: "code", Type: ir.Domain{
					Name:     "country_code",
					BaseType: ir.Text{},
					Checks:   nil,
				}},
			},
		},
	}}
	w.maybeWarnDomainCheckDrop(context.Background(), schema)
	if !strings.Contains(buf.String(), "country_code") {
		t.Errorf("WARN should fire on CHECK-less DOMAIN; got %q", buf.String())
	}
}

// TestMySQLVersionSupportsInlineCheck is the version-parser table.
// MySQL 8.0.16+ is the threshold (the version that started enforcing
// CHECK); MariaDB is always excluded regardless of version (separate
// dialect; conservative default).
func TestMySQLVersionSupportsInlineCheck(t *testing.T) {
	cases := []struct {
		version string
		want    bool
	}{
		{"8.0.46", true},
		{"8.0.46-debug", true},
		{"8.0.16", true},
		{"8.0.15", false},
		{"8.0.0", false},
		{"8.1.0", true},
		{"8.4.2", true},
		{"9.0.0", true},
		{"10.0.0", true},
		{"5.7.42", false},
		{"5.7.42-log", false},
		{"5.6.51", false},
		{"10.11.4-MariaDB", false},          // MariaDB always excluded
		{"8.0.46-MariaDB-something", false}, // suffix wins
		{"", false},
		{"unparseable", false},
	}
	for _, c := range cases {
		got := mysqlVersionSupportsInlineCheck(c.version)
		if got != c.want {
			t.Errorf("mysqlVersionSupportsInlineCheck(%q) = %v; want %v", c.version, got, c.want)
		}
	}
}
