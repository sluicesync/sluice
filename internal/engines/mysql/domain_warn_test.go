// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// Unit tests for the MySQL cross-engine PG → MySQL DOMAIN-CHECK
// silent-downgrade WARN (residual carry-over from v0.95.x Bug 113
// round-trip closure).
//
// The contract:
//   - PG → MySQL with any ir.Domain-typed column logs exactly one
//     WARN line per writer lifetime (one per stream), regardless of
//     how many DOMAIN columns the schema carries, listing every
//     (table.column, source_domain, target_base_type) tuple plus the
//     count of dropped DOMAIN CHECK constraints.
//   - MySQL → PG (and any other MySQL-source case) emits zero WARNs.
//     MySQL has no DOMAIN, so the MySQL SchemaReader never populates
//     ir.Domain and the writer no-ops cleanly.
//   - Same-engine PG → PG never reaches this codepath (the
//     orchestrator routes to internal/engines/postgres's SchemaWriter
//     which handles DOMAIN round-trip via Phase 1a').

import (
	"context"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestMaybeWarnDomainCheckDrop_NoOpForMySQLSource: a schema with no
// ir.Domain columns produces zero WARNs. This is the MySQL → PG /
// MySQL → MySQL green-path case — the WARN is strictly PG → MySQL.
func TestMaybeWarnDomainCheckDrop_NoOpForMySQLSource(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "users", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Text{}},
		}},
	}}
	w.maybeWarnDomainCheckDrop(context.Background(), schema)
	if buf.Len() != 0 {
		t.Errorf("expected zero WARNs on no-DOMAIN schema; got %q", buf.String())
	}
}

// TestMaybeWarnDomainCheckDrop_TextDomain: a single text-DOMAIN
// column fires one WARN naming the column, the source DOMAIN, and
// the target base type. This is the canonical BUG-CATALOG Bug 113
// shape: `email email_address NOT NULL` where `email_address` is a
// PG DOMAIN over text with a regex CHECK.
func TestMaybeWarnDomainCheckDrop_TextDomain(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "gl_users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "email", Type: ir.Domain{
					Name:     "email_address",
					BaseType: ir.Text{},
					Checks:   []ir.DomainCheck{{Name: "email_address_check", Body: `VALUE ~ '^[^@]+@[^@]+\.[^@]+$'`}},
				}},
			},
		},
	}}
	w.maybeWarnDomainCheckDrop(context.Background(), schema)
	out := buf.String()
	if !strings.Contains(out, "DOMAIN") || !strings.Contains(out, "downgrade") {
		t.Errorf("expected WARN naming DOMAIN downgrade; got %q", out)
	}
	if !strings.Contains(out, "gl_users.email") {
		t.Errorf("WARN should name affected column 'gl_users.email'; got %q", out)
	}
	if !strings.Contains(out, "email_address") {
		t.Errorf("WARN should name source DOMAIN 'email_address'; got %q", out)
	}
	if !strings.Contains(out, "TEXT") {
		t.Errorf("WARN should name a MySQL TEXT-family target base type (PG text → MySQL *TEXT); got %q", out)
	}
	if !strings.Contains(out, "check_constraint_dropped") {
		t.Errorf("WARN should carry the check_constraint_dropped field; got %q", out)
	}
}

// TestMaybeWarnDomainCheckDrop_NumericDomain: DOMAIN over numeric
// also fires the WARN. Generic across BaseType.
func TestMaybeWarnDomainCheckDrop_NumericDomain(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "pct_test",
			Columns: []*ir.Column{
				{Name: "pct", Type: ir.Domain{
					Name:     "percentage",
					BaseType: ir.Decimal{Unconstrained: true},
					Checks:   []ir.DomainCheck{{Name: "percentage_check", Body: "VALUE >= 0 AND VALUE <= 100"}},
				}},
			},
		},
	}}
	w.maybeWarnDomainCheckDrop(context.Background(), schema)
	out := buf.String()
	if !strings.Contains(out, "percentage") {
		t.Errorf("WARN should name source DOMAIN 'percentage'; got %q", out)
	}
	if !strings.Contains(out, "DECIMAL") {
		t.Errorf("WARN should name target base type 'DECIMAL' (PG numeric → MySQL DECIMAL); got %q", out)
	}
}

// TestMaybeWarnDomainCheckDrop_OnceAcrossManyColumns: multiple
// DOMAIN columns produce one WARN, not many. Same sync.Once
// rationale as the RLS WARN — per-column flooding would mask the
// signal.
func TestMaybeWarnDomainCheckDrop_OnceAcrossManyColumns(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "t1", Columns: []*ir.Column{
			{Name: "email", Type: ir.Domain{Name: "email_address", BaseType: ir.Text{}, Checks: []ir.DomainCheck{{Name: "c1", Body: "VALUE > 0"}}}},
		}},
		{Name: "t2", Columns: []*ir.Column{
			{Name: "pct", Type: ir.Domain{Name: "percentage", BaseType: ir.Decimal{Unconstrained: true}, Checks: []ir.DomainCheck{{Name: "c2", Body: "VALUE > 0"}}}},
		}},
		{Name: "t3", Columns: []*ir.Column{
			{Name: "code", Type: ir.Domain{Name: "country_code", BaseType: ir.Text{}, Checks: []ir.DomainCheck{{Name: "c3", Body: "VALUE > 0"}}}},
		}},
	}}
	w.maybeWarnDomainCheckDrop(context.Background(), schema)
	got := strings.Count(buf.String(), "\n")
	if got != 1 {
		t.Errorf("expected exactly 1 WARN line for 3 DOMAIN columns; got %d:\n%s", got, buf.String())
	}
	for _, name := range []string{"t1.email", "t2.pct", "t3.code"} {
		if !strings.Contains(buf.String(), name) {
			t.Errorf("WARN should name affected column %q; got %q", name, buf.String())
		}
	}
	for _, dom := range []string{"email_address", "percentage", "country_code"} {
		if !strings.Contains(buf.String(), dom) {
			t.Errorf("WARN should name source DOMAIN %q; got %q", dom, buf.String())
		}
	}
}

// TestMaybeWarnDomainCheckDrop_OnceAcrossMultipleCalls: re-calls on
// the same writer don't re-WARN. sync.Once semantics.
func TestMaybeWarnDomainCheckDrop_OnceAcrossMultipleCalls(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "t", Columns: []*ir.Column{
			{Name: "email", Type: ir.Domain{Name: "email_address", BaseType: ir.Text{}, Checks: []ir.DomainCheck{{Name: "c", Body: "VALUE > 0"}}}},
		}},
	}}
	for i := 0; i < 5; i++ {
		w.maybeWarnDomainCheckDrop(context.Background(), schema)
	}
	got := strings.Count(buf.String(), "\n")
	if got != 1 {
		t.Errorf("expected 1 WARN across 5 calls; got %d:\n%s", got, buf.String())
	}
}

// TestMaybeWarnDomainCheckDrop_DomainWithoutChecksStillWarns: a
// DOMAIN with zero attached CHECKs still triggers the WARN — the
// CHECK list is one part of the DOMAIN's identity; even a DOMAIN
// declared without an explicit CHECK can have NOT NULL or other
// type-level semantics that don't carry. The check_constraint_dropped
// field reflects 0 in this case.
func TestMaybeWarnDomainCheckDrop_DomainWithoutChecksStillWarns(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "t", Columns: []*ir.Column{
			{Name: "code", Type: ir.Domain{Name: "country_code", BaseType: ir.Text{}, Checks: nil}},
		}},
	}}
	w.maybeWarnDomainCheckDrop(context.Background(), schema)
	if !strings.Contains(buf.String(), "country_code") {
		t.Errorf("expected WARN on CHECK-less DOMAIN; got %q", buf.String())
	}
}

// TestMaybeWarnDomainCheckDrop_SeparateWritersWarnIndependently:
// each writer (each stream) gets its own sync.Once; the WARN fires
// per-stream, not globally. Without this the second stream of a
// multi-stream consolidation run would silently swallow the
// DOMAIN-drop signal.
func TestMaybeWarnDomainCheckDrop_SeparateWritersWarnIndependently(t *testing.T) {
	buf := captureSlogWarn(t)
	w1 := &SchemaWriter{}
	w2 := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "t", Columns: []*ir.Column{
			{Name: "email", Type: ir.Domain{Name: "email_address", BaseType: ir.Text{}, Checks: []ir.DomainCheck{{Name: "c", Body: "VALUE > 0"}}}},
		}},
	}}
	w1.maybeWarnDomainCheckDrop(context.Background(), schema)
	w2.maybeWarnDomainCheckDrop(context.Background(), schema)
	got := strings.Count(buf.String(), "\n")
	if got != 2 {
		t.Errorf("expected 2 WARNs (one per writer); got %d:\n%s", got, buf.String())
	}
}

// TestMaybeWarnDomainCheckDrop_NilSchemaNoOp: defensive — a nil
// schema shouldn't panic or WARN.
func TestMaybeWarnDomainCheckDrop_NilSchemaNoOp(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	w.maybeWarnDomainCheckDrop(context.Background(), nil)
	if buf.Len() != 0 {
		t.Errorf("expected zero WARNs on nil schema; got %q", buf.String())
	}
}
