// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// Unit tests for the MySQL cross-engine RLS-drop WARN
// (ADR-0063 — task #52 sub-deliverable 3).
//
// The contract:
//   - PG → MySQL with any RLS-enabled / FORCE-RLS / policy-bearing
//     table logs exactly one WARN line per writer lifetime (one per
//     stream), regardless of how many tables carry RLS state.
//   - MySQL → PG (and any other MySQL-source case) emits zero WARNs
//     — MySQL sources never populate the RLS fields, so the writer
//     no-ops cleanly. The integration suite for MySQL → PG round-trips
//     covers the symmetric green case.
//
// Bug-74 discipline applies even here: the trigger is "any RLS state
// at all" — RLSEnabled OR RLSForced OR len(Policies) > 0. Testing
// only one of those three would leave the other two as silent-drop
// classes; the matrix below exercises each independently.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// captureSlogWarn installs a JSON slog handler that writes into a
// buffer for the duration of the test, then restores the previous
// default. Used by every test in this file to assert WARN-or-no-WARN
// behaviour without coupling to stderr.
func captureSlogWarn(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})))
	t.Cleanup(func() {
		slog.SetDefault(prev)
	})
	return buf
}

// TestMaybeWarnRLSDrop_NoOpForMySQLSource: a MySQL-source schema (no
// RLS fields populated) produces zero WARNs. This is the symmetric
// MySQL → PG green-path case — the WARN is strictly PG → MySQL.
func TestMaybeWarnRLSDrop_NoOpForMySQLSource(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		{Name: "posts", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	w.maybeWarnRLSDrop(context.Background(), schema)
	if buf.Len() != 0 {
		t.Errorf("expected zero WARNs on MySQL-source schema; got %q", buf.String())
	}
}

// TestMaybeWarnRLSDrop_RLSEnabledOnly: the WARN fires for a table
// with RLSEnabled=true even without Policies. PG operators can
// ENABLE RLS for "deny all non-owners" semantics; that's still a
// security shape that doesn't carry to MySQL.
func TestMaybeWarnRLSDrop_RLSEnabledOnly(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name:       "secrets",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			RLSEnabled: true,
		},
	}}
	w.maybeWarnRLSDrop(context.Background(), schema)
	if !strings.Contains(buf.String(), "row-level security") {
		t.Errorf("expected WARN on RLSEnabled-only table; got %q", buf.String())
	}
	if !strings.Contains(buf.String(), "secrets") {
		t.Errorf("WARN should name affected table; got %q", buf.String())
	}
}

// TestMaybeWarnRLSDrop_RLSForcedOnly: hand-built IR with RLSForced=true
// alone (an edge — PG never produces this without RLSEnabled, but the
// IR contract permits it). WARN still fires; the "any RLS state at
// all" predicate is the trigger.
func TestMaybeWarnRLSDrop_RLSForcedOnly(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name:      "forced",
			Columns:   []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			RLSForced: true,
		},
	}}
	w.maybeWarnRLSDrop(context.Background(), schema)
	if !strings.Contains(buf.String(), "row-level security") {
		t.Errorf("expected WARN on RLSForced-only table; got %q", buf.String())
	}
}

// TestMaybeWarnRLSDrop_PoliciesOnly: hand-built IR where Policies is
// populated but the RLS flags are false. Reader never produces this
// shape; the defensive predicate "any RLS state at all" still fires
// the WARN so a downstream re-population of the flags doesn't
// suddenly start the WARNs silently.
func TestMaybeWarnRLSDrop_PoliciesOnly(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name:    "policy_only",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			Policies: []*ir.Policy{
				{Name: "p", Command: "ALL", Permissive: true, Roles: []string{"public"}, Using: "true"},
			},
		},
	}}
	w.maybeWarnRLSDrop(context.Background(), schema)
	if !strings.Contains(buf.String(), "row-level security") {
		t.Errorf("expected WARN on policies-only table; got %q", buf.String())
	}
}

// TestMaybeWarnRLSDrop_OnceAcrossManyTables is the sync.Once
// guarantee — three RLS-bearing tables produce one WARN, not three.
// Per-table WARN flooding for a multi-tenant schema would mask the
// signal; one WARN is the operator-actionable density.
func TestMaybeWarnRLSDrop_OnceAcrossManyTables(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "t1", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}, RLSEnabled: true},
		{Name: "t2", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}, RLSEnabled: true},
		{Name: "t3", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}, RLSEnabled: true, RLSForced: true},
	}}
	w.maybeWarnRLSDrop(context.Background(), schema)
	// Count the number of JSON log lines (slog JSON handler one-line-per-record).
	got := strings.Count(buf.String(), "\n")
	if got != 1 {
		t.Errorf("expected exactly 1 WARN line for 3 RLS tables; got %d:\n%s", got, buf.String())
	}
	// All three table names should appear in the affected_tables payload.
	for _, name := range []string{"t1", "t2", "t3"} {
		if !strings.Contains(buf.String(), name) {
			t.Errorf("WARN should name affected table %q; got %q", name, buf.String())
		}
	}
}

// TestMaybeWarnRLSDrop_OnceAcrossMultipleCalls confirms the sync.Once
// gate survives multiple invocations on the same writer (which is
// what happens if the orchestrator re-calls CreateTablesWithoutConstraints
// for any reason — re-runs, schema-redo). Each call sees policies;
// only the first call WARNs.
func TestMaybeWarnRLSDrop_OnceAcrossMultipleCalls(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}, RLSEnabled: true},
	}}
	for i := 0; i < 5; i++ {
		w.maybeWarnRLSDrop(context.Background(), schema)
	}
	got := strings.Count(buf.String(), "\n")
	if got != 1 {
		t.Errorf("expected 1 WARN across 5 calls; got %d:\n%s", got, buf.String())
	}
}

// TestMaybeWarnRLSDrop_SeparateWritersWarnIndependently: each writer
// (each stream) gets its own sync.Once; the WARN fires per-stream,
// not globally. Without this the second stream of a multi-stream
// consolidation run would silently swallow the RLS-drop signal.
func TestMaybeWarnRLSDrop_SeparateWritersWarnIndependently(t *testing.T) {
	buf := captureSlogWarn(t)
	w1 := &SchemaWriter{}
	w2 := &SchemaWriter{}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}, RLSEnabled: true},
	}}
	w1.maybeWarnRLSDrop(context.Background(), schema)
	w2.maybeWarnRLSDrop(context.Background(), schema)
	got := strings.Count(buf.String(), "\n")
	if got != 2 {
		t.Errorf("expected 2 WARNs (one per writer); got %d:\n%s", got, buf.String())
	}
}

// TestMaybeWarnRLSDrop_NilSchemaNoOp: defensive — a nil schema
// shouldn't panic or WARN.
func TestMaybeWarnRLSDrop_NilSchemaNoOp(t *testing.T) {
	buf := captureSlogWarn(t)
	w := &SchemaWriter{}
	w.maybeWarnRLSDrop(context.Background(), nil)
	if buf.Len() != 0 {
		t.Errorf("expected zero WARNs on nil schema; got %q", buf.String())
	}
}
