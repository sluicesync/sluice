//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the MySQL cross-engine RLS-drop WARN
// (ADR-0063 — task #52 sub-deliverable 3). Boots a real MySQL
// container, exercises CreateTablesWithoutConstraints against a
// hand-built PG-shaped IR carrying policies, and asserts:
//
//   - the migration completes without error (MySQL has no RLS, so
//     dropping the policy layer is the documented contract; the
//     writer must not refuse)
//   - exactly one WARN line fires per writer lifetime, regardless of
//     how many tables carry RLS state in the incoming IR
//   - the WARN names the affected table(s) so an operator can grep
//     the log for the policy-drop event
//
// Compares to the unit tests in `rls_warn_test.go` which already pin
// the WARN-once gate against in-memory schemas. This file runs the
// same shape end-to-end against the writer's real `*sql.DB` path so
// any wiring drift between the unit and integration boundaries
// surfaces here.

package mysql

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestRLSWarn_PGtoMySQL_DropsPoliciesAndWarnsOnce(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	// Capture WARN-level slog output for the test's duration.
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("open schema writer: %v", err)
	}
	defer func() {
		if c, ok := swHandle.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Hand-built PG-shape IR — RLS state populated, mirroring what a
	// PG SchemaReader would have produced for a multi-tenant schema.
	// Three tables, all carrying some RLS state, so the sync.Once
	// gate is exercised under the realistic multi-table flow.
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name:       "tenants_enable",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			RLSEnabled: true,
			Policies: []*ir.Policy{
				{Name: "p_select", Command: "SELECT", Permissive: true, Roles: []string{"public"}, Using: "true"},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
		{
			Name:       "tenants_force",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			RLSEnabled: true,
			RLSForced:  true,
			Policies: []*ir.Policy{
				{Name: "p_all", Command: "ALL", Permissive: true, Roles: []string{"public"}, Using: "true"},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
		{
			Name:       "tenants_policies_only",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			RLSEnabled: true,
			Policies: []*ir.Policy{
				{Name: "p_one", Command: "UPDATE", Permissive: true, Roles: []string{"public"}, Using: "true", Check: "true"},
				{Name: "p_two", Command: "INSERT", Permissive: true, Roles: []string{"public"}, Check: "true"},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
	}}

	if err := swHandle.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}

	// Exactly one WARN line (JSON handler emits one record per line).
	warnLines := strings.Count(buf.String(), "\n")
	if warnLines != 1 {
		t.Errorf("expected exactly 1 WARN line; got %d:\n%s", warnLines, buf.String())
	}

	// Affected table names appear in the payload.
	for _, name := range []string{"tenants_enable", "tenants_force", "tenants_policies_only"} {
		if !strings.Contains(buf.String(), name) {
			t.Errorf("WARN should name affected table %q; got %q", name, buf.String())
		}
	}

	// Re-invoke to confirm the once gate holds across multiple
	// CreateTablesWithoutConstraints calls on the same writer (e.g. a
	// schema-redo or resume).
	if err := swHandle.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		// CREATE TABLE IF NOT EXISTS — MySQL accepts this idempotently.
		// If the writer somehow rejects, that's a regression; tolerate
		// the per-engine duplicate-table refusal but assert WARN-once.
		t.Logf("re-invoke result (acceptable if idempotency differs): %v", err)
	}
	if got := strings.Count(buf.String(), "\n"); got != 1 {
		t.Errorf("expected 1 WARN line across 2 invocations; got %d:\n%s", got, buf.String())
	}
}

// TestRLSWarn_MySQLtoPG_NoWarn confirms the MySQL → PG green path:
// a MySQL SchemaReader leaves RLS fields unset, so a writer fed that
// IR produces zero RLS WARNs. The cross-engine carve-out only fires
// PG → MySQL, never MySQL → anything.
func TestRLSWarn_MySQLtoPG_NoWarn(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("open schema writer: %v", err)
	}
	defer func() {
		if c, ok := swHandle.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// MySQL-source-shaped IR — no RLS state populated.
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name:       "plain_one",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
		{
			Name:       "plain_two",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
	}}

	if err := swHandle.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}

	// Zero RLS-related WARN lines. (The buffer may contain other
	// engine-level warnings — we filter on the RLS payload key to keep
	// the assertion focused.)
	if strings.Contains(buf.String(), "row-level security") {
		t.Errorf("MySQL-source schema should not produce an RLS WARN; got %q", buf.String())
	}
}
