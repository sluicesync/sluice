//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the PG RLS IR-capture + emit round-trip
// (ADR-0063 — task #52 sub-deliverables 2 + 3). Boots a real PG 16
// container, applies a multi-policy schema on the source side, runs
// sluice's PG SchemaReader → SchemaWriter round-trip into a target
// schema in the same database, then re-queries pg_policies on the
// target to assert the IR captured + re-emitted every policy.
//
// Bug-74 discipline: this suite exercises the policy matrix (Command
// × Permissive × USING/CHECK shape × ENABLE/FORCE), NOT a single
// "happy path" representative policy. The reader and writer both
// dispatch on Command via a string literal; one-shape coverage would
// silently let SELECT / INSERT / UPDATE / DELETE / ALL drift apart
// at the catalog. See ADR-0063's test-strategy section.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// rlsMatrixFixtureDDL is the source-side multi-policy schema that
// exercises every cell of the matrix. The policies are intentionally
// named with the (command, shape) tuple so failures point straight at
// the offending cell.
const rlsMatrixFixtureDDL = `
	-- Plain (non-RLS) control table; should round-trip with RLSEnabled=false.
	CREATE TABLE rlsmtx_plain (
		id BIGINT PRIMARY KEY,
		payload TEXT
	);

	-- ENABLE only (no FORCE), policies covering every Command.
	CREATE TABLE rlsmtx_enable (
		id BIGINT PRIMARY KEY,
		tenant TEXT NOT NULL,
		owner TEXT NOT NULL,
		payload TEXT
	);
	ALTER TABLE rlsmtx_enable ENABLE ROW LEVEL SECURITY;

	-- ALL + permissive + USING only
	CREATE POLICY p_all_using
		ON rlsmtx_enable
		FOR ALL
		USING (tenant = current_setting('app.tenant', true));

	-- SELECT + permissive + USING only
	CREATE POLICY p_select_using
		ON rlsmtx_enable
		FOR SELECT
		USING (owner = current_user);

	-- INSERT + permissive + WITH CHECK only
	CREATE POLICY p_insert_check
		ON rlsmtx_enable
		FOR INSERT
		WITH CHECK (owner = current_user);

	-- UPDATE + permissive + USING + WITH CHECK
	CREATE POLICY p_update_both
		ON rlsmtx_enable
		FOR UPDATE
		USING (owner = current_user)
		WITH CHECK (owner = current_user);

	-- DELETE + restrictive + USING only
	CREATE POLICY p_delete_restrictive
		ON rlsmtx_enable
		AS RESTRICTIVE
		FOR DELETE
		USING (owner = current_user);

	-- ENABLE + FORCE, one policy.
	CREATE TABLE rlsmtx_force (
		id BIGINT PRIMARY KEY,
		tenant TEXT NOT NULL,
		payload TEXT
	);
	ALTER TABLE rlsmtx_force ENABLE ROW LEVEL SECURITY;
	ALTER TABLE rlsmtx_force FORCE ROW LEVEL SECURITY;
	CREATE POLICY p_force_all
		ON rlsmtx_force
		FOR ALL
		USING (tenant = current_setting('app.tenant', true))
		WITH CHECK (tenant = current_setting('app.tenant', true));
`

// pgPolicyRow mirrors a pg_policies row, scanned post-round-trip on
// the target side to assert IR + emit preserved every captured field.
type pgPolicyRow struct {
	Name       string
	Cmd        string
	Permissive string
	Roles      string // raw text, parsed sufficient for the assertions
	Qual       sql.NullString
	WithCheck  sql.NullString
}

func TestRLSIREmit_RoundTripMatrix(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, dsn, rlsMatrixFixtureDDL)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Source-side read.
	srHandle, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("open source schema reader: %v", err)
	}
	defer func() {
		if c, ok := srHandle.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	source, err := srHandle.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("read source schema: %v", err)
	}

	// Assert IR-capture shape before re-emit.
	verifyIRMatrix(t, source)

	// Target-side write into a dedicated schema (so the round-trip
	// pg_policies probe matches only the round-tripped rows, not the
	// source's).
	const targetSchema = "rlsmtx_target"
	applyDDL(t, dsn, `CREATE SCHEMA IF NOT EXISTS `+targetSchema+`;`)

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("open target schema writer: %v", err)
	}
	defer func() {
		if c, ok := swHandle.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	// Re-bind to the target schema.
	if setter, ok := swHandle.(interface{ SetSchema(string) }); ok {
		setter.SetSchema(targetSchema)
	} else {
		t.Fatalf("schema writer does not implement SetSchema")
	}

	if err := swHandle.CreateTablesWithoutConstraints(ctx, source); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints on target: %v", err)
	}

	// Re-query pg_policies on the target schema; assert each policy
	// round-tripped with the right Command / Permissive / USING /
	// WITH CHECK and that ENABLE/FORCE flags landed.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open target db: %v", err)
	}
	defer func() { _ = db.Close() }()

	verifyTargetTableFlags(ctx, t, db, targetSchema)
	verifyTargetPolicies(ctx, t, db, targetSchema)
}

// verifyIRMatrix asserts the source-side ReadSchema produced the
// expected RLS-IR shape — Bug-74 catches a per-Command divergence
// here BEFORE the round-trip exercises the writer.
func verifyIRMatrix(t *testing.T, source *ir.Schema) {
	t.Helper()
	tables := map[string]*ir.Table{}
	for _, tbl := range source.Tables {
		tables[tbl.Name] = tbl
	}

	plain, ok := tables["rlsmtx_plain"]
	if !ok {
		t.Fatal("rlsmtx_plain missing from source IR")
	}
	if plain.RLSEnabled || plain.RLSForced || len(plain.Policies) > 0 {
		t.Errorf("rlsmtx_plain should have no RLS state; got enabled=%v forced=%v policies=%d",
			plain.RLSEnabled, plain.RLSForced, len(plain.Policies))
	}

	enable, ok := tables["rlsmtx_enable"]
	if !ok {
		t.Fatal("rlsmtx_enable missing from source IR")
	}
	if !enable.RLSEnabled || enable.RLSForced {
		t.Errorf("rlsmtx_enable: want enabled=true forced=false; got enabled=%v forced=%v",
			enable.RLSEnabled, enable.RLSForced)
	}
	// 5 policies covering ALL/SELECT/INSERT/UPDATE/DELETE.
	if got := len(enable.Policies); got != 5 {
		t.Fatalf("rlsmtx_enable: expected 5 policies; got %d", got)
	}
	got := map[string]*ir.Policy{}
	for _, p := range enable.Policies {
		got[p.Name] = p
	}
	// Per-cell assertions; Bug-74 discipline is to verify every cell.
	type want struct {
		Cmd        string
		Permissive bool
		HasUsing   bool
		HasCheck   bool
	}
	matrix := map[string]want{
		"p_all_using":          {Cmd: "ALL", Permissive: true, HasUsing: true, HasCheck: false},
		"p_select_using":       {Cmd: "SELECT", Permissive: true, HasUsing: true, HasCheck: false},
		"p_insert_check":       {Cmd: "INSERT", Permissive: true, HasUsing: false, HasCheck: true},
		"p_update_both":        {Cmd: "UPDATE", Permissive: true, HasUsing: true, HasCheck: true},
		"p_delete_restrictive": {Cmd: "DELETE", Permissive: false, HasUsing: true, HasCheck: false},
	}
	for name, w := range matrix {
		p, ok := got[name]
		if !ok {
			t.Errorf("policy %q missing from source IR", name)
			continue
		}
		if p.Command != w.Cmd {
			t.Errorf("%s: Command = %q; want %q", name, p.Command, w.Cmd)
		}
		if p.Permissive != w.Permissive {
			t.Errorf("%s: Permissive = %v; want %v", name, p.Permissive, w.Permissive)
		}
		if (p.Using != "") != w.HasUsing {
			t.Errorf("%s: HasUsing = %v; want %v (using=%q)", name, p.Using != "", w.HasUsing, p.Using)
		}
		if (p.Check != "") != w.HasCheck {
			t.Errorf("%s: HasCheck = %v; want %v (check=%q)", name, p.Check != "", w.HasCheck, p.Check)
		}
	}

	force, ok := tables["rlsmtx_force"]
	if !ok {
		t.Fatal("rlsmtx_force missing from source IR")
	}
	if !force.RLSEnabled || !force.RLSForced {
		t.Errorf("rlsmtx_force: want enabled=true forced=true; got enabled=%v forced=%v",
			force.RLSEnabled, force.RLSForced)
	}
	if got := len(force.Policies); got != 1 {
		t.Errorf("rlsmtx_force: expected 1 policy; got %d", got)
	}
}

// verifyTargetTableFlags reads pg_class for the target-schema tables
// to confirm ENABLE / FORCE landed on emit.
func verifyTargetTableFlags(ctx context.Context, t *testing.T, db *sql.DB, schema string) {
	t.Helper()
	cases := []struct {
		table       string
		wantEnabled bool
		wantForced  bool
	}{
		{"rlsmtx_plain", false, false},
		{"rlsmtx_enable", true, false},
		{"rlsmtx_force", true, true},
	}
	for _, tc := range cases {
		enabled, forced, err := probeTableRLSStatus(ctx, db, schema, tc.table)
		if err != nil {
			t.Errorf("probe target %s.%s: %v", schema, tc.table, err)
			continue
		}
		if enabled != tc.wantEnabled || forced != tc.wantForced {
			t.Errorf("target %s.%s: got enabled=%v forced=%v; want enabled=%v forced=%v",
				schema, tc.table, enabled, forced, tc.wantEnabled, tc.wantForced)
		}
	}
}

// verifyTargetPolicies queries pg_policies on the target schema and
// asserts each policy round-tripped: name, Command, Permissive, and
// USING/CHECK shape match the source matrix. The USING/CHECK
// expression text may be canonicalised by PG (pg_get_expr renders
// from the parse-tree form), so we assert presence rather than
// byte-equality.
func verifyTargetPolicies(ctx context.Context, t *testing.T, db *sql.DB, schema string) {
	t.Helper()
	const q = `
		SELECT policyname, cmd, permissive,
		       COALESCE(qual, ''), COALESCE(with_check, '')
		FROM   pg_policies
		WHERE  schemaname = $1
		ORDER  BY tablename, policyname`
	rows, err := db.QueryContext(ctx, q, schema)
	if err != nil {
		t.Fatalf("query target pg_policies: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var observed []pgPolicyRow
	for rows.Next() {
		var p pgPolicyRow
		var qual, check string
		if err := rows.Scan(&p.Name, &p.Cmd, &p.Permissive, &qual, &check); err != nil {
			t.Fatalf("scan target pg_policies: %v", err)
		}
		if qual != "" {
			p.Qual = sql.NullString{String: qual, Valid: true}
		}
		if check != "" {
			p.WithCheck = sql.NullString{String: check, Valid: true}
		}
		observed = append(observed, p)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate target pg_policies: %v", err)
	}

	// Expected matrix.
	type expect struct {
		Cmd        string
		Permissive string // "PERMISSIVE" / "RESTRICTIVE"
		HasUsing   bool
		HasCheck   bool
	}
	want := map[string]expect{
		"p_all_using":          {"ALL", "PERMISSIVE", true, false},
		"p_select_using":       {"SELECT", "PERMISSIVE", true, false},
		"p_insert_check":       {"INSERT", "PERMISSIVE", false, true},
		"p_update_both":        {"UPDATE", "PERMISSIVE", true, true},
		"p_delete_restrictive": {"DELETE", "RESTRICTIVE", true, false},
		"p_force_all":          {"ALL", "PERMISSIVE", true, true},
	}
	if len(observed) != len(want) {
		t.Errorf("policy count mismatch: got %d want %d; observed=%v",
			len(observed), len(want), observed)
	}
	for _, p := range observed {
		w, ok := want[p.Name]
		if !ok {
			t.Errorf("unexpected policy on target: %q", p.Name)
			continue
		}
		if !strings.EqualFold(p.Cmd, w.Cmd) {
			t.Errorf("%s: cmd = %q; want %q", p.Name, p.Cmd, w.Cmd)
		}
		if !strings.EqualFold(p.Permissive, w.Permissive) {
			t.Errorf("%s: permissive = %q; want %q", p.Name, p.Permissive, w.Permissive)
		}
		if p.Qual.Valid != w.HasUsing {
			t.Errorf("%s: HasUsing = %v; want %v (qual=%v)",
				p.Name, p.Qual.Valid, w.HasUsing, p.Qual.String)
		}
		if p.WithCheck.Valid != w.HasCheck {
			t.Errorf("%s: HasCheck = %v; want %v (check=%v)",
				p.Name, p.WithCheck.Valid, w.HasCheck, p.WithCheck.String)
		}
	}
}

// TestRLSIREmit_PreviewDDLEmitsRLS confirms `sluice schema preview`
// surfaces every RLS DDL operator inspection needs — ENABLE, FORCE,
// CREATE POLICY all appear with their Kind tags. Operators run
// preview to vet the target shape before applying; missing RLS rows
// would let them ship to a target whose policy layer they hadn't
// reviewed.
func TestRLSIREmit_PreviewDDLEmitsRLS(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, dsn, rlsMatrixFixtureDDL)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("open schema reader: %v", err)
	}
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	source, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}

	sw, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("open schema writer: %v", err)
	}
	defer func() {
		if c, ok := sw.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	previewer, ok := sw.(interface {
		PreviewDDL(context.Context, *ir.Schema) ([]ir.DDLStatement, error)
	})
	if !ok {
		t.Fatalf("schema writer does not implement PreviewDDL")
	}
	stmts, err := previewer.PreviewDDL(ctx, source)
	if err != nil {
		t.Fatalf("PreviewDDL: %v", err)
	}

	kinds := map[string]int{}
	for _, s := range stmts {
		kinds[s.Kind]++
	}
	if kinds["ALTER TABLE ENABLE RLS"] < 2 {
		t.Errorf("expected >= 2 ENABLE RLS rows (rlsmtx_enable + rlsmtx_force); got kinds=%v", kinds)
	}
	if kinds["ALTER TABLE FORCE RLS"] < 1 {
		t.Errorf("expected >= 1 FORCE RLS row; got kinds=%v", kinds)
	}
	if kinds["CREATE POLICY"] < 6 {
		t.Errorf("expected >= 6 CREATE POLICY rows; got kinds=%v", kinds)
	}
	// Order: every ENABLE / FORCE for a given table must precede that
	// table's CREATE POLICY rows. Scan the slice and confirm.
	verifyPreviewOrder(t, stmts)
}

func verifyPreviewOrder(t *testing.T, stmts []ir.DDLStatement) {
	t.Helper()
	enableSeen := map[string]bool{}
	for _, s := range stmts {
		switch s.Kind {
		case "ALTER TABLE ENABLE RLS":
			enableSeen[s.Table] = true
		case "CREATE POLICY":
			if !enableSeen[s.Table] {
				t.Errorf("CREATE POLICY for table %q precedes its ENABLE RLS row", s.Table)
			}
		}
	}
}

// TestRLSIREmit_EmptySchemaIsClean: a PG schema with zero RLS-bearing
// tables produces zero RLS DDL on the round-trip. Verifies the
// no-op short-circuit hasn't accidentally been turned into an
// always-emit case (which would produce a forest of empty ALTER
// TABLE rows on every CREATE TABLE).
func TestRLSIREmit_EmptySchemaIsClean(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, dsn, `
		CREATE TABLE plain_one (id BIGINT PRIMARY KEY, payload TEXT);
		CREATE TABLE plain_two (id BIGINT PRIMARY KEY, payload TEXT);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("open schema reader: %v", err)
	}
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	source, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	for _, tbl := range source.Tables {
		if tbl.RLSEnabled || tbl.RLSForced || len(tbl.Policies) > 0 {
			t.Errorf("plain table %q: unexpected RLS state enabled=%v forced=%v policies=%d",
				tbl.Name, tbl.RLSEnabled, tbl.RLSForced, len(tbl.Policies))
		}
	}

	sw, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("open schema writer: %v", err)
	}
	defer func() {
		if c, ok := sw.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	previewer, ok := sw.(interface {
		PreviewDDL(context.Context, *ir.Schema) ([]ir.DDLStatement, error)
	})
	if !ok {
		t.Fatalf("schema writer does not implement PreviewDDL")
	}
	stmts, err := previewer.PreviewDDL(ctx, source)
	if err != nil {
		t.Fatalf("PreviewDDL: %v", err)
	}
	for _, s := range stmts {
		switch s.Kind {
		case "ALTER TABLE ENABLE RLS", "ALTER TABLE FORCE RLS", "CREATE POLICY":
			t.Errorf("plain schema produced unexpected RLS row: %s %s", s.Kind, s.SQL)
		}
	}
}
