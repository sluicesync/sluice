// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Unit tests for the row-level-security DDL emit (ADR-0063 — task #52
// sub-deliverables 2 + 3). Pairs with `rls_emit.go`.
//
// Bug-74 discipline: the matrix exercised is Command × Permissive ×
// USING/CHECK shape × ENABLE/FORCE, NOT a single representative
// policy. The reader/writer both dispatch on the Command kind via a
// string literal, so a green test on SELECT alone would not cover
// INSERT / UPDATE / DELETE / ALL — each shape gets its own row in the
// matrix below. See ADR-0063's test-strategy section for the rationale.

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestEmitRLSStatements_NoOpWhenNothingToEmit pins the common-case
// no-op: a table with RLSEnabled=false, RLSForced=false, and no
// Policies produces zero statements. Without this, the writer would
// emit empty ALTER TABLE lines for every non-RLS table — clutter at
// best, parse error at worst.
func TestEmitRLSStatements_NoOpWhenNothingToEmit(t *testing.T) {
	tbl := &ir.Table{Name: "plain"}
	got := emitRLSStatements("public", tbl)
	if len(got) != 0 {
		t.Errorf("expected zero statements for plain table; got %d: %v", len(got), got)
	}
}

// TestEmitRLSStatements_EnableOnly: RLSEnabled=true with no Policies
// emits exactly the ALTER TABLE ENABLE statement. Edge case operators
// hit when they want RLS enforcement turned on without per-row
// policies (PG's default with RLS on is "deny all" for non-owners).
func TestEmitRLSStatements_EnableOnly(t *testing.T) {
	tbl := &ir.Table{Name: "secrets", RLSEnabled: true}
	got := emitRLSStatements("public", tbl)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement; got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], `ALTER TABLE "public"."secrets" ENABLE ROW LEVEL SECURITY`) {
		t.Errorf("expected ENABLE statement; got %q", got[0])
	}
}

// TestEmitRLSStatements_EnablePlusForce: both flags set emits two
// ALTER statements, ENABLE then FORCE. Order matters because PG
// rejects FORCE on a non-ENABLE'd table; testing the order makes the
// invariant explicit.
func TestEmitRLSStatements_EnablePlusForce(t *testing.T) {
	tbl := &ir.Table{Name: "audits", RLSEnabled: true, RLSForced: true}
	got := emitRLSStatements("public", tbl)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements; got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], "ENABLE ROW LEVEL SECURITY") {
		t.Errorf("first statement should ENABLE; got %q", got[0])
	}
	if !strings.Contains(got[1], "FORCE ROW LEVEL SECURITY") {
		t.Errorf("second statement should FORCE; got %q", got[1])
	}
}

// TestEmitRLSStatements_EmitOrder pins the canonical order across the
// full matrix: ENABLE → FORCE → CREATE POLICY (in policy declaration
// order). CREATE POLICY before ENABLE is the silent-bug shape ADR-0063
// exists to close — a hand-edit that reorders these would fail this
// pin loudly.
func TestEmitRLSStatements_EmitOrder(t *testing.T) {
	tbl := &ir.Table{
		Name:       "tenants",
		RLSEnabled: true,
		RLSForced:  true,
		Policies: []*ir.Policy{
			{Name: "p_one", Command: "ALL", Permissive: true, Roles: []string{"public"}, Using: "tenant = current_user"},
			{Name: "p_two", Command: "SELECT", Permissive: true, Roles: []string{"public"}, Using: "active = true"},
		},
	}
	got := emitRLSStatements("public", tbl)
	if len(got) != 4 {
		t.Fatalf("expected 4 statements (ENABLE + FORCE + 2 policies); got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], "ENABLE") {
		t.Errorf("[0] expected ENABLE; got %q", got[0])
	}
	if !strings.Contains(got[1], "FORCE") {
		t.Errorf("[1] expected FORCE; got %q", got[1])
	}
	if !strings.HasPrefix(got[2], "CREATE POLICY") {
		t.Errorf("[2] expected CREATE POLICY; got %q", got[2])
	}
	if !strings.HasPrefix(got[3], "CREATE POLICY") {
		t.Errorf("[3] expected CREATE POLICY; got %q", got[3])
	}
	if !strings.Contains(got[2], `"p_one"`) || !strings.Contains(got[3], `"p_two"`) {
		t.Errorf("policy order mismatch; got %v", got[2:])
	}
}

// TestEmitCreatePolicy_CommandMatrix walks every Command variant the
// IR contract permits — ALL / SELECT / INSERT / UPDATE / DELETE — and
// confirms the `FOR <cmd>` clause renders correctly. This is the
// "pin the class, not the representative" pin for the Command
// dispatch — a per-command quirk in the emitter (e.g. UPPER-casing
// only some kinds) would surface here.
func TestEmitCreatePolicy_CommandMatrix(t *testing.T) {
	cases := []struct {
		cmd     string
		wantFor string
	}{
		{"ALL", "FOR ALL"},
		{"SELECT", "FOR SELECT"},
		{"INSERT", "FOR INSERT"},
		{"UPDATE", "FOR UPDATE"},
		{"DELETE", "FOR DELETE"},
	}
	for _, tc := range cases {
		t.Run(tc.cmd, func(t *testing.T) {
			p := &ir.Policy{
				Name:       "p_" + strings.ToLower(tc.cmd),
				Command:    tc.cmd,
				Permissive: true,
				Roles:      []string{"public"},
				Using:      "tenant = current_user",
			}
			got := emitCreatePolicy(`"public"."t"`, p)
			if !strings.Contains(got, tc.wantFor) {
				t.Errorf("Command %q: emit missing %q\n got %q", tc.cmd, tc.wantFor, got)
			}
		})
	}
}

// TestEmitCreatePolicy_PermissiveAndRestrictive: the default is
// PERMISSIVE and the writer omits the AS clause; RESTRICTIVE emits
// AS RESTRICTIVE explicitly. Pinning both branches ensures a future
// refactor to "always emit AS X" doesn't slip through.
func TestEmitCreatePolicy_PermissiveAndRestrictive(t *testing.T) {
	perm := emitCreatePolicy(`"public"."t"`, &ir.Policy{
		Name: "p", Command: "ALL", Permissive: true, Roles: []string{"public"}, Using: "true",
	})
	if strings.Contains(perm, "AS PERMISSIVE") || strings.Contains(perm, "AS RESTRICTIVE") {
		t.Errorf("permissive default should omit AS clause; got %q", perm)
	}
	restr := emitCreatePolicy(`"public"."t"`, &ir.Policy{
		Name: "p", Command: "ALL", Permissive: false, Roles: []string{"public"}, Using: "true",
	})
	if !strings.Contains(restr, "AS RESTRICTIVE") {
		t.Errorf("restrictive should emit AS RESTRICTIVE; got %q", restr)
	}
}

// TestEmitCreatePolicy_UsingCheckShapes covers the three USING/CHECK
// shapes ADR-0063 calls out as the matrix axis:
//   - USING only (SELECT / DELETE policies)
//   - CHECK only (INSERT policies)
//   - both USING and CHECK (UPDATE policies)
func TestEmitCreatePolicy_UsingCheckShapes(t *testing.T) {
	usingOnly := emitCreatePolicy(`"public"."t"`, &ir.Policy{
		Name: "p", Command: "SELECT", Permissive: true,
		Roles: []string{"public"}, Using: "owner = current_user",
	})
	if !strings.Contains(usingOnly, "USING (owner = current_user)") {
		t.Errorf("USING-only: missing USING clause; got %q", usingOnly)
	}
	if strings.Contains(usingOnly, "WITH CHECK") {
		t.Errorf("USING-only: spurious WITH CHECK; got %q", usingOnly)
	}

	checkOnly := emitCreatePolicy(`"public"."t"`, &ir.Policy{
		Name: "p", Command: "INSERT", Permissive: true,
		Roles: []string{"public"}, Check: "owner = current_user",
	})
	if !strings.Contains(checkOnly, "WITH CHECK (owner = current_user)") {
		t.Errorf("CHECK-only: missing WITH CHECK; got %q", checkOnly)
	}
	if strings.Contains(checkOnly, "USING (") {
		t.Errorf("CHECK-only: spurious USING; got %q", checkOnly)
	}

	both := emitCreatePolicy(`"public"."t"`, &ir.Policy{
		Name: "p", Command: "UPDATE", Permissive: true,
		Roles: []string{"public"},
		Using: "owner = current_user", Check: "owner = current_user",
	})
	if !strings.Contains(both, "USING (owner = current_user)") {
		t.Errorf("both: missing USING; got %q", both)
	}
	if !strings.Contains(both, "WITH CHECK (owner = current_user)") {
		t.Errorf("both: missing WITH CHECK; got %q", both)
	}
	// Order: USING precedes WITH CHECK in PG's grammar.
	if strings.Index(both, "USING (") > strings.Index(both, "WITH CHECK (") {
		t.Errorf("both: USING must precede WITH CHECK; got %q", both)
	}
}

// TestEmitCreatePolicy_RolesRendering: `public` renders unquoted
// (PG-pg_dump convention), arbitrary role names get double-quoted.
// Multiple roles are comma-joined.
func TestEmitCreatePolicy_RolesRendering(t *testing.T) {
	pub := emitCreatePolicy(`"public"."t"`, &ir.Policy{
		Name: "p", Command: "ALL", Permissive: true, Roles: []string{"public"}, Using: "true",
	})
	if !strings.Contains(pub, "TO public ") {
		t.Errorf("public role should emit unquoted; got %q", pub)
	}

	named := emitCreatePolicy(`"public"."t"`, &ir.Policy{
		Name: "p", Command: "ALL", Permissive: true,
		Roles: []string{"app_user", "admin"}, Using: "true",
	})
	if !strings.Contains(named, `TO "app_user", "admin"`) {
		t.Errorf("named roles should emit quoted + comma-joined; got %q", named)
	}
}

// TestEmitCreatePolicy_EmptyRolesDefaultsToPublic: hand-built IR
// without a Roles slice falls back to `public` (PG's CREATE POLICY
// default when TO is omitted). The reader never produces this shape;
// the test covers the defensive fallback so the writer can't emit
// `TO ` (syntax error).
func TestEmitCreatePolicy_EmptyRolesDefaultsToPublic(t *testing.T) {
	got := emitCreatePolicy(`"public"."t"`, &ir.Policy{
		Name: "p", Command: "ALL", Permissive: true, Using: "true",
	})
	if !strings.Contains(got, "TO public") {
		t.Errorf("empty Roles should fall back to public; got %q", got)
	}
}

// TestEmitCreatePolicy_EmptyCommandDefaultsToAll: the IR contract
// describes empty Command as the "fall back to ALL" hand-built case
// (PG's CREATE POLICY default). Same defensive shape as the roles
// fallback above.
func TestEmitCreatePolicy_EmptyCommandDefaultsToAll(t *testing.T) {
	got := emitCreatePolicy(`"public"."t"`, &ir.Policy{
		Name: "p", Permissive: true, Roles: []string{"public"}, Using: "true",
	})
	if !strings.Contains(got, "FOR ALL") {
		t.Errorf("empty Command should fall back to ALL; got %q", got)
	}
}

// TestEmitCreatePolicy_IdentifierQuoting: policy names and role names
// containing reserved-word / mixed-case characters round-trip with
// quoting, but the table reference is passed in pre-qualified by the
// caller so quoting policy is uniform across all writer surfaces.
func TestEmitCreatePolicy_IdentifierQuoting(t *testing.T) {
	got := emitCreatePolicy(`"my-schema"."Audit Log"`, &ir.Policy{
		Name: "Policy With Spaces", Command: "ALL", Permissive: true,
		Roles: []string{"app-user"}, Using: "true",
	})
	if !strings.Contains(got, `"Policy With Spaces"`) {
		t.Errorf("policy name should be quoted; got %q", got)
	}
	if !strings.Contains(got, `"app-user"`) {
		t.Errorf("role name should be quoted; got %q", got)
	}
	if !strings.Contains(got, `"my-schema"."Audit Log"`) {
		t.Errorf("table ref should round-trip verbatim; got %q", got)
	}
}

// TestRLSStatementKind classifies each rendered statement into the
// preview-DDL Kind tag — exercised by PreviewDDL output to label
// rows for operator inspection.
func TestRLSStatementKind(t *testing.T) {
	cases := []struct {
		stmt string
		want string
	}{
		{`ALTER TABLE "public"."t" ENABLE ROW LEVEL SECURITY;`, "ALTER TABLE ENABLE RLS"},
		{`ALTER TABLE "public"."t" FORCE ROW LEVEL SECURITY;`, "ALTER TABLE FORCE RLS"},
		{`CREATE POLICY "p" ON "public"."t" FOR ALL TO public;`, "CREATE POLICY"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			if got := rlsStatementKind(tc.stmt); got != tc.want {
				t.Errorf("rlsStatementKind(%q) = %q; want %q", tc.stmt, got, tc.want)
			}
		})
	}
}

// TestEmitRLSStatements_PoliciesWithoutEnableStillEnable: hand-built
// IR with Policies set but RLSEnabled=false (which the reader never
// produces — pg_class state always agrees with pg_policies presence)
// still emits ENABLE so the policies aren't inert. Documented in the
// emitter's comment block; pinned here so a future "trust RLSEnabled
// strictly" refactor doesn't silently regress the defensive shape.
func TestEmitRLSStatements_PoliciesWithoutEnableStillEnable(t *testing.T) {
	tbl := &ir.Table{
		Name: "edge",
		Policies: []*ir.Policy{
			{Name: "p", Command: "ALL", Permissive: true, Roles: []string{"public"}, Using: "true"},
		},
	}
	got := emitRLSStatements("public", tbl)
	if len(got) != 2 {
		t.Fatalf("expected ENABLE + CREATE POLICY; got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], "ENABLE ROW LEVEL SECURITY") {
		t.Errorf("[0] should ENABLE first (without it the policy is inert); got %q", got[0])
	}
}

// TestDecodeRoleArray walks the shapes the schema reader's roles
// JSON can take: a one-element `{public}` (the PG default), a
// multi-role list, and the catalog-malformed empty / null edge.
func TestDecodeRoleArray(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"single public", `["public"]`, []string{"public"}},
		{"multi", `["app_user","admin"]`, []string{"app_user", "admin"}},
		{"empty string", ``, nil},
		{"null", `null`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeRoleArray(tc.in)
			if err != nil {
				t.Fatalf("decodeRoleArray: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("len mismatch: got %v want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("[%d]: got %q want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}
