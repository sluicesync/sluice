// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// PG-side DDL emit helpers for row-level security (ADR-0063 — task #52
// sub-deliverables 2 + 3). Pairs with [SchemaReader.populateRLS] on
// the read side: the reader captures `pg_class.relrowsecurity` /
// `relforcerowsecurity` and `pg_policies` rows into [ir.Table]; this
// file renders them back as `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`
// / `... FORCE ROW LEVEL SECURITY` / `CREATE POLICY` against the target.
//
// Emit order is load-bearing: CREATE POLICY before ALTER TABLE ENABLE
// is a silent-bug shape — the policies are defined but never enforced
// because RLS is off for the table. The helper always emits ENABLE
// first, then FORCE, then per-policy CREATE POLICY rows in the IR's
// captured order (the reader sorts by policy name; the writer
// preserves that for stable, diff-friendly DDL).

import (
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// emitRLSStatements renders the row-level-security DDL for a single
// table — empty slice for the common case (RLS off + no policies).
//
// Order, per ADR-0063's emit-order rationale:
//
//  1. `ALTER TABLE <qual> ENABLE ROW LEVEL SECURITY` when RLSEnabled
//  2. `ALTER TABLE <qual> FORCE ROW LEVEL SECURITY` when RLSForced
//  3. `CREATE POLICY <name> ON <qual> [AS RESTRICTIVE]
//     FOR <cmd> TO <roles> [USING (...)] [WITH CHECK (...)]`
//     for each policy in [ir.Table.Policies]
//
// The qualified-name shape mirrors the rest of the writer's emit (e.g.
// emitTableDef) — schema-qualified, double-quoted identifiers. The
// policy name and role names get the same quoting treatment so SQL
// identifiers with mixed case / reserved words round-trip cleanly.
//
// Defensive shape: when RLS is OFF (RLSEnabled=false) but the IR still
// carries policies, emit each policy AND the ENABLE — operators
// constructing the IR by hand may set Policies without explicitly
// flipping RLSEnabled, but PG requires ENABLE for the policies to
// have effect. The reader never produces this shape (RLSEnabled comes
// directly from pg_class) so the inverse — Policies set but
// RLSEnabled false — is exercised only by hand-built IR / tests.
func emitRLSStatements(schema string, table *ir.Table) []string {
	if table == nil {
		return nil
	}
	hasPolicies := len(table.Policies) > 0
	if !table.RLSEnabled && !table.RLSForced && !hasPolicies {
		return nil
	}

	qualified := quoteIdent(schema) + "." + quoteIdent(table.Name)

	out := make([]string, 0, 2+len(table.Policies))

	// ENABLE always when RLSEnabled OR when Policies are populated
	// without an explicit RLSEnabled — without ENABLE the policy
	// definitions are inert.
	if table.RLSEnabled || hasPolicies {
		out = append(out, "ALTER TABLE "+qualified+" ENABLE ROW LEVEL SECURITY;")
	}
	if table.RLSForced {
		out = append(out, "ALTER TABLE "+qualified+" FORCE ROW LEVEL SECURITY;")
	}

	for _, p := range table.Policies {
		out = append(out, emitCreatePolicy(qualified, p))
	}
	return out
}

// emitCreatePolicy renders one `CREATE POLICY` statement for a single
// [ir.Policy]. Defensive defaults:
//
//   - empty Command defaults to "ALL" (PG's default when `FOR ...` is
//     omitted in CREATE POLICY)
//   - empty Roles defaults to a single-element `{public}` (PG's
//     default when `TO ...` is omitted) — matches what the reader
//     would have captured from a `CREATE POLICY` without an explicit
//     TO clause
//   - empty USING + empty Check on a non-INSERT/non-UPDATE policy
//     would mean "no filter" — the writer emits neither clause,
//     letting PG's permissive default fire. The reader never produces
//     this shape (PG always populates qual for SELECT/UPDATE/DELETE
//     /ALL); reserved as the legitimate hand-built shape rather than
//     a hard refusal.
//
// Bug 74 discipline: the per-Command branch is collapsed into a
// single FOR-clause render so the emit path is byte-identical across
// ALL / SELECT / INSERT / UPDATE / DELETE — only the FOR keyword
// varies. This prevents the "tested SELECT, missed UPDATE" trap the
// ADR's test-strategy section calls out.
func emitCreatePolicy(qualifiedTable string, p *ir.Policy) string {
	if p == nil {
		return ""
	}
	cmd := strings.ToUpper(p.Command)
	if cmd == "" {
		cmd = "ALL"
	}
	roles := p.Roles
	if len(roles) == 0 {
		roles = []string{"public"}
	}

	var sb strings.Builder
	sb.WriteString("CREATE POLICY ")
	sb.WriteString(quoteIdent(p.Name))
	sb.WriteString(" ON ")
	sb.WriteString(qualifiedTable)
	// Permissive is PG's default; emit AS RESTRICTIVE only when the
	// IR says so. Keeping the emit minimal in the default case keeps
	// the target's pg_dump output diff-stable against a hand-written
	// permissive source.
	if !p.Permissive {
		sb.WriteString(" AS RESTRICTIVE")
	}
	sb.WriteString(" FOR ")
	sb.WriteString(cmd)
	sb.WriteString(" TO ")
	sb.WriteString(formatPolicyRoles(roles))
	if p.Using != "" {
		sb.WriteString(" USING (")
		sb.WriteString(p.Using)
		sb.WriteByte(')')
	}
	if p.Check != "" {
		sb.WriteString(" WITH CHECK (")
		sb.WriteString(p.Check)
		sb.WriteByte(')')
	}
	sb.WriteByte(';')
	return sb.String()
}

// formatPolicyRoles renders the `TO` role-list for CREATE POLICY.
// PG's `public` (lowercase, unquoted) is the reserved pseudo-role
// every CREATE POLICY without an explicit TO clause carries. The
// reader captures it verbatim; the writer emits it bare so the
// resulting DDL is `TO public` (not `TO "public"`). Other roles get
// the standard quoteIdent treatment so case-preserved names with
// mixed case / dashes round-trip cleanly.
//
// The `public` carve-out matches PG's own pg_dump output, which emits
// `TO public` for the default case. Quoting it as `"public"` would
// produce semantically equivalent DDL but make every target's
// pg_dump differ from the source's by exactly this one quoting
// difference, complicating operator-side diff comparisons.
func formatPolicyRoles(roles []string) string {
	parts := make([]string, 0, len(roles))
	for _, r := range roles {
		if r == "public" || r == "PUBLIC" {
			parts = append(parts, "public")
			continue
		}
		parts = append(parts, quoteIdent(r))
	}
	return strings.Join(parts, ", ")
}

// rlsStatementKind classifies a rendered RLS statement for the
// PreviewDDL output (which carries a Kind tag alongside the SQL text).
// Three shapes correspond to the three emit lines; the prefix match
// is sufficient because emitRLSStatements is the only producer and
// its leading tokens are stable.
func rlsStatementKind(stmt string) string {
	switch {
	case strings.HasPrefix(stmt, "CREATE POLICY"):
		return "CREATE POLICY"
	case strings.Contains(stmt, "FORCE ROW LEVEL SECURITY"):
		return "ALTER TABLE FORCE RLS"
	case strings.Contains(stmt, "ENABLE ROW LEVEL SECURITY"):
		return "ALTER TABLE ENABLE RLS"
	default:
		// Defensive — emitRLSStatements is the only producer; an
		// unknown shape would be a bug. Fall through to a generic
		// label so preview rendering still works.
		return fmt.Sprintf("ALTER TABLE %s", stmt)
	}
}
