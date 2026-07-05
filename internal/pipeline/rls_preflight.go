// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Postgres Row-Level Security (RLS) preflight (task #52 sub-deliverable 1).
//
// Closes two silent-loss classes RLS introduces when sluice is run as a
// non-BYPASSRLS role against tables that have RLS enabled:
//
//   - Source snapshot silently RLS-filtered (PG → *): the reader role
//     lacks BYPASSRLS, so `pg_policies` filters rows out of bulk-copy.
//     The migration "succeeds" with fewer rows than the source — no
//     error, no warning, no signal. This is the worst class: the
//     operator only finds out by independent row-count audit.
//   - Target INSERTs blocked by WITH CHECK (* → PG): the writer role
//     lacks BYPASSRLS, so bulk-copy / CDC apply trips a "new row
//     violates row-level security policy" error mid-pipeline — opaque
//     symptom, the operator has to know what to look for.
//
// Both modes are loud-failure-incompatible. The preflight inspects
// `pg_class.relrowsecurity` (RLS enabled) + `relforcerowsecurity`
// (forced even for the table owner) for every in-scope table on each
// PG side, plus `pg_roles.rolbypassrls` for the connecting role. When
// any table has RLS enabled AND the role lacks BYPASSRLS, the preflight
// refuses with a structured error that names the offending tables, the
// current role, what BYPASSRLS does, and the operator-actionable
// recovery (grant BYPASSRLS, use a superuser/owner role, or
// `--exclude-table` the table out of scope).
//
// Non-PG engines (MySQL on either side) silently skip — RLS is a PG-only
// concept. Tables without RLS enabled also skip silently — the preflight
// is a no-op on the common case (verified by the lowercase-control test).
//
// Reference: PlanetScale's "RLS sounds great until it isn't" blog and
// CLAUDE.md's loud-failure tenet. No `--allow-rls-without-bypass` opt-
// out flag is provided per the tenet; operators who want to migrate
// RLS-enabled tables grant BYPASSRLS or scope around them with
// `--exclude-table`.
//
// Scope (this file is sub-deliverable 1 of 3):
//   - Probe + refuse before bulk-copy / CDC engagement.
//   - Diagnose-bundle surface (per-table RLS status + role attribute).
//   - Unit + integration tests pinning the 4 cells of {RLS on/off} ×
//     {role BYPASSRLS yes/no} plus the FORCE variant.
//
// Out of scope (sub-deliverables 2 + 3):
//   - IR capture (ir.Policy on ir.Table) and CREATE POLICY emit.
//   - Full Bug-74-style matrix integration tests.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// errRLSRefused is the sentinel cause for an RLS-preflight refusal.
// Wrapped with the per-side message naming the offending tables and the
// current role. Tests assert via errors.Is to avoid coupling to the
// message text.
var errRLSRefused = errors.New("pipeline: RLS preflight refused")

// rlsSide identifies which side of the migration the preflight is
// probing — its only purpose is to render an operator-facing
// "source" / "target" label in the refusal message.
type rlsSide int

const (
	rlsSideSource rlsSide = iota
	rlsSideTarget
)

func (s rlsSide) label() string {
	if s == rlsSideSource {
		return "source"
	}
	return "target"
}

// rlsPreflightProber is the optional surface a PG engine handle (the
// [ir.SchemaReader] for the source-side probe, the [ir.RowWriter] for
// the target-side probe) implements to drive the RLS preflight.
//
// Each method maps to one piece of operator-relevant catalog state:
//
//   - TableRLSStatus reports the per-table RLS-enable flag and the
//     FORCE-RLS flag. `enabled` corresponds to `pg_class.relrowsecurity`;
//     `forced` to `relforcerowsecurity`. The two are orthogonal: a
//     table can be ENABLE'd without FORCE (the owner / superuser
//     bypasses) or both ENABLE'd and FORCE'd (no one bypasses without
//     the role-level BYPASSRLS attribute).
//   - CurrentRoleBypassesRLS reports whether the connected role has the
//     BYPASSRLS attribute (`pg_roles.rolbypassrls`) plus the role's
//     own name. The role name is surfaced in the refusal message so
//     the operator knows which role to grant BYPASSRLS to (or which
//     role to swap for a superuser).
//
// Engines that don't implement the surface (MySQL today; PG always
// does) are silently skipped — RLS is a PG-only concept.
//
// Defined in the pipeline package rather than `ir` because it is
// orchestrator-private (matches the shape of [shardPreflightProber]).
type rlsPreflightProber interface {
	TableRLSStatus(ctx context.Context, table *ir.Table) (enabled, forced bool, err error)
	CurrentRoleBypassesRLS(ctx context.Context) (bypass bool, role string, err error)
}

// rlsViolation captures one table whose RLS state would silently break
// a migration when the connecting role lacks BYPASSRLS. Sorted output
// in the refusal message keeps error text deterministic across runs
// (matching preflightColdStart's first-offender shape).
type rlsViolation struct {
	Table  string
	Forced bool
}

// preflightRLS runs the RLS-preflight against one side (source or
// target). Returns nil when:
//
//   - The handle doesn't implement [rlsPreflightProber] (non-PG engine,
//     or a PG engine surface that doesn't expose the probes — the
//     opportunistic-skip posture matches [preflightColdStart]).
//   - No table in the schema has RLS enabled.
//   - The current role has BYPASSRLS (so RLS policies don't apply
//     regardless of per-table state).
//
// Returns a wrapped [errRLSRefused] when at least one table has
// `relrowsecurity=true` AND the role lacks BYPASSRLS. The message
// names every offending table (sorted), the FORCE-RLS subset (each
// offender's `forced` flag drives a parenthetical note), the
// connecting role, and the three operator-actionable recovery paths
// (grant BYPASSRLS, use a superuser/owner role, --exclude-table).
//
// `side` controls the operator-facing wording — "source" vs "target"
// — and is used to disambiguate the two-sided refusal message.
func preflightRLS(ctx context.Context, schema *ir.Schema, handle any, side rlsSide) error {
	if schema == nil || len(schema.Tables) == 0 {
		return nil
	}
	prober, ok := handle.(rlsPreflightProber)
	if !ok {
		// Non-PG side or PG surface that doesn't expose the probes —
		// silently skip. RLS is PG-only; the opportunistic-skip posture
		// matches preflightColdStart for engines without the surface.
		return nil
	}

	// Probe BYPASSRLS first. The common operator setup is "superuser
	// connection with BYPASSRLS" — we can short-circuit before walking
	// every table in that case.
	bypass, role, err := prober.CurrentRoleBypassesRLS(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
			"pipeline: RLS preflight: probe %s role BYPASSRLS: %w", side.label(), err,
		))
	}
	if bypass {
		// Role has BYPASSRLS → RLS policies don't apply on this side,
		// regardless of per-table state. Nothing to refuse.
		return nil
	}

	var violations []rlsViolation
	for _, table := range schema.Tables {
		if table == nil {
			continue
		}
		enabled, forced, err := prober.TableRLSStatus(ctx, table)
		if err != nil {
			return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
				"pipeline: RLS preflight: probe %s table %q RLS state: %w",
				side.label(), table.Name, err,
			))
		}
		if !enabled {
			continue
		}
		violations = append(violations, rlsViolation{Table: table.Name, Forced: forced})
	}
	if len(violations) == 0 {
		return nil
	}

	sort.Slice(violations, func(i, j int) bool { return violations[i].Table < violations[j].Table })
	return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
		"%w: %s",
		errRLSRefused, formatRLSRefusal(side, role, violations),
	))
}

// formatRLSRefusal renders the operator-facing refusal message. The
// shape mirrors other preflight refusals in this package — name the
// concrete state (tables + role), explain the mechanism (BYPASSRLS),
// and list every operator-actionable recovery path so the operator
// can pick the one that fits their security posture.
func formatRLSRefusal(side rlsSide, role string, violations []rlsViolation) string {
	tableList := make([]string, 0, len(violations))
	hasForce := false
	for _, v := range violations {
		entry := fmt.Sprintf("%q", v.Table)
		if v.Forced {
			entry += " (FORCE RLS)"
			hasForce = true
		}
		tableList = append(tableList, entry)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s table(s) have row-level security enabled and the connecting role %q lacks BYPASSRLS: %s. ",
		side.label(), role, strings.Join(tableList, ", "))

	b.WriteString("Without BYPASSRLS the role is subject to every CREATE POLICY rule on these tables — ")
	if side == rlsSideSource {
		b.WriteString("rows that fail the USING expression are silently filtered out of the source snapshot " +
			"(silent data loss; the migration would 'succeed' with fewer rows than the source). ")
	} else {
		b.WriteString("INSERTs that fail any WITH CHECK expression are rejected with " +
			"'new row violates row-level security policy' mid-bulk-copy or mid-CDC-apply. ")
	}
	if hasForce {
		b.WriteString("At least one table is marked FORCE ROW LEVEL SECURITY — even the table owner is RLS-checked under FORCE; " +
			"you need BYPASSRLS regardless of table ownership. ")
	}
	b.WriteString("Recovery: (a) grant BYPASSRLS to the sluice role: `ALTER ROLE ")
	b.WriteString(role)
	b.WriteString(" BYPASSRLS;` [preferred — the documented PG-source / PG-target prep step]; ")
	b.WriteString("(b) re-run sluice with a superuser or table-owner role that already has BYPASSRLS ")
	b.WriteString("(note: a non-superuser owner still needs BYPASSRLS when the table is FORCE-RLS); ")
	b.WriteString("(c) explicitly scope the table(s) out of the migration via `--exclude-table` if the data they hold ")
	b.WriteString("is intentionally tenant-scoped and should not cross to the target")
	return b.String()
}
