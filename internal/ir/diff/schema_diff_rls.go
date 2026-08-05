// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

// Row-level-security comparison for `sluice schema diff` (audit
// 2026-08-04 B-10). ADR-0063 put RLSEnabled / RLSForced / Policies in the
// IR to close task #52's silent-security-regression failure mode — a
// target schema arriving without them. Nothing compared them, so the one
// operator surface for checking that outcome reported "in sync", exit 0,
// on a target where row-level security had been switched off and every
// tenant could read every tenant's rows.

import (
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// PolicyDiff captures one RLS policy's expected-vs-actual mismatch, for a
// policy present on both sides under the same name. Only the fields that
// actually differ are populated.
//
// # What is compared, and why exactly these
//
// Every attribute of a [ir.Policy] decides what the policy ENFORCES, so
// unlike the index and FK comparisons there is nothing here to leave out:
//
//   - Command — which statements the policy filters at all. A policy
//     narrowed from ALL to SELECT stops constraining writes entirely.
//   - Permissive — permissive policies OR together, restrictive ones AND.
//     Flipping one to permissive can ADMIT rows the source refuses.
//   - Roles — who the policy applies to. A policy retargeted off the
//     application's role is inert.
//   - Using — the read filter (which rows are visible).
//   - Check — the write filter (which rows may be written).
//
// USING and WITH CHECK are compared as verbatim trimmed text. Both sides
// come from PG's `pg_policies`, which renders them from the parsed tree,
// so two structurally identical expressions produce identical text and
// any divergence is a real change. RLS is PG-only, so there is no
// cross-engine spelling noise to soften for.
type PolicyDiff struct {
	Name string `json:"name"`

	ExpectedCommand string `json:"expected_command,omitempty"`
	ActualCommand   string `json:"actual_command,omitempty"`

	// PermissiveMismatched gates the bool pair for the reason
	// [IndexDiff.UniqueMismatched] does.
	PermissiveMismatched bool `json:"permissive_mismatched,omitempty"`
	ExpectedPermissive   bool `json:"expected_permissive,omitempty"`
	ActualPermissive     bool `json:"actual_permissive,omitempty"`

	// ExpectedRoles / ActualRoles render the role list as a sorted,
	// comma-joined string. Sorted because a role LIST is a set — PG does
	// not guarantee `pg_policies.roles` ordering across catalog reads, and
	// reporting a reordering as drift would be a false positive on a
	// healthy pair.
	ExpectedRoles string `json:"expected_roles,omitempty"`
	ActualRoles   string `json:"actual_roles,omitempty"`

	ExpectedUsing string `json:"expected_using,omitempty"`
	ActualUsing   string `json:"actual_using,omitempty"`

	ExpectedCheck string `json:"expected_check,omitempty"`
	ActualCheck   string `json:"actual_check,omitempty"`
}

// diffRowLevelSecurity populates the RLS flag pair and the policy slices
// on td. Policies are matched by name (set semantics, the same the CHECK,
// EXCLUDE and FK comparisons use).
//
// The ENABLE/FORCE flags are compared as a UNIT and are NOT scoped by
// opts.IgnoreExtras: a flag is not an "extra on target" object, it is a
// property of a table present on both sides, and an operator who asked to
// ignore other applications' tables has not asked to be kept from hearing
// that this one stopped enforcing its policies.
func diffRowLevelSecurity(td *TableDiff, expected, actual *ir.Table, opts Options) {
	if expected.RLSEnabled != actual.RLSEnabled || expected.RLSForced != actual.RLSForced {
		td.RLSMismatched = true
		td.ExpectedRLSEnabled, td.ActualRLSEnabled = expected.RLSEnabled, actual.RLSEnabled
		td.ExpectedRLSForced, td.ActualRLSForced = expected.RLSForced, actual.RLSForced
	}

	expPol := policiesByName(expected)
	actPol := policiesByName(actual)

	for name := range expPol {
		if _, ok := actPol[name]; !ok {
			td.PoliciesMissing = append(td.PoliciesMissing, name)
		}
	}
	sort.Strings(td.PoliciesMissing)

	if !opts.IgnoreExtras {
		for name := range actPol {
			if _, ok := expPol[name]; !ok {
				td.PoliciesExtra = append(td.PoliciesExtra, name)
			}
		}
		sort.Strings(td.PoliciesExtra)
	}

	common := make([]string, 0, len(expPol))
	for name := range expPol {
		if _, ok := actPol[name]; ok {
			common = append(common, name)
		}
	}
	sort.Strings(common)

	for _, name := range common {
		if pd, mismatched := diffPolicy(name, expPol[name], actPol[name]); mismatched {
			td.PoliciesMismatched = append(td.PoliciesMismatched, pd)
		}
	}
}

// diffPolicy compares one policy pair. See [PolicyDiff] for the attribute
// set and the reasoning.
func diffPolicy(name string, expected, actual *ir.Policy) (PolicyDiff, bool) {
	pd := PolicyDiff{Name: name}
	mismatched := false

	if e, a := strings.ToUpper(strings.TrimSpace(expected.Command)), strings.ToUpper(strings.TrimSpace(actual.Command)); e != a {
		pd.ExpectedCommand, pd.ActualCommand = e, a
		mismatched = true
	}
	if expected.Permissive != actual.Permissive {
		pd.PermissiveMismatched = true
		pd.ExpectedPermissive, pd.ActualPermissive = expected.Permissive, actual.Permissive
		mismatched = true
	}
	if e, a := renderPolicyRoles(expected.Roles), renderPolicyRoles(actual.Roles); e != a {
		pd.ExpectedRoles, pd.ActualRoles = e, a
		mismatched = true
	}
	if e, a := strings.TrimSpace(expected.Using), strings.TrimSpace(actual.Using); e != a {
		pd.ExpectedUsing, pd.ActualUsing = e, a
		mismatched = true
	}
	if e, a := strings.TrimSpace(expected.Check), strings.TrimSpace(actual.Check); e != a {
		pd.ExpectedCheck, pd.ActualCheck = e, a
		mismatched = true
	}
	return pd, mismatched
}

// renderPolicyRoles renders a policy's role list as a sorted, comma-
// joined string. "<none>" for an empty list so an empty-vs-populated
// difference is visible rather than rendering as an empty pair (an empty
// list is a sluice-bug condition the PG writer already refuses loudly —
// PG always populates it, `{public}` for the default case).
func renderPolicyRoles(roles []string) string {
	if len(roles) == 0 {
		return "<none>"
	}
	out := make([]string, len(roles))
	copy(out, roles)
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// policiesByName indexes a table's Policies by name. Unnamed entries are
// skipped on the same rationale as [checksByName]; PG always names a
// policy (system-generated when not explicitly named), so an unnamed
// entry means a corrupt or hand-built IR.
func policiesByName(t *ir.Table) map[string]*ir.Policy {
	out := make(map[string]*ir.Policy, len(t.Policies))
	for _, p := range t.Policies {
		if p == nil || p.Name == "" {
			continue
		}
		out[p.Name] = p
	}
	return out
}
