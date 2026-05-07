// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Operator-facing hint layer for wrapped engine errors.
//
// Engine errors today come through wrapped with phase prefixes (e.g.
// "pipeline: bulk copy: postgres: insert into \"users\": ERROR:
// relation \"users\" does not exist"). The phase prefix is good; the
// inner Postgres/MySQL error is correct but cryptic to operators who
// haven't memorised every database's surface. A single hint line —
// appended after the wrapped error, on its own line — turns most
// head-scratchers into one-glance diagnoses.
//
// Design tenets for this file:
//
//   - Tiny, load-bearing registry. Each hint is maintenance forever;
//     anything beyond the most common 5-10 errors is noise. If a
//     hint would only fire on an obscure SQLSTATE that 99% of
//     operators will never hit, it doesn't belong here.
//   - Substring matching is intentional. v1 doesn't try to extract
//     SQLSTATE codes or structured fields from driver errors —
//     case-insensitive substrings are good enough and survive minor
//     wording changes between database versions.
//   - Hints never replace the original error. They're appended after
//     a newline with a "hint:" prefix; the underlying error stays
//     intact for [errors.Is]/[errors.As] traversal.
//   - Phase-scoped. The same substring can mean different things in
//     different phases (e.g. "does not exist" during bulk-copy means
//     the target table is missing; during CDC it could mean a
//     replication slot is gone). Empty phase = match in any phase.
//
// What NOT to put here:
//
//   - Translation/localisation of every possible driver error.
//   - Anything that requires parsing the error message structure.
//   - Hints that duplicate information already in the wrapped phase
//     prefix (e.g. "this happened during bulk-copy").
//   - Engine-version-specific or vendor-specific errors that surface
//     for <1% of operators (e.g. Vitess error 1370). When in doubt,
//     leave it out.

import (
	"fmt"
	"strings"
)

// Phase identifiers used by [hintFor] and [wrapWithHint] to scope
// hint matching. The strings are stable: tests reference them, and
// the registry keys off them. Empty string means "any phase".
const (
	PhaseConnect     = "connect"
	PhaseSchemaApply = "schema-apply"
	PhaseBulkCopy    = "bulk-copy"
	PhaseIndexes     = "indexes"
	PhaseConstraints = "constraints"
	PhaseCDC         = "cdc"
	PhaseSnapshot    = "snapshot"
)

// errorHint is one entry in the hint registry: a substring match
// against the wrapped error's text plus a phase scope, mapped to a
// short operator-facing line. Order matters — first match wins.
type errorHint struct {
	// phase scopes the match. Empty matches in any phase.
	phase string

	// contains is matched case-insensitively against err.Error().
	contains string

	// hint is the line emitted after the wrapped error. No leading
	// "hint:" prefix; that's added by wrapWithHint.
	hint string
}

// hintRegistry is the ordered list of hints. Order matters because
// the first match wins — put more-specific entries before more-
// general ones. Each entry's comment explains when it fires.
var hintRegistry = []errorHint{
	// Bulk-copy: target table not present. Postgres surfaces
	// "relation \"x\" does not exist"; MySQL surfaces
	// "Table 'x.y' doesn't exist". Both point at the same root
	// cause: schema-apply silently failed or wrote into a
	// different schema/database than the bulk-copy target uses.
	{
		phase:    PhaseBulkCopy,
		contains: "does not exist",
		hint:     "target table not found — did the schema-apply phase fail or apply to a different schema?",
	},
	{
		phase:    PhaseBulkCopy,
		contains: "doesn't exist",
		hint:     "target table not found — did the schema-apply phase fail or apply to a different schema?",
	},

	// Connect-time DSN errors. These three cover the bulk of
	// "I can't even start the migration" reports: the host is
	// unreachable, the credentials are wrong, or the named
	// database isn't there.
	{
		phase:    PhaseConnect,
		contains: "connection refused",
		hint:     "verify the DSN host/port and that the database is reachable from this machine",
	},
	{
		phase:    PhaseConnect,
		contains: "password authentication failed",
		hint:     "verify the DSN username and password",
	},
	// Connect-phase "database does not exist": PG emits
	// "database \"foo\" does not exist". The substring is narrow
	// enough that scoping to PhaseConnect avoids overlap with the
	// bulk-copy "does not exist" hint above.
	{
		phase:    PhaseConnect,
		contains: "does not exist",
		hint:     "verify the --target DSN database name",
	},

	// Schema-apply: target role lacks CREATE on the schema.
	// Postgres surfaces "permission denied for schema X". The
	// fix is operator-side (GRANT or use a different role); the
	// hint nudges operators toward that diagnosis without us
	// silently retrying with elevated privileges.
	{
		phase:    PhaseSchemaApply,
		contains: "permission denied for schema",
		hint:     "the target role lacks CREATE on the schema; verify GRANT or use a different role",
	},

	// CDC: replication-role attribute missing. Postgres surfaces
	// "permission denied for replication" when the connecting
	// role doesn't have the REPLICATION attribute. docs/postgres-
	// source-prep.md documents the GUCs and roles this needs.
	{
		phase:    PhaseCDC,
		contains: "permission denied for replication",
		hint:     "the connecting role needs the REPLICATION attribute (ALTER ROLE x REPLICATION); see docs/postgres-source-prep.md",
	},
}

// hintFor returns the hint line for the given phase and error, or
// "" when no entry matches. Lookup is case-insensitive substring
// matching; the first registered entry whose phase scope and
// substring match both apply wins.
//
// Phase-empty entries (errorHint.phase == "") match in any phase.
// An empty phase argument matches only entries that are themselves
// phase-empty — the orchestrator always passes a concrete phase, so
// this case is mostly for future use and tests.
func hintFor(phase string, err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	for _, h := range hintRegistry {
		if h.phase != "" && h.phase != phase {
			continue
		}
		if !strings.Contains(msg, strings.ToLower(h.contains)) {
			continue
		}
		return h.hint
	}
	return ""
}

// wrapWithHint returns err with a "hint: ..." line appended when
// the registry has a relevant entry for the given phase. When no
// hint matches, returns err unchanged so the call site reads the
// same as it did before this layer existed.
//
// The wrapping uses %w, so [errors.Is] and [errors.As] still
// traverse the chain normally — the hint is presentation, not
// structure. A nil err returns nil.
func wrapWithHint(phase string, err error) error {
	if err == nil {
		return nil
	}
	h := hintFor(phase, err)
	if h == "" {
		return err
	}
	return fmt.Errorf("%w\nhint: %s", err, h)
}
