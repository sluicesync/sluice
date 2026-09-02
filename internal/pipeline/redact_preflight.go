// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/redact"
)

// The redaction preflight itself lives in [migcore.PreflightRedactRules]
// (moved for audit 2026-08-27 NEW-1, so the backup lane — whose package
// this one imports — can run the same check before its first chunk).
// These are the pipeline-side names the orchestrator and its tests use;
// the sentinels alias migcore's so `errors.Is` matches either spelling.
var (
	errRedactTypeMismatch           = migcore.ErrRedactTypeMismatch
	errRedactRandomizeNoPK          = migcore.ErrRedactRandomizeNoPK
	errRedactKeysetMissing          = migcore.ErrRedactKeysetMissing
	errRedactSelectorUnresolved     = migcore.ErrRedactSelectorUnresolved
	errRedactOnGeneratedColumn      = migcore.ErrRedactOnGeneratedColumn
	errRedactRandomizeRangeOverflow = migcore.ErrRedactRandomizeRangeOverflow
)

// preflightRedactTypes runs the redaction preflight for a single-
// namespace run (migrate without namespace-scope flags, the single-
// database sync cold-start, add-table). See [migcore.PreflightRedactRules].
func preflightRedactTypes(reg *redact.Registry, schema *ir.Schema) error {
	return migcore.PreflightRedactRules(reg, schema, "")
}

// preflightRedactTypesInScope is the multi-namespace-aware form: scope
// is nil for a single-namespace run and carries the pass's namespace
// in the ADR-0074 / ADR-0075 fan-out, where a schema-qualified rule for
// ANOTHER selected namespace is that namespace's pass's business (and
// [migcore.PreflightRedactNamespaces], run once by the fan-out driver,
// has already refused a rule naming a namespace outside the selection).
func preflightRedactTypesInScope(reg *redact.Registry, schema *ir.Schema, scope *multiDBScope) error {
	return migcore.PreflightRedactRules(reg, schema, scope.namespace())
}

// namespace returns the source namespace this pass reads, or "" for a
// nil (single-namespace) scope.
func (s *multiDBScope) namespace() string {
	if s == nil {
		return ""
	}
	return s.database
}

// preflightRedact is the add-table lane's redaction preflight (Bug 60 /
// Bug 99 / audit 2026-08-27 NEW-1), run on the one table about to be
// bulk-copied, after the mappings. The migrate and sync cold-start
// lanes have run this since v0.58.1; add-table's bulk copy did not, so
// a --redact rule that resolved to nothing (or to a namespace no bulk
// row carries) reached runBulkCopyForAddTable unchecked. The stream's
// registry also holds rules for tables that are NOT the one being
// added; those are expected to miss the single-table schema and must
// not refuse, so only the rules naming this table are checked.
func (a *AddTable) preflightRedact(scoped *ir.Schema) error {
	if err := preflightRedactTypes(redactRulesForTable(a.Redactor, a.TableName), scoped); err != nil {
		return fmt.Errorf("pipeline: add-table: %w", err)
	}
	return nil
}

// redactRulesForTable narrows reg to the rules naming table, for the
// add-table lane: its schema holds ONE table, while the stream's
// registry holds rules for every table the stream redacts, and a rule
// for another table is not a typo. Returns reg itself when it is
// nil/empty (the zero-cost path); the table compare is case-folded
// because [redact.Rule] hands back the registry's lowercased key.
func redactRulesForTable(reg *redact.Registry, table string) *redact.Registry {
	if reg.Empty() {
		return reg
	}
	out := redact.New()
	for _, rule := range reg.Rules() {
		if strings.EqualFold(rule.Table, table) {
			out.Set(rule.Schema, rule.Table, rule.Column, rule.Strategy)
		}
	}
	return out
}
