// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

// Engine-neutral target-schema / table-scope / verbatim-passthrough
// configurators (ADR-0031 / ADR-0047). These thread an operator
// decision onto a freshly-opened engine reader/writer/applier through
// the optional IR setter surfaces. They carry no orchestrator state —
// pure `any`-typed / capability-only helpers — so they live in migcore
// where both the migrate orchestrator and the carved backup/restore
// cluster can call them (audit 3.7b).

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// ValidateTargetSchema enforces the engine-capability gate for
// `--target-schema`. Engines whose schema scope is namespaced (PG)
// accept the override; engines with a flat namespace (MySQL) refuse
// with a clear message naming the DSN-choice workaround.
//
// Empty TargetSchema is a no-op — the orchestrator field defaults to
// the empty string, which preserves today's "use the DSN's schema"
// behaviour.
func ValidateTargetSchema(target ir.Engine, targetSchema string) error {
	if targetSchema == "" {
		return nil
	}
	if target == nil {
		return nil // validate() catches the nil engine separately
	}
	if target.Capabilities().SchemaScope == ir.SchemaScopeNamespaced {
		return nil
	}
	return fmt.Errorf(
		"pipeline: --target-schema is not supported on engine %q "+
			"(MySQL has no schema concept distinct from databases; "+
			"use a different --target DSN to namespace per-source "+
			"streams, e.g. --target=mysql://...:3306/customer_svc). "+
			"Multi-source --target-schema is PG-only in this release; "+
			"see docs/adr/adr-0031-multi-source-aggregation-target-schema.md",
		target.Name(),
	)
}

// ApplyTargetSchema threads an operator-supplied schema-name override
// to a freshly-opened engine reader/writer/applier via the optional
// [ir.SchemaSetter] surface. Engines that don't implement the setter
// are silently passed through — the validate gate has already refused
// the field for non-namespaced engines, so any engine that reaches
// this call with a non-empty targetSchema is expected to honour it.
//
// No-op when targetSchema is empty (today's default behaviour).
func ApplyTargetSchema(target any, targetSchema string) {
	if targetSchema == "" {
		return
	}
	if setter, ok := target.(ir.SchemaSetter); ok {
		setter.SetSchema(targetSchema)
	}
}

// ApplyVerbatimExtensionPassthrough threads the ADR-0047 verbatim
// passthrough decision to a freshly-opened engine reader / writer via
// the optional [ir.VerbatimExtensionAware] surface. Engines that don't
// implement it (today: MySQL) skip cleanly.
//
// The orchestrator is the determination authority and stays
// engine-neutral: it passes a boolean computed purely from the
// engines' declared [ir.Capabilities.VerbatimExtensionTypes] (never
// importing an engine package). enabled MUST be true only when the
// run provably does not need semantic type understanding for
// uncatalogued extension types:
//
//   - live PG → PG: both engines declare VerbatimExtensionTypes
//     (see verbatimLiveSameEnginePG); or
//   - a PG backup: the source declares VerbatimExtensionTypes and the
//     restore-target engine is unknown at backup time, so verbatim
//     columns are recorded on the lineage segment and a loud
//     restore-time engine gate enforces PG-restore-only.
//
// Cross-engine and non-PG runs pass enabled=false (or never call
// this), preserving ADR-0047 tier (c): the existing loud refusal for
// uncatalogued user-defined types is unchanged.
func ApplyVerbatimExtensionPassthrough(target any, enabled bool) {
	if !enabled {
		return
	}
	if aware, ok := target.(ir.VerbatimExtensionAware); ok {
		aware.SetVerbatimExtensionPassthrough(true)
	}
}

// ApplyTableScope threads the operator's table filter to a freshly-
// opened source [ir.SchemaReader] via the optional [ir.TableScoper]
// surface, so per-column type validation is scoped to the
// to-be-migrated tables (catalog Bug 76). Engines that don't implement
// TableScoper (today: MySQL) skip cleanly — the authoritative
// post-read [ApplyTableFilter] still prunes the schema there; only the
// Bug-76 usability gap (a scoped-out unsupported column aborting the
// run) remains until that engine grows the same push-down.
//
// An empty filter is still threaded: the predicate then admits every
// table, which is exactly the unscoped behaviour, so this is safe to
// call unconditionally. The filter passed here MUST already have
// engine-default exclusions merged ([EffectiveTableFilter]) so the
// push-down matches the post-read prune.
func ApplyTableScope(reader any, filter TableFilter) {
	scoper, ok := reader.(ir.TableScoper)
	if !ok {
		return
	}
	if filter.IsEmpty() {
		scoper.SetTableScope(nil)
		return
	}
	scoper.SetTableScope(filter.Allows)
}

// VerbatimBackupSourcePG reports whether a BACKUP run qualifies for
// the ADR-0047 verbatim tier: the source engine declares
// [ir.Capabilities.VerbatimExtensionTypes]. The restore-target engine
// is unknown at backup time, so qualifying here only enables CAPTURE;
// the PG-restore-only constraint is enforced by the recorded lineage
// marker + the loud restore-time engine gate (refuseVerbatimRestoreToNonPG).
func VerbatimBackupSourcePG(source ir.Engine) bool {
	return source != nil && source.Capabilities().VerbatimExtensionTypes
}
