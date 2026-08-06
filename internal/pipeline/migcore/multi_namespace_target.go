// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ValidateMultiNamespaceTarget refuses a multi-namespace fan-out aimed at a
// target that cannot keep the namespaces apart (roadmap item 148, route 2).
//
// # The false premise this replaces
//
// Both fan-out orchestrators (`migrate` and `sync` cold-start) branch on the
// target implementing [ir.DatabaseDSNDeriver]. The migrate branch's `else` arm
// carried the comment "it is unreachable with today's engines, but kept so the
// orchestrator stays engine-neutral", and set `TargetSchema` to the target
// namespace on the assumption that a non-deriver target must at least be
// namespaced. That premise was FALSE from the moment SQLite landed: SQLite
// implements neither [ir.DatabaseDSNDeriver] nor [ir.SchemaSetter], so the
// assignment was made and then silently discarded by [ApplyTargetSchema] —
// which skips engines without the setter — and every source namespace wrote
// bare, unqualified table names into ONE SQLite file. Two source namespaces
// carrying a same-named table merged, at exit 0.
//
// It was also invisible to the gate that WOULD have caught it:
// [ValidateTargetSchema] refuses `--target-schema` on a flat-scope engine, but
// it runs from `Migrator.validate` at the top of `Run`, and the fan-out sets
// `TargetSchema` per database AFTERWARDS and calls `runSingleDatabase`
// directly, which never re-validates.
//
// # Why this refuses the CONFIGURATION rather than the collision
//
// A name-collision check is the wrong instrument here, and the difference is
// not conservatism. The collision is a symptom of the run having no target
// namespace at all: the fan-out's contract is N source namespaces → N target
// namespaces, and a flat target can offer exactly one. Same-named tables are
// the case where that loses data today; distinct names still produce a merged
// file that neither `verify`, a later `add-table`, nor CDC apply can route
// back to a source namespace. The project already refuses the explicit form of
// this — [resolveTargetNamespaces] rejects two sources mapped to one target
// name — and a flat target is that same many-to-one for every N > 1.
//
// # What the predicate is, and why it is engine-neutral
//
// A target can namespace a fan-out if it can derive a per-namespace DSN
// ([ir.DatabaseDSNDeriver] — MySQL's CREATE DATABASE, PG's CREATE SCHEMA), or
// if it declares [ir.SchemaScopeNamespaced], which is the same capability
// [ValidateTargetSchema] gates `--target-schema` on. Anything else is flat.
// No engine package is imported and no connection is opened; the answer comes
// from the engine's own declarations.
//
// N is the number of selected source namespaces and is used only for the
// message: a fan-out resolving to exactly ONE namespace is a rename or a
// single-namespace filter, which a flat target CAN serve losslessly, so it is
// deliberately allowed through.
func ValidateMultiNamespaceTarget(target ir.Engine, mode string, selected []string) error {
	if target == nil {
		return errors.New("migcore: ValidateMultiNamespaceTarget: target engine is nil")
	}
	if len(selected) < 2 {
		// One source namespace needs no target namespacing: everything the
		// run writes belongs to it, so the result is byte-identical to the
		// plain single-namespace migrate a flat target already serves.
		// Refusing it would be the over-refusal direction. The residual,
		// stated rather than left implied: a `--map-database old:new` here is
		// COSMETIC on a flat target, because there is no target namespace for
		// the new name to be. Nothing is lost — a flat target never carries a
		// namespace name in any emitted identifier — but the rename is not
		// honoured either.
		return nil
	}
	if _, canDerive := target.(ir.DatabaseDSNDeriver); canDerive {
		return nil
	}
	if target.Capabilities().SchemaScope == ir.SchemaScopeNamespaced {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeConfigMultiNamespaceTargetFlat,
		"run one source namespace per invocation, each with its own target (for a file target, its own "+
			"file), or choose a target engine that namespaces",
		fmt.Errorf(
			"pipeline: %s: %d source namespaces (%v) were selected for a fan-out into target engine %q, "+
				"which has a FLAT namespace: it can neither derive a per-namespace target DSN nor accept a "+
				"target-schema override, so every source namespace would be written into ONE target "+
				"namespace under bare, unqualified names. Two namespaces carrying a same-named table would "+
				"SILENTLY MERGE — the second table's rows landing in the first, at exit 0 — and even "+
				"without a name collision the result cannot be routed back to a source namespace",
			mode, len(selected), selected, target.Name(),
		),
	)
}
