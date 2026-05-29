// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// DegradedFK records one foreign-key constraint that was attached to
// the target in a non-validated state because the source carried at
// least one row that would have violated it. The constraint is real
// on the target (PG attaches it via `ALTER TABLE ... ADD CONSTRAINT
// ... FOREIGN KEY ... NOT VALID`) — what's deferred is the validation
// pass over existing rows. New rows that violate the constraint are
// still rejected by PG; the operator runs `ALTER TABLE ... VALIDATE
// CONSTRAINT <name>` after fixing the orphans.
//
// Surfaced when an operator opts into `--allow-degraded-fks` on a
// PG target. The dirty-source case where this matters is when the
// source has known orphan rows the migration shouldn't fail on, and
// the operator wants the target's constraint definitions intact for
// future inserts even though existing rows can't all be validated.
// See `docs/dev/notes/pgcopydb-planetscale-fork-review.md`
// (pgcopydb PR #27) for the upstream pattern this mirrors.
type DegradedFK struct {
	// Schema and Table identify the child (referencing) table on the
	// target. Schema is the writer's emit-schema (typically the
	// destination namespace from the DSN, not the source's namespace).
	Schema string
	Table  string

	// ConstraintName is the FK's name as it appears on the target.
	ConstraintName string

	// LocalColumns is the column list on the child table that
	// references the parent.
	LocalColumns []string

	// ReferencedTable is the parent (referenced) table. Carried
	// unqualified — the writer's emit-schema applies.
	ReferencedTable string

	// ReferencedColumns is the column list on the parent.
	ReferencedColumns []string

	// Reason is the original error text from the failed validating
	// `ADD CONSTRAINT`. Verbatim so operators can grep their logs
	// and reproduce what PG reported.
	Reason string

	// Hint is the operator-facing follow-up — typically the exact
	// SQL the operator should run after fixing the orphans, plus a
	// short pointer to the docs.
	Hint string
}

// DegradedFKAllower is implemented by [SchemaWriter] implementations
// that support the `--allow-degraded-fks` opt-in. The pipeline calls
// [EnableDegradedFKs] before [SchemaWriter.CreateConstraints] when
// the operator passes the flag; writers that do NOT implement this
// interface refuse loudly (the pipeline catches that case so the
// operator gets an actionable error before any DDL runs).
//
// PG implements it (NOT VALID FK semantic is native). MySQL does
// NOT — its closest analogue is session-wide `SET FOREIGN_KEY_CHECKS
// = 0`, which is a different contract; the flag is PG-target-only by
// design.
type DegradedFKAllower interface {
	EnableDegradedFKs()
}

// DegradedFKReporter exposes the list of FKs that were attached
// degraded on the most-recent [SchemaWriter.CreateConstraints] call.
// Returns nil/empty if the feature wasn't enabled, the writer doesn't
// support it, or every FK validated cleanly. The pipeline surfaces
// the list in its operator-facing report after the constraints phase
// completes.
type DegradedFKReporter interface {
	DegradedFKs() []DegradedFK
}
