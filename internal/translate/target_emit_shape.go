// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The COMPARE-lane target-emitter shape pass (Bug 237(a) / roadmap item
// 156's type half). See [targetEmitShapeRuleFor] for why these rewrites
// live beside the pair rule tables rather than inside them.

package translate

import "sluicesync.dev/sluice/internal/ir"

// MySQLMaterializedTemporalPrecision is the fractional-second precision
// a MySQL-family target DECLARES for a source temporal that names no
// precision at all (a bare PG `time`/`timestamp`/`timestamptz`, a
// precision-less SQLite temporal).
//
// MySQL has no "engine-default 6" bare form — bare means 0 and would
// silently truncate fractional seconds — so the writer materializes the
// explicit maximum. That is stated in exactly one other place,
// internal/engines/mysql.effectiveTemporalPrecision, and the two must
// not disagree: a prediction that is off by one digit reports phantom
// drift on every bare temporal column, and a `migrate` re-run REFUSES on
// it (the ADR-0166 pre-create gate compares the same rendering).
//
// TestMaterializedTemporalPrecisionMatchesTheEmitter in
// internal/engines/mysql binds the two by driving the real emitter, so
// this constant cannot drift from the DDL it predicts.
const MySQLMaterializedTemporalPrecision = 6

// columnShapeRule rewrites ONE column's type into the shape the target's
// catalog will read back, given the whole column rather than just its
// type. Returning nil means "no rewrite" — the same convention
// [retargetRule] uses.
//
// It exists as a separate rule kind because two of the target emitters'
// shape decisions are not a function of the column's TYPE alone: the
// Postgres writer emits an enum-typed column as TEXT only when the
// column is GENERATED (Bug 25), so a `func(ir.Type) ir.Type` cannot
// express it.
type columnShapeRule func(*ir.Column) ir.Type

// targetEmitShapeRuleFor returns the COMPARE-lane rule that models what
// the TARGET engine's DDL emitter does to a column beyond what the
// (source, target) pair rule table says.
//
// # Why these are not arms of the pair rule tables
//
// Both rules below are keyed on the TARGET ALONE. They fire for every
// source — including a same-engine one, where no pair rule table exists
// at all — because the emitter applies them to whatever IR it is handed:
//
//   - A bare PG `timestamp` reaching a MySQL target lands as
//     `DATETIME(6)`, and a MySQL catalog reads back an explicit 6. That
//     is [retargetPGtoMySQL]'s territory, so it COULD have been an arm
//     there — but see the emit-lane note below.
//   - A generated enum column reaching a POSTGRES target lands as `TEXT`
//     and reads back as `Text[long]`, on a postgres→postgres pair as
//     much as on a mysql→postgres one. postgres→postgres has no rule
//     table and never will (the pair is identity by construction), so an
//     arm in a pair table could not have reached it.
//
// # Why COMPARE-lane only, and how that is enforced
//
// This pass is reachable from [RetargetForShapeCompare] and from nowhere
// else — [RetargetForEngine] never builds it. That is deliberate, and
// the reason is NOT that the rewrite would produce different DDL: it
// would not. The MySQL emitter already materializes 6 from the
// unspecified form, and the PG emitter already emits TEXT for a
// generated enum, so pre-applying either ahead of a SchemaWriter is a
// no-op on the SQL text.
//
// What it is not a no-op on is [ir.Column.SourceColumnType]. Every
// rewrite [retargetTable] performs RECORDS the type it replaced (Bug
// 232), and the emit lane's consumers — `restore`, `chain restore`, the
// broker's schema-delta apply — hand that provenance to a RowWriter.
// Running these two rewrites there would newly stamp a provenance on
// every bare temporal column and every generated enum column, for zero
// gain. The compare lane has no RowWriter downstream, so the same
// stamping is inert there.
//
// Identity for every target with no emitter effects of this kind
// (SQLite: it stores temporals as TEXT and declares no precision, so a
// precision-unspecified source reads back unspecified).
func targetEmitShapeRuleFor(targetEngine string) columnShapeRule {
	switch {
	case IsMySQLFamily(targetEngine):
		return mysqlTargetEmitShape
	case storageShapeFamily(targetEngine) == storageFamilyPostgres:
		return postgresTargetEmitShape
	}
	return nil
}

// mysqlTargetEmitShape materializes the temporal precision a MySQL
// target declares, and drops the time-zone flag MySQL's `TIME` cannot
// carry.
//
// # The two axes, both measured against a real MySQL 8.0 catalog
//
// PRECISION. A PG `TIMESTAMP` with no precision arrives as
// `ir.DateTime{PrecisionUnspecified: true}` (TRIAGE #3) and the MySQL
// writer declares `DATETIME(6)`; information_schema reports
// datetime_precision 6 and the reader rebuilds `ir.DateTime{Precision:
// 6}`. Un-normalised, the expected side says `DateTime(unspecified)`
// against `DateTime(6)` on every such column.
//
// ZONE. PG's `TIME WITH TIME ZONE` arrives as `ir.Time{WithTimeZone:
// true}`. MySQL has no `timetz` at all — internal/engines/mysql's
// emitColumnType has ONE `ir.Time` arm and it emits a plain `TIME(p)` —
// so the catalog reads back `ir.Time{}` with the flag clear, at EVERY
// precision. That axis is independent of the first: a `TIME(3) WITH TIME
// ZONE` needs no precision fix and still mismatches on the flag.
// `ir.Timestamp{WithTimeZone: true}` is NOT touched: MySQL's `TIMESTAMP`
// converts to UTC on store and back on retrieval, so its reader
// deliberately reports the column as a zoned timestamp and the flag
// round-trips.
//
// What this deliberately stops distinguishing, stated because it is a
// real cost: an expected `timestamp(6)` and a target `datetime(6)` still
// differ (they are different IR types); but an expected BARE temporal and
// a target explicitly-declared-6 one now compare equal, so an operator
// who deliberately re-declared a target column as `DATETIME(6)` when the
// source is bare sees no drift. Those two are the same column on MySQL,
// which is the whole point — precisions 0-5 stay distinct and still
// report.
func mysqlTargetEmitShape(c *ir.Column) ir.Type {
	if c == nil {
		return nil
	}
	// [ir.UnwrapDomain], not the declared type: this asks what STORAGE
	// the column lands in, and internal/engines/mysql's emitColumnType
	// answers it the same way — its `case ir.Domain` arm recurses into
	// the base type, so a domain over `timestamp` declares DATETIME(6)
	// exactly as a bare one does. (On a postgres→mysql pair the
	// storage-shape rule has already unwrapped by the time this runs;
	// doing it here too is idempotent and keeps the rule correct for a
	// pair that has no rule table.)
	switch v := ir.UnwrapDomain(c.Type).(type) {
	case ir.Time:
		if !v.PrecisionUnspecified && !v.WithTimeZone {
			return nil
		}
		return ir.Time{Precision: materializedTemporalPrecision(v.Precision, v.PrecisionUnspecified)}
	case ir.DateTime:
		if !v.PrecisionUnspecified {
			return nil
		}
		return ir.DateTime{Precision: MySQLMaterializedTemporalPrecision}
	case ir.Timestamp:
		if !v.PrecisionUnspecified {
			return nil
		}
		return ir.Timestamp{Precision: MySQLMaterializedTemporalPrecision, WithTimeZone: v.WithTimeZone}
	}
	return nil
}

// materializedTemporalPrecision mirrors
// internal/engines/mysql.effectiveTemporalPrecision. Bound to it by
// TestMaterializedTemporalPrecisionMatchesTheEmitter.
func materializedTemporalPrecision(precision int, unspecified bool) int {
	if unspecified {
		return MySQLMaterializedTemporalPrecision
	}
	return precision
}

// postgresTargetEmitShape models Bug 25: internal/engines/postgres's
// emitColumnDef emits an enum-typed STORED GENERATED column as `TEXT`
// rather than as the named enum type, because the `(body)::enum_type`
// cast a generated column would need calls `enum_in()`, which is STABLE
// and not IMMUTABLE. The value list survives as a table-level CHECK
// (see the emitter's emitGeneratedEnumCheckConstraint), which is the
// other half of this same defect and is handled by
// [ir.EmittedCheckPredictor].
//
// A NON-generated enum column keeps the native PG enum type and reads
// back as `ir.Enum`, so it is deliberately untouched — this is the arm
// that cannot be written as a `func(ir.Type) ir.Type`.
//
// The DECLARED type is read here, NOT [ir.UnwrapDomain] — deliberately,
// and against this package's usual rule. The emitter dispatches on the
// declared type at every one of its three sites, so a DOMAIN-wrapped
// enum lands as its domain and a prediction that unwrapped would
// disagree with the DDL. Registered in
// translateDomainDispatchExemptions with that reason, and bound to the
// emitter by TestPostgresGeneratedEnumEmitDispatchIsDeclaredType.
func postgresTargetEmitShape(c *ir.Column) ir.Type {
	if c == nil || !c.IsGenerated() {
		return nil
	}
	if _, isEnum := c.Type.(ir.Enum); !isEnum {
		return nil
	}
	return ir.Text{Size: ir.TextLong}
}
