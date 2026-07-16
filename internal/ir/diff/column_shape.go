// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

// Focused column-shape comparison for the migrate pre-create gate
// (ADR-0166): given the table the migration INTENDS to create and the
// same-named table that already exists on the target, decide whether
// the existing table is shape-equal enough to skip the CREATE — or
// name exactly which columns differ so the refusal is actionable.
//
// The gate deliberately compares LESS than Schemas does: column names
// (order-insensitive), types (the same typeString rendering the
// schema-diff command trusts), and nullability. Indexes, constraints,
// defaults, generated expressions, comments, and charset/collation are
// all OUT of scope — later migrate phases create indexes/constraints
// idempotently (detect-then-skip), and a deploy-request-bootstrapped
// table legitimately carries them already; defaults and charsets are
// the schema-diff's documented cross-engine noise sources and don't
// affect whether the bulk copy can land rows faithfully.

import (
	"sort"

	"sluicesync.dev/sluice/internal/ir"
)

// ColumnShapeMismatch names one column whose shape differs between the
// intended table and the existing target table. Expected/Actual carry
// a rendered "type, NULL|NOT NULL" description (or the absent
// sentinel) so refusal messages can show both sides verbatim.
type ColumnShapeMismatch struct {
	Column   string
	Expected string
	Actual   string
}

// columnShapeAbsent is the rendered side of a mismatch where the
// column exists on only one side.
const columnShapeAbsent = "(absent)"

// TableColumnShape compares expected (the table the migration intends
// to create, already translated/retargeted to the target engine's
// storage shapes) against actual (the same-named table read back from
// the target catalog) and returns the column-shape mismatches, sorted
// by column name. An empty result means the existing table's column
// shape matches and the CREATE can be skipped.
//
// Nullability is NOT compared for columns in expected's primary key:
// both MySQL and PG force PK columns NOT NULL regardless of the
// declared flag, and readers report the enforced state — comparing the
// redundant flag would false-refuse a table the engine itself
// normalized. A wrong-nullability PK is unrepresentable on the target
// anyway. Integer AutoIncrement is likewise excluded for every column
// (see actualForCompare): it doesn't affect copy correctness and PG
// doesn't round-trip it.
func TableColumnShape(expected, actual *ir.Table) []ColumnShapeMismatch {
	if expected == nil || actual == nil {
		return nil
	}
	expCols := columnsByName(expected)
	actCols := columnsByName(actual)
	pkCols := primaryKeyColumnSet(expected)

	var out []ColumnShapeMismatch
	for name, exp := range expCols {
		act, ok := actCols[name]
		if !ok {
			out = append(out, ColumnShapeMismatch{
				Column:   name,
				Expected: renderColumnShape(exp, pkCols),
				Actual:   columnShapeAbsent,
			})
			continue
		}
		expShape := renderColumnShape(exp, pkCols)
		actShape := renderColumnShape(actualForCompare(act, exp, pkCols), pkCols)
		if expShape != actShape {
			out = append(out, ColumnShapeMismatch{Column: name, Expected: expShape, Actual: actShape})
		}
	}
	for name, act := range actCols {
		if _, ok := expCols[name]; ok {
			continue
		}
		out = append(out, ColumnShapeMismatch{
			Column:   name,
			Expected: columnShapeAbsent,
			Actual:   renderColumnShape(act, nil),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Column < out[j].Column })
	return out
}

// actualForCompare normalizes the actual column for the comparison:
// excluded fields are overwritten with the expected side's value so
// they can never produce a mismatch (see TableColumnShape's doc
// comment). The copy keeps the inputs pristine. Excluded:
//   - PK-member nullability (engines force NOT NULL; readers report
//     the enforced state).
//   - Integer AutoIncrement, for EVERY column: it never affects
//     whether the bulk copy can land rows (inserts carry explicit
//     values either way), and readers cannot always round-trip it —
//     PG reads a table whose id was created as bigserial/identity
//     back without the flag, which false-refused the Bug-81
//     sibling-shard load (a second shard migrating into the first
//     shard's tables) in CI. DDL for genuinely new tables still
//     carries the flag; only the skip-vs-refuse decision ignores it.
func actualForCompare(act, exp *ir.Column, pkCols map[string]struct{}) *ir.Column {
	normalized := *act
	if actInt, ok := normalized.Type.(ir.Integer); ok {
		if expInt, ok2 := exp.Type.(ir.Integer); ok2 {
			actInt.AutoIncrement = expInt.AutoIncrement
			normalized.Type = actInt
		}
	}
	if _, isPK := pkCols[act.Name]; isPK {
		normalized.Nullable = exp.Nullable
	}
	return &normalized
}

// renderColumnShape renders the compared portion of a column — type +
// nullability — as one stable string, shared by the compare and the
// refusal message so what is compared is exactly what is shown.
func renderColumnShape(c *ir.Column, pkCols map[string]struct{}) string {
	if c == nil {
		return columnShapeAbsent
	}
	shape := typeString(c.Type)
	if _, isPK := pkCols[c.Name]; isPK {
		// PK nullability is excluded from the compare; render the
		// column without a nullability clause so the two sides can't
		// disagree on the excluded field.
		return shape + ", PRIMARY KEY member"
	}
	if c.Nullable {
		return shape + ", NULL"
	}
	return shape + ", NOT NULL"
}

// primaryKeyColumnSet returns the set of column names in t's primary
// key (empty for a PK-less table).
func primaryKeyColumnSet(t *ir.Table) map[string]struct{} {
	if t == nil || t.PrimaryKey == nil {
		return nil
	}
	out := make(map[string]struct{}, len(t.PrimaryKey.Columns))
	for _, ic := range t.PrimaryKey.Columns {
		out[ic.Column] = struct{}{}
	}
	return out
}
