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
// anyway.
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
// for a PK-member column the nullability flag is overwritten with the
// expected side's (i.e. excluded from the compare — see
// TableColumnShape's doc comment). The copy keeps the inputs pristine.
func actualForCompare(act, exp *ir.Column, pkCols map[string]struct{}) *ir.Column {
	if _, isPK := pkCols[act.Name]; !isPK {
		return act
	}
	normalized := *act
	normalized.Nullable = exp.Nullable
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
