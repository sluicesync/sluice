// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"sluicesync.dev/sluice/internal/ir"
)

// MissingNonGeneratedColumns returns, sorted, the target table's
// non-generated columns that row does NOT carry — i.e. the columns an
// INSERT built from row would leave to the target's DEFAULT.
//
// This is the discriminator between the two shapes an [ir.Update]'s
// after-image can have, which the appliers must NOT confuse:
//
//   - COMPLETE — every non-generated target column is present. An UPDATE
//     and an INSERT(after-image) upsert have the same end state, so the
//     upsert form (MySQL's ON DUPLICATE KEY UPDATE, ADR-0140) is a valid
//     rewrite of the UPDATE.
//   - PARTIAL — at least one non-generated target column is absent. The
//     IR's contract for an absent after-image key is "preserve the
//     target's existing value" (see the Postgres CDC reader's
//     unchanged-TOAST omission: pgoutput sends an unchanged out-of-line
//     column as the 'u' datum and decodeTuple drops it). An UPDATE
//     honours that — it SETs only the present columns. An upsert does
//     NOT: when the target row is absent the INSERT branch fires and
//     writes the target's DEFAULT into a column the source never spoke
//     about, fabricating a row that never existed at the source (audit
//     2026-08-05 C-10).
//
// Generated columns are excluded on the same grounds as
// [NonGeneratedRowKeys]: neither engine accepts a value for one, so
// their absence from an after-image is never an omission.
//
// A nil/empty colTypes map (cache cold) returns no missing columns —
// tolerant in the same direction as [NonGeneratedRowKeys], because an
// unknown target shape is not evidence of an omission.
func MissingNonGeneratedColumns(row ir.Row, colTypes map[string]*ir.Column) []string {
	if len(colTypes) == 0 {
		return nil
	}
	var missing []string
	for name, col := range colTypes {
		if col != nil && col.IsGenerated() {
			continue
		}
		if _, ok := row[name]; !ok {
			missing = append(missing, name)
		}
	}
	slices.Sort(missing)
	return missing
}

// ErrNoRowPredicate is the sentinel for an [ir.Update] / [ir.Delete] that
// carries no usable before-image, so no WHERE predicate can be built for
// it. Errors wrapping it are matchable with [errors.Is].
var ErrNoRowPredicate = errors.New("change carries no before-image row predicate")

// RefuseNoRowPredicate builds the loud refusal for an UPDATE or DELETE
// whose before-image renders to an empty WHERE clause — because Before is
// nil, empty, or consists only of generated columns (which
// [NonGeneratedRowKeys] filters out).
//
// Refusing is the decision, not a fallback, and it is deliberately the
// SAME decision on every applier (audit 2026-08-05 C-9). The alternatives
// are both wrong: a predicate-less UPDATE/DELETE would touch every row of
// the table, and rewriting the change into a key-derived upsert — the one
// form that "works" without a before-image — silently INSERTs a row the
// source only ever described as an update, which is C-10 by another door.
// Before the fix the two appliers diverged here for exactly that reason:
// MySQL's coalescing batch path rewrote a nil-Before Update into an
// ON DUPLICATE KEY upsert and applied it, while Postgres (and MySQL's own
// serial path) emitted `... WHERE ` and let the SERVER reject the
// syntax — loud, but as an unattributed 1064/42601 naming neither the
// stream nor the row.
//
// Every source in the tree supplies a before-image for both events (the
// Postgres reader synthesizes a key-only one when pgoutput omits the old
// tuple, and refuses REPLICA IDENTITY NOTHING outright; both trigger
// readers already refuse a NULL before-image on DELETE), so this is a
// corruption / new-engine guard rather than a routine path.
func RefuseNoRowPredicate(engine, op, schema, table string, before ir.Row) error {
	return fmt.Errorf(
		"%s: applier: %s on %s: %w (before-image carries %v, none of it usable as a predicate) "+
			"— applying it would either touch every row in the table or fabricate one; the source "+
			"must supply the row's key columns",
		engine, op, qualified(schema, table), ErrNoRowPredicate, rowColumnNames(before),
	)
}

// qualified renders "schema.table", or just "table" when schema is empty —
// the same spelling [ir.Change.QualifiedName] produces, so a refusal names
// the row the way every other applier message does.
func qualified(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// rowColumnNames is the sorted key set of row. Used by the engines' refusal
// messages to name what the before-image DID carry when nothing in it was
// usable — a before-image of only generated columns is a different operator
// problem from a nil one, and the message should not flatten them.
func rowColumnNames(row ir.Row) []string {
	return slices.Sorted(maps.Keys(row))
}
