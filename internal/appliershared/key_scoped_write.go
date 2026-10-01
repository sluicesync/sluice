// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// KeyScopedWriteMultiMatchMarker is the grep-stable marker every applier's
// [RefuseKeyScopedMultiMatch] refusal carries; it is the text of
// [ErrKeyScopedWriteMatchedMultipleRows] (TestRefuseKeyScopedMultiMatch).
const KeyScopedWriteMultiMatchMarker = "KEY-SCOPED-WRITE-MATCHED-MULTIPLE-ROWS"

// ErrKeyScopedWriteMatchedMultipleRows is the sentinel for an UPDATE or
// DELETE that identifies its row by key and matched more than one row on a
// target table. Errors wrapping it are matchable with [errors.Is]. It IS
// [ir.ErrKeyScopedWriteMatchedMultipleRows], whose text is the marker, so the
// fleet supervisor, which keys on the ir sentinel, does not restart a leg
// that hit it.
var ErrKeyScopedWriteMatchedMultipleRows = ir.ErrKeyScopedWriteMatchedMultipleRows

// keyScopedMultiMatchHint is the CodedError hint: the remedy, stripped of
// the diagnosis the message carries.
const keyScopedMultiMatchHint = "nothing was written — the apply transaction was rolled back — but resuming re-applies the same change " +
	"and refuses again: re-copy the target with `sluice sync start --reset-target-data`; if the source table has a DEFERRABLE key, " +
	"its key-shifting transactions will recur, so make the source key immediate or keep such transactions off the table"

// keyScopedMultiMatchError is the terminal refusal. Retrying cannot help:
// the same change against the same target state matches the same rows.
type keyScopedMultiMatchError struct{ msg string }

func (e *keyScopedMultiMatchError) Error() string  { return e.msg }
func (e *keyScopedMultiMatchError) Unwrap() error  { return ErrKeyScopedWriteMatchedMultipleRows }
func (e *keyScopedMultiMatchError) Terminal() bool { return true }

// RefuseKeyScopedMultiMatch builds the loud refusal for an UPDATE or DELETE
// whose WHERE predicate — the change's before-image, which the source
// readers narrow to the row's identity key — matched `matched` > 1 rows on
// the target (GC-42).
//
// A key-scoped write that touches two rows has, by construction, touched a
// row the source never named. The main mechanism, on Postgres, is a
// DEFERRABLE primary key: a source transaction may move key values through
// each other (`UPDATE t SET id = id + 1`, a swap), the
// constraint is checked only at commit, and a key-narrowed change in between
// carries only the OLD key — so once two target rows share a key value
// mid-transaction, `DELETE … WHERE id = 2` deletes both, and the commit-time
// re-check passes because the final state is unique. Before this refusal
// that was a silent loss at exit 0. No applier can recover the intended row
// from a narrowed before-image, so refusing is the decision, not a fallback.
//
// Callers skip only the writes [KeyScopedWriteExempt] exempts (a keyless
// table addressed by its whole row). Zero rows stays tolerated (ADR-0010
// resume idempotency); only MORE than one is refused.
//
// The key VALUES are named because they are what an operator must query to
// triage. They are post-redaction (the applier redacts before dispatch), so
// they are exactly what the target already holds.
func RefuseKeyScopedMultiMatch(engine, op, schema, table string, before ir.Row, matched int64) error {
	return sluicecode.Wrap(sluicecode.CodeCDCKeyMatchedMultipleRows, keyScopedMultiMatchHint, &keyScopedMultiMatchError{
		msg: fmt.Sprintf(
			"%s: applier: %s on %s: %s: the change's key (%s) matched %d target rows, so applying it would %s rows "+
				"the source never touched — the key is not unique on the target at this point in the stream: either a "+
				"DEFERRABLE primary key the source transaction moved key values through (a key shift or swap), or a "+
				"target table that does not hold these columns unique; the apply transaction was rolled back, nothing "+
				"was written",
			engine, op, qualified(schema, table), KeyScopedWriteMultiMatchMarker,
			describeRowKey(before), matched, verbFor(op),
		),
	})
}

// KeyScopedWriteExempt reports whether an UPDATE/DELETE is exempt from the
// multi-row check: the target table has no key (keyed is false: no PRIMARY
// KEY and no NOT NULL unique index) AND the before-image names every
// non-generated target column, so the WHERE is the whole row. Two rows that
// match a whole-row predicate are identical, and which one goes is the
// ADR-0089 keyless caveat, not a mis-addressed write.
//
// Keylessness alone is NOT enough, because the before-image's narrowing
// follows the SOURCE: a source with a key under REPLICA IDENTITY FULL (or a
// trigger source) sends a before-image of only its key columns, and against
// an operator-made keyless target `DELETE … WHERE id = 2` would delete every
// row with that id. Such a write is checked like any keyed one. An empty
// colTypes (an unknown target shape) cannot prove the image covers the row,
// so it is not exempt.
func KeyScopedWriteExempt(keyed bool, before ir.Row, colTypes map[string]*ir.Column) bool {
	return !keyed && len(colTypes) > 0 && len(MissingNonGeneratedColumns(before, colTypes)) == 0
}

// verbFor is the operator-facing consequence of a multi-matched op.
func verbFor(op string) string {
	if op == "delete" {
		return "delete"
	}
	return "overwrite"
}

// describeRowKeyValueMax bounds one rendered key value so a wide key column
// (a bytea, a long text) cannot turn a refusal into a log flood.
const describeRowKeyValueMax = 80

// describeRowKey renders a before-image as sorted `col=value` pairs.
func describeRowKey(row ir.Row) string {
	cols := rowColumnNames(row)
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		v := fmt.Sprintf("%v", row[c])
		if row[c] == nil {
			v = "NULL"
		}
		if len(v) > describeRowKeyValueMax {
			v = v[:describeRowKeyValueMax] + "…"
		}
		parts = append(parts, c+"="+v)
	}
	return strings.Join(parts, ", ")
}
