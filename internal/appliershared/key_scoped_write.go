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
// the diagnosis the message carries. It is a constant, so it states only what
// holds on every apply path: the TARGET transaction is rolled back, and the
// SOURCE transaction may not be — see [keyScopedMultiMatchRolledBack].
const keyScopedMultiMatchHint = "this target transaction was rolled back, but earlier parts of the same source transaction may already be " +
	"committed on the target, and resuming re-applies the same change and refuses again: re-copy the target with " +
	"`sluice sync start --reset-target-data`; if the source table has a DEFERRABLE key, its key-shifting transactions will " +
	"recur, so make the source key immediate or keep such transactions off the table"

// keyScopedMultiMatchRolledBack is what the refusal can say, on every apply
// path, about what the rollback undid (Bug 294). Rolling back the target
// transaction undoes everything in it — and nothing a source transaction had
// already committed in an EARLIER target transaction, which the per-change
// path, a batch flush, a lane and a lane barrier all do. Through v0.156.8
// this said "nothing was written", which was false on exactly the default
// apply path: the repro's key-shifting UPDATE was already committed, and the
// table held two rows on one key under a DEFERRABLE primary key.
const keyScopedMultiMatchRolledBack = "this target transaction was rolled back, so nothing in it was written; earlier parts of the " +
	"same source transaction may already be committed on the target (an apply batch boundary, a lane, a key change applied " +
	"alone as a lane barrier, and --apply-batch-size 1 each split a source transaction across target transactions)"

// keyScopedMultiMatchSplitNote is the [ir.SourceTxSplitNoter] sentence, for
// an apply loop that KNOWS an earlier target transaction committed part of
// this source transaction.
const keyScopedMultiMatchSplitNote = "this source transaction WAS split: an earlier target transaction committed part of it, " +
	"which stays on the target, so the table now holds a state the source never had (under a DEFERRABLE key, possibly " +
	"two rows on one key) until the re-copy replaces it"

// keyScopedMultiMatchError is the terminal refusal. Retrying cannot help:
// the same change against the same target state matches the same rows.
type keyScopedMultiMatchError struct{ msg string }

func (e *keyScopedMultiMatchError) Error() string             { return e.msg }
func (e *keyScopedMultiMatchError) Unwrap() error             { return ErrKeyScopedWriteMatchedMultipleRows }
func (e *keyScopedMultiMatchError) Terminal() bool            { return true }
func (e *keyScopedMultiMatchError) SourceTxSplitNote() string { return keyScopedMultiMatchSplitNote }

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
// It applies to EVERY UPDATE/DELETE, decided from the count alone: a keyed
// table's key matches one row, and a keyless table's whole-row write is
// addressed to one row ([WholeRowImage]), so a second match is never right.
// Zero rows stays tolerated (ADR-0010 resume idempotency).
//
// The key VALUES are named because they are what an operator must query to
// triage. They are post-redaction (the applier redacts before dispatch), so
// they are exactly what the target already holds.
//
// What the message says about the rollback is the part an apply loop may
// sharpen: an engine's dispatch cannot tell whether an earlier target
// transaction committed part of this source transaction, so the refusal says
// it may have, and a loop that knows it did appends that through
// [ir.NoteSourceTxSplit] (Bug 294).
func RefuseKeyScopedMultiMatch(engine, op, schema, table string, before ir.Row, matched int64) error {
	return sluicecode.Wrap(sluicecode.CodeCDCKeyMatchedMultipleRows, keyScopedMultiMatchHint, &keyScopedMultiMatchError{
		msg: fmt.Sprintf(
			"%s: applier: %s on %s: %s: the change's key (%s) matched %d target rows, so applying it would %s rows "+
				"the source never touched — the key is not unique on the target at this point in the stream: either a "+
				"DEFERRABLE primary key the source transaction moved key values through (a key shift or swap), or a "+
				"target table that does not hold these columns unique; %s",
			engine, op, qualified(schema, table), KeyScopedWriteMultiMatchMarker,
			describeRowKey(before), matched, verbFor(op), keyScopedMultiMatchRolledBack,
		),
	})
}

// WholeRowImage reports whether a before-image names every non-generated
// target column, so a WHERE built from it is the whole row. It is the first
// half of the keyless one-row address (GC-42): against a table with no key
// (no PRIMARY KEY and no NOT NULL unique index), a whole-row WHERE matches
// every IDENTICAL copy of the row, while the source changed exactly one of
// them — so the appliers address one row instead (Postgres by
// (tableoid, ctid), MySQL with LIMIT 1). Which copy is immaterial: they are
// identical in every column.
//
// Keylessness alone is NOT enough to pick one row, because the before-image's
// narrowing follows the SOURCE: a keyed source under REPLICA IDENTITY FULL (or
// a trigger source) sends only its key columns, and against an operator-made
// keyless target `DELETE … WHERE id = 2` matches rows that DIFFER in other
// columns — picking one would be a guess. Such a write keeps its every-match
// WHERE, and the multi-row check refuses a second match. An empty colTypes (an
// unknown target shape) cannot prove the image covers the row.
func WholeRowImage(before ir.Row, colTypes map[string]*ir.Column) bool {
	return len(colTypes) > 0 && len(MissingNonGeneratedColumns(before, colTypes)) == 0
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
