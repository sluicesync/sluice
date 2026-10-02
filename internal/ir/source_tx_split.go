// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "errors"

// SourceTxSplitNoter is implemented by an apply refusal whose account of what
// the target holds depends on something only the apply LOOP knows: whether the
// refused change's source transaction was split across target transactions,
// with an earlier one already committed (Bug 294).
//
// The refusal is raised deep in an engine's dispatch, which sees one change
// and the target transaction it is in. Rolling that transaction back undoes
// the change and whatever else rode with it — but not the source
// transaction's earlier statements when a batch flush, a lane, a key change
// applied alone as a lane barrier, or `--apply-batch-size 1` committed them in
// an earlier target transaction. Those stay, and the table can then hold a
// state the source never had. So a refusal says "may" by default, and the
// loop that does know adds, through [NoteSourceTxSplit], that it did.
type SourceTxSplitNoter interface {
	// SourceTxSplitNote is the sentence that states, for this refusal, that
	// an earlier target transaction committed part of its source
	// transaction.
	SourceTxSplitNote() string
}

// NoteSourceTxSplit returns err with its [SourceTxSplitNoter]'s split note
// appended, when err's chain holds one and the note is not already there;
// every other error comes back unchanged. Call it only on positive knowledge
// that an earlier target transaction committed part of the refused change's
// source transaction: not knowing is what the refusal's own text says.
//
// It wraps rather than flipping a flag on the refusal because apply paths
// flatten their errors to strings on the way out (`commit: %w`), and a flag
// set after that would never reach the message. The wrapper unwraps to err,
// so errors.Is/As, the terminal verdict and the sluice code all see through
// it.
func NoteSourceTxSplit(err error) error {
	var n SourceTxSplitNoter
	if !errors.As(err, &n) {
		return err
	}
	var already *sourceTxSplitError
	if errors.As(err, &already) {
		return err
	}
	return &sourceTxSplitError{err: err, note: n.SourceTxSplitNote()}
}

// sourceTxSplitError is err with its refusal's split note appended.
type sourceTxSplitError struct {
	err  error
	note string
}

func (e *sourceTxSplitError) Error() string { return e.err.Error() + "; " + e.note }
func (e *sourceTxSplitError) Unwrap() error { return e.err }
