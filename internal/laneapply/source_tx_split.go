// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import "sluicesync.dev/sluice/internal/ir"

// laneSourceTx is the coordinator's view of how much of the open source
// transaction is durable on the target, for the account a refusal gives of
// what its rollback undid (Bug 294, [ir.SourceTxSplitNoter]).
//
// The lane path always applies a source transaction across target
// transactions — its changes spread over lanes by key, and a keyless or
// key-changing change is applied alone as a barrier — but most of that is not
// KNOWLEDGE: when a lane batch fails, the other lanes' batches holding the
// same transaction's earlier changes may or may not have committed yet. The
// one point the coordinator knows is a barrier: it drains every lane to its
// predecessor, so every earlier change of the transaction is durable once the
// drain returns, and the barrier commits itself before the coordinator routes
// anything after it. That is all partCommitted records; anything else leaves
// the refusal's own "may" in place.
type laneSourceTx struct {
	// open is true between a TxBegin and its TxCommit. A marker-less stream
	// (the trigger sources) never opens one: each of its changes is its own
	// transaction, which nothing can split.
	open bool

	// rowsSeen is true once a row change of the open transaction has been
	// routed to a lane or applied as a barrier.
	rowsSeen bool

	// partCommitted is true once part of the open transaction is durable: a
	// barrier inside it drained the lanes after a row of it was routed, or
	// committed a statement of it itself.
	partCommitted bool
}

// noteTxSplit tells a refusal from a lane batch that its source transaction
// was split with part of it already durable — when that holds for whichever
// change in buf was refused. Only an UPDATE or a DELETE can be refused this
// way (GC-42), and the error does not say which one, so it requires every
// UPDATE and DELETE in buf to have been routed after part of its source
// transaction was durable; a batch mixing those with a change that was not
// keeps the refusal's own "may".
func noteTxSplit(buf []LaneChange, err error) error {
	writes := 0
	for _, e := range buf {
		switch e.Change.(type) {
		case ir.Update, ir.Delete:
			if !e.txPartCommitted {
				return err
			}
			writes++
		}
	}
	if writes == 0 {
		return err
	}
	return ir.NoteSourceTxSplit(err)
}
