// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applymarks

import "sluicesync.dev/sluice/internal/ir"

// Sequencer is a CDC reader's per-transaction [ir.ApplyID] stamper: it holds
// the identity of the source transaction the reader is inside and numbers
// that transaction's row changes per table, in the order the reader emits
// them. A reader opens a transaction with [Sequencer.Begin] at its opening
// event, stamps each row change with [Sequencer.Next], and closes it with
// [Sequencer.End] at its commit (and wherever a new transaction group
// provably starts). The zero value is closed. Not safe for concurrent use —
// it belongs to the reader's single pump goroutine.
type Sequencer struct {
	txID string
	seq  map[string]uint64
}

// Begin opens transaction txID. An empty txID — a source position mode that
// cannot name the transaction stably — stamps nothing until the next Begin.
func (s *Sequencer) Begin(txID string) {
	s.txID = txID
	s.seq = nil
}

// End closes the transaction: rows after it carry no identity until the next
// Begin.
func (s *Sequencer) End() {
	s.txID = ""
	s.seq = nil
}

// Next returns the identity of the transaction's next row change to table,
// or the zero identity outside a transaction.
func (s *Sequencer) Next(table string) ir.ApplyID {
	if s.txID == "" {
		return ir.ApplyID{}
	}
	if s.seq == nil {
		s.seq = map[string]uint64{}
	}
	s.seq[table]++
	return ir.ApplyID{TxID: s.txID, Seq: s.seq[table]}
}
