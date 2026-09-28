// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package triggercdc

import (
	"strconv"

	"sluicesync.dev/sluice/internal/ir"
)

// ChangeApplyID is the ADR-0190 identity of the change-log row id (phase 5):
// each captured change is its OWN transaction, `<engine>:<id>`, ordinal 1.
//
// # What the change log gives, and why this shape
//
// The change log gives each captured row change a persisted, unique,
// monotone id, and the reader re-delivers exactly those rows with exactly
// those ids on every resume, so the identity is stable by construction — no
// premise to verify. It gives NO source transaction boundaries the reader
// can use: concurrent source transactions interleave in id order (pgtrigger
// records the txid but a transaction's rows need not be contiguous), and the
// SQLite / D1 triggers record none. So the reader emits no TxBegin/TxCommit
// and every change carries its own position.
//
// ADR-0190 §1 proposed an empty TxID with the change-log id as a
// cross-change ordinal ("skip if the mark's id ≥ this id"). That is the
// cross-transaction rule amendment A rejected: it is sound only while no mark
// is stale, and it needs its own staleness guard. A per-change identity
// needs nothing new, because every apply path already keeps a marked change's
// predecessors out of any replay:
//
//   - the serial paths persist the position WITH the data at every flush on a
//     marker-less stream, so an applied change is never re-delivered (the
//     marks are closed at that same position write and never even written —
//     appliershared commitBatch / applyOneImpl);
//   - the lane path fences before every marked change (amendment A: drain,
//     checkpoint at its predecessor), and barriers the keyless and
//     primary-key-changing ones the same way, so only the change the
//     checkpoint stopped before can be re-delivered with a mark; each
//     marker-less boundary closes the change before it, deleting its marks.
//
// The engine prefix keeps one source's ids from ever naming another's.
func ChangeApplyID(engine string, id int64) ir.ApplyID {
	return ir.ApplyID{TxID: engine + ":" + strconv.FormatInt(id, 10), Seq: 1}
}
