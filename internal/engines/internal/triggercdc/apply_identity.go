// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package triggercdc

import (
	"strconv"

	"sluicesync.dev/sluice/internal/ir"
)

// ChangeApplyID is the ADR-0190 identity of the change-log row id (phase 5):
// each captured change is its OWN transaction, `<engine>:<id>:<stamp>`,
// ordinal 1 (the stamp is explained below).
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
//
// # Why the id alone is not enough: the stamp
//
// A mark is only as good as the promise that its TxID never names two
// different changes, and the change-log id breaks that promise the moment its
// sequence is reset. That happens by operator action, not by accident: the
// SQLite / D1 watermark refusal once told the operator to set the sequence
// "to the watermark or higher", and exactly the watermark W re-issues W+1;
// on pgtrigger an `ALTER SEQUENCE … RESTART` or a teardown / re-setup is
// undetectable by the gap-free guard. A mark left by the change that held id
// W+1 before the reset would then match the NEW W+1 — same TxID, same
// ordinal — and on a keyless table, where the digest tripwire agrees on
// identical content and no key collision is loud, the new change would be
// skipped at exit 0.
//
// So the TxID also carries a stamp: a value the change log PERSISTS with the
// row (so every re-delivery of the same change yields it byte-identically and
// the crash suites still converge) and that a later capture cannot repeat.
// The engines supply it:
//
//   - pgtrigger: the capturing transaction's `pg_current_xact_id()` (the
//     `txid` column) — 64-bit and epoch-extended, so it never wraps and is
//     never reissued within a cluster, and a sequence reset does not touch it;
//   - SQLite / D1: the row's `captured_at` TEXT exactly as stored
//     (millisecond wall clock). A reset change is captured after an
//     operator's intervention, so it cannot share the millisecond of the
//     change it collides with.
//
// UNVERIFIED PREMISE (named, not tested): the SQLite / D1 stamp assumes the
// source's wall clock does not step back onto the exact millisecond of the
// marked change across the operator's reset; the pgtrigger stamp assumes the
// source is the same cluster (a restore from a physical backup rewinds the
// txid counter). Either failure needs a reset AND a coincidence on top of it.
//
// An empty stamp returns the zero ApplyID — no mark, no skip, the pre-ADR
// behaviour — rather than an identity that could repeat. (captured_at is
// NOT NULL, so that is only a defence against a hand-edited change log.)
func ChangeApplyID(engine string, id int64, stamp string) ir.ApplyID {
	if stamp == "" {
		return ir.ApplyID{}
	}
	return ir.ApplyID{TxID: engine + ":" + strconv.FormatInt(id, 10) + ":" + stamp, Seq: 1}
}
