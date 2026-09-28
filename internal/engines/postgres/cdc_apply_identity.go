// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// # ADR-0190 change identity on the pgoutput reader
//
// Every row change this reader emits inside a source transaction carries an
// [ir.ApplyID] (stamped by the reader's applymarks.Sequencer): the
// transaction's identity and the change's ordinal among the transaction's
// changes to the same table. An applier skips a re-delivered change on that
// identity alone (when a durable apply mark names it), so the property that
// matters is that a re-delivery of the same transaction produces the same
// identities.
//
// The transaction identity is its commit LSN (BeginMessage.FinalLSN — the
// same value every row of the transaction already carries as its Position),
// qualified by the pinned system id and timeline. Logical decoding re-emits a
// transaction's changes in WAL order on every delivery, an LSN names one
// commit record within one timeline, and the (system id, timeline) pin
// already refuses a resume against a different history (ADR-0051) — so the
// identity is unique and stable by construction. No system id (never observed
// in practice; IDENTIFY_SYSTEM always supplies one) means no identity.
//
// The ordinal counts the changes the PUBLICATION delivers for the table. A
// publication row filter (`--where` pushed down on PG 15+) that changes
// between runs changes which changes are delivered and so their ordinals; the
// apply marks carry the stream's row-filter scope and refuse
// (applymarks.MismatchMarker) rather than trust a mark written under another.
// Rows inside a pgoutput streamed chunk (never enabled by sluice) carry no
// identity.

// openTransaction starts stamping the transaction whose commit LSN is
// commitLSN (a BeginMessage) and returns the position its TxBegin and every
// row carry — the pre-transaction point, commitLSN itself.
func (r *CDCReader) openTransaction(commitLSN pglogrepl.LSN) (ir.Position, error) {
	r.applyIDs.Begin(r.transactionIdentity(commitLSN))
	return r.positionAt(commitLSN)
}

// closeTransaction ends the transaction's stamping (a CommitMessage) and
// returns its post-commit position, endLSN.
func (r *CDCReader) closeTransaction(endLSN pglogrepl.LSN) (ir.Position, error) {
	r.applyIDs.End()
	return r.positionAt(endLSN)
}

// transactionIdentity names the transaction whose commit LSN is commitLSN, or
// "" when the reader has no system identity to qualify it with.
func (r *CDCReader) transactionIdentity(commitLSN pglogrepl.LSN) string {
	if r.systemID == "" {
		return ""
	}
	return fmt.Sprintf("pg:%s:%d:%s", r.systemID, r.timeline, commitLSN)
}

// StampsApplyIdentity implements [ir.ApplyIdentityProvider].
func (r *CDCReader) StampsApplyIdentity() bool { return true }

var _ ir.ApplyIdentityProvider = (*CDCReader)(nil)
