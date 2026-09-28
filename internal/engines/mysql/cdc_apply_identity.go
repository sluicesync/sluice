// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

// # ADR-0190 change identity on the binlog reader
//
// Every row change this reader emits inside a source transaction carries an
// [ir.ApplyID] (stamped by the reader's applymarks.Sequencer): the
// transaction's identity and the change's ordinal among the transaction's
// changes to the same table. An applier skips a re-delivered change on that
// identity alone (when a durable apply mark names it), so the one property
// that matters is that a re-delivery of the same transaction produces the
// same identities — a drift would skip work the target never received.
//
// The transaction identity, per position mode:
//
//   - GTID (MySQL `uuid:n`, MariaDB `domain-server-seq`): the transaction's
//     OWN GTID — the one [CDCReader.stageGTID] holds while the transaction is
//     read — never the row's Position, which is the pre-transaction executed
//     set. A GTID names one transaction on every server that holds it.
//   - file/pos: the source's @@server_uuid, the binlog file and the offset of
//     the transaction's opening event. A resume always restarts at a
//     transaction boundary (CheckpointOnlyAtTxBoundary), so the opening event
//     re-reads at the same offset, and the server_uuid is what the v0.137.2
//     instance pin already refuses a replaced server on. No server_uuid (the
//     lookup failed) means no identity: an offset alone could name a
//     different server's transaction.
//
// The Sequencer opens at the transaction's opening event (BEGIN, or MariaDB's
// transactional GTID group) and closes at its commit (XID / COMMIT), at every
// new group ([CDCReader.stageGTID], and the GTID event in file/pos mode, so an
// unterminated group such as an XA PREPARE cannot lend its identity to a
// later row), and at every stream (re)start.
//
// The ordinal counts ROWS as this reader emits them, per table. It is counted
// after the reader's own scope gate, which drops a table WHOLE (never some of
// a table's rows), and before every pipeline-side filter.
//
// The ADR-0190 §1 premise — a replica that re-encodes a GTID transaction into
// differently-split rows events emits the same rows in the same order, so the
// per-table ROW ordinal is the same on a delivery from either server — is
// pinned on real servers by TestCDCReader_ApplyIdentity_GTID_StableFromReplica
// (a replica booted with the minimum --binlog-row-event-max-size, so it
// provably splits what the primary wrote as one event). Were it ever false,
// the tripwire (applymarks.MismatchMarker) refuses rather than skips.

// transactionIdentity names the transaction whose opening event hdr is (see
// the file comment), or "" when this position mode cannot name it stably.
func (r *CDCReader) transactionIdentity(hdr *replication.EventHeader) string {
	switch r.posMode {
	case positionModeGTID:
		return r.pendingGTID
	case positionModeFilePos:
		if r.serverUUID == "" || hdr == nil {
			return ""
		}
		return fmt.Sprintf("filepos:%s:%s:%d", r.serverUUID, r.currentFile, hdr.LogPos)
	}
	return ""
}

// StampsApplyIdentity implements [ir.ApplyIdentityProvider].
func (r *CDCReader) StampsApplyIdentity() bool { return true }

var _ ir.ApplyIdentityProvider = (*CDCReader)(nil)
