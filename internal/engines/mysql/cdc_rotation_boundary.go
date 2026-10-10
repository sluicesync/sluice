// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"log/slog"
	"math"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

// # Rotation boundaries (GC-43 (r))
//
// The consumer persists a position only at a source-transaction boundary — a
// TxCommit (or a Truncate). On a busy source that is every InnoDB
// transaction, including the ones whose rows are all out of scope: their
// BEGIN and XID still reach the applier as a row-less pair, and it persists
// the commit (since Bug 300 a run of such pairs persists once, at its last —
// the pipeline's live stage bounds a run at 1,000, so a source busy only
// elsewhere still moves the position). But three kinds of binlog traffic
// persist nothing — a ROTATE, a heartbeat, and a standalone GTID group (DDL,
// CREATE USER / GRANT, OPTIMIZE: implicit commits that carry no XID). So a
// stream that sees only
// those never moves its persisted position, however many binlog files go
// by. Measured on mysql:8.0 with binlog_expire_logs_seconds=60 and on
// mariadb:11.4 (2026-10-01): an idle stream sat at mysql-bin.000003 while
// the source rotated to .000005 and purged .000003; the next start failed
// the resume check as "purged", and the ADR-0093 auto-resnapshot DROPPED
// the target tables and re-copied them — though nothing had been missed. In
// GTID mode the same happens when the last GTIDs before a purge are
// standalone groups (gtid_purged then exceeds the persisted set), and on
// MariaDB an idle stream additionally never persists its re-anchored lineage
// (mariadb_lineage.go), so after the anchor's file is purged the next start
// can only resume under UNVERIFIED-INSTANCE-IDENTITY.
//
// A binlog file can only be purged once the server has rotated past it, and
// the ROTATE that ends a file reaches a connected stream before the next
// file's first event. So the rotation is a boundary every stream sees before
// it can need one: when no transaction is open, the pump emits a
// boundary-only transaction there (TxBegin then TxCommit, no rows), and the
// consumer persists it like any other.
//
// # Why persisting the rotation loses nothing
//
// The ROTATE is the last event of file N, and the dump stream is ordered, so
// every event of N has been dispatched ahead of it. With no transaction open
// and no GTID staged, each of those groups has either emitted its TxCommit or
// been folded as a standalone group (item 132). That is not yet everything a
// group owes: an in-scope DDL's schema boundary is emitted lazily, at each
// table's next row, and a restart forgets it is owed (GC-43 (r) F1). So the
// boundary first settles every owed table, inside its own transaction, or is
// skipped (cdc_owed_schema_boundary.go). The boundary then names:
//
//   - file/pos: (N+1, the rotate's own Position — the first event of N+1).
//     A resume reads every event of N+1 onward, which is everything not yet
//     dispatched.
//   - GTID (MySQL and MariaDB): the running executed set, unchanged — every
//     group of N is in it, and a resume from it re-sends exactly the groups
//     after it.
//   - MariaDB, either mode: also the lineage the arm above just re-anchored
//     at (N+1, 4), so the anchor the target holds follows the stream.
//
// The guards are what make "no transaction open" true rather than assumed,
// and each skips the boundary rather than guessing — a skipped boundary is
// the pre-fix behaviour, never a loss:
//
//   - inSourceTx: a transaction's BEGIN (or non-standalone MariaDB GTID)
//     was dispatched and its commit was not.
//   - inXA: between XA START and XA END.
//   - a staged GTID: a group whose terminator sluice does not model (MySQL's
//     XA_PREPARE_LOG_EVENT) folds at the NEXT group (stageGTID), so until
//     then the executed set omits it. The boundary would be sound with the
//     set as it stands — it resumes one group early, replaying a group with
//     no in-scope rows — but folding it here would rest on "a group never
//     spans binlog files", a fact about the server this code does not check,
//     and the skip costs only that rare shape's residual. Named, not folded.
//
// An ARTIFICIAL rotate is not a boundary (the dispatch arm returns before
// calling here). The dump thread sends one when a connection opens (and
// go-mysql's internal re-dial opens one), naming the position the connection
// was asked to start from — which after a re-dial is wherever go-mysql had
// read to, mid-transaction or not. It carries a zero header timestamp and
// the LOG_EVENT_ARTIFICIAL_F flag; a rotate written into the binlog carries
// neither. Either mark excludes it. It does not move the MariaDB anchor
// either: it names the file the stream is already in, so re-anchoring there
// bought no retention and traded the capture door's mid-file anchor for the
// file's start — on a server's first binlog, (file, 4, "") with the empty
// set omitted from the token, an anchor every fresh instance reproduces.
//
// # Shape of the boundary
//
// Mirrors the Postgres keepalive boundary (postgres/cdc_keepalive_boundary.go,
// GC-41 (j)). Both events carry the same position: for a transaction with no
// rows, the pre- and post-transaction points coincide. The TxBegin opens no
// ADR-0190 identity — there is no source transaction to name — so no apply
// mark is ever written for it. CommitTime is zero: no source transaction
// committed there, and a zero time is what the sync-lag tracker and the
// --apply-delay hold both read as "no timestamp" (the rotate's own header
// time would be a fabricated commit). One boundary per rotation, so the cost
// is one position write per binlog file.
//
// # Residuals, stated
//
//   - A server RESTART ends its file with a STOP_EVENT, not a ROTATE, so no
//     boundary is written at that crossing (whatever the dump thread sends
//     for it is not a rotate read from the binlog — UNVERIFIED which event
//     that is; if it is artificial, the exclusion above applies). An idle
//     stream whose source restarts and purges the pre-restart file before
//     the next rotation still resumes from the purged file.
//   - The staged-GTID skip above: an XA PREPARE as the last group of a file,
//     on an otherwise idle GTID-mode MySQL source.
//   - VStream is a separate reader and does not pass through here (see the
//     GC-43 (r) backlog entry for its DDL-only tail).

// emitRotationBoundary emits a boundary-only transaction at the rotation e
// (read from the binlog, not artificial) describes, when the file comment's
// guards allow one.
func (r *CDCReader) emitRotationBoundary(ctx context.Context, e *replication.RotateEvent, out chan<- ir.Change) error {
	if r.inSourceTx || r.inXA || r.pendingGTID != "" {
		return nil
	}
	if e.Position < 4 || e.Position > math.MaxUint32 {
		// Every binlog file's first event is at offset 4; anything else is a
		// rotate this code does not understand, and a boundary built from it
		// would persist a position no resume can use.
		slog.WarnContext(ctx, "mysql: cdc: binlog rotate names an unexpected start offset; not persisting a "+
			"position at this rotation",
			slog.String("file", string(e.NextLogName)), slog.Uint64("position", e.Position))
		return nil
	}
	// A DDL's boundary is emitted lazily, at the table's next row, and a
	// restart forgets that it is owed — so settle every owed one before
	// persisting past the DDL, or skip (cdc_owed_schema_boundary.go).
	owed, ok := r.owedSchemaBoundaryTables(ctx)
	if !ok {
		return nil
	}
	pos, err := r.positionAt(string(e.NextLogName), uint32(e.Position))
	if err != nil {
		return err
	}
	if err := send(ctx, out, ir.TxBegin{Position: pos}); err != nil {
		return err
	}
	if err := r.settleSchemaBoundaries(ctx, owed, out); err != nil {
		return err
	}
	return send(ctx, out, ir.TxCommit{Position: pos})
}

// isArtificialRotate reports whether a ROTATE was synthesized by the dump
// thread when a connection opened, rather than read from the binlog — see the
// file comment.
func isArtificialRotate(hdr *replication.EventHeader) bool {
	return hdr == nil || hdr.Timestamp == 0 || hdr.Flags&replication.LOG_EVENT_ARTIFICIAL_F != 0
}
