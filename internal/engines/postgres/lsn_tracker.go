// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// LSN tracking for slot-ack-after-apply correctness (Bug 15, ADR-0020).
//
// Postgres logical replication relies on the consumer telling the
// server, via [pglogrepl.SendStandbyStatusUpdate], "I'm caught up to
// LSN X" — at which point the server is free to recycle WAL up to X.
// The slot's confirmed_flush_lsn advances to whatever the latest
// standby update reported as flushed/applied.
//
// The pre-v0.5.0 reader confused two distinct LSNs and reported the
// same value for both: the highest LSN it had *parsed from the WAL
// stream* (which is what the slot uses for "are you alive?"
// keepalive correctness) and the highest LSN whose data was *durably
// applied to the target* (which is what the slot must use for
// confirmed_flush_lsn). The two diverge whenever events are buffered
// in an in-memory apply batch; if the streamer exits cleanly mid-
// batch (operator-issued [sluice sync stop], ctx cancel, etc.), the
// buffer is dropped, but the slot has already been told those events
// are durable. On warm-resume the slot streams from a position past
// the dropped events, and they're permanently lost.
//
// The fix is two LSNs and one rule: only ack to the slot the LSN
// whose data has been committed to the target. The reader still
// needs the parsed-LSN for keepalive timing decisions, but that's
// internal — what crosses the wire is `applied`.
//
// Single producer (the applier's commit path), single consumer (the
// reader's keepalive path). Atomic uint64 is sufficient and avoids
// a mutex on the hot path.
//
// The lsnTracker is opaque to the rest of the codebase — engines
// outside postgres don't need it, and the cross-engine pipeline
// wiring uses [any] plus structural interfaces to attach it without
// taking a hard dependency on this package.

package postgres

import (
	"sync/atomic"

	"github.com/jackc/pglogrepl"

	"github.com/orware/sluice/internal/ir"
)

// lsnTracker is a single-producer / single-consumer holder for the
// "highest LSN durably applied to target" value. Producer is the
// [ChangeApplier]'s commit path; consumer is the [CDCReader]'s
// keepalive routine.
//
// The zero value is usable: applied = 0/0 (no progress yet). The
// reader starts up reporting the slot's startLSN until the applier
// reports its first committed batch, after which the tracker takes
// over.
type lsnTracker struct {
	// applied is the highest pglogrepl.LSN whose data has been
	// committed to the target. Stored as a uint64 because
	// pglogrepl.LSN is itself a uint64, and the atomic package
	// works on the unsigned 64-bit form natively.
	applied atomic.Uint64
}

// newLSNTracker returns a fresh tracker with applied = 0.
func newLSNTracker() *lsnTracker {
	return &lsnTracker{}
}

// ReportApplied sets applied to lsn iff lsn is greater than the
// current value. A monotonic-advance CAS loop guards the otherwise-
// rare reordering window where two batches commit out of strict LSN
// order (the applier is single-goroutine today, but the CAS keeps
// the invariant honest under future concurrency without needing to
// re-prove it).
//
// Calling with the zero LSN is a no-op so an empty position token
// from the applier doesn't reset progress.
func (t *lsnTracker) ReportApplied(lsn pglogrepl.LSN) {
	if lsn == 0 {
		return
	}
	for {
		cur := t.applied.Load()
		if uint64(lsn) <= cur {
			return
		}
		if t.applied.CompareAndSwap(cur, uint64(lsn)) {
			return
		}
	}
}

// LoadApplied returns the highest applied LSN, or 0 when none has
// been reported yet.
func (t *lsnTracker) LoadApplied() pglogrepl.LSN {
	return pglogrepl.LSN(t.applied.Load())
}

// lsnFromPositionToken parses the LSN out of a [pgPos] token (the
// JSON blob the applier writes to the control table). Used by the
// applier's commit path to convert a freshly-committed change's
// position into a tracker update without making the applier care
// about pgPos's internal layout.
//
// Returns 0 with a nil error when the token is empty (the per-
// change Apply path on a no-position change, defensive). Returns 0
// with an error when the token is malformed; the applier logs and
// continues — losing one tracker update is not worth aborting a
// successful batch commit over.
func lsnFromPositionToken(token string) (pglogrepl.LSN, error) {
	if token == "" {
		return 0, nil
	}
	decoded, ok, err := decodePGPos(ir.Position{Engine: engineNamePostgres, Token: token})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return pglogrepl.ParseLSN(decoded.LSN)
}
