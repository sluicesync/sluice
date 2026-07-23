// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Native-MySQL concurrent cold-copy RESUMABILITY (ADR-0111).
//
// cdc_snapshot_concurrent.go builds the consistent N-snapshot cold-copy:
// FTWRL → ONE binlog position P (the CDC anchor) → N pinned CONSISTENT
// SNAPSHOT readers → UNLOCK. That copy was TERMINAL on a source-read drop:
// a connection lost mid-copy (the backpressure-EOF a stalled PlanetScale
// target induces during a storage grow — ADR-0109/0110) aborted the whole
// errgroup → auto-resnapshot → restart from row 0 (the v0.99.103 PS-320-v13
// finding). InnoDB cannot recreate a dropped connection's CONSISTENT
// SNAPSHOT read-view, so the migrate path's fresh-reader resume
// (copyTableWithSourceReadRetry, ADR-0109) is unsafe here — a fresh reader
// reads at a DIFFERENT position, silently mixing snapshot points.
//
// This file gives the native concurrent reader the ADR-0072 (VStream)
// resilience analog, accounting for the non-re-observable snapshot:
//
//  1. PER-TABLE PK-CURSOR TRACKING. concurrentBinlogRows tracks each table's
//     last-handed-off ordered-PK tuple + a "complete" marker IN MEMORY — the
//     source of truth the in-process recovery resumes from. ADR-0111 §1's
//     control-table PERSISTENCE of these cursors (and thus a PROCESS-restart
//     resume of an interrupted native cold-copy) is DEFERRED: persisting a
//     mid-cold-copy position on the native path without also (a) implementing
//     native SnapshotStreamResumer routing and (b) coupling a durable-write
//     watermark to the cursor (the concurrent copy path deliberately wires NO
//     durable-progress reporter — copy_concurrent_tables.go) would let a
//     process crash leave the persisted cursor AHEAD of the durably-written
//     rows → a silent gap on restart. That is a separable, larger change; the
//     in-process source-read-drop recovery (the ADR's core WIN) needs only the
//     in-memory cursor. See the ADR Consequences for the deferral.
//
//  2. RE-SNAPSHOT-FROM-CURSOR RECOVERY. On a CLASSIFIED source-read drop
//     during a per-table read, the reader re-establishes a FRESH consistent
//     snapshot (new FTWRL → new position P′ → N fresh pinned conns), then:
//     SKIPS tables already complete; RESUMES each incomplete KEYED table
//     from its cursor (WHERE (pk) > lastpk) read at P′; re-reads each
//     incomplete KEYLESS table from the START at P′. A keyless table has no
//     safe mid-table cursor, so the re-read re-emits the rows already copied
//     before the drop — loss-free, possibly with duplicate rows, which is
//     exactly the existing keyless at-least-once cold-copy contract (Bug 143;
//     the recovery WARN names it). The reader cannot dedup a keyless target
//     (that would need a target TRUNCATE, which is target knowledge a source
//     reader must not hold), so dup-freeness for keyless tables is the one
//     ADR-0111 §4 refinement deferred to a future pipeline hook — see the
//     ADR Consequences. Bounded (a wall-clock budget), loud on exhaustion /
//     binlog purge, then the existing full restart-from-scratch takes over.
//
//  3. THE VALUE-FIDELITY INVARIANT (the #1 correctness requirement). The
//     CDC anchor stays at the ORIGINAL, earliest P across the recovery — the
//     reader re-snapshots its INNER read connections but NEVER mutates the
//     SnapshotStream's Position field (set once at open in
//     cdc_snapshot_concurrent.go, read by the streamer only after the copy
//     errgroup joins). Idempotent CDC replay from P then converges keyed
//     tables exactly: a row that changed between P and P′ re-applies
//     idempotently (UPSERT / delete-by-PK), and keyless tables stay
//     at-least-once. Anchoring at P′ would SKIP P→P′ changes on
//     already-completed tables — silent loss — so the earliest-anchor is
//     load-bearing. Enforced by [verifyCDCAnchorUnchanged] (runtime guard)
//     and pinned by a dedicated unit test.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// nativeResnapshotMaxWall bounds the re-snapshot-from-cursor recovery: a
// classified source-read drop rides this wall-clock window (mirroring
// ADR-0109's coldCopySourceReadMaxWall) before recovery surfaces a LOUD
// terminal error and the existing full restart-from-scratch takes over.
var nativeResnapshotMaxWall = 30 * time.Minute

// nativeResnapshotBackoffBase / nativeResnapshotBackoffCap are the
// per-attempt recovery backoff (exponential doubling, capped) — the same
// shape as coldCopySourceReadBackoff.
var (
	nativeResnapshotBackoffBase = 100 * time.Millisecond
	nativeResnapshotBackoffCap  = 30 * time.Second
)

// nativeResnapshotBackoff returns the per-attempt recovery backoff. attempt
// is 1-based (attempt 1 is the wait before the first re-snapshot).
func nativeResnapshotBackoff(attempt int) time.Duration {
	b := nativeResnapshotBackoffBase
	for i := 1; i < attempt; i++ {
		b *= 2
		if b > nativeResnapshotBackoffCap {
			return nativeResnapshotBackoffCap
		}
	}
	return b
}

// resnapshotFn re-establishes a FRESH consistent N-snapshot at a new
// position P′ and returns the new pinned connections + pool + position. It is
// the recovery counterpart of the initial open in
// openBinlogSnapshotStreamConcurrent (same FTWRL → N CONSISTENT SNAPSHOT →
// record P′ → UNLOCK sequence), supplied to the reader by the opener so the
// reader stays free of the engine-open plumbing (DSN parsing, timeouts). The
// reader uses the new conns to continue the copy; it NEVER touches the CDC
// anchor with P′. p2File/p2Pos are surfaced only for the binlog-purge check.
type resnapshotFn func(ctx context.Context) (conns []*sql.Conn, db *sql.DB, p2File string, p2Pos uint32, err error)

// tableCursor is the IN-MEMORY per-table resume state the reader maintains
// during the copy — the source of truth the in-process re-snapshot recovery
// reads. ADR-0111 scope note: these cursors are NOT persisted to the control
// table (see the file-header "deferred" note + the ADR Consequences); they
// live only for the in-process source-read-drop recovery.
type tableCursor struct {
	keyed    bool
	complete bool
	lastPK   []any // last-handed-off ordered-PK tuple; nil until the first row
}

// noteRowEmitted records that table handed off a row whose ordered-PK tuple is
// pk (nil for a keyless table), advancing the in-memory cursor. Called AFTER
// the row is on the out channel (the exactly-once ordering — see
// readKeyedPaged). lastPK reuses its backing array across rows to avoid
// per-row allocation; cursorFor copies it for the recovery read.
func (r *concurrentBinlogRows) noteRowEmitted(table string, keyed bool, pk []any) {
	r.cursMu.Lock()
	c := r.cursors[table]
	if c == nil {
		c = &tableCursor{keyed: keyed}
		r.cursors[table] = c
	}
	c.keyed = keyed
	if keyed && pk != nil {
		if cap(c.lastPK) >= len(pk) {
			c.lastPK = c.lastPK[:len(pk)]
			copy(c.lastPK, pk)
		} else {
			c.lastPK = append([]any(nil), pk...)
		}
	}
	r.cursMu.Unlock()
}

// noteTableComplete marks table fully copied (EOF). A complete table is
// SKIPPED on re-snapshot recovery.
func (r *concurrentBinlogRows) noteTableComplete(table string, keyed bool) {
	r.cursMu.Lock()
	c := r.cursors[table]
	if c == nil {
		c = &tableCursor{keyed: keyed}
		r.cursors[table] = c
	}
	c.complete = true
	r.cursMu.Unlock()
}

// cursorFor returns a COPY of table's current resume cursor (keyed, complete,
// lastPK), or the zero value when none recorded yet. Caller does not hold a
// lock (this takes cursMu).
func (r *concurrentBinlogRows) cursorFor(table string) tableCursor {
	r.cursMu.Lock()
	defer r.cursMu.Unlock()
	c := r.cursors[table]
	if c == nil {
		return tableCursor{}
	}
	return tableCursor{keyed: c.keyed, complete: c.complete, lastPK: append([]any(nil), c.lastPK...)}
}

// verifyCDCAnchorUnchanged is the runtime guard for the ADR-0111
// value-fidelity invariant: a re-snapshot recovery must leave the CDC anchor
// (Mode/File/Pos/GTIDSet/ServerUUID) at the ORIGINAL P. It returns a LOUD
// error if the two binlogPos values ever diverge — turning a would-be
// silent-loss anchor advance into a refusal.
func verifyCDCAnchorUnchanged(original, current binlogPos) error {
	o, c := original, current
	if o.Mode != c.Mode || o.File != c.File || o.Pos != c.Pos ||
		o.GTIDSet != c.GTIDSet || o.ServerUUID != c.ServerUUID {
		return fmt.Errorf(
			"mysql: native concurrent cold-copy: INTERNAL INVARIANT VIOLATION — the CDC anchor changed "+
				"across a re-snapshot recovery (was %s/%s:%d/%s, now %s/%s:%d/%s); refusing because anchoring CDC at "+
				"the later position would skip changes on already-completed tables (silent loss, ADR-0111 §3)",
			o.Mode, o.File, o.Pos, o.GTIDSet, c.Mode, c.File, c.Pos, c.GTIDSet,
		)
	}
	return nil
}

// decodeAnchorTokenOrZero decodes a MySQL anchor token into a binlogPos for
// the loud invariant-mismatch error, returning the zero value if it can't
// decode (the error message still names the mismatch).
func decodeAnchorTokenOrZero(token string) binlogPos {
	bp, _, err := decodeBinlogPos(ir.Position{Engine: engineNameMySQL, Token: token})
	if err != nil {
		return binlogPos{}
	}
	return bp
}

// errBinlogPurgedDuringResnapshot signals that the source binlog at the
// original anchor P is no longer available (retention advanced past P during
// the copy). The caller maps it to the existing full restart-from-scratch
// (ADR-0111 §5): resuming would still anchor CDC at P, but P is gone, so the
// only safe path is a fresh re-snapshot at P′ from row 0.
var errBinlogPurgedDuringResnapshot = errors.New(
	"mysql: native concurrent cold-copy: source binlog at the original CDC anchor was purged during the copy " +
		"(binlog_expire_logs_seconds too short for the copy duration); falling back to full re-snapshot from scratch",
)

// readResumable streams table from the reader's current snapshot connection,
// transparently riding out classified source-read drops via
// re-snapshot-from-cursor recovery (ADR-0111). It produces ONE continuous row
// channel: the consumer sees a normal drain to EOF even when the underlying
// snapshot was re-established mid-table. readerIdx selects the pinned
// connection (work-stealing path); a negative readerIdx uses the table's
// statically-assigned owner (static-partition path).
//
// lowerPK/upperPK bound the read to a half-open PK range (lowerPK, upperPK]
// (ADR-0119, roadmap 21b): nil/nil is the WHOLE table (the tier-(a) and serial
// callers), a non-nil bound is an intra-table chunk. cursorKey identifies the
// per-work-item in-memory resume cursor — table.Name for a whole-table item,
// "table#chunkIndex" for a chunk — so concurrent chunks of one table never
// alias on the shared cursor map.
func (r *concurrentBinlogRows) readResumable(ctx context.Context, table *ir.Table, lowerPK, upperPK []any, cursorKey string, readerIdx int) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("mysql: native concurrent cold-copy resume: table is nil")
	}
	out := make(chan ir.Row, rowChanBuffer)
	go r.streamResumable(ctx, table, lowerPK, upperPK, cursorKey, readerIdx, out)
	return out, nil
}

// streamResumable is readResumable's goroutine. It reads the table from the
// current snapshot — keyed tables via cursor-paginated batches
// (ReadRowsBatch, so a re-snapshot can resume WHERE (pk) > lastpk), keyless
// tables via a single full scan — forwarding rows (tracking the cursor +
// cadence checkpoint). On a classified source-read drop it runs the bounded
// recovery loop, then re-reads the SAME table from its cursor. It owns out
// (closes it on exit) and the reader's sticky Err.
func (r *concurrentBinlogRows) streamResumable(ctx context.Context, table *ir.Table, lowerPK, upperPK []any, cursorKey string, readerIdx int, out chan<- ir.Row) {
	defer close(out)

	keyed := tableHasOrderablePK(table)

	for {
		// A previously-recorded complete marker (e.g. the work item finished,
		// then a sibling's drop triggered a re-snapshot) means nothing left to
		// read — return cleanly. Defensive: the consumer reads each work item
		// once, so a completed item isn't re-opened in normal flow, but the
		// recovery's "skip completed" is enforced here too. Keyed on cursorKey
		// so a per-chunk complete marker is distinct from its siblings.
		if r.cursorFor(cursorKey).complete {
			return
		}

		// Capture the recovery generation this read attempt runs against,
		// BEFORE the read. If the read drops and a PEER re-snapshotted in the
		// meantime, recoveryGen will have advanced past observedGen and
		// recoverFromDrop coalesces (this lane resumes on the swapped conns
		// instead of doing its own redundant FTWRL re-snapshot).
		observedGen := r.currentRecoveryGen()

		err := r.readOnce(ctx, table, lowerPK, upperPK, cursorKey, readerIdx, keyed, out)
		if err == nil {
			r.noteTableComplete(cursorKey, keyed)
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			r.setErr(ctx.Err())
			return
		}
		if !isRetriableReadErr(err) {
			// A real decode/query fault — terminal, exactly as the
			// non-resumable reader. Surface it on the sticky Err.
			r.setErr(err)
			return
		}

		// Classified source-read drop → bounded re-snapshot-from-cursor
		// recovery. On success the loop re-reads this table from its cursor on
		// the fresh snapshot; on terminal failure (budget exhausted / binlog
		// purged) the error is surfaced and the whole copy aborts loudly so
		// the existing full restart-from-scratch takes over.
		if rerr := r.recoverFromDrop(ctx, err, observedGen); rerr != nil {
			r.setErr(rerr)
			return
		}
		// loop: re-read the table from its (advanced) cursor on the new conns
	}
}

// readOnce reads the table once from the CURRENT snapshot connection,
// forwarding rows and tracking the in-memory per-table cursor. For a keyed
// table it pages with ReadRowsBatch from the table's cursor (so a re-snapshot
// continues WHERE (pk) > lastpk); for a keyless table it does one full scan.
// It returns nil on a clean EOF (table fully read), or the underlying read
// error (classified) on a drop. It does NOT mark the table complete (the
// caller does) and does NOT close out.
func (r *concurrentBinlogRows) readOnce(ctx context.Context, table *ir.Table, lowerPK, upperPK []any, cursorKey string, readerIdx int, keyed bool, out chan<- ir.Row) error {
	if keyed {
		return r.readKeyedPaged(ctx, table, lowerPK, upperPK, cursorKey, readerIdx, out)
	}
	// Keyless tables are NEVER chunked (no orderable key to tile on, ADR-0119
	// Decision 1), so a keyless read is always whole — cursorKey is the table
	// name and the PK-range bounds are nil.
	return r.readKeyless(ctx, table, cursorKey, readerIdx, out)
}

// nativeResumeBatchSize is the page size for the keyed cursor-paginated read.
// Large enough that paging overhead is negligible on the happy path, small
// enough that a drop loses at most one page of progress before the persisted
// cursor catches up. Var for test-shrink.
var nativeResumeBatchSize = 50_000

// concurrentDropInjector is a TEST-ONLY seam (nil in production — a single nil
// check on the hot path) that simulates a CLASSIFIED source-read drop after a
// chosen number of rows of a chosen table, WITHOUT killing a real connection.
// The integration test sets it to inject one mid-table drop so the genuine
// re-snapshot-from-cursor recovery (real FTWRL on the container, real resume
// WHERE pk > cursor) runs end-to-end. It returns a non-nil CLASSIFIED error
// (isRetriableReadErr true) to trigger recovery, or nil to let the read
// continue. tableName + rowsHandedOff let it target one table at one offset.
var concurrentDropInjector func(tableName string, rowsHandedOff int) error

// readKeyedPaged pages the keyed table from its per-work-item cursor using
// ReadRowsBatchBounded (WHERE (pk) > lastpk AND (pk) <= upperPK ORDER BY pk
// LIMIT N) so a re-snapshot resumes exactly where it left off, clipped to the
// chunk's upper bound. A short page (< limit) is the clean EOF.
//
// CHUNK BOUNDS (ADR-0119). The first page's lower cursor is `after`: the
// per-work-item cursor's lastPK once a row has been handed off, else lowerPK
// (the chunk's exclusive lower bound; nil for chunk 0 / a whole-table read).
// upperPK is the chunk's INCLUSIVE upper bound, pushed into SQL in the column's
// native collation — nil for the last chunk / a whole table, which makes the
// query byte-identical to the unbounded whole-table form (the upperPK==nil case
// of ReadRowsBatchBounded == today's ReadRowsBatch). So the M chunks of a table
// tile its rows with no gap and no overlap (the Bug-74 collation contract).
//
// EXACTLY-ONCE on recovery (the value-fidelity core). The cursor is advanced
// (noteRowEmitted, keyed on cursorKey) ONLY AFTER the row is successfully handed
// to out — so the cursor never names a row the writer was not given. Because
// recovery keeps out OPEN and continues (it never closes the channel mid-read),
// the writer drains EVERY row placed on out to durability; thus every row with
// pk ≤ cursor is written, and the resume WHERE (pk) > cursor neither skips an
// in-flight row (no gap) nor re-reads a handed-off one (no dup). Ordering the
// advance AFTER the send is what makes this hold even if the send loses the
// ctx race (the row is then NOT handed off and the cursor is NOT advanced).
func (r *concurrentBinlogRows) readKeyedPaged(ctx context.Context, table *ir.Table, lowerPK, upperPK []any, cursorKey string, readerIdx int, out chan<- ir.Row) error {
	pkNames := primaryKeyColumnNames(table)
	handedOff := 0
	for {
		// Resume from the cursor's lastPK once a row has been handed off; before
		// the first row, start at the chunk's exclusive lower bound (nil for
		// chunk 0 / whole-table → no lower predicate).
		after := r.cursorFor(cursorKey).lastPK
		if len(after) == 0 {
			after = lowerPK
		}
		rr := r.pickReader(readerIdx)
		if rr == nil {
			return errors.New("mysql: native concurrent cold-copy resume: no snapshot reader available")
		}
		ch, err := rr.ReadRowsBatchBounded(ctx, table, after, upperPK, nativeResumeBatchSize)
		if err != nil {
			return err
		}
		n := 0
		for row := range ch {
			pk := extractPKTuple(row, pkNames)
			select {
			case out <- row:
			case <-ctx.Done():
				return ctx.Err()
			}
			// Advance the cursor AFTER the successful hand-off (see the
			// exactly-once argument above).
			r.noteRowEmitted(cursorKey, true, pk)
			n++
			handedOff++
			if inj := concurrentDropInjector; inj != nil {
				if derr := inj(table.Name, handedOff); derr != nil {
					return derr // TEST-ONLY: simulate a classified mid-table drop
				}
			}
		}
		if rerr := rr.Err(); rerr != nil {
			return rerr
		}
		if n < nativeResumeBatchSize {
			// Short page → table fully drained.
			return nil
		}
	}
}

// readKeyless reads the keyless table in one full scan (no safe mid-table
// cursor). A drop here is recovered by re-reading from the START on the fresh
// snapshot; without a usable key the re-read re-emits rows already copied
// before the drop, so a keyless table degrades to AT-LEAST-ONCE on recovery
// (loss-free, possible duplicates — the existing keyless cold-copy contract,
// Bug 143; the recovery WARN names it). It tracks no LastPK but still counts
// rows toward the cursor's keyless marker so the complete marker on EOF lands.
func (r *concurrentBinlogRows) readKeyless(ctx context.Context, table *ir.Table, cursorKey string, readerIdx int, out chan<- ir.Row) error {
	rr := r.pickReader(readerIdx)
	if rr == nil {
		return errors.New("mysql: native concurrent cold-copy resume: no snapshot reader available")
	}
	ch, err := rr.ReadRows(ctx, table)
	if err != nil {
		return err
	}
	handedOff := 0
	for row := range ch {
		select {
		case out <- row:
		case <-ctx.Done():
			return ctx.Err()
		}
		// Keyless tables record no LastPK; the note keeps the keyless marker
		// (recorded after the hand-off, for consistency with the keyed path).
		// cursorKey == table.Name here (keyless tables are never chunked).
		r.noteRowEmitted(cursorKey, false, nil)
		handedOff++
		if inj := concurrentDropInjector; inj != nil {
			if derr := inj(table.Name, handedOff); derr != nil {
				return derr // TEST-ONLY: simulate a classified mid-table drop
			}
		}
	}
	return rr.Err()
}

// pickReader returns the inner RowReader to read on: the connection at
// readerIdx (work-stealing path) when readerIdx >= 0, else the table-agnostic
// reader 0 (static path resolves the owner via byTable, but after a
// re-snapshot every reader sees the same fresh cut, so any reader is correct;
// reader 0 is the simplest stable choice and matches the work-stealing
// invariant of one query per connection because the static consumer holds a
// distinct readerIdx per group via ReadRowsOn). Takes connMu (read) so it
// never observes a half-swapped connection set.
func (r *concurrentBinlogRows) pickReader(readerIdx int) *RowReader {
	r.connMu.RLock()
	defer r.connMu.RUnlock()
	if readerIdx >= 0 && readerIdx < len(r.readers) {
		return r.readers[readerIdx]
	}
	if len(r.readers) > 0 {
		return r.readers[0]
	}
	return nil
}

// recoverFromDrop runs the bounded re-snapshot-from-cursor recovery after a
// classified source-read drop (ADR-0111 §2). It coalesces with peer pipelines
// under r.recoveryMu: the FIRST pipeline to hit the current drop generation
// performs the re-snapshot; peers wait and observe the advanced generation,
// then return nil (their connections were already swapped). On terminal
// failure (budget exhausted / binlog purged / re-snapshot non-transient) it
// returns a LOUD error.
// currentRecoveryGen returns the count of completed re-snapshot recoveries.
// A read pipeline captures this BEFORE a read so recoverFromDrop can tell
// whether a peer already re-snapshotted the drop it then hits (coalescing).
func (r *concurrentBinlogRows) currentRecoveryGen() int {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	return r.recoveryGen
}

func (r *concurrentBinlogRows) recoverFromDrop(ctx context.Context, cause error, observedGen int) error {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()

	// Coalesce: a peer already re-snapshotted since this read began (recoveryGen
	// advanced past the generation the dropped read ran against), so the fresh
	// connections are already swapped in — just resume on them (the caller's
	// loop re-reads from the cursor on the new conns). Only the FIRST lane of a
	// given drop generation falls through to the actual re-snapshot below, so a
	// grow window costs ONE FTWRL re-snapshot, not W.
	if r.recoveryGen > observedGen {
		return nil
	}

	slog.WarnContext(ctx,
		"mysql: native concurrent cold-copy hit a source-read drop; re-snapshotting and resuming incomplete tables from their cursors "+
			"(skip completed; resume keyed tables from cursor; keyless tables re-read from start = at-least-once, Bug 143) — "+
			"CDC anchor stays at the ORIGINAL position",
		slog.String("cause", cause.Error()))

	deadline := time.Now().Add(nativeResnapshotMaxWall)
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-time.After(nativeResnapshotBackoff(attempt)):
		case <-ctx.Done():
			return ctx.Err()
		}

		err := r.recoverViaResnapshot(ctx)
		if err == nil {
			r.recoveryGen++
			return nil
		}
		if errors.Is(err, errBinlogPurgedDuringResnapshot) {
			return err // loud → caller falls back to full restart-from-scratch
		}
		if !isRetriableReadErr(err) {
			// A non-transient re-snapshot failure (e.g. FTWRL privilege lost) —
			// terminal; surface loudly.
			return fmt.Errorf("mysql: native concurrent cold-copy: re-snapshot recovery failed terminally: %w", err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf(
				"mysql: native concurrent cold-copy: source-read drop still failing after riding the re-snapshot window "+
					"(%s wall-clock); the source may be wedged or a prolonged target stall keeps backpressuring the read: %w",
				nativeResnapshotMaxWall, err,
			)
		}
		// Transient re-snapshot failure — back off and retry within budget.
	}
}

// recoverViaResnapshot performs ONE re-snapshot attempt: re-establish a fresh
// consistent snapshot, verify the original CDC anchor P is still available,
// then swap the reader's inner connections to the fresh snapshot. The CDC
// anchor is NEVER touched (r.anchor and the SnapshotStream.Position stay at
// P); only the inner read connections move to P′.
func (r *concurrentBinlogRows) recoverViaResnapshot(ctx context.Context) error {
	if r.resnapshot == nil {
		return errors.New("mysql: native concurrent cold-copy: re-snapshot recovery is not wired; source-read drop is terminal")
	}
	if !r.anchorSet {
		// No recorded anchor ⇒ we cannot prove the §3 invariant (the resumed
		// reads would have no anchor to verify against). Refuse loudly rather
		// than re-read at an unverifiable point.
		return errors.New("mysql: native concurrent cold-copy: no CDC anchor recorded; cannot verify the re-snapshot value-fidelity invariant — source-read drop is terminal")
	}

	// CLOSE the old pinned connections + pool BEFORE acquiring the fresh FTWRL.
	// The old snapshot transactions hold metadata locks on the tables they
	// touched; a new FLUSH TABLES WITH READ LOCK would block behind those MDLs
	// (observed as a multi-minute FTWRL stall). The old snapshot is being
	// abandoned wholesale (every incomplete table re-reads at P′ from its
	// cursor), so releasing it first is correct AND unblocks the re-snapshot.
	// The CDC anchor lives in r.anchor / stream.Position, not in these conns,
	// so this does not touch P.
	r.releaseOldConns()

	conns, db, p2File, p2Pos, err := r.resnapshot(ctx)
	if err != nil {
		return err
	}

	r.cursMu.Lock()
	anchor := r.anchor
	r.cursMu.Unlock()

	// The original anchor P must still be present in the source's binlog for
	// CDC to replay from it. In file_pos mode, if P′ rotated to a strictly
	// later file and P's file is gone, P is purged. (GTID-mode purge is caught
	// later by verifyGTIDSetReachable on the CDC start — the existing loud
	// ErrPositionInvalid path.)
	if anchor.Mode == positionModeFilePos {
		purged, perr := binlogFileBefore(ctx, db, anchor.File, p2File)
		if perr == nil && purged {
			closeConns(conns)
			_ = db.Close()
			return errBinlogPurgedDuringResnapshot
		}
	}

	r.swapConnections(conns, db)

	// Defence-in-depth (the value-fidelity invariant, ADR-0111 §3): prove the
	// reader's recorded anchor was NOT mutated to P′ across the recovery. The
	// reader stamps stream.Position from r.anchor at open and never reassigns
	// r.anchor, so re-encoding it MUST still equal the open-time anchor token;
	// a mismatch means a future edit accidentally advanced the anchor (which
	// would skip P→P′ changes on completed tables — silent loss), and we refuse
	// loudly rather than proceed.
	reAnchor, eerr := encodeBinlogPos(anchor)
	if eerr != nil {
		return fmt.Errorf("mysql: native concurrent cold-copy: re-encode anchor for invariant check: %w", eerr)
	}
	if reAnchor.Token != r.anchorToken {
		return verifyCDCAnchorUnchanged(decodeAnchorTokenOrZero(r.anchorToken), anchor)
	}
	slog.InfoContext(ctx,
		"mysql: native concurrent cold-copy: re-snapshotted at a fresh position; resuming incomplete tables from their cursors "+
			"(CDC anchor stays at the ORIGINAL position — ADR-0111)",
		slog.String("new_snapshot_file", p2File),
		slog.Int("new_snapshot_pos", int(p2Pos)))
	return nil
}

// releaseOldConns rolls back + closes the reader's CURRENT pinned connections
// and pool, clearing readers/metaDB under connMu. Called by recoverViaResnapshot
// BEFORE the fresh FTWRL so the old snapshot transactions' metadata locks are
// released first (otherwise the new FTWRL blocks behind them). The CDC anchor
// is untouched (it lives in r.anchor / stream.Position). After this, pickReader
// returns nil until swapConnections installs the fresh set.
//
// Concurrency: a PEER pipeline that has not yet hit a drop may still be reading
// on these old conns when this runs — that is SAFE, not a data race: the
// per-conn access is serialised by database/sql's own *sql.Conn mutex, and the
// peer's resulting mid-iteration error is classified retriable (row_reader.go →
// classifyApplierError), so the peer enters recoverFromDrop and COALESCES on the
// recovery this goroutine is performing (its captured generation is now behind
// recoveryGen) — it does not start a second re-snapshot. (Earlier wording
// claimed peers are "parked on recoveryMu"; that was inaccurate — peers only
// contend on recoveryMu once they themselves drop. The -race gate covers the
// no-data-race conclusion for copy_table_parallelism>1.)
func (r *concurrentBinlogRows) releaseOldConns() {
	r.connMu.Lock()
	old := r.readers
	oldDB := r.metaDB
	r.readers = nil
	r.byTable = nil
	r.metaDB = nil
	r.connMu.Unlock()

	for _, rr := range old {
		if c, ok := rr.q.(*sql.Conn); ok {
			_, _ = c.ExecContext(context.Background(), "ROLLBACK")
			_ = c.Close()
		}
	}
	if oldDB != nil {
		_ = oldDB.Close()
	}
}

// swapConnections INSTALLS the fresh re-snapshot connections + pool (the old
// set was already released by releaseOldConns before the FTWRL). After the
// swap, readers / byTable / metaDB point at the new snapshot. Holds connMu
// (write) so an in-flight pickReader never observes a half-swapped state.
func (r *concurrentBinlogRows) swapConnections(conns []*sql.Conn, db *sql.DB) {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	readers := make([]*RowReader, len(conns))
	for i, c := range conns {
		// rowFilters re-stamped alongside zeroDate (Bug 201): a recovery that
		// dropped the filters would silently resume filtered tables unfiltered.
		readers[i] = &RowReader{q: c, schema: r.dbName, qualifyBySchema: false, closer: nil, zeroDate: r.zeroDate, rowFilters: r.rowFilters}
	}
	byTable := make(map[string]*RowReader)
	for i, g := range r.groups {
		if i < len(readers) {
			for _, t := range g {
				byTable[t] = readers[i]
			}
		}
	}
	r.readers = readers
	r.byTable = byTable
	r.metaDB = db
}

// closeConns rolls back + closes a set of pinned snapshot connections
// (best-effort), used on the fallback path when a just-opened re-snapshot is
// abandoned.
func closeConns(conns []*sql.Conn) {
	for _, c := range conns {
		_, _ = c.ExecContext(context.Background(), "ROLLBACK")
		_ = c.Close()
	}
}

// commitAndCloseConns COMMITs + closes the reader's CURRENT pinned connections
// and pool, first-error-wins (the ADR-0101 §8 lifecycle, now reader-owned so
// it cleans up whatever connection set the reader holds NOW — after an
// ADR-0111 re-snapshot recovery those are the FRESH P′ conns, the dropped
// originals already closed by swapConnections). The CDC anchor is unaffected
// (it lives in stream.Position / r.anchor, not in the connections). Reads the
// current set under connMu, then does the network work off the lock.
func (r *concurrentBinlogRows) commitAndCloseConns() error {
	r.connMu.Lock()
	readers := r.readers
	db := r.metaDB
	r.readers = nil
	r.metaDB = nil
	r.connMu.Unlock()

	var firstErr error
	for _, rr := range readers {
		c, ok := rr.q.(*sql.Conn)
		if !ok {
			continue
		}
		if _, err := c.ExecContext(context.Background(), "COMMIT"); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if db != nil {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// binlogFileBefore reports whether the candidate anchor file is STRICTLY
// before the earliest binlog the source still retains — i.e. purged. It reads
// SHOW BINARY LOGS (the list of retained files) and checks whether anchorFile
// appears; absence (when a later file p2File exists, proving binlogging is
// on) means it was purged. On any query error it returns (false, err) so the
// caller does NOT misclassify a transient as a purge.
func binlogFileBefore(ctx context.Context, db *sql.DB, anchorFile, p2File string) (bool, error) {
	if anchorFile == "" {
		return false, nil
	}
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		// Older servers: SHOW BINLOG EVENTS / privilege issues. Don't treat as
		// purge — return the error so the caller keeps the anchor.
		return false, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return false, err
	}
	present := false
	for rows.Next() {
		dest := make([]any, len(cols))
		holders := make([]any, len(cols))
		for i := range dest {
			holders[i] = &dest[i]
		}
		if err := rows.Scan(holders...); err != nil {
			return false, err
		}
		name, _ := scanString(dest[0])
		if name == anchorFile {
			present = true
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	// anchorFile absent AND a current file exists (p2File) ⇒ purged.
	return !present && p2File != "", nil
}

// tableHasOrderablePK reports whether table has a primary key whose every
// column is an orderable type — the engine-side mirror of the pipeline's
// isOrderablePKType (the engine cannot import the pipeline package). A keyed
// table resumes WHERE (pk) > lastpk; a non-keyed table truncate-restarts. The
// family set is kept in lockstep with isOrderablePKType (a divergence would
// only mean a keyed table degrades to truncate-restart — never silent loss —
// but lockstep keeps the two paths consistent).
func tableHasOrderablePK(table *ir.Table) bool {
	if table == nil || table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		return false
	}
	for _, pkc := range table.PrimaryKey.Columns {
		col := lookupColumnByName(table, pkc.Column)
		if col == nil || col.SluiceInjected || !isOrderablePKLeaf(col.Type) {
			return false
		}
	}
	return true
}

// isOrderablePKLeaf mirrors pipeline.isOrderablePKType: a type usable as (part
// of) a resume cursor — sorts deterministically under ORDER BY and round-trips
// through a `?` placeholder. ir.Domain unwraps to its base; JSON / Array /
// Geometry / Set / Enum / unknown are NOT orderable.
func isOrderablePKLeaf(t ir.Type) bool {
	if dom, ok := t.(ir.Domain); ok {
		if dom.BaseType == nil {
			return false
		}
		return isOrderablePKLeaf(dom.BaseType)
	}
	switch t.(type) {
	case ir.Integer, ir.Decimal,
		ir.Char, ir.Varchar, ir.Text, ir.UUID,
		ir.Binary, ir.Varbinary, ir.Blob, ir.Bit,
		ir.Date, ir.Time, ir.Timestamp, ir.DateTime:
		return true
	default:
		return false
	}
}

// lookupColumnByName returns table's column named name, or nil. Linear scan
// (tables have few columns); named distinctly from the pipeline's
// lookupColumn / the test-only findColumn.
func lookupColumnByName(table *ir.Table, name string) *ir.Column {
	for _, c := range table.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// primaryKeyColumnNames returns table's PK column names in index order, or
// nil for a keyless table.
func primaryKeyColumnNames(table *ir.Table) []string {
	if table == nil || table.PrimaryKey == nil {
		return nil
	}
	out := make([]string, 0, len(table.PrimaryKey.Columns))
	for _, c := range table.PrimaryKey.Columns {
		out = append(out, c.Column)
	}
	return out
}

// extractPKTuple pulls the ordered-PK tuple from a just-emitted row in PK
// column order. A missing PK column (shouldn't happen — the PK columns are in
// the SELECT projection) contributes a nil, which the next WHERE (pk) > (?)
// would compare as NULL; that is defensive only.
func extractPKTuple(row ir.Row, pkNames []string) []any {
	if len(pkNames) == 0 {
		return nil
	}
	out := make([]any, len(pkNames))
	for i, name := range pkNames {
		out[i] = row[name]
	}
	return out
}

// isRetriableReadErr reports whether err is the connection-drop class the
// re-snapshot recovery should ride out. It mirrors the pipeline's
// isRetriableSourceReadError (walks the chain for ir.RetriableError) but
// lives in the engine because the recovery is engine-side; the MySQL reader
// already classifies its rows-iteration error via classifyApplierError
// (row_reader.go), so the same ir.RetriableError surface is reachable here.
func isRetriableReadErr(err error) bool {
	if err == nil {
		return false
	}
	var re ir.RetriableError
	return errors.As(err, &re) && re.Retriable()
}
