// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"

	"sluicesync.dev/sluice/internal/ir"
)

// openVStreamSnapshotStream is the FlavorPlanetScale path of
// [Engine.OpenSnapshotStream]. It rides VStream's built-in COPY mode:
// a [binlogdata.ShardGtid] with an empty Gtid asks vtgate to run an
// internal table-copy phase before tailing CDC, with the seam marked
// by COPY_COMPLETED events.
//
// The function captures a no-gap, no-overlap snapshot in a single
// physical stream WITHOUT draining it to completion first (ADR-0071):
//
//  1. Open the gRPC VStream with the from-beginning sentinel
//     ([fromBeginningVStreamPos]).
//  2. Build a [SnapshotStream] and spawn a background COPY-pump
//     goroutine ([copyPump]) that Recv's the gRPC stream, appends
//     rows to per-table queues UNDER the byte cap, updates the field
//     cache, and tracks the latest VGTID as it arrives. The single
//     global COPY_COMPLETED event (one with empty Keyspace+Shard,
//     fired after every per-shard/per-table COPY_COMPLETED has
//     arrived) marks the boundary; at that point the pump records
//     the final [ir.Position] onto the [SnapshotStream] and signals
//     copy-completion to every queue.
//  3. [Rows.ReadRows] streams a table's rows from its queue AS THEY
//     ARRIVE, blocking until a row is available or copy completes.
//     A slow target backpressures the queue, which backpressures the
//     pump's append, which backpressures Recv, which backpressures
//     Vitess — so a single large table copies in constant memory.
//  4. [Changes] resumes reading the same gRPC stream after the COPY
//     phase and routes events as [ir.Change] values.
//
// Bounded memory is the point (ADR-0071, extending ADR-0028): the
// pre-streaming reader buffered the ENTIRE COPY phase before a single
// row reached the target — a 13 GB table drove RSS to ~41 GB and got
// OOM-killed with zero target writes. The byte cap ([maxBufferBytes],
// default 64 MiB) bounds the per-table queue; the streaming handoff
// means target writes begin immediately. The multi-table interleaving
// edge — rows for a not-yet-consumed table accumulating past the cap
// while another table is being drained — is a loud refusal (Phase 1
// floor), not an OOM; disk-spill for that tail is deferred (Phase 3).
func (e Engine) openVStreamSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.DBName == "" {
		return nil, errors.New("mysql/vstream: snapshot: DSN has no database name (vitess keyspace expected)")
	}

	endpoint, err := vstreamEndpointFromDSN(cfg)
	if err != nil {
		return nil, err
	}
	dialOpts, _, err := vstreamDialOptions(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(endpoint, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("mysql/vstream: snapshot: dial %s: %w", endpoint, err)
	}

	keyspace := cfg.DBName
	shards, err := resolveVStreamShards(ctx, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	client := vtgateservice.NewVitessClient(conn)

	// The gRPC stream lives for the lifetime of the SnapshotStream:
	// the COPY phase reads it synchronously here, the CDC phase
	// resumes reading it from a goroutine started by StreamChanges.
	// streamCtx is cancelled by CloseFn so a hung Recv unblocks.
	streamCtx, streamCancel := context.WithCancel(context.Background())

	// COPY mode runs against the PRIMARY tablet rather than a
	// REPLICA: vtgate's `uvstreamer.buildTablePlan` enumerates
	// tables via the tablet's schema engine, and the PRIMARY is
	// guaranteed to have the freshest catalog (replicas can lag the
	// schema-tracker by minutes on a quiet binlog). The standalone
	// CDC reader streams from REPLICA to keep load off the primary,
	// but the snapshot is a one-shot operation where catalog
	// freshness matters more than read isolation.
	req := &vtgate.VStreamRequest{
		TabletType: topodata.TabletType_PRIMARY,
		Vgtid: &binlogdata.VGtid{
			ShardGtids: toProtoShardGtids(fromBeginningVStreamPos(keyspace, shards)),
		},
		Filter: &binlogdata.Filter{Rules: []*binlogdata.Rule{{Match: "/.*/"}}},
		Flags: &vtgate.VStreamFlags{
			MinimizeSkew:      true,
			StopOnReshard:     true,
			HeartbeatInterval: 5,
		},
	}

	grpcStream, err := client.VStream(streamCtx, req)
	if err != nil {
		streamCancel()
		_ = conn.Close()
		return nil, fmt.Errorf("mysql/vstream: snapshot: open stream: %w", err)
	}

	snap := &vstreamSnapshotStream{
		keyspace:            keyspace,
		fields:              make(map[string][]*query.Field),
		rowBuffer:           make(map[string][]ir.Row),
		copyCompletedShards: make(map[string]bool),
		maxBufferBytes:      defaultSnapshotMaxBufferBytes,
		conn:                conn,
		grpcStream:          grpcStream,
		grpcCancel:          streamCancel,
	}
	snap.cond = sync.NewCond(&snap.mu)

	stream := &ir.SnapshotStream{
		// Position is finalised by the COPY pump at the global
		// COPY_COMPLETED event and read by the orchestrator only AFTER
		// bulk-copy (ADR-0071). The happens-before edge is the per-table
		// row-channel close: the pump records the final position BEFORE
		// signalling copy-completion, and the orchestrator's post-bulk-
		// copy read is ordered after every ReadRows channel has closed.
		// It is the zero Position at open time; the cold-start log line
		// that reads it immediately after open simply shows an empty
		// token (cosmetic — the load-bearing read is the post-copy one).
		Rows:    &vstreamSnapshotRows{snap: snap},
		Changes: &vstreamSnapshotChanges{snap: snap},
	}
	stream.CloseFn = snap.close

	// Pump the COPY phase concurrently rather than draining it to
	// completion here. streamCtx (owned by CloseFn) bounds the pump's
	// lifetime; the caller's ctx does NOT, because the SnapshotStream —
	// and the gRPC stream it rides — must outlive this function.
	snap.copyDone = make(chan struct{})
	go snap.copyPump(streamCtx, stream)

	return stream, nil
}

// vstreamSnapshotStream owns the gRPC connection and stream that
// produce both the snapshot rows and the post-snapshot CDC events.
// Lives for the life of the [ir.SnapshotStream] returned by
// [Engine.openVStreamSnapshotStream]; closed via [close].
//
// The struct exists in three logical states (ADR-0071 reshaped the
// COPY phase from a synchronous pre-drain into a concurrent streaming
// pump):
//
//  1. COPY phase, pumped CONCURRENTLY by [copyPump] (a background
//     goroutine spawned at [openVStreamSnapshotStream] return). The
//     pump Recv's the gRPC stream and appends rows to the per-table
//     queues in rowBuffer UNDER the byte cap (maxBufferBytes), updates
//     the field cache, and tracks the latest VGTID. Meanwhile
//     [vstreamSnapshotRows.ReadRows] streams each table's rows from
//     its queue AS THEY ARRIVE: a consumer blocks on cond until a row
//     is available or copy completes, and the pump blocks on cond when
//     the active table's queue is over the cap (backpressure → Recv →
//     Vitess). A not-yet-consumed table accumulating past the cap is a
//     loud refusal, not an OOM (Phase 1 floor; Phase 3 disk-spill
//     deferred). The global COPY_COMPLETED event (empty Keyspace+Shard)
//     records the final [ir.Position] onto the SnapshotStream, sets
//     copyComplete, and broadcasts; the copyDone channel is closed.
//  2. Idle, between the pump finishing COPY and [Changes.StreamChanges]
//     being called. The gRPC stream is held but no events are being
//     consumed — vtgate buffers them server-side until the orchestrator
//     finishes the bulk-copy phase.
//  3. CDC phase, after [Changes.StreamChanges] is called. The same
//     gRPC stream is resumed by [pump], which emits ir.Change values
//     onto the changes channel.
//
// Concurrency: mu (with cond) guards fields, currentVgtid, rowBuffer,
// bufferedBytes, activeTable, copyComplete, maxBufferBytes, and err.
// The COPY pump is the sole writer of rowBuffer/bufferedBytes during
// state 1; ReadRows consumers remove from it under the same lock. The
// final Position is written onto the SnapshotStream by the pump BEFORE
// it sets copyComplete + closes copyDone, so the orchestrator's
// post-bulk-copy Position read is ordered after that write via the
// ReadRows channel-close happens-before edge.
type vstreamSnapshotStream struct {
	keyspace string

	// fields caches column metadata keyed by [fieldCacheKey]. Shared
	// between COPY-phase row decoding and post-COPY change decoding —
	// FIELD events arrive in both phases, and a row cannot be decoded
	// without its field list. Guarded by mu (the COPY pump and the CDC
	// pump both write it; ReadRows never touches it).
	fields map[string][]*query.Field

	// currentVgtid is the latest VGTID observed on the stream. When the
	// COPY pump reaches the global COPY_COMPLETED, this is the snapshot-
	// consistent position; during the CDC phase it advances with each
	// transaction's VGTID event. Guarded by mu.
	currentVgtid []shardGtid

	// rowBuffer holds the not-yet-consumed COPY-phase rows keyed by
	// unqualified table name — a per-table FIFO queue the pump appends
	// to and [vstreamSnapshotRows.ReadRows] drains AS rows arrive
	// (ADR-0071: streaming, not buffer-then-serve). A table's queue is
	// deleted once drained AND copy is complete, so a second ReadRows
	// on the same table returns an empty channel (matches the single-
	// shot-per-table contract in row_reader.go). Guarded by mu/cond.
	//
	// Multi-shard sharded keyspaces fan rows for the *same logical
	// table* in from multiple shards. Keying by unqualified table
	// name (rather than per-shard) merges them into one queue so
	// the orchestrator's single-table ReadRows call surfaces every
	// row regardless of shard origin.
	rowBuffer map[string][]ir.Row

	// bufferedBytes is the running [ir.ApproximateRowBytes] sum of every
	// row currently sitting in rowBuffer (appended on enqueue, debited
	// on ReadRows handoff). The byte cap (maxBufferBytes) is enforced
	// against it. Guarded by mu.
	bufferedBytes int64

	// maxBufferBytes is the soft byte cap (ADR-0028 / ADR-0071) on
	// rowBuffer. The pump backpressures (or, for a not-yet-consumed
	// table, refuses loudly) when an append would push bufferedBytes
	// over it. Default [defaultSnapshotMaxBufferBytes]; overridable via
	// [vstreamSnapshotRows.SetMaxBufferBytes]. Guarded by mu.
	maxBufferBytes int64

	// activeTable is the unqualified name of the table whose ReadRows
	// channel is currently being drained (empty when none). The pump
	// backpressures only on the active table's over-cap growth (a
	// consumer is draining it, so the stall resolves); growth of a
	// DIFFERENT, not-yet-consumed table past the cap is the loud-refuse
	// case. Guarded by mu.
	activeTable string

	// copyComplete is set true when the COPY pump reaches the global
	// COPY_COMPLETED. ReadRows uses it to close a table's channel once
	// its queue is empty (before it, an empty queue means "more may
	// arrive — block"). Guarded by mu.
	copyComplete bool

	// copyDone is closed by the COPY pump exactly once, when COPY ends
	// (either at COPY_COMPLETED or on a terminal pump error). Lets
	// [startPump] join the COPY phase before resuming the stream in CDC
	// mode so the two pumps never Recv concurrently.
	copyDone chan struct{}

	// cond signals queue/byte-cap state changes between the COPY pump
	// and ReadRows consumers. Built over mu in the constructor.
	cond *sync.Cond

	// copyCompletedShards tracks per-scope COPY_COMPLETED events
	// (those carrying a non-empty Keyspace/Shard) seen during the
	// COPY phase. The COPY pump terminates on vtgate's *global*
	// COPY_COMPLETED event (Keyspace and Shard both empty), which
	// fires once every per-scope copy has finished. The per-scope
	// set is recorded for visibility — multi-shard snapshots emit
	// one entry per (keyspace, shard, table) tuple before the
	// global terminator, and surfacing the count via tests confirms
	// the per-scope-vs-global routing is wired correctly.
	copyCompletedShards map[string]bool

	conn       *grpc.ClientConn
	grpcStream vtgateservice.Vitess_VStreamClient
	grpcCancel context.CancelFunc // cancels the gRPC stream context

	// pumpStarted prevents StreamChanges from being called twice on
	// the same SnapshotStream (the underlying gRPC stream has linear
	// state — two concurrent pumps would race on r.fields and the
	// stream's Recv). Guarded by mu.
	pumpStarted bool

	mu  sync.Mutex
	err error
}

// broadcast wakes every goroutine parked on cond. Guarded against a
// nil cond so the dispatch/enqueue helpers stay callable from unit
// tests that construct a bare vstreamSnapshotStream literal (no
// constructor, no cond) and exercise the dispatcher single-threaded —
// those tests use sub-cap rows, so the backpressure cond.Wait path is
// never reached; only the post-enqueue signal needs the guard.
func (s *vstreamSnapshotStream) broadcast() {
	if s.cond != nil {
		s.cond.Broadcast()
	}
}

// defaultSnapshotMaxBufferBytes is the byte cap the COPY pump enforces
// on rowBuffer when the orchestrator never calls SetMaxBufferBytes. It
// matches ADR-0028's `--max-buffer-bytes` default (64 MiB) so the
// snapshot path bounds memory out of the box; the constructor seeds it
// and [vstreamSnapshotRows.SetMaxBufferBytes] overrides it.
const defaultSnapshotMaxBufferBytes int64 = 64 << 20

// copyPump is the background COPY-phase goroutine (ADR-0071). It Recv's
// the gRPC stream and dispatches each VEvent until the global
// COPY_COMPLETED arrives (or ctx cancels / Recv errors), then closes
// copyDone exactly once so the CDC pump can resume the same stream.
//
// On the COPY_COMPLETED boundary the dispatcher has already recorded
// the snapshot-consistent VGTID and written the final [ir.Position]
// onto stream; copyPump only has to broadcast the terminal state so any
// ReadRows consumer still blocked on an empty queue wakes and closes
// its channel.
func (s *vstreamSnapshotStream) copyPump(ctx context.Context, stream *ir.SnapshotStream) {
	defer close(s.copyDone)

	for {
		select {
		case <-ctx.Done():
			s.failCopy(ctx.Err())
			return
		default:
		}
		resp, err := s.grpcStream.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				s.failCopy(err)
				return
			}
			s.failCopy(classifyReaderError(fmt.Errorf("mysql/vstream: snapshot: copy recv: %w", err)))
			return
		}
		for _, ev := range resp.GetEvents() {
			done, err := s.dispatchCopyEvent(ev)
			if err != nil {
				s.failCopy(err)
				return
			}
			if done {
				s.finishCopy(stream)
				return
			}
		}
	}
}

// finishCopy records the final snapshot position onto the stream and
// flips copyComplete, then broadcasts so blocked ReadRows consumers
// drain-and-close. The Position write happens-before every ReadRows
// channel close (a consumer can only observe copyComplete under the
// same mu the pump holds here), which in turn happens-before the
// orchestrator's post-bulk-copy stream.Position read — so the plain
// stream.Position field write is race-clean despite the orchestrator
// reading the field without a lock. encodeVStreamPos can fail only
// when no VGTID was ever observed (empty snapshot with no GTID); that
// surfaces as a terminal pump error rather than a silent empty
// position.
func (s *vstreamSnapshotStream) finishCopy(stream *ir.SnapshotStream) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err == nil {
		pos, err := encodeVStreamPos(s.currentVgtid)
		if err != nil {
			s.err = fmt.Errorf("mysql/vstream: snapshot: encode position: %w", err)
		} else {
			stream.Position = pos
		}
	}
	s.copyComplete = true
	s.broadcast()
}

// failCopy records a terminal COPY-phase error (first one wins) and
// flips copyComplete so blocked ReadRows consumers wake, observe the
// error via Err, and close their channels rather than hang forever.
func (s *vstreamSnapshotStream) failCopy(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
	s.copyComplete = true
	s.broadcast()
}

// drainCopyPhase reads VEvents off the gRPC stream synchronously
// until the global COPY_COMPLETED event arrives, populating
// rowBuffer and updating currentVgtid as it goes. The caller's ctx
// bounds how long we'll wait for the COPY phase to finish; if ctx
// dispatchCopyEvent routes a single COPY-phase VEvent. Returns
// done=true when the global COPY_COMPLETED arrives (the boundary
// between snapshot and CDC). All non-row, non-FIELD, non-VGTID
// events during COPY are bookkeeping and silently dropped.
//
// Acquires s.mu for the whole dispatch: the COPY pump is the sole
// caller in production and runs concurrently with ReadRows consumers,
// so every mutation of fields / currentVgtid / rowBuffer /
// copyCompletedShards must be guarded. bufferCopyRow may release and
// reacquire the lock via cond.Wait while backpressuring; that is the
// only point at which a consumer can interleave, and it does so safely
// (the queue and byte counters are consistent at every Wait boundary).
func (s *vstreamSnapshotStream) dispatchCopyEvent(ev *binlogdata.VEvent) (done bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dispatchCopyEventLocked(ev)
}

// dispatchCopyEventLocked is the body of [dispatchCopyEvent]; the
// caller holds s.mu.
func (s *vstreamSnapshotStream) dispatchCopyEventLocked(ev *binlogdata.VEvent) (done bool, err error) {
	switch ev.GetType() {
	case binlogdata.VEventType_FIELD:
		fe := ev.GetFieldEvent()
		if fe == nil {
			return false, nil
		}
		key := fieldCacheKey(fe.GetShard(), fe.GetTableName())
		s.fields[key] = fe.GetFields()
		return false, nil

	case binlogdata.VEventType_ROW:
		return false, s.bufferCopyRow(ev)

	case binlogdata.VEventType_VGTID:
		vg := ev.GetVgtid()
		if vg == nil {
			return false, nil
		}
		s.currentVgtid = vgtidToShardGtidSlice(vg)
		return false, nil

	case binlogdata.VEventType_COPY_COMPLETED:
		// COPY_COMPLETED has two flavours during a multi-shard
		// snapshot:
		//
		//   1. Per-scope: Keyspace+Shard populated. Fires when one
		//      (shard, table) pair finishes its copy — a progress
		//      marker. We track these so an operator can observe
		//      shard-level progress, but they DO NOT terminate the
		//      drain.
		//   2. Global: Keyspace+Shard both empty. Fires once after
		//      every per-scope copy has finished (cf. vtgate's
		//      vstream_manager.isCopyFullyCompleted). This is the
		//      snapshot→CDC handoff boundary.
		//
		// Only the global event terminates. Single-shard streams
		// see exactly one per-scope event followed by one global
		// event; multi-shard streams see N×T per-scope events
		// (N shards × T tables) followed by one global event.
		if ev.GetKeyspace() == "" && ev.GetShard() == "" {
			return true, nil
		}
		if s.copyCompletedShards == nil {
			s.copyCompletedShards = make(map[string]bool)
		}
		key := shardScopeKey(ev.GetKeyspace(), ev.GetShard())
		s.copyCompletedShards[key] = true
		return false, nil

	case binlogdata.VEventType_JOURNAL:
		// Reshard during COPY. v1 of multi-shard snapshot doesn't
		// recover in place — the row buffer is keyed by table not
		// shard, and the new shards' COPY phases would re-emit rows
		// the old shards already buffered. Surface the typed error
		// so the caller (typically [pipeline.Streamer.coldStart])
		// drops the snapshot stream and starts a fresh one against
		// the new layout. Full multi-shard COPY-with-reshard
		// recovery is a future chunk.
		return false, journalToShardLayoutErr(ev.GetJournal())

	default:
		// LASTPK, BEGIN, COMMIT, HEARTBEAT, GTID, OTHER, etc. — all
		// fine to ignore during COPY.
		return false, nil
	}
}

// bufferCopyRow decodes a COPY-phase ROW event and appends each row
// to rowBuffer under the unqualified table name. During COPY mode
// every RowChange has only an After image (the rows are being
// copied, not modified); we treat anything that decodes as a row as
// a snapshot row.
//
// Caller holds s.mu. Each kept row is enqueued via [enqueueRowLocked],
// which enforces the byte cap (ADR-0071): backpressure for the table a
// consumer is actively draining, loud refusal for a not-yet-consumed
// table accumulating past the cap.
func (s *vstreamSnapshotStream) bufferCopyRow(ev *binlogdata.VEvent) error {
	rev := ev.GetRowEvent()
	if rev == nil {
		return nil
	}
	key := fieldCacheKey(rev.GetShard(), rev.GetTableName())
	fields, ok := s.fields[key]
	if !ok {
		return fmt.Errorf("mysql/vstream: snapshot: row event for %q without preceding FIELD event", key)
	}
	tableName := stripKeyspaceFromTable(rev.GetTableName(), rev.GetKeyspace())
	for _, rc := range rev.GetRowChanges() {
		row, ok := decodeVStreamRow(rc.GetAfter(), fields)
		if !ok {
			// COPY-phase rows always have After populated. A missing
			// After is malformed; skip it so the rest of the table
			// still buffers cleanly.
			continue
		}
		// Bug 125: every decoded COPY row is buffered — we do NOT drop
		// behind-the-scan emissions. The pre-v0.100 dedup (GitHub #14)
		// assumed Vitess's COPY scan emits in PK-ascending order of the
		// PRI_KEY_FLAG column and dropped any row with PK <= max-seen;
		// that assumption is FALSE when Vitess orders the scan by a
		// cheaper unique key than the flagged PK, silently dropping
		// legitimate forward rows (the 13.5M-of-19M incident). The
		// idempotent COPY writer (ON DUPLICATE KEY UPDATE, see
		// [vstreamSnapshotRows.CopyNeedsIdempotentWriter]) absorbs the
		// catchup re-emissions without any ordering assumption, so the
		// drop is gone entirely.
		if err := s.enqueueRowLocked(tableName, row); err != nil {
			return err
		}
	}
	return nil
}

// enqueueRowLocked appends one kept COPY row to tableName's queue under
// the byte cap (ADR-0071). The caller holds s.mu.
//
//   - Backpressure: when the append would push bufferedBytes over the
//     cap AND tableName is the table a consumer is actively draining
//     (or no consumer is active yet — the orchestrator drains every
//     table in turn, so one is coming), cond.Wait blocks until the
//     consumer drains enough to fit. The consumer's ReadRows debit +
//     cond.Broadcast wakes us. This is the dominant single-large-table
//     path — the queue never grows past the cap because the target
//     drains it, so memory stays constant and Recv backpressures
//     Vitess. cond.Wait may be woken by close() cancelling the stream;
//     we re-check the terminal state each iteration to avoid a wedge
//     on shutdown.
//   - Loud refusal: when the over-cap table is NOT the one being
//     drained while a DIFFERENT table IS being drained (a not-yet-
//     consumed table accumulating while another table is read — the
//     multi-table interleaving edge), blocking would deadlock: the
//     active consumer's table gets no more rows because the pump is
//     blocked, so neither side progresses. We refuse loudly rather
//     than OOM-or-deadlock (Phase 1 floor; disk-spill is the deferred
//     Phase 3).
//
// A single row larger than the cap on an otherwise-empty queue still
// goes through (bufferedBytes==0, so the guard's bufferedBytes>0 term
// is false); this matches ADR-0028's soft-target semantics and avoids
// wedging a table whose individual rows exceed the cap.
func (s *vstreamSnapshotStream) enqueueRowLocked(tableName string, row ir.Row) error {
	rowBytes := ir.ApproximateRowBytes(row)

	for s.bufferedBytes > 0 && s.bufferedBytes+rowBytes > s.maxBufferBytes {
		// Over cap with at least one row already queued. A consumer
		// actively draining a DIFFERENT table can never relieve the
		// pressure on this one — refuse rather than deadlock.
		if s.activeTable != "" && s.activeTable != tableName {
			return fmt.Errorf(
				"mysql/vstream: snapshot: table %q would buffer %d bytes, exceeding the --max-buffer-bytes cap of %d "+
					"while table %q is being copied; this multi-table interleaving case is not yet disk-spilled "+
					"(ADR-0071 Phase 3). Raise --max-buffer-bytes, or migrate the large tables in separate runs",
				tableName, s.bufferedBytes+rowBytes, s.maxBufferBytes, s.activeTable,
			)
		}
		// Wait for a consumer to drain this table. close() flips err +
		// copyComplete and broadcasts, so a shutdown mid-wait unwedges.
		if s.err != nil || s.copyComplete {
			if s.err != nil {
				return s.err
			}
			return errors.New("mysql/vstream: snapshot: copy ended before backpressured row could be buffered")
		}
		s.cond.Wait()
	}

	s.rowBuffer[tableName] = append(s.rowBuffer[tableName], row)
	s.bufferedBytes += rowBytes
	s.broadcast()
	return nil
}

// startPump spawns the post-COPY CDC pump goroutine. Returns the
// changes channel the pump owns and closes on shutdown. Idempotent
// guard via pumpStarted: a second StreamChanges call returns an
// error rather than racing on the gRPC stream.
//
// Joins the COPY pump first (copyDone): the CDC pump reuses the same
// gRPC stream, and the two must never Recv concurrently. The
// orchestrator only calls StreamChanges after bulk-copy (every
// ReadRows drained → COPY_COMPLETED reached → copyDone closed), so this
// is effectively non-blocking in production; the explicit join makes
// the no-concurrent-Recv invariant a structural guarantee rather than
// a sequencing assumption. A terminal COPY-phase error short-circuits
// here so the streamer's cold-start surfaces it instead of starting a
// CDC pump against a dead stream.
func (s *vstreamSnapshotStream) startPump(ctx context.Context) (<-chan ir.Change, error) {
	s.mu.Lock()
	if s.pumpStarted {
		s.mu.Unlock()
		return nil, errors.New("mysql/vstream: snapshot: StreamChanges already called")
	}
	s.pumpStarted = true
	s.mu.Unlock()

	if s.copyDone != nil {
		select {
		case <-s.copyDone:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := s.Err(); err != nil {
		return nil, err
	}

	out := make(chan ir.Change, vstreamChannelBuffer)
	go s.pump(ctx, out)
	return out, nil
}

// pump owns out and closes it before returning. The CDC phase
// reuses the same gRPC stream the COPY phase drained; events still
// flow against the cached field map (which may grow if new tables
// surface or FIELD events refresh on DDL).
func (s *vstreamSnapshotStream) pump(ctx context.Context, out chan<- ir.Change) {
	defer close(out)

	for {
		// Honour caller cancellation independently of the stream's
		// internal cancellation: ctx is the StreamChanges caller's
		// context, while grpcCancel is owned by CloseFn.
		select {
		case <-ctx.Done():
			return
		default:
		}

		resp, err := s.grpcStream.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			s.setErr(classifyReaderError(fmt.Errorf("mysql/vstream: snapshot: cdc recv: %w", err)))
			return
		}
		for _, ev := range resp.GetEvents() {
			if err := s.dispatchCDCEvent(ctx, ev, out); err != nil {
				s.setErr(err)
				return
			}
		}
	}
}

// dispatchCDCEvent is the post-COPY counterpart to
// [dispatchCopyEvent]. Same shape as [vstreamCDCReader.dispatch] but
// inlined here so the snapshot stream doesn't have to share a
// reader-state struct with the standalone CDC reader. The two paths
// have small but real differences (e.g., truncate is meaningful in
// CDC mode, ignored during COPY) that justify the duplication.
func (s *vstreamSnapshotStream) dispatchCDCEvent(ctx context.Context, ev *binlogdata.VEvent, out chan<- ir.Change) error {
	switch ev.GetType() {
	case binlogdata.VEventType_FIELD:
		fe := ev.GetFieldEvent()
		if fe == nil {
			return nil
		}
		key := fieldCacheKey(fe.GetShard(), fe.GetTableName())
		s.fields[key] = fe.GetFields()
		return nil

	case binlogdata.VEventType_ROW:
		return s.dispatchCDCRow(ctx, ev, out)

	case binlogdata.VEventType_VGTID:
		vg := ev.GetVgtid()
		if vg == nil {
			return nil
		}
		s.currentVgtid = vgtidToShardGtidSlice(vg)
		return nil

	case binlogdata.VEventType_DDL:
		return s.dispatchCDCDDL(ctx, ev, out)

	case binlogdata.VEventType_JOURNAL:
		// Same contract as [vstreamCDCReader.dispatch]: surface a
		// typed [ShardLayoutChangedError] carrying the new layout
		// so the caller can decide whether to reopen against the
		// new shard set or fail loudly.
		return journalToShardLayoutErr(ev.GetJournal())

	default:
		// BEGIN, COMMIT, HEARTBEAT, GTID, OTHER, VERSION, LASTPK,
		// SAVEPOINT, ROLLBACK, COPY_COMPLETED (a stray one), etc. —
		// all bookkeeping. Drop silently.
		return nil
	}
}

// dispatchCDCRow turns a ROW event into [ir.Insert] / [ir.Update] /
// [ir.Delete] events. Mirrors [vstreamCDCReader.dispatchRow] —
// kept side-by-side rather than refactored into a shared core so
// each file reads end-to-end without cross-file jumps.
func (s *vstreamSnapshotStream) dispatchCDCRow(ctx context.Context, ev *binlogdata.VEvent, out chan<- ir.Change) error {
	rev := ev.GetRowEvent()
	if rev == nil {
		return nil
	}
	key := fieldCacheKey(rev.GetShard(), rev.GetTableName())
	fields, ok := s.fields[key]
	if !ok {
		return fmt.Errorf("mysql/vstream: snapshot: row event for %q without preceding FIELD event", key)
	}
	pos, err := s.positionFor()
	if err != nil {
		return err
	}
	tableName := stripKeyspaceFromTable(rev.GetTableName(), rev.GetKeyspace())

	for _, rc := range rev.GetRowChanges() {
		before, beforeOK := decodeVStreamRow(rc.GetBefore(), fields)
		after, afterOK := decodeVStreamRow(rc.GetAfter(), fields)
		switch {
		case afterOK && !beforeOK:
			if err := send(ctx, out, ir.Insert{
				Position: pos,
				Schema:   rev.GetKeyspace(),
				Table:    tableName,
				Row:      after,
			}); err != nil {
				return err
			}
		case beforeOK && afterOK:
			if err := send(ctx, out, ir.Update{
				Position: pos,
				Schema:   rev.GetKeyspace(),
				Table:    tableName,
				Before:   before,
				After:    after,
			}); err != nil {
				return err
			}
		case beforeOK && !afterOK:
			if err := send(ctx, out, ir.Delete{
				Position: pos,
				Schema:   rev.GetKeyspace(),
				Table:    tableName,
				Before:   before,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// dispatchCDCDDL handles a post-COPY DDL event. Same shape as
// [vstreamCDCReader.dispatchDDL]: parse for TRUNCATE TABLE; emit
// [ir.Truncate] when matched and inside our keyspace; either way
// invalidate the field cache.
func (s *vstreamSnapshotStream) dispatchCDCDDL(ctx context.Context, ev *binlogdata.VEvent, out chan<- ir.Change) error {
	stmt := ev.GetStatement()
	if stmt == "" {
		clear(s.fields)
		return nil
	}

	if truncSchema, truncTable, ok := parseTruncateTable(stmt); ok {
		if truncSchema == "" {
			truncSchema = ev.GetKeyspace()
		}
		truncTable = stripKeyspaceFromTable(truncTable, truncSchema)
		if truncSchema == s.keyspace {
			pos, err := s.positionFor()
			if err != nil {
				return err
			}
			if err := send(ctx, out, ir.Truncate{
				Position: pos,
				Schema:   truncSchema,
				Table:    truncTable,
			}); err != nil {
				return err
			}
		}
	}

	clear(s.fields)
	return nil
}

// positionFor encodes the current VGTID into an [ir.Position]. The
// returned position is what the next emitted change advertises as
// its resume point.
func (s *vstreamSnapshotStream) positionFor() (ir.Position, error) {
	if len(s.currentVgtid) == 0 {
		return ir.Position{}, nil
	}
	return encodeVStreamPos(s.currentVgtid)
}

// setErr stores the first error the pump goroutine sees. Subsequent
// errors are dropped; the original cause is the useful one.
func (s *vstreamSnapshotStream) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// Err returns the error that terminated the pump goroutine, if any.
// nil after a clean ctx-cancellation shutdown.
func (s *vstreamSnapshotStream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// close cancels the gRPC stream and closes the connection. Wired
// into [ir.SnapshotStream.CloseFn]. Safe to call multiple times.
//
// Cancelling the gRPC context unblocks a COPY pump parked in Recv; it
// then records the cancellation as a terminal error (failCopy), which
// flips copyComplete and broadcasts cond — so a COPY pump parked in
// enqueue backpressure or a ReadRows consumer parked waiting for more
// rows both unwedge. We also broadcast here directly to cover the
// window before the pump observes the cancellation.
func (s *vstreamSnapshotStream) close() error {
	if s.grpcCancel != nil {
		s.grpcCancel()
		s.grpcCancel = nil
	}
	if s.cond != nil {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	}
	if s.conn != nil {
		err := s.conn.Close()
		s.conn = nil
		return err
	}
	return nil
}

// vstreamSnapshotRows is the [ir.RowReader] half of the snapshot
// stream. It STREAMS rows from the per-table queue the COPY pump fills
// as they arrive (ADR-0071) rather than serving a fully-buffered slice
// — so a single large table copies in constant memory and target
// writes begin before the COPY phase finishes.
//
// The reader is stateless: ReadRows can be called for any subset of
// the source's tables in any order. Each call drains a table's queue
// and returns an empty channel for unknown tables (callers don't
// always read every table — translation may filter some out).
type vstreamSnapshotRows struct {
	snap *vstreamSnapshotStream
}

// SetMaxBufferBytes implements [ir.MaxBufferBytesSetter] (ADR-0028 /
// ADR-0071). It overrides the byte cap the COPY pump enforces on the
// per-table queue. Zero or negative means "no cap" — the pump never
// backpressures and never refuses (the pre-bounded behaviour, useful
// only when the operator has explicitly opted out of the memory
// bound). Guarded by mu so a late call from the orchestrator races
// cleanly with the already-running pump; the constructor seeds the
// 64 MiB default so the bound holds even when this is never called.
func (r *vstreamSnapshotRows) SetMaxBufferBytes(bytes int64) {
	s := r.snap
	s.mu.Lock()
	defer s.mu.Unlock()
	if bytes <= 0 {
		// "No cap": a value larger than any plausible accumulation.
		s.maxBufferBytes = 1 << 62
	} else {
		s.maxBufferBytes = bytes
	}
	if s.cond != nil {
		s.cond.Broadcast()
	}
}

// Err implements [ir.RowReader]. Rows stream off the per-table queue
// the COPY pump fills; a pump that died mid-COPY records its terminal
// error on the backing snapshot stream. Delegating keeps the loud-
// failure contract (Bug 68) honest for the vstream snapshot path: a
// pump that died mid-COPY surfaces here rather than looking like an
// empty buffer (the streaming ReadRows below closes the channel on a
// terminal pump error so the orchestrator reaches this check).
func (r *vstreamSnapshotRows) Err() error {
	return r.snap.Err()
}

// CopyNeedsIdempotentWriter implements [ir.IdempotentCopyReader]
// (Bug 125). The VStream COPY phase re-emits rows already past the
// scan during binlog catchup, and — because Vitess can order the COPY
// scan by a cheaper unique key than the table's PK — can deliver
// legitimate forward rows out of PK order too. The bulk-copy writer
// must therefore upsert (ON DUPLICATE KEY UPDATE) rather than plain-
// INSERT so those re-emissions absorb instead of colliding on a unique
// key. Returning true makes the orchestrator route the cold-start
// bulk copy through [ir.IdempotentRowWriter.WriteRowsIdempotent].
//
// This replaces the deleted copyDedupTracker: rather than DROP behind-
// the-scan rows (the silent-loss bug), the reader keeps every row and
// the writer absorbs the overlap idempotently.
func (r *vstreamSnapshotRows) CopyNeedsIdempotentWriter() bool { return true }

// ReadRows returns a channel that streams the rows the COPY pump
// captures for table.Name AS THEY ARRIVE, then closes once the table's
// queue is empty and the COPY phase has completed (or the pump hit a
// terminal error, or ctx cancelled). Blocking on an empty-but-not-yet-
// complete queue is the backpressure seam: a slow consumer here stalls
// the pump's enqueue, which stalls Recv, which stalls Vitess.
//
// Returning a nil-table-name table is rejected at the same signature
// point [RowReader.ReadRows] does, so the orchestrator's validation
// looks the same for both flavors.
func (r *vstreamSnapshotRows) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("mysql/vstream: snapshot: ReadRows: table is nil")
	}
	if table.Name == "" {
		return nil, errors.New("mysql/vstream: snapshot: ReadRows: table.Name is empty")
	}

	s := r.snap
	tableName := table.Name

	// Mark this table active so the pump backpressures (rather than
	// refuses) on its over-cap growth while we drain it.
	s.mu.Lock()
	s.activeTable = tableName
	s.cond.Broadcast()
	s.mu.Unlock()

	out := make(chan ir.Row)
	go func() {
		defer close(out)
		defer func() {
			s.mu.Lock()
			if s.activeTable == tableName {
				s.activeTable = ""
			}
			s.cond.Broadcast()
			s.mu.Unlock()
		}()

		for {
			s.mu.Lock()
			// Wait for a row, completion, a terminal error, or
			// cancellation. ctx is polled here (we can't select on a
			// cond) and again on the send below.
			for len(s.rowBuffer[tableName]) == 0 && !s.copyComplete && s.err == nil && ctx.Err() == nil {
				s.cond.Wait()
			}
			queue := s.rowBuffer[tableName]
			if len(queue) == 0 {
				// Empty queue + (complete | error | cancelled): the
				// table is fully delivered. Drop its now-empty entry so
				// a second ReadRows returns immediately.
				delete(s.rowBuffer, tableName)
				s.mu.Unlock()
				return
			}
			// Pop the head row, debit its bytes, wake the pump (which
			// may be backpressured on this table), and release the lock
			// before the (potentially blocking) send. Nil the popped
			// slot so the drained row is GC-eligible immediately rather
			// than pinned by the backing array's head (the queue keeps
			// growing as the pump appends).
			row := queue[0]
			queue[0] = nil
			s.rowBuffer[tableName] = queue[1:]
			s.bufferedBytes -= ir.ApproximateRowBytes(row)
			s.cond.Broadcast()
			s.mu.Unlock()

			select {
			case out <- row:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// vstreamSnapshotChanges is the [ir.CDCReader] half of the snapshot
// stream. StreamChanges starts the pump goroutine that resumes the
// gRPC stream in CDC mode; the from parameter is informational only
// — the position is whatever the COPY phase captured at the global
// COPY_COMPLETED (recorded onto the SnapshotStream by [finishCopy],
// before the orchestrator reads it post-bulk-copy).
type vstreamSnapshotChanges struct {
	snap *vstreamSnapshotStream
}

// StreamChanges returns the channel the pump goroutine writes to.
// from is ignored: VStream resumes from where COPY_COMPLETED left
// off automatically (we never closed the underlying stream), so
// supplying a position would either match (no-op) or contradict
// (silently wrong). The orchestrator passes the captured Position
// here for symmetry with the standalone CDC path; mismatches are
// surfaced as a validation error so misconfigured callers fail
// loudly.
//
// The captured-VGTID read is taken under mu — by the time the
// orchestrator calls this (after bulk-copy → COPY_COMPLETED → copyDone)
// the COPY pump has stopped writing currentVgtid, but reading it under
// the same lock the pump used keeps the comparison race-clean by
// construction rather than by sequencing assumption.
func (c *vstreamSnapshotChanges) StreamChanges(ctx context.Context, from ir.Position) (<-chan ir.Change, error) {
	if from.Engine != "" || from.Token != "" {
		shards, ok, err := decodeVStreamPos(from)
		if err != nil {
			return nil, fmt.Errorf("mysql/vstream: snapshot: StreamChanges: decode from position: %w", err)
		}
		c.snap.mu.Lock()
		captured := c.snap.currentVgtid
		c.snap.mu.Unlock()
		if ok && !sameVgtid(shards, captured) {
			return nil, fmt.Errorf(
				"mysql/vstream: snapshot: StreamChanges: from position %v does not match captured snapshot position %v",
				shards, captured,
			)
		}
	}
	return c.snap.startPump(ctx)
}

// Close is provided so the snapshot's CDC half implements the same
// io.Closer-shaped optional interface the standalone CDC reader does
// — keeps the [Streamer]'s defer chain symmetric. Actual cleanup
// happens via [SnapshotStream.Close]; this is a no-op.
func (c *vstreamSnapshotChanges) Close() error { return nil }

// shardScopeKey is the key shape used in
// [vstreamSnapshotStream.copyCompletedShards]. Combines keyspace and
// shard so two shards with the same name in different keyspaces
// (theoretically possible in a multi-keyspace stream; sluice's v1
// streams a single keyspace) don't collide.
func shardScopeKey(keyspace, shard string) string {
	return keyspace + "/" + shard
}

// sameVgtid is a strict equality check: same shards in the same
// order with the same Gtids. Used only to catch the case where the
// orchestrator passes a position that doesn't correspond to the
// captured snapshot.
func sameVgtid(a, b []shardGtid) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// toProtoShardGtids converts our domain type to the proto type.
// Inverse of [vgtidToShardGtidSlice]; lives here so only one file
// imports binlogdata for request-construction.
func toProtoShardGtids(in []shardGtid) []*binlogdata.ShardGtid {
	out := make([]*binlogdata.ShardGtid, len(in))
	for i, s := range in {
		out[i] = &binlogdata.ShardGtid{
			Keyspace: s.Keyspace,
			Shard:    s.Shard,
			Gtid:     s.Gtid,
		}
	}
	return out
}
