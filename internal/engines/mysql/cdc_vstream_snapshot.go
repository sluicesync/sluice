// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

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
	// fromBeginningVStreamPos carries no TablePKs (empty-Gtid sentinel,
	// no mid-COPY cursor), so toProtoShardGtids can't fail here — but it
	// returns an error in the general case (Phase A), so we handle it.
	protoShardGtids, err := toProtoShardGtids(fromBeginningVStreamPos(keyspace, shards))
	if err != nil {
		streamCancel()
		_ = conn.Close()
		return nil, fmt.Errorf("mysql/vstream: snapshot: build start position: %w", err)
	}
	req := &vtgate.VStreamRequest{
		TabletType: topodata.TabletType_PRIMARY,
		Vgtid: &binlogdata.VGtid{
			ShardGtids: protoShardGtids,
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
		keyspace:             keyspace,
		client:               client,
		shards:               shards,
		reconnectMax:         defaultCopyReconnectMax,
		reconnectBackoffBase: defaultCopyReconnectBackoffBase,
		reconnectBackoffCap:  defaultCopyReconnectBackoffCap,
		fields:               make(map[string][]*query.Field),
		rowBuffer:            make(map[string][]ir.Row),
		copyCompletedShards:  make(map[string]bool),
		maxBufferBytes:       defaultSnapshotMaxBufferBytes,
		checkpointRows:       defaultCopyCheckpointRows,
		checkpointInterval:   defaultCopyCheckpointInterval,
		conn:                 conn,
		grpcStream:           grpcStream,
		grpcCancel:           streamCancel,
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

	// client is the typed Vitess gRPC client. Held so the COPY pump can
	// re-open the VStream IN PLACE on a retriable Recv error (ADR-0072
	// Phase C) — reusing the same underlying conn but replacing the
	// stream — without unwinding the whole cold-start to runWithRetry.
	client vtgateservice.VitessClient

	// shards is the shard layout the snapshot streams. Captured at open
	// so an in-place reconnect (Phase C) can rebuild the request; the
	// resume request's per-shard Gtid + TablePKs come from currentVgtid,
	// but the shard list itself is the constructor's resolved layout.
	shards []string

	// reconnectMax is the in-place COPY-reconnect budget (Phase C):
	// consecutive retriable Recv failures the pump absorbs before giving
	// up and failing the COPY (the outer runWithRetry then becomes the
	// backstop). reconnectBackoffBase/Cap bound the exponential backoff
	// between attempts. Seeded with safe defaults by the constructor.
	reconnectMax         int
	reconnectBackoffBase time.Duration
	reconnectBackoffCap  time.Duration

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

	// checkpointFn is the durable COPY-cursor sink (ADR-0072 Phase B).
	// nil until the pipeline wires it via [vstreamSnapshotRows.
	// SetCopyCheckpoint] (before bulk-copy drains the stream). When set,
	// the COPY pump persists currentVgtid (including its TablePKs resume
	// cursor) to the control table on a bounded cadence so a post-fault
	// resume reads the checkpoint instead of restarting from row 0.
	// Read by the pump goroutine under mu; the actual DB write happens
	// OUTSIDE mu (the pump is the sole writer of currentVgtid during
	// COPY, so it snapshots the position under mu then writes unlocked).
	checkpointFn ir.CopyCheckpointFunc

	// checkpointRows / checkpointInterval are the bounded cadence: the
	// pump checkpoints after either checkpointRows COPY rows have been
	// buffered since the last checkpoint OR checkpointInterval has
	// elapsed, whichever comes first. Seeded with safe defaults by the
	// constructor.
	checkpointRows     int
	checkpointInterval time.Duration

	// rowsSinceCheckpoint counts COPY rows buffered since the last
	// successful checkpoint (the N-rows half of the cadence). Mutated by
	// the pump under mu. lastCheckpoint is the wall-clock time of the
	// last checkpoint (the T-seconds half); the pump owns it (no lock
	// needed — single goroutine), seeded at copyPump start.
	rowsSinceCheckpoint int

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

// defaultCopyCheckpointRows / defaultCopyCheckpointInterval are the
// bounded cadence (ADR-0072 Phase B) at which the COPY pump persists the
// resume cursor: whichever of "this many rows buffered" or "this much
// wall-clock elapsed" comes first. The row count bounds write
// amplification (one control-table upsert per 50k rows rather than per
// row — the rejected "persist every row" alternative); the interval
// bounds data-loss-on-fault for slow/idle copies to one interval. Both
// are conservative defaults; the cursor itself is correct at any
// cadence, these only trade resume-granularity against write traffic.
const (
	defaultCopyCheckpointRows     = 50_000
	defaultCopyCheckpointInterval = 10 * time.Second
)

// defaultCopyReconnect* tune the in-place COPY-reconnect (ADR-0072
// Phase C): on a retriable Recv error the COPY pump re-opens the VStream
// from the last-observed cursor up to defaultCopyReconnectMax times,
// with exponential backoff bounded by base/cap, before surfacing the
// error to the outer runWithRetry backstop. The budget is generous
// because each in-place reconnect is cheap (no pipeline teardown) and
// the reported failure mode is sustained link flakiness on a large copy.
const (
	defaultCopyReconnectMax         = 10
	defaultCopyReconnectBackoffBase = 200 * time.Millisecond
	defaultCopyReconnectBackoffCap  = 10 * time.Second
)

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

	// lastCheckpoint is the wall-clock anchor for the T-seconds half of
	// the cadence. Owned by this goroutine (the sole checkpoint caller),
	// so it needs no lock.
	lastCheckpoint := time.Now()

	// reconnectAttempts counts consecutive in-place reconnects (Phase C)
	// since the last successful Recv. Reset to 0 on any successful Recv so
	// a copy that survives one blip gets the full budget again for the
	// next; exhausting it surfaces the error to the outer runWithRetry
	// backstop. Owned by this goroutine.
	reconnectAttempts := 0

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
			classified := classifyReaderError(fmt.Errorf("mysql/vstream: snapshot: copy recv: %w", err))
			// Phase C: a retriable mid-COPY Recv error reconnects the
			// VStream IN PLACE from the last-observed cursor (currentVgtid,
			// carrying TablePKs) rather than failing the whole cold-start.
			// In-place reconnect keeps the bulk-copy goroutines warm and
			// skips schema-apply / pre-flight; runWithRetry stays the outer
			// backstop for budget exhaustion or non-retriable shapes.
			var re ir.RetriableError
			if errors.As(classified, &re) && reconnectAttempts < s.reconnectMax {
				reconnectAttempts++
				if rerr := s.reconnectCopy(ctx, reconnectAttempts); rerr != nil {
					s.failCopy(rerr)
					return
				}
				// Fresh stream installed; loop and Recv from it.
				continue
			}
			s.failCopy(classified)
			return
		}
		reconnectAttempts = 0
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
		// Bounded-cadence COPY checkpoint (ADR-0072 Phase B), between
		// Recv batches so the DB write never holds the dispatch lock. A
		// checkpoint failure is non-fatal: the COPY itself is fine, and
		// the final position is still persisted at COPY_COMPLETED — we
		// just lose this intermediate resume point. Log-and-continue
		// rather than failCopy so a flaky control-table write can't abort
		// an otherwise-healthy snapshot.
		lastCheckpoint = s.maybeCheckpoint(ctx, lastCheckpoint)
	}
}

// reconnectCopy re-opens the VStream IN PLACE after a retriable mid-COPY
// Recv error (ADR-0072 Phase C), resuming from the last-observed cursor
// (currentVgtid, carrying TablePKs) so vtgate continues the COPY scan
// from the last-copied PK rather than restarting from row 0. The
// underlying gRPC conn is reused; only the stream is replaced.
//
// The resume request carries each shard's Gtid AND TablePKs as observed
// so far. When no VGTID has been seen yet (a fault before the first
// LASTPK), currentVgtid is empty and we fall back to the from-beginning
// sentinel — i.e. restart the COPY, which is the only correct option
// when there's no cursor to resume from.
//
// Backoff is exponential, bounded by reconnectBackoffBase/Cap, and
// interruptible by ctx (CloseFn cancels the stream ctx; a parked
// reconnect must unwedge). attempt is 1-based for the backoff scaling
// and the log line.
//
// The new stream is installed on s.grpcStream under mu — the field is
// read by this same pump goroutine (the only Recv caller during COPY),
// but ReadRows/close may observe it, so the write is guarded.
func (s *vstreamSnapshotStream) reconnectCopy(ctx context.Context, attempt int) error {
	// Backoff before re-dialing. Exponential on the attempt count,
	// capped. Interruptible so close() during a flaky window doesn't hang.
	backoff := s.reconnectBackoffBase << (attempt - 1)
	if backoff <= 0 || backoff > s.reconnectBackoffCap {
		backoff = s.reconnectBackoffCap
	}
	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		return ctx.Err()
	}

	// Snapshot the resume cursor under mu (the pump is the sole writer of
	// currentVgtid, but reading it under the same lock keeps the contract
	// uniform).
	s.mu.Lock()
	resume := make([]shardGtid, len(s.currentVgtid))
	copy(resume, s.currentVgtid)
	s.mu.Unlock()

	if len(resume) == 0 {
		// No cursor observed yet — the only correct resume is a full
		// restart from the beginning of the configured shard layout.
		resume = fromBeginningVStreamPos(s.keyspace, s.shards)
	}

	protoShardGtids, err := toProtoShardGtids(resume)
	if err != nil {
		return fmt.Errorf("mysql/vstream: snapshot: copy reconnect: build resume position: %w", err)
	}
	req := &vtgate.VStreamRequest{
		TabletType: topodata.TabletType_PRIMARY,
		Vgtid:      &binlogdata.VGtid{ShardGtids: protoShardGtids},
		Filter:     &binlogdata.Filter{Rules: []*binlogdata.Rule{{Match: "/.*/"}}},
		Flags: &vtgate.VStreamFlags{
			MinimizeSkew:      true,
			StopOnReshard:     true,
			HeartbeatInterval: 5,
		},
	}

	slog.WarnContext(ctx, "mysql/vstream: snapshot: COPY stream dropped; reconnecting in place from cursor",
		slog.Int("attempt", attempt),
		slog.Int("max_attempts", s.reconnectMax),
		slog.Duration("backoff", backoff))

	grpcStream, err := s.client.VStream(ctx, req)
	if err != nil {
		return fmt.Errorf("mysql/vstream: snapshot: copy reconnect: open stream: %w", err)
	}
	s.mu.Lock()
	s.grpcStream = grpcStream
	s.mu.Unlock()
	return nil
}

// maybeCheckpoint persists the current COPY cursor to the durable
// control table when the bounded cadence (N rows OR T seconds) is due.
// Returns the (possibly updated) lastCheckpoint anchor. The pump is the
// sole caller and the sole writer of currentVgtid during COPY, so it
// snapshots the position under mu, then releases the lock BEFORE the DB
// write — the write must not block ReadRows consumers or the cond-wait
// backpressure path. A nil checkpointFn (pipeline never wired a sink, or
// a non-cold-start path) is a no-op.
func (s *vstreamSnapshotStream) maybeCheckpoint(ctx context.Context, lastCheckpoint time.Time) time.Time {
	s.mu.Lock()
	fn := s.checkpointFn
	if fn == nil {
		s.mu.Unlock()
		return lastCheckpoint
	}
	rowsDue := s.checkpointRows > 0 && s.rowsSinceCheckpoint >= s.checkpointRows
	timeDue := s.checkpointInterval > 0 && time.Since(lastCheckpoint) >= s.checkpointInterval &&
		s.rowsSinceCheckpoint > 0
	if !rowsDue && !timeDue {
		s.mu.Unlock()
		return lastCheckpoint
	}
	// Snapshot the position under the lock; encode while holding it is
	// cheap and keeps the read consistent with the pump's own writes.
	if len(s.currentVgtid) == 0 {
		// No VGTID observed yet (no cursor to persist); reset the row
		// counter so we don't spin re-evaluating the same empty state.
		s.rowsSinceCheckpoint = 0
		s.mu.Unlock()
		return time.Now()
	}
	pos, err := encodeVStreamPos(s.currentVgtid)
	if err != nil {
		// A position that won't encode is the same terminal condition
		// finishCopy guards against; surface it loudly rather than
		// silently skipping every checkpoint.
		s.rowsSinceCheckpoint = 0
		s.mu.Unlock()
		s.failCopy(fmt.Errorf("mysql/vstream: snapshot: checkpoint: encode position: %w", err))
		return time.Now()
	}
	s.rowsSinceCheckpoint = 0
	s.mu.Unlock()

	if err := fn(ctx, pos); err != nil {
		// Non-fatal: log and keep copying. The cursor is re-attempted on
		// the next cadence tick, and COPY_COMPLETED persists the final
		// position regardless.
		slog.WarnContext(ctx, "mysql/vstream: snapshot: COPY checkpoint write failed; continuing",
			slog.String("error", err.Error()))
	}
	return time.Now()
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
		// ADR-0073 (c): drop FIELD events for Vitess-internal tables
		// (online-DDL shadows like `_vt_vrp_*`, GC-renamed tables, …)
		// during the COPY phase. vtgate streams them under the `/.*/`
		// filter, but they are never logical user tables — caching their
		// field metadata is the precursor to bufferCopyRow buffering
		// their rows (the Bug-125 leak). Strip the keyspace prefix first.
		if isVitessInternalTable(stripKeyspaceFromTable(fe.GetTableName(), fe.GetKeyspace())) {
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
		// During COPY, each VGTID after a LASTPK event carries the
		// per-shard TablePKs cursor (ADR-0072 Phase A) — capturing it
		// here is what lets a post-fault resume continue the COPY scan
		// from the last-copied PK rather than restarting from row 0.
		next, err := vgtidToShardGtidSlice(vg)
		if err != nil {
			return false, err
		}
		s.currentVgtid = next
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
	// ADR-0073 (c): drop COPY rows for Vitess-internal tables BEFORE
	// buffering. This is the exact Bug-125 choke point — the probe
	// reproduced an in-flight online DDL's `_vt_vrp_*` shadow being
	// buffered here, which then tripped the ADR-0071 scope-name-mismatch
	// loud refusal (enqueueRowLocked's activeTable guard) and aborted the
	// cold-start with zero rows. Skipping before buffering means the
	// shadow never enters rowBuffer, never sets activeTable, and can
	// never fire that refusal. Their FIELD events were already dropped
	// (dispatchCopyEventLocked's FIELD branch), so an internal ROW would
	// otherwise trip the "row event without preceding FIELD event" floor
	// below; this skip precedes the field lookup so that floor stays
	// reserved for genuine logical-table bugs. Strip the keyspace prefix
	// first — the internal-table naming is on the table component.
	if isVitessInternalTable(stripKeyspaceFromTable(rev.GetTableName(), rev.GetKeyspace())) {
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
	// Count this row toward the N-rows half of the checkpoint cadence
	// (ADR-0072 Phase B). The pump reads + resets this under mu in
	// maybeCheckpoint between Recv iterations.
	s.rowsSinceCheckpoint++
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
		// ADR-0073 (c): drop FIELD events for Vitess-internal tables in
		// the post-COPY CDC phase too — a steady-state online DDL emits
		// the shadow's FIELD/ROW events here, not just during COPY.
		// Symmetric with vstreamCDCReader.dispatch's FIELD branch.
		if isVitessInternalTable(stripKeyspaceFromTable(fe.GetTableName(), fe.GetKeyspace())) {
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
		next, err := vgtidToShardGtidSlice(vg)
		if err != nil {
			return err
		}
		s.currentVgtid = next
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
	// ADR-0073 (c): drop ROW events for Vitess-internal tables before the
	// FIELD lookup (their FIELD was already dropped above, so this also
	// keeps the missing-FIELD floor reserved for logical-table bugs).
	if isVitessInternalTable(stripKeyspaceFromTable(rev.GetTableName(), rev.GetKeyspace())) {
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

	// ADR-0073 (c) cutover survival: skip internal shadow-table DDLs so
	// they don't invalidate the logical field cache. Symmetric with
	// vstreamCDCReader.dispatchDDL; see isVitessInternalDDL.
	if isVitessInternalDDL(stmt) {
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

// SetCopyCheckpoint implements [ir.CopyCheckpointer] (ADR-0072 Phase B).
// The pipeline wires the durable position sink here on the cold-start
// path, BEFORE bulk-copy drains the stream, so the COPY pump persists
// the resume cursor (currentVgtid, including its TablePKs) to the
// control table on the bounded cadence. A nil fn disables checkpointing
// (the pre-ADR-0072 behaviour: position persisted only at
// COPY_COMPLETED). Guarded by mu so the late wire races cleanly with the
// already-running pump.
func (r *vstreamSnapshotRows) SetCopyCheckpoint(fn ir.CopyCheckpointFunc) {
	s := r.snap
	s.mu.Lock()
	s.checkpointFn = fn
	s.mu.Unlock()
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
// order with the same (keyspace, shard, gtid). Used only to catch the
// case where the orchestrator passes a position that doesn't correspond
// to the captured snapshot.
//
// The per-shard TablePKs cursor (ADR-0072 Phase A) is intentionally NOT
// compared: it is the transient COPY-resume cursor, empty once COPY
// completes, and the captured snapshot position this guard checks is the
// COPY_COMPLETED anchor (TablePKs already drained). Comparing it would
// also require deep-equality — shardGtid now holds a slice and is no
// longer comparable with ==. The GTID is the load-bearing identity here.
func sameVgtid(a, b []shardGtid) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Keyspace != b[i].Keyspace || a[i].Shard != b[i].Shard || a[i].Gtid != b[i].Gtid {
			return false
		}
	}
	return true
}

// toProtoShardGtids converts our domain type to the proto type.
// Inverse of [vgtidToShardGtidSlice]; lives here so only one file
// imports binlogdata for request-construction.
//
// TablePKs (the COPY-resume cursor, ADR-0072 Phase A) are decoded back
// into the proto so a resume request asks vtgate to continue the COPY
// scan from the last-copied PK rather than restarting from row 0. The
// decode can fail only on a corrupt persisted token (bad base64 or a
// TableLastPK that won't unmarshal), surfaced as an error so a wedged
// position fails loudly at stream-open rather than silently restarting
// the whole table copy.
func toProtoShardGtids(in []shardGtid) ([]*binlogdata.ShardGtid, error) {
	out := make([]*binlogdata.ShardGtid, len(in))
	for i, s := range in {
		pks, err := decodeTablePKs(s.TablePKs)
		if err != nil {
			return nil, err
		}
		out[i] = &binlogdata.ShardGtid{
			Keyspace: s.Keyspace,
			Shard:    s.Shard,
			Gtid:     s.Gtid,
			TablePKs: pks,
		}
	}
	return out, nil
}
