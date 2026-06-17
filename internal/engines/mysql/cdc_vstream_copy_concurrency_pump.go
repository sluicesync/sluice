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

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"

	"sluicesync.dev/sluice/internal/ir"
)

// Concurrent cross-table VStream COPY pump (ADR-0099). When the operator
// opts into K > 1 concurrent streams (vstream_copy_table_parallelism), the
// auto-shard cold-copy runs K INDEPENDENT single-table COPY sub-sequences,
// each over a disjoint group of the in-scope tables on its OWN vtgate
// VStream. Each sub-sequence is exactly the ADR-0095 per-table loop
// restricted to its group; every per-table snapshot from every stream
// flows into the SHARED perTableSnapshots, and after ALL K streams finish
// ALL their tables the driver stitches the per-shard GTID-set minimum
// across the UNION (stitchSnapshotMin, parallelism-agnostic) — the single
// gapless CDC-resume position.
//
// Why a separate pump (not a parameterization of copyPumpAutoShard): the
// sequential path's per-table helpers (pumpOneTableCopy, reopenForTable,
// reconnectCopy) read+write the SHARED s.grpcStream / s.currentVgtid /
// s.fields / s.posBreadcrumbs directly — single-pump state. Running K of
// them concurrently would race on those fields. The concurrent pump gives
// each stream its OWN copyStream (gRPC stream + cursor + field cache), so
// the per-stream state is isolated; only the genuinely shared state
// (rowBuffer keyed by DISJOINT table, perTableSnapshots, tableCopyComplete,
// copyComplete, mu/cond) is touched under the parent's lock. K = 1 never
// reaches here — it stays on the byte-identical sequential copyPumpAutoShard.

// copyStream is one of the K concurrent COPY streams (ADR-0099). It owns
// its own vtgate VStream and per-stream cursor/field-cache state, and
// copies its assigned disjoint table group one table at a time (the
// ADR-0095 constant-memory per-table shape, per stream). It appends rows to
// the PARENT's shared rowBuffer (keyed by table; disjoint partition ⇒ each
// table queue has exactly one producing copyStream), and records each
// completed table's snapshot into the parent's shared perTableSnapshots
// under the parent mu.
type copyStream struct {
	parent *vstreamSnapshotStream

	// idx is the stream's index in [0, K) — used only for log context.
	idx int

	// tables is this stream's disjoint table group, copied one at a time.
	tables []string

	// grpcStream is this stream's OWN vtgate VStream (its own logical
	// gRPC stream over the parent's shared *grpc.ClientConn — gRPC
	// multiplexes K concurrent streams over one HTTP/2 connection, so K
	// streams cost one TCP/TLS connection, K logical streams). Replaced in
	// place by reopenStreamForTable / reconnectStream; this goroutine is
	// the sole Recv caller, so it needs no lock for its own access, but
	// close() never touches it (only the parent grpcCancel tears the whole
	// conn down).
	grpcStream vtgateservice.Vitess_VStreamClient

	// cur is this stream's current per-shard VGTID cursor (the table in
	// flight). Reset between tables. Stream-owned (sole pump goroutine).
	cur []shardGtid

	// fields is this stream's field cache (keyed by fieldCacheKey). A
	// per-stream cache because each stream's single-table COPY re-emits its
	// own FIELD events; sharing the parent's would race K writers.
	// Stream-owned.
	fields map[string][]*query.Field

	// resumeSeed / resumeSeedTable carry the ADR-0098 mid-COPY cursor for
	// the ONE in-progress table THIS stream's group contains (empty on a
	// fresh cold-start, or when the in-progress table belongs to a
	// different stream's group). Set by the driver at partition time.
	resumeSeed      []shardGtid
	resumeSeedTable string
}

// copyPumpAutoShardConcurrent is the K > 1 concurrent driver (ADR-0099). It
// partitions the in-scope tables into K disjoint groups, spawns one
// copyStream pump per group on its own VStream, joins all K, then stitches
// the per-shard set-min across the UNION of every stream's per-table
// snapshots and records the single CDC-resume position — only after ALL K
// streams complete ALL their tables (ADR-0007: the global position never
// advances past an incompletely-copied table).
//
// Any stream's failure cancels the shared copy context, aborting the other
// K-1 streams LOUDLY with no global position advance. ctx-cancel (CloseFn)
// unblocks every parked Recv; the WaitGroup join guarantees no leaked
// goroutine and the parent close() tears down the shared conn (and thus
// every stream).
func (s *vstreamSnapshotStream) copyPumpAutoShardConcurrent(ctx context.Context, cancel context.CancelFunc, stream *ir.SnapshotStream, groups [][]string) {
	defer close(s.copyDone)

	// Place the ADR-0098 resume seed into the ONE group whose table set
	// contains the in-progress table the persisted cursor names. The other
	// groups re-copy their tables fresh (idempotent upsert absorbs any
	// overlap). resolveResumeAutoShard already validated, before the
	// partition, that the cursor names exactly one in-scope table.
	seeds := make([]*copyStream, len(groups))
	for g := range groups {
		cs := &copyStream{
			parent: s,
			idx:    g,
			tables: groups[g],
			fields: make(map[string][]*query.Field),
		}
		if s.resumeSeedTable != "" {
			for _, t := range groups[g] {
				if t == s.resumeSeedTable {
					cs.resumeSeed = s.resumeSeed
					cs.resumeSeedTable = s.resumeSeedTable
					break
				}
			}
		}
		seeds[g] = cs
	}

	slog.InfoContext(ctx, "mysql/vstream: snapshot: cross-table concurrent COPY (ADR-0099)",
		slog.String("keyspace", s.keyspace),
		slog.Int("streams", len(groups)),
		slog.Int("tables", len(s.copyTablesSeq)))

	var wg sync.WaitGroup
	for g := range seeds {
		wg.Add(1)
		go func(cs *copyStream) {
			defer wg.Done()
			if err := cs.run(ctx, cancel); err != nil {
				// First error wins (failCopy), and cancel the shared copy
				// context so the other streams unwind. failCopy flips
				// copyComplete + broadcasts, so blocked ReadRows consumers
				// wake and surface the error (Bug 68 loud-failure gate).
				s.failCopy(err)
				cancel()
			}
		}(seeds[g])
	}
	wg.Wait()

	// All K streams joined. finishCopyAutoShard stitches the per-shard
	// set-min across the SHARED perTableSnapshots (the union of every
	// stream's captured per-table P_i) and records the position — only when
	// no stream errored (it no-ops the position write on s.err != nil).
	s.finishCopyAutoShard(stream)
}

// run drives this stream's disjoint table group one table at a time (the
// ADR-0095 per-table shape, isolated to per-stream state). It opens the
// first table's COPY, drains it to COPY_COMPLETED, records the snapshot,
// reopens for the next table, and so on. The in-progress table named by an
// ADR-0098 resume seed is opened SEEDED from its cursor; every other table
// (in this group) opens from-beginning.
func (cs *copyStream) run(ctx context.Context, cancel context.CancelFunc) error {
	s := cs.parent
	for i, table := range cs.tables {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Open this table's single-table COPY. The first table in the group
		// has no stream yet; subsequent tables reopen in place. The resume
		// table opens seeded from its cursor (no row-0 restart) even when it
		// is the group's first table.
		seed := []shardGtid(nil)
		if cs.resumeSeedTable != "" && table == cs.resumeSeedTable {
			seed = cs.resumeSeed
		}
		if i == 0 && seed == nil {
			// Group's first table, fresh: open from-beginning.
			if err := cs.openStreamForTable(ctx, table, nil); err != nil {
				return err
			}
		} else {
			if err := cs.openStreamForTable(ctx, table, seed); err != nil {
				return err
			}
		}

		snap, err := cs.pumpTable(ctx, cancel, table)
		if err != nil {
			return err
		}

		// Record this table's snapshot into the SHARED perTableSnapshots and
		// signal the table complete so the orchestrator's ReadRows for it
		// closes. The partition is disjoint, so no other stream touches this
		// table's queue or completion flag.
		s.mu.Lock()
		s.perTableSnapshots = append(s.perTableSnapshots, snap)
		s.tableCopyComplete[table] = true
		s.broadcast()
		s.mu.Unlock()

		// Reset per-stream cursor + field cache for the next table (each
		// single-table COPY is independent and captures its own P).
		cs.cur = nil
		clear(cs.fields)
	}
	return nil
}

// openStreamForTable opens (or reopens in place) this stream's vtgate
// VStream scoped to a single table. seed nil ⇒ from-beginning (fresh
// table); non-nil ⇒ the ADR-0098 resume cursor (continue the scan from the
// last-copied PK). Reuses the parent's shared gRPC connection + client
// (gRPC multiplexes the K logical streams over it).
func (cs *copyStream) openStreamForTable(ctx context.Context, table string, seed []shardGtid) error {
	s := cs.parent
	startPos := seed
	if startPos == nil {
		startPos = fromBeginningVStreamPos(s.keyspace, s.shards)
	}
	protoShardGtids, err := toProtoShardGtids(startPos)
	if err != nil {
		return fmt.Errorf("mysql/vstream: snapshot: concurrent COPY: build start position for %q: %w", table, err)
	}
	req := &vtgate.VStreamRequest{
		TabletType: topodata.TabletType_PRIMARY,
		Vgtid:      &binlogdata.VGtid{ShardGtids: protoShardGtids},
		Filter:     &binlogdata.Filter{Rules: vstreamCopyFilterRules([]string{table})},
		Flags: &vtgate.VStreamFlags{
			MinimizeSkew:      true,
			StopOnReshard:     true,
			HeartbeatInterval: 5,
		},
	}
	grpcStream, err := s.client.VStream(ctx, req)
	if err != nil {
		return fmt.Errorf("mysql/vstream: snapshot: concurrent COPY: open stream %d for %q: %w", cs.idx, table, err)
	}
	cs.grpcStream = grpcStream
	if seed != nil {
		slog.DebugContext(ctx, "mysql/vstream: snapshot: concurrent COPY resumed in-progress table from cursor",
			slog.Int("stream", cs.idx), slog.String("table", table))
	} else {
		slog.DebugContext(ctx, "mysql/vstream: snapshot: concurrent COPY advanced to table",
			slog.Int("stream", cs.idx), slog.String("table", table))
	}
	return nil
}

// pumpTable Recv-drives this stream's currently-open single-table COPY
// until its (global) COPY_COMPLETED, returning the table's snapshot VGTID.
// It mirrors pumpOneTableCopy's liveness watchdog + in-place reconnect, but
// operates on PER-STREAM state (cs.grpcStream / cs.cur / cs.fields) and
// enqueues into the parent's SHARED rowBuffer.
//
// The mid-COPY durable-write checkpoint (ADR-0072 Phase B) is DELIBERATELY
// NOT run here: under K concurrent producers the flat durable-row frontier
// is not order-equivalent to any single stream's enqueue frontier (the same
// reason ADR-0097 §3 disables the watermark under write fan-out). A
// concurrent copy therefore persists NO mid-COPY cursor; on resume each
// stream re-copies its group from-beginning (idempotent upsert absorbs the
// overlap — ADR-0098's re-copy-the-prefix decision, per stream). The
// whole-copy join + the final stitched position is the sole durability
// guarantee for a concurrent copy.
func (cs *copyStream) pumpTable(ctx context.Context, cancel context.CancelFunc, table string) ([]shardGtid, error) {
	s := cs.parent
	// On a liveness/progress timeout, record the loud error AND cancel the
	// shared copy context so every parked Recv across all K streams unblocks
	// (the wedge unwinds the whole concurrent copy, not just this stream).
	live := startVStreamLiveness(ctx, s.livenessWindow, s.copyProgressWindow, s.idleWarnWindow,
		func() {
			s.setErr(vstreamLivenessTimeoutError(s.livenessWindow, topodata.TabletType_PRIMARY, s.keyspace, s.shards))
			cancel()
		},
		func() {
			s.setErr(vstreamProgressTimeoutError(s.copyProgressWindow, topodata.TabletType_PRIMARY, s.keyspace, s.shards))
			cancel()
		},
		func() {
			slog.WarnContext(ctx, vstreamIdleWarnMessage(s.idleWarnWindow, s.keyspace, s.shards))
		})
	defer live.stop()

	reconnectAttempts := 0
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		resp, err := cs.grpcStream.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			classified := classifyReaderError(fmt.Errorf("mysql/vstream: snapshot: concurrent copy recv (stream %d, table %q): %w", cs.idx, table, err))
			var re ir.RetriableError
			if errors.As(classified, &re) && reconnectAttempts < s.reconnectMax {
				reconnectAttempts++
				if rerr := cs.reconnectStream(ctx, table, reconnectAttempts); rerr != nil {
					return nil, rerr
				}
				continue
			}
			return nil, classified
		}
		if evs := resp.GetEvents(); len(evs) > 0 {
			live.observe(eventsProveLiveness(evs))
		}
		copiedRow := false
		for _, ev := range resp.GetEvents() {
			if ev.GetType() == binlogdata.VEventType_ROW {
				copiedRow = true
			}
			done, derr := cs.dispatchCopyEvent(table, ev)
			if derr != nil {
				return nil, derr
			}
			if done {
				snap := make([]shardGtid, len(cs.cur))
				copy(snap, cs.cur)
				if len(snap) == 0 {
					snap = fromBeginningVStreamPos(s.keyspace, s.shards)
				}
				return snap, nil
			}
		}
		if copiedRow {
			reconnectAttempts = 0
		}
	}
}

// reconnectStream re-opens this stream's VStream IN PLACE after a retriable
// Recv error, resuming from cs.cur (the last-observed cursor for the table
// in flight). Mirrors reconnectCopy but on per-stream state. Backoff is
// exponential, bounded, and ctx-interruptible.
func (cs *copyStream) reconnectStream(ctx context.Context, table string, attempt int) error {
	s := cs.parent
	backoff := s.reconnectBackoffBase << (attempt - 1)
	if backoff <= 0 || backoff > s.reconnectBackoffCap {
		backoff = s.reconnectBackoffCap
	}
	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		return ctx.Err()
	}
	resume := cs.cur
	if len(resume) == 0 {
		resume = fromBeginningVStreamPos(s.keyspace, s.shards)
	}
	slog.WarnContext(ctx, "mysql/vstream: snapshot: concurrent COPY stream dropped; reconnecting in place from cursor",
		slog.Int("stream", cs.idx), slog.String("table", table),
		slog.Int("attempt", attempt), slog.Int("max_attempts", s.reconnectMax))
	return cs.openStreamForTable(ctx, table, resume)
}

// dispatchCopyEvent routes one COPY-phase VEvent for this stream. Returns
// done=true at the (single-table) global COPY_COMPLETED. It mirrors
// dispatchCopyEventLocked but writes PER-STREAM state (cs.fields / cs.cur)
// and enqueues rows into the parent's SHARED rowBuffer under the parent mu.
// No mid-COPY checkpoint breadcrumb is recorded (see pumpTable).
func (cs *copyStream) dispatchCopyEvent(table string, ev *binlogdata.VEvent) (bool, error) {
	s := cs.parent
	switch ev.GetType() {
	case binlogdata.VEventType_FIELD:
		fe := ev.GetFieldEvent()
		if fe == nil {
			return false, nil
		}
		if isVitessInternalTable(stripKeyspaceFromTable(fe.GetTableName(), fe.GetKeyspace())) {
			return false, nil
		}
		cs.fields[fieldCacheKey(fe.GetShard(), fe.GetTableName())] = fe.GetFields()
		return false, nil

	case binlogdata.VEventType_ROW:
		return false, cs.bufferRow(table, ev)

	case binlogdata.VEventType_VGTID:
		vg := ev.GetVgtid()
		if vg == nil {
			return false, nil
		}
		next, err := vgtidToShardGtidSlice(vg)
		if err != nil {
			return false, err
		}
		cs.cur = next
		return false, nil

	case binlogdata.VEventType_COPY_COMPLETED:
		// The single-table COPY emits exactly one global COPY_COMPLETED
		// (empty keyspace+shard) as its terminator. A per-scope event
		// (populated keyspace/shard) is a progress marker — record it on the
		// shared set for visibility, but don't terminate.
		if ev.GetKeyspace() == "" && ev.GetShard() == "" {
			return true, nil
		}
		s.mu.Lock()
		if s.copyCompletedShards == nil {
			s.copyCompletedShards = make(map[string]bool)
		}
		s.copyCompletedShards[shardScopeKey(ev.GetKeyspace(), ev.GetShard())] = true
		s.mu.Unlock()
		return false, nil

	case binlogdata.VEventType_JOURNAL:
		// Reshard during a concurrent COPY: surface the typed error loudly.
		// In-place multi-stream reshard recovery is out of scope (same v1
		// stance as the sequential COPY's JOURNAL branch).
		return false, journalToShardLayoutErr(ev.GetJournal())

	default:
		return false, nil
	}
}

// bufferRow decodes a COPY-phase ROW for this stream's table and appends
// each row to the parent's SHARED rowBuffer under the parent mu/cond. The
// partition is disjoint, so this table's queue has exactly this stream as
// its producer — but the byte cap is shared across all K streams' in-flight
// queues, so the enqueue backpressures (cond.Wait) against the SHARED
// bufferedBytes, which the K consumers' ReadRows debits relieve. This is
// where K × one-table memory is bounded by K × the per-stream slack under
// one shared cap.
func (cs *copyStream) bufferRow(table string, ev *binlogdata.VEvent) error {
	s := cs.parent
	rev := ev.GetRowEvent()
	if rev == nil {
		return nil
	}
	if isVitessInternalTable(stripKeyspaceFromTable(rev.GetTableName(), rev.GetKeyspace())) {
		return nil
	}
	fields, ok := cs.fields[fieldCacheKey(rev.GetShard(), rev.GetTableName())]
	if !ok {
		return fmt.Errorf("mysql/vstream: snapshot: concurrent COPY: row event for %q without preceding FIELD event", table)
	}
	tableName := stripKeyspaceFromTable(rev.GetTableName(), rev.GetKeyspace())
	for _, rc := range rev.GetRowChanges() {
		row, ok, err := decodeVStreamRow(rc.GetAfter(), fields, tableName, s.boolWarn)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := s.enqueueConcurrentRowLocked(tableName, row); err != nil {
			return err
		}
	}
	return nil
}

// enqueueConcurrentRowLocked appends one COPY row to tableName's SHARED
// queue under the SHARED byte cap (ADR-0099). It is the concurrent-pump
// analogue of enqueueRowLocked, taking the parent mu itself (the sequential
// enqueueRowLocked assumes the caller already holds mu via dispatchCopyEvent;
// here each stream's pump dispatches without holding the lock, so we acquire
// it here).
//
// Backpressure: when the append would push the SHARED bufferedBytes over the
// cap, it waits on cond until a consumer drains enough — UNLESS this table's
// own queue is empty (bufferedBytes is over cap because OTHER streams' tables
// are buffered): in auto-shard / concurrent mode there is no cross-table
// loud-refusal (every buffered table has, or imminently will have, a
// draining consumer — the orchestrator drains every table), so waiting is
// always correct. The ADR-0071 cross-table refusal is the single-stream
// INTERLEAVE guard only; it never fires here (copyTablesSeq is non-empty).
//
// A shutdown (s.err / s.copyComplete flipped by close/failCopy) unwedges a
// parked wait loudly.
func (s *vstreamSnapshotStream) enqueueConcurrentRowLocked(tableName string, row ir.Row) error {
	rowBytes := ir.ApproximateRowBytes(row)
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.bufferedBytes > 0 && s.bufferedBytes+rowBytes > s.maxBufferBytes {
		if s.err != nil {
			return s.err
		}
		if s.copyComplete {
			return errors.New("mysql/vstream: snapshot: concurrent copy ended before backpressured row could be buffered")
		}
		s.cond.Wait()
	}
	s.rowBuffer[tableName] = append(s.rowBuffer[tableName], row)
	s.bufferedBytes += rowBytes
	s.broadcast()
	return nil
}
