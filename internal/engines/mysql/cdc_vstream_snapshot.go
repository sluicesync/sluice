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

	"github.com/orware/sluice/internal/ir"
)

// openVStreamSnapshotStream is the FlavorPlanetScale path of
// [Engine.OpenSnapshotStream]. It rides VStream's built-in COPY mode:
// a [binlogdata.ShardGtid] with an empty Gtid asks vtgate to run an
// internal table-copy phase before tailing CDC, with the seam marked
// by COPY_COMPLETED events.
//
// The function captures a no-gap, no-overlap snapshot in a single
// physical stream:
//
//  1. Open the gRPC VStream with the from-beginning sentinel
//     ([fromBeginningVStreamPos]).
//  2. Synchronously drain COPY-phase events into a per-table row
//     buffer, updating the field cache and tracking the latest VGTID
//     as it arrives. The single global COPY_COMPLETED event (one
//     with empty Keyspace+Shard, fired after every per-shard/per-
//     table COPY_COMPLETED has arrived) marks the boundary.
//  3. Encode the captured VGTID into an [ir.Position] — this is the
//     position from which CDC will resume.
//  4. Build a [SnapshotStream] whose [Rows] returns the buffered rows
//     for any requested table, and whose [Changes] resumes reading
//     the same gRPC stream and routes events as [ir.Change] values.
//
// Buffering all COPY rows in memory (option (a) from the design
// brief) is what lets [Rows.ReadRows] be called table-by-table in any
// order — VStream emits FIELD-then-ROW-then-COPY_COMPLETED per
// (shard, table), but we don't know the orchestrator's table-iteration
// order at stream-open time. Sluice's v1 simple-mode workloads fit
// well in memory; sharded / very large tables are out of scope and
// would need a streaming variant.
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
		conn:                conn,
		grpcStream:          grpcStream,
		grpcCancel:          streamCancel,
	}

	// Drain the COPY phase synchronously. The caller's ctx bounds how
	// long we'll wait for COPY_COMPLETED — it isn't tied to streamCtx
	// because streamCtx must outlive this function.
	if err := snap.drainCopyPhase(ctx); err != nil {
		streamCancel()
		_ = conn.Close()
		return nil, err
	}

	position, err := encodeVStreamPos(snap.currentVgtid)
	if err != nil {
		streamCancel()
		_ = conn.Close()
		return nil, fmt.Errorf("mysql/vstream: snapshot: encode position: %w", err)
	}

	stream := &ir.SnapshotStream{
		Position: position,
		Rows:     &vstreamSnapshotRows{snap: snap},
		Changes:  &vstreamSnapshotChanges{snap: snap},
	}
	stream.CloseFn = snap.close
	return stream, nil
}

// vstreamSnapshotStream owns the gRPC connection and stream that
// produce both the snapshot rows and the post-snapshot CDC events.
// Lives for the life of the [ir.SnapshotStream] returned by
// [Engine.openVStreamSnapshotStream]; closed via [close].
//
// The struct exists in three logical states:
//
//  1. COPY phase, draining synchronously in
//     [openVStreamSnapshotStream]. fields and rowBuffer fill;
//     currentVgtid updates as VGTID events arrive. Terminates on
//     the global COPY_COMPLETED event.
//  2. Idle, between OpenSnapshotStream returning and [Changes.
//     StreamChanges] being called. The gRPC stream is held but no
//     events are being consumed — vtgate buffers them server-side
//     until the orchestrator finishes the bulk-copy phase.
//  3. CDC phase, after [Changes.StreamChanges] is called. A pump
//     goroutine consumes the stream and emits ir.Change values
//     onto the changes channel.
type vstreamSnapshotStream struct {
	keyspace string

	// fields caches column metadata keyed by [fieldCacheKey]. Shared
	// between COPY-phase row decoding and post-COPY change decoding —
	// FIELD events arrive in both phases, and a row cannot be decoded
	// without its field list.
	fields map[string][]*query.Field

	// currentVgtid is the latest VGTID observed on the stream. After
	// [drainCopyPhase] returns, this is the snapshot-consistent
	// position; during the CDC phase it advances with each
	// transaction's VGTID event.
	currentVgtid []shardGtid

	// rowBuffer accumulates COPY-phase rows keyed by unqualified
	// table name. Populated during drainCopyPhase, drained by
	// [vstreamSnapshotRows.ReadRows]. Once drained, the per-table
	// slice is cleared so a second ReadRows on the same table
	// returns an empty slice (matches the contract in row_reader.go
	// where ReadRows is single-shot per table).
	//
	// Multi-shard sharded keyspaces fan rows for the *same logical
	// table* in from multiple shards. Keying by unqualified table
	// name (rather than per-shard) merges them into one slice so
	// the orchestrator's single-table ReadRows call surfaces every
	// row regardless of shard origin.
	rowBuffer map[string][]ir.Row

	// copyCompletedShards tracks per-scope COPY_COMPLETED events
	// (those carrying a non-empty Keyspace/Shard) seen during the
	// COPY phase. drainCopyPhase terminates on vtgate's *global*
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
	// stream's Recv).
	pumpStarted bool

	mu  sync.Mutex
	err error
}

// drainCopyPhase reads VEvents off the gRPC stream synchronously
// until the global COPY_COMPLETED event arrives, populating
// rowBuffer and updating currentVgtid as it goes. The caller's ctx
// bounds how long we'll wait for the COPY phase to finish; if ctx
// cancels before COPY_COMPLETED, the gRPC call returns ctx.Err.
func (s *vstreamSnapshotStream) drainCopyPhase(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		resp, err := s.grpcStream.Recv()
		if err != nil {
			return fmt.Errorf("mysql/vstream: snapshot: copy recv: %w", err)
		}
		for _, ev := range resp.GetEvents() {
			done, err := s.dispatchCopyEvent(ev)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}
}

// dispatchCopyEvent routes a single COPY-phase VEvent. Returns
// done=true when the global COPY_COMPLETED arrives (the boundary
// between snapshot and CDC). All non-row, non-FIELD, non-VGTID
// events during COPY are bookkeeping and silently dropped.
func (s *vstreamSnapshotStream) dispatchCopyEvent(ev *binlogdata.VEvent) (done bool, err error) {
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
		s.rowBuffer[tableName] = append(s.rowBuffer[tableName], row)
	}
	return nil
}

// startPump spawns the post-COPY CDC pump goroutine. Returns the
// changes channel the pump owns and closes on shutdown. Idempotent
// guard via pumpStarted: a second StreamChanges call returns an
// error rather than racing on the gRPC stream.
func (s *vstreamSnapshotStream) startPump(ctx context.Context) (<-chan ir.Change, error) {
	s.mu.Lock()
	if s.pumpStarted {
		s.mu.Unlock()
		return nil, errors.New("mysql/vstream: snapshot: StreamChanges already called")
	}
	s.pumpStarted = true
	s.mu.Unlock()

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
			s.setErr(fmt.Errorf("mysql/vstream: snapshot: cdc recv: %w", err))
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
func (s *vstreamSnapshotStream) close() error {
	if s.grpcCancel != nil {
		s.grpcCancel()
		s.grpcCancel = nil
	}
	if s.conn != nil {
		err := s.conn.Close()
		s.conn = nil
		return err
	}
	return nil
}

// vstreamSnapshotRows is the [ir.RowReader] half of the snapshot
// stream. It serves rows from the in-memory buffer that
// [drainCopyPhase] populated; no I/O happens after Open.
//
// The reader is stateless: ReadRows can be called for any subset of
// the source's tables in any order. Each call drains the buffer for
// the requested table and returns nil for unknown tables (callers
// don't always read every table — translation may filter some out).
type vstreamSnapshotRows struct {
	snap *vstreamSnapshotStream
}

// ReadRows returns a channel that yields every row the COPY phase
// captured for table.Name, then closes. Synchronous emission inside
// a goroutine keeps the contract identical to the SQL-backed
// [RowReader] (caller drains the channel).
//
// Returning a nil-table-name table is rejected at the same
// signature point [RowReader.ReadRows] does, so the orchestrator's
// validation looks the same for both flavors.
func (r *vstreamSnapshotRows) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("mysql/vstream: snapshot: ReadRows: table is nil")
	}
	if table.Name == "" {
		return nil, errors.New("mysql/vstream: snapshot: ReadRows: table.Name is empty")
	}

	r.snap.mu.Lock()
	rows := r.snap.rowBuffer[table.Name]
	delete(r.snap.rowBuffer, table.Name)
	r.snap.mu.Unlock()

	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for _, row := range rows {
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
// — the position is whatever the COPY phase captured, set on the
// SnapshotStream at OpenSnapshotStream return time.
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
func (c *vstreamSnapshotChanges) StreamChanges(ctx context.Context, from ir.Position) (<-chan ir.Change, error) {
	if from.Engine != "" || from.Token != "" {
		shards, ok, err := decodeVStreamPos(from)
		if err != nil {
			return nil, fmt.Errorf("mysql/vstream: snapshot: StreamChanges: decode from position: %w", err)
		}
		if ok && !sameVgtid(shards, c.snap.currentVgtid) {
			return nil, fmt.Errorf(
				"mysql/vstream: snapshot: StreamChanges: from position %v does not match captured snapshot position %v",
				shards, c.snap.currentVgtid)
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
