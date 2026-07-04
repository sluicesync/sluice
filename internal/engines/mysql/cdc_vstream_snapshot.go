// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
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
	// The default public path starts the COPY from the beginning of the
	// binlog (empty-Gtid sentinel, no mid-COPY cursor). The shard layout
	// is resolved inside openVStreamSnapshotStreamFrom — passing nil tells
	// it to seed from-beginning against the resolved layout. The nil
	// tables arg keeps the keyspace-wide COPY (every table).
	return e.openVStreamSnapshotStreamFrom(ctx, dsn, nil, nil)
}

// vstreamCopyFilterRules builds the VStream COPY filter rules. With no
// tables it returns the keyspace-wide catch-all (every table). With a
// table allowlist it returns one rule per table (exact Match + a
// `select * from <t>` Filter) so vtgate's COPY scans only those tables —
// a large unrelated table in the same keyspace is never streamed/buffered
// (avoids the ADR-0071 multi-table interleaving buffer overflow).
func vstreamCopyFilterRules(tables []string) []*binlogdata.Rule {
	if len(tables) == 0 {
		return []*binlogdata.Rule{{Match: "/.*/"}}
	}
	rules := make([]*binlogdata.Rule, 0, len(tables))
	for _, t := range tables {
		rules = append(rules, &binlogdata.Rule{Match: t, Filter: "select * from " + t})
	}
	return rules
}

// openVStreamSnapshotStreamFrom is the seedable core of
// [openVStreamSnapshotStream]. When start is nil it seeds the COPY from
// the beginning of the binlog (the fresh cold-start path); when start is
// a non-nil []shardGtid carrying Vitess's per-shard TablePKs cursor it
// seeds the COPY to RESUME from that cursor (the process-restart
// interrupted-cold-start path, v0.99.8). Seeding from a cursor makes
// vtgate continue the COPY scan from the last-copied PK — the SAME
// machinery the in-place [reconnectCopy] uses — so the resumed COPY rows
// flow through the bulk [copyPump]/ReadRows path (batched bulk-COPY
// writer) rather than the per-row CDC apply path the plain CDC reader
// would use. On COPY completion it transitions to CDC exactly as a fresh
// cold-start does.
//
// Auto-shard composes with resume (ADR-0098). When the scope has more than
// one table (and the operator hasn't opted out via
// vstream_copy_single_stream), BOTH a fresh start AND a resume drive the
// per-table auto-shard pump ([copyPumpAutoShard]) — one single-table COPY at
// a time, constant memory, NO interleave. On a resume the persisted cursor
// names the one in-progress table; the pump re-copies the tables before it
// idempotently, resumes that table from its cursor, and copies the tables
// after it fresh ([resolveResumeAutoShard]). This is the fix for the bug
// where a resume of a large multi-table keyspace fell back to the legacy
// single keyspace-wide interleaved stream and crash-looped on the ADR-0071
// buffer cap. A single-table scope (or the opt-out) keeps the legacy
// single-stream resume path.
//
// A non-nil start is REQUIRED to carry a TablePKs cursor on at least one
// shard (the caller is resuming an interrupted COPY); seeding a bulk
// snapshot stream from a pure-CDC position would silently start a full
// re-copy from row 0 (vtgate ignores an empty-TablePKs Gtid for COPY),
// so the resumer guards against that before it reaches here. The shard
// list in start MUST match the resolved layout — a reshard since the
// checkpoint surfaces as a loud JOURNAL error from the pump rather than
// a silent mis-resume.
//
// tables scopes the COPY filter (vstreamCopyFilterRules): empty/nil copies
// every table in the keyspace; a non-empty allowlist makes vtgate's COPY
// scan only those tables so a large unrelated table in the same keyspace
// is never streamed/buffered (the ADR-0071 multi-table-interleaving
// overflow). The scope is captured on the stream (copyTables) so an
// in-place [reconnectCopy] re-applies it.
func (e Engine) openVStreamSnapshotStreamFrom(ctx context.Context, dsn string, start []shardGtid, tables []string) (*ir.SnapshotStream, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.DBName == "" {
		return nil, errors.New("mysql/vstream: snapshot: DSN has no database name (vitess keyspace expected)")
	}
	// Per-sync zero-date policy (ADR-0127); invalid zero_date refuses loudly.
	// The DSN param wins; absent, the engine's --zero-date default applies.
	zeroDate, err := readerZeroDateMode(cfg)
	if err != nil {
		return nil, err
	}
	zeroDate = e.resolveReaderZeroDate(zeroDate)
	// Self-hosted vitess flavor: default transport=plaintext / auth=none so
	// the cold-start snapshot (and the backup snapshot, which funnels through
	// here) dials a self-hosted vtgate without hand-set vstream_* params —
	// the same defaults openVStreamReader applies to the CDC path. Covers
	// every VStream dial entry point; the hosted planetscale flavor is left
	// on its secure defaults.
	applyVStreamFlavorDefaults(cfg, e.Flavor)

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
	livenessWindow, err := vstreamLivenessWindowFromDSN(cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	progressWindow, err := vstreamProgressWindowFromDSN(cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	copyProgressWindow, err := vstreamCopyProgressWindowFromDSN(cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	idleWarnWindow, err := vstreamIdleWarnWindowFromDSN(cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	// ADR-0099: cross-table COPY concurrency. The raw operator intent is
	// parsed here (loud on a malformed value); the effective stream count is
	// resolved against the in-scope table count below, once auto-shard
	// eligibility is known.
	rawTableParallelism, err := vstreamCopyTableParallelismFromDSN(cfg, e.opts.vstreamCopyTableParallelism)
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
	// freshness matters more than read isolation. A COPY-resume from a
	// TablePKs cursor MUST target PRIMARY for the same reason
	// buildVStreamRequest does (a REPLICA's schema engine may be cold →
	// buildTablePlan finds no copy plan → silent degrade to plain CDC).
	//
	// startPos seeds the request: nil → from-beginning sentinel (fresh
	// cold-start); non-nil → the resume cursor (carrying TablePKs).
	// fromBeginningVStreamPos carries no TablePKs, so toProtoShardGtids
	// can't fail for it — but the resume cursor's base64 TableLastPK can
	// (corrupt persisted token), and the general decode returns an error,
	// so we handle it.
	// Auto-shard-by-table eligibility (ADR-0095, extended to resume by
	// ADR-0098): copy each table as its own single-table VStream (constant
	// memory, no interleave) instead of one keyspace-wide interleaved
	// stream. Engaged with MORE THAN ONE table in scope, unless the operator
	// opts out via vstream_copy_single_stream — for BOTH a fresh cold-start
	// (start == nil) AND an interrupted-cold-start RESUME (start != nil
	// carrying a per-table TablePKs cursor). A one-table scope is already
	// constant-memory single-stream, so auto-shard adds nothing there. The
	// order is the caller's filtered table order, which matches the
	// orchestrator's per-table ReadRows order — the invariant that keeps
	// exactly one table in flight.
	//
	// ADR-0098: the resume gate USED to require start == nil, so a resume of
	// a >1-table keyspace fell back to the legacy single keyspace-wide
	// interleaved stream — which re-introduced the very multi-table
	// interleave ADR-0095 removed, crash-looping on the ADR-0071 cap for any
	// large keyspace. Resume is now auto-shard-aware: the persisted cursor
	// names exactly the in-progress table (the only table carrying TablePKs);
	// the per-table loop re-copies the tables before it idempotently
	// (Bug-125 upsert absorbs the overlap), resumes that table from its
	// cursor, and copies the tables after it fresh. See resolveResumeAutoShard.
	autoShard := len(tables) > 1 && !vstreamCopySingleStreamFromDSN(cfg)

	// ADR-0099: cross-table COPY concurrency. When auto-shard is engaged and
	// the operator opted into K > 1 streams, the in-scope tables are
	// partitioned into K disjoint groups and the concurrent driver runs one
	// single-table COPY sub-sequence per group on its own VStream. K resolves
	// to 1 (the sequential single-stream auto-shard, byte-identical to
	// ADR-0095/0098) for the zero value / absent param / a one-table scope —
	// so concurrent is a pure opt-in and every existing caller is unchanged.
	// The partition is a DETERMINISTIC pure function of (sorted tables, K)
	// (cross-start/resume stability — ADR-0099 §5); v1 has no per-table size
	// estimator wired, so it uses the deterministic round-robin floor.
	var concurrentGroups [][]string
	if autoShard {
		if k := resolveCopyTableParallelism(rawTableParallelism, len(tables)); k > 1 {
			concurrentGroups = partitionTablesForStreams(tables, k, nil)
		}
	}
	concurrent := len(concurrentGroups) > 1

	// resumeSeed/resumeSeedTable carry the persisted cursor into the
	// auto-shard pump (ADR-0098): when resuming, the pump seeds the
	// in-progress table's per-table COPY from this cursor instead of
	// from-beginning. resolveResumeAutoShard validates the persisted cursor
	// names exactly one in-scope table and returns its name; a cursor that
	// can't be placed in the table sequence is refused loudly rather than
	// silently re-copying or skipping. It runs against the FULL in-scope set
	// before partitioning, so the concurrent driver can place the seed into
	// whichever group contains the in-progress table.
	var (
		resumeSeed      []shardGtid
		resumeSeedTable string
	)
	if start != nil && autoShard {
		seedTable, rerr := resolveResumeAutoShard(start, tables)
		if rerr != nil {
			streamCancel()
			_ = conn.Close()
			return nil, rerr
		}
		resumeSeed = start
		resumeSeedTable = seedTable
	}

	// In auto-shard mode the constructor's stream for table[0] is always
	// opened from the BEGINNING (the fresh-cold-start shape). On a resume the
	// pump's first iteration reopens table[0] with the seed when table[0] IS
	// the in-progress table (resumeSeedTable == tables[0]); a single-stream
	// (non-auto-shard) resume keeps seeding the one stream from the cursor as
	// before. This keeps exactly one per-table open path in the pump.
	//
	// ADR-0099: in CONCURRENT mode the constructor opens NO stream — each of
	// the K copyStreams opens its own per-table VStream in its own pump
	// goroutine (cs.openStreamForTable). The grpcStream field stays nil; the
	// concurrent driver never touches it.
	var grpcStream vtgateservice.Vitess_VStreamClient
	if !concurrent {
		startPos := start
		if startPos == nil || autoShard {
			startPos = fromBeginningVStreamPos(keyspace, shards)
		}
		protoShardGtids, perr := toProtoShardGtids(startPos)
		if perr != nil {
			streamCancel()
			_ = conn.Close()
			return nil, fmt.Errorf("mysql/vstream: snapshot: build start position: %w", perr)
		}

		// In auto-shard mode the first physical stream is scoped to the FIRST
		// table only; the pump reopens per-table as it advances. Otherwise the
		// single stream carries the whole (possibly keyspace-wide) scope.
		firstFilterTables := tables
		if autoShard {
			firstFilterTables = []string{tables[0]}
		}
		req := &vtgate.VStreamRequest{
			TabletType: topodata.TabletType_PRIMARY,
			Vgtid: &binlogdata.VGtid{
				ShardGtids: protoShardGtids,
			},
			Filter: &binlogdata.Filter{Rules: vstreamCopyFilterRules(firstFilterTables)},
			Flags: &vtgate.VStreamFlags{
				MinimizeSkew:      true,
				StopOnReshard:     true,
				HeartbeatInterval: 5,
			},
		}

		grpcStream, err = client.VStream(streamCtx, req)
		if err != nil {
			streamCancel()
			_ = conn.Close()
			return nil, fmt.Errorf("mysql/vstream: snapshot: open stream: %w", err)
		}
	}

	// copyTables is the scope the in-place reconnect re-applies: in
	// auto-shard mode that is the currently-open single table; otherwise
	// the full scope.
	reconnectScope := tables
	var seq []string
	if autoShard {
		reconnectScope = []string{tables[0]}
		seq = tables
		switch {
		case concurrent:
			slog.InfoContext(ctx, "mysql/vstream: snapshot: cross-table concurrent COPY enabled (ADR-0099)",
				slog.String("keyspace", keyspace),
				slog.Int("tables", len(tables)),
				slog.Int("streams", len(concurrentGroups)))
		case resumeSeedTable != "":
			slog.InfoContext(ctx, "mysql/vstream: snapshot: auto-shard-by-table COPY RESUME (one table at a time, bounded memory; in-progress table seeded from cursor)",
				slog.String("keyspace", keyspace),
				slog.Int("tables", len(tables)),
				slog.String("resume_table", resumeSeedTable))
		default:
			// Perf-parity gap 3: the sequential default is DELIBERATE on the
			// VStream path (see defaultCopyTableParallelism's rationale —
			// resume-partition stability + the per-stream buffer split), so
			// the multi-table cold-copy names the throughput knob loudly
			// instead of leaving a hidden serial ceiling.
			slog.InfoContext(ctx, "mysql/vstream: snapshot: auto-shard-by-table COPY (one table at a time, bounded memory); "+
				"cold-copy runs SEQUENTIAL by default on VStream sources — N concurrent COPY streams are available via "+
				"--vstream-copy-table-parallelism (see docs/throughput-tuning.md)",
				slog.String("keyspace", keyspace),
				slog.Int("tables", len(tables)))
		}
	}

	snap := &vstreamSnapshotStream{
		keyspace:             keyspace,
		zeroDate:             zeroDate,
		client:               client,
		shards:               shards,
		copyTables:           reconnectScope,
		copyTablesSeq:        seq,
		concurrentCopy:       concurrent,
		concurrentGroups:     concurrentGroups,
		resumeSeed:           resumeSeed,
		resumeSeedTable:      resumeSeedTable,
		tableCopyComplete:    make(map[string]bool),
		reconnectMax:         defaultCopyReconnectMax,
		reconnectBackoffBase: defaultCopyReconnectBackoffBase,
		reconnectBackoffCap:  defaultCopyReconnectBackoffCap,
		fields:               make(map[string][]*query.Field),
		rowBuffer:            make(map[string][]ir.Row),
		boolWarn:             newBoolRangeWarner(),
		copyCompletedShards:  make(map[string]bool),
		maxBufferBytes:       defaultSnapshotMaxBufferBytes,
		checkpointRows:       defaultCopyCheckpointRows,
		checkpointInterval:   defaultCopyCheckpointInterval,
		livenessWindow:       livenessWindow,
		progressWindow:       progressWindow,
		copyProgressWindow:   copyProgressWindow,
		idleWarnWindow:       idleWarnWindow,
		conn:                 conn,
		grpcStream:           grpcStream,
		grpcCancel:           streamCancel,
	}
	snap.cond = sync.NewCond(&snap.mu)

	stream := &ir.SnapshotStream{
		// Position is finalised by the COPY pump at the global
		// COPY_COMPLETED event and read by the orchestrator only AFTER
		// bulk-copy (ADR-0071).
		//
		// On the SINGLE-STREAM path, the happens-before edge to the
		// orchestrator's Position read is the row-channel close: the
		// pump records Position under mu, flips copyComplete, and only
		// then can a draining ReadRows observe completion and close, so
		// a post-drain Position read is ordered after the write.
		//
		// On the AUTO-SHARD / concurrent paths (ADR-0095 / ADR-0099)
		// that edge does NOT hold: each ReadRows closes on a PER-TABLE
		// signal (tableCopyComplete), so the last ReadRows can return
		// BEFORE the producer goroutine stitches and writes the
		// stitched-minimum Position. The orchestrator must therefore
		// join the producer's completion barrier (WaitCopyCompleteFn,
		// below — copyDone, closed only AFTER finishCopyAutoShard writes
		// Position under mu) before reading Position. Without the join
		// the handoff races the write and can read an EMPTY Position —
		// the wrong CDC start position, a potential silent gap.
		//
		// It is the zero Position at open time; the cold-start log line
		// that reads it immediately after open simply shows an empty
		// token (cosmetic — the load-bearing read is the post-copy one).
		Rows:    &vstreamSnapshotRows{snap: snap},
		Changes: &vstreamSnapshotChanges{snap: snap},
	}
	stream.CloseFn = snap.close
	// The cold-start handoff joins this barrier after bulk-copy drains
	// and before it reads stream.Position. copyDone is closed by the
	// COPY pump exactly once, AFTER finishCopy / finishCopyAutoShard has
	// written Position under mu, so the join establishes
	// producer-writes-Position → closes-copyDone → handoff-waits-copyDone
	// → handoff-reads-Position. Required on the auto-shard / concurrent
	// paths (per-table ReadRows close); harmless (already closed) on the
	// single-stream path.
	stream.WaitCopyCompleteFn = snap.waitCopyComplete

	// Pump the COPY phase concurrently rather than draining it to
	// completion here. streamCtx (owned by CloseFn) bounds the pump's
	// lifetime; the caller's ctx does NOT, because the SnapshotStream —
	// and the gRPC stream it rides — must outlive this function.
	//
	// Auto-shard mode (ADR-0095) runs the per-table COPY driver, which
	// reopens the stream once per table on the same connection and
	// stitches the per-table snapshot points; otherwise the legacy
	// single-stream pump drains the one keyspace-wide / single-table COPY.
	//
	// ADR-0099: when K > 1 concurrent streams were resolved, the concurrent
	// driver runs one single-table COPY sub-sequence per disjoint table group
	// on its own VStream, then stitches the per-shard set-min across the union
	// of every stream's per-table snapshots. K == 1 stays byte-identical on
	// the sequential copyPumpAutoShard.
	snap.copyDone = make(chan struct{})
	switch {
	case concurrent:
		go snap.copyPumpAutoShardConcurrent(streamCtx, streamCancel, stream, concurrentGroups)
	case len(snap.copyTablesSeq) > 0:
		go snap.copyPumpAutoShard(streamCtx, streamCancel, stream)
	default:
		go snap.copyPump(streamCtx, streamCancel, stream)
	}

	return stream, nil
}

// vstreamCopySingleStreamFromDSN reports whether the operator opted OUT
// of the ADR-0095 auto-shard-by-table COPY via
// `vstream_copy_single_stream=true`, restoring the legacy single
// keyspace-wide interleaved COPY stream (and its ADR-0071 multi-table
// loud-refusal floor). Default false — auto-shard is on by default so the
// full-keyspace cold-copy just works at bounded memory.
func vstreamCopySingleStreamFromDSN(cfg *gomysql.Config) bool {
	return cfg.Params["vstream_copy_single_stream"] == "true"
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

	// zeroDate is this snapshot stream's per-sync zero/partial-date policy
	// (ADR-0127), parsed from the `zero_date` source-DSN param at open. The
	// zero value (zeroDateInherit) resolves to the loud refuse default (the engine --zero-date default is folded at reader construction, task 2.5)
	// (--zero-date); set in the constructor so the COPY cold-copy honors the
	// same per-sync policy as the steady-state VStream CDC reader.
	zeroDate zeroDateMode

	// boolWarn carries the one-time-per-column TINYINT(1)-out-of-range
	// WARN (Vector D) for the VStream cold-start COPY + its CDC catch-up.
	// Set in the constructor; nil-safe (test literals leave it nil and
	// observeNamed no-ops).
	boolWarn *boolRangeWarner

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

	// copyTables is the COPY filter scope captured at open: empty means
	// keyspace-wide (every table), non-empty restricts the COPY to those
	// unqualified table names (vstreamCopyFilterRules). Held so an
	// in-place [reconnectCopy] re-applies the SAME scope — otherwise a
	// reconnect would silently widen the COPY back to the whole keyspace
	// and start streaming the large unrelated table the original scope
	// excluded (the ADR-0071 overflow this feature avoids).
	//
	// In auto-shard-by-table mode (ADR-0095, copyTablesSeq non-empty)
	// copyTables is the SINGLE table the currently-open per-table COPY
	// stream is scoped to (so [reconnectCopy] re-applies that one table's
	// scope on an in-place reconnect). The pump rewrites it as it advances
	// from table to table.
	copyTables []string

	// copyTablesSeq is the auto-shard-by-table COPY iteration order
	// (ADR-0095). When non-empty, the COPY pump copies these tables ONE AT
	// A TIME — each a single-table VStream (Match:<table>, constant memory,
	// no interleave) opened on the same connection — instead of one
	// keyspace-wide interleaved stream. The order MUST match the
	// orchestrator's per-table ReadRows order (the filtered schema order)
	// so exactly one table is ever in flight. Empty means the legacy
	// single-stream keyspace COPY (the opt-out escape hatch, or a
	// one-table/keyspace-wide open). Set at construction; read by the pump.
	copyTablesSeq []string

	// autoShardIdx is the index into copyTablesSeq of the table whose
	// per-table COPY is currently in flight. Pump-owned (advanced as each
	// table's COPY_COMPLETED arrives); read under mu by helpers that name
	// the active table in log/error messages.
	autoShardIdx int

	// resumeSeed / resumeSeedTable carry the persisted mid-COPY cursor into
	// the auto-shard pump on an interrupted-cold-start RESUME (ADR-0098).
	// resumeSeedTable is the unqualified name of the in-progress table the
	// persisted cursor names (the only table carrying TablePKs); resumeSeed
	// is the full []shardGtid resume position. When the auto-shard pump
	// reaches resumeSeedTable it opens that table's per-table COPY SEEDED
	// from resumeSeed (so vtgate continues the scan from the last-copied PK)
	// instead of from-beginning; every other table in the sequence opens
	// from-beginning (the tables before it are re-copied idempotently — the
	// Bug-125 upsert absorbs the overlap — and the tables after it are fresh).
	// Empty (resumeSeedTable == "") on a fresh cold-start. Set at
	// construction; read by the pump.
	resumeSeed      []shardGtid
	resumeSeedTable string

	// perTableSnapshots accumulates each completed per-table COPY's
	// snapshot VGTID (P_i), in copyTablesSeq order (ADR-0095). After the
	// last table the pump stitches these into the single CDC-resume
	// position via [stitchSnapshotMin] (the per-shard GTID-set minimum).
	// Pump-owned during the auto-shard COPY.
	perTableSnapshots [][]shardGtid

	// tableCopyComplete marks, per unqualified table name, that the
	// table's per-table COPY has finished (auto-shard mode). [ReadRows]
	// closes a table's channel once its queue drains AND this is set for
	// it — the per-table analogue of the global copyComplete, so a
	// consumer draining table t_0 closes when t_0's COPY ends, not only
	// when the whole keyspace copy ends. Guarded by mu/cond.
	tableCopyComplete map[string]bool

	// --- per-stream byte budget (ADR-0099 §2, the deadlock fix) ---
	//
	// The concurrent cross-table COPY (ADR-0099) runs K independent producer
	// streams against ONE shared rowBuffer + byte cap, but the orchestrator
	// consumer drains tables ONE AT A TIME. If all K streams shared the single
	// maxBufferBytes cap, a stream racing ahead on a look-ahead table the
	// consumer hasn't reached yet could fill the whole cap, leaving the stream
	// that owns the table the consumer IS draining unable to enqueue its first
	// row — a liveness deadlock the progress watchdog only escapes from after
	// ~10 minutes (the LOUD timeout). We give each of the K streams its OWN
	// byte sub-budget = maxBufferBytes / K (perStreamCap): a look-ahead stream
	// parks on its OWN sub-cap (bounded), while the stream whose table is being
	// drained always has its full sub-cap free to produce. Total bounded memory
	// is unchanged (K × (cap/K) = cap).
	//
	// perStreamBytes[idx] is the running ApproximateRowBytes sum currently
	// buffered by stream idx (incremented on enqueue by the producing stream,
	// debited on ReadRows handoff via tableStreamIdx). perStreamCap is the
	// per-stream sub-budget (>= 1; floored so a stream can always make
	// progress one row at a time even at a tiny cap / large K, matching the
	// single-row-exceeds-cap fallback of the sequential enqueueRowLocked).
	// tableStreamIdx maps each in-scope table to the index of the stream that
	// produces it (the disjoint partition, built once before the pumps start;
	// read-only thereafter). nil/empty on every non-concurrent path — the
	// sequential pumps never touch these. Guarded by mu.
	perStreamBytes []int64
	perStreamCap   int64
	tableStreamIdx map[string]int

	// concurrentCopy is the immutable (set-at-construction, read-only)
	// discriminator for the ADR-0099/0100 concurrent cross-table COPY path.
	// True iff K > 1 streams were resolved (len(concurrentGroups) > 1). Read
	// by [ReadRows] WITHOUT the lock to decide whether to skip the
	// sequential-only activeTable bookkeeping (which W concurrent consumers
	// would race-clobber); safe to read lock-free because it is never
	// mutated after construction. Distinct from tableStreamIdx (which the
	// pump populates under the lock AFTER the goroutine spawns, so reading it
	// lock-free from ReadRows would data-race).
	concurrentCopy bool

	// concurrentGroups is the disjoint table partition the concurrent
	// producer driver runs (ADR-0099), surfaced to the pipeline so it can
	// run one read→write CONSUMER pipeline per group concurrently (ADR-0100,
	// the write-side companion). It is EXACTLY the [][]string
	// copyPumpAutoShardConcurrent partitions the producers into, so the
	// consumer partition the pipeline reads ≡ the producer partition by
	// construction (coverage + disjointness inherited, never re-derived). nil
	// on every non-concurrent path (K = 1 / single-stream / one-table scope)
	// — the pipeline then runs the serial table loop byte-identically. Set
	// once at construction; read-only thereafter (surfaced via
	// [vstreamSnapshotRows.ConcurrentCopyGroups]).
	concurrentGroups [][]string

	// reconnectMax is the in-place COPY-reconnect budget (Phase C):
	// consecutive retriable Recv failures the pump absorbs before giving
	// up and failing the COPY (the outer runWithRetry then becomes the
	// backstop). reconnectBackoffBase/Cap bound the exponential backoff
	// between attempts. Seeded with safe defaults by the constructor.
	reconnectMax         int
	reconnectBackoffBase time.Duration
	reconnectBackoffCap  time.Duration

	// livenessWindow is the Phase-1 watchdog window for both pumps (COPY
	// and post-COPY CDC): how long to wait for the FIRST NON-HEARTBEAT
	// (serving-proof) event after the stream opens before failing LOUDLY
	// rather than hanging silently (the dead-stream / no-tablet wedge;
	// ADR-0073 (b2)). From vstream_liveness_timeout; 0 disables the whole
	// watchdog.
	livenessWindow time.Duration

	// progressWindow is the Phase-2 (mid-stream progress) window for the
	// post-COPY CDC pump: once a serving tablet is proven, how long it
	// tolerates TOTAL silence before failing LOUDLY (the post-failover
	// dead-Recv wedge; ADR-0073 (F3)). From vstream_progress_timeout; 0
	// disables Phase 2 on the CDC pump.
	progressWindow time.Duration

	// copyProgressWindow is the Phase-2 window for the COPY pump —
	// deliberately far more generous than progressWindow because the COPY
	// phase can take MINUTES of legitimate vreplication/schema-engine
	// warmup before its first row (the only event meanwhile may be the
	// attach VGTID, which proves serving and arms Phase 2). From
	// vstream_copy_progress_timeout; 0 disables Phase 2 on the COPY pump.
	copyProgressWindow time.Duration

	// idleWarnWindow is the Phase-2 SOFT idle-WARN sub-window (item 19(a))
	// for BOTH pumps: how long a proven stream may receive only heartbeats
	// (no change events) before the watchdog emits a single rate-limited
	// WARN per quiet spell — the throttle/idle heads-up. OBSERVABILITY ONLY;
	// never fails the stream. From vstream_idle_warn_timeout; 0 disables the
	// soft WARN only.
	idleWarnWindow time.Duration

	// fields caches column metadata keyed by [fieldCacheKey]. Shared
	// between COPY-phase row decoding and post-COPY change decoding —
	// FIELD events arrive in both phases, and a row cannot be decoded
	// without its field list. Guarded by mu (the COPY pump and the CDC
	// pump both write it; ReadRows never touches it).
	fields map[string][]*query.Field

	// snapshotSig is the per-table structural fingerprint of the last
	// ir.SchemaSnapshot emitted on the POST-COPY CDC phase — the
	// true-delta gate that mirrors [vstreamCDCReader.snapshotSig]
	// (ADR-0049 Chunk B2). F7c: the cold-start→CDC path
	// ([dispatchCDCEvent]) must emit the SAME SchemaSnapshot boundary the
	// standalone reader's FIELD branch does, otherwise an online ADD /
	// DROP / MODIFY COLUMN that lands after a VStream cold-start never
	// reaches the ADR-0091 schema-forward intercept (the boundary signal
	// was silently dropped, leaving only the field-cache update — so the
	// post-DDL ROW decodes with the new column but the target schema is
	// never altered: SQLSTATE 42703 / MySQL 1054). Lazily initialised on
	// first post-COPY FIELD so COPY-phase FIELD caching is untouched.
	//
	// Unlike the sibling fields above, this is touched ONLY by the single
	// post-COPY CDC pump goroutine (in maybeSnapshotSchemaCDC) — by then
	// the COPY pump that shares mu has exited — so it needs no lock. (The
	// CDC pump is the sole writer; the -race Integration gate guards this
	// contract.)
	snapshotSig map[string]ir.SchemaSignature

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

	// --- durable-write watermark (v0.99.9 silent-loss fix) ---
	//
	// The Phase-B checkpoint must persist a position no further ahead than
	// the rows the consumer has DURABLY written to the target. The pump's
	// currentVgtid (the TablePKs cursor) advances as rows are RECEIVED into
	// the bounded in-flight buffer, which runs AHEAD of the consumer by up
	// to maxBufferBytes. Persisting that received frontier meant a hard
	// crash (buffer lost, cursor advanced) left resume restarting past
	// un-written rows — silent loss. We now key the checkpoint on the
	// durable frontier instead.
	//
	// enqueuedRows is the monotonic count of rows appended to rowBuffer
	// over the whole COPY (pump-owned, mutated under mu in
	// enqueueRowLocked). durableRows is the cumulative count the bulk-copy
	// writer reports DURABLY committed via AdvanceDurableRows (consumer-
	// driven, mutated under mu). Vitess emits a VGTID *after* the rows it
	// covers, so when a VGTID arrives every row enqueued so far is covered
	// by it: we record a breadcrumb {enqueuedRows, encodedPosition}. A
	// breadcrumb is safe to checkpoint once durableRows >= its rowsCovered
	// (all the rows it covers are on the target). maybeCheckpoint persists
	// the highest such breadcrumb instead of currentVgtid. Guarded by mu.
	enqueuedRows   int64
	durableRows    int64
	posBreadcrumbs []posBreadcrumb

	mu  sync.Mutex
	err error
}

// posBreadcrumb pairs an encoded COPY-resume position with the
// monotonic enqueued-row count it covers (v0.99.9). Vitess emits the
// VGTID carrying a position AFTER the rows that position resumes past,
// so rowsCovered = the pump's enqueuedRows at the moment the VGTID
// arrived: every one of those rows is at-or-before pos in the COPY scan.
// The position is checkpoint-safe once the consumer has durably written
// rowsCovered rows.
type posBreadcrumb struct {
	rowsCovered int64
	pos         ir.Position
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
func (s *vstreamSnapshotStream) copyPump(ctx context.Context, cancel context.CancelFunc, stream *ir.SnapshotStream) {
	defer close(s.copyDone)

	// Continuous two-phase liveness watchdog (ADR-0073 (b2)+(F3)):
	//
	//   - PHASE 1 (s.livenessWindow): no serving-proof event within the
	//     window of opening the stream — the dead-stream / no-tablet
	//     signature — fails LOUDLY. The snapshot COPY targets PRIMARY (so
	//     the no-REPLICA wedge can't fire here), but a dead PRIMARY stream
	//     is the same silent-hang hazard, and keeping the guard symmetric
	//     with the CDC tail is cheap.
	//   - PHASE 2 (s.copyProgressWindow): once serving is proven, total
	//     silence past the window fails LOUDLY (the post-failover wedge).
	//     The COPY Phase-2 window is DELIBERATELY generous (~10 min default)
	//     to tolerate the legitimate multi-minute vreplication/schema-engine
	//     warmup during which the only event may be the attach VGTID.
	//
	// Either fire cancels the stream so the parked Recv unblocks rather
	// than hanging the cold-start forever.
	live := startVStreamLiveness(ctx, s.livenessWindow, s.copyProgressWindow, s.idleWarnWindow,
		func() {
			s.failCopy(vstreamLivenessTimeoutError(s.livenessWindow, topodata.TabletType_PRIMARY, s.keyspace, s.shards))
			cancel()
		},
		func() {
			s.failCopy(vstreamProgressTimeoutError(s.copyProgressWindow, topodata.TabletType_PRIMARY, s.keyspace, s.shards))
			cancel()
		},
		func() {
			// SOFT idle-WARN (item 19(a)): heartbeats flowing but no change
			// events for the soft window during COPY. OBSERVABILITY ONLY — do
			// NOT failCopy, do NOT cancel; the COPY stays resilient. On the
			// COPY pump this also flags a legitimately long warmup, which is
			// harmless heads-up noise the operator can ignore.
			slog.WarnContext(ctx, vstreamIdleWarnMessage(s.idleWarnWindow, s.keyspace, s.shards))
		})
	defer live.stop()

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
		// Feed the watchdog: a non-heartbeat event clears Phase 1 (serving
		// proven); any event re-arms the Phase-2 progress deadline. A
		// heartbeat alone does NOT clear Phase 1 (ADR-0073 (b2)).
		if evs := resp.GetEvents(); len(evs) > 0 {
			live.observe(eventsProveLiveness(evs))
		}
		// Reset the in-place reconnect budget only on actual COPY PROGRESS (a
		// ROW buffered), NOT on any received event. Otherwise a reconnect that
		// re-establishes the stream but then yields only non-progress events —
		// heartbeats, or a stale VGTID when the cursor is unresumable after a
		// tablet death + reparent — resets reconnectAttempts every cycle, so
		// the reconnect loop churns forever: it never exhausts reconnectMax and
		// never fails loud (the mid-COPY tablet-kill finding). Gating the reset
		// on a copied row means an unproductive reconnect loop burns its budget
		// and surfaces a LOUD failCopy (~reconnectMax × backoff), which the
		// pipeline's retry can then act on.
		copiedRow := false
		for _, ev := range resp.GetEvents() {
			if ev.GetType() == binlogdata.VEventType_ROW {
				copiedRow = true
			}
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
		if copiedRow {
			reconnectAttempts = 0
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

// resolveResumeAutoShard validates that a persisted mid-COPY resume
// position (start) can drive the ADR-0098 auto-shard-aware resume over the
// in-scope table sequence, and returns the unqualified name of the
// in-progress table the cursor names.
//
// The persisted position carries a per-shard TablePKs cursor for EXACTLY the
// one table whose per-table COPY was in flight when the run was interrupted
// (auto-shard scopes each per-table COPY to a single table, so vtgate only
// ever emits a TablePKs entry for that one table). The auto-shard pump then:
//
//   - re-copies every table BEFORE the in-progress one from-beginning
//     (idempotent upsert absorbs the re-copy overlap, Bug-125), so each
//     contributes a fresh per-table snapshot to the stitch;
//   - resumes the in-progress table from its cursor (vtgate continues the
//     scan from the last-copied PK, no row-0 restart);
//   - copies every table AFTER it fresh.
//
// It refuses LOUDLY (never silently re-copies or skips) when:
//
//   - the cursor names MORE THAN ONE table (a single-stream / legacy
//     interleaved-resume token, or a corrupt one) — auto-shard cannot place
//     a multi-table cursor in the per-table sequence safely;
//   - the cursor names a table NOT in the current in-scope sequence (an
//     --include-table change since the checkpoint, or a stale token) — the
//     operator-facing error names the table so the mismatch is diagnosable.
//
// A position with no TablePKs cursor never reaches here (the resume entry
// point gates on anyTablePKsPresent before calling).
func resolveResumeAutoShard(start []shardGtid, tables []string) (string, error) {
	inScope := make(map[string]bool, len(tables))
	for _, t := range tables {
		inScope[t] = true
	}

	cursorTables := make(map[string]bool)
	for _, sg := range start {
		decoded, err := decodeTablePKs(sg.TablePKs)
		if err != nil {
			return "", fmt.Errorf("mysql/vstream: snapshot resume: auto-shard: decode TablePKs cursor: %w", err)
		}
		for _, pk := range decoded {
			if name := pk.GetTableName(); name != "" {
				cursorTables[name] = true
			}
		}
	}

	switch len(cursorTables) {
	case 0:
		// The resume entry point gates on anyTablePKsPresent, so an empty
		// cursor here is a contract violation — refuse rather than silently
		// re-copy the whole keyspace from row 0.
		return "", errors.New(
			"mysql/vstream: snapshot resume: auto-shard: position carries no in-progress table cursor; " +
				"refusing to drive an auto-shard resume without a seed (the cursor-less warm-resume belongs on the plain CDC path)",
		)
	case 1:
		// fallthrough to the single-table extraction below.
	default:
		names := make([]string, 0, len(cursorTables))
		for n := range cursorTables {
			names = append(names, n)
		}
		sort.Strings(names)
		return "", fmt.Errorf(
			"mysql/vstream: snapshot resume: auto-shard: persisted cursor names %d tables (%v); "+
				"auto-shard resume expects a single in-progress table cursor. This token was written by the legacy "+
				"single keyspace-wide COPY (pre-ADR-0098) or is corrupt — re-run with vstream_copy_single_stream=true "+
				"to resume it on the legacy path, or restart the cold-start (the idempotent COPY writer absorbs the re-copy)",
			len(cursorTables), names,
		)
	}

	var seedTable string
	for n := range cursorTables {
		seedTable = n
	}
	if !inScope[seedTable] {
		return "", fmt.Errorf(
			"mysql/vstream: snapshot resume: auto-shard: persisted cursor names in-progress table %q, "+
				"which is not in the current in-scope table set %v (an --include-table change since the checkpoint, "+
				"or a stale token). Restore the original table scope to resume, or restart the cold-start",
			seedTable, tables,
		)
	}
	return seedTable, nil
}

// copyPumpAutoShard is the auto-shard-by-table COPY driver (ADR-0095). It
// copies each table in s.copyTablesSeq as its OWN single-table VStream —
// constant memory, no interleave — sequentially on the same gRPC
// connection, then stitches the per-table snapshot points into one
// CDC-resume position.
//
// For each table it runs the SAME per-event dispatch / watchdog /
// in-place-reconnect / checkpoint machinery the single-stream [copyPump]
// uses (factored into [pumpOneTableCopy]); the only difference is what
// happens at a COPY_COMPLETED: instead of finishing the whole copy, it
// records that table's snapshot VGTID (P_i), signals the table complete
// so the draining [ReadRows] closes, and reopens the stream scoped to the
// NEXT table. After the last table it computes the per-shard GTID-set
// minimum ([stitchSnapshotMin]) — the gapless, overlap-safe CDC-resume
// position (ADR-0095 consistency model) — records it onto the stream, and
// flips the global copyComplete so any straggler consumer unwedges.
//
// Concurrency: exactly one per-table stream is Recv'd at a time (the
// loop is sequential), so this introduces no concurrent-Recv hazard
// beyond what [copyPump] already manages. The streamCtx (CloseFn-owned)
// bounds every per-table Recv; cancel unblocks a parked Recv.
func (s *vstreamSnapshotStream) copyPumpAutoShard(ctx context.Context, cancel context.CancelFunc, stream *ir.SnapshotStream) {
	defer close(s.copyDone)

	for idx, table := range s.copyTablesSeq {
		select {
		case <-ctx.Done():
			s.failCopy(ctx.Err())
			return
		default:
		}

		s.mu.Lock()
		s.autoShardIdx = idx
		s.mu.Unlock()

		// Reopen the stream scoped to this table for every table after the
		// first (the constructor already opened table[0] from-beginning).
		// Reopening on a FRESH from-beginning cursor for the new table is
		// correct for a cold-start: each single-table COPY is independent
		// and captures its own snapshot point; there is no cross-table
		// cursor to carry.
		//
		// ADR-0098 resume: when this table IS the in-progress table the
		// persisted cursor names (resumeSeedTable), reopen it SEEDED from
		// resumeSeed so vtgate continues the COPY scan from the last-copied
		// PK rather than from row 0 — even when it is table[0] (the
		// constructor opened table[0] from-beginning, so we must reopen it
		// seeded here). Tables BEFORE the in-progress table are re-copied
		// from-beginning (idempotent upsert absorbs the overlap, Bug-125);
		// tables AFTER it are fresh from-beginning.
		switch {
		case s.resumeSeedTable != "" && table == s.resumeSeedTable:
			if err := s.reopenForTableSeeded(ctx, table, s.resumeSeed); err != nil {
				s.failCopy(err)
				return
			}
		case idx > 0:
			if err := s.reopenForTable(ctx, table); err != nil {
				s.failCopy(err)
				return
			}
		}

		snap, err := s.pumpOneTableCopy(ctx, cancel, table)
		if err != nil {
			s.failCopy(err)
			return
		}

		// Record this table's snapshot point and signal it complete so the
		// consumer draining it (ReadRows) closes its channel. The next
		// table's COPY then streams into a fresh field cache / cursor.
		s.mu.Lock()
		s.perTableSnapshots = append(s.perTableSnapshots, snap)
		s.tableCopyComplete[table] = true
		// Reset the per-stream cursor + field cache for the next table's
		// single-table COPY. currentVgtid is the JUST-captured P_i; the
		// next table starts from the beginning and captures its own P.
		s.currentVgtid = nil
		clear(s.fields)
		s.posBreadcrumbs = nil
		s.broadcast()
		s.mu.Unlock()
	}

	s.finishCopyAutoShard(stream)
}

// pumpOneTableCopy Recv-drives the currently-open single-table COPY
// stream until its (global) COPY_COMPLETED, returning that table's
// snapshot VGTID (a copy of s.currentVgtid at completion). It reuses the
// single-stream pump's liveness watchdog, in-place reconnect, and
// bounded-cadence checkpoint verbatim — only the terminal action differs
// (return the snapshot instead of finishing the whole copy). table is the
// unqualified name, used only for log/error context.
//
// A single-table COPY emits exactly one COPY_COMPLETED (the global one,
// empty keyspace+shard), which [dispatchCopyEventLocked] returns as
// done=true — the natural per-table terminator.
func (s *vstreamSnapshotStream) pumpOneTableCopy(ctx context.Context, cancel context.CancelFunc, table string) ([]shardGtid, error) {
	live := startVStreamLiveness(ctx, s.livenessWindow, s.copyProgressWindow, s.idleWarnWindow,
		func() {
			err := vstreamLivenessTimeoutError(s.livenessWindow, topodata.TabletType_PRIMARY, s.keyspace, s.shards)
			s.setErr(err)
			cancel()
		},
		func() {
			err := vstreamProgressTimeoutError(s.copyProgressWindow, topodata.TabletType_PRIMARY, s.keyspace, s.shards)
			s.setErr(err)
			cancel()
		},
		func() {
			slog.WarnContext(ctx, vstreamIdleWarnMessage(s.idleWarnWindow, s.keyspace, s.shards))
		})
	defer live.stop()

	lastCheckpoint := time.Now()
	reconnectAttempts := 0

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		resp, err := s.grpcStream.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			classified := classifyReaderError(fmt.Errorf("mysql/vstream: snapshot: copy recv (table %q): %w", table, err))
			var re ir.RetriableError
			if errors.As(classified, &re) && reconnectAttempts < s.reconnectMax {
				reconnectAttempts++
				if rerr := s.reconnectCopy(ctx, reconnectAttempts); rerr != nil {
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
			done, derr := s.dispatchCopyEvent(ev)
			if derr != nil {
				return nil, derr
			}
			if done {
				// This table's COPY is complete. Snapshot its captured
				// VGTID under mu (the pump is the sole writer, but reading
				// it under the same lock keeps the contract uniform).
				s.mu.Lock()
				snap := make([]shardGtid, len(s.currentVgtid))
				copy(snap, s.currentVgtid)
				s.mu.Unlock()
				if len(snap) == 0 {
					// A COPY_COMPLETED with no observed VGTID means vtgate
					// emitted no GTID for this (possibly empty) table. Fall
					// back to the from-beginning sentinel for the resolved
					// shard layout: the stitch treats "" as the dominating
					// minimum (resume the keyspace from the beginning — the
					// most conservative, no-gap choice).
					snap = fromBeginningVStreamPos(s.keyspace, s.shards)
				}
				return snap, nil
			}
		}
		if copiedRow {
			reconnectAttempts = 0
		}
		lastCheckpoint = s.maybeCheckpoint(ctx, lastCheckpoint)
	}
}

// reopenForTable closes the current per-table COPY stream and opens a
// fresh one scoped to the next table (auto-shard, ADR-0095). The gRPC
// CONNECTION is reused; only the stream is replaced — the same in-place
// pattern [reconnectCopy] uses, but seeded from-beginning for the new
// table (each single-table COPY is independent) rather than from a resume
// cursor. copyTables is rewritten to the new single table so a subsequent
// [reconnectCopy] re-applies the correct (one-table) scope.
func (s *vstreamSnapshotStream) reopenForTable(ctx context.Context, table string) error {
	return s.reopenForTableSeeded(ctx, table, nil)
}

// reopenForTableSeeded is [reopenForTable] with an explicit start position:
// nil (the cold-start / fresh-table case) seeds the per-table COPY from the
// beginning; a non-nil seed (ADR-0098 resume) seeds it from the persisted
// per-shard cursor (Gtid + TablePKs) so vtgate continues the in-progress
// table's COPY scan from the last-copied PK rather than restarting at row 0.
// The seed carries TablePKs for exactly the one in-progress table; that is
// the correct per-table cursor because in auto-shard mode the stream is
// scoped to a single table, so vtgate's resume clause names only that table.
func (s *vstreamSnapshotStream) reopenForTableSeeded(ctx context.Context, table string, seed []shardGtid) error {
	startPos := seed
	if startPos == nil {
		startPos = fromBeginningVStreamPos(s.keyspace, s.shards)
	}
	protoShardGtids, err := toProtoShardGtids(startPos)
	if err != nil {
		return fmt.Errorf("mysql/vstream: snapshot: auto-shard: build start position for %q: %w", table, err)
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
		return fmt.Errorf("mysql/vstream: snapshot: auto-shard: open stream for %q: %w", table, err)
	}
	s.mu.Lock()
	s.grpcStream = grpcStream
	s.copyTables = []string{table}
	s.mu.Unlock()
	if seed != nil {
		slog.DebugContext(ctx, "mysql/vstream: snapshot: auto-shard resumed in-progress table COPY from cursor",
			slog.String("keyspace", s.keyspace),
			slog.String("table", table))
	} else {
		slog.DebugContext(ctx, "mysql/vstream: snapshot: auto-shard advanced to next table COPY",
			slog.String("keyspace", s.keyspace),
			slog.String("table", table))
	}
	return nil
}

// finishCopyAutoShard stitches the captured per-table snapshot points
// into the single CDC-resume position (the per-shard GTID-set minimum,
// ADR-0095), records it onto the stream, and flips the global
// copyComplete so any straggler consumer unwedges.
//
// Happens-before to the orchestrator's post-bulk-copy Position read:
// this runs as a regular call inside the COPY pump, BEFORE the pump's
// deferred close(copyDone). The Position write here is under mu; the
// orchestrator joins copyDone (via WaitCopyComplete) before reading
// Position. The chain is: write Position under mu → close copyDone →
// handoff waits copyDone → handoff reads Position. NOTE the per-table
// ReadRows close (tableCopyComplete) does NOT order this write — the
// last ReadRows can return before this runs — so the copyDone join is
// the load-bearing edge, not the row-channel close.
func (s *vstreamSnapshotStream) finishCopyAutoShard(stream *ir.SnapshotStream) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err == nil {
		startShards, err := stitchSnapshotMin(s.perTableSnapshots)
		if err != nil {
			s.err = fmt.Errorf("mysql/vstream: snapshot: auto-shard stitch: %w", err)
		} else {
			pos, encErr := encodeVStreamPos(startShards)
			if encErr != nil {
				s.err = fmt.Errorf("mysql/vstream: snapshot: auto-shard encode stitched position: %w", encErr)
			} else {
				stream.Position = pos
				// The CDC tail resumes from the stitched minimum, so seed
				// currentVgtid with it — StreamChanges reuses the same
				// in-flight stream, and the post-COPY pump advances from
				// here.
				s.currentVgtid = startShards
			}
		}
	}
	s.copyComplete = true
	s.broadcast()
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
		Filter:     &binlogdata.Filter{Rules: vstreamCopyFilterRules(s.copyTables)},
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

// reopenAfterReshard rebuilds the post-COPY CDC stream against the new
// shard layout carried by a [ShardLayoutChangedError] (ADR-0094). It is
// the cold-start counterpart of [vstreamCDCReader.Reopen]: the production
// cold-start path hands the Streamer a [vstreamSnapshotChanges] (this
// stream's CDC half), NOT the standalone reader, so the reshard-follow
// capability must live here too — otherwise a reshard during a
// cold-started sync hits the loud-terminal exit ADR-0094 set out to
// replace (found by the Streamer-level reshard e2e test).
//
// Like Reopen it reuses the gRPC CONNECTION and replaces only the STREAM,
// seeded from the journal-stamped per-shard GTIDs (no gap/overlap at the
// cut). It cancels the old (dead) stream, installs a fresh streamCtx +
// grpcCancel so [close] still tears the new stream down, resets the
// layout/cursor/field-cache/error under mu, and starts a new CDC pump on a
// fresh channel. The previous pump already closed its channel (that close
// is what surfaced the reshard), so there is never a second concurrent
// pump.
func (s *vstreamSnapshotStream) reopenAfterReshard(ctx context.Context, resh *ShardLayoutChangedError) (<-chan ir.Change, error) {
	if resh == nil || len(resh.NewShards) == 0 {
		return nil, errors.New("mysql/vstream: snapshot: reopen: no new shards in journal")
	}

	// Tear down the old (dead) stream before opening the replacement so
	// the position bookkeeping never has two streams against one keyspace.
	s.cancelGRPCStream()

	protoShardGtids, err := toProtoShardGtids(resh.NewShards)
	if err != nil {
		return nil, fmt.Errorf("mysql/vstream: snapshot: reopen: build resume position: %w", err)
	}
	req := &vtgate.VStreamRequest{
		TabletType: topodata.TabletType_PRIMARY,
		Vgtid:      &binlogdata.VGtid{ShardGtids: protoShardGtids},
		Filter:     &binlogdata.Filter{Rules: vstreamCopyFilterRules(s.copyTables)},
		Flags: &vtgate.VStreamFlags{
			MinimizeSkew:      true,
			StopOnReshard:     true,
			HeartbeatInterval: 5,
		},
	}

	// Fresh streamCtx so CloseFn ([close]) can still cancel the new stream.
	streamCtx, streamCancel := context.WithCancel(context.Background())
	grpcStream, err := s.client.VStream(streamCtx, req)
	if err != nil {
		streamCancel()
		return nil, fmt.Errorf("mysql/vstream: snapshot: reopen: open stream: %w", err)
	}

	newShards := make([]string, 0, len(resh.NewShards))
	for _, sg := range resh.NewShards {
		newShards = append(newShards, sg.Shard)
	}
	newVgtid := make([]shardGtid, len(resh.NewShards))
	copy(newVgtid, resh.NewShards)

	s.mu.Lock()
	s.err = nil // observed via Err(); clearing avoids masking a future failure
	s.shards = newShards
	s.currentVgtid = newVgtid
	clear(s.fields) // post-reshard tablets re-emit FIELD events
	s.grpcStream = grpcStream
	s.grpcCancel = streamCancel
	s.mu.Unlock()

	slog.InfoContext(ctx, "mysql/vstream: snapshot: reopened CDC stream on new shard layout",
		slog.String("keyspace", s.keyspace),
		slog.Int("new_shards", len(newShards)))

	out := make(chan ir.Change, vstreamChannelBuffer)
	go s.pump(ctx, out)
	return out, nil
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
	// The persisted position is the DURABLE-WRITE watermark (v0.99.9), NOT
	// the pump's received frontier (currentVgtid). durableCheckpointLocked
	// returns the highest breadcrumb position the consumer has fully
	// written to the target, or ok=false when no breadcrumb is durable yet
	// (the consumer is still catching up to the first VGTID boundary). In
	// the not-yet-durable case we leave the cadence counter alone so the
	// next tick re-evaluates as soon as the consumer advances — persisting
	// nothing is strictly safe (an absent/older checkpoint only ever
	// resumes EARLIER, which the idempotent COPY writer absorbs).
	pos, ok := s.durableCheckpointLocked()
	if !ok {
		s.mu.Unlock()
		// Reset the wall-clock anchor but NOT rowsSinceCheckpoint: the
		// rows are buffered, just not yet durable, so the N-rows trigger
		// should stay armed until a durable breadcrumb exists.
		return time.Now()
	}
	s.rowsSinceCheckpoint = 0
	s.mu.Unlock()

	if err := fn(ctx, pos); err != nil {
		// Non-fatal: log and keep copying. The watermark is re-attempted
		// on the next cadence tick, and the final fully-durable position
		// is persisted at COPY_COMPLETED regardless.
		slog.WarnContext(ctx, "mysql/vstream: snapshot: COPY checkpoint write failed; continuing",
			slog.String("error", err.Error()))
	}
	return time.Now()
}

// recordBreadcrumbLocked appends a durable-watermark breadcrumb pairing
// the just-arrived position with the rows it covers (the pump's current
// enqueuedRows). Caller holds s.mu. Consecutive VGTIDs that arrive with
// NO intervening rows (enqueuedRows unchanged) collapse onto the same
// rowsCovered: keep the latest (further-along) position for that count
// by overwriting the tail rather than appending a redundant entry.
func (s *vstreamSnapshotStream) recordBreadcrumbLocked(pos ir.Position) {
	n := len(s.posBreadcrumbs)
	if n > 0 && s.posBreadcrumbs[n-1].rowsCovered == s.enqueuedRows {
		s.posBreadcrumbs[n-1].pos = pos
		return
	}
	s.posBreadcrumbs = append(s.posBreadcrumbs, posBreadcrumb{
		rowsCovered: s.enqueuedRows,
		pos:         pos,
	})
}

// durableCheckpointLocked returns the highest breadcrumb position whose
// covered rows are all durably written (rowsCovered <= durableRows),
// pruning every breadcrumb at-or-below it (a later checkpoint only ever
// moves forward). ok is false when no breadcrumb is durable yet. Caller
// holds s.mu.
//
// This is the load-bearing invariant of the v0.99.9 fix: the returned
// position is <= the position of the last row the consumer durably
// committed, so a crash + resume restarts at-or-before the last durable
// row and the idempotent COPY writer absorbs the re-copied overlap.
func (s *vstreamSnapshotStream) durableCheckpointLocked() (ir.Position, bool) {
	idx := -1
	for i := range s.posBreadcrumbs {
		if s.posBreadcrumbs[i].rowsCovered <= s.durableRows {
			idx = i
			continue
		}
		break
	}
	if idx < 0 {
		return ir.Position{}, false
	}
	pos := s.posBreadcrumbs[idx].pos
	// Prune the consumed prefix (keep everything past idx). Copy the tail
	// down so the backing array's head entries are GC-eligible.
	rest := s.posBreadcrumbs[idx+1:]
	s.posBreadcrumbs = append(s.posBreadcrumbs[:0], rest...)
	return pos, true
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
		// Record a durable-watermark breadcrumb (v0.99.9): Vitess emits
		// this VGTID AFTER every row it resumes past, so all
		// s.enqueuedRows rows buffered so far are covered by this
		// position. The checkpoint may persist it once the consumer has
		// durably written that many rows. encodeVStreamPos can fail only
		// on a position that won't marshal — the same terminal condition
		// finishCopy guards; surface it loudly rather than silently
		// dropping the breadcrumb (which would let the checkpoint fall
		// back to an older, but still durable, position — safe, yet a
		// won't-encode position is a real fault worth surfacing).
		if len(s.currentVgtid) > 0 {
			pos, err := encodeVStreamPos(s.currentVgtid)
			if err != nil {
				return false, fmt.Errorf("mysql/vstream: snapshot: encode checkpoint breadcrumb: %w", err)
			}
			s.recordBreadcrumbLocked(pos)
		}
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
		row, ok, err := decodeVStreamRow(rc.GetAfter(), fields, tableName, s.boolWarn, s.zeroDate)
		if err != nil {
			return err
		}
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
		//
		// Auto-shard mode (ADR-0095) NEVER hits this: the pump copies
		// exactly one table at a time, so the only rows in flight are this
		// table's, and a consumer is (or imminently will be) draining it —
		// backpressure-wait is always the correct response. The
		// cross-table refusal is the single-stream-interleave guard only;
		// suppressing it here also avoids a benign false-positive in the
		// window between one table's COPY_COMPLETED and the next ReadRows
		// setting activeTable (the pump may enqueue the next table's first
		// rows while activeTable still names the just-finished table).
		if len(s.copyTablesSeq) == 0 && s.activeTable != "" && s.activeTable != tableName {
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
	// Advance the monotonic enqueued-row counter (v0.99.9): the next
	// VGTID breadcrumb records this value as the rows it covers, and the
	// durable watermark gates the checkpoint on it.
	s.enqueuedRows++
	s.broadcast()
	return nil
}

// waitCopyComplete blocks until the COPY pump closes copyDone — the
// barrier the cold-start handoff joins (via
// [ir.SnapshotStream.WaitCopyComplete]) after bulk-copy drains and
// BEFORE it reads stream.Position. copyDone is closed exactly once by
// the COPY pump, AFTER finishCopy / finishCopyAutoShard has written
// Position under mu (and seeded currentVgtid), so the join establishes:
// producer writes Position under mu → closes copyDone → handoff waits
// copyDone → handoff reads Position. This is the load-bearing edge on
// the auto-shard / concurrent paths, where each ReadRows closes on a
// PER-TABLE signal and the last ReadRows can return before Position is
// written; on the single-stream path copyDone is already closed by the
// time the handoff calls this, so it returns immediately. ctx-cancellable
// so a shutdown mid-wait unwedges; idempotent (a closed channel keeps
// returning). A nil copyDone (never constructed) is a no-op.
func (s *vstreamSnapshotStream) waitCopyComplete(ctx context.Context) error {
	if s.copyDone == nil {
		return nil
	}
	select {
	case <-s.copyDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

	if err := s.waitCopyComplete(ctx); err != nil {
		return nil, err
	}
	if err := s.Err(); err != nil {
		return nil, err
	}

	// Auto-shard mode (ADR-0095): the COPY phase left the gRPC stream
	// scoped to the LAST per-table COPY (Match:<lastTable>) and positioned
	// at that table's GTID — neither the keyspace-wide scope nor the
	// stitched resume position the CDC tail needs. Open a FRESH
	// keyspace-wide CDC stream seeded from the stitched minimum
	// (currentVgtid) so the tail covers every table, gapless, from
	// P_start. The single-stream path skips this — its one stream is
	// already keyspace-wide / single-table and at the right position, so
	// it just resumes in place.
	if len(s.copyTablesSeq) > 0 {
		if err := s.reopenForCDC(ctx); err != nil {
			return nil, err
		}
	}

	out := make(chan ir.Change, vstreamChannelBuffer)
	go s.pump(ctx, out)
	return out, nil
}

// reopenForCDC replaces the last per-table COPY stream with a fresh
// keyspace-wide CDC stream seeded from the stitched resume position
// (auto-shard, ADR-0095). It is the auto-shard handoff: the per-table
// COPY streams were each scoped to one table, but the CDC tail must
// follow EVERY table from the stitched minimum P_start. The gRPC
// connection is reused; only the stream is replaced. currentVgtid holds
// P_start (seeded by [finishCopyAutoShard]); a from-scratch keyspace
// filter ("/.*/") with that position resumes CDC for the whole keyspace.
func (s *vstreamSnapshotStream) reopenForCDC(ctx context.Context) error {
	s.mu.Lock()
	resume := make([]shardGtid, len(s.currentVgtid))
	copy(resume, s.currentVgtid)
	s.mu.Unlock()
	if len(resume) == 0 {
		return errors.New("mysql/vstream: snapshot: auto-shard CDC handoff: no stitched resume position")
	}
	protoShardGtids, err := toProtoShardGtids(resume)
	if err != nil {
		return fmt.Errorf("mysql/vstream: snapshot: auto-shard CDC handoff: build resume position: %w", err)
	}
	req := &vtgate.VStreamRequest{
		TabletType: topodata.TabletType_PRIMARY,
		Vgtid:      &binlogdata.VGtid{ShardGtids: protoShardGtids},
		// Keyspace-wide: the CDC tail follows every table, not just the
		// last one copied. ADR-0073 internal-table exclusion still strips
		// _vt_* events in the CDC dispatch.
		Filter: &binlogdata.Filter{Rules: vstreamCopyFilterRules(nil)},
		Flags: &vtgate.VStreamFlags{
			MinimizeSkew:      true,
			StopOnReshard:     true,
			HeartbeatInterval: 5,
		},
	}
	grpcStream, err := s.client.VStream(ctx, req)
	if err != nil {
		return fmt.Errorf("mysql/vstream: snapshot: auto-shard CDC handoff: open stream: %w", err)
	}
	s.mu.Lock()
	s.grpcStream = grpcStream
	clear(s.fields) // the keyspace-wide stream re-emits FIELD per table
	s.mu.Unlock()
	slog.InfoContext(ctx, "mysql/vstream: snapshot: auto-shard CDC handoff (keyspace-wide tail from stitched position)",
		slog.String("keyspace", s.keyspace))
	return nil
}

// pump owns out and closes it before returning. The CDC phase
// reuses the same gRPC stream the COPY phase drained; events still
// flow against the cached field map (which may grow if new tables
// surface or FIELD events refresh on DDL).
//
// Continuous two-phase liveness watchdog (ADR-0073 (b2)+(F3)): symmetric
// with the COPY pump and the standalone CDC tail. Phase 1 (s.livenessWindow)
// guards the first serving-proof event; Phase 2 (s.progressWindow — the
// CDC-tail window, NOT the generous COPY one) fires on total mid-stream
// silence once serving is proven. The no-REPLICA wedge cannot fire here
// (this CDC phase reuses the PRIMARY stream the COPY phase already proved
// live), but a post-failover dead-Recv is the same silent-hang hazard, so
// the guard stays. On timeout it records a loud error and cancels the gRPC
// stream so the parked Recv unblocks.
func (s *vstreamSnapshotStream) pump(ctx context.Context, out chan<- ir.Change) {
	defer close(out)

	live := startVStreamLiveness(ctx, s.livenessWindow, s.progressWindow, s.idleWarnWindow,
		func() {
			s.setErr(vstreamLivenessTimeoutError(s.livenessWindow, topodata.TabletType_PRIMARY, s.keyspace, s.shards))
			s.cancelGRPCStream()
		},
		func() {
			s.setErr(vstreamProgressTimeoutError(s.progressWindow, topodata.TabletType_PRIMARY, s.keyspace, s.shards))
			s.cancelGRPCStream()
		},
		func() {
			// SOFT idle-WARN (item 19(a)): heartbeats flowing but no change
			// events for the soft window on the post-COPY CDC tail.
			// OBSERVABILITY ONLY — do NOT setErr, do NOT cancel; the tail
			// stays resilient and catches up when events resume.
			slog.WarnContext(ctx, vstreamIdleWarnMessage(s.idleWarnWindow, s.keyspace, s.shards))
		})
	defer live.stop()

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
		// Feed the watchdog: a non-heartbeat event clears Phase 1; any
		// event re-arms the Phase-2 progress deadline (ADR-0073 (b2)+(F3)).
		if evs := resp.GetEvents(); len(evs) > 0 {
			live.observe(eventsProveLiveness(evs))
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
		// F7c: emit the ADR-0049 SchemaSnapshot boundary on a true-delta
		// FIELD signature change, exactly as [vstreamCDCReader.dispatch]
		// does. Without this the cold-start→CDC path silently dropped the
		// boundary, so an online ADD/DROP/MODIFY COLUMN after a VStream
		// cold-start never reached the ADR-0091 schema-forward intercept.
		return s.maybeSnapshotSchemaCDC(ctx, fe, out)

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

// maybeSnapshotSchemaCDC is the post-COPY counterpart to
// [vstreamCDCReader.maybeSnapshotSchema] (ADR-0049 Chunk B2). On every
// post-COPY FIELD event it projects the field metadata into an
// [ir.Table] and emits an [ir.SchemaSnapshot] iff the projected
// (column-name, ordered-type) signature is a TRUE DELTA against the last
// one emitted for this (shard, table). VStream re-emits FIELD on stream
// (re)start / first-touch, so the true-delta gate (snapshotSig) keeps a
// no-op re-emit from writing a phantom version — the same dedup the
// standalone reader uses.
//
// F7c: this is the boundary signal the cold-start→CDC path was missing.
// The standalone [vstreamCDCReader] emits it from its FIELD branch, but
// the snapshot stream's [dispatchCDCEvent] FIELD branch only cached the
// fields, so an online ADD/DROP/MODIFY COLUMN after a VStream cold-start
// never produced a SchemaSnapshot — the ADR-0091 schema-forward
// intercept (and the ADR-0049 schema-history write) never saw it. The
// post-DDL ROW then decoded with the new field set against a target
// whose schema was never altered (SQLSTATE 42703 on PG / 1054 on
// MySQL). The logic here is a faithful mirror of the standalone reader's
// method, reusing the same projectVStreamFields / SchemaSignatureOf /
// keyspace-gate machinery rather than duplicating a new projection.
//
// The caller (the CDC pump) holds no lock; snapshotSig and fields are
// touched only by this single pump goroutine in the post-COPY phase, so
// no additional locking is required here (the COPY pump that shared the
// mu has already exited by the time the CDC pump runs).
func (s *vstreamSnapshotStream) maybeSnapshotSchemaCDC(ctx context.Context, fe *binlogdata.FieldEvent, out chan<- ir.Change) error {
	keyspace := fe.GetKeyspace()
	if keyspace == "" {
		keyspace = s.keyspace
	}
	// The stream is bound to a single keyspace via the DSN; a FIELD event
	// for an unrelated keyspace carries no table the applier could host a
	// schema-history row for. Skip — symmetric with the standalone
	// reader's keyspace gate.
	if keyspace != s.keyspace {
		return nil
	}
	table := stripKeyspaceFromTable(fe.GetTableName(), keyspace)

	tbl, err := projectVStreamFields(keyspace, table, fe.GetFields())
	if err != nil {
		if errors.Is(err, errFieldMetadataUnavailable) {
			// Position-anchored metadata absent on this FIELD event —
			// degrade to the safe floor (no version written). NOT fatal:
			// the loud ROW-without-FIELD floor (dispatchCDCRow) is
			// untouched. Mirrors maybeSnapshotSchema.
			return nil
		}
		// A present-but-unmappable ColumnType is a genuine unknown type —
		// fatal/loud (the loud-failure tenet).
		return err
	}

	if s.snapshotSig == nil {
		s.snapshotSig = make(map[string]ir.SchemaSignature)
	}
	cacheKey := fieldCacheKey(fe.GetShard(), fe.GetTableName())
	sig := ir.SchemaSignatureOf(tbl)
	if prev, ok := s.snapshotSig[cacheKey]; ok && prev.Equal(sig) {
		// No-op FIELD re-emit (restart / first-touch / reconnect with no
		// DDL): not a true delta — do NOT write a new version.
		return nil
	}

	pos, err := s.positionFor()
	if err != nil {
		return err
	}

	if err := send(ctx, out, ir.SchemaSnapshot{
		Position: pos,
		Schema:   keyspace,
		Table:    table,
		IR:       tbl,
	}); err != nil {
		return err
	}
	s.snapshotSig[cacheKey] = sig
	return nil
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
		before, beforeOK, err := decodeVStreamRow(rc.GetBefore(), fields, tableName, s.boolWarn, s.zeroDate)
		if err != nil {
			return err
		}
		after, afterOK, err := decodeVStreamRow(rc.GetAfter(), fields, tableName, s.boolWarn, s.zeroDate)
		if err != nil {
			return err
		}
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

// cancelGRPCStream cancels the gRPC stream context so a parked Recv
// unblocks, WITHOUT closing the connection. Used by the liveness watchdog
// to tear down a dead/silent stream after recording its loud error.
// Reads s.grpcCancel under mu (close also writes it) and invokes it
// without niling — context.CancelFunc is idempotent, so a concurrent
// close() calling it again is harmless.
func (s *vstreamSnapshotStream) cancelGRPCStream() {
	s.mu.Lock()
	cancel := s.grpcCancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
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
	// Read+clear grpcCancel under mu — the liveness watchdog
	// (cancelGRPCStream) reads it under the same lock, so an unguarded
	// access here would race it.
	s.mu.Lock()
	cancel := s.grpcCancel
	s.grpcCancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
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

// AdvanceDurableRows implements [ir.CopyDurableProgressSink] (v0.99.9).
// The pipeline hands this method to the bulk-copy writer as its
// [ir.CopyDurableProgressFunc]; the writer calls it after each successful
// flush with the per-flush DELTA of rows it has DURABLY committed. The
// sink sums the deltas into the global durable frontier (durableRows).
// The checkpoint (maybeCheckpoint) persists only breadcrumbs whose
// covered rows are at-or-below that frontier, so the resume cursor can
// never run ahead of the durably-written rows.
//
// Per-flush deltas sum cleanly across tables — the writer is invoked once
// per table and its internal counters restart each call, but the running
// sum here is global, matching the pump's global enqueuedRows
// breadcrumbs. A non-positive delta is ignored (the writer never reports
// an empty flush, but the guard keeps the frontier monotonic regardless).
func (r *vstreamSnapshotRows) AdvanceDurableRows(flushedRows int64) {
	if flushedRows <= 0 {
		return
	}
	s := r.snap
	s.mu.Lock()
	s.durableRows += flushedRows
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

// ConcurrentCopyGroups implements [ir.ConcurrentCopyPartitioner]
// (ADR-0100). It returns the disjoint table partition the concurrent
// producer driver runs (ADR-0099) so the pipeline can run one read→write
// consumer pipeline per group CONCURRENTLY (W = K), instead of draining
// every table through one serial consumer (the ~1.4× ceiling). The groups
// are the EXACT [][]string copyPumpAutoShardConcurrent partitions the
// producers into (stored at open), so the consumer partition ≡ the
// producer partition — coverage + disjointness inherited from ADR-0099's
// unit-pinned partition, never re-derived here. nil on every
// non-concurrent path (K = 1 / single-stream / one-table scope); the
// pipeline then runs the serial table loop byte-identically.
func (r *vstreamSnapshotRows) ConcurrentCopyGroups() [][]string {
	return r.snap.concurrentGroups
}

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
	//
	// ADR-0100: activeTable governs ONLY the sequential single-stream
	// interleave guard (enqueueRowLocked, gated on copyTablesSeq == 0). On
	// the CONCURRENT path (tableStreamIdx != nil) W consumers call ReadRows
	// at once and would race-clobber this single field — so we don't touch
	// it there (the concurrent pump uses per-stream byte sub-budgets, never
	// activeTable, so the field is dead state on that path). Skipping the
	// write keeps activeTable's invariant clean (it always names the one
	// active table on the sequential path) and avoids W goroutines fighting
	// over one field for no purpose.
	concurrent := s.concurrentCopy
	if !concurrent {
		s.mu.Lock()
		s.activeTable = tableName
		s.cond.Broadcast()
		s.mu.Unlock()
	}

	out := make(chan ir.Row)
	go func() {
		defer close(out)
		defer func() {
			if concurrent {
				return
			}
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
			//
			// "Completion" for THIS table is either the global copyComplete
			// (single-stream path) OR this table's per-table COPY finishing
			// (auto-shard, ADR-0095 — tableCopyComplete[tableName]). The
			// per-table signal is essential: in auto-shard mode the pump
			// has moved on to the NEXT table's COPY, so a consumer draining
			// this table must close on the per-table signal — the global
			// copyComplete only flips after EVERY table, far too late.
			for len(s.rowBuffer[tableName]) == 0 && !s.copyComplete &&
				!s.tableCopyComplete[tableName] && s.err == nil && ctx.Err() == nil {
				s.cond.Wait()
			}
			queue := s.rowBuffer[tableName]
			if len(queue) == 0 {
				// Empty queue + (complete | per-table-complete | error |
				// cancelled): the table is fully delivered. Drop its now-
				// empty entry so a second ReadRows returns immediately.
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
			rowBytes := ir.ApproximateRowBytes(row)
			s.bufferedBytes -= rowBytes
			// Concurrent COPY (ADR-0099): also credit the PRODUCING stream's
			// own sub-budget so its backpressure (enqueueConcurrentRowLocked,
			// which waits on perStreamBytes[idx] vs perStreamCap) is relieved
			// by this drain. tableStreamIdx is nil on the sequential paths, so
			// this is a clean no-op there. The disjoint partition guarantees
			// exactly one producing stream per table.
			if s.tableStreamIdx != nil {
				if idx, ok := s.tableStreamIdx[tableName]; ok {
					s.perStreamBytes[idx] -= rowBytes
				}
			}
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

// Err exposes the underlying snapshot stream's terminal pump error so the
// pipeline's optional-Err probe (`cdc.(interface{ Err() error })`) and tests
// can SEE a loud CDC-pump failure on the cold-start path — e.g. the F3
// mid-stream progress-timeout (vstreamProgressTimeoutError) after a
// failover-induced stream wedge. Without this delegation the wrapper has no
// Err(), the probe finds nothing, and a loud pump failure is silently
// swallowed — the sync appears to stall with no surfaced error. Mirrors the
// standalone vstreamCDCReader.Err() contract.
func (c *vstreamSnapshotChanges) Err() error { return c.snap.Err() }

// ReopenAfterReshard implements [ir.ReshardReopener] on the cold-start
// CDC half (ADR-0094). The production cold-start path hands the Streamer
// THIS wrapper (not the standalone *vstreamCDCReader), so the reshard-
// follow capability must be exposed here or the Streamer's
// `stream.Changes.(ir.ReshardReopener)` probe fails and a reshard during
// a cold-started sync exits loud-terminal. Inspects the underlying
// stream's cached Err(); on a reshard signal it rebuilds the stream
// against the new layout via [vstreamSnapshotStream.reopenAfterReshard],
// else reports ok=false so the caller settles the error normally.
func (c *vstreamSnapshotChanges) ReopenAfterReshard(ctx context.Context) (changes <-chan ir.Change, wasReshard bool, err error) {
	var resh *ShardLayoutChangedError
	if !errors.As(c.snap.Err(), &resh) {
		return nil, false, nil
	}
	ch, rerr := c.snap.reopenAfterReshard(ctx, resh)
	if rerr != nil {
		return nil, true, rerr
	}
	return ch, true, nil
}

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
