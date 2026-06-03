// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Source-side sluice_heartbeat writer — severity-A finding F17 of the
// 2026-05-22 Reddit-research run. See ADR-0061.
//
// The streamer attaches a background goroutine that periodically INSERTs
// a row into a sluice-owned table on the source database. The INSERT
// generates WAL (Postgres) / binlog (MySQL) traffic so the consumer's
// position advances even against an otherwise-idle source, preventing
// slot-eviction / binlog-rotation failure cascades on low-traffic
// streams.
//
// **Opt-in by default.** F17's INSERTs are a behaviour change on the
// source DB — operators must explicitly enable via
// `--source-heartbeat-interval=DUR` (a zero / unset interval leaves the
// source untouched). The opt-in default is the safer posture per
// CLAUDE.md's loud-failure tenet: a brand-new sluice user pointed at a
// regulated source shouldn't suddenly observe new tables and writes.
//
// **Non-fatal on every branch.** Missing engine surface (engine doesn't
// implement [ir.HeartbeatWriter]), failed source open, insufficient
// privilege on the source — all degrade to "WARN once, skip the writer"
// rather than failing the streamer. The CDC consumer still works
// without F17; the heartbeat just doesn't fire.
//
// The wiring mirrors [Streamer.attachSlotHealthProbe] (F13 / ADR-0059):
// per-stream dedicated source connection, per-stream goroutine, cleanup
// closure released on streamer teardown. The two reporters share the
// SchemaReader surface but operate on independent interfaces — each
// engine can implement one without the other.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// DefaultSourceHeartbeatTableName is the source-side table the per-
// stream heartbeat writer INSERTs into. The convention mirrors
// `sluice_cdc_state` on the target — a single, predictable, sluice-
// owned name operators can grep for in `information_schema` / pg_class.
// Configurable via the CLI's `--source-heartbeat-table-name` flag.
const DefaultSourceHeartbeatTableName = "sluice_heartbeat"

// DefaultSourceHeartbeatPruneWindow is the default age threshold for
// the periodic DELETE that bounds heartbeat-table growth. One hour is
// long enough to retain a window of heartbeats for ad-hoc operator
// inspection (`SELECT MAX(ts) FROM sluice_heartbeat` answering "when
// was the last write?") while keeping the table small. 0 in the CLI
// flag disables prune entirely; this constant is the production
// default the help text references.
const DefaultSourceHeartbeatPruneWindow = time.Hour

// sourceHeartbeatPruneCadence is the wall-clock interval between
// prune passes (independent of the write cadence). Pruning every minute
// is sufficient — heartbeat rows are tiny and the table's worst-case
// row count is `(60s / write_interval) * 60` per minute, which at a
// 1s write interval is 3600 rows/min — trivial.
//
// Exposed as a package-level var so integration tests can drive it
// down to sub-second cadence without sleeping.
var sourceHeartbeatPruneCadence = time.Minute

// sourceHeartbeatAttachment is the bundle the streamer holds onto so it
// can release resources (close the dedicated SchemaReader, cancel the
// goroutine ctx) when the stream tears down. Mirrors the cleanup-closure
// shape of [slotHealthProbeAttachment].
type sourceHeartbeatAttachment struct {
	cancel context.CancelFunc
	once   sync.Once
	close  func()
}

// Close releases the writer goroutine and its dedicated source-DB
// connection. Idempotent.
func (a *sourceHeartbeatAttachment) Close() {
	a.once.Do(func() {
		if a.cancel != nil {
			a.cancel()
		}
		if a.close != nil {
			a.close()
		}
	})
}

// sourceHeartbeatLoop is the per-stream background goroutine that
// drives the F17 writer. Owned by the streamer; exits cleanly on ctx
// cancellation. Errors from the underlying writer log at WARN (one tick
// silent-skip) and the loop advances — a transient INSERT failure
// shouldn't kill the goroutine. A *terminal* failure (insufficient
// privilege surfaced via [ir.ErrHeartbeatPermission]) exits the loop
// cleanly with a WARN; the stream continues without heartbeat.
//
// Prune cadence is independent of write cadence (one prune every
// [sourceHeartbeatPruneCadence] regardless of write interval). When
// pruneWindow <= 0 the prune branch is skipped entirely.
func sourceHeartbeatLoop(
	ctx context.Context,
	writer ir.HeartbeatWriter,
	tableName, streamID string,
	writeInterval, pruneWindow time.Duration,
) {
	writeTicker := time.NewTicker(writeInterval)
	defer writeTicker.Stop()

	// Prune ticker is created unconditionally but its channel is only
	// consumed when pruneWindow > 0. A separate ticker keeps the loop
	// simple: each tick triggers its own action without a derived clock
	// computation.
	pruneTicker := time.NewTicker(sourceHeartbeatPruneCadence)
	defer pruneTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-writeTicker.C:
			if err := writer.WriteHeartbeat(ctx, tableName, streamID); err != nil {
				if errors.Is(err, ir.ErrHeartbeatPermission) {
					slog.WarnContext(
						ctx, "source heartbeat: write privilege revoked — disabling writer",
						slog.String("stream_id", streamID),
						slog.String("table", tableName),
						slog.String("err", err.Error()),
						slog.String("see", "ADR-0061"),
					)
					return
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					// ctx tear-down — the next loop iter will see Done.
					continue
				}
				slog.WarnContext(
					ctx, "source heartbeat: write failed (will retry next tick)",
					slog.String("stream_id", streamID),
					slog.String("table", tableName),
					slog.String("err", err.Error()),
				)
			}
		case <-pruneTicker.C:
			if pruneWindow <= 0 {
				continue
			}
			deleted, err := writer.PruneHeartbeat(ctx, tableName, pruneWindow)
			if err != nil {
				if errors.Is(err, ir.ErrHeartbeatPermission) {
					// DELETE privilege revoked: the rest of the writer
					// can still INSERT, so we don't tear down — just
					// stop pruning by gating future ticks.
					slog.WarnContext(
						ctx, "source heartbeat: prune privilege missing — heartbeat table will grow unbounded",
						slog.String("stream_id", streamID),
						slog.String("table", tableName),
						slog.String("err", err.Error()),
					)
					// Stop the prune ticker so we don't keep retrying.
					pruneTicker.Stop()
					continue
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				slog.WarnContext(
					ctx, "source heartbeat: prune failed (will retry next pass)",
					slog.String("stream_id", streamID),
					slog.String("table", tableName),
					slog.String("err", err.Error()),
				)
				continue
			}
			if deleted > 0 {
				slog.DebugContext(
					ctx, "source heartbeat: prune complete",
					slog.String("stream_id", streamID),
					slog.String("table", tableName),
					slog.Int64("rows_deleted", deleted),
					slog.Duration("older_than", pruneWindow),
				)
			}
		}
	}
}

// attachSourceHeartbeat opens a source-side [ir.SchemaReader], type-
// asserts it to [ir.HeartbeatWriter], ensures the heartbeat table
// exists, and spawns a per-stream goroutine that periodically INSERTs.
// Returns a non-nil attachment so the caller can `defer attachment.Close()`
// unconditionally.
//
// Skipped on every of the following:
//
//   - The streamer has [Streamer.SourceHeartbeatInterval] <= 0 (the
//     opt-in default).
//   - The streamer has [Streamer.NoSourceHeartbeat] set true (operator
//     opted out at the flag level even if config supplied an interval).
//   - The streamer's source or source DSN is unset (test fixture).
//   - The source engine's SchemaReader doesn't implement
//     [ir.HeartbeatWriter] (no engine today; reserved for future
//     read-only engines).
//   - EnsureHeartbeatTable wraps [ir.ErrHeartbeatPermission] — the
//     operator's role lacks CREATE TABLE.
//
// Every skip path WARNs once and returns the noop attachment; the
// streamer continues without the writer.
//
// **Why a dedicated SchemaReader.** The CDC reader's connection lives
// in replication mode and isn't safe for ad-hoc INSERTs; opening a
// dedicated connection for the heartbeat keeps the two paths
// independent. The per-tick INSERT + bounded prune is cheap (one short
// statement each) so the dedicated connection's cost is trivial.
func (s *Streamer) attachSourceHeartbeat(ctx context.Context, streamID string) *sourceHeartbeatAttachment {
	noop := &sourceHeartbeatAttachment{}
	if s.SourceHeartbeatInterval <= 0 || s.NoSourceHeartbeat {
		return noop
	}
	if s.Source == nil || s.SourceDSN == "" {
		return noop
	}

	tableName := s.SourceHeartbeatTableName
	if tableName == "" {
		tableName = DefaultSourceHeartbeatTableName
	}
	pruneWindow := s.SourceHeartbeatPruneWindow // 0 means "no prune"

	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		slog.WarnContext(
			ctx, "source heartbeat: open source schema reader failed — skipping",
			slog.String("stream_id", streamID),
			slog.String("err", err.Error()),
			slog.String("see", "ADR-0061"),
		)
		return noop
	}
	writer, ok := sr.(ir.HeartbeatWriter)
	if !ok {
		// Engine doesn't expose the writer surface. Close the dedicated
		// reader so the connection doesn't sit idle.
		closeIf(sr)
		slog.WarnContext(
			ctx, "source heartbeat: source engine does not implement HeartbeatWriter — skipping",
			slog.String("stream_id", streamID),
			slog.String("engine", s.Source.Name()),
		)
		return noop
	}
	if err := writer.EnsureHeartbeatTable(ctx, tableName); err != nil {
		if errors.Is(err, ir.ErrHeartbeatPermission) {
			slog.WarnContext(
				ctx, "source heartbeat: insufficient privilege to create heartbeat table — skipping writer",
				slog.String("stream_id", streamID),
				slog.String("table", tableName),
				slog.String("hint", fmt.Sprintf(
					"the connecting role on the source lacks CREATE TABLE; either grant CREATE on the schema, pre-create %q manually, or set --no-source-heartbeat to silence this warning. F17 (idle-source slot/binlog protection) will not be active for this stream.",
					tableName,
				)),
				slog.String("see", "ADR-0061"),
				slog.String("err", err.Error()),
			)
			closeIf(sr)
			return noop
		}
		// Other DDL failures are also non-fatal — WARN, skip, continue.
		slog.WarnContext(
			ctx, "source heartbeat: EnsureHeartbeatTable failed — skipping writer",
			slog.String("stream_id", streamID),
			slog.String("table", tableName),
			slog.String("err", err.Error()),
			slog.String("see", "ADR-0061"),
		)
		closeIf(sr)
		return noop
	}

	probeCtx, cancel := context.WithCancel(ctx)
	att := &sourceHeartbeatAttachment{
		cancel: cancel,
		close:  func() { closeIf(sr) },
	}

	go sourceHeartbeatLoop(probeCtx, writer, tableName, streamID, s.SourceHeartbeatInterval, pruneWindow)

	slog.InfoContext(
		ctx, "source heartbeat writer attached",
		slog.String("stream_id", streamID),
		slog.String("table", tableName),
		slog.Duration("write_interval", s.SourceHeartbeatInterval),
		slog.Duration("prune_window", pruneWindow),
		slog.String("see", "ADR-0061"),
	)
	return att
}
