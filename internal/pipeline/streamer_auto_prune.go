// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/diagnose"
	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0137 Phase B — in-stream AUTO-PRUNE of a trigger-CDC source's change-log
// (Bug 165), as amended by the SOURCE-SIDE CONSUMER REGISTRY (roadmap item 115).
// The trigger engines (sqlite-trigger / d1-trigger / pgtrigger) capture every
// source change into `sluice_change_log` and never reap it, so the change-log
// grows unbounded for the life of a continuous sync (on D1 it is billable).
// Phase A shipped the operator-run `sluice trigger prune`; this sidecar makes it
// automatic so operators don't schedule a cron.
//
// The sidecar mirrors the failure-isolation shape of the ADR-0107 telemetry
// sidecars ([startTargetMetricsHistoryRecorder] / [startStorageHeadroomWatch]):
// a slow-tick goroutine that runs OFF the apply hot path and whose every error
// is logged at WARN and SWALLOWED — a prune failure must NEVER break or stall
// the sync. This is the one deliberate divergence from the Phase-A command,
// which fails LOUD: Phase A is an operator action, Phase B is background
// housekeeping.
//
// # The load-bearing safety rules
//
// (1) ADR-0137: never prune above the TARGET's durably-persisted frontier. The
// source reader's read cursor runs AHEAD of it, so pruning on the read cursor
// (or MAX(id), or a TTL) would delete not-yet-applied rows → silent permanent
// loss on warm-resume. The engine-side decode refuses a FOREIGN token loudly,
// and a non-positive cut is a safe no-op.
//
// (2) Item 115: never prune above ANY OTHER stream's frontier either. A source
// change log is shared by every sync reading that database, so cutting at THIS
// stream's frontier by change-log id deletes the rows between two frontiers
// before a slower peer reads them — silently, with no way for it to discover
// they existed. Every trigger-CDC stream therefore REGISTERS its own frontier in
// a source-side table ([Streamer.startChangeLogConsumerRegistration], running
// whether or not that stream opted into auto-prune), and the prune cuts at the
// MIN across all registered consumers.
//
// Rule (2) fails CLOSED: a source engine that exposes [ir.ChangeLogPruner]
// without the [ir.ChangeLogConsumerRegistry] companion does not get pruned at
// all. A fail-open default would recreate the defect for exactly the engine
// someone forgot to migrate.

const defaultAutoPruneInterval = 5 * time.Minute

// changeLogConsumerRefreshInterval is the cadence at which a trigger-CDC stream
// re-publishes its durably-applied frontier to the source-side registry. It is
// deliberately FASTER than the prune cadence (5 min default): the registry is
// what holds a peer's prune back, so a frontier that refreshes slowly costs
// change-log growth, while one that refreshes quickly costs one tiny upsert a
// minute (on D1, ~1.4k billable row-writes a day — the same order as a single
// second of capture traffic).
const changeLogConsumerRefreshInterval = 1 * time.Minute

// maxChangeLogConsumerID bounds the composed consumer id. The registry column is
// TEXT on every trigger engine, so this is politeness rather than a limit; it
// keeps operator-facing log lines and `SELECT * FROM sluice_change_log_consumers`
// readable when a stream id and a DSN are both long.
const maxChangeLogConsumerID = 512

// autoPruneGate bounds the auto-prune cadence to at most once per interval. The
// sidecar's base ticker already fires at the interval, so at steady state every
// tick is due; the gate makes the "at most once per interval, skip within"
// contract explicit and unit-testable with an injected clock, and keeps the
// cadence bounded if a future checkpoint-driven nudge is ever wired to prune
// sooner (an ADR-0137 Phase-B extension). It is not concurrency-safe; the single
// sidecar goroutine owns it.
type autoPruneGate struct {
	interval time.Duration
	last     time.Time
}

// due reports whether a prune is allowed at now, advancing the gate when it is.
// The first call (zero last) is always due, so the first prune happens on the
// first tick rather than after two intervals.
func (g *autoPruneGate) due(now time.Time) bool {
	if g.last.IsZero() || now.Sub(g.last) >= g.interval {
		g.last = now
		return true
	}
	return false
}

// ChangeLogConsumerID composes a stream's identity in the SOURCE-side change-log
// consumer registry. Exported because `sluice trigger prune` composes the SAME
// id for the stream its cut was derived from — the operator command and the
// sidecar must agree on which registry row belongs to that stream, or the
// command would clamp itself against a stale copy of its own frontier.
//
// The stream id alone is not enough: it is unique per TARGET (it keys
// the target's cdc-state row) and defaults to a source→target locator that drops
// the database name, so two syncs off one source into two databases on the same
// server can collide on it. Two colliding streams would share ONE registry row,
// the later writer would overwrite the earlier's frontier, and the prune would
// cut above the slower one — reintroducing exactly the bug the registry closes.
//
// Appending the redacted target locator (host[:port][/db], credentials stripped
// by [diagnose.RedactDSN]) makes the identity unique per (stream, target) with
// no hashing and nothing secret written to the source. A cosmetic DSN change
// (a different host spelling) mints a SECOND registry row rather than merging
// two streams into one — the safe direction: the stale row holds the prune back
// and is named in the staleness WARN, instead of silently unblocking it.
func ChangeLogConsumerID(streamID, targetEngine, targetDSN string) string {
	id := streamID + " -> " + targetEngine + "://" + diagnose.RedactDSN(targetDSN)
	if len(id) > maxChangeLogConsumerID {
		id = id[:maxChangeLogConsumerID]
	}
	return id
}

// captureChangeLogConsumerRegistry records the source reader's item-115
// companion surface when it implements one. Called from every site that opens a
// source stream — the cold-start snapshot open (earliest, so the copy window is
// covered), the cold-start CDC begin, and warm resume — because the
// registration sidecar must run for a trigger-CDC stream no matter which path
// brought it up. A non-trigger source implements nothing and leaves it nil,
// which is what makes the auto-prune's fail-closed branch quiet for them.
func (s *Streamer) captureChangeLogConsumerRegistry(reader any) {
	if r, ok := reader.(ir.ChangeLogConsumerRegistry); ok {
		s.changeLogConsumers = r
	}
}

// startChangeLogConsumerRegistration publishes this stream's durably-applied
// frontier into the SOURCE-side consumer registry on a cadence, so every OTHER
// sync reading the same change log can prune without deleting rows this one has
// not read (roadmap item 115).
//
// It runs for EVERY trigger-CDC stream whose source exposes
// [ir.ChangeLogConsumerRegistry] — deliberately NOT gated on
// --auto-prune-change-log. The defect this closes is asymmetric: the sync doing
// the pruning is the one with the flag, and the sync that loses rows is
// typically a plain `sluice sync` with no flag at all. Gating registration on
// the flag would leave that peer invisible, which is the whole bug.
//
// Idempotent per attempt: the first caller wins, so the cold-start path can
// start it BEFORE the bulk copy (a stream in cold copy has applied nothing, and
// its registered frontier of 0 blocks every peer's prune for the duration — the
// safe direction) while the warm-resume path starts it from the apply sidecars.
// Both callers run on the runOnce goroutine, so the guard needs no lock.
func (s *Streamer) startChangeLogConsumerRegistration(ctx context.Context, streamID string, applier ir.ChangeApplier) {
	registry := s.changeLogConsumers
	if registry == nil || s.changeLogConsumerStarted {
		return
	}
	s.changeLogConsumerStarted = true
	consumerID := ChangeLogConsumerID(streamID, s.Target.Name(), s.TargetDSN)
	logger := slog.Default()
	slog.InfoContext(
		ctx, "change-log consumer registry: publishing this stream's applied frontier to the source so a peer "+
			"sync's auto-prune cannot reap rows this stream has not read (roadmap item 115)",
		slog.String("stream_id", streamID),
		slog.String("consumer_id", consumerID),
		slog.Duration("interval", changeLogConsumerRefreshInterval),
	)
	// Register once immediately: a stream entering a multi-hour cold copy must
	// be visible to a peer's pruner from the start, not one cadence later.
	runChangeLogConsumerTick(ctx, logger, applier, registry, streamID, consumerID)
	go func() {
		ticker := time.NewTicker(changeLogConsumerRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runChangeLogConsumerTick(ctx, logger, applier, registry, streamID, consumerID)
			}
		}
	}()
}

// runChangeLogConsumerTick publishes one frontier refresh. Like the prune tick
// it is failure-isolated — a registry write that fails is logged at WARN and
// swallowed, never able to stall the sync — but the WARN is deliberately blunt:
// a stream that cannot register is INVISIBLE to a peer's pruner, which is the
// silent-loss shape item 115 exists to close, and the operator needs to know.
//
// A stream with no durable position yet (cold start, nothing applied) registers
// an EMPTY token, which the engine records as frontier 0 — "I have applied
// nothing; prune nothing on my account".
func runChangeLogConsumerTick(
	ctx context.Context,
	logger *slog.Logger,
	applier ir.ChangeApplier,
	registry ir.ChangeLogConsumerRegistry,
	streamID, consumerID string,
) {
	var token string
	pos, found, err := applier.ReadPosition(ctx, streamID)
	if err != nil {
		logger.WarnContext(
			ctx, "change-log consumer registry: could not read this stream's durable position; registering a "+
				"frontier of 0 for this cadence (conservative — it holds a peer's prune back, never advances it)",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()),
		)
	} else if found {
		token = pos.Token
	}
	if err := registry.RegisterChangeLogConsumer(ctx, consumerID, token); err != nil {
		logger.WarnContext(
			ctx, "change-log consumer registry: FAILED to publish this stream's applied frontier to the source. "+
				"Until it succeeds, another sync pruning this shared change log cannot see this stream and may "+
				"delete rows it has not read yet (roadmap item 115). Will retry next interval; the sync itself is "+
				"unaffected",
			slog.String("stream_id", streamID),
			slog.String("consumer_id", consumerID),
			slog.String("error", err.Error()),
		)
	}
}

// startAutoPruneChangeLog spawns the ADR-0137 Phase-B auto-prune sidecar for the
// stream. It mirrors [startTargetMetricsHistoryRecorder]'s shape: a total no-op
// (returns without spawning) unless the operator opted in AND the source is a
// trigger-CDC engine, a single goroutine otherwise, exiting on ctx.Done. The
// caller does not track the goroutine.
//
// No-op when:
//   - AutoPruneChangeLog is not set (the default — pre-Phase-B behaviour), or
//   - the source didn't expose an [ir.ChangeLogPruner] (a non-trigger source:
//     vanilla PG/MySQL/vitess have no change-log ⇒ s.changeLogPruner is nil), or
//   - the source exposed [ir.ChangeLogPruner] but NOT the item-115
//     [ir.ChangeLogConsumerRegistry] companion — the FAIL-CLOSED branch, which
//     logs loudly and prunes nothing.
//
// The first two preconditions make the zero-value construction (every test /
// broker / future caller that never sets the flag or streams a non-trigger
// source) a total no-op — the safe default by construction.
func (s *Streamer) startAutoPruneChangeLog(ctx context.Context, streamID string, applier ir.ChangeApplier) {
	if !s.AutoPruneChangeLog {
		return
	}
	pruner := s.changeLogPruner
	if pruner == nil {
		return
	}
	registry := s.changeLogConsumers
	if registry == nil {
		// FAIL CLOSED. This source can be pruned but cannot tell us who else is
		// reading its change log, so any prune we ran would be the unscoped cut
		// item 115 exists to remove. Refusing costs unbounded change-log growth
		// on an opted-in sync — loud, visible, recoverable — where pruning
		// anyway costs a peer's rows, silently and permanently.
		slog.ErrorContext(
			ctx, "auto-prune: REFUSING to prune — this source engine does not implement the change-log consumer "+
				"registry, so sluice cannot tell whether another sync reads the same change log. Pruning without "+
				"it could delete a peer sync's unread rows silently (roadmap item 115). The change log will keep "+
				"growing; upgrade the engine, or run `sluice trigger prune` deliberately",
			slog.String("stream_id", streamID),
		)
		return
	}
	interval := s.AutoPruneInterval
	if interval <= 0 {
		interval = defaultAutoPruneInterval
	}
	keep := s.AutoPruneKeep
	if keep < 0 {
		keep = 0
	}
	consumerID := ChangeLogConsumerID(streamID, s.Target.Name(), s.TargetDSN)
	logger := slog.Default()
	slog.InfoContext(
		ctx, "auto-prune: bounding source change-log growth on a cadence, cutting at the MIN frontier across "+
			"every REGISTERED consumer of this change log (ADR-0137 Phase B + roadmap item 115)",
		slog.String("stream_id", streamID),
		slog.String("consumer_id", consumerID),
		slog.Duration("interval", interval),
		slog.Int64("keep", keep),
	)
	// The pre-item-115 hazard — "the prune cuts at THIS stream's frontier, so a
	// slower peer loses the rows between the two frontiers" — is CLOSED for every
	// peer that registers, which is every trigger-CDC stream running this version
	// or newer, regardless of its own flags. What is NOT closed, and cannot be
	// detected from the source, is a peer running a sluice OLDER than the
	// registry: it streams without ever writing a registry row, so it is
	// invisible here. (A peer that ran an OLD `trigger setup` IS caught — that
	// rewrites schema_version below the registry floor, and the engine-side
	// preflight refuses.)
	slog.WarnContext(
		ctx, "auto-prune: every OTHER sync reading this source's change log must be running a sluice version with "+
			"the consumer registry (roadmap item 115) — a pre-registry peer never registers, so its frontier is "+
			"invisible here and its unread rows could be pruned. Upgrade every sync on this source before enabling "+
			"this flag on a staged/wave migration",
		slog.String("stream_id", streamID),
	)
	gate := &autoPruneGate{interval: interval}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				runAutoPruneTick(ctx, logger, applier, registry, streamID, consumerID, keep, gate, now)
			}
		}
	}()
}

// runAutoPruneTick is one cadence of the auto-prune sidecar, pulled out so the
// cadence-gate + failure-isolation semantics are unit-testable without a live
// ticker. When the gate is due it reads the TARGET's durable position and, if a
// position has been persisted, hands the token to the source's consumer
// registry, which cuts at `min(registry MIN, this stream's frontier) - keep`.
// Every error (position read OR prune) is logged at WARN and SWALLOWED — the
// sync is never affected; the next cadence retries.
//
// The freshly-read position is passed even though this stream's frontier is
// already IN the registry: it is the INDEPENDENT bound on our own row (which was
// written from an earlier read of the same state), so a stale or wrong registry
// row cannot lift the cut above what this stream has actually applied.
func runAutoPruneTick(
	ctx context.Context,
	logger *slog.Logger,
	applier ir.ChangeApplier,
	registry ir.ChangeLogConsumerRegistry,
	streamID, consumerID string,
	keep int64,
	gate *autoPruneGate,
	now time.Time,
) {
	if !gate.due(now) {
		return
	}
	pos, found, err := applier.ReadPosition(ctx, streamID)
	if err != nil {
		logger.WarnContext(
			ctx, "auto-prune: could not read the target's durable position; skipping this cadence, will retry next interval (advisory, sync unaffected)",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()),
		)
		return
	}
	if !found {
		// No durable frontier persisted yet (cold-start, nothing applied) — there
		// is no safe lower bound, so nothing to prune. Not an error.
		return
	}
	deleted, err := registry.PruneConsumedChangeLogToRegisteredMin(ctx, consumerID, pos.Token, keep)
	if err != nil {
		logger.WarnContext(
			ctx, "auto-prune: change-log prune refused or failed; NOTHING was deleted, will retry next interval (advisory, sync unaffected)",
			slog.String("stream_id", streamID),
			slog.String("consumer_id", consumerID),
			slog.Int64("keep", keep),
			slog.String("error", err.Error()),
		)
		return
	}
	if deleted > 0 {
		logger.InfoContext(
			ctx, "auto-prune: reaped source change-log rows below the slowest registered consumer's frontier",
			slog.String("stream_id", streamID),
			slog.Int64("deleted", deleted),
			slog.Int64("keep", keep),
		)
	}
}
