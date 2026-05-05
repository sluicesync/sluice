// Stop-signal polling for the Streamer.
//
// Operators stop a running `sluice sync start` today by hitting
// Ctrl-C — that still works (and remains the fastest path on a
// single host). The motivating gap was scriptability across
// machines: in container orchestrators (k8s lifecycle hooks,
// systemd, Nomad) the operator needs a network-reachable trigger,
// not a process-local signal. `sluice sync stop` fills that gap.
//
// Why control-table-based signaling rather than PIDs / pipes /
// SIGTERM via SSH:
//
//   - The per-target control table is already the source of truth
//     for stream identity (ADR-0007). Adding one nullable column to
//     it costs nothing structurally and reuses the connection the
//     applier already holds.
//   - It works regardless of where `sync stop` runs — different
//     machine, container, or cluster — because both sides agree on
//     the target database, not the host running the streamer.
//   - It survives streamer process restarts: if the streamer is
//     restarted between the stop request and the drain, the new
//     instance sees the flag on its next poll and exits.
//   - It's portable. PID files don't survive container restarts,
//     SIGTERM-via-SSH presumes login access the operator may not
//     have.
//
// What the polling loop does:
//
//   - Every 5 seconds, query `stop_requested_at IS NOT NULL` on the
//     applier's control row.
//   - When the flag is observed for the first time, cancel a
//     derived context the apply loop is using. The applier's Apply
//     method already returns `nil` on context.Canceled (existing
//     resume-friendly behavior); the Streamer's outer Run sees nil
//     and exits cleanly.
//   - The current change finishes (the per-change tx commits both
//     the data write and the position write atomically — see ADR-
//     0007); the next iteration of the apply loop sees ctx.Done and
//     returns without yanking the rug mid-transaction.
//
// What the polling loop does NOT do:
//
//   - Tune itself. 5s is a reasonable default for the first cut: a
//     responsive feel for operators without spamming the database.
//     Operators who need a sub-second response time on stop will
//     ask; until then a tuning knob is surface area no-one needs.
//   - Replace SIGINT/SIGTERM. Ctrl-C still cancels the streamer's
//     ctx the same way it always did; `sync stop` is additive.
//   - Hold a long-running connection. The poll uses the applier's
//     existing *sql.DB pool, releasing the connection after each
//     query.

package pipeline

import (
	"context"
	"log/slog"
	"time"
)

// stopFlagReader is the optional applier-side surface the polling
// loop consults. Engine-package appliers (MySQL, Postgres) implement
// it; test stubs typically don't, in which case the streamer skips
// the polling loop entirely. The interface is internal to the
// pipeline package — engines satisfy it structurally, not via an
// import. This keeps the [ir.ChangeApplier] surface lean: the
// poll-shape is a streamer concern, not part of the cross-engine
// applier contract.
//
// The method is exported (ReadStopRequested) because Go's method
// set rules require exported methods to satisfy interfaces from
// other packages — even when the interface is itself unexported.
type stopFlagReader interface {
	ReadStopRequested(ctx context.Context, streamID string) (bool, error)
}

// stopSignalPollInterval is how often the polling goroutine checks
// the control row. 5 seconds balances responsiveness against query
// noise. Not exposed as a flag in v1; see the file-level rationale.
const stopSignalPollInterval = 5 * time.Second

// stopDrainTimeout bounds how long the streamer will wait for a
// graceful drain (CDC reader stops → channel closes → applier commits
// partial batch) before hard-cancelling the apply context. Generous
// by design: a healthy apply commits a partial batch in under a
// second; the timeout only fires when the apply is genuinely wedged
// (lost connection, deadlock, etc.). 30s matches the same envelope
// the integration tests use for "should be done by now" assertions.
const stopDrainTimeout = 30 * time.Second

// pollIntervalForTest is the live cadence pollStopSignal uses. It
// defaults to stopSignalPollInterval; unit tests override it to a
// few-millisecond value so the goroutine ticks fast enough to make
// assertions snappy. Production code never reassigns it.
var pollIntervalForTest = stopSignalPollInterval

// drainTimeoutForTest is the live timeout the graceful-drain watchdog
// uses. Defaults to stopDrainTimeout; unit/integration tests override
// it for snappy assertions on the hard-cancel fallback. Production
// code never reassigns it.
var drainTimeoutForTest = stopDrainTimeout

// pollStopSignal runs until pollCtx is cancelled or until reader
// reports the stop flag is set. On stop-flag observation it triggers
// a graceful drain: cancelStream stops the CDC reader (which closes
// the change channel), the applier sees the channel close and
// commits its in-flight partial batch cleanly via the existing
// channel-closed branch in applyOneBatch — no rolled-back events
// (Bug 15 CLI path, ADR-0025).
//
// A watchdog goroutine then waits for the apply loop to finish (via
// pollCtx.Done, which fires when Streamer.Run's deferred cancelApply
// runs after dispatchApply returns). If the drain doesn't complete
// within stopDrainTimeout, cancelApply fires as a hard fallback.
//
// reader is typed as stopFlagReader — the optional interface the
// engine appliers satisfy. Callers that pass a non-conforming
// applier should skip calling pollStopSignal entirely.
func pollStopSignal(pollCtx context.Context, reader stopFlagReader, streamID string, cancelStream, cancelApply context.CancelFunc) {
	t := time.NewTicker(pollIntervalForTest)
	defer t.Stop()

	// Backoff for transient query errors. We don't want to spam the
	// database with sub-second retries when, e.g., the connection
	// briefly drops; we also don't want to sleep so long that a
	// real stop request is delayed by a previous unrelated blip.
	// The ticker cadence (5s) is plenty of breathing room — we
	// just log and try again on the next tick.
	for {
		select {
		case <-pollCtx.Done():
			return
		case <-t.C:
		}
		stopRequested, err := reader.ReadStopRequested(pollCtx, streamID)
		if err != nil {
			// Don't propagate transient query errors as fatal —
			// the polling loop is best-effort. ctx-cancel errors
			// during shutdown also flow through here.
			if pollCtx.Err() != nil {
				return
			}
			slog.WarnContext(pollCtx, "stop-signal poll failed; will retry on next tick",
				slog.String("err", err.Error()),
			)
			continue
		}
		if stopRequested {
			slog.InfoContext(pollCtx, "stop requested via control table; draining stream and exiting",
				slog.String("stream_id", streamID),
			)
			cancelStream()
			// Hard-timeout watchdog: if the graceful drain doesn't
			// complete within drainTimeoutForTest, force-cancel the
			// apply context. Exits cleanly when pollCtx (= applyCtx)
			// fires first, signalling apply finished naturally.
			go func() {
				select {
				case <-pollCtx.Done():
				case <-time.After(drainTimeoutForTest):
					slog.WarnContext(pollCtx, "graceful drain timed out; hard-cancelling apply",
						slog.String("stream_id", streamID),
						slog.Duration("timeout", drainTimeoutForTest),
					)
					cancelApply()
				}
			}()
			return
		}
	}
}
