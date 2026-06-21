// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliercontrol

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// Config bundles the operator-configurable knobs for a [Controller].
// Zero-valued fields fall back to the ADR-0052-recommended defaults.
type Config struct {
	// StreamID identifies the controller's owning stream. Stamped on
	// every emitted slog line and used as the Prometheus label so
	// multi-stream processes can be inspected per stream.
	StreamID string

	// EngineName is the target engine's [ir.Engine.Name] — used by
	// MetricsSnapshot consumers and (in future) as a tie-break in the
	// telemetry. Optional; no behaviour gates on it.
	EngineName string

	// Floor is the lower bound on batch size. The controller never
	// produces a NextBatchSize below this value. ADR-0017's
	// conservative-default of 1 is the canonical floor; the field
	// exists so unit tests can pin behaviour at non-default values
	// (operator override is not exposed).
	Floor int

	// Ceiling is the upper bound on batch size. Engine-default unless
	// the operator overrode via --apply-batch-size=N.
	Ceiling int

	// InitialSize is the batch size the controller starts at. When
	// zero, falls back to Ceiling. Operators who passed an explicit
	// --apply-batch-size=N use N as the start (and as the cap).
	InitialSize int

	// TargetLatency is the p95 wall-clock target the controller drives
	// AI/MD around. Engine-default per ADR-0052 DP-2 (planetscale=5s,
	// mysql=10s, postgres=10s); the streamer resolves the default
	// before constructing the controller and may pass an operator
	// override via --apply-tune-target-latency.
	TargetLatency time.Duration

	// WindowSize is the latency-window sample count. Default 50.
	WindowSize int

	// AdditiveStep is the +rows-per-batch increment on AI ticks.
	// Default +5.
	AdditiveStep int

	// MultiplicativeFactor is the multiplicative-decrease factor.
	// Default 0.5 (halve).
	MultiplicativeFactor float64

	// RetriableErrorThreshold is the retriable-errors-per-minute
	// threshold that trips an MD outside the latency path. Default 3.
	RetriableErrorThreshold int

	// RetriableErrorWindow is the rolling-window length the retry-rate
	// counter operates over. Default 60s (1 minute).
	RetriableErrorWindow time.Duration

	// CoolOffBatches is the count of consecutive successful batches at
	// the post-MD size before AI re-engages. Default 20.
	CoolOffBatches int

	// Now is a clock function the controller uses for retry-window
	// expiry. Unit tests inject deterministic clocks; production uses
	// [time.Now]. Nil falls back to time.Now.
	Now func() time.Time

	// OnShrink, when non-nil, is invoked with the new (post-MD) batch
	// size after every multiplicative decrease. The streamer wires it
	// to persist the shrunk size ACROSS runOnce restarts: a Vitess
	// tx-killer abort propagates out of runOnce to the ADR-0038
	// streamer-level retry loop, which reconstructs the controller on
	// the next attempt — without this hook the new controller would
	// reset to the ceiling and re-submit the same too-large batch that
	// was just killed, exhausting the retry budget (the v0.99.69
	// finding). The callback runs UNDER the controller's mutex, so it
	// must be cheap and non-blocking (a single field store). Nil is the
	// default — the controller is self-contained within one runOnce
	// without it.
	OnShrink func(newSize int)

	// TelemetryHint, when non-nil, is an advisory proactive-saturation
	// signal (ADR-0107 Phase 1). It is consulted under the controller
	// mutex during ObserveBatch: when a FRESH snapshot reports CPU or
	// memory utilisation at/above the high-water mark, the controller
	// suppresses additive-increase (HOLD) and applies AT MOST ONE
	// multiplicative-decrease on the fresh saturation edge, then holds.
	// It can ONLY push toward smaller/held sizes — never raise the
	// ceiling, grow a batch, advance a position, or stall the stream.
	// Nil (the default) is a no-op: the controller takes its existing
	// reactive AIMD path, byte-for-byte. A stale / no-signal snapshot is
	// ignored exactly as if the hint were nil — see [TelemetryHint].
	TelemetryHint TelemetryHint

	// TelemetryHighWater is informational only — the saturation decision
	// lives in the [TelemetryHint] (which owns the snapshot, freshness,
	// and the threshold comparison). It is stamped on log lines so an
	// operator can see the configured mark. Default 0.85 when zero
	// (ADR-0107's decided high-water).
	TelemetryHighWater float64
}

// TelemetryHint is the advisory proactive-saturation surface the
// controller consults (ADR-0107 Phase 1). The streamer adapts an
// ir.TargetTelemetry provider into one of these, so the appliercontrol
// package stays free of the ir import on the telemetry path — exactly
// as it mirrors ir.RetriableError / ir.TransactionKilledError
// structurally rather than importing them (see [retriable] / [txKilled]).
//
// Saturated owns the freshness check and the high-water comparison: the
// controller never sees the raw snapshot, only the distilled verdict.
type TelemetryHint interface {
	// Saturated reports whether the target is at/above the high-water
	// mark on a FRESH snapshot. ok=false means "no usable signal right
	// now" (provider not warmed up, or the last poll went stale) and the
	// controller degrades to its reactive AIMD path, exactly as if no
	// hint were wired.
	Saturated() (saturated, ok bool)
}

// DefaultTelemetryHighWater is the CPU/memory utilisation fraction
// at/above which the proactive telemetry damp engages (ADR-0107). It is
// the threshold the streamer's hint adapter applies; the controller only
// stamps it on logs. 0.85 leaves headroom before the target's reactive
// signals (latency, tx-killer) would otherwise push back.
const DefaultTelemetryHighWater = 0.85

// Defaults applied per ADR-0052 § "Implementation pre-resolutions".
const (
	defaultFloor                   = 1
	defaultAdditiveStep            = 5
	defaultMultiplicativeFactor    = 0.5
	defaultWindowSize              = 50
	defaultRetriableErrorThreshold = 3
	defaultRetriableErrorWindow    = time.Minute
	defaultCoolOffBatches          = 20
)

func (c Config) withDefaults() Config {
	if c.Floor <= 0 {
		c.Floor = defaultFloor
	}
	if c.Ceiling <= 0 {
		c.Ceiling = c.Floor
	}
	if c.Ceiling < c.Floor {
		c.Ceiling = c.Floor
	}
	if c.InitialSize <= 0 {
		c.InitialSize = c.Ceiling
	}
	if c.InitialSize < c.Floor {
		c.InitialSize = c.Floor
	}
	if c.InitialSize > c.Ceiling {
		c.InitialSize = c.Ceiling
	}
	if c.TargetLatency <= 0 {
		// No ADR-0052-mandated absolute default — engine-default
		// resolution lives in the streamer. The controller refuses to
		// run with a zero target (see New); this fallback exists only
		// to keep the zero-value Config sane for unit tests that don't
		// care about the threshold.
		c.TargetLatency = 5 * time.Second
	}
	if c.WindowSize <= 0 {
		c.WindowSize = defaultWindowSize
	}
	if c.AdditiveStep <= 0 {
		c.AdditiveStep = defaultAdditiveStep
	}
	if c.MultiplicativeFactor <= 0 || c.MultiplicativeFactor >= 1 {
		c.MultiplicativeFactor = defaultMultiplicativeFactor
	}
	if c.RetriableErrorThreshold <= 0 {
		c.RetriableErrorThreshold = defaultRetriableErrorThreshold
	}
	if c.RetriableErrorWindow <= 0 {
		c.RetriableErrorWindow = defaultRetriableErrorWindow
	}
	if c.CoolOffBatches <= 0 {
		c.CoolOffBatches = defaultCoolOffBatches
	}
	if c.TelemetryHighWater <= 0 {
		c.TelemetryHighWater = DefaultTelemetryHighWater
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Controller is the per-stream AIMD apply-batch-size controller.
// One controller instance per active [ir.ChangeApplier]; the streamer
// wires it via the optional [ir.BatchSizeProvider] / [ir.BatchObserver]
// surfaces (ADR-0052).
//
// Concurrency: the controller is safe for concurrent NextBatchSize /
// ObserveBatch / Snapshot calls — the metrics exporter scrapes
// Snapshot from a separate goroutine while the applier drives
// NextBatchSize + ObserveBatch from its single apply goroutine. A
// mutex guards the small state; the apply path's per-batch cost
// remains negligible vs the batch's tx-commit work.
type Controller struct {
	cfg Config

	mu              sync.Mutex
	window          *LatencyWindow
	currentSize     int
	coolOffRemain   int
	decreaseCount   uint64
	retryTimes      []time.Time // sorted ascending; pruned on the fly
	lastByteCapHint time.Time

	// lastTelemetrySaturated is the edge-detection latch for the
	// ADR-0107 proactive telemetry damp: it records whether the PREVIOUS
	// ObserveBatch saw the hint report saturated. A fresh rising edge
	// (was false, now true) fires AT MOST ONE multiplicative-decrease;
	// sustained saturation only holds (suppresses AI) so a hot target
	// shrinks once then waits, rather than collapsing to the floor on
	// every batch.
	lastTelemetrySaturated bool

	// telemetryDamped reports, for the metrics scrape, whether the most
	// recent telemetry consultation was actively damping (holding /
	// edge-shrinking) the controller. Snapshot-only; no behaviour gates
	// on it.
	telemetryDamped bool
}

// New returns a Controller initialized with the supplied config. Config
// defaults per the file-level Defaults block. Returns an error if the
// target latency is non-positive or the ceiling is below the floor —
// these would silently misbehave so the controller fails loud per the
// project's loud-failure tenet.
func New(cfg Config) (*Controller, error) {
	if cfg.TargetLatency < 0 {
		return nil, errors.New("applier_control: TargetLatency must be non-negative")
	}
	cfg = cfg.withDefaults()
	if cfg.Ceiling < cfg.Floor {
		// withDefaults clamps; this branch should be unreachable but
		// preserves the loud-failure contract if a future change skips
		// the clamp.
		return nil, errors.New("applier_control: Ceiling is below Floor after defaults")
	}
	return &Controller{
		cfg:         cfg,
		window:      NewLatencyWindow(cfg.WindowSize),
		currentSize: cfg.InitialSize,
	}, nil
}

// NextBatchSize returns the controller's current target batch size.
// Implements the engine-neutral [ir.BatchSizeProvider] contract.
//
// The returned value is always within [Floor, Ceiling] — invariants
// the controller maintains across every state transition.
func (c *Controller) NextBatchSize() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentSize
}

// ObserveBatch records one batch's outcome and (when conditions allow)
// applies the AIMD decision. Implements the engine-neutral
// [ir.BatchObserver] contract.
//
// The decision rules (ADR-0052 § Decision):
//
//   - If err is non-nil AND the wrapped error is retriable per the
//     retry-rate accumulator, immediately MD and enter cool-off.
//   - Else if rows > 0 AND p95 ≥ target, MD and enter cool-off.
//   - Else if rows > 0 AND not in cool-off AND p95 < target, AI by
//     AdditiveStep (capped at Ceiling).
//   - Else if in cool-off, decrement the cool-off remaining count on
//     a successful batch (err == nil).
//
// Idempotent on calls with rows == 0 AND err == nil (the idle-flush
// of an empty batch — no signal to feed the window).
func (c *Controller) ObserveBatch(ctx context.Context, latency time.Duration, rows int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	prevSize := c.currentSize

	// Record latency for non-empty batches regardless of error — a
	// failed batch's latency is still a signal (it tells us how long
	// the target took to refuse).
	if rows > 0 {
		c.window.Observe(latency)
	}

	// Transaction-killer abort: a STRONG, IMMEDIATE shrink signal
	// (ADR-0052; the v0.99.69 sustained-tx-killer finding). When the
	// target's server-side transaction killer rolled back the batch
	// (Vitess `code = Aborted ... for tx killer rollback`), the batch
	// was simply too large to commit inside the target's tx-timeout
	// window — re-submitting the SAME size will be killed again. We MD
	// at once, bypassing the generic retry-rate accumulator's
	// threshold-of-3 (which can never trip from tx-killer aborts in
	// practice: one abort is fatal to the runOnce attempt, so they
	// never accumulate before the run dies and the controller is torn
	// down). The OnShrink hook then persists the smaller size across
	// the ADR-0038 streamer-level retry so the next attempt re-applies
	// at the shrunk size and converges, instead of dying at the
	// ceiling. The tx-killer timestamp still feeds the retry-rate
	// accumulator below so a slow trickle of mixed transients is
	// accounted, but the immediate MD here is what makes convergence
	// happen. Checked BEFORE the generic accumulator so the first
	// tx-killer abort shrinks without waiting for a fourth.
	if err != nil && isTxKilledError(err) {
		c.multiplicativeDecreaseLocked(ctx, "transaction-killer", prevSize)
		return
	}

	// Retry-rate accounting. We count *any* observation whose err
	// satisfies [ir.RetriableError] semantics — the caller has the
	// engine-side classifier; we just pin the timestamp here.
	if err != nil && isRetriableError(err) {
		now := c.cfg.Now()
		c.pruneRetriesLocked(now)
		c.retryTimes = append(c.retryTimes, now)
		if len(c.retryTimes) > c.cfg.RetriableErrorThreshold {
			c.multiplicativeDecreaseLocked(ctx, "retriable-error-rate", prevSize)
			return
		}
	}

	// On successful batches we may exit cool-off, then potentially AI.
	if err == nil && rows > 0 {
		// Latency-driven MD path takes precedence over AI. We gate the
		// MD check on a small minimum-sample count (3) so a single
		// outlier doesn't trigger an MD; the AI path is also gated on
		// the same sample count so AI doesn't race ahead of MD on the
		// very first slow batch. Cool-off accounting is independent of
		// the sample count — a successful batch counts toward exiting
		// cool-off regardless of whether the window is warmed up.
		const minSamplesForDecision = 3
		if c.window.Len() >= minSamplesForDecision && c.window.P95() >= c.cfg.TargetLatency {
			c.multiplicativeDecreaseLocked(ctx, "p95-over-target", prevSize)
			return
		}

		// ADR-0107 proactive telemetry damp. Consulted AFTER the reactive
		// latency-MD (which keeps precedence — a hot target that is also
		// slow still shrinks for the slowness) and BEFORE cool-off / AI.
		// The hint owns freshness + the high-water comparison; we only act
		// on (saturated, ok). Behaviour:
		//
		//   - FRESH rising edge (was not saturated, now is): one MD, latch
		//     the edge, return. The MD itself enters cool-off, so AI is
		//     already suppressed for the cool-off window.
		//   - FRESH sustained saturation (already latched): HOLD — suppress
		//     AI and return, but never shrink again. A persistently hot
		//     target damps once then waits rather than collapsing to Floor.
		//   - not saturated, OR no usable signal (ok=false / nil hint):
		//     clear the latch and fall through to the unchanged reactive
		//     path. This is the degrade-to-today contract.
		//
		// It can only HOLD or SHRINK within [Floor, Ceiling]; it never
		// grows a batch or advances a position.
		if c.cfg.TelemetryHint != nil {
			if saturated, ok := c.cfg.TelemetryHint.Saturated(); ok && saturated {
				if !c.lastTelemetrySaturated {
					c.lastTelemetrySaturated = true
					c.telemetryDamped = true
					c.multiplicativeDecreaseLocked(ctx, "telemetry-saturation-edge", prevSize)
					return
				}
				// Sustained saturation: hold (suppress AI), shrink no further.
				c.telemetryDamped = true
				c.debugDecisionLocked(ctx, "telemetry-hold")
				return
			}
			// No saturation signal (healthy / stale / not-warmed-up):
			// reset the edge latch so the NEXT crossing fires a fresh MD,
			// and resume the reactive path below.
			c.lastTelemetrySaturated = false
			c.telemetryDamped = false
		}

		if c.coolOffRemain > 0 {
			c.coolOffRemain--
			if c.coolOffRemain == 0 {
				slog.InfoContext(
					ctx, "applier: aimd cool-off cleared",
					slog.String("stream_id", c.cfg.StreamID),
					slog.Int("size", c.currentSize),
				)
			}
			// In cool-off → no AI even if p95 is healthy.
			c.debugDecisionLocked(ctx, "in-cool-off")
			return
		}

		if c.window.Len() < minSamplesForDecision {
			// Not enough samples to AI; hold the current size.
			c.debugDecisionLocked(ctx, "warming-up")
			return
		}

		// AI: bump by AdditiveStep up to Ceiling.
		next := c.currentSize + c.cfg.AdditiveStep
		atCeiling := false
		if next >= c.cfg.Ceiling {
			next = c.cfg.Ceiling
			atCeiling = c.currentSize != next || next == c.cfg.Ceiling
		}
		if next != c.currentSize {
			c.currentSize = next
			c.debugDecisionLocked(ctx, "additive-increase")
		} else {
			c.debugDecisionLocked(ctx, "at-ceiling")
		}
		// One-shot INFO when a fresh AI ride just clipped the ceiling.
		if atCeiling && next == c.cfg.Ceiling && prevSize != c.cfg.Ceiling {
			slog.InfoContext(
				ctx, "applier: aimd reached ceiling",
				slog.String("stream_id", c.cfg.StreamID),
				slog.Int("size", c.currentSize),
				slog.Int("ceiling", c.cfg.Ceiling),
			)
		}
	}
}

// pruneRetriesLocked drops retry timestamps that fall outside the
// current rolling window. Called under c.mu.
func (c *Controller) pruneRetriesLocked(now time.Time) {
	cutoff := now.Add(-c.cfg.RetriableErrorWindow)
	keep := 0
	for _, t := range c.retryTimes {
		if t.After(cutoff) {
			c.retryTimes[keep] = t
			keep++
		}
	}
	c.retryTimes = c.retryTimes[:keep]
}

// multiplicativeDecreaseLocked halves the batch size, floors at
// cfg.Floor, enters cool-off, increments the decrease counter, and
// emits the INFO log line. Called under c.mu.
func (c *Controller) multiplicativeDecreaseLocked(ctx context.Context, reason string, prevSize int) {
	next := int(float64(c.currentSize) * c.cfg.MultiplicativeFactor)
	if next < c.cfg.Floor {
		next = c.cfg.Floor
	}
	c.currentSize = next
	c.coolOffRemain = c.cfg.CoolOffBatches
	c.decreaseCount++
	slog.InfoContext(
		ctx, "applier: aimd multiplicative decrease",
		slog.String("stream_id", c.cfg.StreamID),
		slog.String("reason", reason),
		slog.Int("prev_size", prevSize),
		slog.Int("new_size", c.currentSize),
		slog.Int("cool_off_batches", c.coolOffRemain),
		slog.Duration("p95", c.window.P95()),
		slog.Duration("target_latency", c.cfg.TargetLatency),
	)
	// Persist the shrunk size so it survives a runOnce restart (the
	// streamer wires OnShrink to its cross-attempt resume-size field).
	// Called under c.mu — the hook is a cheap field store; see the
	// Config.OnShrink doc for the lifecycle rationale.
	if c.cfg.OnShrink != nil {
		c.cfg.OnShrink(c.currentSize)
	}
}

// debugDecisionLocked emits the per-batch DEBUG log line capturing the
// just-applied decision plus the current size and p95 (DP-3). Called
// under c.mu so the read of window/state matches the just-committed
// decision.
func (c *Controller) debugDecisionLocked(ctx context.Context, decision string) {
	slog.DebugContext(
		ctx, "applier: aimd decision",
		slog.String("stream_id", c.cfg.StreamID),
		slog.String("decision", decision),
		slog.Int("size", c.currentSize),
		slog.Duration("p95", c.window.P95()),
		slog.Duration("target_latency", c.cfg.TargetLatency),
		slog.Int("cool_off_remaining", c.coolOffRemain),
	)
}

// MetricsSnapshot is the read-only view exposed to the Prometheus
// exporter (DP-3). Captured atomically under the mutex so a scrape
// can never observe torn state.
type MetricsSnapshot struct {
	StreamID        string
	CurrentSize     int
	P95             time.Duration
	DecreasesTotal  uint64
	InCoolOff       bool
	TelemetryDamped bool
}

// Snapshot returns the current scrape-time view.
func (c *Controller) Snapshot() MetricsSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return MetricsSnapshot{
		StreamID:        c.cfg.StreamID,
		CurrentSize:     c.currentSize,
		P95:             c.window.P95(),
		DecreasesTotal:  c.decreaseCount,
		InCoolOff:       c.coolOffRemain > 0,
		TelemetryDamped: c.telemetryDamped,
	}
}

// NoteByteCapDominant emits the rate-limited advisory INFO log line
// described in ADR-0052 DP-4 (b): when ADR-0028's byte-cap fires
// before the row-cap on multiple consecutive batches, operators get a
// hint that raising --max-buffer-bytes (not --apply-batch-size) is
// what would help. The log fires at most once per cool-off period.
//
// This is the controller's only outward signal that doesn't drive
// AI/MD — it's an advisory, not a control input. The applier calls it
// from the byte-cap branch of applyOneBatch when the row-count hasn't
// been hit by the time the byte-cap fires.
func (c *Controller) NoteByteCapDominant(ctx context.Context, rows int, bytes, byteCap int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.cfg.Now()
	// Rate-limit to one per cool-off-batches × estimated batch time.
	// We use a wall-clock estimate (cool-off-batches × target-latency)
	// since the controller has no per-batch tick available here.
	rateLimit := time.Duration(c.cfg.CoolOffBatches) * c.cfg.TargetLatency
	if rateLimit <= 0 {
		rateLimit = 30 * time.Second
	}
	if !c.lastByteCapHint.IsZero() && now.Sub(c.lastByteCapHint) < rateLimit {
		return
	}
	c.lastByteCapHint = now
	slog.InfoContext(
		ctx, "applier: byte-cap dominant",
		slog.String("stream_id", c.cfg.StreamID),
		slog.Int("rows", rows),
		slog.Int64("bytes", bytes),
		slog.Int64("byte_cap", byteCap),
		slog.Int("current_batch_size", c.currentSize),
		slog.String("hint", "byte-cap fired before row-cap on a sustained shape; consider raising --max-buffer-bytes (AIMD controller cannot reduce per-batch bytes by changing row count)"),
	)
}

// retriable is the structural shape we look for in the error chain.
// Mirrors [ir.RetriableError] without importing the ir package — the
// controller is engine-neutral and keeping the import surface narrow
// keeps the unit tests off the broader IR contract. Both engine
// classifiers produce values satisfying this shape via their wrapped
// retriable error type.
type retriable interface {
	error
	Retriable() bool
}

// isRetriableError walks the error chain via [errors.As] looking for
// a value that satisfies the [retriable] shape and returns true. The
// errors.As walk traverses Unwrap chains AND multi-error siblings
// correctly, unlike the bare type-assertion form (which the errorlint
// rule flags as unsound on wrapped errors).
func isRetriableError(err error) bool {
	if err == nil {
		return false
	}
	var r retriable
	if errors.As(err, &r) {
		return r.Retriable()
	}
	return false
}

// txKilled is the structural shape the controller looks for to detect
// a server-side transaction-killer abort. Mirrors
// [ir.TransactionKilledError] without importing the ir package — same
// engine-neutrality reasoning as [retriable]. The MySQL applier's
// classifier produces a value satisfying this shape for Vitess
// tx-killer 1105 aborts (it returns false for the other retriable 1105
// shapes, so a same-size retry still rides out a benign transient
// without an unnecessary shrink).
type txKilled interface {
	error
	TransactionKilled() bool
}

// isTxKilledError walks the error chain via [errors.As] looking for a
// value that satisfies [txKilled] and reports its verdict. Returns
// false for errors that don't implement the surface (the common case)
// and for tx-killer-capable errors whose per-error flag is false.
func isTxKilledError(err error) bool {
	if err == nil {
		return false
	}
	var t txKilled
	if errors.As(err, &t) {
		return t.TransactionKilled()
	}
	return false
}
