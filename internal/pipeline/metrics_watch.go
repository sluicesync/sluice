// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
)

// Standalone target-metrics WATCH loop — the engine behind the
// `sluice metrics-watch` daemon (ADR-0107 leftover). It reuses the item-36
// alerter's rule set, evaluator, and notify sinks, but runs WITHOUT a sync:
// no source, no target connection, no apply path. It only consults the
// PlanetScale telemetry provider's cached snapshot (the same control-plane
// poll the sync path uses) and, per interval, optionally prints a live line
// and fires threshold alerts.
//
// Why a standalone mode exists: the sync-scoped alerter only watches a target
// WHILE a migration/sync is running. Operators also want a long-lived,
// sync-independent watch — a tiny daemon they can point at a PlanetScale
// database to get a live CPU/mem/storage/lag view plus the same edge-triggered
// Slack/webhook alerts, with no migration attached. The loop is a thin shell
// over the already-tested alerter pieces, so the alert semantics
// (edge-trigger + cooldown + hysteresis + rate-of-change + *Known honesty +
// failure-isolation) are IDENTICAL to the sync path by construction.

// metricsWatchWarmup bounds how long [RunMetricsWatch] waits for the freshly
// constructed provider's background poll to land a first sample before the
// first print/evaluate, so the operator sees real data immediately rather
// than after a full poll interval. Sized just above the provider's ~10s
// per-poll HTTP timeout (mirrors `sluice diagnose`'s warm-up).
const metricsWatchWarmup = 12 * time.Second

// MetricsWatchConfig configures [RunMetricsWatch]. The threshold fields mirror
// the sync path's --notify-* config one-for-one (a 0 threshold leaves that
// rule inert); the sink fields mirror --notify-webhook / --notify-slack.
type MetricsWatchConfig struct {
	// Threshold rules (fraction-of-capacity for util metrics, seconds for lag,
	// fraction-per-minute for the growth rule). 0 = rule disabled.
	StorageUtil         float64
	CPUUtil             float64
	MemUtil             float64
	LagSeconds          float64
	StorageGrowthPerMin float64

	// Cooldown is the minimum interval between re-fires of a still-breached
	// rule. 0 ⇒ defaultNotifyCooldown (15m).
	Cooldown time.Duration

	// Notify sinks. Empty ⇒ no alert delivery (watch-only).
	WebhookURL      string
	SlackWebhookURL string

	// SMTP is the optional email sink (roadmap item 48). Inert unless
	// configured (a non-empty Host). Same advisory + failure-isolated
	// semantics as the webhook/Slack sinks.
	SMTP notify.SMTPConfig

	// Interval is the poll/print cadence. 0 ⇒ telemetryPollInterval (60s,
	// matching the PlanetScale metrics granularity — polling faster only
	// re-reads the same sample).
	Interval time.Duration

	// Label identifies this watch in emitted notifications + log lines (the
	// notification StreamID field). Empty ⇒ "metrics-watch".
	Label string

	// Once polls a single sample (after the warm-up), prints/evaluates it, and
	// returns — the one-shot mode for scripts and `--once`.
	Once bool

	// Print emits a human-readable sample line to Out each tick. Out defaults
	// to io.Discard when nil (so Print without Out is silent, not a panic).
	Print bool
	Out   io.Writer

	// MetricsListen, when non-empty, serves a Prometheus `/metrics` endpoint
	// at the given address (e.g. ":9090") for the watch's lifetime — turning
	// the daemon into a standalone PlanetScale-metrics exporter. It re-exports
	// the watched database's cached health snapshot as the sluice_target_*
	// gauge family, plus sluice_build_info + the Go-runtime block. Off by
	// default. Ignored in --once mode (no long-lived loop to serve).
	MetricsListen string

	// BuildVersion / BuildCommit populate the exporter's sluice_build_info
	// gauge. Set from the cmd layer's build vars.
	BuildVersion string
	BuildCommit  string
}

// RunMetricsWatch runs the standalone watch loop against provider until ctx is
// cancelled (returning nil on cancellation — a clean SIGINT shutdown), or until
// the single sample is taken when cfg.Once is set. It is a no-op-safe shell:
// a nil provider is the one hard error (the caller must supply a configured
// PlanetScale telemetry provider); everything else degrades gracefully (no
// rules ⇒ watch-only; no sinks ⇒ rules are not evaluated; an unobserved or
// stale sample ⇒ "no fresh sample", never a spurious alert).
func RunMetricsWatch(ctx context.Context, provider ir.TargetTelemetry, cfg MetricsWatchConfig) error {
	if provider == nil {
		return errors.New("metrics-watch: nil telemetry provider (supply --planetscale-org and the metrics token)")
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = telemetryPollInterval
	}
	cooldown := cfg.Cooldown
	if cooldown <= 0 {
		cooldown = defaultNotifyCooldown
	}
	label := cfg.Label
	if label == "" {
		label = "metrics-watch"
	}
	out := cfg.Out
	if out == nil {
		out = io.Discard
	}

	rules := buildMetricsNotifyRulesFrom(cfg.StorageUtil, cfg.CPUUtil, cfg.MemUtil, cfg.LagSeconds, cfg.StorageGrowthPerMin)
	notifier := buildMetricsNotifierFrom(cfg.WebhookURL, cfg.SlackWebhookURL, cfg.SMTP)
	logger := slog.Default()
	state := newMetricsNotifyState()
	alerting := notifier != nil && len(rules) > 0

	logWatchStart(ctx, logger, label, interval, len(rules), alerting, notifier == nil)

	// Give the provider's background poll a brief moment to land the first
	// sample so the immediate tick shows real data, not the cold cache.
	warmUpForSample(ctx, provider)

	tick := func() {
		if cfg.Print {
			snap, ok := provider.Sample(ctx)
			fmt.Fprintln(out, formatWatchLine(time.Now(), snap, ok))
		}
		if alerting {
			// Reuse the exact sync-path alerter tick (it re-reads the cached
			// sample, re-checks freshness, and applies edge-trigger + cooldown
			// + failure-isolation). Identical semantics to a running sync.
			runMetricsNotifyTick(ctx, logger, provider, notifier, label, rules, state, cooldown, time.Now)
		}
	}

	tick() // immediate first sample after warm-up
	if cfg.Once {
		return nil
	}

	// Optional standalone Prometheus exporter: re-export the watched DB's
	// health as sluice_target_* + build_info + runtime. Bind failures are
	// fatal here (the operator asked for the endpoint); served until ctx ends.
	if cfg.MetricsListen != "" {
		stop, err := startWatchExporter(ctx, cfg.MetricsListen, provider, cfg.Label, cfg.BuildVersion, cfg.BuildCommit, logger)
		if err != nil {
			return err
		}
		defer stop()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil // clean shutdown (SIGINT/SIGTERM) — not an error
		case <-ticker.C:
			tick()
		}
	}
}

// warmUpForSample blocks (bounded by [metricsWatchWarmup] or ctx) until the
// provider returns a sample, so the first tick reflects real data. Best-effort:
// returning on timeout simply yields a "no fresh sample" first line.
func warmUpForSample(ctx context.Context, provider ir.TargetTelemetry) {
	deadline := time.Now().Add(metricsWatchWarmup)
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		if _, ok := provider.Sample(ctx); ok {
			return
		}
		if time.Now().After(deadline) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// formatWatchLine renders one human-readable watch line. It honours the
// *Known honesty contract: an unobserved metric prints "n/a", never a
// misleading 0. now is injected for deterministic tests.
func formatWatchLine(now time.Time, snap ir.TargetHealthSnapshot, ok bool) string {
	stamp := now.UTC().Format(time.RFC3339)
	if !ok {
		return stamp + "  no fresh sample (provider warming up or last poll failed)"
	}
	frac := func(known bool, v float64) string {
		if !known {
			return "n/a"
		}
		return fmt.Sprintf("%.3f", v)
	}
	storageDetail := ""
	if snap.StorageKnown && snap.StorageCapacityBytes > 0 {
		usedGB := float64(snap.StorageCapacityBytes-snap.StorageAvailableBytes) / 1e9
		capGB := float64(snap.StorageCapacityBytes) / 1e9
		storageDetail = fmt.Sprintf(" (used %.1fG/%.1fG)", usedGB, capGB)
	}
	lag := "n/a"
	if snap.LagKnown {
		lag = fmt.Sprintf("%.1fs", snap.ReplicaLagSeconds)
	}
	conns := "n/a"
	if snap.ConnKnown {
		conns = fmt.Sprintf("%d/%d", snap.ActiveConnections, snap.MaxConnections)
	}
	return fmt.Sprintf(
		"%s  cpu=%s mem=%s storage=%s%s lag=%s conns=%s  sampled=%s fresh=%t",
		stamp,
		frac(snap.CPUKnown, snap.CPUUtil),
		frac(snap.MemKnown, snap.MemUtil),
		frac(snap.StorageKnown, snap.StorageUtil), storageDetail,
		lag, conns,
		snap.SampledAt.UTC().Format(time.RFC3339),
		snap.Fresh(now, telemetryFreshnessWindow),
	)
}

// startWatchExporter binds a tiny HTTP server serving GET /metrics (and
// /healthz) that re-exports the watched database's cached health snapshot as
// the sluice_target_* gauge family, alongside sluice_build_info and the
// Go-runtime block — so a `metrics-watch --metrics-listen` invocation IS a
// standalone PlanetScale-metrics Prometheus exporter. It reuses the exact emit
// functions the sync /metrics endpoint uses. Returns a stop func that shuts the
// server down. The bind is synchronous (so an address-in-use error surfaces
// immediately); serving runs in a background goroutine until stop or ctx ends.
func startWatchExporter(ctx context.Context, addr string, provider ir.TargetTelemetry, label, version, commit string, logger *slog.Logger) (func(), error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		emitBuildInfoMetrics(w, version, commit)
		emitGoRuntimeMetrics(w)
		if snap, ok := provider.Sample(r.Context()); ok {
			emitTargetTelemetryMetrics(w, label, snap)
		} else {
			fmt.Fprintln(w, "\n# target-telemetry: no signal")
		}
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return func() {}, fmt.Errorf("metrics-watch: listen %s: %w", addr, err)
	}
	go func() { _ = srv.Serve(ln) }()
	logger.InfoContext(ctx, "metrics-watch exporter listening", slog.String("addr", addr))
	return func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}, nil
}

// logWatchStart emits the one INFO line describing the watch configuration, so
// an operator running the daemon sees at a glance what it will do.
func logWatchStart(ctx context.Context, logger *slog.Logger, label string, interval time.Duration, ruleCount int, alerting, noSink bool) {
	mode := "watch-only"
	switch {
	case alerting:
		mode = fmt.Sprintf("alerting on %d rule(s)", ruleCount)
	case ruleCount > 0 && noSink:
		mode = fmt.Sprintf("%d rule(s) configured but NO sink (--notify-webhook/--notify-slack) — alerts will not be delivered", ruleCount)
	}
	logger.InfoContext(
		ctx, "metrics-watch starting (ADR-0107 standalone target-metrics watch — advisory only)",
		slog.String("label", label),
		slog.Duration("interval", interval),
		slog.String("mode", mode),
	)
}
