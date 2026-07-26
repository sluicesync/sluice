// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
	"sluicesync.dev/sluice/internal/progress"
	"sluicesync.dev/sluice/internal/telemetrysink"
)

// ORG-WIDE metrics watch (roadmap item 75b) — the fan-out sibling of
// [RunMetricsWatch]'s single-database loop.
//
// Everything downstream of "read the samples" is the SAME code the
// single-database watch uses: the rule set, the evaluator, the edge-trigger +
// cooldown + hysteresis latches, the failure-isolated notifier fan-out, and
// the persistent sink. What differs is only the shape of a tick — N targets
// instead of one — which shows up in three places: per-target alerter STATE
// (each database+branch latches independently), the exporter's label set
// (`database`/`branch` instead of `stream_id`), and the live line, which
// collapses to ONE summary row because a per-database line is unreadable
// across dozens of databases.

// runFleetMetricsWatch is the org-wide watch loop. It mirrors
// [RunMetricsWatch]'s structure exactly (warm-up → immediate tick → optional
// exporter → ticker), so the two modes cannot drift in cadence or shutdown
// semantics.
func runFleetMetricsWatch(ctx context.Context, fleet ir.FleetTelemetry, cfg MetricsWatchConfig) error {
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
	if cfg.Event != nil {
		notifier = notify.NewMultiNotifier(notifier, panelEventNotifier{emit: cfg.Event})
	}
	logger := slog.Default()
	alerting := notifier != nil && len(rules) > 0
	// Per-target latch state. Keyed by target so one database's breach can
	// never re-arm or suppress another's; pruned each tick against the
	// discovered set so a removed database doesn't leak state forever.
	states := map[ir.FleetTarget]*metricsNotifyState{}

	logFleetWatchStart(ctx, logger, label, interval, len(rules), alerting, notifier == nil)
	warmUpForFleetSample(ctx, fleet)
	// One line naming what discovery actually matched, so a mis-typed
	// --include-database is visible immediately rather than as a silently
	// empty fleet.
	discovered := fleet.SampleFleet(ctx)
	logger.InfoContext(
		ctx, "metrics-watch fleet discovered",
		slog.Int("targets", len(discovered)),
		slog.String("names", fleetTargetsLabel(discovered, 20)),
	)

	tick := func() {
		samples := fleet.SampleFleet(ctx)
		now := time.Now()
		switch {
		case cfg.Readout != nil:
			cfg.Readout(fleetWatchReadoutFields(now, samples))
		case cfg.Print:
			fmt.Fprintln(out, formatFleetWatchLine(now, samples))
		}
		if cfg.Sink != nil {
			writeSampleRecords(ctx, logger, cfg.Sink, fleetSampleRecords(now, label, samples))
		}
		if !alerting {
			return
		}
		live := make(map[ir.FleetTarget]bool, len(samples))
		for _, s := range samples {
			live[s.Target] = true
			if !s.OK {
				continue
			}
			state := states[s.Target]
			if state == nil {
				state = newMetricsNotifyState()
				states[s.Target] = state
			}
			deliverMetricsNotifyTick(
				ctx, logger, s.Snapshot, notifier,
				label+"/"+s.Target.String(), rules, state, cooldown, time.Now,
			)
		}
		for t := range states {
			if !live[t] {
				delete(states, t)
			}
		}
	}

	tick()
	if cfg.Once {
		return nil
	}

	if cfg.MetricsListen != "" {
		stop, err := startFleetWatchExporter(ctx, cfg.MetricsListen, fleet, cfg.BuildVersion, cfg.BuildCommit, logger)
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

// warmUpForFleetSample blocks (bounded by [metricsWatchWarmup] or ctx) until
// the fleet provider has discovered at least one target, so the first tick
// reflects a real fan-out rather than an empty org. Best-effort: returning on
// timeout simply yields a "no targets" first line.
func warmUpForFleetSample(ctx context.Context, fleet ir.FleetTelemetry) {
	deadline := time.Now().Add(metricsWatchWarmup)
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		if len(fleet.SampleFleet(ctx)) > 0 {
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

// formatFleetWatchLine renders ONE summary line for a whole tick. Per the
// item-75b gotcha, org-wide mode leads with the exporter + sink paths: a
// per-database line would be unreadable across dozens of databases, so the
// live line reports the fleet's size, how much of it was observed, and the
// WORST reading of each metric with the database that produced it. It honours
// the same *Known honesty contract as [formatWatchLine] — a metric no target
// observed prints "n/a", never 0.
func formatFleetWatchLine(now time.Time, samples []ir.FleetHealthSample) string {
	stamp := now.UTC().Format(time.RFC3339)
	if len(samples) == 0 {
		return stamp + "  no targets discovered (provider warming up, or the filters matched nothing)"
	}
	observed := 0
	for _, s := range samples {
		if s.OK {
			observed++
		}
	}
	worst := fleetWorst(samples)
	return fmt.Sprintf(
		"%s  targets=%d observed=%d  cpu.max=%s mem.max=%s storage.max=%s lag.max=%s",
		stamp, len(samples), observed,
		worst.render("cpu", "%.3f"),
		worst.render("mem", "%.3f"),
		worst.render("storage", "%.3f"),
		worst.render("lag", "%.1fs"),
	)
}

// fleetWatchReadoutFields renders one tick as the ADR-0156 live-panel
// readout: the same summary [formatFleetWatchLine] prints, as an ordered
// label/value list the panel renders in place.
func fleetWatchReadoutFields(_ time.Time, samples []ir.FleetHealthSample) []progress.Field {
	if len(samples) == 0 {
		return []progress.Field{{Label: "status", Value: "no targets discovered (provider warming up, or the filters matched nothing)"}}
	}
	observed := 0
	for _, s := range samples {
		if s.OK {
			observed++
		}
	}
	worst := fleetWorst(samples)
	return []progress.Field{
		{Label: "targets", Value: fmt.Sprintf("%d", len(samples))},
		{Label: "observed", Value: fmt.Sprintf("%d", observed)},
		{Label: "cpu.max", Value: worst.render("cpu", "%.3f")},
		{Label: "mem.max", Value: worst.render("mem", "%.3f")},
		{Label: "storage.max", Value: worst.render("storage", "%.3f")},
		{Label: "lag.max", Value: worst.render("lag", "%.1fs")},
	}
}

// fleetExtreme is the per-metric worst reading across a tick, with the
// target that produced it. known=false ⇒ NO target observed the metric.
type fleetExtreme struct {
	value  float64
	target ir.FleetTarget
	known  bool
}

// fleetWorstSet holds the per-metric extremes for one tick.
type fleetWorstSet map[string]fleetExtreme

// render formats one metric's extreme as `value(database)`, or "n/a" when no
// target observed it.
func (w fleetWorstSet) render(metric, format string) string {
	e, ok := w[metric]
	if !ok || !e.known {
		return "n/a"
	}
	return fmt.Sprintf(format, e.value) + "(" + e.target.Database + ")"
}

// fleetWorst reduces a tick to the highest observed reading per metric. Only
// FRESH targets participate — a stale target must not contribute a reading
// that looks current.
func fleetWorst(samples []ir.FleetHealthSample) fleetWorstSet {
	out := fleetWorstSet{}
	consider := func(metric string, value float64, known bool, target ir.FleetTarget) {
		if !known {
			return
		}
		if e, ok := out[metric]; ok && e.known && e.value >= value {
			return
		}
		out[metric] = fleetExtreme{value: value, target: target, known: true}
	}
	for _, s := range samples {
		if !s.OK {
			continue
		}
		consider("cpu", s.Snapshot.CPUUtil, s.Snapshot.CPUKnown, s.Target)
		consider("mem", s.Snapshot.MemUtil, s.Snapshot.MemKnown, s.Target)
		consider("storage", s.Snapshot.StorageUtil, s.Snapshot.StorageKnown, s.Target)
		consider("lag", s.Snapshot.ReplicaLagSeconds, s.Snapshot.LagKnown, s.Target)
	}
	return out
}

// fleetSampleRecords maps one tick's samples into persistent-sink records —
// one row per DISCOVERED target, including targets with no fresh sample
// (fresh=false, every metric null), so a portal can see a gap rather than
// infer one from a missing row.
func fleetSampleRecords(now time.Time, label string, samples []ir.FleetHealthSample) []telemetrysink.Record {
	out := make([]telemetrysink.Record, 0, len(samples))
	for _, s := range samples {
		out = append(out, sampleRecord(now, label, s.Target.Database, s.Target.Branch, s.Snapshot, s.OK))
	}
	return out
}

// startFleetWatchExporter binds the org-wide `/metrics` endpoint: the same
// sluice_target_* family the single-database exporter serves, one series per
// database+branch, plus the fleet-size gauges. Returns a stop func; the bind
// is synchronous so an address-in-use error surfaces immediately.
func startFleetWatchExporter(ctx context.Context, addr string, fleet ir.FleetTelemetry, version, commit string, logger *slog.Logger) (func(), error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		emitBuildInfoMetrics(w, version, commit)
		emitGoRuntimeMetrics(w)
		emitFleetTelemetryMetrics(w, fleet.SampleFleet(r.Context()))
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
	logger.InfoContext(ctx, "metrics-watch fleet exporter listening", slog.String("addr", addr))
	return func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}, nil
}

// logFleetWatchStart emits the one INFO line describing an org-wide watch, so
// an operator sees at a glance that the fan-out (not the single-database
// mode) is what started.
func logFleetWatchStart(ctx context.Context, logger *slog.Logger, label string, interval time.Duration, ruleCount int, alerting, noSink bool) {
	mode := "watch-only"
	switch {
	case alerting:
		mode = fmt.Sprintf("alerting on %d rule(s) per target", ruleCount)
	case ruleCount > 0 && noSink:
		mode = fmt.Sprintf("%d rule(s) configured but NO sink (--notify-webhook/--notify-slack) — alerts will not be delivered", ruleCount)
	}
	logger.InfoContext(
		ctx, "metrics-watch starting in ORG-WIDE mode (roadmap item 75b — every database+branch the org exposes; advisory only)",
		slog.String("label", label),
		slog.Duration("interval", interval),
		slog.String("mode", mode),
	)
}

// fleetTargetsLabel renders a discovered target list for a log line, sorted
// and truncated so a large org doesn't emit an unbounded line.
func fleetTargetsLabel(samples []ir.FleetHealthSample, limit int) string {
	names := make([]string, 0, len(samples))
	for _, s := range samples {
		names = append(names, s.Target.String())
	}
	sort.Strings(names)
	if len(names) > limit {
		return strings.Join(names[:limit], ",") + fmt.Sprintf(",… (+%d)", len(names)-limit)
	}
	return strings.Join(names, ",")
}
