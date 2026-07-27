// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"sluicesync.dev/sluice/internal/notify"
	"sluicesync.dev/sluice/internal/pipeline"
	pstelemetry "sluicesync.dev/sluice/internal/planetscale/telemetry"
	"sluicesync.dev/sluice/internal/progress"
	"sluicesync.dev/sluice/internal/telemetrysink"
)

// MetricsWatchCmd implements `sluice metrics-watch` — the standalone
// target-metrics watch daemon (ADR-0107 leftover).
//
// It is the sync-independent sibling of the item-36 sync-scoped alerter: point
// it at a PlanetScale database and it polls that database's control-plane
// metrics endpoint on an interval, printing a live CPU/mem/storage/lag line and
// firing the SAME edge-triggered Slack/webhook threshold alerts — with no
// migration or sync attached. Unlike `sync start` / `migrate`, it opens NO
// connection to the database itself; it only reads the PlanetScale metrics API
// (control-plane), so it needs only the --planetscale-* telemetry credentials,
// not a --target DSN.
//
// Use it to keep an eye on a PlanetScale instance between migrations, or to
// wire storage/CPU alerts into Slack for a database sluice isn't actively
// touching. Threshold + sink semantics are identical to `sync start`'s
// --notify-* flags by construction (the rule set and evaluator are shared).
//
// **Two modes** (roadmap item 75b), selected EXPLICITLY. With
// --planetscale-metrics-db it watches exactly that one database — the
// original, unchanged behaviour. With --fleet it goes ORG-WIDE: the org's
// metrics service-discovery document enumerates every database+branch, the
// poll fans out across them with bounded concurrency, and
// --include-database/--exclude-database scope the set. Exactly one of the two
// is required; neither is the default, because org-wide inferred from an empty
// --planetscale-metrics-db meant a wrapper script with an unset variable fanned
// out across the whole org silently (audit 2026-07-26 ARCH-3). Org-wide mode leads with the exporter (--metrics-listen, one series
// per database+branch) and the persistent sink (--sink-file/--sink-http)
// rather than the live line, which collapses to one summary row because a
// per-database line is unreadable across dozens of databases.
//
// **Exit codes.** 0 on a clean shutdown (SIGINT/SIGTERM) or after --once; 2 on
// an operational error (incomplete telemetry opt-in, unknown engine).
type MetricsWatchCmd struct {
	// Engine selects which PlanetScale metric-name table to read — a Postgres
	// target exposes `planetscale_volume_*` / `planetscale_pods_*` whereas a
	// Vitess/MySQL target exposes `vttablet_*` / `mysql_*`. No database
	// connection is made; this only picks the metric vocabulary.
	Engine string `help:"Target engine the watched PlanetScale database runs (mysql|postgres|planetscale|vitess). Selects which PlanetScale metric-name table to read. No database connection is opened — only the control-plane metrics endpoint is polled." required:"" placeholder:"NAME"`

	PlanetScaleOrg            string `name:"planetscale-org" help:"PlanetScale org slug (REQUIRED). The watch reads this org's metrics endpoint. Control-plane only — no data-plane DSN is used." required:"" placeholder:"ORG"`
	PlanetScaleMetricsTokenID string `name:"planetscale-metrics-token-id" help:"PlanetScale service-token ID (granted read_metrics_endpoints). Prefer the env var so the id never lands in shell history." env:"PLANETSCALE_METRICS_TOKEN_ID" placeholder:"ID"`
	PlanetScaleMetricsToken   string `name:"planetscale-metrics-token" help:"PlanetScale service-token secret. Set via the env var (never on the command line); masked in all logging." env:"PLANETSCALE_METRICS_TOKEN" placeholder:"SECRET"`
	PlanetScaleMetricsBranch  string `name:"planetscale-metrics-branch" help:"Branch to filter telemetry series to. Single-database mode defaults to 'main'; in org-wide mode an unset value means EVERY branch, and a set value restricts the fan-out to branches with that name." placeholder:"BRANCH"`
	PlanetScaleMetricsDB      string `name:"planetscale-metrics-db" help:"Database name to watch. Mutually exclusive with --fleet; exactly one of the two is required." placeholder:"DATABASE"`
	Fleet                     bool   `name:"fleet" help:"Watch the WHOLE ORG: every database+branch the org's metrics service discovery returns, fanned out on the same cadence. Mutually exclusive with --planetscale-metrics-db."`

	// Org-wide fan-out scoping (roadmap item 75b). Ignored in
	// single-database mode. Include and exclude may be combined (exclude
	// wins) — deliberately unlike --include-table/--exclude-table; see
	// telemetry.fleetFilter for why.
	IncludeDatabase  []string `help:"Org-wide mode: only watch databases matching this glob (comma-separated, repeatable). Matched against 'database' and 'database/branch'. Combines with --exclude-database, which wins on a conflict." sep:"," placeholder:"GLOB"`
	ExcludeDatabase  []string `help:"Org-wide mode: skip databases matching this glob (comma-separated, repeatable). Same matching as --include-database; exclude wins when both match." sep:"," placeholder:"GLOB"`
	FleetConcurrency int      `help:"Org-wide mode: how many per-branch metric scrapes run concurrently each poll (default 4, max 16). Bounds the load the fan-out puts on the PlanetScale metrics API." placeholder:"N"`

	// Persistent sample sink (roadmap item 75c). Records EVERY polled
	// sample, unlike the --notify-* sinks which fire only on a threshold
	// breach. Opt-in, advisory, failure-isolated.
	SinkFile         string `help:"Append every polled sample to this rotating JSONL file (one record per line). Opt-in durable storage for a portal/warehouse — no Prometheus required. ADVISORY: a write failure is logged-and-swallowed, never stalling the poll." placeholder:"PATH"`
	SinkFileMaxBytes int64  `help:"Rotate --sink-file at this size. 0 (default) = 64MiB; negative disables rotation (you own the file's growth)." placeholder:"BYTES"`
	SinkFileMaxFiles int    `help:"How many rotated --sink-file generations to keep (PATH.1 … PATH.N). 0 (default) = 5." placeholder:"N"`
	SinkHTTP         string `help:"POST every polled sample batch as JSON to this endpoint. Same record schema as --sink-file. A credential (set via the env var); advisory + failure-isolated." env:"SLUICE_METRICS_SINK_HTTP" placeholder:"URL"`

	NotifyWebhook string `help:"Generic webhook URL to POST threshold alerts to as JSON. Opt-in; only fires when at least one --notify-* threshold is set. ADVISORY — a dead sink is logged-and-swallowed. A credential (set via the env var)." env:"SLUICE_NOTIFY_WEBHOOK" placeholder:"URL"`
	NotifySlack   string `help:"Slack incoming-webhook URL to POST threshold alerts to. Same gating + advisory + failure-isolated semantics as --notify-webhook. A credential (set via the env var)." env:"SLUICE_NOTIFY_SLACK" placeholder:"URL"`

	// Email / SMTP sink (roadmap item 48) — identical to the sync-path flags.
	// Opt-in (inert unless --notify-smtp-host); password via env only.
	NotifySMTPHost string   `help:"SMTP relay hostname to email threshold alerts through (roadmap item 48). Opt-in; the email sink is inert unless this is set. Advisory + failure-isolated." placeholder:"HOST"`
	NotifySMTPPort int      `help:"SMTP relay port. Defaults per --notify-smtp-tls: 587 for starttls/none, 465 for implicit." placeholder:"PORT"`
	NotifySMTPFrom string   `help:"From address for alert emails (required when --notify-smtp-host is set)." placeholder:"ADDR"`
	NotifySMTPTo   []string `help:"Recipient address for alert emails (repeatable; required when --notify-smtp-host is set)." placeholder:"ADDR"`
	// name tag: see SyncStartCmd.NotifySMTPTLS — kong derives
	// "notify-smtptls" from the trailing acronym; the documented spelling
	// is --notify-smtp-tls (TestSyncSpecFlagParity's catch).
	NotifySMTPTLS      string `name:"notify-smtp-tls" aliases:"notify-smtptls" help:"SMTP transport security: starttls (default), implicit, or none." default:"starttls" enum:"starttls,implicit,none" placeholder:"MODE"`
	NotifySMTPAuth     string `help:"SMTP authentication mechanism: none (default), plain, or login." default:"none" enum:"none,plain,login" placeholder:"MECH"`
	NotifySMTPUsername string `help:"SMTP auth username. Required for --notify-smtp-auth=plain|login." placeholder:"USER"`
	NotifySMTPPassword string `help:"SMTP auth secret. Set via the env var SLUICE_NOTIFY_SMTP_PASSWORD ONLY — never on the command line." env:"SLUICE_NOTIFY_SMTP_PASSWORD" placeholder:"SECRET"`

	NotifyStorageUtil         float64 `help:"Alert when storage utilisation (used/capacity, 0-1) is at or above this fraction. 0 (default) disables. Edge-triggered + cooldown'd. Requires a --notify-webhook/--notify-slack sink to deliver." placeholder:"FRAC"`
	NotifyCPUUtil             float64 `help:"Alert when CPU utilisation (0-1) is at or above this fraction. 0 disables." placeholder:"FRAC"`
	NotifyMemUtil             float64 `help:"Alert when memory utilisation (0-1) is at or above this fraction. 0 disables." placeholder:"FRAC"`
	NotifyLagSeconds          float64 `help:"Alert when replica lag (seconds) is at or above this value. 0 disables." placeholder:"SECONDS"`
	NotifyStorageGrowthPerMin float64 `help:"Alert when storage utilisation is CLIMBING at or above this fraction-of-capacity per minute (a pre-grow early warning). e.g. 0.02 = +2%/min. 0 disables." placeholder:"FRAC_PER_MIN"`

	NotifyCooldown time.Duration `help:"Minimum interval between re-fires of a STILL-breached alert. Default 15m." default:"15m" placeholder:"DUR"`

	Interval      time.Duration `help:"Poll/print cadence. Default 60s (the PlanetScale metrics granularity — polling faster only re-reads the same sample)." default:"60s" placeholder:"DUR"`
	Once          bool          `help:"Poll a single sample (after a brief warm-up), print/evaluate it, and exit. The one-shot mode for scripts."`
	Quiet         bool          `help:"Suppress the per-poll live line; only emit threshold alerts (and the startup log). Useful when running headless as an alert-only daemon."`
	MetricsListen string        `help:"Also serve a Prometheus /metrics endpoint at this address (e.g. ':9090') re-exporting the watched database's CPU/mem/storage/lag as the sluice_target_* gauge family (+ sluice_build_info + Go-runtime), turning the daemon into a standalone PlanetScale-metrics exporter. Off by default; ignored with --once." placeholder:"ADDR"`
}

// smtpConfig assembles the [notify.SMTPConfig] email sink (roadmap item 48)
// from the --notify-smtp-* flags + the env-only password. TLS/auth are kong
// enum-validated; an empty Host leaves the sink inert.
func (m *MetricsWatchCmd) smtpConfig() notify.SMTPConfig {
	return notify.SMTPConfig{
		Host:     m.NotifySMTPHost,
		Port:     m.NotifySMTPPort,
		From:     m.NotifySMTPFrom,
		To:       m.NotifySMTPTo,
		Username: m.NotifySMTPUsername,
		Password: m.NotifySMTPPassword,
		TLS:      notify.TLSMode(m.NotifySMTPTLS),
		Auth:     notify.SMTPAuth(m.NotifySMTPAuth),
	}
}

// buildSampleSink assembles the roadmap-75c persistent-sample sink from the
// --sink-* flags: a rotating JSONL file, a generic HTTP push, or both. It
// returns a TRUE nil interface when no sink is configured (NOT a typed-nil
// MultiSink), so the pipeline's `Sink != nil` guard stays exact. A bad file
// path is a LOUD startup refusal — the opt-in must never silently record
// nothing — while every RUNTIME write failure is logged-and-swallowed.
func (m *MetricsWatchCmd) buildSampleSink() (telemetrysink.Sink, error) {
	var sinks []telemetrysink.Sink
	if m.SinkFile != "" {
		fs, err := telemetrysink.NewFileSink(telemetrysink.FileConfig{
			Path:     m.SinkFile,
			MaxBytes: m.SinkFileMaxBytes,
			MaxFiles: m.SinkFileMaxFiles,
		})
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, fs)
	}
	if m.SinkHTTP != "" {
		sinks = append(sinks, &telemetrysink.HTTPSink{URL: m.SinkHTTP})
	}
	multi := telemetrysink.NewMultiSink(sinks...)
	if multi == nil {
		return nil, nil //nolint:nilnil // (nil, nil) == "no sink configured", a valid no-op result distinct from an error
	}
	return multi, nil
}

// Run implements `sluice metrics-watch`.
func (m *MetricsWatchCmd) Run(g *Globals) error {
	if _, err := resolveEngine(m.Engine); err != nil {
		return operationalError{err: fmt.Errorf("--engine: %w", err)}
	}
	if err := m.smtpConfig().Validate(); err != nil {
		return operationalError{err: err}
	}

	ctx := kongContext()

	// ADR-0156 phase 3: whether the TTY-aware live panel will render. Computed
	// BEFORE the telemetry provider is built so its "telemetry enabled" INFO can
	// be suppressed (quiet) on the panel path — that line fires before the
	// panel's slog gate installs and would otherwise leak above the panel.
	// Renders only for an interactive, text-log, non-once invocation.
	pretty := wantPrettyProgress(g, false, false, false) && !m.Once

	// Mode split (roadmap item 75b): --planetscale-metrics-db names ONE
	// database; --fleet watches the whole org. The mode is now selected by an
	// EXPLICIT flag rather than by the ABSENCE of one (audit 2026-07-26
	// ARCH-3).
	//
	// It used to be inferred from an empty --planetscale-metrics-db, which had
	// dropped its `required:""` tag to make org-wide reachable. That turned a
	// wrapper script whose $DB happens to be unset from a loud kong refusal
	// into a silent org-wide fan-out — one that also flips the persisted
	// record's identity from metrics-watch:<db> to metrics-watch:<org> and
	// inverts --planetscale-metrics-branch (unset now meaning EVERY branch).
	// This is the CLI-layer form of the zero-value-safe-config rule: a mode
	// whose "on" state is the zero value will eventually be entered by
	// accident.
	switch {
	case m.Fleet && m.PlanetScaleMetricsDB != "":
		return errors.New("metrics-watch: --fleet and --planetscale-metrics-db are mutually exclusive: " +
			"--fleet watches every database in the org, --planetscale-metrics-db watches exactly one. Pass one or the other")
	case !m.Fleet && m.PlanetScaleMetricsDB == "":
		return errors.New("metrics-watch: pass --planetscale-metrics-db <database> to watch one database, " +
			"or --fleet to watch every database in the org. Neither was given — and org-wide is NOT the default, " +
			"because a wrapper script with an unset variable would otherwise fan out across the whole org silently")
	}
	orgWide := m.Fleet

	var (
		provider *pstelemetry.Provider
		fleet    *pstelemetry.Fleet
		err      error
	)
	if orgWide {
		fleet, err = buildFleetTelemetryProvider(ctx, fleetTelemetryParams{
			org:         m.PlanetScaleOrg,
			tokenID:     m.PlanetScaleMetricsTokenID,
			token:       m.PlanetScaleMetricsToken,
			branch:      m.PlanetScaleMetricsBranch,
			include:     m.IncludeDatabase,
			exclude:     m.ExcludeDatabase,
			engine:      m.Engine,
			concurrency: m.FleetConcurrency,
			quiet:       pretty,
		})
	} else {
		provider, err = buildTargetTelemetryProvider(ctx, telemetryParams{
			org:       m.PlanetScaleOrg,
			tokenID:   m.PlanetScaleMetricsTokenID,
			token:     m.PlanetScaleMetricsToken,
			metricsDB: m.PlanetScaleMetricsDB,
			branch:    m.PlanetScaleMetricsBranch,
			targetDSN: "", // standalone: no data-plane DSN; metrics-db is supplied directly
			engine:    m.Engine,
			quiet:     pretty,
		})
	}
	if err != nil {
		return operationalError{err: err}
	}
	if provider != nil {
		defer func() { _ = provider.Close() }()
	}
	if fleet != nil {
		defer func() { _ = fleet.Close() }()
	}

	sampleSink, err := m.buildSampleSink()
	if err != nil {
		return operationalError{err: err}
	}
	if sampleSink != nil {
		defer func() { _ = sampleSink.Close() }()
	}

	label := "metrics-watch:" + m.PlanetScaleMetricsDB
	if orgWide {
		label = "metrics-watch:" + m.PlanetScaleOrg
	}
	cfg := pipeline.MetricsWatchConfig{
		StorageUtil:         m.NotifyStorageUtil,
		CPUUtil:             m.NotifyCPUUtil,
		MemUtil:             m.NotifyMemUtil,
		LagSeconds:          m.NotifyLagSeconds,
		StorageGrowthPerMin: m.NotifyStorageGrowthPerMin,
		Cooldown:            m.NotifyCooldown,
		WebhookURL:          m.NotifyWebhook,
		SlackWebhookURL:     m.NotifySlack,
		SMTP:                m.smtpConfig(),
		Interval:            m.Interval,
		Label:               label,
		Database:            m.PlanetScaleMetricsDB,
		Branch:              branchOrMainLabel(m.PlanetScaleMetricsBranch),
		Once:                m.Once,
		Print:               !m.Quiet,
		Out:                 os.Stdout,
		MetricsListen:       m.MetricsListen,
		BuildVersion:        version,
		BuildCommit:         commit,
		Fleet:               fleetProviderOrNil(fleet),
		Sink:                sampleSink,
	}
	// The panel gate (`pretty`) was computed above, before the telemetry build,
	// so its enable-INFO could be suppressed on the panel path. --once is a
	// scripted one-shot with no long-lived loop to render; every non-panel
	// invocation keeps today's byte-identical sample-line stream.
	provNil := telemetryProviderOrNil(provider)
	if pretty {
		watching := m.PlanetScaleMetricsDB
		if orgWide {
			watching = "all databases"
		}
		header := progress.LiveHeader{
			Mode:   "metrics-watch",
			Detail: "watching " + watching + "  ·  " + m.PlanetScaleOrg,
		}
		err := runReadoutLivePanel(ctx, header, func(sink *progress.LiveTTYSink) func(context.Context) error {
			cfg.Readout = sink.Readout
			cfg.Event = sink.Event
			return func(runCtx context.Context) error {
				return pipeline.RunMetricsWatch(runCtx, provNil, cfg)
			}
		})
		if err != nil {
			return operationalError{err: err}
		}
		return nil
	}

	if err := pipeline.RunMetricsWatch(ctx, provNil, cfg); err != nil {
		return operationalError{err: err}
	}
	return nil
}
