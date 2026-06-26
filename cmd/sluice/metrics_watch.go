// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"time"

	"sluicesync.dev/sluice/internal/notify"
	"sluicesync.dev/sluice/internal/pipeline"
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
	PlanetScaleMetricsBranch  string `name:"planetscale-metrics-branch" help:"Branch to filter telemetry series to (defaults to 'main')." placeholder:"BRANCH"`
	PlanetScaleMetricsDB      string `name:"planetscale-metrics-db" help:"Database name to watch (REQUIRED — there is no --target DSN to derive it from)." required:"" placeholder:"DATABASE"`

	NotifyWebhook string `help:"Generic webhook URL to POST threshold alerts to as JSON. Opt-in; only fires when at least one --notify-* threshold is set. ADVISORY — a dead sink is logged-and-swallowed. A credential (set via the env var)." env:"SLUICE_NOTIFY_WEBHOOK" placeholder:"URL"`
	NotifySlack   string `help:"Slack incoming-webhook URL to POST threshold alerts to. Same gating + advisory + failure-isolated semantics as --notify-webhook. A credential (set via the env var)." env:"SLUICE_NOTIFY_SLACK" placeholder:"URL"`

	// Email / SMTP sink (roadmap item 48) — identical to the sync-path flags.
	// Opt-in (inert unless --notify-smtp-host); password via env only.
	NotifySMTPHost     string   `help:"SMTP relay hostname to email threshold alerts through (roadmap item 48). Opt-in; the email sink is inert unless this is set. Advisory + failure-isolated." placeholder:"HOST"`
	NotifySMTPPort     int      `help:"SMTP relay port. Defaults per --notify-smtp-tls: 587 for starttls/none, 465 for implicit." placeholder:"PORT"`
	NotifySMTPFrom     string   `help:"From address for alert emails (required when --notify-smtp-host is set)." placeholder:"ADDR"`
	NotifySMTPTo       []string `help:"Recipient address for alert emails (repeatable; required when --notify-smtp-host is set)." placeholder:"ADDR"`
	NotifySMTPTLS      string   `help:"SMTP transport security: starttls (default), implicit, or none." default:"starttls" enum:"starttls,implicit,none" placeholder:"MODE"`
	NotifySMTPAuth     string   `help:"SMTP authentication mechanism: none (default), plain, or login." default:"none" enum:"none,plain,login" placeholder:"MECH"`
	NotifySMTPUsername string   `help:"SMTP auth username. Required for --notify-smtp-auth=plain|login." placeholder:"USER"`
	NotifySMTPPassword string   `help:"SMTP auth secret. Set via the env var SLUICE_NOTIFY_SMTP_PASSWORD ONLY — never on the command line." env:"SLUICE_NOTIFY_SMTP_PASSWORD" placeholder:"SECRET"`

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

// Run implements `sluice metrics-watch`.
func (m *MetricsWatchCmd) Run(_ *Globals) error {
	if _, err := resolveEngine(m.Engine); err != nil {
		return operationalError{err: fmt.Errorf("--engine: %w", err)}
	}
	if err := m.smtpConfig().Validate(); err != nil {
		return operationalError{err: err}
	}

	ctx := kongContext()

	provider, err := buildTargetTelemetryProvider(ctx, telemetryParams{
		org:       m.PlanetScaleOrg,
		tokenID:   m.PlanetScaleMetricsTokenID,
		token:     m.PlanetScaleMetricsToken,
		metricsDB: m.PlanetScaleMetricsDB,
		branch:    m.PlanetScaleMetricsBranch,
		targetDSN: "", // standalone: no data-plane DSN; metrics-db is supplied directly
		engine:    m.Engine,
	})
	if err != nil {
		return operationalError{err: err}
	}
	if provider != nil {
		defer func() { _ = provider.Close() }()
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
		Label:               "metrics-watch:" + m.PlanetScaleMetricsDB,
		Once:                m.Once,
		Print:               !m.Quiet,
		Out:                 os.Stdout,
		MetricsListen:       m.MetricsListen,
		BuildVersion:        version,
		BuildCommit:         commit,
	}
	if err := pipeline.RunMetricsWatch(ctx, telemetryProviderOrNil(provider), cfg); err != nil {
		return operationalError{err: err}
	}
	return nil
}
