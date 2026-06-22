// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"sluicesync.dev/sluice/internal/diagnose"
	"sluicesync.dev/sluice/internal/ir"
)

// telemetryWarmupTimeout bounds how long `sluice diagnose` waits for the
// freshly-constructed PlanetScale provider's background poll to land a first
// sample. Sized just above the provider's 10s per-poll HTTP timeout so one
// successful immediate poll lands; on timeout the bundle's target-health
// section degrades to "no fresh sample" (advisory — never fatal).
const telemetryWarmupTimeout = 12 * time.Second

// waitForFirstTelemetrySample blocks (bounded by [telemetryWarmupTimeout] or
// ctx) until the provider returns a fresh sample, so the one-shot diagnose
// bundle captures real data rather than the cold cache. It is best-effort:
// returning on timeout simply yields the "no fresh sample" section.
func waitForFirstTelemetrySample(ctx context.Context, provider ir.TargetTelemetry) {
	deadline := time.Now().Add(telemetryWarmupTimeout)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
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
		case <-tick.C:
		}
	}
}

// DiagnoseCmd implements `sluice diagnose` — the operator-bundle
// assembler. ADR-0056.
//
// Two invocation paths exist:
//
//  1. Operator-initiated: `sluice diagnose --stream-id X --output
//     bundle.zip --privacy=standard`. Produces a ZIP suitable for
//     attaching to a GitHub issue.
//  2. Automatic on crash: the long-running subcommands (sync start,
//     migrate, sync from-backup run) expose a `--diagnose-on-crash-dir`
//     flag that, when set, installs an [diagnose.CrashHook] writing a
//     bundle to the named directory whenever the subcommand exits with
//     an error. Opt-in only (default off).
//
// **Exit codes.** Mirror `sluice verify`/`sluice diff`:
//
//   - 0 — bundle written successfully.
//   - 2 — operational error (couldn't open the target, couldn't write
//     the ZIP, etc.).
//
// The diagnose subcommand does NOT have a "drift" / threshold-style
// exit code 1; the bundle either assembles or it doesn't.
type DiagnoseCmd struct {
	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	SourceDriver string `help:"Source engine name (optional). When set together with --source, the bundle also includes a source-side engine snapshot + cross-engine health probe." placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN (optional). See --source-driver." env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`
	SlotName     string `help:"Replication-slot name on the source (PG-only, requires --source). Defaults to 'sluice_slot' on PG sources." placeholder:"NAME" group:"source"`

	StreamID string `help:"Stream identifier the bundle is scoped to. Required — the bundle without a stream ID would be meaningless." required:"" placeholder:"ID"`
	Output   string `help:"Path to write the bundle ZIP to." required:"" short:"o" placeholder:"PATH"`

	Privacy string `help:"Privacy level (basic|standard|verbose). basic: state-table dumps only — no version, no DSN, no logs. standard: + redacted CLI args, sluice version, engine health probes, capabilities, target-health telemetry. verbose: + per-table COUNT(*) on the target (slow path on large tables) + last 200 lines of --log-file. ADR-0056 has the full inclusion/exclusion contract." default:"standard" enum:"basic,standard,verbose" placeholder:"LEVEL"`

	LogFile string `help:"Path to sluice's slog output file. PrivacyVerbose includes the last 200 lines in the bundle. Empty disables." placeholder:"PATH"`

	// Optional PlanetScale target-health telemetry (ADR-0107) — when set, the
	// PrivacyStandard+ bundle includes a one-shot CPU/mem/storage/lag snapshot
	// of the target so the recipient sees WHY apply was slow. Off when unset.
	PlanetScaleOrg            string `help:"PlanetScale org slug; enables the OPTIONAL target-health telemetry snapshot (CPU/mem/storage) in the bundle (ADR-0107). Requires --planetscale-metrics-token-id and --planetscale-metrics-token. Control-plane only — distinct from --target." placeholder:"ORG" group:"target"`
	PlanetScaleMetricsTokenID string `help:"PlanetScale service-token ID (read_metrics_endpoints) for the target-health snapshot. Prefer the env var." env:"PLANETSCALE_METRICS_TOKEN_ID" placeholder:"ID" group:"target"`
	PlanetScaleMetricsToken   string `help:"PlanetScale service-token secret for the target-health snapshot. Set via the env var; masked in all logging." env:"PLANETSCALE_METRICS_TOKEN" placeholder:"SECRET" group:"target"`
	PlanetScaleMetricsBranch  string `help:"Target branch to filter telemetry series to (defaults to 'main'). Only consulted when --planetscale-org is set." placeholder:"BRANCH" group:"target"`
	PlanetScaleMetricsDB      string `help:"Target database name to filter PlanetScale telemetry SD to. Defaults to the --target DSN's database. Only consulted when --planetscale-org is set." placeholder:"DATABASE" group:"target"`
}

// Run implements `sluice diagnose`.
func (d *DiagnoseCmd) Run(_ *Globals) error {
	target, err := resolveEngine(d.TargetDriver)
	if err != nil {
		return operationalError{err: fmt.Errorf("--target-driver: %w", err)}
	}
	req := diagnose.Request{
		StreamID:        d.StreamID,
		TargetEngine:    target,
		TargetDSN:       d.Target,
		SluiceVersion:   version,
		SluiceCommit:    commit,
		SluiceBuildDate: date,
		CLIArgs:         os.Args[1:],
		LogFile:         d.LogFile,
		SlotName:        d.SlotName,
	}
	if d.SourceDriver != "" {
		source, err := resolveEngine(d.SourceDriver)
		if err != nil {
			return operationalError{err: fmt.Errorf("--source-driver: %w", err)}
		}
		req.SourceEngine = source
		req.SourceDSN = d.Source
	} else if d.Source != "" {
		return operationalError{err: errors.New("--source given without --source-driver")}
	}

	level, err := diagnose.ParsePrivacyLevel(d.Privacy)
	if err != nil {
		return operationalError{err: err}
	}
	req.PrivacyLevel = level

	ctx := kongContext()

	// Optional PlanetScale target-health telemetry (ADR-0107 Phase 3(b)). When
	// the operator supplied --planetscale-org, construct a one-shot provider and
	// give its background poll a brief moment to land a first sample before the
	// bundle is assembled. A telemetry failure is NEVER fatal to the bundle —
	// the section degrades to "no fresh sample" (the same advisory contract the
	// sync path follows).
	if d.PlanetScaleOrg != "" {
		provider, terr := buildTargetTelemetryProvider(ctx, telemetryParams{
			org:       d.PlanetScaleOrg,
			tokenID:   d.PlanetScaleMetricsTokenID,
			token:     d.PlanetScaleMetricsToken,
			metricsDB: d.PlanetScaleMetricsDB,
			branch:    d.PlanetScaleMetricsBranch,
			targetDSN: d.Target,
			engine:    d.TargetDriver,
		})
		if terr != nil {
			// An incomplete opt-in (org without token pair) is the one loud
			// telemetry refusal — surface it; the operator asked for it.
			return operationalError{err: terr}
		}
		if provider != nil {
			defer func() { _ = provider.Close() }()
			waitForFirstTelemetrySample(ctx, provider)
			req.TargetTelemetry = provider
		}
	}

	f, err := os.Create(d.Output) //nolint:gosec // operator-supplied output path
	if err != nil {
		return operationalError{err: fmt.Errorf("create output %q: %w", d.Output, err)}
	}
	defer func() { _ = f.Close() }()

	if err := diagnose.Write(ctx, f, req); err != nil {
		// Clean up the partial file — a half-written zip is just
		// noise on the operator's disk. Best-effort; if removal
		// fails we still surface the underlying error.
		_ = f.Close()
		_ = os.Remove(d.Output)
		return operationalError{err: fmt.Errorf("assemble bundle: %w", err)}
	}
	fmt.Fprintf(os.Stdout, "diagnose bundle written to %s (privacy=%s, stream=%s)\n", d.Output, level, d.StreamID)
	return nil
}
