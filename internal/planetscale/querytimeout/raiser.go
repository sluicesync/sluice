// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package querytimeout is the control-plane half of the opt-in PlanetScale
// query-timeout raise (ADR-0182).
//
// [Raiser] implements [ir.QueryTimeoutController]: for the duration of a
// `migrate` it raises the target keyspace's queryserver-config-query-timeout
// to PlanetScale's 3600s maximum, then restores whatever value it found. The
// raise unblocks the direct index/constraint/copy path on tiers where the
// work fits under 3600s but not the default ~900s wall — a useful option that
// composes with --upfront-indexes and the ADR-0148 deploy-request fallback,
// NOT a silver bullet (on network-attached storage a large index build
// exceeds even 3600s, so the raise does not help that case).
//
// Lives in this package (not the engine-neutral pipeline, not the mysql
// engine) so it composes the api.Client and the fake-API test harness
// directly; the CLI is the composer, exactly as the ADR-0148 index fallback
// is. The pipeline owns the crash-safe WORKFLOW (recording the previous value
// in migrate-state before the raise, reverting on every exit path, the size
// gate); this type owns the control-plane HOW.
//
// The config change is KEYSPACE-WIDE and its rollout is a rolling tablet
// restart (~2–6m each way) that affects everyone else on the keyspace — the
// arming flag's help text says so, and this package never raises unless the
// pipeline armed and size-gated it.
package querytimeout

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/planetscale/api"
)

const (
	// queryTimeoutOption is the vttablet config key ADR-0182 raises.
	queryTimeoutOption = "queryserver-config-query-timeout"

	// MaxQueryTimeout is PlanetScale's documented maximum for
	// [queryTimeoutOption], the value the raise targets.
	MaxQueryTimeout = "3600"

	// defaultQueryTimeout is the Vitess default the revert restores when the
	// keyspace recorded no explicit vttablet option (an empty
	// previous_options / vttablet_options ⇒ the keyspace was at 900).
	defaultQueryTimeout = "900"

	// changeTypeVTTablet selects the keyspace-level vttablet config change
	// (as opposed to "mysqld"; vtgate config is branch-level and not
	// relevant here).
	changeTypeVTTablet = "vttablet"

	rolloutStateComplete = "complete"
)

// Raiser is the PlanetScale implementation of [ir.QueryTimeoutController].
// Construct once per migrate run (the CLI arms it only for a planetscale
// target with an org + service token).
type Raiser struct {
	// API is the shared control-plane client. Required.
	API *api.Client

	// Org, Database, Branch identify the production branch the migrate
	// targets (Branch defaults to "main"). Database is the PlanetScale
	// database name — the CLI derives it from the target DSN when the
	// operator doesn't pass it explicitly.
	Org      string
	Database string
	Branch   string

	// PollInterval shapes the rollout polling cadence; zero defaults to
	// [defaultPollInterval].
	PollInterval time.Duration

	// RolloutTimeout bounds the wait for one rolling restart to complete AND
	// the wait for a pre-existing config change to settle. Zero defaults to
	// [defaultRolloutTimeout]. A rolling restart is minutes; the bound
	// exists so a wedged rollout (or someone else's never-settling change)
	// is a loud refusal rather than a hang.
	RolloutTimeout time.Duration

	// resolvedKeyspace caches the keyspace resolution across a run's raise
	// and revert so the multi-keyspace refusal and the list call happen once.
	resolvedKeyspace string
}

const (
	defaultPollInterval   = 10 * time.Second
	defaultRolloutTimeout = 20 * time.Minute
)

// Compile-time proof the raiser satisfies the pipeline-facing surface.
var _ ir.QueryTimeoutController = (*Raiser)(nil)

// Raise implements [ir.QueryTimeoutController].
func (r *Raiser) Raise(ctx context.Context, record func(previous string) error) error {
	ks, current, err := r.resolveAndSettle(ctx)
	if err != nil {
		return err
	}
	if current == MaxQueryTimeout {
		slog.InfoContext(ctx,
			"planetscale: keyspace query timeout is already at the maximum; skipping the raise (nothing to revert)",
			slog.String("keyspace", ks), slog.String("query_timeout", current))
		return nil
	}
	// Persist the previous value BEFORE anything takes effect — a crash
	// during the rollout below must leave a record the next run reverts.
	if err := record(current); err != nil {
		return fmt.Errorf("planetscale: record query-timeout raise before applying: %w", err)
	}
	slog.InfoContext(ctx,
		"planetscale: raising the keyspace query timeout for the migration — this is a KEYSPACE-WIDE config change and its rollout is a rolling tablet restart (~2–6m) affecting anyone else on the keyspace (ADR-0182)",
		slog.String("keyspace", ks),
		slog.String("from", current), slog.String("to", MaxQueryTimeout))
	return r.applyTimeout(ctx, ks, current, MaxQueryTimeout)
}

// Revert implements [ir.QueryTimeoutController].
func (r *Raiser) Revert(ctx context.Context, previous string) error {
	target := previous
	if target == "" {
		// The keyspace recorded no explicit option when we raised it (it was
		// at the Vitess default), so restore the documented default rather
		// than leave it pinned at the maximum.
		target = defaultQueryTimeout
	}
	ks, current, err := r.resolveAndSettle(ctx)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx,
		"planetscale: reverting the keyspace query timeout — rolling tablet restart (~2–6m) (ADR-0182)",
		slog.String("keyspace", ks),
		slog.String("from", current), slog.String("to", target))
	return r.applyTimeout(ctx, ks, current, target)
}

// resolveAndSettle resolves the single target keyspace, waits out any config
// change already in progress (bounded — a refusal, not a hang), and returns
// the keyspace name plus its current query-timeout value. It is the shared
// preamble to both Raise and Revert.
func (r *Raiser) resolveAndSettle(ctx context.Context) (keyspace, current string, err error) {
	ks, err := r.resolveKeyspace(ctx)
	if err != nil {
		return "", "", err
	}
	// Do not stack config changes: if one is rolling out, wait for it to
	// settle (this also covers our OWN crashed raise whose rollout was still
	// in flight), bounded by RolloutTimeout so someone else's never-settling
	// change refuses loudly instead of hanging.
	if err := r.waitConfigChangeSettled(ctx, ks); err != nil {
		return "", "", err
	}
	got, err := r.API.GetKeyspace(ctx, r.Org, r.Database, r.branch(), ks)
	if err != nil {
		return "", "", fmt.Errorf("planetscale: read keyspace %q: %w", ks, err)
	}
	return ks, currentQueryTimeout(got.VTTabletOptions), nil
}

// resolveKeyspace lists the branch's keyspaces and returns the single one, or
// refuses loudly on zero or more than one (naming them) — picking a keyspace
// on a sharded/multi-keyspace database is out of scope and must not be
// guessed. Cached across a run's raise + revert.
func (r *Raiser) resolveKeyspace(ctx context.Context) (string, error) {
	if r.resolvedKeyspace != "" {
		return r.resolvedKeyspace, nil
	}
	kss, err := r.API.ListKeyspaces(ctx, r.Org, r.Database, r.branch())
	if err != nil {
		return "", fmt.Errorf("planetscale: list keyspaces for database %q branch %q: %w", r.Database, r.branch(), err)
	}
	switch len(kss) {
	case 0:
		return "", fmt.Errorf("planetscale: database %q branch %q reports no keyspaces; cannot raise the query timeout", r.Database, r.branch())
	case 1:
		r.resolvedKeyspace = kss[0].Name
		return r.resolvedKeyspace, nil
	default:
		names := make([]string, len(kss))
		for i := range kss {
			names[i] = kss[i].Name
		}
		return "", fmt.Errorf("planetscale: database %q branch %q has %d keyspaces (%s); --planetscale-raise-query-timeout does not support a multi-keyspace/sharded database — choosing which keyspace to raise is out of scope",
			r.Database, r.branch(), len(kss), strings.Join(names, ", "))
	}
}

// applyTimeout stages, submits, and waits out the rollout of a vttablet
// config change setting the keyspace query timeout to value. A no-op (no
// rolling restart) when the keyspace already holds value.
func (r *Raiser) applyTimeout(ctx context.Context, keyspace, current, value string) error {
	if current == value {
		slog.InfoContext(ctx,
			"planetscale: keyspace query timeout already at the target value; no config change needed",
			slog.String("keyspace", keyspace), slog.String("query_timeout", value))
		return nil
	}
	change, err := r.API.CreateKeyspaceConfigChange(ctx, r.Org, r.Database, r.branch(), keyspace,
		changeTypeVTTablet, map[string]string{queryTimeoutOption: value})
	if err != nil {
		return fmt.Errorf("planetscale: stage query-timeout config change on keyspace %q: %w", keyspace, err)
	}
	if change.ID == "" {
		return fmt.Errorf("planetscale: staged query-timeout config change on keyspace %q returned no id", keyspace)
	}
	if err := r.API.SubmitConfigChanges(ctx, r.Org, r.Database, r.branch(), []string{change.ID}); err != nil {
		return fmt.Errorf("planetscale: submit query-timeout config change %q on keyspace %q: %w", change.ID, keyspace, err)
	}
	return r.waitRolloutComplete(ctx, keyspace)
}

// waitConfigChangeSettled blocks until the keyspace reports no config change
// in progress, bounded by RolloutTimeout.
func (r *Raiser) waitConfigChangeSettled(ctx context.Context, keyspace string) error {
	ks, err := r.API.GetKeyspace(ctx, r.Org, r.Database, r.branch(), keyspace)
	if err != nil {
		return fmt.Errorf("planetscale: read keyspace %q: %w", keyspace, err)
	}
	if !ks.ConfigChangeInProgress {
		return nil
	}
	slog.InfoContext(ctx,
		"planetscale: a config change is already rolling out on the keyspace; waiting for it to settle before touching the query timeout",
		slog.String("keyspace", keyspace))
	deadline := time.Now().Add(r.rolloutTimeout())
	for {
		if err := r.sleep(ctx); err != nil {
			return err
		}
		ks, err := r.API.GetKeyspace(ctx, r.Org, r.Database, r.branch(), keyspace)
		if err != nil {
			return fmt.Errorf("planetscale: poll keyspace %q for config-change settle: %w", keyspace, err)
		}
		if !ks.ConfigChangeInProgress {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("planetscale: a config change on keyspace %q did not settle within %s; refusing to stack a second one — retry once it completes",
				keyspace, r.rolloutTimeout())
		}
	}
}

// waitRolloutComplete polls the keyspace's rollout status until it reports
// complete AND the keyspace stops reporting config_change_in_progress,
// bounded by RolloutTimeout.
func (r *Raiser) waitRolloutComplete(ctx context.Context, keyspace string) error {
	deadline := time.Now().Add(r.rolloutTimeout())
	for {
		status, err := r.API.GetKeyspaceRolloutStatus(ctx, r.Org, r.Database, r.branch(), keyspace)
		if err != nil {
			return fmt.Errorf("planetscale: poll rollout status on keyspace %q: %w", keyspace, err)
		}
		if status.State == rolloutStateComplete {
			ks, err := r.API.GetKeyspace(ctx, r.Org, r.Database, r.branch(), keyspace)
			if err != nil {
				return fmt.Errorf("planetscale: read keyspace %q after rollout: %w", keyspace, err)
			}
			if !ks.ConfigChangeInProgress {
				slog.InfoContext(ctx, "planetscale: keyspace config-change rollout complete",
					slog.String("keyspace", keyspace))
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("planetscale: keyspace %q config-change rollout did not complete within %s (last state %q)",
				keyspace, r.rolloutTimeout(), status.State)
		}
		if err := r.sleep(ctx); err != nil {
			return err
		}
	}
}

// currentQueryTimeout reads the query-timeout out of a keyspace's
// vttablet_options, defaulting to the Vitess default when unset (an empty or
// absent map means the keyspace is at 900).
func currentQueryTimeout(vttablet map[string]string) string {
	if v, ok := vttablet[queryTimeoutOption]; ok && v != "" {
		return v
	}
	return defaultQueryTimeout
}

func (r *Raiser) branch() string {
	if r.Branch == "" {
		return "main"
	}
	return r.Branch
}

func (r *Raiser) pollInterval() time.Duration {
	if r.PollInterval <= 0 {
		return defaultPollInterval
	}
	return r.PollInterval
}

func (r *Raiser) rolloutTimeout() time.Duration {
	if r.RolloutTimeout <= 0 {
		return defaultRolloutTimeout
	}
	return r.RolloutTimeout
}

// sleep waits one poll interval via the client's injectable clock (ctx-aware),
// so tests don't spend wall-clock time.
func (r *Raiser) sleep(ctx context.Context) error {
	if err := r.API.SleepFor(ctx, r.pollInterval()); err != nil {
		return fmt.Errorf("planetscale: cancelled while polling a keyspace rollout: %w", err)
	}
	return nil
}
