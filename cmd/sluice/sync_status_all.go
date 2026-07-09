// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// runStatusAllOnce is the fleet-view one-shot path (ADR-0122 §6): query
// every DISTINCT target in the fleet (deduped so a shared target is hit
// once), merge every stream into one set, and render it through the
// EXISTING status renderer. A target that can't be reached is reported
// inline and skipped (a dead target must not blank the whole fleet
// view — the same failure-isolation discipline the supervisor uses).
func runStatusAllOnce(ctx context.Context, fleet *SyncFleetConfig, out io.Writer, opts statusRenderOpts) error {
	streams, err := collectFleetStreams(ctx, fleet, out, listTargetStreams)
	if err != nil {
		return err
	}
	streams = filterStreams(streams, opts.StreamID)
	// No consolidation leases in the fleet roll-up: leases are a
	// per-target Shape-A surface, out of scope for the aggregate view.
	return renderStatus(out, streams, nil, opts, time.Now())
}

// runStatusAllWatch is the live-refresh fleet view. Mirrors
// runStatusWatch but over the aggregated set.
func runStatusAllWatch(ctx context.Context, fleet *SyncFleetConfig, out io.Writer, opts statusRenderOpts, interval time.Duration) error {
	render := func() error {
		// ESC[2J clear + ESC[H home, same as the single-target watch.
		if _, err := fmt.Fprint(out, "\x1b[2J\x1b[H"); err != nil {
			return err
		}
		return runStatusAllOnce(ctx, fleet, out, opts)
	}
	if err := render(); err != nil {
		return err
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := render(); err != nil {
				fmt.Fprintf(out, "watch: refresh failed: %v\n", err)
			}
		}
	}
}

// targetStreamLister lists the streams recorded on one target. Injected
// so the fleet aggregation (dedup + failure-isolation) is unit-testable
// without real databases; production passes listTargetStreams. controlKeyspace
// is the per-sync --control-keyspace (task 1) — the sidecar keyspace whose
// control tables the streams live in on a sharded MySQL/VStream target.
type targetStreamLister func(ctx context.Context, driver, dsn, controlKeyspace string) ([]ir.StreamStatus, error)

// collectFleetStreams opens each distinct target applier in the fleet,
// lists its streams, and returns the merged set. Distinct targets are
// keyed by driver+DSN+control-keyspace so a shared target is queried once —
// but two syncs pointing at the same server through DIFFERENT control
// keyspaces are queried separately, since their control tables (and thus
// their stream rows) live in different keyspaces. An applier open / list
// failure for one target is reported to out and skipped, so one unreachable
// target doesn't blank the rest of the fleet view — but when EVERY
// target errored, the roll-up returns an error (non-zero exit): an
// all-dead fleet view rendering an empty-but-successful table would
// read as "no streams" to a script checking $?, which is exactly the
// wrong signal.
func collectFleetStreams(ctx context.Context, fleet *SyncFleetConfig, out io.Writer, list targetStreamLister) ([]ir.StreamStatus, error) {
	type targetRef struct {
		driver          string
		dsn             string
		controlKeyspace string
	}
	seen := make(map[targetRef]bool)
	var merged []ir.StreamStatus
	unreachable := 0

	for i := range fleet.Syncs {
		spec := &fleet.Syncs[i]
		ref := targetRef{driver: spec.TargetDriver, dsn: spec.Target, controlKeyspace: spec.ControlKeyspace}
		if seen[ref] {
			continue
		}
		seen[ref] = true

		streams, err := list(ctx, ref.driver, ref.dsn, ref.controlKeyspace)
		if err != nil {
			// Failure-isolated: report and keep going.
			fmt.Fprintf(out, "status --all: target %s://%s unreachable: %v\n",
				ref.driver, dsnEndpoint(ref.dsn), err)
			unreachable++
			continue
		}
		merged = append(merged, streams...)
	}
	if unreachable > 0 && unreachable == len(seen) {
		// A plain error: the generic exit 1 (the taxonomy's 2 is
		// config-load, 3 is coded refusals; total unreachability is a
		// runtime failure). The per-target lines above already name
		// each endpoint and cause.
		return nil, fmt.Errorf("status --all: all %d configured target(s) unreachable", unreachable)
	}
	return merged, nil
}

// listTargetStreams opens a one-shot applier for the given target and
// returns its recorded streams. controlKeyspace is resolved + recorded on the
// engine (same applyControlKeyspace chain as `sync start`) BEFORE the applier
// opens, so a sharded MySQL/VStream target's control tables are read from the
// right sidecar keyspace. The applier is closed before returning.
func listTargetStreams(ctx context.Context, driver, dsn, controlKeyspace string) ([]ir.StreamStatus, error) {
	target, err := resolveEngine(driver)
	if err != nil {
		return nil, fmt.Errorf("target-driver: %w", err)
	}
	if target, err = applyControlKeyspace(ctx, target, controlKeyspace, dsn); err != nil {
		return nil, err
	}
	applier, err := target.OpenChangeApplier(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open target applier: %w", err)
	}
	defer func() {
		if c, ok := applier.(io.Closer); ok {
			_ = c.Close()
		}
	}()
	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("list streams: %w", err)
	}
	return streams, nil
}
