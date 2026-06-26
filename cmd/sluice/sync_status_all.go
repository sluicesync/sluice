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
// without real databases; production passes listTargetStreams.
type targetStreamLister func(ctx context.Context, driver, dsn string) ([]ir.StreamStatus, error)

// collectFleetStreams opens each distinct target applier in the fleet,
// lists its streams, and returns the merged set. Distinct targets are
// keyed by driver+DSN so a shared target is queried once. An applier
// open / list failure for one target is reported to out and skipped, so
// one unreachable target doesn't blank the rest of the fleet view.
func collectFleetStreams(ctx context.Context, fleet *SyncFleetConfig, out io.Writer, list targetStreamLister) ([]ir.StreamStatus, error) {
	type targetRef struct {
		driver string
		dsn    string
	}
	seen := make(map[targetRef]bool)
	var merged []ir.StreamStatus

	for i := range fleet.Syncs {
		spec := &fleet.Syncs[i]
		ref := targetRef{driver: spec.TargetDriver, dsn: spec.Target}
		if seen[ref] {
			continue
		}
		seen[ref] = true

		streams, err := list(ctx, ref.driver, ref.dsn)
		if err != nil {
			// Failure-isolated: report and keep going.
			fmt.Fprintf(out, "status --all: target %s://%s unreachable: %v\n",
				ref.driver, dsnEndpoint(ref.dsn), err)
			continue
		}
		merged = append(merged, streams...)
	}
	return merged, nil
}

// listTargetStreams opens a one-shot applier for the given target and
// returns its recorded streams. The applier is closed before returning.
func listTargetStreams(ctx context.Context, driver, dsn string) ([]ir.StreamStatus, error) {
	target, err := resolveEngine(driver)
	if err != nil {
		return nil, fmt.Errorf("target-driver: %w", err)
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
