// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// SyncHealthCmd implements `sluice sync health` (proto-ADR
// docs/dev/design-sync-health-monitoring.md, MVP slice).
//
// Reads the target's sluice_cdc_state for the supplied --stream-id,
// computes wall-clock seconds-since-last-apply, compares against
// operator-supplied thresholds, returns structured exit code:
//
//   - 0 healthy.
//   - 1 stale (a threshold was breached).
//   - 2 operational error (couldn't connect, stream not found, etc.).
//
// v0.13.0 MVP exposes only target-side state — what the existing
// ListStreams surface already carries (UpdatedAt + Position). Source-
// side position comparison + true lag-events / lag-seconds metrics
// follow in a subsequent release with the new ir.HealthReporter
// interface; today's MVP closes the cron-friendly "is the target
// still ticking?" probe gap, which is the load-bearing operator
// concern (Fivetran-stops-silently shape).
type SyncHealthCmd struct {
	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`
	StreamID     string `help:"Stream identifier to probe. Required — point at the stream you want to monitor." required:"" placeholder:"ID"`

	MaxStaleSeconds int `help:"Threshold: exit 1 if target's last apply was more than N seconds ago. 0 disables the check (informational only)." default:"0" placeholder:"N"`

	Format string `help:"Output format: 'text' (default) or 'json' (machine-readable for alertmanager / scripting pipes)." default:"text" enum:"text,json" placeholder:"FORMAT"`
	Output string `help:"Write to FILE instead of stdout. Atomic." short:"o" placeholder:"FILE"`
}

// HealthResult is the structured output of `sluice sync health`. Same
// shape across text + JSON renderers; the JSON encoder consumes this
// directly.
type HealthResult struct {
	StreamID              string `json:"stream_id"`
	Found                 bool   `json:"found"`
	Position              string `json:"position,omitempty"`
	UpdatedAt             string `json:"updated_at,omitempty"`
	SecondsSinceLastApply int64  `json:"seconds_since_last_apply,omitempty"`
	Threshold             int    `json:"max_stale_seconds_threshold,omitempty"`
	Stale                 bool   `json:"stale"`
}

// Run implements `sluice sync health`. Same boilerplate shape as
// SyncStatusCmd — open the target's ChangeApplier, list streams,
// filter to the requested stream, then evaluate thresholds.
func (s *SyncHealthCmd) Run(_ *Globals) error {
	target, err := resolveEngine(s.TargetDriver)
	if err != nil {
		return operationalError{err: fmt.Errorf("--target-driver: %w", err)}
	}

	writer, finalize, err := openVerifyOutput(s.Output)
	if err != nil {
		return operationalError{err: err}
	}
	var runErr error
	defer func() { _ = finalize(runErr) }()

	ctx := kongContext()
	applier, err := target.OpenChangeApplier(ctx, s.Target)
	if err != nil {
		return operationalError{err: fmt.Errorf("open target applier: %w", err)}
	}
	defer func() {
		if c, ok := applier.(io.Closer); ok {
			_ = c.Close()
		}
	}()

	streams, err := applier.ListStreams(ctx)
	if err != nil {
		runErr = err
		return operationalError{err: fmt.Errorf("list streams: %w", err)}
	}

	result := evaluateHealth(streams, s.StreamID, s.MaxStaleSeconds, time.Now())
	if err := renderHealth(writer, result, s.Format); err != nil {
		runErr = err
		return operationalError{err: err}
	}

	if !result.Found {
		return operationalError{err: fmt.Errorf("stream %q not found on target", s.StreamID)}
	}
	if result.Stale {
		return staleStreamError{streamID: s.StreamID, secondsAgo: result.SecondsSinceLastApply, threshold: s.MaxStaleSeconds}
	}
	return nil
}

// evaluateHealth filters the stream list to the requested ID and
// computes the freshness comparison against the threshold. Pure
// function — testable without a live target.
func evaluateHealth(streams []ir.StreamStatus, streamID string, maxStaleSeconds int, now time.Time) HealthResult {
	r := HealthResult{StreamID: streamID, Threshold: maxStaleSeconds}
	for _, st := range streams {
		if st.StreamID != streamID {
			continue
		}
		r.Found = true
		r.Position = truncatePositionToken(st.Position.Token, 60)
		r.UpdatedAt = st.UpdatedAt.UTC().Format(time.RFC3339)
		r.SecondsSinceLastApply = int64(now.Sub(st.UpdatedAt).Seconds())
		if maxStaleSeconds > 0 && r.SecondsSinceLastApply > int64(maxStaleSeconds) {
			r.Stale = true
		}
		return r
	}
	return r
}

func renderHealth(w io.Writer, r HealthResult, format string) error {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	case "", "text":
		return renderHealthText(w, r)
	}
	return fmt.Errorf("unknown format %q (want 'text' or 'json')", format)
}

func renderHealthText(w io.Writer, r HealthResult) error {
	if !r.Found {
		_, err := fmt.Fprintf(w, "stream: %s\nfound:  false\n", r.StreamID)
		return err
	}
	state := "healthy"
	if r.Stale {
		state = fmt.Sprintf("STALE (last apply %ds ago, threshold %ds)", r.SecondsSinceLastApply, r.Threshold)
	}
	_, err := fmt.Fprintf(w,
		"stream: %s\nfound: true\nstate: %s\nposition: %s\nupdated_at: %s\nseconds_since_last_apply: %d\n",
		r.StreamID, state, r.Position, r.UpdatedAt, r.SecondsSinceLastApply)
	return err
}

// staleStreamError signals "threshold breached" with kong exit code 1.
// Mirrors driftError's shape for the diff command — short message on
// stderr; the structured detail is on stdout (or --output FILE).
type staleStreamError struct {
	streamID   string
	secondsAgo int64
	threshold  int
}

func (staleStreamError) ExitCode() int { return 1 }

func (e staleStreamError) Error() string {
	return fmt.Sprintf("stream %q stale: last apply %ds ago, threshold %ds",
		e.streamID, e.secondsAgo, e.threshold)
}
