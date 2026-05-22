// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// statusRenderOpts is the rendering knobs SyncStatusCmd surfaces.
// Held as a struct (not inlined as Run-method parameters) so the
// --watch loop can re-use the same options across iterations without
// re-parsing.
type statusRenderOpts struct {
	// Format is the output shape: "text" (default, tabwriter-aligned
	// human-readable) or "json" (a JSON array of objects, one per
	// stream, ready for piping to jq or similar). Validated by kong's
	// enum: tag on the SyncStatusCmd field; unknown values reach here
	// as their last validated value.
	Format string

	// Summary toggles the aggregate header (stream count, oldest /
	// most-recent ages). Off by default; useful for fleet-wide skim.
	Summary bool

	// StreamID, when non-empty, filters the rendered set to that one
	// stream. Empty string = render everything (the default).
	StreamID string
}

// runStatusOnce is the one-shot path: query the target, render once,
// return. The default --watch=0 path; also reused by --watch's tick
// loop.
func runStatusOnce(ctx context.Context, applier ir.ChangeApplier, out io.Writer, opts statusRenderOpts) error {
	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return fmt.Errorf("list streams: %w", err)
	}
	streams = filterStreams(streams, opts.StreamID)
	// ADR-0054 §6: query the per-target lease control table when the
	// engine implements [ir.ShardConsolidationLeaseLister]. Engines
	// without the surface return nil leases (the status output omits
	// the consolidation_lease block, preserving pre-v0.73 shape).
	var leases []ir.ShardConsolidationLeaseRow
	if lister, ok := applier.(ir.ShardConsolidationLeaseLister); ok {
		leases, err = lister.ListLeases(ctx)
		if err != nil {
			return fmt.Errorf("list shard consolidation leases: %w", err)
		}
	}
	return renderStatus(out, streams, leases, opts, time.Now())
}

// runStatusWatch is the live-refresh path: re-query and re-render at
// every `interval` tick until the ctx is cancelled (Ctrl-C / SIGTERM).
// Each render clears the terminal first so output stays in place
// rather than scrolling forever.
//
// The clear sequence is the ANSI "clear screen + home cursor" pair
// (ESC[2J ESC[H), which works on every common terminal — including
// Windows Terminal, modern conhost, every Linux/macOS terminal — and
// is a documented no-op when stdout isn't a TTY (the bytes just print
// as escape sequences with no visible effect; not ideal but the
// fallback is "stop rendering on non-TTY" which is more confusing).
//
// On ctx cancel the loop returns cleanly (no error); kong's signal
// handling already maps Ctrl-C to ctx cancellation so the operator
// sees the partial output and a clean exit.
func runStatusWatch(ctx context.Context, applier ir.ChangeApplier, out io.Writer, opts statusRenderOpts, interval time.Duration) error {
	// Initial render before sleeping. Operators expect immediate
	// output on `sluice sync status --watch 2s`; making them wait
	// the first interval would be confusing.
	if err := clearAndRender(ctx, applier, out, opts); err != nil {
		// First-iteration errors are likely a misconfigured target;
		// surface immediately rather than silently looping on the
		// same failure. Subsequent-iteration errors (network blip
		// etc.) are tolerated below.
		return err
	}

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := clearAndRender(ctx, applier, out, opts); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				// Transient: log inline and keep watching. An operator
				// running --watch wants persistent visibility through a
				// brief target outage, not an abort.
				fmt.Fprintf(out, "watch: refresh failed: %v\n", err)
			}
		}
	}
}

// clearAndRender clears the terminal and runs one render pass.
// Factored so the initial render and each ticker iteration share
// identical behaviour.
func clearAndRender(ctx context.Context, applier ir.ChangeApplier, out io.Writer, opts statusRenderOpts) error {
	// ESC[2J = clear entire screen; ESC[H = move cursor to (1,1).
	// Together they re-render the screen in place each tick.
	if _, err := fmt.Fprint(out, "\x1b[2J\x1b[H"); err != nil {
		return fmt.Errorf("write clear-screen: %w", err)
	}
	return runStatusOnce(ctx, applier, out, opts)
}

// filterStreams returns either the input as-is (when streamID is
// empty) or the subset whose StreamID matches exactly. Order is
// preserved.
func filterStreams(streams []ir.StreamStatus, streamID string) []ir.StreamStatus {
	if streamID == "" {
		return streams
	}
	out := streams[:0]
	for _, st := range streams {
		if st.StreamID == streamID {
			out = append(out, st)
		}
	}
	return out
}

// renderStatus dispatches by format. Single entry point so the watch
// loop and the one-shot path share identical rendering logic; tests
// drive it directly.
func renderStatus(out io.Writer, streams []ir.StreamStatus, leases []ir.ShardConsolidationLeaseRow, opts statusRenderOpts, now time.Time) error {
	// Sort for stable output across runs. Most-recently-updated first
	// matches the operator's interest ("what's been moving?").
	sort.Slice(streams, func(i, j int) bool {
		return streams[i].UpdatedAt.After(streams[j].UpdatedAt)
	})

	switch opts.Format {
	case "", "text":
		return renderStatusText(out, streams, leases, opts, now)
	case "json":
		return renderStatusJSON(out, streams, leases, opts, now)
	default:
		return fmt.Errorf("unknown --format %q (want text or json)", opts.Format)
	}
}

// renderStatusText is the human-readable tabwriter path. Keeps the
// pre-refactor output shape exactly when --summary is off, so
// existing scripts that parsed the text output still work.
func renderStatusText(out io.Writer, streams []ir.StreamStatus, leases []ir.ShardConsolidationLeaseRow, opts statusRenderOpts, now time.Time) error {
	if len(streams) == 0 {
		return writeEmptyText(out, opts.StreamID)
	}

	if opts.Summary {
		writeSummaryText(out, streams, now)
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	defer func() { _ = tw.Flush() }()
	if _, err := fmt.Fprintln(tw, "STREAM\tUPDATED\tAGE\tPOSITION"); err != nil {
		return err
	}
	for _, st := range streams {
		if _, err := fmt.Fprintf(
			tw, "%s\t%s\t%s\t%s\n",
			st.StreamID,
			st.UpdatedAt.UTC().Format(time.RFC3339),
			humanAgo(now.Sub(st.UpdatedAt)),
			truncatePositionToken(st.Position.Token, 60),
		); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	// ADR-0054 §6: append the consolidation-lease one-line summary
	// when any leases exist. Shape: "Shape A: N tables, M applied, K
	// held, J expired" — operator-skimmable at a glance.
	if len(leases) > 0 {
		held, applied, expired := classifyLeasesForSummary(leases, now)
		if _, err := fmt.Fprintf(
			out,
			"\nShape A consolidation: %d %s, %d applied, %d held, %d expired\n",
			len(leases), pluralize("table", len(leases)), applied, held, expired,
		); err != nil {
			return err
		}
	}
	return nil
}

// classifyLeaseForJSON returns the textual state label for a lease
// row's JSON entry. Mirrors classifyLeasesForSummary's logic to keep
// text + JSON consistent.
func classifyLeaseForJSON(row ir.ShardConsolidationLeaseRow, now time.Time) string {
	switch {
	case row.HasAppliedAt:
		return "applied"
	case row.HasLeaseExpiresAt && row.LeaseExpiresAt.After(now):
		return "held"
	default:
		return "expired"
	}
}

// classifyLeasesForSummary buckets each lease row into held/applied/
// expired counts for the text-summary one-liner. Mirrors the
// pipeline's classifyLeaseRow ordering exactly (Applied takes
// precedence over Held/Expired; absent rows don't appear in the
// summary because they wouldn't be in the slice).
func classifyLeasesForSummary(leases []ir.ShardConsolidationLeaseRow, now time.Time) (held, applied, expired int) {
	for _, row := range leases {
		switch {
		case row.HasAppliedAt:
			applied++
		case row.HasLeaseExpiresAt && row.LeaseExpiresAt.After(now):
			held++
		default:
			expired++
		}
	}
	return held, applied, expired
}

// writeEmptyText handles the no-streams case for the text format.
// Mirrors the pre-refactor messages exactly.
func writeEmptyText(out io.Writer, streamID string) error {
	if streamID != "" {
		_, err := fmt.Fprintf(out, "no stream %q on target\n", streamID)
		return err
	}
	_, err := fmt.Fprintln(out, "no streams recorded on target")
	return err
}

// writeSummaryText prepends the aggregate header for --summary mode.
// Shape: "SUMMARY: N streams, oldest=Xm ago, most-recent=Ys ago".
// Computed against the same `now` the row table uses for age (so the
// summary's ages and the rows' ages line up consistent within a render).
func writeSummaryText(out io.Writer, streams []ir.StreamStatus, now time.Time) {
	oldest, newest := agesSpan(streams, now)
	fmt.Fprintf(
		out,
		"SUMMARY: %d %s, oldest=%s, most-recent=%s\n\n",
		len(streams), pluralize("stream", len(streams)),
		humanAgo(oldest), humanAgo(newest),
	)
}

// pluralize returns word for n==1, word+"s" otherwise. Tiny helper
// to keep summary copy idiomatic ("1 stream" vs "5 streams").
func pluralize(word string, n int) string {
	if n == 1 {
		return word
	}
	return word + "s"
}

// agesSpan returns the (oldest, most-recent) update ages over the
// given streams against `now`. Returns (0,0) for an empty slice so
// the caller can't trip on uninitialised values; the empty-slice
// path doesn't reach this function in practice because renderStatusText
// short-circuits on len==0 first.
func agesSpan(streams []ir.StreamStatus, now time.Time) (oldest, newest time.Duration) {
	if len(streams) == 0 {
		return 0, 0
	}
	oldest = now.Sub(streams[0].UpdatedAt)
	newest = oldest
	for _, st := range streams[1:] {
		age := now.Sub(st.UpdatedAt)
		if age > oldest {
			oldest = age
		}
		if age < newest {
			newest = age
		}
	}
	return oldest, newest
}

// renderStatusJSON marshals streams as a JSON document keyed for
// scripted consumption. Top-level shape:
//
//	{
//	  "generated_at": "2026-05-20T22:30:00Z",
//	  "summary": {"count":3, "oldest_seconds":425, "newest_seconds":4},
//	  "streams": [{...}, ...]
//	}
//
// The summary block is included regardless of --summary (the cost is
// trivial and a JSON consumer can ignore fields it doesn't want;
// "scriptable output should always include aggregates" is the more
// useful default). Field names match Go's json:"" tag conventions
// (snake_case for JSON) so jq pipes are predictable.
func renderStatusJSON(out io.Writer, streams []ir.StreamStatus, leases []ir.ShardConsolidationLeaseRow, _ statusRenderOpts, now time.Time) error {
	type jsonPosition struct {
		Engine string `json:"engine"`
		Token  string `json:"token"`
	}
	type jsonStream struct {
		StreamID             string       `json:"stream_id"`
		Position             jsonPosition `json:"position"`
		UpdatedAt            time.Time    `json:"updated_at"`
		AgeSeconds           int64        `json:"age_seconds"`
		SlotName             string       `json:"slot_name,omitempty"`
		SourceDSNFingerprint string       `json:"source_dsn_fingerprint,omitempty"`
		TargetSchema         string       `json:"target_schema,omitempty"`
	}
	type jsonSummary struct {
		Count         int   `json:"count"`
		OldestSeconds int64 `json:"oldest_seconds"`
		NewestSeconds int64 `json:"newest_seconds"`
	}
	// ADR-0054 §6 — per-lease block. Field names mirror the storage
	// columns for jq predictability; the `state` field is the
	// derived classification (held/applied/expired) for operator
	// skim.
	type jsonLease struct {
		TargetTable          string `json:"target_table"`
		State                string `json:"state"`
		HolderStreamID       string `json:"holder_stream_id,omitempty"`
		ExpiresAt            string `json:"expires_at,omitempty"`
		DDLChecksum          string `json:"ddl_checksum,omitempty"`
		AppliedSchemaVersion int64  `json:"applied_schema_version"`
		AppliedAt            string `json:"applied_at,omitempty"`
	}
	type jsonDoc struct {
		GeneratedAt time.Time    `json:"generated_at"`
		Summary     jsonSummary  `json:"summary"`
		Streams     []jsonStream `json:"streams"`
		Leases      []jsonLease  `json:"consolidation_leases,omitempty"`
	}

	out2 := make([]jsonStream, 0, len(streams))
	for _, st := range streams {
		out2 = append(out2, jsonStream{
			StreamID: st.StreamID,
			Position: jsonPosition{
				Engine: st.Position.Engine,
				Token:  st.Position.Token,
			},
			UpdatedAt:            st.UpdatedAt.UTC(),
			AgeSeconds:           int64(now.Sub(st.UpdatedAt).Seconds()),
			SlotName:             st.SlotName,
			SourceDSNFingerprint: st.SourceDSNFingerprint,
			TargetSchema:         st.TargetSchema,
		})
	}
	oldest, newest := agesSpan(streams, now)
	leasesJSON := make([]jsonLease, 0, len(leases))
	for _, l := range leases {
		state := classifyLeaseForJSON(l, now)
		entry := jsonLease{
			TargetTable:          l.TargetTableFullName,
			State:                state,
			HolderStreamID:       l.LeaseHolderStreamID,
			DDLChecksum:          l.DDLChecksum,
			AppliedSchemaVersion: l.AppliedSchemaVersion,
		}
		if l.HasLeaseExpiresAt {
			entry.ExpiresAt = l.LeaseExpiresAt.UTC().Format(time.RFC3339)
		}
		if l.HasAppliedAt {
			entry.AppliedAt = l.AppliedAt.UTC().Format(time.RFC3339)
		}
		leasesJSON = append(leasesJSON, entry)
	}
	doc := jsonDoc{
		GeneratedAt: now.UTC(),
		Summary: jsonSummary{
			Count:         len(streams),
			OldestSeconds: int64(oldest.Seconds()),
			NewestSeconds: int64(newest.Seconds()),
		},
		Streams: out2,
		Leases:  leasesJSON,
	}

	// Indent for human-skimmable scripted output. jq pipes don't care
	// about whitespace; humans skim better with newlines.
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(doc)
}

// (intentionally unused parameter `os.Stdout` interception: the
// runStatus* functions take an io.Writer so tests can capture
// rendered output. The os import is kept for the kongContext default
// in cli.go's caller path.)
var _ = os.Stdout
