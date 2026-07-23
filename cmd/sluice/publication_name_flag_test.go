// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0175 CLI-layer pins for --publication-name.
//
// Per the Bug 180 lesson ("pin a value-gated fix THROUGH the CLI
// layer"): a flag whose new branch is reachable only for a particular
// parsed value can be unreachable in practice while a direct-call unit
// test greens it. These parse through the real kong grammar.

package main

import "testing"

// TestSyncStartCmd_FilteredStreamDerivationInputs pins — through the
// real kong grammar — the exact parsed-value combination that makes
// the ADR-0176-prerequisite per-stream-default branch fire in
// pipeline.Streamer: a `--where` row filter present, NO
// --publication-name, and the stream id. The branch itself
// (resolveEffectivePublication) is unit-pinned in internal/pipeline
// and end-to-end-pinned by TestPublicationScope_* on real PG; this
// test guards the CLI layer, where a kong default / builder collapse
// could make the branch unreachable while direct-call tests stay
// green (the Bug 180 class).
func TestSyncStartCmd_FilteredStreamDerivationInputs(t *testing.T) {
	cli := parseInto(
		t, "sync", "start",
		"--source-driver", "postgres", "--source", "postgres://u:p@src/db",
		"--target-driver", "postgres", "--target", "postgres://u:p@dst/db",
		"--stream-id", "wave-a",
		"--where", "users=country IN ('US','CA')",
	)
	s := cli.Sync.Start
	// (b) no explicit --publication-name: MUST parse empty. A kong
	// default here would defeat the whole ratchet (every stream would
	// look explicitly named and never reuse its record).
	if s.PublicationName != "" {
		t.Errorf("--publication-name must default to empty for the ratchet to engage; got %q", s.PublicationName)
	}
	// (a) the --where filter must survive the parse AND the same
	// parser Run feeds to Streamer.RowFilters must yield a non-empty
	// map — RowFilters (not the raw slice) is what gates derivation.
	rowFilters, err := parseWhereFilters(s.Where)
	if err != nil {
		t.Fatalf("parseWhereFilters(%q): %v", s.Where, err)
	}
	if got := rowFilters["users"]; got != "country IN ('US','CA')" {
		t.Errorf("rowFilters[users] = %q; the --where predicate did not reach the filter map intact", got)
	}
	// (c) the stream id the per-stream default derives from.
	if s.StreamID != "wave-a" {
		t.Errorf("--stream-id did not reach the command: got %q", s.StreamID)
	}
}

func TestSyncStartCmd_PublicationNameFlagParses(t *testing.T) {
	cli := parseInto(
		t, "sync", "start",
		"--source-driver", "postgres", "--source", "postgres://u:p@src/db",
		"--target-driver", "postgres", "--target", "postgres://u:p@dst/db",
		"--stream-id", "wave-a",
		"--slot-name", "wave_a",
		"--publication-name", "wave_a",
	)
	if got := cli.Sync.Start.PublicationName; got != "wave_a" {
		t.Errorf("--publication-name did not reach the command: got %q, want %q", got, "wave_a")
	}
	// The sibling flag must still land — these are set together and a
	// regression that dropped one would be easy to miss.
	if got := cli.Sync.Start.SlotName; got != "wave_a" {
		t.Errorf("--slot-name did not reach the command: got %q", got)
	}
}

// TestSyncStartCmd_PublicationNameDefaultsEmpty pins the non-breaking
// default: omitted means empty, which the engine collapses to the
// historical `sluice_pub`. If kong ever grew a default here, every
// existing PG deployment would restart against a different publication
// name on upgrade.
func TestSyncStartCmd_PublicationNameDefaultsEmpty(t *testing.T) {
	cli := parseInto(
		t, "sync", "start",
		"--source-driver", "postgres", "--source", "postgres://u:p@src/db",
		"--target-driver", "postgres", "--target", "postgres://u:p@dst/db",
		"--stream-id", "wave-a",
	)
	if got := cli.Sync.Start.PublicationName; got != "" {
		t.Errorf("--publication-name default must be empty (engine default); got %q", got)
	}
}
