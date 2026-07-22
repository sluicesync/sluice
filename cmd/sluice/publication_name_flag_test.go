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
