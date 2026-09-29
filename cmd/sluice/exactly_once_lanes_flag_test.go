// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"

	"github.com/alecthomas/kong"
)

// TestExactlyOnceLanes_OffByDefault_OnWhenAsked pins `--exactly-once-lanes`
// (ADR-0190 amendment C) through the real parsers, both entry points: the
// kong flag on `sync start` and the fleet `exactly-once-lanes` key through
// loadFleetConfig and buildStreamerFromSpec. Omitted must mean OFF — the
// lane path's marks are opt-in because their fence measured ~99.9% slower —
// and present must reach the Streamer.
func TestExactlyOnceLanes_OffByDefault_OnWhenAsked(t *testing.T) {
	parse := func(extra ...string) bool {
		t.Helper()
		cli := &CLI{}
		parser, err := kong.New(cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
		if err != nil {
			t.Fatalf("kong.New: %v", err)
		}
		args := append([]string{
			"sync", "start",
			"--source-driver=mysql", "--source=u:p@/db",
			"--target-driver=postgres", "--target=postgres://u:p@h/db",
		}, extra...)
		if _, err := parser.Parse(args); err != nil {
			t.Fatalf("Parse %v: %v", args, err)
		}
		return cli.Sync.Start.ExactlyOnceLanes
	}
	if parse() {
		t.Error("sync start: --exactly-once-lanes is ON with the flag omitted; the default must be off")
	}
	if !parse("--exactly-once-lanes") {
		t.Error("sync start: --exactly-once-lanes did not set the flag")
	}

	path := writeFleetYAML(t, `
syncs:
  - stream-id: plain
    slot-name: plain
    source-driver: postgres
    source: postgres://u:p@src:5432/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
  - stream-id: exact
    slot-name: exact
    source-driver: postgres
    source: postgres://u:p@src:5432/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
    exactly-once-lanes: true
`)
	fleet, err := loadFleetConfig(path)
	if err != nil {
		t.Fatalf("loadFleetConfig: %v", err)
	}
	want := map[string]bool{"plain": false, "exact": true}
	for i := range fleet.Syncs {
		spec := &fleet.Syncs[i]
		s, err := buildStreamerFromSpec(context.Background(), spec, testFleetGlobals())
		if err != nil {
			t.Fatalf("buildStreamerFromSpec(%s): %v", spec.StreamID, err)
		}
		if s.ExactlyOnceLanes != want[spec.StreamID] {
			t.Errorf("fleet sync %q: Streamer.ExactlyOnceLanes = %v; want %v", spec.StreamID, s.ExactlyOnceLanes, want[spec.StreamID])
		}
	}
}
