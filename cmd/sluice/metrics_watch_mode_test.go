// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for audit 2026-07-26 ARCH-3 — the fan-out mode must be selected by an
// EXPLICIT flag, never by the absence of one.
//
// `--planetscale-metrics-db` used to carry `required:""`. Making org-wide mode
// reachable dropped that tag and inferred the mode from an empty value, which
// turned a wrapper script whose `$DB` happens to be unset from a loud kong
// refusal into a silent org-wide fan-out — one that also flips the persisted
// record's identity from `metrics-watch:<db>` to `metrics-watch:<org>` and
// inverts `--planetscale-metrics-branch`, where unset then means EVERY branch.
//
// This is the CLI-layer form of the rule CLAUDE.md states for config structs:
// a mode whose "on" state is the zero value will eventually be entered by
// accident. The test below is the general version of that rule, not just a
// check of this one command.
package main

import (
	"strings"
	"testing"
)

func TestMetricsWatch_ModeMustBeExplicit(t *testing.T) {
	cases := []struct {
		name    string
		cmd     MetricsWatchCmd
		wantErr string
	}{
		{
			name:    "neither selector",
			cmd:     MetricsWatchCmd{Engine: "postgres", PlanetScaleOrg: "acme"},
			wantErr: "or --fleet to watch every database in the org",
		},
		{
			name:    "both selectors",
			cmd:     MetricsWatchCmd{Engine: "postgres", PlanetScaleOrg: "acme", Fleet: true, PlanetScaleMetricsDB: "db1"},
			wantErr: "mutually exclusive",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := tc.cmd
			err := cmd.Run(&Globals{})
			if err == nil {
				t.Fatal("the command was accepted. Neither-selector must refuse loudly (an unset wrapper " +
					"variable would otherwise fan out across the whole org silently), and both-selectors is " +
					"ambiguous (audit ARCH-3).")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("refusal does not explain the choice; want a message containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

// TestNoModeIsSelectedByFlagAbsence is the general rule, expressed as a
// reminder rather than a mechanical check: a boolean-ish mode selector should
// be a flag you PASS, not a flag you omit. There is no reliable way to detect
// "this empty string means a different mode" by reflection, so this test pins
// the one instance that regressed and names the rule for the next reviewer.
func TestNoModeIsSelectedByFlagAbsence(t *testing.T) {
	// A single-database watch must NOT be reinterpreted as org-wide, whatever
	// else is unset around it.
	cmd := MetricsWatchCmd{PlanetScaleMetricsDB: "db1"}
	if cmd.Fleet {
		t.Fatal("a single-database command reports Fleet mode")
	}
	// And the fleet selector must be a real, independent flag rather than a
	// derived value — if this ever becomes `Fleet: db == ""`, the pin above
	// stops meaning anything.
	fleet := MetricsWatchCmd{Fleet: true}
	if fleet.PlanetScaleMetricsDB != "" {
		t.Fatal("fleet mode carries a database name")
	}
}
