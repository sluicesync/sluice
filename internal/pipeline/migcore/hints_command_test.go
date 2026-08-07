// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"errors"
	"strings"
	"testing"
)

// errCopyFailure is the error shape that reaches the bulk-copy catch-all —
// the entry Bug 230 was found on.
var errCopyFailure = errors.New(`pipeline: copy table "bad": postgres: time out of range`)

// TestHintTextIsPerCommand is the unit-level pin on Bug 230's fix: the SAME
// failure, on the SAME shared phase, must hand each command a remedy that
// command can actually run. The flag-validity half is cmd/sluice's
// TestHintRegistryTextsNameOnlyFlagsTheirCommandAccepts — kong's model lives
// there — so this pins the DISPATCH: that the text changes at all, and that
// the neutral fallback is the flag-free one.
func TestHintTextIsPerCommand(t *testing.T) {
	defer SetRunningCommand("")

	cases := []struct {
		cmd     Command
		want    string
		wantNot []string
	}{
		{
			cmd:  CommandMigrate,
			want: "use --resume to continue",
		},
		{
			cmd:     CommandSyncStart,
			want:    "there is no resume flag",
			wantNot: []string{"use --resume"},
		},
		{
			cmd:     CommandSyncRun,
			want:    "`exclude-table` list in the fleet config",
			wantNot: []string{"--resume", "--exclude-table"},
		},
		{
			cmd:     CommandAddTable,
			want:    "copies exactly the ONE table you named",
			wantNot: []string{"--resume", "--exclude-table", "any earlier tables"},
		},
		{
			// The zero value. Every command with no override lands here, so
			// it may name no command-specific flag at all.
			cmd:     "",
			want:    "take the table out of this run's table scope",
			wantNot: []string{"--resume", "--exclude-table"},
		},
		{
			// An unknown command path — a future command, or a typo in a
			// perCommand key — must fall back to the neutral text rather
			// than to whatever the map happens to iterate first.
			cmd:     "backfill",
			want:    "take the table out of this run's table scope",
			wantNot: []string{"--resume", "--exclude-table"},
		},
	}

	for _, c := range cases {
		t.Run(string(c.cmd), func(t *testing.T) {
			SetRunningCommand(string(c.cmd))
			got := hintFor(PhaseBulkCopy, errCopyFailure)
			if got == "" {
				t.Fatal("no hint at all for the bulk-copy catch-all")
			}
			if !strings.Contains(got, c.want) {
				t.Errorf("hint for %q lacks %q:\n%s", c.cmd, c.want, got)
			}
			for _, no := range c.wantNot {
				if strings.Contains(got, no) {
					t.Errorf("hint for %q still names %q — `sluice %s` has no such flag (Bug 230):\n%s",
						c.cmd, no, c.cmd, got)
				}
			}
		})
	}
}

// TestHintTextsCoversEveryRegistryEntry pins the surface the cmd/sluice gate
// grades against the registry itself: a hint the exporter drops is a hint
// nothing checks, which is the exact shape ("the gate's coverage is narrower
// than its name") this whole change exists to close.
func TestHintTextsCoversEveryRegistryEntry(t *testing.T) {
	texts := HintTexts()

	neutral := map[string]bool{}
	perCommand := 0
	for _, ht := range texts {
		if ht.Text == "" {
			t.Errorf("HintTexts returned an EMPTY text for %s/%q", ht.Code, ht.Command)
		}
		if ht.Command == "" {
			neutral[ht.Text] = true
			continue
		}
		perCommand++
	}

	for i, h := range hintRegistry {
		if !neutral[h.hint] {
			t.Errorf("hintRegistry[%d] (contains=%q) has a neutral text HintTexts did not return; the gate "+
				"in cmd/sluice never sees it", i, h.contains)
		}
		if len(h.perCommand) == 0 {
			continue
		}
		for cmd, text := range h.perCommand {
			if text == "" {
				t.Errorf("hintRegistry[%d] has an EMPTY override for %q — that command silently gets the "+
					"neutral text via a key that reads like a decision", i, cmd)
			}
		}
	}

	// Anti-vacuity: the dynamic AAAA-only classifier is outside the registry
	// and is the one text an exporter walking only hintRegistry would miss.
	var sawDNS bool
	for _, ht := range texts {
		if strings.Contains(ht.Text, "is IPv6-only") {
			sawDNS = true
		}
	}
	if !sawDNS {
		t.Error("HintTexts does not include the dynamic AAAA-only hint; it is the one operator-facing hint " +
			"built outside hintRegistry, so a registry-only walk leaves it ungraded")
	}
	if perCommand < 4 {
		t.Errorf("only %d per-command overrides exported (floor 4) — Bug 230's fix has been unwound", perCommand)
	}
}
