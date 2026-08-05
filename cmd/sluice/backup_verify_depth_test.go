// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

// `backup verify --depth`, pinned THROUGH the real kong parser.
//
// A depth whose new branch fires only for a CLI value is exactly the shape
// that greens in a direct-call unit test and is unreachable in the binary: a
// kong default, an enum, or a builder collapse can make the flag never reach
// [backup.VerifyOptions]. So this parses real argv and asserts the parsed
// string maps onto the intended [backup.VerifyDepth] — including the DEFAULT,
// which is the value nobody types and everybody gets.

import (
	"strings"
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/pipeline/backup"
)

func parseVerifyArgs(t *testing.T, args []string) (*CLI, error) {
	t.Helper()
	cli := &CLI{}
	parser, err := kong.New(cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	_, perr := parser.Parse(args)
	return cli, perr
}

func TestBackupVerifyDepthFlag(t *testing.T) {
	t.Run("default is the hash-only depth", func(t *testing.T) {
		cli, err := parseVerifyArgs(t, []string{"backup", "verify", "--from-dir", "/tmp/b"})
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		depth, err := backup.ParseVerifyDepth(cli.Backup.Verify.Depth)
		if err != nil {
			t.Fatalf("ParseVerifyDepth(%q): %v", cli.Backup.Verify.Depth, err)
		}
		if depth != backup.VerifyDepthHash {
			t.Fatalf("default depth = %q; want %q — verify must stay cheap by default, so a cron probe "+
				"against object storage does not silently start paying a full read per chunk",
				depth, backup.VerifyDepthHash)
		}
	})

	t.Run("--depth read reaches the pipeline as the read depth", func(t *testing.T) {
		cli, err := parseVerifyArgs(t, []string{"backup", "verify", "--from-dir", "/tmp/b", "--depth", "read"})
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		depth, err := backup.ParseVerifyDepth(cli.Backup.Verify.Depth)
		if err != nil {
			t.Fatalf("ParseVerifyDepth: %v", err)
		}
		if depth != backup.VerifyDepthRead {
			t.Fatalf("depth = %q; want %q — the flag parses but never reaches the scan", depth, backup.VerifyDepthRead)
		}
	})

	t.Run("an unknown depth is refused at parse time", func(t *testing.T) {
		// `sluice verify` (the data-comparison command) uses count/sample;
		// borrowing one of its values here must not silently degrade to the
		// default.
		for _, bad := range []string{"count", "sample", "full", "rows"} {
			if _, err := parseVerifyArgs(t, []string{"backup", "verify", "--from-dir", "/tmp/b", "--depth", bad}); err == nil {
				t.Errorf("--depth %s was accepted", bad)
			} else if !strings.Contains(err.Error(), "depth") {
				t.Errorf("--depth %s refused with a message that does not name the flag: %v", bad, err)
			}
		}
	})
}
