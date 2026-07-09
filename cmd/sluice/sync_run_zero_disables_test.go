// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/pipeline"
)

// buildFleetStreamer loads a one-sync fleet YAML with the given extra
// per-sync knob lines through the REAL config path (loadFleetConfig →
// validate → buildStreamerFromSpec) and returns the resolved Streamer.
// Testing through the YAML decoder is the Bug-180 lesson: a direct
// SyncSpec literal would green a branch the decoder never produces —
// here, whether an explicit `key: 0` decodes to a non-nil pointer at all.
func buildFleetStreamer(t *testing.T, knobs string) *pipeline.Streamer {
	t.Helper()
	path := writeFleetYAML(t, `
syncs:
  - stream-id: orders
    source-driver: postgres
    source: postgres://u:p@src:5432/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
    slot-name: orders
`+knobs)
	fleet, err := loadFleetConfig(path)
	if err != nil {
		t.Fatalf("loadFleetConfig: %v", err)
	}
	if err := fleet.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	streamer, err := buildStreamerFromSpec(context.Background(), &fleet.Syncs[0], testFleetGlobals())
	if err != nil {
		t.Fatalf("buildStreamerFromSpec: %v", err)
	}
	return streamer
}

// TestFleetZeroDisables_ThroughLoadPath_N11 pins the audit N-11 fix through
// the real fleet YAML load path: for each knob whose `sync start` flag
// documents a meaningful 0 (--apply-exec-timeout / --heartbeat-interval
// "0 disables"; --max-buffer-bytes 0 = no orchestrator cap, the engine
// default applies), the matrix is {key omitted → the flag default;
// explicit 0 → 0 on the Streamer; explicit non-zero → verbatim}. Pre-fix,
// firstNonZero* silently coerced an explicit 0 back to the 60s/60s/64MiB
// defaults — conflating "unset" with "disable" on the fleet layer only.
func TestFleetZeroDisables_ThroughLoadPath_N11(t *testing.T) {
	assertKnobs := func(t *testing.T, s *pipeline.Streamer, wantBuf int64, wantExec, wantHB time.Duration) {
		t.Helper()
		if s.MaxBufferBytes != wantBuf {
			t.Errorf("MaxBufferBytes = %d; want %d", s.MaxBufferBytes, wantBuf)
		}
		if s.ApplyExecTimeout != wantExec {
			t.Errorf("ApplyExecTimeout = %s; want %s", s.ApplyExecTimeout, wantExec)
		}
		if s.HeartbeatInterval != wantHB {
			t.Errorf("HeartbeatInterval = %s; want %s", s.HeartbeatInterval, wantHB)
		}
	}

	t.Run("omitted keys → sync start defaults", func(t *testing.T) {
		s := buildFleetStreamer(t, "")
		assertKnobs(t, s, defaultMaxBufferBytes, defaultApplyExecTimeout, defaultHeartbeatInterval)
	})

	t.Run("explicit 0 → disabled, passes through as 0", func(t *testing.T) {
		s := buildFleetStreamer(t,
			"    max-buffer-bytes: 0\n"+
				"    apply-exec-timeout: 0\n"+
				"    heartbeat-interval: 0\n")
		assertKnobs(t, s, 0, 0, 0)
	})

	t.Run("explicit 0s duration string → disabled", func(t *testing.T) {
		// The duration knobs also accept the string form ("0s") via the
		// mapstructure duration hook; max-buffer-bytes omitted keeps its
		// default — the two decode shapes are independent.
		s := buildFleetStreamer(t,
			"    apply-exec-timeout: 0s\n"+
				"    heartbeat-interval: 0s\n")
		assertKnobs(t, s, defaultMaxBufferBytes, 0, 0)
	})

	t.Run("explicit non-zero → verbatim", func(t *testing.T) {
		s := buildFleetStreamer(t,
			"    max-buffer-bytes: 1048576\n"+
				"    apply-exec-timeout: 5m\n"+
				"    heartbeat-interval: 30s\n")
		assertKnobs(t, s, 1<<20, 5*time.Minute, 30*time.Second)
	})
}

// TestSyncStartFleetParity_ExplicitZero_N11 pins the parity claim on the
// fleet-defaults const block ("a fleet sync behaves identically to the same
// flags on `sync start`") at its sharpest edge: `sync start` with
// --max-buffer-bytes=0 --apply-exec-timeout=0 --heartbeat-interval=0,
// parsed through the REAL kong parser, must yield the same Streamer values
// as a fleet spec with the same keys at 0 — and both must be 0 (the disable
// semantics), not a shared default. `sync start` passes these three parsed
// fields into its Streamer literal VERBATIM (cli.go — no coercion), so the
// kong-parsed field values ARE the `sync start` Streamer values.
func TestSyncStartFleetParity_ExplicitZero_N11(t *testing.T) {
	parse := func(t *testing.T, args ...string) *SyncStartCmd {
		t.Helper()
		cli := &CLI{}
		parser, err := kong.New(cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
		if err != nil {
			t.Fatalf("kong.New: %v", err)
		}
		if _, err := parser.Parse(args); err != nil {
			t.Fatalf("parse %v: %v", args, err)
		}
		return &cli.Sync.Start
	}

	base := []string{
		"sync", "start",
		"--source-driver=postgres", "--source=postgres://u:p@src:5432/app",
		"--target-driver=mysql", "--target=mysql://u:p@dst:3306/app",
	}

	t.Run("explicit 0 → identical (both disabled)", func(t *testing.T) {
		s := parse(t, append(append([]string{}, base...),
			"--max-buffer-bytes=0", "--apply-exec-timeout=0", "--heartbeat-interval=0")...)
		if s.MaxBufferBytes != 0 || s.ApplyExecTimeout != 0 || s.HeartbeatInterval != 0 {
			t.Fatalf("kong parsed explicit 0 as %d/%s/%s; want 0/0/0 (0 disables)",
				s.MaxBufferBytes, s.ApplyExecTimeout, s.HeartbeatInterval)
		}
		fs := buildFleetStreamer(t,
			"    max-buffer-bytes: 0\n"+
				"    apply-exec-timeout: 0\n"+
				"    heartbeat-interval: 0\n")
		if fs.MaxBufferBytes != s.MaxBufferBytes {
			t.Errorf("fleet MaxBufferBytes = %d; sync start = %d (parity broken)", fs.MaxBufferBytes, s.MaxBufferBytes)
		}
		if fs.ApplyExecTimeout != s.ApplyExecTimeout {
			t.Errorf("fleet ApplyExecTimeout = %s; sync start = %s (parity broken)", fs.ApplyExecTimeout, s.ApplyExecTimeout)
		}
		if fs.HeartbeatInterval != s.HeartbeatInterval {
			t.Errorf("fleet HeartbeatInterval = %s; sync start = %s (parity broken)", fs.HeartbeatInterval, s.HeartbeatInterval)
		}
	})

	t.Run("omitted → identical (both default)", func(t *testing.T) {
		s := parse(t, base...)
		fs := buildFleetStreamer(t, "")
		if fs.MaxBufferBytes != s.MaxBufferBytes {
			t.Errorf("fleet MaxBufferBytes = %d; sync start default = %d (parity broken)", fs.MaxBufferBytes, s.MaxBufferBytes)
		}
		if fs.ApplyExecTimeout != s.ApplyExecTimeout {
			t.Errorf("fleet ApplyExecTimeout = %s; sync start default = %s (parity broken)", fs.ApplyExecTimeout, s.ApplyExecTimeout)
		}
		if fs.HeartbeatInterval != s.HeartbeatInterval {
			t.Errorf("fleet HeartbeatInterval = %s; sync start default = %s (parity broken)", fs.HeartbeatInterval, s.HeartbeatInterval)
		}
	})
}

// TestFleetRetryZeroRefused_ThroughLoadPath_N11 pins the refusal side of
// N-11: an EXPLICIT 0 on any apply-retry-* key reaches validateRetryFlags
// verbatim through the real load path and is refused with the CLI's EXACT
// out-of-range message (computed here by calling validateRetryFlags the way
// `sync start` does), instead of being silently absorbed into the ADR-0038
// default — the pre-fix behaviour, the same silent-config-inversion class
// as the "0 disables" knobs. Omitted keys still take the defaults.
func TestFleetRetryZeroRefused_ThroughLoadPath_N11(t *testing.T) {
	loadAndValidate := func(t *testing.T, knobs string) error {
		t.Helper()
		fleet, err := loadFleetConfig(writeFleetYAML(t, `
syncs:
  - stream-id: orders
    source-driver: postgres
    source: postgres://u:p@src:5432/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
    slot-name: orders
`+knobs))
		if err != nil {
			t.Fatalf("loadFleetConfig: %v", err)
		}
		return fleet.validate()
	}

	cases := []struct {
		name    string
		knobs   string
		wantMsg string
	}{
		{
			name:    "apply-retry-attempts: 0",
			knobs:   "    apply-retry-attempts: 0\n",
			wantMsg: validateRetryFlags(0, defaultApplyRetryBackoffBase, defaultApplyRetryBackoffCap).Error(),
		},
		{
			name:    "apply-retry-backoff-base: 0",
			knobs:   "    apply-retry-backoff-base: 0\n",
			wantMsg: validateRetryFlags(defaultApplyRetryAttempts, 0, defaultApplyRetryBackoffCap).Error(),
		},
		{
			name:    "apply-retry-backoff-cap: 0",
			knobs:   "    apply-retry-backoff-cap: 0\n",
			wantMsg: validateRetryFlags(defaultApplyRetryAttempts, defaultApplyRetryBackoffBase, 0).Error(),
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := loadAndValidate(t, c.knobs)
			if err == nil {
				t.Fatal("explicit 0 validated clean; want the CLI out-of-range refusal")
			}
			if !strings.Contains(err.Error(), c.wantMsg) {
				t.Errorf("refusal %q missing the CLI-identical message %q", err.Error(), c.wantMsg)
			}
		})
	}

	t.Run("omitted → ADR-0038 defaults, validates clean", func(t *testing.T) {
		s := buildFleetStreamer(t, "") // runs validate() on the way
		if s.ApplyRetryAttempts != defaultApplyRetryAttempts {
			t.Errorf("ApplyRetryAttempts = %d; want default %d", s.ApplyRetryAttempts, defaultApplyRetryAttempts)
		}
		if s.ApplyRetryBackoffBase != defaultApplyRetryBackoffBase {
			t.Errorf("ApplyRetryBackoffBase = %s; want default %s", s.ApplyRetryBackoffBase, defaultApplyRetryBackoffBase)
		}
		if s.ApplyRetryBackoffCap != defaultApplyRetryBackoffCap {
			t.Errorf("ApplyRetryBackoffCap = %s; want default %s", s.ApplyRetryBackoffCap, defaultApplyRetryBackoffCap)
		}
	})

	t.Run("explicit in-range values → verbatim", func(t *testing.T) {
		s := buildFleetStreamer(t,
			"    apply-retry-attempts: 3\n"+
				"    apply-retry-backoff-base: 300ms\n"+
				"    apply-retry-backoff-cap: 5s\n")
		if s.ApplyRetryAttempts != 3 {
			t.Errorf("ApplyRetryAttempts = %d; want 3", s.ApplyRetryAttempts)
		}
		if s.ApplyRetryBackoffBase != 300*time.Millisecond {
			t.Errorf("ApplyRetryBackoffBase = %s; want 300ms", s.ApplyRetryBackoffBase)
		}
		if s.ApplyRetryBackoffCap != 5*time.Second {
			t.Errorf("ApplyRetryBackoffCap = %s; want 5s", s.ApplyRetryBackoffCap)
		}
	})
}

// TestSyncSpecFingerprint_ZeroVsOmitted_N11 pins that "explicit 0" and
// "key omitted" are DIFFERENT specs under the hot-reload fingerprint (nil
// vs a 0 pointer): a SIGHUP reload that flips one to the other must be
// seen as a CHANGED sync and restart with the new semantics, not be
// mistaken for unchanged. Loaded through the real YAML path both times.
func TestSyncSpecFingerprint_ZeroVsOmitted_N11(t *testing.T) {
	load := func(t *testing.T, knobs string) *SyncSpec {
		t.Helper()
		fleet, err := loadFleetConfig(writeFleetYAML(t, `
syncs:
  - stream-id: orders
    source-driver: postgres
    source: postgres://u:p@src:5432/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
    slot-name: orders
`+knobs))
		if err != nil {
			t.Fatalf("loadFleetConfig: %v", err)
		}
		return &fleet.Syncs[0]
	}

	omitted := load(t, "")
	zero := load(t, "    apply-exec-timeout: 0\n")
	if omitted.fingerprint() == zero.fingerprint() {
		t.Error("omitted and explicit-0 apply-exec-timeout hashed identically; a reload flipping between them would not restart the sync")
	}

	// Sanity: two identical loads still hash identically.
	zero2 := load(t, "    apply-exec-timeout: 0\n")
	if zero.fingerprint() != zero2.fingerprint() {
		t.Error("equal explicit-0 specs hashed differently")
	}
}
