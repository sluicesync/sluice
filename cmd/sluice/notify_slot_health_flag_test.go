// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

// TestNotifySlotHealth_DefaultOnThroughKong pins the roadmap-64a
// zero-value-safe gate THROUGH the real kong parser (the Bug 180 lesson: a
// default that greens only on a direct field-set can be unreachable via
// the CLI). With no --notify-slot-health flag, kong's default:"true" must
// resolve NotifySlotHealth=true → SuppressSlotHealthNotify=false
// (ENABLED); an explicit =false must flip it to suppressed. Mirrors
// [TestNotifySchemaDrift_DefaultOnThroughKong] (ADR-0157).
func TestNotifySlotHealth_DefaultOnThroughKong(t *testing.T) {
	base := []string{
		"sync", "start",
		"--source-driver=postgres", "--source=src",
		"--target-driver=mysql", "--target=tgt",
		"--stream-id=s1",
	}

	t.Run("omitted → enabled (suppress=false)", func(t *testing.T) {
		cli := parseInto(t, base...)
		if !cli.Sync.Start.NotifySlotHealth {
			t.Fatal("omitted --notify-slot-health did not default true through kong (Bug 180 trap)")
		}
		if got := cli.Sync.Start.suppressSlotHealthNotify(); got {
			t.Fatalf("suppressSlotHealthNotify() = %v; want false (default ENABLED)", got)
		}
	})

	t.Run("explicit false → suppressed", func(t *testing.T) {
		cli := parseInto(t, append(append([]string{}, base...), "--notify-slot-health=false")...)
		if cli.Sync.Start.NotifySlotHealth {
			t.Fatal("--notify-slot-health=false did not clear the field")
		}
		if got := cli.Sync.Start.suppressSlotHealthNotify(); !got {
			t.Fatalf("suppressSlotHealthNotify() = %v; want true (disabled)", got)
		}
	})

	t.Run("explicit true → enabled", func(t *testing.T) {
		cli := parseInto(t, append(append([]string{}, base...), "--notify-slot-health=true")...)
		if got := cli.Sync.Start.suppressSlotHealthNotify(); got {
			t.Fatalf("suppressSlotHealthNotify() = %v; want false (enabled)", got)
		}
	})
}

// TestSlotHealthSuppressFromSpec pins the fleet-path zero-value-safe
// mapping: an omitted key (nil *bool) keeps the alert ENABLED — the
// v0.99.51 trap would silently disable it for every YAML spec that
// doesn't mention the key. Mirrors [TestSchemaDriftSuppressFromSpec].
func TestSlotHealthSuppressFromSpec(t *testing.T) {
	tru, fls := true, false
	cases := []struct {
		name string
		in   *bool
		want bool // want SuppressSlotHealthNotify
	}{
		{"omitted (nil) → enabled", nil, false},
		{"explicit true → enabled", &tru, false},
		{"explicit false → suppressed", &fls, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := slotHealthSuppressFromSpec(tc.in); got != tc.want {
				t.Errorf("slotHealthSuppressFromSpec(%v) = %v; want %v", tc.in, got, tc.want)
			}
		})
	}
}
