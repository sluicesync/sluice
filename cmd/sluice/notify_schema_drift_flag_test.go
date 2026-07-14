// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

// TestNotifySchemaDrift_DefaultOnThroughKong pins the ADR-0157 zero-value-safe
// gate THROUGH the real kong parser (the Bug 180 lesson: a default that greens
// only on a direct field-set can be unreachable via the CLI). With no
// --notify-schema-drift flag, kong's default:"true" must resolve
// NotifySchemaDrift=true → SuppressSchemaDriftNotify=false (ENABLED); an
// explicit =false must flip it to suppressed.
func TestNotifySchemaDrift_DefaultOnThroughKong(t *testing.T) {
	base := []string{
		"sync", "start",
		"--source-driver=mysql", "--source=src",
		"--target-driver=postgres", "--target=tgt",
		"--stream-id=s1",
	}

	t.Run("omitted → enabled (suppress=false)", func(t *testing.T) {
		cli := parseInto(t, base...)
		if !cli.Sync.Start.NotifySchemaDrift {
			t.Fatal("omitted --notify-schema-drift did not default true through kong (Bug 180 trap)")
		}
		if got := cli.Sync.Start.suppressSchemaDriftNotify(); got {
			t.Fatalf("suppressSchemaDriftNotify() = %v; want false (default ENABLED)", got)
		}
	})

	t.Run("explicit false → suppressed", func(t *testing.T) {
		cli := parseInto(t, append(append([]string{}, base...), "--notify-schema-drift=false")...)
		if cli.Sync.Start.NotifySchemaDrift {
			t.Fatal("--notify-schema-drift=false did not clear the field")
		}
		if got := cli.Sync.Start.suppressSchemaDriftNotify(); !got {
			t.Fatalf("suppressSchemaDriftNotify() = %v; want true (disabled)", got)
		}
	})

	t.Run("explicit true → enabled", func(t *testing.T) {
		cli := parseInto(t, append(append([]string{}, base...), "--notify-schema-drift=true")...)
		if got := cli.Sync.Start.suppressSchemaDriftNotify(); got {
			t.Fatalf("suppressSchemaDriftNotify() = %v; want false (enabled)", got)
		}
	})
}

// TestSchemaDriftSuppressFromSpec pins the fleet-path zero-value-safe mapping:
// an omitted key (nil *bool) keeps the alert ENABLED — the v0.99.51 trap would
// otherwise silently disable it for every YAML that doesn't set the field.
func TestSchemaDriftSuppressFromSpec(t *testing.T) {
	tru, fls := true, false
	cases := []struct {
		name string
		in   *bool
		want bool // want SuppressSchemaDriftNotify
	}{
		{"omitted (nil) → enabled", nil, false},
		{"explicit true → enabled", &tru, false},
		{"explicit false → suppressed", &fls, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := schemaDriftSuppressFromSpec(tc.in); got != tc.want {
				t.Errorf("schemaDriftSuppressFromSpec(%v) = %v; want %v", tc.in, got, tc.want)
			}
		})
	}
}
