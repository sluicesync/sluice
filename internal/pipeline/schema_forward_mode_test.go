// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "testing"

// TestForwardSchemaEnabled_ModeMapping pins the ADR-0091 flag model:
// schema-change forwarding is ON by default (empty or "forward"), OFF
// only on an explicit "refuse". The deprecated --forward-schema-add-column
// bool does NOT gate forwarding (forwarding is already on by default);
// it only triggers a deprecation warning at engage time, so it is
// intentionally absent from this predicate.
func TestForwardSchemaEnabled_ModeMapping(t *testing.T) {
	cases := []struct {
		name          string
		schemaChanges string
		want          bool
	}{
		{"empty defaults to forward", "", true},
		{"explicit forward", "forward", true},
		{"explicit refuse", "refuse", false},
		{"case-insensitive refuse", "REFUSE", false},
		{"unknown value treated as forward", "weird", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Streamer{SchemaChanges: tc.schemaChanges}
			if got := s.forwardSchemaEnabled(); got != tc.want {
				t.Errorf("forwardSchemaEnabled(%q) = %v; want %v", tc.schemaChanges, got, tc.want)
			}
		})
	}
}

// TestForwardSchemaEnabled_DeprecatedFlagDoesNotGate verifies the
// deprecated bool is orthogonal to the predicate: setting it does not
// turn forwarding on under refuse mode, nor off under forward mode.
func TestForwardSchemaEnabled_DeprecatedFlagDoesNotGate(t *testing.T) {
	if (&Streamer{SchemaChanges: "refuse", ForwardSchemaAddColumn: true}).forwardSchemaEnabled() {
		t.Errorf("refuse mode must stay disabled even with the deprecated flag set")
	}
	if !(&Streamer{SchemaChanges: "forward", ForwardSchemaAddColumn: false}).forwardSchemaEnabled() {
		t.Errorf("forward mode must stay enabled without the deprecated flag")
	}
}
