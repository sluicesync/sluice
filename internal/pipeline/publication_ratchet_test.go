// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0176 prerequisite ratchet: the effective-
// publication resolution matrix and the per-stream default's
// identifier-safety normalization. The end-to-end behaviour (a real
// PG source, the record round-trip, cleanup parity) is pinned by the
// TestPublicationScope_* integration gates.

package pipeline

import (
	"strings"
	"testing"
)

// TestResolveEffectivePublication covers the whole ratchet decision
// matrix. The zero-value rows are the widest-blast-radius pins: every
// legacy stream, every unfiltered stream, and every programmatic
// construction must resolve to "" (the shared engine default) —
// byte-identical to pre-chunk behaviour.
func TestResolveEffectivePublication(t *testing.T) {
	tests := []struct {
		name          string
		explicit      string
		recorded      string
		rowExists     bool
		filtered      bool
		wantEffective string
		wantOverrode  bool
	}{
		{
			name:          "all zero — legacy shared default (the ratchet's floor)",
			wantEffective: "",
		},
		{
			name:          "existing unfiltered stream, nothing recorded — stays legacy",
			rowExists:     true,
			wantEffective: "",
		},
		{
			name:          "existing FILTERED stream with no record — stays legacy (ratchet, never flip)",
			rowExists:     true,
			filtered:      true,
			wantEffective: "",
		},
		{
			name:          "new UNfiltered stream — stays on the shared default",
			rowExists:     false,
			filtered:      false,
			wantEffective: "",
		},
		{
			name:          "new filtered stream — derives the per-stream default",
			rowExists:     false,
			filtered:      true,
			wantEffective: "sluice_wave_a",
		},
		{
			name:          "recorded name wins over the derived default on resume",
			recorded:      "sluice_recorded",
			rowExists:     true,
			filtered:      true,
			wantEffective: "sluice_recorded",
		},
		{
			name:          "recorded name reused when no flag is passed",
			recorded:      "sluice_recorded",
			rowExists:     true,
			wantEffective: "sluice_recorded",
		},
		{
			name:          "explicit flag wins over the record and reports the override",
			explicit:      "sluice_explicit",
			recorded:      "sluice_recorded",
			rowExists:     true,
			wantEffective: "sluice_explicit",
			wantOverrode:  true,
		},
		{
			name:          "explicit flag equal to the record is not an override",
			explicit:      "sluice_same",
			recorded:      "sluice_same",
			rowExists:     true,
			wantEffective: "sluice_same",
		},
		{
			name:          "explicit flag with no record wins silently",
			explicit:      "sluice_explicit",
			filtered:      true,
			wantEffective: "sluice_explicit",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			effective, overrode := resolveEffectivePublication(tc.explicit, tc.recorded, tc.rowExists, tc.filtered, "Wave-A")
			if effective != tc.wantEffective {
				t.Errorf("effective = %q, want %q", effective, tc.wantEffective)
			}
			if overrode != tc.wantOverrode {
				t.Errorf("explicitOverridesRecorded = %v, want %v", overrode, tc.wantOverrode)
			}
		})
	}
}

// TestDerivePerStreamPublicationName pins the identifier-safety
// normalization: the derived default must ALWAYS be a valid,
// unquoted-safe Postgres identifier, because CREATE PUBLICATION
// silently TRUNCATES identifiers over NAMEDATALEN-1 (63 bytes) while
// START_REPLICATION matches publication_names verbatim — a too-long
// derived name would create one publication and stream from another.
func TestDerivePerStreamPublicationName(t *testing.T) {
	t.Run("short id: prefix + lowercase + underscore mapping", func(t *testing.T) {
		if got, want := derivePerStreamPublicationName("Wave-A"), "sluice_wave_a"; got != want {
			t.Errorf("derived = %q, want %q", got, want)
		}
	})
	t.Run("auto-generated id shape sanitizes cleanly", func(t *testing.T) {
		got := derivePerStreamPublicationName("postgres://src:5432/db -> postgres://dst:5432/db")
		if !strings.HasPrefix(got, "sluice_postgres___") {
			t.Errorf("derived = %q; want the sluice_ prefix + sanitized id", got)
		}
		for _, r := range got {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
				t.Fatalf("derived %q contains %q — must be [a-z0-9_] only", got, r)
			}
		}
	})
	t.Run("long ids cap at 63 bytes with a collision-resistant hash tail", func(t *testing.T) {
		longA := strings.Repeat("verylonghost.example.com", 5) + "-tail-a"
		longB := strings.Repeat("verylonghost.example.com", 5) + "-tail-b"
		a, b := derivePerStreamPublicationName(longA), derivePerStreamPublicationName(longB)
		if len(a) != pgMaxIdentifierBytes || len(b) != pgMaxIdentifierBytes {
			t.Fatalf("lengths = %d, %d; want exactly %d (NAMEDATALEN-1)", len(a), len(b), pgMaxIdentifierBytes)
		}
		// The two ids differ only past the truncation point; without the
		// hash tail they would silently collide on ONE publication.
		if a == b {
			t.Fatalf("two distinct long stream ids derived the same publication %q — the hash tail is not doing its job", a)
		}
		// Deterministic: the same id derives the same name on every run
		// (a cold-start retry must land on the publication it created).
		if again := derivePerStreamPublicationName(longA); again != a {
			t.Errorf("derivation is not deterministic: %q then %q", a, again)
		}
	})
	t.Run("already-prefixed ids are not double-prefixed by resolve, but derive keeps its own prefix", func(t *testing.T) {
		// The derived name is sluice_ + sanitized(id) even when the id
		// itself starts with sluice_ — the id is a stream id, not a
		// publication name, and stability matters more than prettiness.
		if got := derivePerStreamPublicationName("sluice_x"); got != "sluice_sluice_x" {
			t.Errorf("derived = %q, want sluice_sluice_x", got)
		}
	})
}
