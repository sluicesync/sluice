// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0176 prerequisite ratchet: the effective-
// publication resolution matrix and the per-stream default's
// identifier-safety normalization. The end-to-end behaviour (a real
// PG source, the record round-trip, cleanup parity) is pinned by the
// TestPublicationScope_* integration gates.

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
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
		reset         bool
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
		{
			// audit 2026-07-23 ARCH-3 (M2-8): --reset-target-data is a
			// from-scratch restart whose ClearStream runs AFTER this phase read
			// the row, so the stale-true rowExists must not suppress the
			// per-stream derivation — pre-fix the reset landed its row filters
			// on the shared `sluice_pub` default.
			name:          "filtered --reset-target-data with a stale row and no record — derives per-stream",
			rowExists:     true,
			reset:         true,
			filtered:      true,
			wantEffective: "sluice_wave_a",
		},
		{
			name:          "--reset-target-data keeps a recorded per-stream publication (continuity, not orphaning)",
			recorded:      "sluice_recorded",
			rowExists:     true,
			reset:         true,
			filtered:      true,
			wantEffective: "sluice_recorded",
		},
		{
			name:          "unfiltered --reset-target-data stays on the shared default",
			rowExists:     true,
			reset:         true,
			wantEffective: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			effective, overrode := resolveEffectivePublication(tc.explicit, tc.recorded, tc.rowExists, tc.reset, tc.filtered, "Wave-A")
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

// scopeRecordingSource is a stub ir.Engine (panics on any Open* — the
// ratchet phase must not touch the source) that implements
// [ir.PublicationScoper] and records every scope push, so a test can
// assert both that a re-push happened and that one did NOT.
type scopeRecordingSource struct {
	stubEngine
	pushes *[]string
}

func (s scopeRecordingSource) WithPublicationScope(publication, ownSlot string) ir.Engine {
	*s.pushes = append(*s.pushes, publication+"|"+ownSlot)
	return s
}

// ratchetRecordingApplier serves the ratchet phase's control-row read
// and records the publication-name / row-filter-hash setters; every
// other ChangeApplier method panics via the embedded nil interface —
// the stubEngine philosophy (an unexpected call is a phase-ordering
// regression).
type ratchetRecordingApplier struct {
	ir.ChangeApplier
	streams []ir.StreamStatus
	pubSet  []string
}

func (a *ratchetRecordingApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return a.streams, nil
}

func (a *ratchetRecordingApplier) SetPublicationName(name string) {
	a.pubSet = append(a.pubSet, name)
}

// TestPhaseResolvePublicationScope_ExplicitOverrideArc pins the
// explicit-`--publication-name`-overrides-recorded arc end to end at the
// unit level (audit 2026-07-23 TEST-6 — previously untested at every
// level): the WARN naming both names, the record updating to the
// explicit name, and — just as load-bearing — NO re-push into the source
// engine (phaseResolveStreamIdentity already pushed the explicit flag;
// a second push would be a silent behavioural drift). The recorded-name
// resume arc pins the opposite: the re-push MUST happen there.
func TestPhaseResolvePublicationScope_ExplicitOverrideArc(t *testing.T) {
	ctx := context.Background()
	newStreamer := func(explicit string, pushes *[]string) *Streamer {
		return &Streamer{
			Source:          scopeRecordingSource{pushes: pushes},
			PublicationName: explicit,
			SlotName:        "sluice_s",
		}
	}
	recordedRow := func(pub string) []ir.StreamStatus {
		return []ir.StreamStatus{{StreamID: "sid", PublicationName: pub}}
	}

	t.Run("explicit differs from recorded — WARN, record update, NO re-push", func(t *testing.T) {
		logs := captureSlog(t)
		pushes := []string{}
		applier := &ratchetRecordingApplier{streams: recordedRow("sluice_recorded")}
		s := newStreamer("sluice_explicit", &pushes)
		if err := s.phaseResolvePublicationScope(ctx, applier, "sid"); err != nil {
			t.Fatalf("phaseResolvePublicationScope: %v", err)
		}
		out := logs.String()
		if !strings.Contains(out, "overrides the publication recorded") {
			t.Errorf("missing the override WARN; logs:\n%s", out)
		}
		if !strings.Contains(out, "recorded=sluice_recorded") || !strings.Contains(out, "explicit=sluice_explicit") {
			t.Errorf("the WARN must name BOTH publications; logs:\n%s", out)
		}
		if len(pushes) != 0 {
			t.Errorf("explicit flag must not re-push (phaseResolveStreamIdentity already did); pushes = %v", pushes)
		}
		if len(applier.pubSet) != 1 || applier.pubSet[0] != "sluice_explicit" {
			t.Errorf("record must update to the explicit name; SetPublicationName calls = %v", applier.pubSet)
		}
		if s.PublicationName != "sluice_explicit" {
			t.Errorf("effective publication = %q; want the explicit flag", s.PublicationName)
		}
	})

	t.Run("explicit equal to recorded — silent, no re-push, record refreshed", func(t *testing.T) {
		logs := captureSlog(t)
		pushes := []string{}
		applier := &ratchetRecordingApplier{streams: recordedRow("sluice_same")}
		if err := newStreamer("sluice_same", &pushes).phaseResolvePublicationScope(ctx, applier, "sid"); err != nil {
			t.Fatalf("phaseResolvePublicationScope: %v", err)
		}
		if out := logs.String(); strings.Contains(out, "overrides the publication recorded") {
			t.Errorf("equal explicit+recorded must not WARN; logs:\n%s", out)
		}
		if len(pushes) != 0 {
			t.Errorf("no re-push expected; pushes = %v", pushes)
		}
		if len(applier.pubSet) != 1 || applier.pubSet[0] != "sluice_same" {
			t.Errorf("SetPublicationName calls = %v; want [sluice_same]", applier.pubSet)
		}
	})

	t.Run("recorded name with no flag — re-pushed into the source engine", func(t *testing.T) {
		logs := captureSlog(t)
		pushes := []string{}
		applier := &ratchetRecordingApplier{streams: recordedRow("sluice_recorded")}
		s := newStreamer("", &pushes)
		if err := s.phaseResolvePublicationScope(ctx, applier, "sid"); err != nil {
			t.Fatalf("phaseResolvePublicationScope: %v", err)
		}
		if len(pushes) != 1 || pushes[0] != "sluice_recorded|sluice_s" {
			t.Errorf("recorded-name resume must re-push scope+own-slot; pushes = %v", pushes)
		}
		if s.PublicationName != "sluice_recorded" {
			t.Errorf("effective publication = %q; want the recorded name", s.PublicationName)
		}
		if !strings.Contains(logs.String(), "resuming through the publication recorded") {
			t.Errorf("missing the recorded-resume INFO; logs:\n%s", logs.String())
		}
	})

	t.Run("dry-run never records", func(t *testing.T) {
		pushes := []string{}
		applier := &ratchetRecordingApplier{streams: recordedRow("sluice_recorded")}
		s := newStreamer("sluice_explicit", &pushes)
		s.DryRun = true
		if err := s.phaseResolvePublicationScope(ctx, applier, "sid"); err != nil {
			t.Fatalf("phaseResolvePublicationScope: %v", err)
		}
		if len(applier.pubSet) != 0 {
			t.Errorf("dry-run must not record; SetPublicationName calls = %v", applier.pubSet)
		}
	})
}

// TestRowFilterPushdownHash pins the row_filter_hash stored-codec value
// (audit 2026-07-23 D0-2): canonical (order-independent, table-case-
// insensitive), 16 lower-hex bytes, sensitive to every field of every
// pair, and the empty-subset SENTINEL is non-empty — the empty string
// stays reserved for "not recorded" so the COALESCE position-write shape
// can distinguish "nothing pushed" from "legacy row".
func TestRowFilterPushdownHash(t *testing.T) {
	a := rowFilterPushdownHash(map[string]string{"orders": "id < 100", "users": "country = 'US'"})
	b := rowFilterPushdownHash(map[string]string{"users": "country = 'US'", "orders": "id < 100"})
	if a != b {
		t.Errorf("hash is insertion-order dependent: %s vs %s", a, b)
	}
	if len(a) != 16 {
		t.Errorf("hash %q is not 16 hex bytes", a)
	}
	if got := rowFilterPushdownHash(map[string]string{"Orders": "id < 100", "Users": "country = 'US'"}); got != a {
		t.Errorf("table-name casing changed the hash: %s vs %s (keys must canonicalize)", got, a)
	}
	if got := rowFilterPushdownHash(map[string]string{"orders": "id < 200", "users": "country = 'US'"}); got == a {
		t.Error("a changed predicate did not change the hash")
	}
	if got := rowFilterPushdownHash(map[string]string{"orders": "id < 100"}); got == a {
		t.Error("a removed table did not change the hash")
	}
	// The empty-subset sentinel: fnv64a's offset basis, pinned byte-exact —
	// it is persisted state (a stored codec), so it must never drift.
	if got := rowFilterPushdownHash(nil); got != "cbf29ce484222325" {
		t.Errorf("empty-subset sentinel = %q; want cbf29ce484222325 (fnv64a offset basis)", got)
	}
	if got := rowFilterPushdownHash(map[string]string{}); got != "cbf29ce484222325" {
		t.Errorf("empty-map sentinel = %q; want cbf29ce484222325", got)
	}
	if rowFilterPushdownHash(nil) == a {
		t.Error("empty subset collides with a non-empty subset")
	}
}

// TestRowFilterHashDrift pins the D0-2 warm-resume drift decision matrix,
// including the escapes that must never be blocked by the refusal itself.
func TestRowFilterHashDrift(t *testing.T) {
	const (
		recorded = "00000000aaaaaaaa"
		other    = "00000000bbbbbbbb"
	)
	cases := []struct {
		name                      string
		rowExists, restart, reset bool
		recorded, current         string
		want                      bool
	}{
		{"no control row — new stream, nothing to drift", false, false, false, "", other, false},
		{"legacy row (hash never recorded) — unknown, allow", true, false, false, "", other, false},
		{"same hash — byte-identical resume proceeds", true, false, false, recorded, recorded, false},
		{"changed subset — refuse", true, false, false, recorded, other, true},
		{"removed --where (current = empty-subset sentinel) — refuse", true, false, false, recorded, rowFilterPushdownHash(nil), true},
		{"added --where onto a recorded empty subset — refuse", true, false, false, rowFilterPushdownHash(nil), other, true},
		{"--restart-from-scratch escape is never blocked", true, true, false, recorded, other, false},
		{"--reset-target-data escape is never blocked", true, false, true, recorded, other, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rowFilterHashDrift(tc.rowExists, tc.restart, tc.reset, tc.recorded, tc.current); got != tc.want {
				t.Errorf("rowFilterHashDrift(rowExists=%v restart=%v reset=%v recorded=%q current=%q) = %v, want %v",
					tc.rowExists, tc.restart, tc.reset, tc.recorded, tc.current, got, tc.want)
			}
		})
	}
}
