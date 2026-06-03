// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestEngine_CompareLSN exercises the live-mode invariant helper
// (ADR-0030) on canonical PG LSN strings. The compare must be
// numeric over the parsed pglogrepl.LSN value, not lexicographic
// over the string form — `0/A` sorts above `0/9` lexicographically
// but pglogrepl.ParseLSN gives 10 vs 9 numerically.
func TestEngine_CompareLSN(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"0/0", "0/0", 0},
		{"0/1000", "0/2000", -1},
		{"0/2000", "0/1000", +1},
		{"1/0", "0/FFFFFFFF", +1},
		{"0/9", "0/A", -1}, // numeric 9 < 10; lexicographic would invert
	}
	var e Engine
	for _, c := range cases {
		c := c
		t.Run(c.a+"_vs_"+c.b, func(t *testing.T) {
			got, err := e.CompareLSN(c.a, c.b)
			if err != nil {
				t.Fatalf("CompareLSN: %v", err)
			}
			if got != c.want {
				t.Errorf("CompareLSN(%q, %q) = %d; want %d", c.a, c.b, got, c.want)
			}
		})
	}
}

// TestEngine_CompareLSN_BadInput surfaces a wrapped error on a
// malformed LSN; the live-mode preflight refuses on this rather
// than silently treating malformed input as comparable.
func TestEngine_CompareLSN_BadInput(t *testing.T) {
	var e Engine
	if _, err := e.CompareLSN("not-an-lsn", "0/1000"); err == nil {
		t.Errorf("expected error on malformed first arg; got nil")
	}
	if _, err := e.CompareLSN("0/1000", "still-not-an-lsn"); err == nil {
		t.Errorf("expected error on malformed second arg; got nil")
	}
}

// TestEngine_ExtractSnapshotLSN_RoundTrip confirms the engine can
// extract the LSN from a position envelope it produced via
// encodePGPos (the same envelope OpenSnapshotStreamWithSlot emits).
func TestEngine_ExtractSnapshotLSN_RoundTrip(t *testing.T) {
	pos, err := encodePGPos(pgPos{Slot: "sluice_addtable_foo", LSN: "0/1234ABCD"})
	if err != nil {
		t.Fatalf("encodePGPos: %v", err)
	}
	var e Engine
	got, ok, err := e.ExtractSnapshotLSN(pos)
	if err != nil {
		t.Fatalf("ExtractSnapshotLSN: %v", err)
	}
	if !ok {
		t.Fatalf("ExtractSnapshotLSN ok = false; want true on a valid envelope")
	}
	if got != "0/1234ABCD" {
		t.Errorf("ExtractSnapshotLSN LSN = %q; want %q", got, "0/1234ABCD")
	}
}

// TestEngine_ExtractSnapshotLSN_Zero reports the zero-value position
// as "no LSN" rather than an error. The orchestrator treats this as
// "skip the invariant check"; surfacing a hard error here would be
// over-strict.
func TestEngine_ExtractSnapshotLSN_Zero(t *testing.T) {
	var e Engine
	_, ok, err := e.ExtractSnapshotLSN(ir.Position{})
	if err != nil {
		t.Errorf("ExtractSnapshotLSN(zero): unexpected err = %v", err)
	}
	if ok {
		t.Errorf("ExtractSnapshotLSN(zero) ok = true; want false")
	}
}

// TestEngine_ExtractSnapshotLSN_WrongEngine rejects a position from
// another engine — a MySQL token replayed into the PG path is a
// caller bug worth surfacing loudly.
func TestEngine_ExtractSnapshotLSN_WrongEngine(t *testing.T) {
	var e Engine
	_, _, err := e.ExtractSnapshotLSN(ir.Position{Engine: "mysql", Token: "{}"})
	if err == nil {
		t.Errorf("expected an engine-mismatch error; got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "engine") {
		t.Errorf("err = %v; want contains \"engine\"", err)
	}
}
