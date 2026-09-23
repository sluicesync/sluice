// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// carrierReader is an ir.CDCReader that carries an unforwarded baseline.
type carrierReader struct {
	ir.CDCReader
	have any
	got  any
}

func (c *carrierReader) UnforwardedBaseline() any     { return c.have }
func (c *carrierReader) SetUnforwardedBaseline(b any) { c.got = b }

// nonCarrierReader carries nothing (VStream, trigger-CDC).
type nonCarrierReader struct{ ir.CDCReader }

// TestWireUnforwardedBaseline_CarriesAcrossAttemptsAndResetsPerRun pins
// GC-32's pipeline half: the reader the Streamer opens for attempt N+1 is
// handed attempt N's baseline, a reader without the surface breaks
// nothing, and a new Run (an operator restart) starts fresh.
func TestWireUnforwardedBaseline_CarriesAcrossAttemptsAndResetsPerRun(t *testing.T) {
	s := &Streamer{}

	first := &carrierReader{have: "baseline-1"}
	s.wireUnforwardedBaseline(first)
	if first.got != nil {
		t.Fatalf("the first reader of a run was handed %v; it must take its own baseline", first.got)
	}

	second := &carrierReader{have: "baseline-2"}
	s.wireUnforwardedBaseline(second)
	if second.got != "baseline-1" {
		t.Fatalf("attempt 2's reader was handed %v; want attempt 1's baseline (a fresh read would absorb a pending change)", second.got)
	}

	// A reader that does not carry the door neither panics nor loses the
	// chain for a later carrier.
	s.wireUnforwardedBaseline(&nonCarrierReader{})
	third := &carrierReader{}
	s.wireUnforwardedBaseline(third)
	if third.got != nil {
		t.Errorf("a carrier after a non-carrier was handed %v; the non-carrier had nothing to give", third.got)
	}

	// A nil snapshot (door inert on the previous reader) is not forwarded.
	fourth := &carrierReader{}
	s.wireUnforwardedBaseline(fourth)
	if fourth.got != nil {
		t.Errorf("a nil baseline was forwarded as %v", fourth.got)
	}
}

// TestUnforwardedBaselineCarry_WiringSites holds the four decisions GC-32
// made in place, by source: Run resets (an operator restart re-baselines),
// the seed helper every streamer reader-open site calls carries, both
// cold-start sites reset (the target was just rebuilt from the current
// source schema), and the backup-stream transient reopen carries.
func TestUnforwardedBaselineCarry_WiringSites(t *testing.T) {
	read := func(name string) string {
		t.Helper()
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	cases := []struct {
		file, needle, why string
	}{
		{"streamer.go", "s.unforwardedBaselineFrom = nil", "Run must reset the carry so an operator restart re-baselines"},
		{"schema_seed.go", "s.wireUnforwardedBaseline(r)", "wireReaderSchemaSeed is the one call every streamer reader-open site makes"},
		{"streamer_coldstart.go", "s.unforwardedBaselineFrom = nil", "a single-stream cold start rebuilt the target and must not carry"},
		{"streamer_multidb.go", "s.unforwardedBaselineFrom = nil", "a multi-database cold start rebuilt the target and must not carry"},
		{"stream.go", "carryUnforwardedBaseline(prevCDC, cdc)", "backup stream's transient reopen must keep the closed pump's baseline"},
	}
	for _, c := range cases {
		if !strings.Contains(read(c.file), c.needle) {
			t.Errorf("%s no longer contains %q: %s (GC-32)", c.file, c.needle, c.why)
		}
	}
}
