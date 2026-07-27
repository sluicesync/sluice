// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The projection-parity ratchets (audit 2026-07-26 QUAL-3 / TEST-3, gate
// G-19).
//
// `ir.TargetHealthSnapshot` is projected into THREE hand-maintained surfaces:
// the single-database `/metrics` exporter, the fleet `/metrics` exporter, and
// the JSONL sink record. They are coupled by intent and by nothing else —
// `fleetGaugeFamilies`' own doc states the coupling as a manual obligation.
//
// The two gates that existed guarded each side with an INDEPENDENT hardcoded
// count (`wantSeries = 30`, `wantSeries = 13`), which structurally cannot see a
// one-sided addition: add a family to one exporter and its own count trips,
// you update that constant, and the sibling stays silently behind. The counts
// were already drifting as documentation — the fleet one explains its 13 as
// "8 target gauges + 2 fleet gauges" when it is 11 + 2.
//
// A count also says nothing about WHICH series. These derive both sides from
// the real code and compare them.
package pipeline

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// fullyKnownSnapshot is a snapshot with EVERY *Known flag true, so every
// projection must emit every family. Built by reflection rather than by hand:
// a hand-written literal is exactly the fixture that goes stale when a signal
// is added, which is the bug this file exists to prevent.
func fullyKnownSnapshot(t *testing.T) ir.TargetHealthSnapshot {
	t.Helper()
	var snap ir.TargetHealthSnapshot
	v := reflect.ValueOf(&snap).Elem()
	typ := v.Type()
	known := 0
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !v.Field(i).CanSet() {
			continue
		}
		switch {
		case strings.HasSuffix(f.Name, "Known") && f.Type.Kind() == reflect.Bool:
			v.Field(i).SetBool(true)
			known++
		case f.Type.Kind() == reflect.Float64:
			v.Field(i).SetFloat(0.5)
		case f.Type.Kind() == reflect.Int:
			v.Field(i).SetInt(7)
		case f.Type.Kind() == reflect.Int64:
			v.Field(i).SetInt(11)
		}
	}
	if known < 4 {
		t.Fatalf("only %d *Known flags found on ir.TargetHealthSnapshot; the naming convention this ratchet "+
			"keys on has changed and the gate is vacuous — re-point it", known)
	}
	snap.SampledAt = time.Unix(1000, 0)
	return snap
}

// TestTargetGaugeFamiliesMatchAcrossExporters is the sibling-divergence gate.
// It derives the single-database exporter's target families by SCRAPING it and
// compares against the fleet exporter's declared list — so a family added to
// one and not the other fails, naming the missing side.
func TestTargetGaugeFamiliesMatchAcrossExporters(t *testing.T) {
	snap := fullyKnownSnapshot(t)

	var sb strings.Builder
	emitTargetTelemetryMetrics(&sb, "stream-1", snap)
	single := map[string]string{} // family -> HELP
	var lastHelp string
	for _, line := range strings.Split(sb.String(), "\n") {
		switch {
		case strings.HasPrefix(line, "# HELP "):
			rest := strings.TrimPrefix(line, "# HELP ")
			if i := strings.IndexByte(rest, ' '); i > 0 {
				lastHelp = rest[i+1:]
			}
		case strings.HasPrefix(line, "sluice_target_"):
			name := line
			if i := strings.IndexAny(line, "{ "); i > 0 {
				name = line[:i]
			}
			single[name] = lastHelp
		}
	}

	fleet := map[string]string{}
	for _, fam := range fleetGaugeFamilies() {
		fleet[fam.name] = fam.help
	}

	// Anti-vacuity: both sides must be non-trivially populated, or the
	// comparison below is between two empty maps.
	if len(single) < 5 || len(fleet) < 5 {
		t.Fatalf("derived %d single-database families and %d fleet families; one of the derivations broke and "+
			"this gate is vacuous — re-point it", len(single), len(fleet))
	}

	for name, help := range single {
		fleetHelp, ok := fleet[name]
		if !ok {
			t.Errorf("%s is emitted by the single-database exporter but NOT by the fleet exporter. The two "+
				"lists are coupled by intent only, so a dashboard written against one silently loses this "+
				"series against the other (audit QUAL-3).", name)
			continue
		}
		if fleetHelp != help {
			t.Errorf("%s has different HELP text in the two exporters:\n  single: %s\n  fleet:  %s\n"+
				"They are documented as mirroring exactly.", name, help, fleetHelp)
		}
	}
	for name := range fleet {
		if _, ok := single[name]; !ok {
			t.Errorf("%s is emitted by the fleet exporter but NOT by the single-database exporter — the same "+
				"divergence in the other direction.", name)
		}
	}
}

// TestSampleRecordCarriesEveryKnownSignal is the third projection, which had
// no gate at all. With every *Known flag true, the JSONL record must carry a
// value for every optional field: a nil pointer there means a signal the
// exporters publish is silently missing from the durable sink.
func TestSampleRecordCarriesEveryKnownSignal(t *testing.T) {
	snap := fullyKnownSnapshot(t)
	rec := sampleRecord(time.Unix(2000, 0), "metrics-watch:db", "db", "main", snap, true)

	v := reflect.ValueOf(rec)
	typ := v.Type()
	var missing []string
	checked := 0
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.Type.Kind() != reflect.Pointer {
			continue
		}
		checked++
		if v.Field(i).IsNil() {
			missing = append(missing, f.Name)
		}
	}

	if checked < 4 {
		t.Fatalf("only %d pointer fields on telemetrysink.Record; the shape changed and this gate is vacuous", checked)
	}
	if len(missing) > 0 {
		t.Errorf("with EVERY snapshot signal observed, the sink record still leaves %d field(s) nil: %s\n\n"+
			"A nil serialises as JSON null, which the Record contract defines as \"not observed\" — so a signal "+
			"the /metrics exporters publish would be permanently invisible to anything reading the durable "+
			"sink. Wire the field in sampleRecord (audit TEST-3).",
			len(missing), strings.Join(missing, ", "))
	}
}
