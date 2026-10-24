// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the `schema add-table --no-drain` refusal: it is withdrawn on
// every source (Bug 332 took the last live path, Postgres's publication
// add), and each source refuses with the marker for the live path it once
// took (NO-DRAIN-POSTGRES-SOURCE, NO-DRAIN-BINLOG-SOURCE,
// NO-DRAIN-VSTREAM-SOURCE) or with the generic refusal.
//
// The matrix walks the REAL engine registry rather than a stub: whether a
// source is a publication source is a type assertion against the concrete
// engine, and its CDC method is its declared capability, so only the real
// engines say which refusal each one reaches. A registered engine the table
// below does not classify fails the test, so a new engine cannot go
// unexamined. The target plays no part in the decision.

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
)

// noDrainExpected classifies every registered engine as a --no-drain
// source: its refusal marker, or "" for the generic refusal. There is no
// live path left for an entry to take.
var noDrainExpected = map[string]string{
	"postgres":    noDrainPostgresSourceMarker,
	"mysql":       noDrainBinlogSourceMarker,
	"mariadb":     noDrainBinlogSourceMarker,
	"planetscale": noDrainVStreamSourceMarker,
	"vitess":      noDrainVStreamSourceMarker,

	"postgres-trigger": "",
	"sqlite-trigger":   "",
	"d1-trigger":       "",

	"sqlite":   "",
	"d1":       "",
	"mydumper": "",
	"csv":      "",
	"tsv":      "",
	"ndjson":   "",
}

func TestNoDrainRefusal_EngineMatrix(t *testing.T) {
	names := engines.Names()
	seen := map[string]bool{}
	for _, name := range names {
		src, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Get(%q) missed a name engines.Names() reported", name)
		}
		want, classified := noDrainExpected[name]
		if !classified {
			t.Errorf("registered engine %q has no --no-drain classification in noDrainExpected; decide which --no-drain refusal it gets as a source", name)
			continue
		}
		seen[name] = true
		// The decision preflightStream makes.
		err := noDrainRefusalForStream(src, ir.StreamStatus{})
		if err == nil {
			t.Errorf("source %s: --no-drain did not refuse; want the refusal (marker %q)", name, want)
			continue
		}
		cdc := src.Capabilities().CDC
		msg := err.Error()
		for _, m := range []string{noDrainPostgresSourceMarker, noDrainBinlogSourceMarker, noDrainVStreamSourceMarker} {
			if has := strings.Contains(msg, m); has != (m == want) {
				t.Errorf("source %s (cdc=%s): marker %s present=%v, want %v; refusal: %v", name, cdc, m, has, m == want, err)
			}
		}
		for _, remedy := range []string{"sluice sync stop --wait", "without --no-drain", "sluice sync start", "Keep the stream stopped"} {
			if !strings.Contains(msg, remedy) {
				t.Errorf("source %s: refusal lacks the drained flow's %q: %v", name, remedy, err)
			}
		}
	}

	// Anti-vacuity: the engines the decision is about must have been walked.
	for _, must := range []string{"postgres", "mysql", "mariadb", "planetscale", "vitess", "postgres-trigger", "sqlite-trigger", "d1-trigger"} {
		if !seen[must] {
			t.Errorf("engine %q was not walked (registered: %v)", must, names)
		}
	}

	// The markers key on the CDC capability: bind the premise that the
	// VStream flavors declare VStream and the binlog ones binlog.
	for name, wantCDC := range map[string]ir.CDCMethod{
		"planetscale": ir.CDCVStream,
		"vitess":      ir.CDCVStream,
		"mysql":       ir.CDCBinlog,
		"mariadb":     ir.CDCBinlog,
	} {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engine %q not registered", name)
		}
		if got := e.Capabilities().CDC; got != wantCDC {
			t.Errorf("engine %q declares CDC %s, want %s", name, got, wantCDC)
		}
	}
}

// TestAddTable_LiveMode_VStreamSourceRefusesBeforeAnyWrite drives Run with
// the REAL planetscale and vitess engines as the source and a MySQL target
// whose applier records live adds (the pair that reached the filter-flip
// path before NO-DRAIN-VSTREAM-SOURCE): the refusal names the marker and the
// drained flow, and nothing reached the target — no live-added record, no
// schema writer, no row writer. A dry run refuses the same way.
func TestAddTable_LiveMode_VStreamSourceRefusesBeforeAnyWrite(t *testing.T) {
	for _, srcName := range []string{"planetscale", "vitess"} {
		for _, dryRun := range []bool{false, true} {
			src, ok := engines.Get(srcName)
			if !ok {
				t.Fatalf("engine %q not registered", srcName)
			}
			tgt := newAddTableTargetEngineWithLiveAdd("mysql")
			tgt.applier.streams = []ir.StreamStatus{{StreamID: "live"}}
			a := &AddTable{
				Source: src, Target: tgt,
				SourceDSN: "user:pw@tcp(127.0.0.1:1)/ks", TargetDSN: "tgt",
				StreamID: "live", TableName: "new_table",
				LiveMode: true,
				DryRun:   dryRun,
			}
			err := a.Run(context.Background())
			if err == nil {
				t.Fatalf("%s dry-run=%v: Run with --no-drain on a VStream source returned nil; want the NO-DRAIN-VSTREAM-SOURCE refusal", srcName, dryRun)
			}
			for _, want := range []string{noDrainVStreamSourceMarker, "sluice sync stop --wait", "without --no-drain", "sluice sync start"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s dry-run=%v: err = %v; want it to contain %q", srcName, dryRun, err, want)
				}
			}
			if got := tgt.applier.recordedLiveAdds; len(got) != 0 {
				t.Errorf("%s dry-run=%v: recorded live adds %v before refusing; want none", srcName, dryRun, got)
			}
			if tgt.openSchemaWriterCalls != 0 || tgt.openRowWriterCalls != 0 {
				t.Errorf("%s dry-run=%v: opened %d schema writer(s) and %d row writer(s) before refusing; want none", srcName, dryRun, tgt.openSchemaWriterCalls, tgt.openRowWriterCalls)
			}
		}
	}
}

// TestAddTable_LiveMode_RecordedPositionPicksTheRefusal drives Run with the
// real MySQL-family engines and a stream position recorded on the target:
// every cell refuses before anything is written, and the marker names the
// CDC method the stream actually runs on. A VStream stream handed
// `--source-driver mysql` (declared binlog) refuses NO-DRAIN-VSTREAM-SOURCE
// from its recorded VGTID; a recorded binlog position, or no position yet,
// refuses NO-DRAIN-BINLOG-SOURCE; a declared VStream source refuses
// NO-DRAIN-VSTREAM-SOURCE whatever the position says. The tokens are the
// shapes the MySQL codecs write (TestPositionCDCMethod binds them to the
// encoders).
func TestAddTable_LiveMode_RecordedPositionPicksTheRefusal(t *testing.T) {
	const (
		vstreamToken = `[{"keyspace":"ks","shard":"0","gtid":"MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"}]`
		binlogToken  = `{"mode":"gtid","gtid_set":"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"}`
	)
	cases := []struct {
		name       string
		source     string
		position   ir.Position
		wantMarker string
	}{
		{"mysql source, recorded vstream position", "mysql", ir.Position{Engine: "mysql", Token: vstreamToken}, noDrainVStreamSourceMarker},
		{"mariadb source, recorded vstream position", "mariadb", ir.Position{Engine: "mysql", Token: vstreamToken}, noDrainVStreamSourceMarker},
		{"mysql source, recorded binlog position", "mysql", ir.Position{Engine: "mysql", Token: binlogToken}, noDrainBinlogSourceMarker},
		{"mariadb source, recorded binlog position", "mariadb", ir.Position{Engine: "mysql", Token: binlogToken}, noDrainBinlogSourceMarker},
		{"mysql source, no position yet", "mysql", ir.Position{}, noDrainBinlogSourceMarker},
		{"planetscale source, recorded binlog position", "planetscale", ir.Position{Engine: "mysql", Token: binlogToken}, noDrainVStreamSourceMarker},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src, ok := engines.Get(c.source)
			if !ok {
				t.Fatalf("engine %q not registered", c.source)
			}
			tgt := newAddTableTargetEngineWithLiveAdd("mysql")
			tgt.applier.streams = []ir.StreamStatus{{StreamID: "live", Position: c.position}}
			err := (&AddTable{
				Source: src, Target: tgt,
				SourceDSN: "user:pw@tcp(127.0.0.1:1)/ks", TargetDSN: "tgt",
				StreamID: "live", TableName: "new_table",
				LiveMode: true,
			}).Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), c.wantMarker) {
				t.Fatalf("Run = %v; want the %s refusal", err, c.wantMarker)
			}
			if len(tgt.applier.recordedLiveAdds) != 0 || tgt.openSchemaWriterCalls != 0 || tgt.openRowWriterCalls != 0 {
				t.Errorf("wrote before refusing: live adds %v, schema writers %d, row writers %d", tgt.applier.recordedLiveAdds, tgt.openSchemaWriterCalls, tgt.openRowWriterCalls)
			}
		})
	}
}

// TestAddTable_InFlightStopHintNeverOffersNoDrain pins the drained
// preflight's in-flight-stop hint: it never offers --no-drain, which every
// source refuses. Until Bug 332 it offered it on a publication source.
func TestAddTable_InFlightStopHintNeverOffersNoDrain(t *testing.T) {
	binlog := newAddTableSourceEngine("binlog-source")
	binlog.cdc = ir.CDCBinlog
	cases := []struct {
		name      string
		src       ir.Engine
		wantOffer bool
	}{
		{"binlog source", binlog, false},
		{"publication source", newAddTableSourceEngineLive("pg-source"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tgt := newAddTableTargetEngine("target")
			tgt.applier.streams = []ir.StreamStatus{{StreamID: "s"}}
			tgt.applier.stopRequested = true
			err := (&AddTable{
				Source: c.src, Target: tgt,
				SourceDSN: "src", TargetDSN: "tgt",
				StreamID: "s", TableName: "new_table",
			}).Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), "in-flight stop request") {
				t.Fatalf("Run = %v; want the in-flight-stop refusal", err)
			}
			if got := strings.Contains(err.Error(), "--no-drain"); got != c.wantOffer {
				t.Errorf("hint offers --no-drain = %v, want %v: %v", got, c.wantOffer, err)
			}
		})
	}
}
