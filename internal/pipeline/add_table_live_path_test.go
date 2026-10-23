// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for liveAddPathFor, the `schema add-table --no-drain` engine-pair
// decision, and for the NO-DRAIN-VSTREAM-SOURCE refusal it feeds.
//
// The matrix walks the REAL engine registry rather than a stub: whether a
// source is a publication source is a type assertion against the concrete
// engine, and its CDC method is its declared capability, so only the real
// engines say which path each one reaches. A registered engine the table
// below does not classify fails the test, so a new engine cannot join the
// live paths unexamined.

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

// liveAddSourceClass is what a source engine's --no-drain path should be.
type liveAddSourceClass int

const (
	liveAddNone        liveAddSourceClass = iota // refuses with every target
	liveAddPublication                           // publication-add, every target
	liveAddBinlog                                // filter-flip, MySQL-family targets only
)

// liveAddExpected classifies every registered engine as a source.
var liveAddExpected = map[string]liveAddSourceClass{
	"postgres":    liveAddPublication,
	"mysql":       liveAddBinlog,
	"mariadb":     liveAddBinlog,
	"planetscale": liveAddNone, // VStream: NO-DRAIN-VSTREAM-SOURCE
	"vitess":      liveAddNone, // VStream: NO-DRAIN-VSTREAM-SOURCE

	"postgres-trigger": liveAddNone,
	"sqlite-trigger":   liveAddNone,
	"d1-trigger":       liveAddNone,

	"sqlite":   liveAddNone,
	"d1":       liveAddNone,
	"mydumper": liveAddNone,
	"csv":      liveAddNone,
	"tsv":      liveAddNone,
	"ndjson":   liveAddNone,
}

func TestLiveAddPathFor_EngineMatrix(t *testing.T) {
	targets := []struct {
		name         string
		mysqlFamily  bool
		recordsAdded bool
	}{
		{"mysql", true, true},
		{"mariadb", true, true},
		{"planetscale", true, true},
		{"vitess", true, true},
		{"mysql", true, false}, // an applier without the live-added surface
		{"postgres", false, true},
		{"postgres", false, false},
	}

	names := engines.Names()
	seen := map[string]bool{}
	for _, name := range names {
		src, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Get(%q) missed a name engines.Names() reported", name)
		}
		want, classified := liveAddExpected[name]
		if !classified {
			t.Errorf("registered engine %q has no --no-drain classification in liveAddExpected; decide which live path (if any) it takes as a source", name)
			continue
		}
		seen[name] = true
		_, publicationSource := src.(publicationAdder)
		cdc := src.Capabilities().CDC
		for _, tgt := range targets {
			mode, ok := liveAddPathFor(publicationSource, cdc, tgt.name, tgt.recordsAdded)
			switch want {
			case liveAddPublication:
				if !ok || mode != liveModePublicationAdd {
					t.Errorf("source %s (cdc=%s) -> target %s (records=%v): got (mode=%d, ok=%v), want the publication-add path", name, cdc, tgt.name, tgt.recordsAdded, mode, ok)
				}
			case liveAddBinlog:
				wantOK := tgt.mysqlFamily && tgt.recordsAdded
				if ok != wantOK || (ok && mode != liveModeBinlogFilterFlip) {
					t.Errorf("source %s (cdc=%s) -> target %s (records=%v): got (mode=%d, ok=%v), want filter-flip=%v", name, cdc, tgt.name, tgt.recordsAdded, mode, ok, wantOK)
				}
			case liveAddNone:
				if ok {
					t.Errorf("source %s (cdc=%s) -> target %s (records=%v): got a live path (mode=%d), want a refusal", name, cdc, tgt.name, tgt.recordsAdded, mode)
				}
			}
		}
	}

	// Anti-vacuity: the engines the decision is about must have been walked.
	for _, must := range []string{"postgres", "mysql", "mariadb", "planetscale", "vitess", "postgres-trigger", "sqlite-trigger", "d1-trigger"} {
		if !seen[must] {
			t.Errorf("engine %q was not walked (registered: %v)", must, names)
		}
	}

	// The VStream refusal message keys on the CDC capability: bind the
	// premise that the VStream flavors declare it, and the binlog ones do not.
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
