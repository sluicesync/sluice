// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The roadmap item 144 pipeline gates: the TARGET-declared write fan-out
// ceiling must actually REACH the workers.
//
// # Why the arithmetic is not the gate
//
// Item 144's defect was not a wrong number. `computeConnectionBudget`
// derived the right tier verdict for a PlanetScale target, the cold-start
// preflight logged it as "capping bulk parallelism … effective=2", and the
// value was then DISCARDED — the copy fanned out to the operator's degree
// regardless. A gate over [applyCopyFanoutCeiling] alone would have been
// green throughout. So the load-bearing test here counts the WORKER CHANNELS
// the writer is handed, which is the only observable that changes.
//
// SCOPE this gate reaches: `runBulkCopyWithOpts`, the copy core BOTH sync
// cold-start entry points call (single-database `streamer_coldstart.go` and
// multi-database `streamer_multidb.go`), on both the concurrent-partition
// and the serial-table-loop branches. It does NOT reach `migrate` or the
// ADR-0079 fast cold-start: neither drives the D-way fan-out at all (they
// use migrate's chunk/table pool, already budget-capped at
// `phaseResolveCopyParallelism`). That the two production callers actually
// SET the field is enforced separately and fail-by-default, by
// TestBulkCopyOptsProductionSitesSetEveryKnob.

// fanoutRecordingWriter is a plain [ir.ParallelCopyWriter] that records how
// many worker channels each table's copy was fanned out across — the exact
// observable item 144 is about — and drains them so the copy completes.
type fanoutRecordingWriter struct {
	mu      sync.Mutex
	degrees map[string]int
}

func newFanoutRecordingWriter() *fanoutRecordingWriter {
	return &fanoutRecordingWriter{degrees: map[string]int{}}
}

func (w *fanoutRecordingWriter) WriteRows(ctx context.Context, t *ir.Table, rows <-chan ir.Row) error {
	w.mu.Lock()
	// The serial single-writer path is degree 1 by definition.
	if _, seen := w.degrees[t.Name]; !seen {
		w.degrees[t.Name] = 1
	}
	w.mu.Unlock()
	for range rows { //nolint:revive // draining is the point
	}
	return ctx.Err()
}

func (w *fanoutRecordingWriter) WriteRowsParallel(ctx context.Context, t *ir.Table, workers []<-chan ir.Row) error {
	w.mu.Lock()
	w.degrees[t.Name] = len(workers)
	w.mu.Unlock()

	var wg sync.WaitGroup
	for _, ch := range workers {
		wg.Add(1)
		go func(c <-chan ir.Row) {
			defer wg.Done()
			for range c { //nolint:revive // draining is the point
			}
		}(ch)
	}
	wg.Wait()
	return ctx.Err()
}

func (w *fanoutRecordingWriter) Close() error { return nil }

func (w *fanoutRecordingWriter) degreeFor(table string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.degrees[table]
}

// TestBulkCopyHonoursTheTargetFanoutCeiling is the pin item 144 exists for,
// stated as a PAIR: the ceiling must bind when the target declares one, AND
// the copy must keep the operator's full degree when it does not. A gate
// that only asserted the first would pass on a hard-coded degree of 2, and
// one that only asserted the second is what shipped.
func TestBulkCopyHonoursTheTargetFanoutCeiling(t *testing.T) {
	groups := [][]string{{"a"}, {"b"}}

	cases := []struct {
		name       string
		degree     int
		ceiling    int
		wantDegree int
	}{
		// No ceiling declared — every non-PlanetScale target, every degraded
		// probe, every zero-valued report. Byte-identical to the prior
		// behaviour, and the anti-vacuity half of the pair.
		{name: "no ceiling ⇒ the operator's degree stands", degree: 4, ceiling: 0, wantDegree: 4},
		{name: "negative ceiling (defensive) ⇒ degree stands", degree: 4, ceiling: -1, wantDegree: 4},

		// The measured case: a PlanetScale target whose plan tier the probe
		// could not read declares 2, and 4 lanes become 2.
		{name: "unknown-tier ceiling binds the default degree", degree: 4, ceiling: 2, wantDegree: 2},

		// It lowers an EXPLICIT --copy-fanout-degree too, exactly as the
		// connection budget lowers an explicit --bulk-parallelism.
		{name: "ceiling binds an explicit wider degree", degree: 8, ceiling: 2, wantDegree: 2},

		// One-directional: a ceiling ABOVE the request never raises it.
		{name: "ceiling above the request never raises it", degree: 2, ceiling: 8, wantDegree: 2},

		// A serial opt-out stays serial under any ceiling.
		{name: "explicit serial stays serial", degree: 1, ceiling: 2, wantDegree: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := concSchema("a", "b")
			reader := newNativeConcReader(groups, map[string]int{"a": 9, "b": 9})
			writer := newFanoutRecordingWriter()
			opts := bulkCopyOpts{CopyFanoutDegree: tc.degree, CopyFanoutCeiling: tc.ceiling}
			if err := runBulkCopyWithOpts(context.Background(), schema, reader, noopSchemaWriter{}, writer, opts); err != nil {
				t.Fatalf("runBulkCopyWithOpts: %v", err)
			}
			for _, tbl := range []string{"a", "b"} {
				if got := writer.degreeFor(tbl); got != tc.wantDegree {
					t.Errorf("table %q fanned out across %d worker channel(s); want %d "+
						"(degree=%d ceiling=%d)", tbl, got, tc.wantDegree, tc.degree, tc.ceiling)
				}
			}
		})
	}
}

// TestBulkCopyFanoutCeiling_ReachesTheSerialTableLoopToo closes the sibling
// inside the copy core: `runBulkCopyWithOpts` has TWO branches that consume
// the degree — the concurrent-partition branch (above) and the serial table
// loop taken when the reader surfaces no partition (a single-stream VStream,
// a serial native-MySQL snapshot, K=1). Both fan out per table, so a ceiling
// wired into only one of them is the shape this project keeps paying for.
func TestBulkCopyFanoutCeiling_ReachesTheSerialTableLoopToo(t *testing.T) {
	schema := concSchema("a", "b")
	// nil groups ⇒ no concurrent partition ⇒ the serial table loop.
	reader := newNativeConcReader(nil, map[string]int{"a": 9, "b": 9})
	writer := newFanoutRecordingWriter()
	opts := bulkCopyOpts{CopyFanoutDegree: 4, CopyFanoutCeiling: 2}
	if err := runBulkCopyWithOpts(context.Background(), schema, reader, noopSchemaWriter{}, writer, opts); err != nil {
		t.Fatalf("runBulkCopyWithOpts: %v", err)
	}
	for _, tbl := range []string{"a", "b"} {
		if got := writer.degreeFor(tbl); got != 2 {
			t.Errorf("serial-loop table %q fanned out across %d worker channel(s); want 2", tbl, got)
		}
	}
}

// coldStartProbingEngine is [coldStartFallbackEngine] plus the optional
// connection-budget prober, so the cold-start preflight takes the branch a
// real PlanetScale target takes.
type coldStartProbingEngine struct {
	*coldStartFallbackEngine
	report ir.ConnectionBudget
	probes int
}

func (e *coldStartProbingEngine) ProbeTargetConnectionBudget(context.Context, string, int, int) (ir.ConnectionBudget, error) {
	e.probes++
	return e.report, nil
}

// TestColdStartOpenTargetWriters_CarriesTheProbedFanoutCeiling is the pin on
// the CROSSING item 144's defect lived at.
//
// The preflight always ran and always computed the target's verdict; the
// cold-start then threw the report away, with a comment saying so and a
// second comment asserting the step was "a no-op on MySQL" — which stopped
// being true in v0.100.0. Both halves are gone, and this test fails if either
// comes back: the probed ceiling must arrive at the caller intact, and a
// target that declares none must still yield 0 (so the fix cannot be a
// hard-coded 2 in disguise).
func TestColdStartOpenTargetWriters_CarriesTheProbedFanoutCeiling(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}

	cases := []struct {
		name   string
		report ir.ConnectionBudget
		want   int
	}{
		{
			name:   "target declares a ceiling ⇒ it reaches the caller",
			report: ir.ConnectionBudget{CopyBudget: 200, EffectiveParallelism: 4, CopyFanoutCeiling: 2},
			want:   2,
		},
		{
			name:   "target declares none ⇒ 0, the no-op sentinel",
			report: ir.ConnectionBudget{CopyBudget: 200, EffectiveParallelism: 4},
			want:   0,
		},
		{
			name:   "a degraded probe carries nothing (blind pre-budget behaviour)",
			report: ir.ConnectionBudget{ProbeFailed: true, Warning: "catalog quirk", CopyFanoutCeiling: 2},
			want:   0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := &coldStartProbingEngine{
				coldStartFallbackEngine: &coldStartFallbackEngine{sw: &coldStartFallbackSchemaWriter{}},
				report:                  tc.report,
			}
			s := &Streamer{Target: eng, TargetDSN: "tgt"}
			_, _, ceiling, err := s.coldStartOpenTargetWriters(context.Background(), schema, &ir.SnapshotStream{})
			if err != nil {
				t.Fatalf("coldStartOpenTargetWriters: %v", err)
			}
			if eng.probes != 1 {
				t.Fatalf("the target was probed %d time(s); want exactly 1 — a preflight that does not run "+
					"cannot carry a verdict", eng.probes)
			}
			if ceiling != tc.want {
				t.Errorf("carried fan-out ceiling = %d; want %d", ceiling, tc.want)
			}
		})
	}
}

// TestApplyCopyFanoutCeiling is the arithmetic pin. It is deliberately the
// SMALL half of this file — see the file header for why the worker-channel
// count above is the one that matters.
func TestApplyCopyFanoutCeiling(t *testing.T) {
	cases := []struct {
		degree, ceiling, want int
		wantCapped            bool
	}{
		{degree: 4, ceiling: 0, want: 4},                    // no ceiling ⇒ no-op
		{degree: 4, ceiling: -1, want: 4},                   // defensive
		{degree: 4, ceiling: 2, want: 2, wantCapped: true},  // the measured case
		{degree: 64, ceiling: 2, want: 2, wantCapped: true}, // the max degree
		{degree: 2, ceiling: 2, want: 2},                    // equal ⇒ not "capped"
		{degree: 1, ceiling: 2, want: 1},                    // serial opt-out
		{degree: 2, ceiling: 8, want: 2},                    // never raises
		{degree: 4, ceiling: 1, want: 1, wantCapped: true},  // a ceiling of 1 is serial
	}
	for _, c := range cases {
		got, capped := applyCopyFanoutCeiling(c.degree, c.ceiling)
		if got != c.want || capped != c.wantCapped {
			t.Errorf("applyCopyFanoutCeiling(%d, %d) = (%d, %v); want (%d, %v)",
				c.degree, c.ceiling, got, capped, c.want, c.wantCapped)
		}
		if got < 1 {
			t.Errorf("applyCopyFanoutCeiling(%d, %d) resolved to %d — no input may produce zero workers",
				c.degree, c.ceiling, got)
		}
	}
}
