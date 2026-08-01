//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// `sync start --upfront-indexes` on the ADR-0079 FAST parallel cold-start
// (the item-111 phase-2 gap closed 2026-08-01).
//
// The fast lane passed a hardcoded `false` for runBulkCopyPhases'
// upfrontIndexes/analyzeAfter arguments while the serial sibling threaded the
// operator's values — and the fast lane is the DEFAULT for any Postgres source
// that exports a shareable snapshot, so the flag was silently inert exactly
// where a PG→PG sync would land. The static gate
// (TestCopyPhaseKnobsReachTheOrchestratorFromAField) prevents the argument
// from becoming a constant again; this is the behavioural half, on a real
// Postgres pair.
//
// The signal is which BUILDER runs, which is mutually exclusive between the
// two settings and needs no timing at all. On the DEFAULT path a PG target
// takes the ADR-0077 overlapped per-table builder, whose worker pool carries
// the postgres.SetIndexBuildStartObserverForTest seam. With --upfront-indexes
// runBulkCopyPhases relocates the phase to the whole-schema CreateIndexes
// BEFORE the copy, and the overlapped pool never runs — so the seam fires
// zero times while every index still exists on the target. A regression that
// re-hardcoded `false` would put the fast lane back on the overlapped builder
// and the seam would fire, failing this test.
//
// Both sub-tests also assert through the coldStartDispatchObserver that the
// FAST lane was actually taken: a silent fall-back to the serial path would
// also produce zero overlapped builds, and without that check the upfront
// assertion could pass for the wrong reason.

package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/postgres" // named: SetIndexBuildStartObserverForTest + self-registers via init
)

func TestStreamer_ColdStartParallel_PG_UpfrontIndexesReachTheFastLane(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const (
		tableCount = 4
		rowsEach   = 2_000
	)

	// run drives one PG→PG cold-start and reports how many OVERLAPPED
	// per-table index builds fired and whether the ADR-0079 fast lane was the
	// dispatch taken.
	run := func(t *testing.T, upfront bool, streamID string) (overlappedBuilds int, fastLane bool) {
		t.Helper()
		src, tgt, cleanup := startPostgresLogical(t)
		defer cleanup()
		seedManyIndexedTables(t, src, tableCount, rowsEach)

		var obsMu sync.Mutex
		prevDispatch := coldStartDispatchObserver
		coldStartDispatchObserver = func(fast bool) {
			obsMu.Lock()
			fastLane = fast
			obsMu.Unlock()
		}
		defer func() { coldStartDispatchObserver = prevDispatch }()
		restoreIdx := postgres.SetIndexBuildStartObserverForTest(func(_ string) {
			obsMu.Lock()
			overlappedBuilds++
			obsMu.Unlock()
		})
		defer restoreIdx()

		streamer := &Streamer{
			Source:              pgEng,
			Target:              pgEng,
			SourceDSN:           src,
			TargetDSN:           tgt,
			StreamID:            streamID,
			TableParallelism:    4,
			BulkParallelMinRows: int64(rowsEach * 10),
			UpfrontIndexes:      upfront,
			// AnalyzeAfter rides the SAME argument pair through the same call;
			// it is advisory (per-table WARN, never fails), so it is set here
			// to prove the fast lane tolerates it end to end rather than to
			// assert an observable phase.
			AnalyzeAfter: upfront,
		}
		streamCtx, streamCancel := context.WithCancel(context.Background())
		defer streamCancel()
		runErr := make(chan error, 1)
		go func() { runErr <- streamer.Run(streamCtx) }()

		for i := 0; i < tableCount; i++ {
			name := fmt.Sprintf("tbl_%02d", i)
			if !waitForExactRowCount(tgt, name, rowsEach, 3*time.Minute) {
				t.Fatalf("cold-start copy never delivered %d rows to %s (got %d)", rowsEach, name, pollRowCount(tgt, name))
			}
		}
		// Every secondary index must exist either way — upfront relocates the
		// phase, it never skips it (and never double-builds: a clashing
		// CREATE INDEX would have failed the copy above).
		idxCtx, idxCancel := context.WithTimeout(context.Background(), time.Minute)
		defer idxCancel()
		assertAllSecondaryIndexesPresent(idxCtx, t, tgt, tableCount)

		streamCancel()
		select {
		case <-runErr:
		case <-time.After(60 * time.Second):
			t.Fatal("streamer did not unwind after cancel")
		}

		obsMu.Lock()
		defer obsMu.Unlock()
		return overlappedBuilds, fastLane
	}

	t.Run("--upfront-indexes relocates the phase off the overlapped builder", func(t *testing.T) {
		builds, fastLane := run(t, true, "coldstart-parallel-upfront-on")
		if !fastLane {
			t.Fatal("the ADR-0079 fast lane was not taken — a serial fall-back also produces zero overlapped builds, so the assertion below would prove nothing")
		}
		if builds != 0 {
			t.Errorf("--upfront-indexes: the ADR-0077 overlapped per-table builder fired %d time(s); with the flag honoured "+
				"the phase runs as the whole-schema CreateIndexes BEFORE the copy and the overlapped pool never runs. "+
				"The flag did not reach the fast lane (the item-111 phase-2 gap).", builds)
		}
	})

	t.Run("control: the default takes the overlapped builder on the same fixture", func(t *testing.T) {
		builds, fastLane := run(t, false, "coldstart-parallel-upfront-off")
		if !fastLane {
			t.Fatal("the ADR-0079 fast lane was not taken; the control does not exercise the path under test")
		}
		if builds == 0 {
			t.Error("default: the ADR-0077 overlapped builder never fired, so `builds == 0` is not evidence of anything " +
				"in the upfront case above — the two settings must be distinguishable on this fixture")
		}
	})
}
