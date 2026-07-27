// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Reachability pins for the `--where` drift contract (Bug 209, found by the
// v0.103.0 regression cycle).
//
// The contract was widened to "every source engine" and shipped INERT for
// every engine except Postgres, because the check lived inside
// phaseResolvePublicationScope — which opens with a type assertion on
// ir.PublicationScoper and returns nil when it fails. Postgres is the only
// implementer. The inner engine gate was removed; the enclosing one was never
// looked at.
//
// The unit matrix that was supposed to cover this called the drift predicate
// DIRECTLY. It passed on every cell, and could not have failed, because it
// never went through the phase. Pinning a predicate proves the predicate;
// only driving the phase proves the contract.
//
// So these tests call the PHASE, with a source that is deliberately NOT a
// PublicationScoper — the shape every MySQL, Vitess, SQLite and trigger-CDC
// stream has.
package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// driftApplier records the hash written back and serves a recorded row.
type driftApplier struct {
	ir.ChangeApplier
	streams   []ir.StreamStatus
	hashSet   []string
	listCalls int
}

func (a *driftApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	a.listCalls++
	return a.streams, nil
}
func (a *driftApplier) SetRowFilterHash(h string) { a.hashSet = append(a.hashSet, h) }

// nonScoperEngine is a source engine WITHOUT ir.PublicationScoper — i.e. every
// engine except Postgres. stubEngine panics on unexpected calls, which is what
// we want: the drift phase must not touch the source beyond the type
// assertions.
type nonScoperEngine struct{ stubEngine }

func TestPhaseCheckRowFilterDrift_RunsForANonPublicationSource(t *testing.T) {
	ctx := context.Background()
	established := map[string]string{"orders": "region = 'EU'"}
	recorded := rowFilterFullHash(established)

	newStreamer := func(filters map[string]string) *Streamer {
		return &Streamer{Source: nonScoperEngine{}, RowFilters: filters}
	}

	t.Run("changed predicate is REFUSED", func(t *testing.T) {
		applier := &driftApplier{streams: []ir.StreamStatus{{StreamID: "sid", RowFilterHash: recorded}}}
		s := newStreamer(map[string]string{"orders": "region != 'US'"})

		err := s.phaseCheckRowFilterDrift(ctx, applier, "sid")
		if err == nil {
			t.Fatal("a MySQL-family stream warm-resumed with a CHANGED --where and was accepted. The target " +
				"still holds what the ORIGINAL predicate copied while the CDC leg classifies under the new one: " +
				"a narrowed filter strands rows forever, a widened one never backfills. Silent either way " +
				"(Bug 209 — the contract shipped inert for every non-Postgres engine).")
		}
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeWherePushdownDrift {
			t.Errorf("refusal carries %v (matched=%v); want %v", ce, ok, sluicecode.CodeWherePushdownDrift)
		}
		if !strings.Contains(err.Error(), "orders") {
			t.Errorf("refusal does not name the filtered table; got: %v", err)
		}
	})

	t.Run("REMOVED predicate is refused", func(t *testing.T) {
		applier := &driftApplier{streams: []ir.StreamStatus{{StreamID: "sid", RowFilterHash: recorded}}}
		s := newStreamer(nil)
		if err := s.phaseCheckRowFilterDrift(ctx, applier, "sid"); err == nil {
			t.Fatal("removing --where entirely on a warm resume was accepted; the stream widens to the full " +
				"table and never backfills the rows the filtered cold start skipped")
		}
	})

	t.Run("unchanged predicate resumes and records the hash", func(t *testing.T) {
		applier := &driftApplier{streams: []ir.StreamStatus{{StreamID: "sid", RowFilterHash: recorded}}}
		s := newStreamer(established)
		if err := s.phaseCheckRowFilterDrift(ctx, applier, "sid"); err != nil {
			t.Fatalf("an UNCHANGED predicate was refused — the false-positive direction, which would strand an "+
				"operator who did nothing wrong: %v", err)
		}
		if len(applier.hashSet) != 1 || applier.hashSet[0] != recorded {
			t.Errorf("hash not recorded for the next run: %v", applier.hashSet)
		}
	})

	t.Run("brand-new stream records without refusing", func(t *testing.T) {
		applier := &driftApplier{streams: nil}
		s := newStreamer(established)
		if err := s.phaseCheckRowFilterDrift(ctx, applier, "sid"); err != nil {
			t.Fatalf("a new stream was refused: %v", err)
		}
		if len(applier.hashSet) != 1 {
			t.Errorf("a new stream must record its hash so the NEXT resume can compare: %v", applier.hashSet)
		}
	})

	// The phase must actually consult the store — a version that never read
	// the recorded row would pass every case above by doing nothing.
	t.Run("the phase reads the control row", func(t *testing.T) {
		applier := &driftApplier{streams: []ir.StreamStatus{{StreamID: "sid", RowFilterHash: recorded}}}
		s := newStreamer(established)
		if err := s.phaseCheckRowFilterDrift(ctx, applier, "sid"); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
		if applier.listCalls == 0 {
			t.Error("the drift phase never read the stream's control row, so it cannot be comparing anything")
		}
	})
}
