// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// verdictSource is a binlog source whose CDC reader answers the stream-less
// position check ([ir.ResumeVerifier]) with verdict — or has no check at all
// (noCheck), or cannot be opened (openErr).
type verdictSource struct {
	*ukDoorEngine
	verdict error
	noCheck bool
	openErr error
}

func (e *verdictSource) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	switch {
	case e.openErr != nil:
		return nil, e.openErr
	case e.noCheck:
		return uncheckedReader{}, nil
	}
	return verdictReader{verdict: e.verdict}, nil
}

type uncheckedReader struct{}

func (uncheckedReader) StreamChanges(context.Context, ir.Position) (<-chan ir.Change, error) {
	return nil, errors.New("verdictSource: the stamp gate must not stream")
}

type verdictReader struct {
	uncheckedReader
	verdict error
}

func (r verdictReader) VerifyResumable(context.Context, ir.Position) error { return r.verdict }

// resumeGateApplier is the control row the gate reads and writes: the
// liveness record, and the scope door's pending add-tables.
type resumeGateApplier struct {
	*scopeDriftApplier
	*fakeLivenessStore
}

// TestWarmResumeStampWaitsForTheSourcesCheck pins the two halves of the
// deferred attempt-start stamp on a warm resume (the fourth fix-aimed review
// of the drained-add-table heartbeat, G2). A target on the same MySQL server
// as its source logs sluice's control-row writes as source GTID
// transactions, so the stamp must not be written before the source's
// position check passes: a failing check writes nothing (the stamp waits for
// the open, or for the automatic re-snapshot's fall-through), an unopenable
// reader fails closed the same way, and a passing check stamps and then
// re-asks the scope door — a drained add written between the door's first
// ask and the stamp read the stop the stamp just replaced, so the start must
// defer to it.
func TestWarmResumeStampWaitsForTheSourcesCheck(t *testing.T) {
	ctx := context.Background()
	src, tgt := binlogPair([]string{"kd", "nt"}, []string{"kd"})
	pos := ir.Position{Engine: "mysql", Token: "pos-1"}
	// A drained add of nt, begun after the door's first ask.
	freshAdd := ir.PendingAddedTable{Table: "nt", Token: "t", StartedAt: time.Now().Add(-time.Minute), HeartbeatAt: time.Now().Add(-5 * time.Second)}

	newGate := func(source *verdictSource, pending ...ir.PendingAddedTable) (*Streamer, resumeGateApplier) {
		source.ukDoorEngine = src
		s := driftStreamer(t, source, tgt, "kd")
		s.liveness = &runLiveness{ctx: ctx}
		s.attemptStampDeferred = true
		app := recorded([]string{"kd"})
		app.pending = pending
		return s, resumeGateApplier{scopeDriftApplier: app, fakeLivenessStore: &fakeLivenessStore{value: 0}}
	}

	t.Run("passing check: stamps, then defers to an add begun since the door's first ask", func(t *testing.T) {
		s, app := newGate(&verdictSource{}, freshAdd)
		err := s.stampBeforeWarmResume(ctx, app, "st", pos)
		if !errors.Is(err, ir.ErrIncludeTableAddInProgress) {
			t.Fatalf("want INCLUDE-TABLE-ADD-IN-PROGRESS from the scope door's second ask, got %v", err)
		}
		if got := app.snapshot(); !slices.Equal(got, []string{"stamp"}) {
			t.Fatalf("liveness writes = %v; want the one attempt-start stamp before the door re-ran", got)
		}
	})
	t.Run("passing check, nothing pending: stamps and proceeds", func(t *testing.T) {
		s, app := newGate(&verdictSource{})
		if err := s.stampBeforeWarmResume(ctx, app, "st", pos); err != nil {
			t.Fatalf("want the resume to proceed, got %v", err)
		}
		if got := app.snapshot(); !slices.Equal(got, []string{"stamp"}) || s.attemptStampDeferred {
			t.Fatalf("liveness writes = %v, still deferred = %v; want one stamp and nothing left deferred", got, s.attemptStampDeferred)
		}
		if err := s.stampDeferredNow(ctx, app, "st"); err != nil || len(app.snapshot()) != 1 {
			t.Fatalf("the post-open stamp wrote again after a pre-open stamp: %v, writes %v", err, app.snapshot())
		}
	})
	t.Run("reader without a check: stamps before the open as before", func(t *testing.T) {
		s, app := newGate(&verdictSource{noCheck: true})
		if err := s.stampBeforeWarmResume(ctx, app, "st", pos); err != nil {
			t.Fatalf("want the resume to proceed, got %v", err)
		}
		if got := app.snapshot(); !slices.Equal(got, []string{"stamp"}) {
			t.Fatalf("liveness writes = %v; want one stamp", got)
		}
	})

	notResumable := []struct {
		name   string
		source *verdictSource
	}{
		{"failing check (purged position)", &verdictSource{verdict: fmt.Errorf("binlog purged: %w", ir.ErrPositionInvalid)}},
		{"failing check (foreign lineage)", &verdictSource{verdict: fmt.Errorf("replaced: %w", ir.ErrPositionForeignLineage)}},
		{"unopenable reader (fails closed)", &verdictSource{openErr: errors.New("dial tcp: connection refused")}},
	}
	for _, tc := range notResumable {
		t.Run(tc.name+": writes nothing before the open, stamps at the fall-through", func(t *testing.T) {
			// The pending add is present so a door run here would refuse:
			// nothing may run, the stream open reports the failure.
			s, app := newGate(tc.source, freshAdd)
			if err := s.stampBeforeWarmResume(ctx, app, "st", pos); err != nil {
				t.Fatalf("want no pre-open verdict (the open reports it), got %v", err)
			}
			if got := app.snapshot(); len(got) != 0 || !s.attemptStampDeferred {
				t.Fatalf("liveness writes = %v, still deferred = %v; want nothing written and the stamp still deferred", got, s.attemptStampDeferred)
			}
			// The automatic re-snapshot's fall-through (or the open, had it
			// passed) makes the deferred stamp, once.
			if err := s.stampDeferredNow(ctx, app, "st"); err != nil {
				t.Fatal(err)
			}
			if err := s.stampDeferredNow(ctx, app, "st"); err != nil {
				t.Fatal(err)
			}
			if got := app.snapshot(); !slices.Equal(got, []string{"stamp"}) || s.attemptStampDeferred {
				t.Fatalf("liveness writes = %v, still deferred = %v; want exactly one deferred stamp", got, s.attemptStampDeferred)
			}
		})
	}
}
