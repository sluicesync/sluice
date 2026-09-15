// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// idleIndexAxisSW is an [ir.IncrementalIndexBuilder] that returns a bare
// context cancellation IMMEDIATELY, without ever reading the channel — the
// shape the real PG builder takes when the schema carries no index jobs and
// the context is already done (schema_writer_index_overlap.go, the
// totalJobs == 0 drain). Returning it unconditionally is what makes the
// errOnce ordering deterministic here; in production it is a race the index
// axis wins because it has nothing to unwind.
type idleIndexAxisSW struct {
	*fakeIndexBuilderSW
}

func (s *idleIndexAxisSW) BuildTableIndexesFromChannel(context.Context, *ir.Schema, <-chan *ir.Table) error {
	return context.Canceled
}

// TestAttributeOverlappedFailure pins which axis names a failed overlapped
// phase, including the case errgroup's first-to-return ordering gets wrong.
//
// TESTFRAGILE-3: on Postgres the overlapped branch runs both axes under one
// errgroup, and when the cancellation arrives from OUTSIDE the group (a
// deadline, a Ctrl-C, a stall watchdog) both axes see it at the same
// instant. With zero index jobs the index axis returns a bare ctx.Err() on
// its next select while the copy axis is still unwinding database calls, so
// it wins errOnce essentially every time — and every such run was labelled
// `create indexes` regardless of where it was actually blocked.
func TestAttributeOverlappedFailure(t *testing.T) {
	t.Parallel()

	realCopyErr := errors.New(`pipeline: copy table "events": connection reset`)

	t.Run("idle index axis does not name the failure; the copy error does", func(t *testing.T) {
		t.Parallel()
		attr := attributeOverlappedFailure(indexAxisError{err: context.Canceled}, realCopyErr, 0)
		if attr.hint != migcore.PhaseBulkCopy || attr.phase != ir.MigrationPhaseBulkCopy {
			t.Errorf("phase = %q/%q; want bulk-copy — an index axis that was handed nothing and saw only "+
				"the shared cancellation must not name the failure", attr.hint, attr.phase)
		}
		if !errors.Is(attr.err, realCopyErr) {
			t.Errorf("surfaced %v; want the copy axis's own error, which is the only one that says where "+
				"the run actually was", attr.err)
		}
		if strings.Contains(attr.err.Error(), "create indexes") {
			t.Errorf("the error still reads as an index failure: %v", attr.err)
		}
	})

	t.Run("idle index axis with no copy error says the index axis was idle", func(t *testing.T) {
		t.Parallel()
		attr := attributeOverlappedFailure(indexAxisError{err: context.DeadlineExceeded}, nil, 0)
		if attr.phase != ir.MigrationPhaseBulkCopy {
			t.Errorf("phase = %q; want bulk-copy", attr.phase)
		}
		if !errors.Is(attr.err, context.DeadlineExceeded) {
			t.Errorf("the cancellation was dropped: %v", attr.err)
		}
		if !strings.Contains(attr.err.Error(), "index axis idle") {
			t.Errorf("the message does not say the index axis was idle, so it still reads as an index "+
				"stall: %v", attr.err)
		}
	})

	t.Run("index axis that was handed work still names the failure", func(t *testing.T) {
		t.Parallel()
		attr := attributeOverlappedFailure(indexAxisError{err: context.Canceled}, realCopyErr, 1)
		if attr.hint != migcore.PhaseIndexes || attr.phase != ir.MigrationPhaseIndexes {
			t.Errorf("phase = %q/%q; want indexes — an axis that was mid-build when the cancellation "+
				"landed is a truthful attribution", attr.hint, attr.phase)
		}
		if !strings.Contains(attr.err.Error(), "pipeline: create indexes:") {
			t.Errorf("error lacks the index-phase prefix: %v", attr.err)
		}
	})

	t.Run("a real index error names the failure even with nothing queued", func(t *testing.T) {
		t.Parallel()
		wall := errors.New("Error 3024: maximum statement execution time exceeded")
		attr := attributeOverlappedFailure(indexAxisError{err: wall}, realCopyErr, 0)
		if attr.hint != migcore.PhaseIndexes {
			t.Errorf("hint = %q; want indexes — only CONTEXT errors are reclassified, or the errno-3024 "+
				"hint this attribution was built for is lost again", attr.hint)
		}
		if !errors.Is(attr.err, wall) {
			t.Errorf("surfaced %v; want the walled index error", attr.err)
		}
	})

	t.Run("a copy-axis failure is unchanged", func(t *testing.T) {
		t.Parallel()
		attr := attributeOverlappedFailure(realCopyErr, realCopyErr, 3)
		if attr.hint != migcore.PhaseBulkCopy || !errors.Is(attr.err, realCopyErr) {
			t.Errorf("phase = %q err = %v; want bulk-copy with the copy error", attr.hint, attr.err)
		}
	})
}

// TestOverlapPhase_IdleIndexAxisDoesNotNameTheFailure drives the real
// orchestrator rather than the attribution function, so the WIRING is
// graded too: copyErr has to be captured and indexJobsQueued has to be
// counted at the handoff, or this fails even while
// attributeOverlappedFailure is itself correct.
func TestOverlapPhase_IdleIndexAxisDoesNotNameTheFailure(t *testing.T) {
	schema := overlapTestSchema(6)
	sw := &idleIndexAxisSW{fakeIndexBuilderSW: &fakeIndexBuilderSW{}}
	state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}
	var stateMu sync.Mutex
	eng := &errAfterNEngine{failAt: 1, rowsEach: 2}

	err := runOverlappedCopyAndIndexPhase(
		context.Background(), resumeContext{enabled: false}, state, &stateMu, schema,
		&maybeErrReader{rowsEach: 2, fail: true}, sw, newPoolFakeWriter(&concurrencyGauge{}, 0), sw,
		false, 0, &parallelBulkCopyDeps{source: eng, target: eng, parallelism: 1},
		4, nil, ShardColumnSpec{},
	)
	if err == nil {
		t.Fatal("expected the phase to fail")
	}
	if strings.Contains(err.Error(), "pipeline: create indexes:") {
		t.Errorf("an index axis that returned only a cancellation, with nothing ever queued to it, named "+
			"the failure — this is the mis-attribution that sent TESTFRAGILE-3's investigation at a phase "+
			"that had no work in it:\n%v", err)
	}
}
