// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The reactive door (audit 2026-09-09 A0909-MYSQL-HIGH-1): before the
// reactive re-snapshot sets RestartFromScratch — the step that drops the
// target's in-scope tables — it asks the source reader whether it is
// still the lineage the persisted position came from. Pinned with stubs
// because the real thing needs a replaced instance; the real-server twin
// is TestStreamer_MySQLForeignLineage_RefusesTheAutomaticRecopy.

// lineageStubReader is a CDC reader that answers VerifyLineage with a
// canned verdict and refuses to stream.
type lineageStubReader struct{ verdict error }

func (r lineageStubReader) StreamChanges(context.Context, ir.Position) (<-chan ir.Change, error) {
	return nil, errors.New("lineageStubReader: StreamChanges must not be called by the door")
}
func (r lineageStubReader) VerifyLineage(context.Context, ir.Position) error { return r.verdict }

var _ ir.LineageVerifier = lineageStubReader{}

// lineageStubSource hands out the stub reader; every other surface is unused.
type lineageStubSource struct {
	resnapshotTargetEngine
	reader ir.CDCReader
	opened int
}

func (s *lineageStubSource) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	s.opened++
	return s.reader, nil
}

// lineageStubTarget's applier reports a persisted position for the stream.
type lineageStubTarget struct{ resnapshotTargetEngine }

type positionedApplier struct{ resnapshotApplier }

func (positionedApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{Engine: "mysql", Token: `{"mode":"gtid","gtid_set":"a:1-9"}`}, true, nil
}

func (lineageStubTarget) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return positionedApplier{}, nil
}

func TestReactiveResnapshot_RefusesAForeignLineageBeforeTheDrop(t *testing.T) {
	foreign := fmt.Errorf("mysql: the source is a different lineage: %w", ir.ErrPositionForeignLineage)

	run := func(t *testing.T, verdict error) (*Streamer, int, error) {
		t.Helper()
		src := &lineageStubSource{reader: lineageStubReader{verdict: verdict}}
		s := &Streamer{
			StreamID:  "test-stream",
			Source:    src,
			Target:    lineageStubTarget{},
			SourceDSN: "src",
			TargetDSN: "tgt",
		}
		calls := 0
		s.runOnceFn = func(context.Context) error {
			calls++
			if calls == 1 {
				return invalidPositionErr() // the reactive trigger
			}
			return nil
		}
		err := s.runOnceWithReactiveResnapshot(context.Background())
		return s, calls, err
	}

	t.Run("foreign verdict: terminal, RestartFromScratch never set, no second attempt", func(t *testing.T) {
		s, calls, err := run(t, foreign)
		if err == nil {
			t.Fatal("a foreign lineage must be terminal; the reactive path re-ran instead")
		}
		if !errors.Is(err, ir.ErrPositionForeignLineage) || !strings.Contains(err.Error(), foreignLineageMarker) {
			t.Fatalf("refusal must carry the sentinel and the %s marker: %v", foreignLineageMarker, err)
		}
		if errors.Is(err, ir.ErrPositionInvalid) {
			t.Fatalf("the refusal must NOT read as an invalid position (that is the auto-recopy route): %v", err)
		}
		if s.RestartFromScratch {
			t.Fatal("RestartFromScratch was set on a foreign lineage — the next attempt would drop the target")
		}
		if calls != 1 {
			t.Fatalf("runOnce called %d times; want exactly 1 (no re-copy attempt)", calls)
		}
		for _, want := range []string{"Nothing on the target was touched", "--restart-from-scratch", "--reset-target-data"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal does not say %q: %v", want, err)
			}
		}
	})

	t.Run("same lineage: the recovery proceeds exactly as before", func(t *testing.T) {
		s, calls, err := run(t, nil)
		if err != nil {
			t.Fatalf("same lineage: %v; want the re-snapshot to run", err)
		}
		if !s.RestartFromScratch || calls != 2 {
			t.Fatalf("RestartFromScratch=%v calls=%d; want true and 2 — the ADR-0093 recovery must be unchanged "+
				"when the source is the same lineage", s.RestartFromScratch, calls)
		}
	})

	t.Run("a probe the reader could not complete does not block the recovery", func(t *testing.T) {
		s, _, err := run(t, errors.New("mysql: GTID_SUBSET: connection reset"))
		if err != nil || !s.RestartFromScratch {
			t.Fatalf("err=%v RestartFromScratch=%v; a failed probe must WARN and proceed, never block", err, s.RestartFromScratch)
		}
	})

	t.Run("a reader without the capability is not asked", func(t *testing.T) {
		src := &lineageStubSource{reader: nil}
		src.reader = plainStubReader{}
		s := &Streamer{StreamID: "test-stream", Source: src, Target: lineageStubTarget{}, SourceDSN: "src", TargetDSN: "tgt"}
		calls := 0
		s.runOnceFn = func(context.Context) error {
			calls++
			if calls == 1 {
				return invalidPositionErr()
			}
			return nil
		}
		if err := s.runOnceWithReactiveResnapshot(context.Background()); err != nil || !s.RestartFromScratch {
			t.Fatalf("err=%v RestartFromScratch=%v; an engine without LineageVerifier keeps the old behaviour", err, s.RestartFromScratch)
		}
	})
}

type plainStubReader struct{}

func (plainStubReader) StreamChanges(context.Context, ir.Position) (<-chan ir.Change, error) {
	return nil, errors.New("unused")
}

// The warm-resume framing: a foreign verdict from the pre-flight is
// wrapped as the pipeline's refusal and keeps both sentinels' truth.
func TestForeignLineageRefusalError_KeepsTheSentinelAndNamesTheWayOut(t *testing.T) {
	t.Parallel()
	err := foreignLineageRefusalError(fmt.Errorf("mysql: different lineage: %w", ir.ErrPositionForeignLineage))
	if !errors.Is(err, ir.ErrPositionForeignLineage) || errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("framing changed the verdict: %v", err)
	}
	for _, want := range []string{foreignLineageMarker, "REFUSED the automatic re-snapshot", "--restart-from-scratch", "--reset-target-data"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("framing lacks %q: %v", want, err)
		}
	}
}
