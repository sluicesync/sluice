// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

type fakeDDLProber struct {
	err    error
	called int
}

func (f *fakeDDLProber) ProbeDirectDDL(context.Context) error {
	f.called++
	return f.err
}

// The preflight's whole value is that it acts on ONE of the two answers.
// A refusal is conclusive and must stop the run before the schema phase; a
// probe that could not run is not a verdict and must not become a new way
// for a working configuration to be refused.
func TestPreflightDirectDDL(t *testing.T) {
	t.Parallel()

	blocked := sluicecode.Wrap(
		sluicecode.CodePSDirectDDLBlocked,
		"disable safe migrations, or use deploy requests",
		errors.New("direct DDL is disabled"),
	)

	t.Run("the safe-migrations refusal stops the run", func(t *testing.T) {
		t.Parallel()
		p := &fakeDDLProber{err: blocked}
		err := PreflightDirectDDL(context.Background(), p, "migrate")
		if err == nil {
			t.Fatal("a target refusing every DDL was allowed to proceed to the schema phase — which is the whole " +
				"failure this preflight exists to move earlier")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodePSDirectDDLBlocked {
			t.Errorf("the refusal lost its code on the way out: %v", err)
		}
		if p.called != 1 {
			t.Errorf("probe called %d times, want 1", p.called)
		}
	})

	t.Run("a probe that could not RUN is not a verdict", func(t *testing.T) {
		t.Parallel()
		// A preflight that cannot complete must report no information. The
		// schema phase a moment later is still the correctness floor, and
		// turning an unreachable probe into a refusal would break working
		// configurations for no safety gain.
		for _, err := range []error{
			errors.New("connection reset by peer"),
			errors.New("Error 1142 (42000): CREATE command denied to user"),
			sluicecode.Wrap(sluicecode.CodeDriverHostMismatch, "unrelated", errors.New("some other coded failure")),
		} {
			p := &fakeDDLProber{err: err}
			if got := PreflightDirectDDL(context.Background(), p, "migrate"); got != nil {
				t.Errorf("probe failure %q became a refusal: %v", err, got)
			}
		}
	})

	t.Run("a PASS is silent — no clearance is claimed", func(t *testing.T) {
		t.Parallel()
		// Asymmetry, pinned: safe migrations propagates asynchronously to
		// each gateway, so an accepted DDL means "this gateway, now" and
		// nothing about the branch. The preflight must therefore return
		// nil and say nothing; the test pins the return, and the absence
		// of any "safe migrations is off" claim is enforced by there being
		// no such string to emit.
		p := &fakeDDLProber{}
		if err := PreflightDirectDDL(context.Background(), p, "sync cold-start"); err != nil {
			t.Errorf("an accepted probe produced an error: %v", err)
		}
		if p.called != 1 {
			t.Errorf("probe called %d times, want 1", p.called)
		}
	})

	t.Run("a writer that cannot probe is skipped entirely", func(t *testing.T) {
		t.Parallel()
		// Anti-vacuity for the cases above: every non-MySQL target reaches
		// here, so the skip must be the type assertion and not an accident
		// of the probe returning nil.
		if err := PreflightDirectDDL(context.Background(), struct{}{}, "migrate"); err != nil {
			t.Errorf("a writer with no probe was refused: %v", err)
		}
		if err := PreflightDirectDDL(context.Background(), nil, "migrate"); err != nil {
			t.Errorf("a nil writer was refused: %v", err)
		}
	})
}
