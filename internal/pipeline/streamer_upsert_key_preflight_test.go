// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the Bug-211 upsert-key preflight PHASE.
//
// These prove the plumbing only — that the phase probes the optional
// surface, hands it the stream's table scope, propagates the engine's
// coded refusal without re-classifying it, and is a silent no-op for an
// applier that doesn't implement the surface. Whether the refusal is
// CORRECT is the engine's business and is pinned against a real Postgres
// (internal/engines/postgres/change_applier_deferrable_key_integration_test.go);
// whether the phase actually runs inside a real sync is pinned by
// deferrable_key_pg_integration_test.go in this package. A unit test
// alone would prove a function, not a path.

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// upsertPreflightApplier implements the optional surface and records the
// scope predicate it was handed.
type upsertPreflightApplier struct {
	ir.ChangeApplier
	err error

	called   bool
	inScope  func(string) bool
	nilScope bool
}

func (a *upsertPreflightApplier) PreflightUpsertKeys(_ context.Context, inScope func(table string) bool) error {
	a.called = true
	a.inScope = inScope
	a.nilScope = inScope == nil
	return a.err
}

// upsertPreflightlessApplier deliberately does NOT implement the surface
// — the shape every non-Postgres target has today.
type upsertPreflightlessApplier struct {
	ir.ChangeApplier
}

func TestPhasePreflightUpsertKeys_PropagatesTheCodedRefusal(t *testing.T) {
	want := sluicecode.Wrap(
		sluicecode.CodeTargetDeferrableKey,
		"make the target constraint immediate",
		errors.New("postgres: target table(s) public.dpk (constraint dpk_pk)"),
	)
	applier := &upsertPreflightApplier{err: want}
	s := &Streamer{}

	err := s.phasePreflightUpsertKeys(context.Background(), applier)
	if err == nil {
		t.Fatal("phase returned nil; the engine's refusal was swallowed")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeTargetDeferrableKey {
		t.Fatalf("refusal lost or re-classified on the way out: %v", err)
	}
	if coded.Hint != "make the target constraint immediate" {
		t.Errorf("remedy hint rewritten: %q", coded.Hint)
	}
}

func TestPhasePreflightUpsertKeys_PassesTheStreamTableScope(t *testing.T) {
	applier := &upsertPreflightApplier{}
	s := &Streamer{Filter: migcore.TableFilter{Include: []string{"orders"}}}

	if err := s.phasePreflightUpsertKeys(context.Background(), applier); err != nil {
		t.Fatalf("phase: %v", err)
	}
	if !applier.called {
		t.Fatal("phase never called the preflight")
	}
	if applier.nilScope {
		t.Fatal("phase handed a nil scope predicate; a table outside --include-table would be refused over")
	}
	if !applier.inScope("orders") {
		t.Error("in-scope table reported out of scope")
	}
	if applier.inScope("sessions") {
		t.Error("out-of-scope table reported in scope — the refusal would fire on tables the stream never touches")
	}
}

func TestPhasePreflightUpsertKeys_NoSurfaceIsASilentNoOp(t *testing.T) {
	s := &Streamer{}
	if err := s.phasePreflightUpsertKeys(context.Background(), &upsertPreflightlessApplier{}); err != nil {
		t.Fatalf("an applier without the surface must skip silently; got %v", err)
	}
}
