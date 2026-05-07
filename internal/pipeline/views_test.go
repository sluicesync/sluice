// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestNewViewFilter_MutualExclusion mirrors the table-filter check:
// supplying both Include and Exclude is rejected up front.
func TestNewViewFilter_MutualExclusion(t *testing.T) {
	_, err := NewViewFilter([]string{"v1"}, []string{"v2"})
	if err == nil {
		t.Fatal("expected mutual-exclusion error; got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("err = %v; want mutual-exclusion message", err)
	}
}

// TestNewViewFilter_RejectsBadPattern covers the malformed-glob path.
func TestNewViewFilter_RejectsBadPattern(t *testing.T) {
	_, err := NewViewFilter(nil, []string{"[unclosed"})
	if err == nil {
		t.Fatal("expected error on malformed pattern; got nil")
	}
}

// TestViewFilter_Allows covers the include / exclude / empty paths.
func TestViewFilter_Allows(t *testing.T) {
	cases := []struct {
		name    string
		include []string
		exclude []string
		view    string
		want    bool
	}{
		{"empty filter allows all", nil, nil, "anything", true},
		{"include exact match", []string{"v1"}, nil, "v1", true},
		{"include miss", []string{"v1"}, nil, "v2", false},
		{"include glob", []string{"audit_*"}, nil, "audit_log", true},
		{"include glob miss", []string{"audit_*"}, nil, "users", false},
		{"exclude exact", nil, []string{"v1"}, "v1", false},
		{"exclude pass", nil, []string{"v1"}, "v2", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			f, err := NewViewFilter(c.include, c.exclude)
			if err != nil {
				t.Fatalf("NewViewFilter: %v", err)
			}
			if got := f.Allows(c.view); got != c.want {
				t.Errorf("Allows(%q) = %v; want %v", c.view, got, c.want)
			}
		})
	}
}

// TestApplyViewFilter_SkipsAll verifies SkipViews=true drops every
// view regardless of filter content.
func TestApplyViewFilter_SkipsAll(t *testing.T) {
	s := &ir.Schema{Views: []*ir.View{
		{Name: "v1"},
		{Name: "v2"},
	}}
	applyViewFilter(context.Background(), s, ViewFilter{}, true)
	if len(s.Views) != 0 {
		t.Errorf("expected views cleared; got %d", len(s.Views))
	}
}

// TestApplyViewFilter_FiltersByPattern verifies the include path
// keeps only matching views.
func TestApplyViewFilter_FiltersByPattern(t *testing.T) {
	s := &ir.Schema{Views: []*ir.View{
		{Name: "audit_log"},
		{Name: "users"},
		{Name: "audit_archive"},
	}}
	f, err := NewViewFilter([]string{"audit_*"}, nil)
	if err != nil {
		t.Fatalf("NewViewFilter: %v", err)
	}
	applyViewFilter(context.Background(), s, f, false)
	if len(s.Views) != 2 {
		t.Errorf("expected 2 views after audit_* include, got %d", len(s.Views))
	}
	for _, v := range s.Views {
		if !strings.HasPrefix(v.Name, "audit_") {
			t.Errorf("unexpected view kept: %s", v.Name)
		}
	}
}

// TestApplyViewFilter_EmptyResultIsOK verifies that filtering down to
// zero views is NOT an error (unlike applyTableFilter which rejects
// the all-empty case). Many schemas have no views; filtering all of
// them away is a legitimate operator choice.
func TestApplyViewFilter_EmptyResultIsOK(t *testing.T) {
	s := &ir.Schema{Views: []*ir.View{{Name: "v1"}}}
	f, err := NewViewFilter([]string{"nomatch"}, nil)
	if err != nil {
		t.Fatalf("NewViewFilter: %v", err)
	}
	applyViewFilter(context.Background(), s, f, false)
	if len(s.Views) != 0 {
		t.Errorf("expected empty result; got %d views", len(s.Views))
	}
}

// viewWriterStub records CreateViews calls and lets a test inject a
// per-call error policy. Used to exercise runViewsPhase's retry
// logic without standing up a real engine.
type viewWriterStub struct {
	// failOnce records a set of view names that should fail on their
	// first CreateViews call but succeed on the next. Used to verify
	// the retry-on-failure path.
	failOnce map[string]bool

	// alwaysFail names views that fail on every call. Used to verify
	// the abandon-after-N-passes path.
	alwaysFail map[string]bool

	// callLog records the (view name, attempt-number) for each
	// CreateViews invocation; tests verify the retry sequence.
	callLog []string
}

func (w *viewWriterStub) CreateTablesWithoutConstraints(_ context.Context, _ *ir.Schema) error {
	return nil
}
func (w *viewWriterStub) CreateIndexes(_ context.Context, _ *ir.Schema) error     { return nil }
func (w *viewWriterStub) CreateConstraints(_ context.Context, _ *ir.Schema) error { return nil }
func (w *viewWriterStub) SyncIdentitySequences(_ context.Context, _ *ir.Schema) error {
	return nil
}

func (w *viewWriterStub) CreateViews(_ context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("nil schema")
	}
	for _, v := range s.Views {
		w.callLog = append(w.callLog, v.Name)
		if w.alwaysFail[v.Name] {
			return errors.New("always-fail view: " + v.Name)
		}
		if w.failOnce[v.Name] {
			delete(w.failOnce, v.Name)
			return errors.New("first-attempt fail: " + v.Name)
		}
	}
	return nil
}

// TestRunViewsPhase_NoOp verifies the phase is a no-op on schemas
// without any views.
func TestRunViewsPhase_NoOp(t *testing.T) {
	w := &viewWriterStub{}
	if err := runViewsPhase(context.Background(), &ir.Schema{}, w); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(w.callLog) != 0 {
		t.Errorf("expected no CreateViews calls on empty schema; got %d", len(w.callLog))
	}
}

// TestRunViewsPhase_Success verifies the happy path: every view
// succeeds on the first try, no retries needed.
func TestRunViewsPhase_Success(t *testing.T) {
	w := &viewWriterStub{}
	s := &ir.Schema{Views: []*ir.View{
		{Name: "v1"},
		{Name: "v2"},
	}}
	if err := runViewsPhase(context.Background(), s, w); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(w.callLog) != 2 {
		t.Errorf("expected 2 CreateViews calls; got %d (%v)", len(w.callLog), w.callLog)
	}
}

// TestRunViewsPhase_RetryOnFailure verifies a view that fails on its
// first attempt succeeds on the retry pass. This is the dependency-
// ordering case: view A depends on view B declared later, so A's
// first attempt fails before B exists, but A's retry after B's
// successful create succeeds.
func TestRunViewsPhase_RetryOnFailure(t *testing.T) {
	w := &viewWriterStub{
		failOnce: map[string]bool{"v_dependent": true},
	}
	s := &ir.Schema{Views: []*ir.View{
		{Name: "v_dependent"}, // declared first; depends on v_base
		{Name: "v_base"},      // declared second; v_dependent's prereq
	}}
	if err := runViewsPhase(context.Background(), s, w); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Expected sequence: v_dependent (fail), v_base (succeed),
	// v_dependent retry (succeed). Three calls total.
	if len(w.callLog) < 3 {
		t.Errorf("expected at least 3 CreateViews calls (one retry); got %v", w.callLog)
	}
}

// TestRunViewsPhase_MaxPassesError verifies the orchestrator surfaces
// a clear error when a view still fails after the maximum retry
// budget is exhausted.
func TestRunViewsPhase_MaxPassesError(t *testing.T) {
	w := &viewWriterStub{
		alwaysFail: map[string]bool{"broken": true},
	}
	s := &ir.Schema{Views: []*ir.View{
		{Name: "broken"},
	}}
	err := runViewsPhase(context.Background(), s, w)
	if err == nil {
		t.Fatal("expected error after retry budget exhaustion; got nil")
	}
	if !strings.Contains(err.Error(), "broken") {
		t.Errorf("err = %v; want mention of failing view name", err)
	}
	if !strings.Contains(err.Error(), "still failing") {
		t.Errorf("err = %v; want mention of retry budget", err)
	}
}

// TestRunViewsPhase_NoProgressBailsEarly verifies the phase doesn't
// burn the full retry budget on a no-progress pass — if no view
// succeeds in a given pass, the next iteration is the last so the
// caller gets the accumulated error promptly.
func TestRunViewsPhase_NoProgressBailsEarly(t *testing.T) {
	w := &viewWriterStub{
		alwaysFail: map[string]bool{"a": true, "b": true},
	}
	s := &ir.Schema{Views: []*ir.View{
		{Name: "a"},
		{Name: "b"},
	}}
	err := runViewsPhase(context.Background(), s, w)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	// Two views fail every attempt; the orchestrator should bail
	// after the first no-progress pass plus the error-recording pass.
	// Accept up to 2 * len(views) * 2 = 8 calls as a reasonable
	// upper bound but want the actual count to be smaller.
	if len(w.callLog) > 6 {
		t.Errorf("too many retry calls (%d); expected early-bail behavior", len(w.callLog))
	}
}
