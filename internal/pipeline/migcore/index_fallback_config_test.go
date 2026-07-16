// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeIndexFallbackSetter records the fallback threaded through the
// optional ir.IndexBuildFallbackSetter surface.
type fakeIndexFallbackSetter struct {
	got ir.IndexBuildFallback
}

func (f *fakeIndexFallbackSetter) SetIndexBuildFallback(fb ir.IndexBuildFallback) { f.got = fb }

// fakeIndexBuildFallback is an inert ir.IndexBuildFallback for the seam
// pin.
type fakeIndexBuildFallback struct{}

func (fakeIndexBuildFallback) BuildIndexDDL(context.Context, string, []string, error) error {
	return nil
}

// TestApplyIndexBuildFallback pins the ADR-0148 threading seam: the
// helper routes through the optional setter, no-ops on non-implementers
// (PG/SQLite writers), and never calls the setter for a nil fallback (the
// zero-value construction every programmatic caller gets).
func TestApplyIndexBuildFallback(t *testing.T) {
	t.Run("routes to setter", func(t *testing.T) {
		target := &fakeIndexFallbackSetter{}
		fb := fakeIndexBuildFallback{}
		ApplyIndexBuildFallback(target, fb)
		if target.got != ir.IndexBuildFallback(fb) {
			t.Errorf("SetIndexBuildFallback got %v; want the threaded fallback", target.got)
		}
	})
	t.Run("non-setter target no-ops", func(_ *testing.T) {
		type plain struct{}
		ApplyIndexBuildFallback(plain{}, fakeIndexBuildFallback{}) // must not panic
	})
	t.Run("nil fallback never touches the setter", func(t *testing.T) {
		target := &fakeIndexFallbackSetter{got: fakeIndexBuildFallback{}}
		ApplyIndexBuildFallback(target, nil)
		if target.got == nil {
			t.Error("nil fallback overwrote the setter (should be a no-op)")
		}
	})
}
