// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeSizeHintSetter records the per-table source-size map threaded through
// the optional ir.IndexSplitSizeHintSetter surface.
type fakeSizeHintSetter struct {
	got    map[string]int64
	called bool
}

func (f *fakeSizeHintSetter) SetIndexSplitSizeHint(m map[string]int64) {
	f.got = m
	f.called = true
}

// byteReader is an ir.RowReader that also supplies per-table byte sizes
// (ir.TableByteSizeEstimator). errOn names a table whose estimate errors.
type byteReader struct {
	bytes map[string]int64
	errOn string
}

func (byteReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) { return nil, nil }
func (byteReader) Err() error                                                 { return nil }

func (r byteReader) EstimateTableBytes(_ context.Context, t *ir.Table) (int64, error) {
	if t.Name == r.errOn {
		return 0, errors.New("boom")
	}
	return r.bytes[t.Name], nil
}

// noByteSizeReader is an ir.RowReader with NO byte-size surface.
type noByteSizeReader struct{}

func (noByteSizeReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) { return nil, nil }
func (noByteSizeReader) Err() error                                                 { return nil }

func schemaWith(names ...string) *ir.Schema {
	s := &ir.Schema{}
	for _, n := range names {
		s.Tables = append(s.Tables, &ir.Table{Name: n})
	}
	return s
}

// TestApplyIndexSplitSizeHint pins the ADR-0184 PlanetScale-leg plumbing seam:
// the orchestrator derives per-table SOURCE byte sizes and threads them to the
// target via the optional setter — and does so ONLY when both sides opt in,
// skipping zero/error estimates, so the writer's split keys off the source and
// never a target probe.
func TestApplyIndexSplitSizeHint(t *testing.T) {
	t.Run("threads source sizes, skipping zero and error tables", func(t *testing.T) {
		setter := &fakeSizeHintSetter{}
		rr := byteReader{bytes: map[string]int64{"big": 9 << 30, "empty": 0, "small": 4096}, errOn: "boom_tbl"}
		ApplyIndexSplitSizeHint(context.Background(), setter, rr, schemaWith("big", "empty", "small", "boom_tbl"))
		if !setter.called {
			t.Fatal("setter was not called; want the source-size map threaded")
		}
		want := map[string]int64{"big": 9 << 30, "small": 4096}
		if len(setter.got) != len(want) {
			t.Fatalf("hint map = %v; want %v (zero and error tables excluded)", setter.got, want)
		}
		for k, v := range want {
			if setter.got[k] != v {
				t.Errorf("hint[%q] = %d; want %d", k, setter.got[k], v)
			}
		}
		if _, ok := setter.got["empty"]; ok {
			t.Error("a zero-byte estimate must be excluded (0 = unknown, never empty)")
		}
		if _, ok := setter.got["boom_tbl"]; ok {
			t.Error("an errored estimate must be excluded, not fatal")
		}
	})

	t.Run("source without the byte surface never touches the setter", func(t *testing.T) {
		setter := &fakeSizeHintSetter{}
		ApplyIndexSplitSizeHint(context.Background(), setter, noByteSizeReader{}, schemaWith("t"))
		if setter.called {
			t.Error("setter called for a source that cannot estimate bytes; want no-op")
		}
	})

	t.Run("target without the setter surface no-ops", func(_ *testing.T) {
		rr := byteReader{bytes: map[string]int64{"t": 9 << 30}}
		ApplyIndexSplitSizeHint(context.Background(), struct{}{}, rr, schemaWith("t")) // must not panic
	})

	t.Run("all-zero source leaves the writer at its default", func(t *testing.T) {
		setter := &fakeSizeHintSetter{}
		rr := byteReader{bytes: map[string]int64{"a": 0, "b": 0}}
		ApplyIndexSplitSizeHint(context.Background(), setter, rr, schemaWith("a", "b"))
		if setter.called {
			t.Error("setter called with an all-zero (empty) map; want no-op so the writer keeps its safe default")
		}
	})
}
