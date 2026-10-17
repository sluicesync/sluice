// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestUniqueKeyBoundaryHook_SchemaIntercepts pins that the schema intercepts
// that forward a snapshot — forward and refuse here, Shape A by the same
// call at both of its snapshot sends — judge it through the unique-key
// boundary hook first, and withhold it when the hook stops the stream. The
// hook replaced a downstream channel stage, which held earlier changes past
// the DDL the forward intercept had already applied (round-3 review).
func TestUniqueKeyBoundaryHook_SchemaIntercepts(t *testing.T) {
	snap := addColForwardSnap(addColForwardTable("users"))
	for _, verdict := range []bool{true, false} {
		var judged []string
		hook := func(_ context.Context, s ir.SchemaSnapshot) bool {
			judged = append(judged, s.Table)
			return verdict
		}
		want := 0
		if verdict {
			want = 1
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		in := make(chan ir.Change, 1)
		in <- snap
		close(in)
		out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{applier: &fakeShapeApplier{}, uniqueKeys: hook}, &atomic.Pointer[error]{})
		if got := drainChannel(t, out, 5*time.Second); len(got) != want || strings.Join(judged, ",") != "users" {
			t.Errorf("forward, verdict %v: forwarded %d, judged %v", verdict, len(got), judged)
		}
		cancel()

		judged = nil
		got, err := runRefuseIntercept(t, unforwardedBoundaryDeps{why: "--schema-changes=refuse", uniqueKeys: hook}, snap)
		if err != nil || len(got) != want || strings.Join(judged, ",") != "users" {
			t.Errorf("refuse, verdict %v: forwarded %d, judged %v, err %v", verdict, len(got), judged, err)
		}
	}
}

// TestPendingDrop pins the boundary check's deferral (round-3 review): a
// target key on a column the boundary snapshot still has but the source
// catalog no longer does is a DROP COLUMN the stream has not reached yet,
// deferred to that DROP's own boundary. A column the snapshot lacks too (the
// DROP being judged), one the catalog still has, an expression part, and a
// check with no snapshot columns (the open door) are never deferred.
func TestPendingDrop(t *testing.T) {
	source := &ir.Table{Name: "kd", Columns: []*ir.Column{{Name: "id"}, {Name: "k"}}} // doomed already dropped in the catalog
	key := func(parts ...ir.IndexColumn) *ir.Index { return &ir.Index{Name: "x", Unique: true, Columns: parts} }
	cases := []struct {
		name string
		key  *ir.Index
		pos  map[string]bool
		want bool
	}{
		{"the snapshot still has doomed", key(ir.IndexColumn{Column: "doomed"}), map[string]bool{"id": true, "k": true, "doomed": true}, true},
		{"composite with a pending column", key(ir.IndexColumn{Column: "k"}, ir.IndexColumn{Column: "Doomed"}), map[string]bool{"k": true, "doomed": true}, true},
		{"the DROP's own boundary", key(ir.IndexColumn{Column: "doomed"}), map[string]bool{"id": true, "k": true}, false},
		{"a column the catalog has", key(ir.IndexColumn{Column: "k"}), map[string]bool{"k": true}, false},
		{"an expression part", key(ir.IndexColumn{Expression: "lower(doomed)"}), map[string]bool{"doomed": true}, false},
		{"no snapshot columns (the open door)", key(ir.IndexColumn{Column: "doomed"}), nil, false},
	}
	for _, c := range cases {
		if got := pendingDrop(c.key, source, c.pos); got != c.want {
			t.Errorf("%s: pendingDrop = %v, want %v", c.name, got, c.want)
		}
	}
}
