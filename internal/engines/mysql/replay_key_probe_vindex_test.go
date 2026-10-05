// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
)

// TestPrimaryVindexSupplied_EveryShape grades the routing half of the
// sharded-keyspace replay rule without a cluster: a key collides on a
// re-sent row only when the row routes to the same shard, which needs every
// primary-vindex column supplied. The live measurement is
// TestVStream_ProbeReplayKey_UnsuppliedPrimaryVindex.
func TestPrimaryVindexSupplied_EveryShape(t *testing.T) {
	carried := map[string]bool{"email": true, "tenant": true, "v": true}
	cases := []struct {
		name    string
		primary []string
		want    bool
	}{
		{"single supplied column", []string{"email"}, true},
		{"supplied, different case", []string{"EMAIL"}, true},
		{"multi-column, all supplied", []string{"tenant", "email"}, true},
		{"unsupplied sequence-backed sid", []string{"sid"}, false},
		{"multi-column, one unsupplied", []string{"tenant", "sid"}, false},
		{"no primary vindex", nil, true},
	}
	for _, c := range cases {
		if got := primaryVindexSupplied(c.primary, carried); got != c.want {
			t.Errorf("%s: primaryVindexSupplied(%v) = %v, want %v", c.name, c.primary, got, c.want)
		}
	}
}

// TestReplayRoutesBySuppliedVindex_NoOpOffVtgate pins the posture for every
// non-vtgate target: no vtgate config means no per-shard enforcement, so the
// routing rule never narrows the verdict (and never touches the database).
func TestReplayRoutesBySuppliedVindex_NoOpOffVtgate(t *testing.T) {
	w := &RowWriter{schema: "app"}
	ok, err := w.replayRoutesBySuppliedVindex(context.Background(), "t", map[string]bool{})
	if err != nil || !ok {
		t.Fatalf("replayRoutesBySuppliedVindex off vtgate = (%v, %v), want (true, nil)", ok, err)
	}
}
