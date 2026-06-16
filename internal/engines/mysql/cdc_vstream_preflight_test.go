// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// Unit tests for the Bug 146 / ADR-0093 VStream purged-GTID pre-flight
// helpers. The live gtid_purged ⊆ resume behaviour (and the tablet-type
// routing of @@global.gtid_purged that makes it correct) is pinned by the
// real-cluster TestVitessCluster_PurgedGTID_ReactiveColdStart.

import (
	"context"
	"testing"

	"vitess.io/vitess/go/vt/proto/topodata"
)

func TestTabletTypeSuffix(t *testing.T) {
	cases := []struct {
		in   topodata.TabletType
		want string
	}{
		{topodata.TabletType_PRIMARY, "primary"},
		{topodata.TabletType_REPLICA, "replica"},
		{topodata.TabletType_RDONLY, "rdonly"},
		// Zero/unknown defaults to replica — the VStream CDC-tail default, so
		// the pre-flight probes the same tablet type the stream binds to.
		{topodata.TabletType_UNKNOWN, "replica"},
	}
	for _, c := range cases {
		if got := tabletTypeSuffix(c.in); got != c.want {
			t.Errorf("tabletTypeSuffix(%v) = %q; want %q", c.in, got, c.want)
		}
	}
}

// TestVerifyVStreamPositionReachable_NilCfg: a reader built without a DSN
// (direct-API / unit construction) has no connection to probe with, so the
// pre-flight is a clean no-op rather than a nil-deref or spurious refusal.
func TestVerifyVStreamPositionReachable_NilCfg(t *testing.T) {
	r := &vstreamCDCReader{cfg: nil, keyspace: "test", tabletType: topodata.TabletType_REPLICA}
	decoded := []shardGtid{{Keyspace: "test", Shard: "-", Gtid: "MySQL56/abc:1-10"}}
	if err := r.verifyVStreamPositionReachable(context.Background(), decoded); err != nil {
		t.Errorf("nil-cfg pre-flight = %v; want nil (no-op)", err)
	}
}
