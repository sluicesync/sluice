// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_MarksCoverReason_SidecarRefusesBeforeAnyRead is the
// vtgate-MULTI row of ADR-0191 §3.5 on a MySQL-family target: with a
// `--control-keyspace` sidecar, a mark and its rows commit on different
// shards and a tear leaves rows WITHOUT their marks by design (GC-41 (c)), so
// no keyless replay is covered — said without reading the target (the applier
// here has no database at all).
func TestChangeApplier_MarksCoverReason_SidecarRefusesBeforeAnyRead(t *testing.T) {
	a := &ChangeApplier{controlKeyspace: "ctl"}
	for _, table := range []*ir.Table{nil, {Name: "kl"}} {
		why, err := a.MarksCoverReason(context.Background(), table)
		if err != nil || !strings.Contains(why, "--control-keyspace") {
			t.Errorf("MarksCoverReason with a sidecar (table %v) = (%q, %v); want the sidecar reason", table, why, err)
		}
	}
}
