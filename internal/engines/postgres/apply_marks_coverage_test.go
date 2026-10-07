// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_MarksCoverReason_NekiRefusesBeforeAnyRead is ADR-0191 Q5
// on a Postgres target: a PlanetScale Neki target never covers a keyless
// replay — the cross-shard-group atomicity of a mark and its rows is an
// unverified premise there — and it says so without reading the target (the
// applier here has no database at all). Broader than Q5's "sharded Neki"
// (§13 R7): the engine knows a target is Neki, not whether it is sharded.
func TestChangeApplier_MarksCoverReason_NekiRefusesBeforeAnyRead(t *testing.T) {
	a := &ChangeApplier{isNeki: true}
	for _, table := range []*ir.Table{nil, {Name: "kl"}} {
		why, err := a.MarksCoverReason(context.Background(), table)
		if err != nil || !strings.Contains(why, "Neki") {
			t.Errorf("MarksCoverReason on Neki (table %v) = (%q, %v); want a Neki reason", table, why, err)
		}
	}
}
