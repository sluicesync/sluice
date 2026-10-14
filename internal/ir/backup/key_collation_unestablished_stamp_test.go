// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"fmt"
	"testing"
)

// TestStampKeyCollationUnestablished grades the stamp's rule over every kind
// spelling × carried × every tier it could start from: only an incremental
// (a CDC segment, the only manifest kind that records a SchemaHistory) whose
// history records a key column with a collation not established for its
// rows is raised, to exactly [FormatVersionKeyCollationUnestablished], and
// nothing else moves — a full stays where it is even when carried. The
// expected value is the rule stated here, not the function's output.
func TestStampKeyCollationUnestablished(t *testing.T) {
	startingVersions := []int{
		FormatVersionLegacy, FormatVersionSecurityMetadata, FormatVersionCDCPositionBinding,
		FormatVersionInjectiveChunkAAD, FormatVersionRedaction, FormatVersionPositionlessFull,
		FormatVersionExactNumbers, FormatVersionKeyCollationDelta, FormatVersionKeyCollationUnestablished,
	}
	raised := 0
	for _, kind := range []string{"", BackupKindFull, BackupKindIncremental} {
		for _, carried := range []bool{false, true} {
			for _, before := range startingVersions {
				m := &Manifest{Kind: kind, FormatVersion: before}
				StampKeyCollationUnestablished(m, carried)
				want := before
				if carried && kind == BackupKindIncremental {
					want = FormatVersionKeyCollationUnestablished
					raised++
				}
				if m.FormatVersion != want {
					t.Errorf("%s: FormatVersion = %d; want %d",
						fmt.Sprintf("kind=%q/carried=%v/from=%d", kind, carried, before), m.FormatVersion, want)
				}
			}
		}
	}
	if raised == 0 {
		t.Fatal("no cell expected a raise — the matrix is vacuous")
	}
	StampKeyCollationUnestablished(nil, true) // must not panic
}
