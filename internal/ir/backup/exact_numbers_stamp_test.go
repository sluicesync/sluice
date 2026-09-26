// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"fmt"
	"testing"
)

// TestStampExactNumbers grades the stamp's rule over every kind spelling ×
// carried × every tier it could start from: only an incremental (a CDC
// segment — the only manifest kind whose chunks are change chunks) that
// carried an exact number is raised, to exactly [FormatVersionExactNumbers],
// and nothing else moves. The expected value is the rule stated below, not
// the function's output.
func TestStampExactNumbers(t *testing.T) {
	startingVersions := []int{
		FormatVersionLegacy, FormatVersionSecurityMetadata, FormatVersionCDCPositionBinding,
		FormatVersionInjectiveChunkAAD, FormatVersionRedaction, FormatVersionPositionlessFull,
		FormatVersionExactNumbers,
	}
	raised := 0
	for _, kind := range []string{"", BackupKindFull, BackupKindIncremental} {
		for _, carried := range []bool{false, true} {
			for _, before := range startingVersions {
				m := &Manifest{Kind: kind, FormatVersion: before}
				StampExactNumbers(m, carried)
				want := before
				if carried && kind == BackupKindIncremental {
					want = FormatVersionExactNumbers
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
	StampExactNumbers(nil, true) // must not panic
}
