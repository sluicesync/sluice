// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"strings"
	"testing"
)

// TestWatermarkStalled_OnlyTheUnexplainedShape pins GC-43 (a)'s tripwire to
// the one no-progress shape that has no legitimate cause. The other two —
// a hole and a DDL marker — are expected stops with their own handling, and
// an empty or fully consumed window is not a stop at all.
func TestWatermarkStalled_OnlyTheUnexplainedShape(t *testing.T) {
	cases := []struct {
		name string
		b    pollBatch
		want bool
	}{
		{"empty window", pollBatch{lastID: 7, seenTo: 7}, false},
		{"window fully consumed", pollBatch{lastID: 20, seenTo: 20, consumed: 13}, false},
		{"hole stops the run", pollBatch{lastID: 9, seenTo: 20, holeAt: 10, holeEnd: 11}, false},
		{"DDL marker stops the run", pollBatch{lastID: 9, seenTo: 10, ddl: &ddlMarker{tag: "ALTER TABLE"}}, false},
		{"rows read, no gap, watermark left behind", pollBatch{lastID: 0, seenTo: 10000, consumed: 10000}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := watermarkStalled(tc.b); got != tc.want {
				t.Fatalf("watermarkStalled(%+v) = %v, want %v", tc.b, got, tc.want)
			}
		})
	}

	msg := refuseStalledWatermark(pollBatch{lastID: 3, seenTo: 10003}).Error()
	for _, want := range []string{watermarkStalledMarker, "10003", "stayed at 3", "WHERE id > 3"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal %q does not name %q", msg, want)
		}
	}
}
