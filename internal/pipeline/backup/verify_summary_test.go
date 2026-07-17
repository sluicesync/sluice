// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestVerifyFailureSummary pins the aggregate-line shape with chunk and
// signature failures counted SEPARATELY (KMS real-cloud validation nit,
// 2026-07-16): pre-split, a wrong-key verify of a signed 1-chunk chain
// folded the manifest + lineage signature failures into the chunk
// numerator and read "2 of 1 chunk(s) failed verification".
func TestVerifyFailureSummary(t *testing.T) {
	cases := []struct {
		name                          string
		total, chunkFailed, sigFailed int
		want                          string
		wantAbsent                    string
	}{
		{
			name:  "chunk-only keeps the historical line",
			total: 5, chunkFailed: 2, sigFailed: 0,
			want:       "verify: 2 of 5 chunk(s) failed verification",
			wantAbsent: "signature",
		},
		{
			name:  "signature-only names signatures, never a chunk numerator",
			total: 1, chunkFailed: 0, sigFailed: 2,
			// The exact wrong-key shape from the live KMS leg: 1 chunk,
			// manifest + lineage signature failures. Must NOT read
			// "2 of 1 chunk(s)".
			want:       "verify: 2 signature failure(s); all 1 chunk(s) passed verification",
			wantAbsent: "2 of 1 chunk(s)",
		},
		{
			name:  "both kinds report both counters",
			total: 3, chunkFailed: 1, sigFailed: 1,
			want: "verify: 1 of 3 chunk(s) failed verification; 1 signature failure(s)",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := verifyFailureSummary(c.total, c.chunkFailed, c.sigFailed).Error()
			if !strings.Contains(got, c.want) {
				t.Errorf("summary = %q; want it to contain %q", got, c.want)
			}
			if c.wantAbsent != "" && strings.Contains(got, c.wantAbsent) {
				t.Errorf("summary = %q; must not contain %q", got, c.wantAbsent)
			}
		})
	}
}

// TestAggregateVerifyError_CodingUnchangedBySplit pins that separating
// the counters did not move the Bug-185 coding: chunk corruption /
// auth failures keep their coded Refusals, and a signature-only
// failure stays UNCODED (the signature refusal is reported
// per-manifest) while now reading as signature failures.
func TestAggregateVerifyError_CodingUnchangedBySplit(t *testing.T) {
	err := aggregateVerifyError(verifyScanTally{total: 3, chunkFailed: 1, sawCorrupt: true})
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupChunkCorrupt {
		t.Errorf("sawCorrupt: want %s; got %v", sluicecode.CodeBackupChunkCorrupt, err)
	}

	err = aggregateVerifyError(verifyScanTally{total: 3, chunkFailed: 1, sawAuth: true})
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupChunkAuthFailed {
		t.Errorf("sawAuth: want %s; got %v", sluicecode.CodeBackupChunkAuthFailed, err)
	}

	err = aggregateVerifyError(verifyScanTally{total: 1, sigFailed: 2})
	if err == nil {
		t.Fatal("signature-only failure: want a non-nil aggregate error")
	}
	if _, ok := sluicecode.FromError(err); ok {
		t.Errorf("signature-only failure must stay uncoded (pre-Bug-185 shape); got %v", err)
	}
	if !strings.Contains(err.Error(), "2 signature failure(s)") {
		t.Errorf("signature-only aggregate should count signatures; got %v", err)
	}
}
