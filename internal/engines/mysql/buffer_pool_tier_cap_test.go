// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// TestBufferPoolParallelismCap is the change-detector pin for the ADR-0116
// Part-B buffer-pool tier buckets. Each PlanetScale tier's live-measured
// @@innodb_buffer_pool_size (2026 large-scale program: PS-10 0.125 GB →
// PS-160 9.80 GB) must land in the bucket the ADR documents, the boundary
// values must map to the higher bucket (the buckets are half-open
// [lo, hi)), and an unreadable size (0/negative) must be a no-op (cap 0).
//
// A boundary edit changes the expected values here, forcing a deliberate
// reviewed change — the project's pinned-threshold discipline.
func TestBufferPoolParallelismCap(t *testing.T) {
	const gib = int64(1) << 30
	const mib = int64(1) << 20

	tests := []struct {
		name  string
		bytes int64
		want  int
	}{
		// Unreadable / absent — the strict no-op sentinel.
		{name: "zero (unreadable) ⇒ no cap", bytes: 0, want: 0},
		{name: "negative (defensive) ⇒ no cap", bytes: -1, want: 0},

		// BELOW the PS-10 floor ⇒ not a tier reading on this flavor, so no
		// cap rather than the tightest cap. This function is only reached
		// on PlanetScale, where 128 MiB is the smallest pool any measured
		// tier reports; a value under it is the probe not reaching the
		// tablet, not a smaller instance. Field-observed 2026-08-04: a
		// PS-160 branch returned 32 MiB and was throttled to parallelism 2.
		{name: "1 byte ⇒ implausible, no cap", bytes: 1, want: 0},
		{name: "32 MiB (the field report) ⇒ implausible, no cap", bytes: 32 * mib, want: 0},
		{name: "just under the PS-10 floor ⇒ no cap", bytes: 128*mib - 1, want: 0},

		// Smallest bucket (< 256 MB ⇒ cap 2), floor-inclusive. PS-10 =
		// 0.125 GB = 128 MB, and a bare-minimum self-hosted dev MySQL
		// (128 MB default) both land here.
		{name: "PS-10 (0.125 GB / 128 MB)", bytes: 134217728, want: bufferPoolCapSmall},
		{name: "just under 256 MB", bytes: 256*mib - 1, want: bufferPoolCapSmall},

		// Boundary 256 MB ⇒ medium bucket (half-open: 256 MB is NOT < 256 MB).
		{name: "exactly 256 MB ⇒ medium", bytes: 256 * mib, want: bufferPoolCapMedium},

		// Medium bucket (< 2 GB ⇒ cap 4). PS-20 (0.83 GB) and PS-40 (1.64 GB),
		// expressed in bytes so the constant conversion is exact.
		{name: "PS-20 (~0.83 GB)", bytes: 891289600, want: bufferPoolCapMedium},
		{name: "PS-40 (~1.64 GB)", bytes: 1761607680, want: bufferPoolCapMedium},
		{name: "just under 2 GB", bytes: 2*gib - 1, want: bufferPoolCapMedium},

		// Boundary 2 GB ⇒ large bucket.
		{name: "exactly 2 GB ⇒ large", bytes: 2 * gib, want: bufferPoolCapLarge},

		// Large bucket (< 8 GB ⇒ cap 6). PS-80 = ~4.91 GB.
		{name: "PS-80 (~4.91 GB)", bytes: 5272799232, want: bufferPoolCapLarge},
		{name: "just under 8 GB", bytes: 8*gib - 1, want: bufferPoolCapLarge},

		// Boundary 8 GB ⇒ xlarge bucket.
		{name: "exactly 8 GB ⇒ xlarge", bytes: 8 * gib, want: bufferPoolCapXLarge},

		// XLarge bucket (>= 8 GB ⇒ cap 8). PS-160 = ~9.80 GB, and any
		// larger tier / sizeable self-hosted box.
		{name: "PS-160 (~9.80 GB)", bytes: 10522669056, want: bufferPoolCapXLarge},
		{name: "128 GB self-hosted", bytes: 128 * gib, want: bufferPoolCapXLarge},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := bufferPoolParallelismCap(tc.bytes); got != tc.want {
				t.Errorf("bufferPoolParallelismCap(%d) = %d, want %d", tc.bytes, got, tc.want)
			}
		})
	}
}

// TestBufferPoolBucketCapsMonotonic guards the invariant the ADR relies
// on: caps are non-decreasing as the buffer pool (≈ tier ≈ CPU) grows.
// A future boundary/cap edit that accidentally inverts the ordering
// (a bigger tier getting a TIGHTER cap) would be a real bug — pin it.
func TestBufferPoolBucketCapsMonotonic(t *testing.T) {
	if bufferPoolCapSmall > bufferPoolCapMedium ||
		bufferPoolCapMedium > bufferPoolCapLarge ||
		bufferPoolCapLarge > bufferPoolCapXLarge {
		t.Errorf("tier caps must be non-decreasing: small=%d medium=%d large=%d xlarge=%d",
			bufferPoolCapSmall, bufferPoolCapMedium, bufferPoolCapLarge, bufferPoolCapXLarge)
	}
	if bufferPoolBucketSmallBytes >= bufferPoolBucketMediumBytes ||
		bufferPoolBucketMediumBytes >= bufferPoolBucketLargeBytes {
		t.Errorf("tier boundaries must be strictly increasing: small=%d medium=%d large=%d",
			bufferPoolBucketSmallBytes, bufferPoolBucketMediumBytes, bufferPoolBucketLargeBytes)
	}
}

// TestBufferPoolTierCap_ImplausibleReadingDoesNotThrottleTheCopy is the
// end-to-end half of the plausibility floor: the change-detector above pins
// the bucket function, this pins what the operator actually experiences —
// the resolved copy budget.
//
// The case is the 2026-08-04 field report: a PlanetScale branch whose plan
// tier the ADR-0116 table measures at 9.80 GB reported
// @@innodb_buffer_pool_size = 32 MiB. Pre-fix that bucketed to the tightest
// cap and the copy ran 4x narrower than the instance could carry, with
// nothing in the output naming the cause.
//
// The assertion is deliberately "not throttled to the tier cap" rather than
// an exact number, so it survives a change to the connection-derived budget
// arithmetic — the defect is the CAP being applied to a reading that cannot
// be a tier, not any particular budget value.
func TestBufferPoolTierCap_ImplausibleReadingDoesNotThrottleTheCopy(t *testing.T) {
	probe := connectionBudgetProbe{
		maxConnections:  1000,
		inUse:           6,
		roleLimit:       unlimited,
		bufferPoolBytes: 32 << 20, // what the PS-160 branch actually returned
	}
	got := computeConnectionBudget(probe, connBudgetReserve, true /* PlanetScale flavor */)

	if got.tierCap != 0 {
		t.Errorf("tierCap = %d; want 0 — a reading below the smallest known tier is the probe not "+
			"reaching the tablet, not evidence of a tiny instance", got.tierCap)
	}
	if got.CopyBudget <= bufferPoolCapSmall {
		t.Fatalf("CopyBudget = %d; want well above the tightest tier cap of %d.\n\n"+
			"This is the field-reported defect: an implausible @@innodb_buffer_pool_size bucketed to the "+
			"smallest tier and throttled a large instance's copy, silently.",
			got.CopyBudget, bufferPoolCapSmall)
	}

	// The control: a PLAUSIBLE small reading must still cap. The floor must
	// not have disabled the heuristic altogether — a real PS-10 is exactly
	// the instance the cap exists to protect.
	probe.bufferPoolBytes = bufferPoolPlanetScaleFloorBytes
	if capped := computeConnectionBudget(probe, connBudgetReserve, true); capped.CopyBudget != bufferPoolCapSmall {
		t.Errorf("a real PS-10 reading resolved CopyBudget = %d; want the tier cap %d — the floor must "+
			"reject implausible readings, not the heuristic", capped.CopyBudget, bufferPoolCapSmall)
	}
}
