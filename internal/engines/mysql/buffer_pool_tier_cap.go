// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// ADR-0116 Part B — the no-credential buffer-pool CPU/tier proxy.
//
// On PlanetScale, connections are abundant (vtgate fronts a large shared
// pool — `conns=6/250` observed during the large-scale program), so a
// connection-slot budget alone bounds the WRONG resource. The scarce
// resource on a small PlanetScale tier is CPU (a PS-10 is 1/8 vCPU and
// pins at 100% under a wide cold copy). sluice has no credential-free way
// to read a PlanetScale branch's CPU allocation directly — but
// @@innodb_buffer_pool_size scales MONOTONICALLY by plan tier and is NOT
// masked by vtgate. Live-measured PS-10 → PS-160 (2026, the large-scale
// program):
//
//	PS-10    0.125 GB   (134217728 bytes)
//	PS-20    0.83  GB
//	PS-40    1.64  GB
//	PS-80    4.91  GB
//	PS-160   9.80  GB
//
// So @@innodb_buffer_pool_size is a usable no-credential proxy for "how
// big is this instance" → a defensible parallelism CAP. The buckets below
// are deliberately coarse and conservative; the metrics-aware clamp
// (ADR-0115 / ADR-0107) is the robust always-correct path WHEN telemetry
// is configured — this cap is the credential-free heuristic that applies
// when it is not, plus a harmless safe upper bound on self-hosted MySQL
// (where buffer pool likewise correlates with box size).
//
// The cap is folded into the returned connection budget via the MIN of
// the connection-derived CopyBudget and this tier cap (see
// computeConnectionBudget). It can only LOWER parallelism, never raise it,
// and is a strict no-op when the size can't be read.

// Buffer-pool bucket boundaries (bytes) and their parallelism caps. The
// boundaries are pinned as named constants with the live tier data above
// as their justification, and exercised by a change-detector unit test
// (TestBufferPoolParallelismCap) so a boundary edit is a deliberate,
// reviewed change — the project's pinned-threshold discipline.
//
// Rationale for each boundary:
//
//   - 256 MB: above the PS-10 buffer pool (0.125 GB = 128 MB), so PS-10
//     (the smallest tier, 1/8 vCPU) buckets to the tightest cap of 2.
//     A bare-minimum self-hosted dev MySQL (the 128 MB default) also
//     lands here, which is correct — a tiny box should not be fanned out
//     wide.
//   - 2 GB: spans PS-20 (0.83 GB) and PS-40 (1.64 GB) — the small paid
//     tiers — to a moderate cap of 4.
//   - 8 GB: spans PS-80 (4.91 GB) to a cap of 6.
//   - >= 8 GB: PS-160 (9.80 GB) and every larger tier / sizeable
//     self-hosted box get the full cap of 8 (sluice's general
//     parallelism ceiling elsewhere, e.g. indexBuildConcurrencyHardCap).
const (
	bufferPoolBucketSmallBytes  = 256 << 20 // 256 MiB
	bufferPoolBucketMediumBytes = 2 << 30   // 2 GiB
	bufferPoolBucketLargeBytes  = 8 << 30   // 8 GiB

	// bufferPoolPlanetScaleFloorBytes is PS-10's measured pool — the
	// SMALLEST value the tier table above records. On the PlanetScale
	// flavor a reading below it is not a smaller tier, it is a reading
	// that isn't reporting the tablet (field-observed 2026-08-04: a
	// PS-160 returning 32 MiB). See [bufferPoolParallelismCap].
	bufferPoolPlanetScaleFloorBytes = 128 << 20 // 128 MiB = PS-10

	bufferPoolCapSmall  = 2 // < 256 MB  (PS-10-class / tiny dev box)
	bufferPoolCapMedium = 4 // < 2 GB    (PS-20 / PS-40-class)
	bufferPoolCapLarge  = 6 // < 8 GB    (PS-80-class)
	bufferPoolCapXLarge = 8 // >= 8 GB   (PS-160+ / sizeable self-hosted)
)

// bufferPoolParallelismCap maps @@innodb_buffer_pool_size (bytes) to a
// copy-parallelism cap, the ADR-0116 Part-B no-credential CPU/tier proxy.
//
// A non-positive bufferPoolBytes (the size could not be read) returns 0,
// the "cap not applied" sentinel: the caller treats 0 as a no-op and the
// connection-derived budget stands unchanged. The cap NEVER fails — a
// missing reading is simply not a cap.
//
// # The plausibility floor, and the premise it protects
//
// This function is only reached on the PlanetScale flavor (see
// computeConnectionBudget's applyTierCap gate), where the whole heuristic
// rests on ONE environmental premise: that `SELECT @@innodb_buffer_pool_size`
// returns the tablet's real pool, monotonic by plan tier, with PS-10's
// 128 MiB as the floor.
//
// That premise has been observed to FAIL. A 2026-08-04 field report on a
// PS-160 branch — a tier the table above measures at 9.80 GB — read
// 32 MiB, which bucketed to the TIGHTEST cap of 2 instead of 8. The
// operator saw a copy running 4x narrower than their instance could carry,
// with nothing naming the cause. A reading below the PS-10 floor cannot be
// a real tier answer on this flavor, so it is treated as UNREADABLE rather
// than as evidence of a tiny instance. That direction is deliberate: an
// absent cap falls back to the connection-derived budget (which is a real
// slot bound), whereas a wrong tight cap silently throttles the copy and
// looks like sluice being slow.
//
// This is the runtime check the premise-naming rule asks for. Its cost if
// PlanetScale ever ships a tier genuinely below 128 MiB is that such a tier
// goes uncapped; that is visible in the log the caller emits, and is the
// safer of the two errors.
func bufferPoolParallelismCap(bufferPoolBytes int64) int {
	switch {
	case bufferPoolBytes <= 0:
		return 0 // unreadable ⇒ cap not applied (no-op).
	case bufferPoolBytes < bufferPoolPlanetScaleFloorBytes:
		return 0 // implausible for this flavor ⇒ not a tier reading.
	case bufferPoolBytes < bufferPoolBucketSmallBytes:
		return bufferPoolCapSmall
	case bufferPoolBytes < bufferPoolBucketMediumBytes:
		return bufferPoolCapMedium
	case bufferPoolBytes < bufferPoolBucketLargeBytes:
		return bufferPoolCapLarge
	default:
		return bufferPoolCapXLarge
	}
}
