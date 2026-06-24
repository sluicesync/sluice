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
func bufferPoolParallelismCap(bufferPoolBytes int64) int {
	switch {
	case bufferPoolBytes <= 0:
		return 0 // unreadable ⇒ cap not applied (no-op).
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
