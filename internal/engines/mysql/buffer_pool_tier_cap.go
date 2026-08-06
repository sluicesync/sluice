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
	// that is not reporting a plan tier at all: a PlanetScale DEV branch
	// returns 32 MiB regardless of the database's tier (MEASURED
	// 2026-08-05, and the explanation for the 2026-08-04 field report).
	// See [bufferPoolParallelismCap].
	bufferPoolPlanetScaleFloorBytes = 128 << 20 // 128 MiB = PS-10

	bufferPoolCapSmall  = 2 // < 256 MB  (PS-10-class / tiny dev box)
	bufferPoolCapMedium = 4 // < 2 GB    (PS-20 / PS-40-class)
	bufferPoolCapLarge  = 6 // < 8 GB    (PS-80-class)
	bufferPoolCapXLarge = 8 // >= 8 GB   (PS-160+ / sizeable self-hosted)

	// copyFanoutCeilingUnknownTier is the WRITE-side fan-out degree
	// ([ir.ConnectionBudget.CopyFanoutCeiling]) sluice will point at a
	// PlanetScale target whose buffer-pool reading is below
	// bufferPoolPlanetScaleFloorBytes — i.e. a target whose plan tier the
	// probe could NOT establish. Roadmap item 144.
	//
	// It is 2 because 2 is the degree that was MEASURED, twice, on exactly
	// that target class (2026-08-05, 1.7M rows / 918 MB, four in-scope
	// tables so the native-concurrent W x D path engages):
	//
	//	degree 4 (the default)  579 s, a connection-drop storm, 6 grow-gate
	//	                        windows, 75 drops, 28.7% of the wall clock
	//	                        quiesced
	//	degree 2                234.8 s, ZERO drops, ZERO gate windows
	//
	// Halving the fan-out made the copy 2.5x FASTER, and re-raising it on
	// the same target brought the storm back (an A/B/A, so "the target had
	// finished settling" is ruled out). More lanes were strictly worse.
	//
	// This constant governs ONLY the fan-out axis, and ONLY when the tier
	// is unknown. It is deliberately NOT a value returned from
	// [bufferPoolParallelismCap]: folding it in there would put a cap back
	// on the CONNECTION BUDGET for a sub-floor reading, which is precisely
	// the shape roadmap item 123 removed (a wrong tight budget throttles
	// migrate's whole table x within-table product). The two decisions
	// share the SAME predicate — see [copyFanoutCeiling] — so they cannot
	// drift apart, but they do different work.
	copyFanoutCeilingUnknownTier = 2
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
// That premise has been observed to FAIL, and the cause is now MEASURED
// rather than guessed. A 2026-08-04 field report on what the operator
// believed was a PS-160 branch — a tier the table above measures at
// 9.80 GB — read 32 MiB, which bucketed to the TIGHTEST cap of 2 instead
// of 8. The copy ran 4x narrower than the instance could carry, with
// nothing naming the cause.
//
// Reproduced against real PlanetScale on 2026-08-05, and the answer is
// that the premise holds for PRODUCTION branches and NOT for dev ones:
//
//	main, production, PS-10   134217728  (128 MiB)  8.4.6-Vitess
//	devbranch, non-production  33554432  ( 32 MiB)  8.4.9-Vitess
//
// A DEV branch reports a small fixed pool that does not track the
// database's plan tier — the same 32 MiB the field report saw, on a
// database whose production branch reports its tier correctly. The two
// branches report different Vitess versions (8.4.6 vs 8.4.9), so they are
// demonstrably not the same infrastructure; the mechanism beyond that was
// not investigated and is not claimed. Every measurement in the table above was
// taken on a production branch, which is why the exception went unnoticed.
//
// A reading below the PS-10 floor therefore cannot be a tier answer, and
// is treated as UNREADABLE rather than as evidence of a tiny instance.
// That direction is deliberate: an absent cap falls back to the
// connection-derived budget (which is a real slot bound), whereas a wrong
// tight cap silently throttles the copy and looks like sluice being slow.
//
// Note how close the boundary is: a real PS-10 returns EXACTLY the floor
// value, so it is still capped (the check is strictly-below). One byte
// higher and this would break the smallest tier it exists to protect.
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

// copyFanoutCeiling derives the WRITE-side copy fan-out ceiling
// ([ir.ConnectionBudget.CopyFanoutCeiling]) from the same probe reading
// [bufferPoolParallelismCap] buckets. Roadmap item 144.
//
// # The defect this closes
//
// Item 123 correctly stopped treating a sub-floor reading as evidence of a
// tiny instance — but "not a tier reading" then meant NO ceiling at all, so
// the cap failed OPEN. Measured on real PlanetScale (2026-08-05, confirmed
// again 2026-08-06 on a freshly-created PS-10): a production branch reports
// exactly the 128 MiB floor and a DEV branch on the same database reports
// 32 MiB, below every known tier. So the weakest target sluice can be
// pointed at was the one target it drove at the WIDEST write fan-out.
//
// # What this function claims, and what it does not
//
// The predicate is a statement about sluice's own tier table, not about
// anyone's platform: a reading below the smallest tier the table records
// cannot be placed on the tier scale, so the target's capacity is UNKNOWN.
// The policy for unknown capacity is a conservative fan-out — not the
// widest, and not the tightest-tier's budget cap (that is item 123's
// defect, and this function deliberately cannot cause it: it returns a
// FAN-OUT ceiling, never a connection budget).
//
// The VALUE, however, is calibrated on measurement rather than derived:
// see [copyFanoutCeilingUnknownTier]. Every sub-floor reading observed so
// far — 2026-08-04 (field report, a database the operator believed was
// PS-160), 2026-08-05 (PS-10, both branches), 2026-08-06 (a fresh PS-10,
// both branches) — came from a PlanetScale DEV branch reporting a small
// FIXED pool that does not track the database's plan tier, which is why
// the calibration runs were taken there.
//
// RESIDUAL, stated rather than implied: if some other target class ever
// reports sub-floor, this ceiling is conservative for it on no evidence.
// The cost of that error is a narrower copy, which is slower at worst and
// LOUD (computeConnectionBudget WARNs, naming the reading, the floor and
// the ceiling, and the pipeline logs the degree it resolved and why) —
// versus the cost of the opposite error, which is the measured drop storm.
//
// applyTierCap is computeConnectionBudget's PlanetScale-flavor gate: on
// vanilla MySQL and self-hosted Vitess the buffer pool is sized to the
// operator's own hardware and is not a tier signal at all, so no ceiling
// is declared. A completely unreadable probe (<= 0) likewise declares
// none — an absent reading is not evidence of anything.
//
// Pinned by TestCopyFanoutCeiling_* (including the item-123 control: a
// real PS-10 at EXACTLY the floor declares no ceiling).
func copyFanoutCeiling(bufferPoolBytes int64, applyTierCap bool) int {
	if !applyTierCap || bufferPoolBytes <= 0 {
		return 0
	}
	if bufferPoolParallelismCap(bufferPoolBytes) > 0 {
		// The probe placed this target on the plan-tier scale. Its tier cap
		// already bounds the connection budget; the fan-out axis is left to
		// the operator's --copy-fanout-degree, which is what the production
		// branch was measured clean at.
		return 0
	}
	return copyFanoutCeilingUnknownTier
}
