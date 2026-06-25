# ADR-0115: Metrics-aware headroom clamp on restore parallelism

- Status: Accepted
- Date: 2026-06-24
- Deciders: sluice maintainers
- Relates: ADR-0112 (restore within-table chunk parallelism), ADR-0084 (cross-table restore pool), ADR-0076 (two-axis copy budget split), ADR-0107 (PlanetScale target telemetry), ADR-0106 (apply-concurrency headroom clamp)

## Context

`sluice restore` fans out on two axes: cross-table (`--table-parallelism`, ADR-0084) and within-table chunk (`--bulk-parallelism`, ADR-0112), both auto-by-default. Their product is bounded at the connection-budget chokepoint (`resolveCopyParallelismBudget`, ADR-0076) — **but only for engines with a `TargetConnectionBudgetProber` (Postgres)**. A MySQL/PlanetScale target has no prober, so the auto product passes through **unbounded**.

On PlanetScale that's the wrong bound anyway: connections are abundant (vtgate fronts a large pool — `conns=6/250` observed during Track C), so connections are *not* the scarce resource. **CPU is** — a PS-10 (1/8 vCPU) pinned at CPU 1.000 during the Track-C restore A/B. The connection-budget split can't see CPU, so nothing throttled the auto fan-out against a hot small-tier instance.

The CDC apply path already solved the analogous problem: `clampConcurrencyByHeadroom` (ADR-0107 Phase 3 / ADR-0106) reduces the auto apply-lane count by the target's live CPU/mem headroom at startup. Restore had no equivalent.

## Decision

**Apply the same headroom clamp to restore's auto parallelism product.**

1. **Extract the shared threshold logic.** A new `headroomDivisor(ctx, ir.TargetTelemetry) → (divisor, busiestUtil, ok)` is the single source of the `{1,2,4}` reduction thresholds (healthy → 1, approaching the high-water `0.70` → 2, at/over `DefaultTelemetryHighWater` → 4). `clampConcurrencyByHeadroom` is refactored to call it (behaviour-preserving — the existing apply-path tests stay green), and the restore clamp calls it too, so the two paths can never disagree on what "tight" means.

2. **Clamp the restore product.** `Restore` gains an optional `TargetTelemetry ir.TargetTelemetry` (wired from the `restore` command's `--planetscale-*` flags, mirroring `sync start`). After `resolveRestoreParallelism` resolves the budget-bounded `table × chunk`, `clampRestoreParallelismByHeadroom` reduces the product ~divisor-fold when headroom is tight — the cross-table axis absorbs the reduction first (preserving each table's within-table chunk fan-out), falling through to the within-table axis only if cross-table can't absorb it alone.

## Correctness / safety

- **Advisory, telemetry-gated, degrades to a no-op.** No provider, a stale snapshot, or neither CPU nor mem observed ⇒ inputs returned unchanged — the pre-ADR-0115 behaviour, byte-for-byte. Restore correctness (disjoint chunk partition → no PK collisions; per-chunk SHA-256; layer-2 row-count) is untouched; this only changes *how wide* the fan-out starts.
- **Never raises; never below 1.** The clamp only reduces, and floors each axis at 1.
- **Respects explicit operator intent.** Only an AUTO axis (the flag left at 0) is reduced; an explicitly-pinned `--table-parallelism` / `--bulk-parallelism` is never clamped, and when both are pinned the clamp is a no-op (mirrors the apply path, where an explicit `--apply-concurrency` never reaches the clamp).
- **PlanetScale-correct.** CPU/mem-driven, the bound that actually matters on a PlanetScale target; complements (does not replace) the connection-budget split that still bounds prober-equipped engines.

## Consequences

- An auto restore into a hot PlanetScale-class target starts more conservatively instead of piling the full fan-out onto a saturated small-tier instance; the per-worker reparent-retry (ADR-0108) + grow-gate (ADR-0110) remain the reactive floor on top.
- One-time startup bias only (like the apply-path clamp): restore resolves parallelism once per run, so there's no mid-run re-partition concern.
- Engine-general: the clamp is in the pipeline resolver; no engine code changed.

### Residual

Restore is a fixed-fan-out-per-run operation, so the clamp is startup-only — it cannot widen back if headroom recovers mid-restore (acceptable: a restore is bounded-duration and the reactive retries absorb transient pressure). A dynamic re-partition would be a larger change with little payoff for a bounded operation; not pursued.
