# sluice v0.99.91

**`--apply-concurrency` is now fast by default.** Steady-state MySQL/Postgres CDC apply was the last major throughput axis still defaulting to serial; it now resolves an unset value to an adaptive, connection-budget-bounded `auto:N` — so continuous-sync catch-up and steady-state throughput are fast out of the box, with no operator action. Operators who want strictly serial apply pass `--apply-concurrency 1`.

## Changed

**Fast-by-default CDC apply (ADR-0106, roadmap item 31).** Every cold-copy axis already defaults to `auto` (`--table-parallelism`, `--bulk-parallelism`, `--copy-fanout-degree`); steady-state CDC apply was the lone exception, still defaulting to serial because `--apply-concurrency` shipped as a v0.99.77 preview and the default was never flipped. The concurrent key-hash apply path has since graduated to GA and been hardened across v0.99.77→v0.99.90 — exactly-once cross-lane commit frontier on both MySQL (ADR-0104) and Postgres (ADR-0105), per-lane AIMD self-throttle, in-lane PlanetScale-tx-killer re-chunk recovery, the Bug 158 silent-loss fix, and the v0.99.90 file/pos warm-resume fix — so flipping the default is safe.

The contract mirrors `--table-parallelism`:
- `0` (unset, the default) → **`auto:N`**, the adaptive default;
- `1` → the **explicit serial opt-out**, byte-identical to the prior default for anyone who wants strictly serial apply;
- `W > 1` → honored verbatim (the operator owns their target's budget).

`N` is conservative and capacity-aware. On a **Postgres** target it is `min(4, budget)`, where `budget` comes from the same connection-slot probe `--max-target-connections` already drives — a constrained instance yields fewer lanes automatically, and an exhausted or unavailable budget degrades to serial rather than refusing (the cold-start preflight still owns the loud connection-budget refusal). On a **MySQL / PlanetScale-MySQL** target there is no connection-slot probe, so the auto value is a fixed conservative ceiling of `4` (PlanetScale per-branch connection limits are generous relative to 4 lanes + 4 dedicated backends across every tier). The auto value deliberately equals the cold-copy axes' `auto:4`, so the whole pipeline fans out ~4-wide by default, bounded by the target.

The headline effect is out-of-the-box CDC catch-up and steady-state throughput rising toward `N×` on the common cross-region / loaded-target case with zero operator action (the lever that closed the item-23 cross-region apply wedge, previously opt-in).

**Correctness is unchanged** — this changes only the default *value* of the lane count `W`, never the apply machinery. The persisted resume position still advances only to a source-transaction boundary durable across all lanes (exactly-once for keyed tables; keyless stays at-least-once, Bug 143 unchanged), per-lane AIMD still self-throttles on a weak target, and in-lane abort recovery is untouched. The default is resolved at the `Streamer` construction level (not only the CLI) — exactly as the sibling `--auto-tune` default is — so every construction path (tests, broker/chain replay, future programmatic callers) gets the fast default rather than silently reverting to serial (the v0.99.51 / Bug-158 zero-value-safe-default trap). The prerequisite for this flip was v0.99.90: before it, the concurrent path was not resume-safe on a native-MySQL file/pos source, so defaulting concurrency on would have shipped a warm-resume crash-loop to every such user.

Pinned by unit tests (the `0 → auto:N`, `1 → serial`, `W>1 → verbatim` contract; the keystone "unset is not serial"; PG budget-bounds-and-caps-lanes; probe-refuse/fail → serial; a programmatic applier without the concurrency surface stays serial) and an integration test that confirms the default engages concurrency end-to-end with no operator action and that the default converges byte-identical to explicit serial. The `-race` integration gate ran before tagging (CDC/exactly-once chunk).

## Compatibility

This **changes a default for every user**: a `sluice sync` started without `--apply-concurrency` now applies CDC changes across multiple key-hash lanes instead of one. It is wire- and result-compatible — final target state is byte-identical to serial — and resumable exactly as before. The only observable differences are higher steady-state apply throughput and slightly more target connections by default (≤ `N` lanes + `N` dedicated backends, bounded by the probe on PG / the fixed ceiling on MySQL, well within real limits). To restore the exact prior behavior, pass `--apply-concurrency 1`.

## Who needs this

Everyone running `sluice sync` (continuous CDC), especially **cross-region or loaded targets** (PlanetScale and similar) where serial apply was the throughput bottleneck — you now get multi-lane apply automatically. If you previously set `--apply-concurrency` explicitly, your value is still honored unchanged. If you require strictly serial apply, set `--apply-concurrency 1`.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.91
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.91
```
