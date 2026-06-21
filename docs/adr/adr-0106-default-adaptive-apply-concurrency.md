# ADR-0106: Fast-by-default CDC apply — `--apply-concurrency` defaults to an adaptive value

## Status

**Accepted** (implemented; resolution lives in `internal/pipeline.Streamer.resolveApplyConcurrency`, called per attempt by `runOnce`). Realizes roadmap item 31. Operator-flagged 2026-06-21 ("better speeds out-of-the-box, rather than users needing to opt-in to faster behavior"). Changes a default for **every** user, so it ships as its own release behind a full regression cycle and the `-race`-before-tag gate (CDC/exactly-once chunk) — the main session drives that release.

## Context

sluice's cold-copy / bulk axes are already **fast by default** and connection-budget-bounded:

| Knob | Path | Default |
| --- | --- | --- |
| `migrate --table-parallelism` | bulk copy (cross-table) | `0 → auto: 4` (ADR-0076) |
| `--bulk-parallelism` | FAST cold-start, PG source (within-table) | `0 → min(8, NumCPU)` (ADR-0079) |
| FAST cold-start `--table-parallelism` | PG-source cold-start (cross-table) | `0 → auto: 4` |
| `--copy-fanout-degree` | VStream/CDC snapshot cold-start (write fan-out) | `0 → auto: 4` (ADR-0097) |
| `backup`/`restore --table-parallelism` | backup read / restore write | `0 → auto: 4` (ADR-0084/0088) |

The lone exception is **steady-state CDC apply**: `--apply-concurrency` (ADR-0104 MySQL / ADR-0105 Postgres key-hash lane apply) still defaults to **serial**. `Streamer.ApplyConcurrency` is `0` (= serial) unless the operator opts into `W > 1` (`internal/pipeline/streamer.go`). So the continuous-sync catch-up + steady-state throughput — often the part of a migration that runs longest and matters most for cutover lag — is the only piece of sluice that is *not* fast out of the box. This is a historical artifact: `--apply-concurrency` shipped as a v0.99.77 PREVIEW, graduated to GA in v0.99.80, but the default was never flipped.

**Why flipping it is safe NOW** (the correctness gate the tenets require is met — and was not when it shipped as a preview):

- **Exactly-once on both engines.** The cross-lane commit frontier ("the resume position advances only to a source-tx boundary durable across all lanes; must NOT persist a partial point") is implemented for MySQL (ADR-0104) and Postgres (ADR-0105, the extracted engine-neutral `internal/laneapply`).
- **Self-throttling.** Each lane runs its own AIMD batch-size controller (v0.99.80): on a slow/weak target each lane multiplicatively shrinks its batch toward 1, so concurrency cannot overwhelm a small instance — it backs off per-lane.
- **In-lane tx-killer recovery** (v0.99.80) + the **lane-local committable-size read cap** (v0.99.81): a PlanetScale tx-killer is handled in-lane (MD-shrink + re-chunk + idempotent retry), no whole-run restart.
- **Silent-loss class closed.** Bug 158 (PG concurrent first-boundary phantom silent loss) fixed in v0.99.83.
- **Serial path itself hardened.** v0.99.89 (ADR-0106's sibling, item 29) fixed the serial applier's mid-tx checkpoint crash-loop — so the *fallback* path (`W=1`) is also correct.

**Live evidence (item 30, in flight 2026-06-21).** A non-Metal PlanetScale **PS-10** (1/8 vCPU, 1 GB, the worst case for a concurrent-by-default policy) is running Track-D cold-copy + CDC at `--apply-concurrency 4`: through the first storage auto-grow (10 GB → 12 GB+) the apply path logged **0 WARN / 0 ERROR / 0 reconnect / proc up**, with per-lane AIMD available to back off. This is the empirical check that budget-bounded concurrency stays safe on tiny instances.

## Decision

Make `--apply-concurrency` resolve **`0 → auto:N`** — an adaptive, connection-budget-bounded default — instead of serial. Keep **`=1` as the explicit serial opt-out** (byte-identical to today's default behavior, for operators who want it). This mirrors the established `--table-parallelism` shape (`0 = auto:4`, `1 = disable`).

### `N`: conservative, capacity-aware, consistent with the cold-copy axes

- **Postgres target:** `N = min(4, budget)` where `budget` is derived from the existing connection-slot probe (ADR-0079 / `--max-target-connections`: `max_connections` − in-use − reserve, minus the reserved CDC connection and any concurrent cold-copy connections). On a constrained instance the probe yields fewer lanes automatically.
- **MySQL / PlanetScale-MySQL target:** there is **no connection-slot probe** (`--max-target-connections` is documented "inert against engines without a connection-slot model"), so the auto value is a **fixed, conservative ceiling of 4** — matching the cold-copy axes' `auto:4`. PlanetScale per-branch connection limits are generous relative to 4 lanes + 4 dedicated backends (e.g. PS-10 caps at 250; the item-30 run sits at ~51 total), so 4 is safe across tiers. Operators raise it explicitly for beefier targets.
- `auto:4` deliberately equals the cold-copy axes' default so the whole pipeline has one mental model ("sluice fans out ~4-wide by default, bounded by your target's budget").

### The default is resolved at the STREAMER/applier level, NOT only in the CLI (load-bearing)

This is the part that must not be gotten wrong. The default resolution lives where the `Streamer` is constructed, exactly as the sibling `AutoTune` field already does it ("the default at the streamer level is also true so any programmatic caller that doesn't set it gets the opted-in shape"). Resolving `0 → auto:N` **only** in the kong CLI layer would re-introduce the **zero-value-safe-default trap** (the v0.99.51 / Bug-158 family, called out in `CLAUDE.md`): every other construction path — unit tests, the broker/chain replay, future programmatic callers — would receive the Go zero value (`0`) and silently fall back to serial, so the "default" would be a lie everywhere except the one CLI path. The resolution therefore happens in `Streamer` construction (CLI passes the operator's explicit value or a sentinel meaning "unset"), and `=1` remains the distinct, honored serial opt-out.

Because `0` and `1` are *both* "serial" today, the engaged-concurrency signal cannot be encoded by the raw int's zero value alone — the resolver maps the unset/`0` case to `auto:N` and treats an explicit `1` as serial, identical to `--table-parallelism`'s contract.

### Scope and rollout

- Engine-general: applies to both MySQL (ADR-0104) and Postgres (ADR-0105) targets via the shared `internal/laneapply` orchestration.
- Ships as its **own release** with a full regression cycle and the `-race` integration gate before tag — it changes default behavior for all users.
- Documentation: `throughput-tuning.md`, the `sync start` help text, and CHANGELOG note the new default + the `--apply-concurrency 1` opt-out and the rationale.

## Consequences

- **Out-of-the-box CDC throughput** rises toward `N×` on the common cross-region / loaded-target case (the item-23 wedge) without any operator action — the headline win.
- **No new silent-loss surface:** the frontier (exactly-once), per-lane AIMD (self-throttle), in-lane tx-killer recovery, and the ADR-0089 keyless guard already hold under `W > 1`; this ADR only changes the default value of `W`, not the machinery.
- **Crash-replay window** under the default is the per-lane in-flight window (idempotent re-apply for keyed/unique tables; the keyless guard keeps keyless at the `=1` at-least-once baseline — Bug 143 unchanged).
- **Slightly more target connections by default** (≤ `N` lanes + `N` dedicated backends), bounded by the probe (PG) or the fixed ceiling (MySQL); well within real connection limits.
- **`--apply-concurrency 1`** is the documented escape hatch to the exact prior behavior for anyone who wants strictly serial apply.
- A differential pin (serial `W=1` == default `W>1`, byte-identical final state) and a connection-budget-bound pin run under the new default in CI.

## Alternatives considered

- **Keep it opt-in (status quo).** Rejected: leaves the longest-running phase of a migration slow by default, contradicting the "fast out-of-the-box" goal now that the safety gate is met. The whole rest of the pipeline is already auto-parallel.
- **Fixed default `W=4` everywhere, no budget bound.** Rejected: ignores constrained targets; a probe-bounded default on PG costs nothing and is strictly safer. (MySQL falls back to the fixed ceiling only because it has no probe.)
- **Probe-only / fully dynamic `N` per target tier (scale up on big instances).** Deferred, not rejected: the conservative `auto:4` matches the cold-copy axes today; scaling `N` (and the `--copy-fanout-degree` / `--table-parallelism` `auto:4` values) with detected target CPU/connection capacity is a clean follow-up under this same ADR (roadmap item 31 gotcha 5) once the flat default is proven.
- **Resolve the default in the CLI layer only.** Rejected outright — the zero-value-safe-default trap; the default must be universal across every `Streamer` construction path.
