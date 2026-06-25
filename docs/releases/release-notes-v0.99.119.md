# sluice v0.99.119

**`sluice restore` can now clamp its automatic parallelism by the target's live CPU/memory headroom — the PlanetScale-correct bound (ADR-0115, roadmap item 40).** Opt-in and fully advisory: with no telemetry configured (the default), restore behaves exactly as before.

## Added

Restore fans out on two automatic axes — cross-table (`--table-parallelism`) and within-table chunk (`--bulk-parallelism`) — whose product is bounded at the connection-budget chokepoint. But that bound only applies to engines that expose a connection-budget prober (Postgres); a MySQL/PlanetScale target has none, so the automatic product passed through **unbounded**. And on PlanetScale, connections are the wrong thing to bound on: vtgate fronts a large connection pool (connections are abundant), while **CPU is the scarce resource** on small tiers — a PS-10 (1/8 vCPU) sat pinned at 100% CPU during the Track-C restore A/B.

This release wires the same control-plane telemetry the CDC apply path already uses into the restore parallelism resolver. The `restore` command now accepts the PlanetScale telemetry flags (`--planetscale-org`, `--planetscale-metrics-token-id`, `--planetscale-metrics-token`, `--planetscale-metrics-db`, `--planetscale-metrics-branch`, the token via environment variable), mirroring `sync start`. When telemetry reports the target's CPU/memory headroom is tight, restore reduces the automatic `table × chunk` product — halved when approaching the high-water mark, quartered when at or over it — with the cross-table axis absorbing the reduction first so each table keeps its within-table chunk fan-out.

The `{1,2,4}` headroom-reduction thresholds are now extracted into a single helper shared with the apply-path clamp, so the two paths can never disagree on what "tight" means (the refactor is behaviour-preserving — the apply-path tests stay green).

**Guarantees:**
- **Advisory and opt-in** — no telemetry configured ⇒ no clamp, byte-for-byte the previous behaviour.
- **Never raises** the resolved parallelism, and **never drops an axis below 1**.
- **Respects explicit operator intent** — only an automatic (`0`) axis is reduced; an explicitly-pinned `--table-parallelism` / `--bulk-parallelism` is never clamped, and when both are pinned the clamp is a no-op.
- **PlanetScale-correct** — CPU/memory-driven, the bound that actually matters there; complements (does not replace) the connection-budget split that still bounds prober-equipped engines.

## Validation

- Unit pins: the product-clamp threshold table (healthy → unchanged, approaching → halved, saturated → quartered, busiest-of-CPU/mem drives, partial/stale snapshot ⇒ no-op), pinned-axis-respected (both-pinned no-op; one-pinned reduces only the auto axis), and the never-below-1 / never-raise floor.
- The behaviour-preserving extraction is covered by the existing apply-path headroom-clamp tests (still green).
- Landed through CI's `-race` integration gate before tagging.

## Compatibility

No behaviour change unless you opt in with the `--planetscale-*` telemetry flags on `restore`. No change to `migrate`, `sync`, `backup`, or to restore correctness (the disjoint-chunk partition, per-chunk SHA-256, and layer-2 row-count checks are untouched). This only changes how wide an automatic restore *starts*; the per-worker storage-grow/reparent ride-through (ADR-0108/0110) remains the reactive floor on top.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.119
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.119
```
