# ADR-0126: Per-sync PlanetScale telemetry in the `sync run` fleet config

## Status

**Accepted (2026-06-26).** Roadmap item 47 deferred-polish (one of the two
process-global gaps ADR-0122 §Status deferred). Makes the optional PlanetScale
target-health telemetry (ADR-0107, item 32) configurable **per sync** in the
`sync run` fleet config, so each supervised sync can watch its own PlanetScale
target's CPU / memory / storage / lag / connections.

## Context

The `--planetscale-org` + `--planetscale-metrics-token-id/-token/-branch/-db` flags
(ADR-0107) enable an optional control-plane telemetry provider for a single
`sync start`: `cmd/sluice` builds a `*pstelemetry.Provider` via
`buildTargetTelemetryProvider` and attaches it to the `Streamer.TargetTelemetry`
seam (the engine-neutral `ir.TargetTelemetry`), which drives the startup AIMD
headroom clamp, the `sluice_target_*` metric family, the PS-gated notify rules, and
`sluice diagnose`'s target-health block. The provider runs its own poll goroutine and
is shut down via `Provider.Close()` (the single-sync path `defer`s it).

The `sync run` fleet config (`SyncSpec` in `cmd/sluice/sync_run.go`) carries ~25
per-sync knobs but **not** the PlanetScale telemetry ones — its own comment defers
them. So a fleet gets **no** PlanetScale telemetry at all today: no per-target
headroom clamp, no `sluice_target_*` series, no PS-gated alerts. Since a fleet's
syncs typically target different PlanetScale databases/branches, the right model is
per-sync, not one process-global setting.

The enabling fact: `ir.TargetTelemetry` is already a **per-`Streamer` instance** seam
(not global state), and `buildTargetTelemetryProvider` is engine-neutral and
parameterized — so per-sync telemetry is additive plumbing, with **no value-path or
global-state change** (unlike the sibling per-sync MySQL-overrides item, ADR-0127).

## Decision

1. **Add the telemetry keys to `SyncSpec`** (kebab-case, mirroring the flag names):
   `planetscale-org`, `planetscale-metrics-token-id`, `planetscale-metrics-token`,
   `planetscale-metrics-branch`, `planetscale-metrics-db`, plus
   `suppress-target-metrics-history` and the PS-gated notify thresholds
   (`notify-storage-util` / `-cpu-util` / `-mem-util` / `-lag-seconds` /
   `-storage-growth-per-min`) so a per-sync provider is fully usable (headroom clamp +
   metrics + diagnose + PS-gated alerts), not just half-wired.

2. **Secret handling: env-first, per-sync override allowed.** The two secret fields
   (`metrics-token-id`, `metrics-token`) fall back to the shared `PLANETSCALE_METRICS_TOKEN_ID`
   / `PLANETSCALE_METRICS_TOKEN` env vars when not set in the spec — the common case is
   one org + one service token across the fleet, so an operator sets the secret once in
   the environment and only the non-secret `planetscale-org` / `-branch` / `-db` per
   sync. The YAML override exists for the rare multi-org fleet but the docs steer the
   secret to the env (never commit a token to a config file). Tokens are never logged.

3. **All-or-nothing per sync, validated at config load.** A sync that sets
   `planetscale-org` without a resolvable token-id + token (spec or env) is refused
   loudly at `fleet.validate()` — the same all-or-nothing posture as `sync start`
   (`buildTargetTelemetry`'s contract), named by stream-id. A sync that sets none is
   telemetry-off (the zero value), byte-identical to today.

4. **One provider per sync, closed on fleet shutdown.** `buildSupervisedFleet` builds
   a provider per telemetry-enabled sync (from that sync's resolved params) and attaches
   it to that sync's `Streamer.TargetTelemetry`; it returns a closer the `sync run`
   command `defer`s so every provider's poll goroutine is shut down on fleet exit. The
   provider persists across a sync's supervisor restarts (it polls independently of the
   Streamer's run loop), exactly as the single-sync provider outlives a reactive
   re-snapshot.

## Fleet config example (`syncs.yaml`)

The common one-org-one-token fleet: set the service token ONCE in the
environment (`PLANETSCALE_METRICS_TOKEN_ID` / `PLANETSCALE_METRICS_TOKEN`) and
add only the non-secret `planetscale-org` (+ optional `-branch`/`-db`) per sync.
NEVER commit a token to the YAML.

```yaml
syncs:
  - stream-id: orders
    source-driver: mysql
    source: mysql://u:p@src:3306/app
    target-driver: postgres
    target: postgres://u:p@orders.psdb:5432/orders_db
    # PlanetScale target-health telemetry for THIS sync. Token resolves from
    # PLANETSCALE_METRICS_TOKEN_ID / PLANETSCALE_METRICS_TOKEN in the env.
    planetscale-org: acme
    planetscale-metrics-branch: main      # optional; defaults to "main"
    planetscale-metrics-db: orders_db     # optional; defaults to the target DSN's db
    # PS-gated threshold alerts (ride the existing per-sync notify sinks):
    notify-storage-util: 0.85
    notify-cpu-util: 0.90
    notify-lag-seconds: 30
    notify-webhook: ""                    # sink URL via SLUICE_NOTIFY_WEBHOOK env
  - stream-id: analytics
    source-driver: postgres
    source: postgres://u:p@src2:5432/app
    target-driver: postgres
    target: postgres://u:p@analytics.psdb:5432/analytics_db
    planetscale-org: acme                 # same org+token, different target db
    # no notify-* thresholds set ⇒ telemetry feeds the headroom clamp + metrics
    # + diagnose only (no alerts)
  - stream-id: legacy
    source-driver: mysql
    source: mysql://u:p@src3:3306/app
    target-driver: mysql
    target: mysql://u:p@dst:3306/app
    # no planetscale-org ⇒ telemetry off (byte-identical default sync)
```

A sync that sets `planetscale-org` but has no resolvable token (neither in the
spec nor the env) is refused at `sync run` config-load, named by stream-id. The
`--dry-run` plan prints each sync's disposition (`telemetry=acme/orders_db@main`
or `telemetry=off`) without the token. For a rare multi-org fleet, override the
token per sync with `planetscale-metrics-token-id` / `planetscale-metrics-token`
(prefer a secrets-manager-injected env over committing it).

## Consequences

- Each fleet sync can watch its own PlanetScale target: per-sync headroom-clamped apply
  concurrency, per-sync `sluice_target_*{stream_id}` metrics (already labeled by
  stream-id), per-sync PS-gated alerts, and `diagnose` target-health — closing the
  ADR-0122 gap. Operators with one org set the token once in the env and add
  `planetscale-org`/`-db` per sync.
- Purely additive and opt-in: a sync with no `planetscale-org` is unchanged. No global
  state, no value-path change; the telemetry stays the engine-neutral, advisory,
  failure-isolated capability ADR-0107 defined (a telemetry outage never stalls a sync).
- The fleet config gains secret-bearing fields; the loader keeps the env-first guidance
  and refuses a half-configured sync loudly rather than silently running telemetry-off.

## Alternatives considered

- **One process-global `--planetscale-org` for the whole fleet.** Rejected: a fleet's
  syncs target different PS databases/branches, so a single global can't filter the
  right series per sync, and it reintroduces the process-global wart this item exists to
  remove.
- **Token only via env, no per-sync YAML override.** Simpler and most secure, but blocks
  the multi-org fleet. The chosen design defaults to env and allows the override with a
  documented warning — env-first without being env-only.
