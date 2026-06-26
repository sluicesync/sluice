# sluice v0.99.139

**New: per-sync PlanetScale target-health telemetry in the `sync run` fleet config — each supervised sync can now watch its OWN PlanetScale target's CPU / memory / storage / lag / connections, instead of a fleet getting none (ADR-0126, roadmap item 47). Opt-in and additive; fully drop-in over v0.99.138.**

## Features

**Per-sync PlanetScale telemetry in `sync run` (ADR-0126).** The optional PlanetScale control-plane telemetry that `sync start` already wires (ADR-0107 — target CPU/mem/storage/lag/connections, driving the startup AIMD headroom clamp, the `sluice_target_*` metric family, the PS-gated threshold alerts, and `sluice diagnose`'s target-health block) is now configurable **per sync** in the `syncs.yaml` fleet config. Until now a `sync run` fleet got no PlanetScale telemetry at all; since a fleet's syncs typically target different PlanetScale databases/branches, the right model is per-sync, and that's what this adds.

Each sync gains the same knobs as the single-sync flags, kebab-cased to match: `planetscale-org` (the opt-in switch), `planetscale-metrics-token-id` / `planetscale-metrics-token` (the control-plane service token), `planetscale-metrics-branch` / `planetscale-metrics-db` (the series filter; the db defaults to the one derived from the target DSN), `suppress-target-metrics-history` (opt-out of the bounded `sluice_target_metrics_history` recording), and the PS-gated threshold alerts `notify-storage-util` / `notify-cpu-util` / `notify-mem-util` / `notify-lag-seconds` / `notify-storage-growth-per-min` (these ride the existing per-sync notify sinks alongside the already-ungated `notify-sync-lag-seconds`). Each telemetry-enabled sync gets its own provider polling independently of the apply path (so it persists across the supervisor's restarts), feeding that sync's headroom clamp, its `sluice_target_*{stream_id}` series, its PS-gated alerts, and `diagnose`; every provider's poll goroutine is shut down cleanly on fleet exit.

**Secrets are env-first.** Leave `planetscale-metrics-token-id` / `-token` empty in the YAML and sluice falls back to the shared `PLANETSCALE_METRICS_TOKEN_ID` / `PLANETSCALE_METRICS_TOKEN` env vars — the common one-org-one-token fleet sets the service token once in the environment and adds only the non-secret `planetscale-org` (+ optional `-branch`/`-db`) per sync, so no token is ever committed to a config file (a per-sync YAML override exists for the rare multi-org fleet, with the same env-preferred guidance). Opt-in is **per-sync all-or-nothing, validated at config load**: a sync that sets `planetscale-org` without a resolvable token (spec or env) is refused loudly — named by stream-id and missing field — before anything is built; a sync that sets none is telemetry-off and byte-identical to today. The `sync run --dry-run` plan shows each sync's telemetry disposition (`telemetry=org/db@branch` or `telemetry=off`) without printing the token.

## Compatibility

Purely additive and opt-in. A sync with no `planetscale-org` is unchanged — no provider, no goroutine, the zero value is off. No value-path change and no process-global state; the telemetry remains the engine-neutral, advisory, failure-isolated capability ADR-0107 defined (a telemetry outage never stalls or affects a sync). Fully drop-in over v0.99.138.

## Who needs this

Operators running a `sync run` fleet against PlanetScale targets who want per-target health-aware apply tuning, `sluice_target_*` metrics, PS-gated alerts, and `diagnose` target-health for each sync — set the service token once in the environment and add `planetscale-org` (+ `-db`) per sync. Everyone else is unaffected.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.139 · **Container:** ghcr.io/sluicesync/sluice:0.99.139
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
