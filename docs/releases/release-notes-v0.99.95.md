# sluice v0.99.95

**PlanetScale target-health telemetry is now live (ADR-0107 Phase 2).** v0.99.92 shipped the engine-neutral advisory seam (default-off); this release adds the optional PlanetScale provider that actually feeds it, so sluice can see the target's CPU / memory / storage directly and react *proactively* — and operators can surface that target health in-tool. Entirely opt-in: with no flags set, nothing changes.

## Added

**PlanetScale metrics provider + CLI flags (ADR-0107 Phase 2, roadmap item 32).** When you supply `--planetscale-org` plus a metrics service token (`--planetscale-metrics-token-id` / `--planetscale-metrics-token`, both env-backed and masked in all logging — a `read_metrics_endpoints` token, distinct from your data-plane DSN), sluice polls PlanetScale's per-branch Prometheus metrics off the hot path and distills the target's **CPU / memory / storage** (plus replica lag and connections) into the engine-neutral health snapshot that the Phase-1 consumers already read. That gives the proactive AIMD apply back-off and the storage-resize-anticipation WARN a real data source, and exports the target's state as `sluice_target_*` gauges alongside sluice's own metrics.

The design keeps PlanetScale strictly contained:
- **Zero PlanetScale leakage into the core.** The provider lives entirely in its own `internal/planetscale/telemetry` package and is wired only at the `cmd/sluice` composition root; `internal/ir`, the pipeline, and the engines never import it.
- **Correct target selection + units.** It selects the target branch by database name (`--planetscale-metrics-database`, defaulting to the `--target` DSN's database; `--planetscale-metrics-branch` defaults to `main`), reads the **primary vttablet** series for write-target health, normalizes the `_util_percentages` metrics (0–100) to a `[0,1]` fraction, and marks any metric absent from the scrape `*Known=false` (it never reports a missing metric as "idle").
- **Polls at the source's real cadence.** 60 s — the confirmed PlanetScale metric granularity (the service-discovery targets advertise `__scrape_interval__=1m` and the sample timestamps advance exactly every 60 s, verified live), so a faster poll would only re-read the same sample.
- **Advisory and failure-isolated.** The opt-in is all-or-nothing (`--planetscale-org` without a complete token pair refuses loudly); a poll error or stale snapshot degrades to "no signal" while the exactly-once frontier, AIMD, and tx-killer machinery stay authoritative; a dead metrics endpoint never stalls or crashes a sync.
- **Default-off, byte-identical when unused.** With no `--planetscale-*` flags, the provider is never constructed and the streamer's telemetry hook stays a typed-nil-guarded no-op — behaviour is identical to v0.99.94.

Validated by `httptest`-fixtured unit tests (branch + primary-vttablet selection, percentage normalization, missing-metric honesty, poll-failure-then-stale, non-blocking `Sample`, loud incomplete-credential refusal) and a credentialed `psverify`-tagged smoke that ran live against the real endpoint. The Postgres-target metric-name table and Phase 3 (auto lane-count from connection metrics, the inverse "export sluice's own richer metrics" sub-item) remain demand-gated follow-ups; persisting a rolling metrics history (roadmap item 35) and a metrics-watch / notification mode (item 36) are scoped as the next extensions.

## Compatibility

Default behaviour is unchanged and byte-identical to v0.99.94 — the telemetry provider is entirely opt-in and engages only when you pass `--planetscale-org` plus a complete metrics token. No resume-format, wire, or result-state changes.

## Who needs this

Operators running `sluice` against **PlanetScale** who want sluice to react to target saturation *before* the reactive signals push back (proactive apply back-off, storage-grow anticipation) and to see target CPU / memory / storage in-tool. Requires a PlanetScale service token with `read_metrics_endpoints`. Everyone else is unaffected.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.95
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.95
```
