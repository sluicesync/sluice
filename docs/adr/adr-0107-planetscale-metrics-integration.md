# ADR-0107: PlanetScale Prometheus-metrics integration — proactive target-health telemetry

## Status

**Accepted** — Phase 1 (the engine-neutral seam + advisory consumers, default-off) shipped in v0.99.92; Phase 2 (the real PlanetScale metrics provider + `--planetscale-*` flags, poll cadence 60 s = the confirmed 1-min source granularity) shipped in v0.99.95. **Phase 3 shipped in v0.99.106:** (a) the auto:N apply-lane count is now clamped by the target's live CPU/memory headroom at startup (`clampConcurrencyByHeadroom` — a STARTUP-only bias, not mid-stream re-partition, since the CDC apply path partitions by a fixed PK-hash→lane map; the per-lane AIMD owns the dynamic sizing); (b) the `sluice diagnose` bundle gained a `health/target_health.json` target-health section (CPU/mem/storage/lag/conns), with `--planetscale-*` flags on the `diagnose` subcommand to populate it; (c) the metric-name table is now engine-parameterised so a **Postgres** target reads `planetscale_volume_*` / `planetscale_postgres_*` instead of the Vitess `vttablet_*` names (PG connection metrics left unmapped pending live confirmation — the honest-unobserved gap). **Item 35 (rolling metrics history) shipped in v0.99.107:** an engine-neutral `ir.TargetMetricsHistoryStore` seam (MySQL + Postgres stores) persists each ~60 s poll into a bounded `sluice_target_metrics_history` table on the target (7-day TTL prune), recorded by a failure-isolated pipeline sidecar; `sluice diagnose` surfaces the recent rows + current/1m/5m/10m avg+max trend; default-on when telemetry is configured, `--suppress-target-metrics-history` opt-out. **Item 36 (sync-scoped threshold alerter) shipped in v0.99.108:** an engine-neutral `internal/notify` sink layer (generic webhook + Slack incoming-webhook, credential-gated via env) + a pipeline threshold-alerter sidecar with edge-trigger, per-rule cooldown, and hysteresis re-arm; rules on storage/cpu/mem/lag util plus a storage rate-of-change "pre-grow" warning; failure-isolated (a dead sink is logged + swallowed) and opt-in (a sink URL + a threshold, telemetry-gated). The SMTP/email sink, the standalone `metrics-watch` daemon, and the inverse own-`/metrics` export remain demand-gated follow-ups (see roadmap items 35 + 36; the SMTP sink and `metrics-watch` have since shipped — item 48 / ADR-0125-era releases). **Item-36 rule family extended (2026-07-22, v0.99.288):** two probe-based TARGET-side vacuum rules — `--notify-dead-tuple-ratio` (worst user table's dead-tuple ratio from `pg_stat_user_tables`, 1000-dead-tuple noise floor, WARNING) and `--notify-xid-age` (`age(datfrozenxid)` wraparound headroom, CRITICAL) — via a new optional `ir.TargetVacuumHealthReporter` implemented by the Postgres `ChangeApplier` over the pool it already holds. Like the item-45 sync-lag rule these are UNGATED from PlanetScale telemetry (sink-only gating; a non-Postgres target WARNs once and stays inert) and share the same edge-trigger/cooldown/hysteresis machinery; deliberately NOT persisted into `sluice_target_metrics_history` (a stored codec — schema change out of scope for an advisory). This ADR records the design for an OPTIONAL PlanetScale capability that consumes the target's per-branch Prometheus metrics (CPU / memory / storage first; lag / connections secondary) so sluice's apply path can react PROACTIVELY — before the reactive AIMD controller, the tx-killer, or a storage-resize stall would otherwise push back — and so operators can see the target's resource state alongside sluice's own progress.

It is **design-gated** (the operator wants a reviewable ADR + plan before any implementation) and, once approved, **demand-gated + opt-in**: the gap it closes is an adaptivity/observability *enhancement*, **not a value-fidelity / silent-loss one** — the frontier (exactly-once), the per-lane AIMD controller, and the in-lane tx-killer recovery already keep the sync correct without it. Nothing here is built today except the engine-neutral interface seam ([`internal/ir/target_telemetry.go`](../../internal/ir/target_telemetry.go), interface-only, no PlanetScale impl) committed alongside this ADR to make the seam concrete and reviewable. It promotes roadmap [item 32](../dev/roadmap.md) from a roadmap write-up into a proper ADR.

It connects to several in-flight resilience items and references them by their shipped/roadmap state:
- [item 31](../dev/roadmap.md) — fast-by-default `--apply-concurrency` (ADR-0106, **shipped v0.99.91**): use (a)'s proactive back-off biases the now-default concurrent path's per-lane AIMD ceiling.
- [item 30](../dev/roadmap.md) — non-Metal storage auto-grow resilience: use (b) anticipates the resize from `volume_available_bytes` instead of meeting it as a confusing stall.
- [item 33](../dev/roadmap.md) — cold-copy reparent/`not serving` resilience: use (b)'s storage-headroom signal pairs with item 33's bulk-copy retry (a resize at the ~10 GB boundary triggers the reparent item 33 must ride through).
- [item 19](../dev/roadmap.md) — Vitess throttle resilience: use (a) complements but does **not** replace the data-plane throttle signal `SHOW VITESS_THROTTLED_APPS` (the metrics set has no throttler-state metric — see Non-goals).

## Context

### sluice's adaptivity is reactive today

Every adaptive lever sluice has acts *after* the target pushes back:

- The **AIMD controller** ([`internal/appliercontrol`](../../internal/appliercontrol/controller.go)) sees only per-batch commit *latency* (rolling p95) and retriable/tx-killer error events. It multiplicative-decreases after a slow or killed batch — i.e. after the target is already saturated.
- The **tx-killer recovery** (per-lane, ADR-0104/0105, GA in v0.99.80) re-chunks and retries in-lane — after the target's transaction killer has already aborted a batch.
- The **storage-resize handling** (items 30/33) reconnects and warm-resumes — after the resize-induced stall/reparent has already surfaced as a timeout or `not serving`.

PlanetScale already exports a rich per-branch Prometheus metrics surface that would let sluice see the target's resource state **directly** and act **before** the pushback. The operator's stated priority is **CPU / memory + storage usage first**; lag/connections secondary.

### The confirmed PlanetScale exposure (from the roadmap item; not independently re-confirmed this pass)

> *Note: the PlanetScale docs pages for this surface returned 404 during this design pass (PlanetScale restructured their docs). The surface below is taken verbatim from roadmap item 32, which records it as "confirmed from the docs." Implementation Phase 2 MUST re-confirm the live endpoint, auth header, HTTP-SD shape, and exact metric names against the current docs before writing the HTTP client — see the impl plan's Phase 2 preflight.*

- **Endpoint:** per-org `https://api.planetscale.com/v1/organizations/${ORG}/metrics`, scraped via Prometheus **HTTP Service Discovery** (per-branch targets, ~10-min SD refresh).
- **Auth:** a **service token** (`${TOKEN_ID}:${TOKEN}`) granted the **`read_metrics_endpoints`** permission — a CONTROL-PLANE credential, distinct from the data-plane DSN (same opt-in posture as ADR-0103's deploy-request token).
- **Key metrics:** CPU `planetscale_pods_cpu_util_percentages`, memory `planetscale_pods_mem_util_percentages` (both engines); storage — Vitess `planetscale_vttablet_volume_{available,capacity}_bytes` + `planetscale_vttablet_table_storage_all_bytes`, Postgres `planetscale_volume_{available,capacity}_bytes`; lag `planetscale_{mysql,postgres}_replica_lag_seconds`, `planetscale_workflow_vreplication_lag`, `planetscale_postgres_wal_size_bytes`; connections `planetscale_edge_active_connections`, `planetscale_vttablet_connection_pool_active`, PG `planetscale_postgres_connection_state` / `planetscale_postgres_settings_max_connections`.

### The tenet hazard this design must clear

PlanetScale-specific control-plane knowledge MUST NOT leak into the engine-neutral core (`internal/ir`, `internal/pipeline` orchestrator) — the IR-first and contain-PS-complexity tenets. The existing precedents show exactly how to clear it:

- **[`ir.TargetConnectionBudgetProber`](../../internal/ir/connection_budget.go)** — an optional surface a *target engine* implements; the orchestrator discovers it with a structural type assertion (`s.Target.(ir.TargetConnectionBudgetProber)`) and reads a plain `ConnectionBudget` report. No engine-specific branch in the orchestrator.
- **[`pipeline.SpillReporterFunc`](../../internal/pipeline/metrics.go)** — an optional *closure* the streamer plugs into the metrics server; it owns its own source connection, returns a plain `(snap, ok, err)`, and degrades to "emit nothing" when `ok=false`.
- **[`ir.BatchSizeProvider` / `ir.BatchObserver`](../../internal/ir/applier_control.go)** — engine-neutral interfaces the AIMD controller satisfies; the applier consults them advisorily via optional setters, zero overhead when absent.

This ADR follows the same shape: a new engine-neutral advisory interface in `ir`, an optional provider that lives in its own package and is *config-wired* (because its credential is control-plane, not an engine DSN), and the applier/AIMD layer consults it advisorily.

## Decision

Add an OPTIONAL telemetry capability with three layers, mirroring the established advisory-surface pattern:

1. **Engine-neutral seam ([`ir.TargetTelemetry`](../../internal/ir/target_telemetry.go), committed with this ADR).** A provider implements `Sample(ctx) (TargetHealthSnapshot, ok bool)`. `TargetHealthSnapshot` is a small, provider-agnostic distillation — `CPUUtil`/`MemUtil`/`StorageUtil` fractions in `[0,1]` (each with a `*Known` flag so "unobserved" is never mistaken for "idle"), raw storage bytes, and the secondary lag/connection fields — plus a `Fresh(now, window)` helper so a stale sample degrades to "no signal." **Nothing in `ir` imports the provider; the core defines only the interface and the report.**

2. **Optional PlanetScale provider (`internal/planetscale/telemetry`, NOT built this pass).** Implements `ir.TargetTelemetry` against the metrics endpoint: a background poll loop (~15-30s) that hits the org HTTP-SD endpoint with the service token, filters to the sync's target branch/keyspace series, parses the documented metric names into a `TargetHealthSnapshot`, and caches it for the non-blocking `Sample`. Lives in its own package so the `planetscale-go`/HTTP-client dependency surface and all PS-specific metric-name knowledge are contained there — `internal/ir`, the engine packages, and the orchestrator never import it.

3. **Advisory consumption in the pipeline.** The streamer is config-wired with an optional `ir.TargetTelemetry` (a field, like `MetricsListen`, set only by the CLI/config when the operator opts in). When present, the AIMD wiring consults it for proactive damping (use a), the storage-resize anticipation path reads it (use b), and the metrics server / `sluice diagnose` surface it (use c). When absent or stale, every consumer degrades to today's reactive behaviour.

### The capability seam — exactly how PlanetScale plugs in WITHOUT a PS import in core

The provider is **config-wired, not engine-implemented** — and that is a deliberate divergence from `TargetConnectionBudgetProber` worth naming. The connection-budget prober is a method on the *target engine* because it probes the target's own DSN. Telemetry is different: its credential is a **control-plane service token + org/branch identifiers**, NOT the data-plane DSN, so making it a target-engine method would force the engine to hold control-plane creds — exactly the PS-sprawl-into-core leak the tenets forbid. Instead:

- `internal/ir` owns the interface (`TargetTelemetry`) and the value (`TargetHealthSnapshot`) — pure data + one method signature, no PS knowledge.
- The PlanetScale provider is an independent package that *imports* `ir` (to satisfy the interface) but is *imported by* nothing in core. It is constructed in `cmd/sluice` (the composition root, which is allowed to know about every engine/provider) from the operator's opt-in flags and handed to the streamer as an `ir.TargetTelemetry`.
- The streamer holds it as `TargetTelemetry ir.TargetTelemetry` and consults it through the interface only. The orchestrator never type-switches on PlanetScale, never imports the provider package, and behaves identically (interface == nil) for every non-PS target.

This keeps the leak surface at exactly one allowed place — `cmd/sluice`, the composition root — identical to how engine packages are selected there by name without the pipeline importing them.

### The three uses, in priority order

#### (a) Proactive back-off — feeds item-31 concurrency + AIMD (PRIORITY: CPU / memory)

When target CPU% / memory% climb toward saturation, bias the apply path DOWN *before* a tx-kill or latency-driven MD. The AIMD controller stays authoritative; telemetry only supplies a proactive *ceiling hint*:

- The `appliercontrol.Controller` gains an OPTIONAL advisory input (a `TelemetryHint` config field — a closure or small interface the controller calls under its existing mutex during `ObserveBatch`). When the hint reports a fresh snapshot with CPU or mem above a high-water mark (e.g. ≥ 0.85), the controller treats it as a soft additional MD trigger / AI-suppressor: it will not additively-increase past the current size while the target is hot, and may MD once on a fresh saturation edge. This composes cleanly with the existing latency-driven AI/MD — it is one more reason to shrink/hold, never a reason to grow.
- Because item 31 made `--apply-concurrency` resolve to auto:N (the per-lane AIMD path is now the default), the hint is wired to **each lane's** controller (the per-lane attach path in `streamer_aimd_attach.go`), so a hot target damps all lanes proactively.
- The **connection** metrics additionally give item 31's "MySQL has no connection-slot probe" gap a real budget number: `ActiveConnections` / `MaxConnections` from telemetry is exactly the figure `autoApplyConcurrency` lacks for PlanetScale-MySQL today (it falls back to a fixed ceiling). A future refinement can let the telemetry-derived connection headroom inform the auto lane count — noted as a Phase-3 follow-up, not Phase 1.

Advisory-only safety: the controller's invariants (`[Floor, Ceiling]`, the latency window, exactly-once) are untouched; the hint can only push toward smaller/held sizes. A nil hint or a stale snapshot is a no-op — the controller behaves exactly as ADR-0052/0104/0105 specify.

#### (b) Storage-resize anticipation — feeds items 30 / 33 (PRIORITY: storage)

Read `volume_available_bytes` / `volume_capacity_bytes` (→ `StorageUtil` + raw bytes) and, when available storage falls below a headroom threshold (or the non-Metal ~10 GB auto-grow boundary is imminent), emit a clear, operator-facing **"target storage resizing / low headroom"** signal — a structured WARN log + a metric line — so the subsequent reconnect/stall (item 30's resize, item 33's reparent) is *explained in advance* rather than meeting the operator as a confusing timeout. This is the "detect → explain → ride through" pairing item 30/33 call for: the resilience code in those items still owns the actual retriable-classification + reconnect; telemetry only adds the anticipatory explanation. It NEVER pauses or gates the stream on its own (advisory-only).

#### (c) Operator observability

Surface target CPU / mem / storage / lag / connections alongside sluice's own progress so the operator sees *why* apply is slow without leaving the tool:

- **`sluice diagnose`** gains a "target health" section populated from `Sample` (when a provider is wired).
- **The `/metrics` endpoint** (already built — [`internal/pipeline/metrics.go`](../../internal/pipeline/metrics.go)) emits a small `sluice_target_*` gauge family (`sluice_target_cpu_util`, `_mem_util`, `_storage_util`, `_storage_available_bytes`, `_replica_lag_seconds`, `_active_connections`, all labelled `stream_id`), via a new `AttachTargetTelemetry` snapshotter mirroring the existing `AttachSpillReporter`. This re-exports the PlanetScale figure into sluice's OWN scrape so an operator's Grafana sees sluice's view-of-target next to PlanetScale's native metrics.

### The inverse sub-item (sluice exporting its OWN richer Prometheus metrics) — DECISION: split, mostly already shipped

The roadmap's sibling sub-item — sluice exporting its own richer `/metrics` (progress %, lag, rows/s, per-lane AIMD batch size, tx-killer/resize counts) — is **engine-neutral and needs no PS credential**, so it is a distinct concern from the PS-metrics *consumption* this ADR is about. **Decision: SPLIT it out, and recognise most of it already exists.** sluice already exports `/metrics` ([`internal/pipeline/metrics.go`](../../internal/pipeline/metrics.go)): `sluice_seconds_since_last_apply`, `sluice_stream_known`, the per-lane AIMD family (`sluice_apply_batch_size_{current,p95_seconds,decreases_total,cooloff}` with `lane="N"` labels — already item-31-aware), and the PG slot-spill counters. What's *missing* from the roadmap's wish-list (progress %, rows/s throughput, tx-killer/resize event counters) is a small, engine-neutral enhancement to that existing surface — it should be its own follow-up roadmap item / tiny ADR, NOT folded into this PS-credentialed capability. Folding it in would wrongly couple a no-credential observability win to the PS control-plane opt-in. The only PS-touching piece of the inverse direction — re-exporting the *consumed* target health as `sluice_target_*` gauges — IS in scope here (use c above), because it depends on the provider this ADR adds.

### Advisory-only safety (the load-bearing invariant)

Telemetry is a HINT. The authoritative machinery is untouched:

- The **frontier** (exactly-once, ADR-0104/0105) owns position advancement. Telemetry cannot advance, hold, or skip a position.
- The **AIMD controller** owns batch sizing within `[Floor, Ceiling]`. The hint can only bias toward smaller/held sizes; it can never raise the ceiling or grow a batch.
- The **tx-killer recovery** owns in-lane shrink-and-retry. Telemetry may make a kill *less likely* by damping ahead of saturation, but the recovery path is unchanged and remains the safety net.
- A provider **outage / staleness** degrades to exactly today's reactive behaviour: `Sample` returns `ok=false`, `Fresh` returns false, every consumer treats it as "no signal." A telemetry failure MUST NEVER stall, error, or corrupt the sync — the provider's poll-loop errors are logged at WARN and swallowed, never propagated into the apply path (same posture as the metrics server's own "a metrics-server failure shouldn't kill the streamer").

## Consequences

- **New dependency + credential surface, contained.** The PS provider package introduces an HTTP client (and possibly `planetscale-go` for auth/types, already proposed by ADR-0103) and a CONTROL-PLANE service-token credential distinct from the data-plane DSN. sluice surfaces it as explicit opt-in config (`--planetscale-org` + a metrics service-token flag/env, never inferred or reused from the data-plane DSN). All of it lives in `internal/planetscale/telemetry` + `cmd/sluice`; core stays clean.
- **Opt-in, off by default, zero overhead when absent.** No provider wired ⇒ `TargetTelemetry == nil` ⇒ every consumer takes its existing reactive path. The default sync is byte-for-byte unchanged. This satisfies the contain-PS-complexity tenet (the capability is surfaced explicitly, never silently auto-handled).
- **Proactive complements reactive; it does not replace it.** The AIMD/tx-killer/resize machinery remains the correctness floor. Telemetry's value is fewer tx-kills and clearer stalls, not a new correctness guarantee.
- **Most of the inverse sub-item already ships;** the remaining engine-neutral enhancements (progress %, rows/s, event counters) are split to a separate follow-up so a no-credential observability win is never gated behind the PS opt-in.
- **Testing is CI-friendly without a live PS org** (see impl plan): the provider parses a fixed Prometheus-text fixture from an `httptest.Server`; the advisory consumption is unit-tested with a fake `ir.TargetTelemetry` returning canned snapshots; no consumer test needs PlanetScale credentials. A credentialed smoke test stays behind a `psverify`-style tag for operators.
- **Phasing keeps risk low:** Phase 1 lands the engine-neutral seam + the advisory consumption wiring + the inverse `sluice_target_*` re-export, all testable with a fake provider; Phase 2 adds the real PlanetScale HTTP provider (the only piece needing live-doc re-confirmation + credentials). The seam can merge and be reviewed before the HTTP client exists.

## Non-goals

- **Does NOT replace the item-19 data-plane throttle signal.** The documented metrics set has **no throttler-state or tx-kill metric**, so Vitess throttle resilience (item 19) still needs `SHOW VITESS_THROTTLED_APPS` over the data plane. Telemetry complements it (CPU/mem saturation often precedes a throttle), it does not subsume it.
- **No PS leakage into the engine-neutral core.** `internal/ir` and `internal/pipeline` never import the PS provider or branch on PlanetScale; the only composition point is `cmd/sluice`.
- **Not a control input that can stall or pause the stream.** Telemetry can only bias the existing adaptive levers toward caution and emit operator-facing signals. It never gates position advancement, never pauses apply, never refuses.
- **Does not auto-tune `--apply-concurrency`'s lane COUNT in Phase 1.** Using telemetry connection headroom to inform the auto lane count (item-31's MySQL gap) is a deliberate Phase-3 follow-up; Phase 1's proactive back-off only damps batch SIZE within the already-resolved lane count.
- **No deploy-request / schema-mutation behaviour** — that is ADR-0103's separate control-plane capability. This ADR only READS metrics; it never mutates the target's branches.

## Alternatives considered

- **Make telemetry a target-engine method (`ir.TargetTelemetry` on the Engine, like the connection-budget prober).** Rejected: the credential is control-plane (service token + org/branch), not the data-plane DSN, so an engine method would force the engine to hold control-plane creds — the exact PS-sprawl-into-core leak the tenets forbid. Config-wiring the provider into the streamer keeps the engine ignorant of the control plane.
- **Have the applier hot loop poll the metrics endpoint directly.** Rejected: a live control-plane round-trip on the apply path would add latency and a failure mode to the correctness-critical loop. The poll loop runs OFF the hot path at ~15-30s and `Sample` returns a cached value — the apply path never blocks on the network.
- **Fold the inverse "export sluice's own metrics" sub-item into this ADR.** Rejected: it is engine-neutral and credential-free; coupling it to the PS opt-in would wrongly gate a broadly-useful observability win. Split to a separate follow-up; most of it already ships.
- **Use `prometheus/client_golang` to both scrape and re-export.** Deferred: the existing hand-written exposition encoder ([`metrics.go`](../../internal/pipeline/metrics.go)) is sufficient for the small `sluice_target_*` gauge family, and the proto-ADR already records the "no new dependency for /metrics" stance. The PS *provider* needs an HTTP client + a Prometheus-text *parser* (not the full client lib); a minimal parser over the documented metric names is enough and keeps the dependency surface small. Revisit if histogram/label aggregation becomes load-bearing.
- **Treat telemetry as authoritative (let it directly set batch size / pause apply).** Rejected outright by the advisory-only invariant: a control-plane metric is laggy (~10-min SD refresh on the discovery side, ~15-30s on our poll), occasionally wrong, and can go stale; letting it drive the apply path authoritatively would risk a stall or mis-size on bad data. It biases the existing reactive levers; the reactive levers stay the floor.
- **Status quo — operator watches PlanetScale's metrics in their own Grafana, sluice stays reactive.** Entirely safe (no silent loss) and the correct default until demand appears — which is why this started demand-gated (the capability has since shipped and the ADR is Accepted; the status header carries the versions). The win it leaves on the table is exactly the proactive-back-off + in-tool observability this ADR scopes.

## Related ADRs

- [ADR-0052](adr-0052-aimd-apply-batch-size-controller.md) — the AIMD apply-batch-size controller this ADR's use (a) supplies an advisory hint to; the `BatchSizeProvider`/`BatchObserver` advisory-surface pattern this design follows.
- [ADR-0104](adr-0104-mysql-pipelined-cdc-apply.md) / [ADR-0105](adr-0105-postgres-concurrent-cdc-apply.md) — the per-lane concurrent apply + per-lane AIMD the proactive hint wires into.
- [ADR-0106](adr-0106-default-adaptive-apply-concurrency.md) — item 31, fast-by-default `--apply-concurrency`; use (a)'s hint biases its now-default per-lane path, and the connection metrics address its MySQL "no connection-slot probe" gap.
- [ADR-0103](adr-0103-forward-index-ddl-during-cdc.md) — the PlanetScale control-plane CREDENTIAL posture (service token distinct from the data-plane DSN, opt-in/report stance) this ADR mirrors for the metrics token; the sibling PS control-plane capability (deploy requests) whose schema-mutation behaviour is explicitly out of scope here.
- [ADR-0069](adr-0069-service-mode-readyz.md) / the sync-health monitoring proto-ADR — the existing `/metrics` + `/readyz` surface use (c) extends with `sluice_target_*` gauges, and where the inverse "richer own-metrics" sub-item belongs.

## Implementation plan

See the sibling [adr-0107-impl-plan.md](adr-0107-impl-plan.md) for the phased steps, exact files/packages, Go signatures, config/flag wiring, LOC estimate, and the CI-without-a-live-PS-org test strategy.
