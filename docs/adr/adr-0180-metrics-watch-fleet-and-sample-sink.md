# ADR-0180: `metrics-watch` goes fleet-wide — org auto-discovery + a persistent sample sink

## Status

**Accepted — implemented 2026-07-24, shipped in v0.102.0.** Roadmap item 75 parts **(b)** and **(c)**. Part **(a)** — the egress metric — is deliberately NOT built here: a live measurement (`workspace/ps-egress-metric-semantics-2026-07-24.md`) falsified its filed plan, and the family choice is unresolved pending a question to PlanetScale. Adding no egress field at all is the honest outcome; the surfaces below are shaped so one can be added later as further nullable record fields + one more gauge family, without reshaping anything.

Shipped surface: `metrics-watch --fleet` watches the whole org (the mode is selected by an EXPLICIT flag — v0.103.x corrected an initial cut that inferred it from an omitted `--planetscale-metrics-db`, which meant a wrapper script with an unset variable fanned out org-wide silently; audit 2026-07-26 ARCH-3); `--include-database` / `--exclude-database` / `--fleet-concurrency` scope and pace the fan-out; `--sink-file` / `--sink-file-max-bytes` / `--sink-file-max-files` / `--sink-http` durably record every polled sample. New packages/types: `ir.FleetTelemetry` + `ir.FleetTarget` + `ir.FleetHealthSample` (the engine-neutral seam), `planetscale/telemetry.Fleet` (the provider), `internal/telemetrysink` (the sink layer).

**Concurrency note:** the fleet provider runs a background poll loop that fans out N concurrent scrapes and swaps a shared cache under a mutex; the file sink serialises writes under its own mutex. `-race` is CI-only on this project, so the Integration job must be green before any tag.

## Context

`metrics-watch` shipped (ADR-0107 leftover) shaped as a per-database ALERTER: one `--planetscale-metrics-db`, threshold rules, `--notify-*` sinks, and an optional `--metrics-listen` Prometheus endpoint. An operator surfaced a different use case (2026-07-24): *informational fleet telemetry* — collect CPU/mem/storage/lag for every database in a PlanetScale org, on the polling cadence, and keep it outside PlanetScale for their own portal. Two gaps blocked it: the watch could only see one database, and its only durable output required standing up a Prometheus to scrape it.

## Decision 1 — discover the org from the metrics SD document, not from the databases API

The roadmap entry proposed enumerating the org with `ListDatabases(org)` + per-database `ListBranches(org, db)` over the existing control-plane client. **That is not what this implements.**

The single-database provider already calls `GET /v1/organizations/{org}/metrics` — a Prometheus HTTP-SD document — and then *filters it down to one element*. Probed live against the `sluicesync` org (2026-07-24), that one call returns **one element per database+branch for the whole org**, each carrying `planetscale_database_name`, `planetscale_branch_name`, `planetscale_organization_name`, `__scrape_interval__=1m`, and its own **signed** scrape URL.

Using it as the discovery source wins on four counts:

- **One control-plane call per refresh, regardless of org size** — the filed route costs 1 + N. Rate limits are the explicit gotcha for this chunk.
- **It lists exactly the branches that HAVE a metrics endpoint** — the fan-out can never poll a target that cannot answer, so there is no "discovered but permanently failing" class to explain away.
- **The signed scrape URL arrives with the element** — a poll cycle is one authenticated call plus N unauthenticated scrapes.
- **No new permission** — the feature already requires `read_metrics_endpoints`; database/branch listing is a different scope, and demanding it would widen the credential for no gain.

So `client.discover` (single) and `client.discoverAll` (fleet) are two views of the same call, and no new API surface was added.

### The one thing the SD document does not carry: the engine

There is **no engine/kind label** on any SD element (probed; the full label set is in the code comment). And a real org is MIXED — `sluicesync` itself runs four Vitess/MySQL databases and two Postgres ones. A single declared `--engine` would therefore read the wrong metric-name table for half the fleet.

The resolution is `metricNamesForExposition`: pick the per-engine table from **marker-metric presence in the target's own exposition**, falling back to the declared `--engine` table when the exposition is ambiguous (both markers, or neither). Ground truth (2026-07-24, six live branches) is that the two surfaces are **disjoint** — a Vitess branch carries `planetscale_vttablet_volume_*` and never `planetscale_volume_*`; a Postgres branch carries `planetscale_volume_*` and never the vttablet spelling.

Disjointness is what makes this a resolution rather than a guess: **picking wrong can only fail to find a series** — the `*Known` flag stays false, the honest "unobserved" degrade — it can never attribute one engine's number to the other's signal. The scan is order-independent, and ambiguity falls back rather than letting whichever series happened to be listed first decide. The single-database path does not use this at all; it keeps the declared table byte-for-byte.

## Decision 2 — a separate fleet loop, not a generalised single loop

`RunMetricsWatch` branches once on `cfg.Fleet != nil` and hands off to `runFleetMetricsWatch`. The single-database loop below that branch is untouched, which is the point: the original mode had to stay byte-identical, and the cheapest proof of that is that its code did not move.

Everything downstream of "read the samples" is nevertheless SHARED — the rule set, the evaluator, the edge-trigger/cooldown/hysteresis latches, the failure-isolated notifier fan-out, the sink. The extraction that made this possible is `deliverMetricsNotifyTick`, the second half of `runMetricsNotifyTick`, so a caller holding an already-read snapshot evaluates through the same freshness check and the same evaluator. Fleet mode differs in exactly three places:

- **Per-target latch state** — a `map[ir.FleetTarget]*metricsNotifyState`, so one database's breach cannot re-arm or suppress another's. It is pruned each tick against the discovered set, so a removed database does not leak state.
- **The exporter's label set** — `{database,branch}` instead of `{stream_id}`. This needed a genuinely different emitter, not a loop over the existing one: Prometheus exposition allows **one `# HELP`/`# TYPE` block per metric name**, so the fan-out groups by metric and emits the header once. Calling the per-target emitter N times would produce N duplicate HELP lines — text a strict scraper rejects. Two fleet gauges (`sluice_fleet_targets`, `sluice_fleet_targets_observed`) make absence legible rather than ambiguous.
- **The live line collapses to ONE summary row** — targets/observed plus the worst reading per metric with the database that produced it. The roadmap's gotcha (3) is right: a per-database line is unreadable across dozens of databases, so org-wide mode leads with the exporter and the sink.

`--planetscale-metrics-branch` deliberately means something different in the two modes: single-database defaults it to `main`; **org-wide leaves it unset meaning EVERY branch**, because a fleet watch that silently skipped non-main branches would under-report the org — the opposite of what the mode is for.

`--include-database`/`--exclude-database` are `path.Match` globs evaluated against both `database` and `database/branch`. **They may be combined, with exclude winning** — a deliberate divergence from `--include-table`/`--exclude-table`, which are mutually exclusive. A fleet watch's natural expression is "the whole prefix, minus these two"; forcing a choice would make that unsayable, and deny-beats-allow is the unambiguous precedence.

## Decision 3 — the sample record is a CODEC, and it is validated as one

`internal/telemetrysink` is the durable sibling of `internal/notify`: same standalone posture (imports no engine package and not `internal/ir` — the caller maps its view into a plain `Record`), same advisory + failure-isolated contract (a dead sink is logged and swallowed, never stalling or failing a poll).

Because a record round-trips through a store, it gets the full new-surface treatment:

- **Every metric is a POINTER.** An unobserved metric serialises as an explicit `null`, never a `0` a portal would plot as "idle" — the persisted form of the `*Known` honesty contract. No `omitempty`: an absent key and a null key would otherwise be indistinguishable.
- **`EncodeRecord` is the ONE encoder for both sinks.** The HTTP push carries pre-encoded `json.RawMessage` entries, so the bytes on the wire per record are byte-identical to the file line — structurally, not coincidentally, which means a validation refusal cannot apply to one sink and not the other.
- **It refuses what `encoding/json` would silently rewrite**, naming the field and the row's target: a string that is not valid UTF-8 (the resume-cursor CRITICAL's exact mechanism — U+FFFD substitution), and a non-finite float (NaN/±Inf, which has no JSON spelling). The refusal is per-row: its siblings in the batch still land, and no partial line is ever written.
- **Integers ride as exact decimal digits**, so an int64 beyond 2^53 survives the write. The consumer-side contract (decode into a 64-bit type or `UseNumber`, not the `any`/float64 default) is stated in the package doc and pinned by a test that reads the line back with `UseNumber` — a reader that is not the writer's convenience path.

The gate that makes this durable is `TestEncodeRecord_ValidatorCoversEveryField`: it walks `Record` **by reflection** and asserts every string field is UTF-8-validated and every `*float64` field is finiteness-validated, with vacuity guards on the counts. A future field — the roadmap-75a egress figures, say — that skips the validator fails there rather than silently shipping a mangling path. This is the "pin the class, not the representative" discipline applied to the schema itself instead of to a hand-listed set of fields.

Rotation uses **numbered generations** (`PATH.1` newest … `PATH.N`) rather than timestamped names: the newest rotated file is always `.1`, so a consumer needs no name parsing and no clock, and two rotations in the same second cannot collide. A whole tick's batch is written atomically with respect to rotation, so a consumer never finds half a fleet's tick in each of two files. `--sink-file-max-bytes` negative disables rotation for operators running an external logrotate.

Zero-value safety (the v0.99.51 trap): the sink adds **no bool that must default on**. Every knob is "0 ⇒ the sane default", and a nil `Sink` is off — so every non-CLI construction (tests, future callers) gets today's behaviour.

## Consequences

- An operator with a `read_metrics_endpoints` token can now point one process at an org and get a Grafana-ready `/metrics` covering every database, and/or a JSONL file they own, with no Prometheus and no per-database configuration.
- The fan-out is bounded (`--fleet-concurrency`, default 4, max 16) and keeps the 60s cadence, which matches the endpoint's own advertised `__scrape_interval__=1m`.
- Validated live against the real `sluicesync` org: 6 targets discovered from one SD call, correct per-engine distillation across a mixed org, filters, the JSONL sink, and the grouped exposition (`internal/planetscale/telemetry/fleet_psverify_test.go`, plus an end-to-end CLI run).

### Known gap surfaced by this work (pre-existing; NOT introduced here)

The live fleet pin exposed something the fixture-based tests could not: **on a real Postgres branch, storage reads `n/a`.** The `planetscale_volume_*` series are per-pod and labelled `planetscale_role="primary"|"replica"` — not `planetscale_tablet_type`, and with no `planetscale_container` — so `selectPrimaryValue`'s cascade sees three indistinguishable series and honestly refuses to guess. That is the `*Known` contract working correctly (no wrong number), but it means PG storage has been unobserved on the live endpoint in **single-database mode too**, and a `--notify-storage-util` rule can never fire on a PG target.

It is deliberately not fixed here: teaching `selectPrimaryValue` the `planetscale_role` label changes the single-database PG path (which this chunk had to leave byte-identical) and needs its own reduction decision — the primary pod's volume, or the fullest volume across pods? — of exactly the kind item 75(a) exists to caution against answering by guess. Same probe also falsified `postgresMetricNames`' comment that the endpoint exposes no PG replica-lag series: `planetscale_postgres_replica_lag_seconds` (per-replica) and `planetscale_postgres_settings_max_connections` / `planetscale_edge_postgres_active_connections` are all live today. Both are filed as follow-ups on roadmap item 75.

## Alternatives considered

**Enumerate the org via `ListDatabases`/`ListBranches`** (the filed plan) — rejected above: more calls, a wider credential, and it can list branches with no metrics endpoint.

**Infer the engine from the declared `--engine` for the whole org** — rejected: the reference org is mixed, so half the fleet would read the wrong table. The marker scan degrades to unobserved rather than wrong, which the declared-only route does not.

**Emit per-database series by calling the existing single-target emitter in a loop** — rejected: duplicate `# HELP` lines per metric name make the exposition invalid.

**Reuse the parquet writer for the sink** — considered per the roadmap's suggestion and rejected: parquet is a columnar batch format that wants schema declaration and file finalisation, which fits an export, not an append-per-minute stream. JSONL appends, tails, greps, and rotates with no framing of its own, and DuckDB reads it directly (`read_json_auto`) if an operator wants columnar analysis later.

**Persist through the existing `--notify-*` webhook** — rejected: those are edge-triggered ALERTS with a threshold-shaped payload. Sending every sample through them would conflate "something is wrong" with "here is a reading" and give the sample stream an alert's schema.
