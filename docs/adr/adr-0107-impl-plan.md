# ADR-0107 implementation plan — PlanetScale target-health telemetry

Companion to [adr-0107-planetscale-metrics-integration.md](adr-0107-planetscale-metrics-integration.md). This is the phased build plan: files/packages, Go signatures, config/flag wiring, the CI-without-a-live-PS-org test strategy, and a LOC + chunk-count estimate. Nothing here is built yet except the engine-neutral seam ([`internal/ir/target_telemetry.go`](../../internal/ir/target_telemetry.go), interface-only).

## Guiding constraints (from the ADR + tenets)

- **Zero PS in core.** `internal/ir` and `internal/pipeline` define and consume only the engine-neutral interface; the PlanetScale provider package is imported nowhere but `cmd/sluice` (the composition root).
- **Advisory-only.** Every consumer degrades to today's reactive behaviour when the provider is nil/stale. No telemetry path can advance a position, grow a batch past `Ceiling`, or stall the stream.
- **Off the hot path.** The provider polls at ~15-30s in its own goroutine; `Sample` returns a cached snapshot and never blocks on the network.
- **Opt-in.** Wired only when the operator supplies `--planetscale-org` + the metrics service token; default sync is byte-identical to today.

## Phasing overview

| Phase | Scope | Needs live PS? | Chunk size |
|---|---|---|---|
| 1 | Engine-neutral seam + advisory consumption (back-off hint, storage signal, `sluice_target_*` re-export) — all driven by a FAKE provider | No | ~1 chunk (~450-650 LOC incl. tests) |
| 2 | Real PlanetScale HTTP provider (`internal/planetscale/telemetry`) + CLI/config flags + composition wiring | Credentialed smoke only (rest fixture-driven) | ~1 chunk (~500-700 LOC incl. tests) |
| 3 (follow-up) | Telemetry-informed auto lane COUNT (item-31 MySQL connection-probe gap); split inverse "richer own-metrics" enhancements | No | small, demand-gated |

Phase 1 is reviewable + mergeable before any HTTP client exists. Phase 2 is the only piece needing live-doc re-confirmation + credentials.

**Status (Phase 2 landed):** Phase 1 shipped (the seam + advisory consumers + `sluice_target_*` re-export). Phase 2 — the real `internal/planetscale/telemetry` provider (SD + signed scrape + Prometheus-text parser + distillation), the `--planetscale-org` / `--planetscale-metrics-token-id` / `--planetscale-metrics-token` / `--planetscale-metrics-branch` / `--planetscale-metrics-database` CLI flags, and the composition-root wiring (loud all-or-nothing refusal on an incomplete opt-in; nil provider ⇒ byte-identical default sync) — is implemented and fixture-tested, with a `psverify`-gated credentialed smoke. With Phase 2 in, **ADR-0107 moved to Accepted** — the canonical ADR's status header records the flip and the shipped versions (Phase 1 v0.99.92, Phase 2 v0.99.95, Phase 3 v0.99.106, items 35/36 v0.99.107–108); this impl-plan is the historical sidecar.

---

## Phase 1 — engine-neutral seam + advisory consumption (fake-provider driven)

### Already landed (with this ADR)

- **`internal/ir/target_telemetry.go`** — `TargetTelemetry` interface (`Sample(ctx) (TargetHealthSnapshot, ok bool)`), `TargetHealthSnapshot` value (CPU/mem/storage fractions + `*Known` flags, raw storage bytes, secondary lag/conn), and `TargetHealthSnapshot.Fresh(now, window) bool`. Builds + gofumpt-clean.

### 1a. Streamer config field + plumb-through

- **`internal/pipeline/streamer.go`** — add one field:
  ```go
  // TargetTelemetry, when non-nil, is an advisory control-plane health
  // provider (ADR-0107). Consulted off the hot path for proactive
  // back-off, storage-resize anticipation, and operator observability.
  // nil ⇒ every consumer takes its reactive path (the default). Wired
  // only by cmd/sluice when the operator opts into PlanetScale metrics.
  TargetTelemetry ir.TargetTelemetry
  ```
- **`cmd/sluice/cli.go`** (the streamer-build block ~line 1106, next to `MetricsListen`) — `TargetTelemetry: telemetryProvider,` where `telemetryProvider` is constructed in Phase 2 (nil in Phase 1; the field exists and is exercised by tests with a fake).

### 1b. Proactive back-off hint into the AIMD controller (use a)

The controller already takes optional config; add an advisory hint it consults under its existing mutex during `ObserveBatch`.

- **`internal/appliercontrol/controller.go`** — add to `Config`:
  ```go
  // TelemetryHint, when non-nil, is an advisory proactive-saturation
  // signal (ADR-0107). Consulted under the controller mutex during
  // ObserveBatch: a fresh snapshot reporting CPU or mem at/above the
  // high-water mark suppresses additive-increase and may trigger one
  // multiplicative-decrease on a fresh saturation edge. It can ONLY
  // push toward smaller/held sizes — never raise the ceiling or grow a
  // batch. Nil (the default) is a no-op; a stale snapshot is ignored.
  TelemetryHint TelemetryHint

  // TelemetryHighWater is the CPU/mem utilisation fraction at/above
  // which the hint damps. Default 0.85 when zero.
  TelemetryHighWater float64
  ```
  And a tiny engine-neutral hint surface in the same package (NOT in `ir`, so the controller stays import-light — it already mirrors `ir.RetriableError`/`ir.TransactionKilledError` structurally rather than importing them):
  ```go
  // TelemetryHint is the advisory proactive-saturation surface the
  // controller consults. The streamer adapts an ir.TargetTelemetry into
  // one of these (so appliercontrol stays free of the ir import for the
  // telemetry path, mirroring the retriable/txKilled structural shapes).
  type TelemetryHint interface {
      // Saturated reports whether the target is at/above the high-water
      // mark on a FRESH snapshot. ok=false ⇒ no usable signal (degrade).
      Saturated() (saturated bool, ok bool)
  }
  ```
- In `ObserveBatch`, after the tx-killer / retriable / latency MD checks and BEFORE the AI branch: if `cfg.TelemetryHint != nil` and `Saturated()` returns `(true, true)`, take the "hold / one-edge MD" path instead of AI. Edge-detection via a `lastTelemetrySaturated bool` controller field so a sustained-hot target shrinks once then holds (not repeatedly). All within `[Floor, Ceiling]` — invariants unchanged.
- Add a `MetricsSnapshot.TelemetryDamped bool` (and a `sluice_apply_batch_size_telemetry_damped` gauge) so operators can see the hint engaged — Phase 1c extends `metrics.go`.

### 1c. Storage-resize anticipation signal (use b) + `sluice_target_*` re-export (use c)

- **`internal/pipeline/metrics.go`** — add `AttachTargetTelemetry(t ir.TargetTelemetry)` mirroring the existing `AttachSpillReporter`, plus an `emitTargetTelemetryMetrics(w, snap)` emitting the `sluice_target_{cpu_util,mem_util,storage_util,storage_available_bytes,replica_lag_seconds,active_connections}` gauges (labelled `stream_id`), each gated on its `*Known` flag (omit unobserved metrics — never emit a misleading 0), and the whole block gated on `Sample` returning `ok` (else a `# target-telemetry: no signal` comment, same posture as the spill reporter's error branch).
- **Storage signal**: a small `internal/pipeline/streamer_telemetry.go` helper goroutine (started in the apply phase only when `s.TargetTelemetry != nil`) that, on a slow tick, reads `Sample`, and when `StorageKnown && StorageUtil >= highWater` (or available bytes near the non-Metal ~10 GB grow boundary) logs a structured one-time-per-edge WARN: `"target storage low headroom — a resize/reparent may interrupt apply shortly (items 30/33 will retry transparently)"`. Pure anticipation/explanation; it does NOT pause or gate. Item 30/33's retriable classification remains the actual ride-through.

### 1d. `sluice diagnose` target-health section (use c)

- **`internal/ir/diagnose.go` + the diagnose renderer** — when a telemetry provider is wired, add a "Target health" block (CPU/mem/storage/lag/conn from `Sample`, "no signal" when `ok=false`). Read-only display; no behaviour gates on it.

### Phase 1 tests (no live PS, no credentials)

- **`appliercontrol/controller_test.go`** — a `fakeHint` implementing `TelemetryHint`: assert (i) `(true,true)` suppresses AI even when p95 is healthy; (ii) a fresh saturation edge triggers exactly one MD then holds; (iii) `(_, false)` (stale/no-signal) is a byte-for-byte no-op vs the no-hint path; (iv) the hint can never push above `Ceiling` or below `Floor`. This is the load-bearing advisory-only pin.
- **`pipeline/metrics_test.go`** — a `fakeTelemetry` returning canned `TargetHealthSnapshot`s: assert the `sluice_target_*` exposition shape, `*Known=false` omits the line, `ok=false` emits the comment not 500.
- **`pipeline/streamer_telemetry_test.go`** — the storage-headroom WARN fires once per edge and never pauses the stream; a nil provider is a total no-op.
- `ir/target_telemetry_test.go` — `Fresh` boundary cases (zero `SampledAt`, zero window, exactly-at-window).

---

## Phase 2 — the real PlanetScale provider + flags (the only PS-touching code)

### 2a. CONFIRMED live surface (probed 2026-06-21 against the real `sluicesync` org endpoint)

The docs-404 caveat from the design pass is RESOLVED — the live endpoint was probed directly with the `read_metrics_endpoints` service token and the mechanics below are what Phase 2 was built against. They REPLACE the roadmap-confirmed starting point in the ADR.

**Service-discovery (auth'd):** `GET https://api.planetscale.com/v1/organizations/{ORG}/metrics` with header `Authorization: {TOKEN_ID}:{TOKEN}` → HTTP 200 JSON array (Prometheus HTTP-SD shape). Each element:

```json
{"targets":["metrics.psdb.cloud"],
 "labels":{"__metrics_path__":"/metrics/branch/<id>","__param_sig":"<sig>","__param_exp":"<unixexp>","__scheme__":"https",
           "planetscale_database_name":"<db>","planetscale_branch_name":"main","planetscale_organization_name":"<org>", ...}}
```

Filter to the element whose `planetscale_database_name` == the sync target's database AND `planetscale_branch_name` == the target branch (default `main`).

**Scrape (signed, NO auth header):** `GET https://metrics.psdb.cloud{__metrics_path__}?sig={__param_sig}&exp={__param_exp}` → Prometheus text exposition (`version=0.0.4`), `name{labels} value [timestamp_ms]`. The URL is pre-signed so the token never travels on the scrape leg.

**Metric names + units (MySQL/Vitess), CONFIRMED** — the parser's name table (`internal/planetscale/telemetry/distill.go`):

| Snapshot field | Metric | Units / selection |
|---|---|---|
| `CPUUtil` | `planetscale_pods_cpu_util_percentages` | PERCENTAGE 0–100 → ÷100. Select `planetscale_component="vttablet"` AND `planetscale_tablet_type="primary"` (the write target); graceful fallback when the label is absent. |
| `MemUtil` | `planetscale_pods_mem_util_percentages` | same shape/units/selection as CPU. |
| `StorageUtil` + raw bytes | `planetscale_vttablet_volume_available_bytes`, `planetscale_vttablet_volume_capacity_bytes` (primary vttablet) | `StorageUtil = 1 - available/capacity`; raw bytes into `StorageAvailableBytes`/`StorageCapacityBytes`. (`planetscale_vttablet_table_storage_all_bytes` also exists, unused.) |
| `ReplicaLagSeconds` (secondary) | `planetscale_mysql_replica_lag_seconds` | seconds. |
| `ActiveConnections`/`MaxConnections` (secondary) | `planetscale_edge_active_connections`, `planetscale_mysql_max_connections` | counts. |

Any metric absent from the scrape leaves its value 0 AND its `*Known` flag false — never 0-as-idle (the honesty/loud-failure tenet, pinned in `distill_test.go`). The selection cascade (exact vttablet-primary → any primary-tagged → single unlabelled series → refuse-to-guess on ambiguous multi-pod) lives in `selectPrimaryValue`. A PG-target metric-name table (`planetscale_volume_*`, `planetscale_postgres_*`) is a small follow-up edit to the same table when a PG telemetry target appears.

### 2b. The provider package — `internal/planetscale/telemetry`

```
internal/planetscale/telemetry/
  provider.go        // Provider implements ir.TargetTelemetry
  poll.go            // background poll loop (~15-30s), cached snapshot
  parse.go           // Prometheus-text → ir.TargetHealthSnapshot (name table)
  parse_test.go      // fixture-driven (real exposition text), no network
  provider_test.go   // httptest.Server, no live PS
```

Signatures:
```go
package telemetry

type Config struct {
    Org          string        // --planetscale-org
    TokenID      string        // service token id
    Token        string        // service token secret
    Branch       string        // target branch to filter series to
    Keyspace     string        // (Vitess) keyspace, optional
    PollInterval time.Duration // default 20s; clamped to [10s, 120s]
    Freshness    time.Duration // default 3*PollInterval (the Fresh window)
    BaseURL      string        // override for tests/self-host; default api.planetscale.com
    HTTPClient   *http.Client  // injected in tests
}

// New constructs a provider and STARTS its background poll loop. The
// returned provider satisfies ir.TargetTelemetry. Close stops the loop.
func New(ctx context.Context, cfg Config) (*Provider, error)

func (p *Provider) Sample(ctx context.Context) (ir.TargetHealthSnapshot, bool) // cached, non-blocking
func (p *Provider) Close() error
```

- The poll loop: hit the org HTTP-SD endpoint, select the target-branch series, parse the documented metric names into a `TargetHealthSnapshot{SampledAt: time.Now(), ...}`, store it under a mutex. On poll error: log WARN, KEEP the last good snapshot but let `Fresh` age it out — `Sample` returns `ok=false` once the cached snapshot is older than `Freshness`. NEVER propagate the error to the apply path.
- Dependency: a thin HTTP call + a minimal Prometheus-text parser (the documented metric subset, not full client_golang). If ADR-0103's `planetscale-go` lands first, reuse its auth/client; otherwise a stdlib `net/http` call with the service-token header is sufficient and lighter.

### 2c. CLI flags + composition wiring (`cmd/sluice`)

- **`cmd/sluice/cli.go`** — new flags on the sync command (mirroring `MetricsListen`'s opt-in posture; credentials via env, never positional):
  ```go
  PlanetScaleOrg            string `help:"PlanetScale org slug; enables OPTIONAL target-health telemetry (CPU/mem/storage) from the PlanetScale metrics endpoint for proactive back-off + observability. Opt-in; requires --planetscale-metrics-token-id/-token. Control-plane only — distinct from the data-plane DSN." placeholder:"ORG"`
  PlanetScaleMetricsTokenID string `help:"PlanetScale service-token ID (read_metrics_endpoints permission) for target-health telemetry." env:"PLANETSCALE_METRICS_TOKEN_ID"`
  PlanetScaleMetricsToken   string `help:"PlanetScale service-token secret for target-health telemetry." env:"PLANETSCALE_METRICS_TOKEN"`
  PlanetScaleMetricsBranch  string `help:"Target branch to filter telemetry series to (defaults to the target DSN's branch)." placeholder:"BRANCH"`
  ```
- The composition root constructs the provider ONLY when `PlanetScaleOrg != "" && tokenID != "" && token != ""` (loud error if org is set but creds are missing — opt-in must be complete or refused, never half-on); otherwise leaves `TargetTelemetry` nil. The provider's lifecycle (`Close`) is tied to the sync's context. Mask the token in any echo/log (`pscale_...` redaction, mirroring the DSN redaction discipline).

### Phase 2 tests (CI-safe)

- **`parse_test.go`** — feed a captured real-exposition fixture (one per engine: Vitess + PG metric names) → assert each `TargetHealthSnapshot` field + `*Known` flag, incl. a fixture missing some metrics (assert `*Known=false`, not 0). This is the parser's family-matrix pin.
- **`provider_test.go`** — `httptest.Server` returning the fixture: assert the poll loop populates `Sample`, that a 500/timeout keeps the last snapshot then ages it to `ok=false` past `Freshness`, and that `Close` stops the loop cleanly (`-race`).
- **Credentialed smoke** — a `psverify`-tagged test that hits a real org endpoint, run by operators only; never in default CI (same gate as the broader PlanetScale suites).
- No `internal/ir` / `internal/pipeline` test needs the provider — they use the Phase-1 fakes.

---

## Phase 3 (follow-up, demand-gated)

- **Telemetry-informed auto lane count** — let `autoApplyConcurrency` (item 31's MySQL no-connection-probe gap, `streamer_apply_concurrency.go`) read `ActiveConnections`/`MaxConnections` from `Sample` to derive a real budget for PlanetScale-MySQL instead of the fixed ceiling of 4. Its own small change once Phase 2 is live.
- **Split inverse sub-item** — file a separate roadmap item / tiny ADR for the engine-neutral "richer own-metrics" enhancements not yet shipped (overall progress %, rows/s throughput, tx-killer/resize event counters) on the existing `/metrics` surface. No PS credential; do NOT couple to this capability.

---

## LOC + chunk estimate

- **Phase 1:** ~250-350 LOC production (controller hint + edge state, metrics emit + attach, storage WARN helper, diagnose block, streamer field) + ~200-300 LOC tests. **One chunk.** Non-concurrency-heavy but touches the controller's `ObserveBatch` and adds a poll goroutine → treat as a concurrency chunk for the `-race`-before-tag rule (the storage-WARN goroutine + the controller-under-mutex hint both warrant it).
- **Phase 2:** ~300-400 LOC production (provider + poll + parser + flags + composition) + ~200-300 LOC tests/fixtures. **One chunk.** `-race` on the poll loop.
- **Phase 3:** small, deferred.

Total for the capability proper (Phases 1+2): roughly **~1,000-1,400 LOC incl. tests across two chunks**, Phase 1 mergeable + reviewable independently of Phase 2.

## Gates / checklist per chunk

- `gofumpt -l .`, `go vet ./...`, `golangci-lint run`, `go test ./...` clean (pre-commit hook).
- `go vet -tags=integration ./...` (tagged test files type-check) — the seam adds an `ir` symbol other packages reference.
- `-race` integration green BEFORE the tag (concurrency chunk: poll goroutine + controller mutex).
- Advisory-only pin (controller test iii: no-signal == byte-identical to no-hint) is the non-negotiable correctness gate.
- Default-path-unchanged pin: a sync with `TargetTelemetry == nil` produces identical behaviour/metrics to pre-ADR-0107.

## Open design questions for the operator (carried up from the ADR)

1. **High-water default** — 0.85 CPU/mem for the proactive damp? And should the damp be hold-only, or hold + one-edge MD (the plan proposes hold + one-edge MD)?
2. **Poll cadence** — 20s default acceptable? (PS-side SD refresh is ~10 min, but the metric values update faster; 15-30s balances freshness vs API load.)
3. **Phase split** — ship Phase 1 (seam + fake-driven consumption) as its own release for review before Phase 2's real provider, or land both together?
4. **`planetscale-go` vs stdlib HTTP** — adopt the SDK now (shared with ADR-0103's deploy-request path if that lands) or a minimal stdlib client for the metrics-only read?
