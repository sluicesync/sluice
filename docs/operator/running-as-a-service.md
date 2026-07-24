# Running sluice as a long-lived service

`sluice sync start` is a daemon — it opens a CDC stream from the source
and applies changes to the target indefinitely. On a server, container,
or orchestrator (systemd, docker, k8s, Heroku) you'll want three things
the standard daemon contract expects: a way to scrape metrics, a
liveness probe so the orchestrator doesn't kill the process while it
boots, and a readiness probe so the orchestrator doesn't depend on the
stream being live before sluice has actually started mirroring.

`--metrics-listen ADDR` on `sluice sync start` turns on a small HTTP
server alongside the streamer with all three endpoints. Off by default;
opt-in.

```
sluice sync start \
  --stream-id myapp-prod \
  --source mysql://... \
  --target pg://... \
  --metrics-listen :9090
```

## The three endpoints

| Path       | Status         | Body          | When to use                                              |
|------------|----------------|---------------|----------------------------------------------------------|
| `/metrics` | always 200     | Prometheus    | Scrape from Prometheus / Grafana Agent / Datadog Agent.  |
| `/healthz` | always 200     | `ok\n`        | Liveness — "is the process responsive?" Restart on fail. |
| `/readyz`  | 503 → 200      | `not ready\n` then `ready\n` | Readiness — "is the stream actively mirroring?" Gate traffic / mark unit started on this. |

`/readyz` flips from 503 to 200 once the streamer has finished cold-start
(snapshot + bulk-copy + schema apply) or warm-resume and is about to
begin the apply loop. The signal is monotonic: a streamer that loses the
stream exits, the orchestrator restarts the process, and the new
process's `/readyz` starts at 503 again.

**`/readyz` does not check lag.** A streamer that has begun applying
but fallen behind still reports ready; alert on lag via the
`sluice_seconds_since_last_apply` gauge in `/metrics`, not via a
readiness probe. See [ADR-0069](../adr/adr-0069-service-mode-readyz.md)
for the design rationale.

## Structured logs

The endpoints cover the probe side of the daemon contract; the log stream is the other half. By default sluice logs human-readable text to stderr. For ingestion into Loki, Datadog, CloudWatch, or any other structured-log pipeline, add the global `--log-format=json` flag (valid on every subcommand) to emit one JSON object per line instead:

```
sluice --log-format=json sync start --stream-id myapp-prod ...
```

```json
{"time":"2026-07-03T14:07:31.802819-07:00","level":"INFO","msg":"stream starting","stream_id":"myapp-prod"}
```

Logs go to stderr in both formats, so point your collector at the service's stderr stream — under systemd that's the journal; under docker and Kubernetes the container log driver picks it up automatically. `--log-level` (`debug`, `info`, `warn`, `error`) controls verbosity independently of the format.

When a terminal error belongs to a class with a stable error code, the final ERROR record additionally carries `code` and `hint` attributes (e.g. `"code":"SLUICE-E-COLDSTART-TARGET-NOT-EMPTY"`), so a log pipeline or an agent can branch on the class without regexing the message text. The full code table lives in [error-codes](error-codes.md).

## Exit codes

The restart policies below key off the exit status, so here is the contract. sluice exits 0 only on success (for `sync start`, only when `sluice sync stop --wait` completed the drain); everything else is non-zero and warrants whatever your orchestrator's failure path is.

| Exit code | Meaning |
|---|---|
| 0 | Success / clean drain. |
| 1 | Generic runtime failure (also: `verify`/`diff`/`sync-health` found drift — those commands' long-standing per-command meaning). |
| 2 | Config error — the `--config` file could not be loaded or parsed (the read-side check commands have always used 2 for "could not run the check at all"). |
| 3 | Named refusal — sluice refused to proceed and named the remedy (e.g. cold-start into a populated target). Restarting without acting on the hint fails identically, so pair `Restart=on-failure` with an alert on repeated exit-3s rather than counting on the retry. |
| 80 | Usage error — kong (the CLI parser) rejects unknown flags/commands with exit 80 before sluice runs. |

Codes 0 and 1 have meant this since the first release; 2 and 3 were carved out of the generic 1 later, so a script checking `!= 0` is unaffected while a script checking `== 1` specifically may need updating. Details and the error-code registry: [error-codes](error-codes.md).

## systemd

`/etc/systemd/system/sluice-sync.service`:

```ini
[Unit]
Description=sluice CDC sync (myapp prod → analytics)
After=network-online.target
Wants=network-online.target

[Service]
Type=notify-reload
EnvironmentFile=/etc/sluice/myapp-prod.env
ExecStart=/usr/local/bin/sluice sync start \
  --stream-id myapp-prod \
  --source ${SLUICE_SOURCE_DSN} \
  --target ${SLUICE_TARGET_DSN} \
  --metrics-listen 127.0.0.1:9090

# Restart on any non-success exit. sluice exits 0 only when
# `sluice sync stop --wait` completed the drain; everything else
# warrants a restart.
Restart=on-failure
RestartSec=10s

# Liveness via /healthz. A process that stops responding to HTTP for
# more than 60 s is considered hung and gets SIGTERM.
WatchdogSec=60s

# Cold-start can take a while (snapshot + bulk-copy). Don't kill
# during it.
TimeoutStartSec=infinity

[Install]
WantedBy=multi-user.target
```

For the watchdog to bite, pair systemd with a tiny shim that polls
`/healthz` and `sd_notify(WATCHDOG=1)` on success; the simplest version
is a 30 s curl loop in `ExecStartPost`. Or skip the watchdog and rely
on `Restart=on-failure` — for a single sluice on a dedicated box,
that's enough.

Mark the unit "Started" only once `/readyz` returns 200:

```bash
# In your deploy script, after `systemctl start sluice-sync`:
until curl -sf http://127.0.0.1:9090/readyz; do sleep 5; done
echo "sluice mirroring is live."
```

## docker / docker-compose

```yaml
services:
  sluice:
    image: ghcr.io/sluicesync/sluice:latest
    command:
      - sync
      - start
      - --stream-id=myapp-prod
      - --source=${SLUICE_SOURCE_DSN}
      - --target=${SLUICE_TARGET_DSN}
      - --metrics-listen=:9090
    ports:
      - "9090:9090"
    restart: unless-stopped
    # The official image (ghcr.io/sluicesync/sluice) is distroless — no shell
    # or curl — so an in-container `healthcheck:` that shells out to curl won't
    # work. Rely on `restart: unless-stopped` for liveness, and gate readiness
    # externally on /readyz (the poll-before-flip pattern above). If you need a
    # Compose-native healthcheck, run a curl-bearing sidecar that probes
    # http://sluice:9090/healthz, or use Kubernetes (its httpGet probes hit the
    # endpoints directly — see below).
```

Docker's `healthcheck` is liveness-only — it doesn't have a separate
readiness concept. If you have a load balancer in front of services
that depend on sluice being mirroring, gate them on `/readyz` from the
outside (script that polls before flipping traffic) rather than from
docker.

## Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sluice-sync
spec:
  replicas: 1                  # CDC streams are single-writer per stream-id
  strategy:
    type: Recreate             # No rolling — two processes on one slot is a bug
  template:
    spec:
      containers:
      - name: sluice
        image: ghcr.io/sluicesync/sluice:0.89.0
        args:
        - sync
        - start
        - --stream-id=myapp-prod
        - --metrics-listen=:9090
        env:
        - name: SLUICE_SOURCE_DSN
          valueFrom: { secretKeyRef: { name: sluice-dsns, key: source } }
        - name: SLUICE_TARGET_DSN
          valueFrom: { secretKeyRef: { name: sluice-dsns, key: target } }
        ports:
        - name: metrics
          containerPort: 9090
        livenessProbe:
          httpGet: { path: /healthz, port: metrics }
          # Cold-start can take a long time; tolerate it.
          initialDelaySeconds: 60
          periodSeconds: 30
          failureThreshold: 3
        readinessProbe:
          httpGet: { path: /readyz, port: metrics }
          # During cold-start /readyz returns 503. Don't mark the pod
          # Ready until the streamer is in the apply loop.
          initialDelaySeconds: 10
          periodSeconds: 10
          failureThreshold: 3   # 30 s window before pod leaves rotation
        resources:
          requests: { cpu: 200m, memory: 512Mi }
          limits:   { cpu: "2",  memory: 4Gi }
---
apiVersion: v1
kind: Service
metadata:
  name: sluice-metrics
spec:
  selector: { app: sluice-sync }
  ports:
  - { name: metrics, port: 9090, targetPort: metrics }
---
# Optional: a PrometheusRule that alerts on lag, since /readyz does NOT.
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata: { name: sluice-lag }
spec:
  groups:
  - name: sluice
    rules:
    - alert: SluiceApplyLagHigh
      expr: sluice_seconds_since_last_apply > 60
      for: 5m
      annotations:
        summary: "sluice stream {{ $labels.stream_id }} is {{ $value }}s behind."
```

**Key choices baked into the manifest above:**

- `replicas: 1` + `strategy: Recreate` — sluice owns the CDC slot on
  the source; two pods sharing a slot is undefined behavior. A rolling
  deploy with `replicas: 2` will starve and confuse both processes.
  Recreate is correct.
- `initialDelaySeconds: 60` on liveness — cold-start commonly exceeds
  the default 0; without the delay k8s will kill the pod mid-bulk.
- Readiness is the gate, not liveness — k8s will keep the pod alive
  during cold-start but won't route traffic / mark dependent services
  ready until `/readyz` flips.

## Heroku

Heroku doesn't have a native readiness probe; the dyno is considered
"up" as soon as the process binds the assigned `$PORT`. For sluice on
Heroku:

```
# Procfile
worker: sluice sync start --metrics-listen=:$PORT
```

…where `$PORT` is what Heroku assigns (you can't pick it). Then run a
release-phase or one-off script that polls `/readyz` against the dyno
and only flips your application config to "use the target" once it
returns 200.

## Prometheus

Scrape the metrics endpoint directly:

```yaml
scrape_configs:
- job_name: sluice
  static_configs:
  - targets: ['sluice-metrics.default.svc.cluster.local:9090']
```

Key gauges to alert on (lag and gap; readiness is not an alerting
signal):

- `sluice_sync_lag_seconds{stream_id="..."}` — how far the target trails
  the source, in seconds. The primary lag alert; above 60 s sustained.
- `sluice_seconds_since_last_apply{stream_id="..."}` — seconds since the
  last applier commit. Ages on a *quiet* source too, so pair it with the
  sync-lag gauge rather than alerting on it alone.
- `count(sluice_stream_known)` — drops to 0 if sluice can't reach the
  target's state table at all.
- `sluice_pg_slot_spill_bytes_total` (PG 14+) — non-zero growth means
  decoded transactions are spilling to disk on the source. Tune
  `logical_decoding_work_mem` (see `docs/postgres-source-prep.md`).

## Metrics reference

Every series `/metrics` can emit, by family. Conditional families follow the honesty contract used throughout: an unobserved signal is **omitted**, never emitted as a misleading `0` — so treat "series absent" as "no signal", not "zero". (A CI test pins this table against the real scrape output, so it cannot silently drift from the code.)

**Stream state** — always emitted; per-stream lines appear once the target's control table has tracked a stream:

| Series | Type | Labels | Meaning | Alert guidance |
|---|---|---|---|---|
| `sluice_seconds_since_last_apply` | gauge | `stream_id` | Wall-clock seconds since the stream's most recent applier commit. | Ages on a quiet source; alert only in combination with `sluice_sync_lag_seconds`, or on a source you know is busy (30–60 s sustained). |
| `sluice_stream_known` | gauge | `stream_id` | Constant 1 for every stream the target has tracked. | Alert when `count(sluice_stream_known)` drops below the expected stream count. |
| `sluice_metrics_scrape_unix_seconds` | gauge | — | Unix timestamp of this scrape. | Scraper-side staleness detection only. |
| `sluice_sync_lag_seconds` | gauge | `stream_id` | Seconds the target trails the source's latest applied commit — 0 when caught up. Omitted until the first source-timestamped change is applied. | The primary lag alert: > 60 s sustained. Distinct from `sluice_seconds_since_last_apply` (which ages on a quiet stream) and `sluice_target_replica_lag_seconds` (target-internal). |
| `sluice_build_info` | gauge | `version`, `commit`, `go_version` | Constant 1 carrying build metadata (the Prometheus exporter convention). | Join into dashboards; no alert. |

**Apply batch-size controller (AIMD)** — emitted while a stream's apply controller is attached. On the default concurrent apply path (`--apply-concurrency` ≥ 2 lanes) each family carries an additional `lane="N"` label, one series per lane; a serial stream emits the lane-less form:

| Series | Type | Labels | Meaning | Alert guidance |
|---|---|---|---|---|
| `sluice_apply_batch_size_current` | gauge | `stream_id`[, `lane`] | The controller's current target apply-batch size. | Diagnostic; a value pinned at the floor means the target can't absorb larger batches. |
| `sluice_apply_batch_size_p95_seconds` | gauge | `stream_id`[, `lane`] | Rolling p95 batch-apply latency over the controller's window. | Rising p95 with a shrinking batch size = target-side pressure. |
| `sluice_apply_batch_size_decreases_total` | counter | `stream_id`[, `lane`] | Multiplicative-decrease events fired. | Alert on persistent growth — the controller is oscillating against a saturated target. |
| `sluice_apply_batch_size_cooloff` | gauge | `stream_id`[, `lane`] | 1 while the controller is in its post-decrease cool-off. | Diagnostic. |
| `sluice_apply_batch_size_telemetry_damped` | gauge | `stream_id`[, `lane`] | 1 while the controller is damping on a target-telemetry saturation signal (ADR-0107). | Diagnostic; sustained 1 means the target's control plane reports saturation. |

**Postgres slot spill** — PG sources on PG 14+, when the stream's metrics server has a source connection to probe (`pg_stat_replication_slots`); cumulative since slot creation, reset only by drop + recreate:

| Series | Type | Labels | Meaning | Alert guidance |
|---|---|---|---|---|
| `sluice_pg_slot_spill_txns_total` | counter | `stream_id`, `slot` | Transactions that spilled to disk during logical decoding. | Growth means decoding is exceeding `logical_decoding_work_mem` (default 64 MB) — raise it on the source, or split large application transactions. |
| `sluice_pg_slot_spill_bytes_total` | counter | `stream_id`, `slot` | Bytes of decoded transaction data spilled to disk. | Same remedy; alert on sustained rate-of-change. |

**Target health re-export** — only with the opt-in control-plane telemetry wiring (`--planetscale-*` flags, ADR-0107); each line additionally gated on its metric actually being observed:

| Series | Type | Labels | Meaning | Alert guidance |
|---|---|---|---|---|
| `sluice_target_cpu_util` | gauge | `stream_id` | Target CPU utilisation, fraction in [0,1]. | > 0.9 sustained: the target is the bottleneck; expect AIMD damping. |
| `sluice_target_mem_util` | gauge | `stream_id` | Target memory utilisation, fraction in [0,1]. | As above. |
| `sluice_target_storage_util` | gauge | `stream_id` | Target storage volume utilisation, fraction in [0,1]. | Alert well before 1.0 — managed platforms resize with a serving pause. |
| `sluice_target_storage_available_bytes` | gauge | `stream_id` | Storage bytes still available before a resize. | Pair with the util fraction for absolute headroom. |
| `sluice_target_storage_capacity_bytes` | gauge | `stream_id` | Storage volume capacity in bytes. | Reference for the two above. |
| `sluice_target_replica_lag_seconds` | gauge | `stream_id` | Target-internal replica lag reported by the control plane. | Secondary signal; not sluice's apply lag. |
| `sluice_target_active_connections` | gauge | `stream_id` | Target active connection count. | Alert as it approaches the max below. |
| `sluice_target_max_connections` | gauge | `stream_id` | Target connection budget. | Reference for the above. |

**Process self-health** — always emitted; sluice's own resource use (the load-bearing signal for the bounded-memory guarantees of the copy/apply paths):

| Series | Type | Labels | Meaning | Alert guidance |
|---|---|---|---|---|
| `sluice_go_goroutines` | gauge | — | Goroutines currently running. | Unbounded growth = a leak; file a bug with a `sluice diagnose` bundle. |
| `sluice_go_gomaxprocs` | gauge | — | Configured GOMAXPROCS (concurrency ceilings derive from it). | Reference. |
| `sluice_go_memstats_heap_alloc_bytes` | gauge | — | Live heap bytes in use. | Alert against your container/unit memory limit. |
| `sluice_go_memstats_heap_sys_bytes` | gauge | — | Heap bytes obtained from the OS. | As above. |
| `sluice_go_memstats_heap_objects` | gauge | — | Live heap object count. | Diagnostic. |
| `sluice_go_gc_completed_total` | counter | — | Completed GC cycles. | Diagnostic. |
| `sluice_go_gc_pause_seconds_total` | counter | — | Cumulative stop-the-world GC pause time. | Rate spikes correlate with apply-latency spikes; usually memory pressure. |

The emitters live in `internal/pipeline/metrics.go`; the design doc with
full rationale is `docs/dev/design/sync-health-monitoring.md`.

## When something goes wrong: `sluice diagnose`

The first support step for a misbehaving service — before log spelunking,
and always before filing an issue or paging whoever operates the source —
is a diagnose bundle:

```
sluice diagnose --stream-id myapp-prod \
  --target-driver postgres --target "$SLUICE_TARGET_DSN" \
  --output bundle.zip
```

It assembles a ZIP of the stream's control-table state, engine health
probes, capabilities, and (optionally) target-health telemetry — the
state another person needs to reason about your stream without access to
your databases. `--privacy` tiers what's included: `basic` is state-table
dumps only (no version, no DSN, no logs), the default `standard` adds
redacted CLI args + version + health probes, `verbose` adds per-table
row counts and the last 200 log lines. See
[ADR-0056](../adr/adr-0056-sluice-diagnose-operator-bundle.md) for the
exact inclusion/exclusion contract.

For unattended services, the long-running subcommands (`sync start`,
`migrate`, `sync from-backup run`) take `--diagnose-on-crash-dir DIR` —
when the process exits with an error, a bundle is written there
automatically, so the state at failure is captured even when nobody was
watching. Off by default (an unattended bundle on disk is a privacy
decision); `--diagnose-on-crash-privacy` defaults to `basic`.
