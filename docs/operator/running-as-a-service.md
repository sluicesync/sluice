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
    healthcheck:
      # Liveness — restarts the container if the process hangs.
      test: ["CMD", "curl", "-sf", "http://localhost:9090/healthz"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 60s
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

- `sluice_seconds_since_last_apply{stream_id="..."}` — apply lag in
  seconds. Alert above 30–60 s sustained.
- `count(sluice_stream_known)` — drops to 0 if sluice can't reach the
  target's state table at all.
- `sluice_pg_slot_spill_bytes_total` (PG 14+) — non-zero growth means
  decoded transactions are spilling to disk on the source. Tune
  `logical_decoding_work_mem` (see `docs/postgres-source-prep.md`).

The full metric set is generated by hand and lives in
`internal/pipeline/metrics.go`; the design doc with full rationale is
`docs/dev/design-sync-health-monitoring.md`.
