# sluice v0.99.108

**Sync-scoped target-metrics threshold alerts (ADR-0107 item 36): sluice can now notify a webhook or Slack when the PlanetScale target's CPU/memory/storage/lag crosses a threshold — or when storage is climbing fast, a pre-grow early warning.** An opt-in, advisory, failure-isolated observability follow-up to the v0.99.92–v0.99.107 metrics work.

## Added

**The alerter.** When PlanetScale telemetry is configured (`--planet-scale-org` + the metrics token) and at least one sink and threshold are set, a slow-tick sidecar — off the apply hot path, mirroring the metrics-history recorder — evaluates the configured rules each ~60 s and fires a notification on the **rising edge** of a breach.

**Sinks (credential-gated via env vars):**
- `--notify-webhook` (`SLUICE_NOTIFY_WEBHOOK`) — a generic JSON `POST` (`{level, stream_id, metric, title, body, value, threshold, at}`) to your own endpoint/relay.
- `--notify-slack` (`SLUICE_NOTIFY_SLACK`) — a Slack incoming-webhook message (`:rotating_light:` for critical, `:warning:` otherwise).

**Rules (each inert unless its threshold is set):**
- `--notify-storage-util`, `--notify-cpu-util`, `--notify-mem-util` — utilisation (0–1) at or above the fraction.
- `--notify-lag-seconds` — replica lag at or above the value.
- `--notify-storage-growth-per-min` — the **rate-of-change** rule: storage utilisation climbing at or above the given fraction-of-capacity per minute (e.g. `0.02` = +2 %/min), so you hear about an impending non-Metal auto-grow *before* it triggers a reparent.

**Edge-trigger + cooldown + hysteresis.** An alert fires once when a rule first breaches; while still breached it re-fires at most once per `--notify-cooldown` (default 15 m) — a reminder, not a per-poll flood; it re-arms only after the metric recovers below the threshold, with a small hysteresis margin so a value parked right at the line doesn't flap. An unobserved metric never fires (the same honesty contract the rest of the telemetry path keeps).

**Failure-isolated and engine-neutral.** A dead or slow sink is logged at WARN and **swallowed** — it can never stall or crash the sync. The new `internal/notify` sink layer imports no engine or telemetry code; the pipeline maps the telemetry snapshot into a generic notification.

## Compatibility

No configuration changes and no behaviour change for any sync that doesn't configure a notify sink — the sidecar simply never starts. The feature is opt-in (a sink URL + at least one threshold) and only active when `--planet-scale-org` telemetry is wired. Advisory only: it never touches the apply path, position, or exactly-once contract.

Scope note: SMTP/email sinks and a standalone `metrics-watch` daemon remain demand-gated — this release is the in-sync, sync-scoped alerter.

## Who needs this

Anyone running a continuous `sync` into a **PlanetScale** target who wants to be paged when the target gets hot, storage approaches capacity, or an auto-grow is imminent — without standing up a separate Prometheus/Alertmanager stack against the metrics API. Point it at a Slack webhook (or your own endpoint) and set the thresholds you care about.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.108
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.108
```
