---
name: fleet-operator
description: Use to operate many continuous syncs from one supervised process (a fleet). Drives `sluice sync run` with a syncs.yaml fleet config, the read-only web dashboard, the `sync tui`, and per-leg `sync health`/`sync status --all`. Gated — each leg writes to its target. Trigger when the user asks to run a fleet / many syncs / sync multiple databases from one process / a sync dashboard.
---

# fleet-operator

Run and monitor a fleet of syncs from a single `sluice sync run` process, each failure-isolated with bounded-backoff restart (ADR-0122). State-changing (every leg writes its target); per-leg destructive-flag rules from `cdc-sync-operator` still apply.

## When to use
The user has multiple source→target sync streams and wants one supervised process managing them, with fleet-wide monitoring — instead of one `sync start` per pair.

## Inputs you need
- A `syncs.yaml` **fleet config** (the global `--config` file): a `syncs:` list plus an optional `restart:` policy. Each entry is a curated subset of the `sync start` flags (kebab-case keys: `stream-id`, `source-driver`, `source`, `target-driver`, `target`, `slot-name`, `target-schema`, `control-keyspace` (MySQL/PlanetScale/Vitess target only — the unsharded sidecar keyspace for CDC control tables; omit to auto-detect on a sharded target), `include-table`, `type-override`, `apply-batch-size`, `poll-interval`, per-leg `notify-*`, optional per-leg `planetscale-*`, `zero-date`, …).
- Credentials via env, never committed to the YAML: DSNs' secrets, `SLUICE_NOTIFY_*`, `PLANETSCALE_METRICS_TOKEN_ID`/`_TOKEN`.

## Steps

1. **Validate the fleet config (dry-run first).** `sluice sync run --config syncs.yaml --dry-run` loads and validates every leg without starting them. Fix any config error (exit 2) before running for real.

2. **Run the fleet.** `sluice sync run --config syncs.yaml`. The supervisor starts each leg, isolates failures (one leg crashing doesn't take down the others), and restarts crashed legs with bounded backoff per the `restart:` policy (`backoff-base`, `backoff-cap`, `healthy-run-threshold`, `max-consecutive-failures`).

3. **(Optional) serve the read-only dashboard.** Add `--dashboard-listen :9300` to expose an HTML view + a `/api/fleet` JSON API. **NO AUTHENTICATION** — bind to localhost or a trusted network only. If the address can't bind, the fleet refuses to start (rather than silently running blind).

4. **Attach the terminal dashboard.** `sluice sync tui --connect :9300` (or a full `http://host:9300/api/fleet` URL) polls a running `sync run --dashboard-listen` server — works over an SSH tunnel to a remote fleet.

5. **Monitor per leg (cron/agent-friendly).** Fleet roll-up: `sluice sync status --all --config syncs.yaml --format json` (one table across every configured target; `--watch 2s` to live-refresh, `--summary` for an aggregate header). Per-stream freshness with cron exit codes: `sluice sync health --format json --target-driver <drv> --target <dsn> --stream-id <id> [--max-stale-seconds N]` (exit 0 healthy / 1 breached / 2 operational) — one call per leg you gate on.

## What you return
- **Fleet composition:** N legs, each `stream-id` → source→target, and the restart policy in effect.
- **Startup result:** dry-run validation outcome; which legs came up; any leg refused/looping and why.
- **Monitoring surface:** the `sync status --all` roll-up and per-leg `sync health` verdicts (+ exit codes); the dashboard/TUI address if enabled (with the no-auth caveat).
- **Per-leg issues:** any breached threshold or restart-looping leg named, routed to `sluice-error-triage` / `cdc-sync-operator`.

Per-leg recovery that needs a destructive flag (`--reset-target-data`, `slot drop`, …) is still approval-gated — surface it, don't auto-apply. Keep tokens/URLs in env, never in the committed YAML.

## References (canonical — don't duplicate)
`AGENTS.md` (taxonomy, envelope, env-first credentials) · `docs/operator/running-as-a-service.md` · `skills/cdc-sync-operator/SKILL.md` (per-leg lifecycle) · `sluice sync run --help` / `sluice sync tui --help` / `sluice sync status --help`.
