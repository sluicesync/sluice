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

2. **Run the fleet.** `sluice sync run --config syncs.yaml`. The supervisor starts each leg, isolates failures (one leg crashing doesn't take down the others), and restarts crashed legs with bounded backoff per the `restart:` policy (`backoff-base`, `backoff-cap`, `healthy-run-threshold`, `max-consecutive-failures`). **One exception:** a leg that stopped with `UNFORWARDED-SCHEMA-CHANGE` (the source changed a constraint, policy, RLS or default that CDC cannot forward) is **not** restarted. It shows `failed`, with an ERROR log line, while the other legs keep running. Every other failure is restarted as usual. Do not try to un-fail it with a restart or a `SIGHUP`, because it refuses again. Route it to the acknowledgement flow in `cdc-sync-operator`. That flow needs human approval, because `--accept-unforwarded-schema-change` has no `syncs.yaml` key and runs as a one-off `sync start` outside the fleet. **The same holds for six more codeless refusals that every restart would repeat:** `SLOT-ACKED-PAST-TARGET-POSITION`, `SHARDED-TARGET-VINDEX-UPDATE`, `APPLY-MARK-MISMATCH`, `CHARSET-NOT-DECODABLE`, `DSN-TIME-ZONE-NOT-UTC` and `CHANGE-LOG-WATERMARK-STALLED` (a `postgres-trigger` source; a sluice bug, so the remedy is a bug report carrying the query its message prints). The leg shows `failed` with an ERROR line naming the marker (`marker=` attribute) and is not restarted, whatever `max-consecutive-failures` says. A `SIGHUP` reload starts every failed leg still in the config again, so reload only after the fix. Remedy: apply the fix the message names. When it is a DSN change (`DSN-TIME-ZONE-NOT-UTC`), edit `syncs.yaml`, then reload. When it needs a flag the fleet config has no key for (`--restart-from-scratch`, `--accept-slot-acked-past-position`), take the leg out of the fleet (remove it from `syncs.yaml` and reload), start it once by hand with `sync start` and that flag (approval-gated), and once it is streaming stop it with `sync stop --wait` and put it back in `syncs.yaml`.

3. **(Optional) serve the read-only dashboard.** Add `--dashboard-listen :9300` to expose an HTML view + a `/api/fleet` JSON API. **NO AUTHENTICATION** — bind to localhost or a trusted network only. If the address can't bind, the fleet refuses to start (rather than silently running blind).

4. **Attach the terminal dashboard.** `sluice sync tui --connect :9300` (or a full `http://host:9300/api/fleet` URL) polls a running `sync run --dashboard-listen` server — works over an SSH tunnel to a remote fleet.

5. **Monitor per leg (cron/agent-friendly).** Fleet roll-up: `sluice sync status --all --config syncs.yaml --format json` (one table across every configured target; `--watch 2s` to live-refresh, `--summary` for an aggregate header). Per-stream freshness with cron exit codes: `sluice sync health --format json --target-driver <drv> --target <dsn> --stream-id <id> [--max-stale-seconds N]` (exit 0 healthy / 1 breached-or-skipped-tables-or-`CONTROL-TIMESTAMP-IN-FUTURE` / 2 operational; a stream row dated more than 60s in the future is state `UNKNOWN` and trips 1 whatever `--max-stale-seconds` says, including 0; a nonzero skipped-tables count trips 1 with no threshold — `schema add-table` or an explicit filter resolves it; if the table exists but the apply role's privileges were revoked, restore the grant instead — the skip clears on the next change) — one call per leg you gate on.

## What you return
- **Fleet composition:** N legs, each `stream-id` → source→target, and the restart policy in effect.
- **Startup result:** dry-run validation outcome; which legs came up; any leg refused/looping and why.
- **Monitoring surface:** the `sync status --all` roll-up and per-leg `sync health` verdicts (+ exit codes); the dashboard/TUI address if enabled (with the no-auth caveat).
- **Per-leg issues:** name any breached threshold, restart-looping leg, or `failed` leg, and route it to `sluice-error-triage` / `cdc-sync-operator`. For a `failed` leg, quote the message, and name `UNFORWARDED-SCHEMA-CHANGE` (or any of the five markers in step 2) explicitly when it appears, and `ADD-COLUMN-BACKFILL-INCOMPLETE` too when present (its repair is to copy the added column's values from the source, not to apply a change to the target).

Per-leg recovery that needs a destructive flag (`--reset-target-data`, `--restart-from-scratch`, `slot drop`, …), `--accept-unforwarded-schema-change` or `--accept-slot-acked-past-position` is still approval-gated. Surface it; don't auto-apply it. Keep tokens/URLs in env, never in the committed YAML.

## References (canonical — don't duplicate)
`AGENTS.md` (taxonomy, envelope, env-first credentials) · `docs/operator/running-as-a-service.md` · `skills/cdc-sync-operator/SKILL.md` (per-leg lifecycle) · `sluice sync run --help` / `sluice sync tui --help` / `sluice sync status --help`.
