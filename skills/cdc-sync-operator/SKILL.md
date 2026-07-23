---
name: cdc-sync-operator
description: Use to stand up and operate a continuous-sync stream — cold-start snapshot → CDC handoff → steady-state monitoring → cutover. Drives `sluice sync start/status/health/stop`, `sluice cutover`, and `sluice slot`. Gated — writes to the target; --reset-target-data and cutover need human approval. Trigger when the user asks to continuously sync / replicate / keep two databases in sync / cut over.
---

# cdc-sync-operator

Operate a sluice continuous-sync stream through its lifecycle. State-changing (writes to the target); the recovery/cutover steps are approval-gated.

## When to use
The user wants ongoing replication (not a one-shot migrate): a cold-start snapshot that hands off gap-free to CDC, kept healthy, and eventually cut over.

## Inputs you need
- Source + target DSNs (env: `SLUICE_SOURCE` / `SLUICE_TARGET`) and drivers. For PlanetScale VStream, `--source-driver planetscale` (see `planetscale-migration`).
- A `--stream-id` (the key position is persisted under; auto-derived from host info if omitted — set it explicitly so you can monitor/stop it later).

## Steps

1. **Preflight (read-only).** Run `migrate-preflight` first, or `sluice sync start --dry-run --format json …` — it shows cold-start-vs-warm-resume, the source schema summary or the persisted position, and the CDC/slot plan, without touching the target.

2. **Start the stream.** `sluice sync start --format json --source-driver <drv> --source "$SLUICE_SOURCE" --target-driver <drv> --target "$SLUICE_TARGET" --stream-id <id>`. It cold-start bulk-copies existing rows, then hands off to CDC automatically. On Postgres the slot name comes from `--slot-name` (default `sluice_slot`, `sluice_`-prefixed); set a distinct one to run multiple streams against one source.

3. **Confirm the cold-start.** After the snapshot completes, run `fidelity-verify` before trusting steady state.

4. **Monitor freshness (cron/agent-friendly).** `sluice sync health --format json --target-driver <drv> --target "$SLUICE_TARGET" --stream-id <id> [--max-stale-seconds N] [--max-lag-bytes N]`. **Exit 0** healthy, **exit 1** a threshold breached (stale/lag), **exit 2** operational (stream not found / connect). Add `--source-driver`/`--source` for source-position + byte-lag (PG only). For a human view use `sluice sync status [--watch 2s] [--all --config <fleet>]`.

5. **Resume semantics.** A stopped/crashed stream warm-resumes from its persisted position on the next `sync start` — re-streaming only the un-applied tail. On PlanetScale, a resume from a purged binlog position auto-recovers with a fresh re-snapshot by default (see `planetscale-migration`; `--no-auto-resnapshot` makes that a loud decision instead).

6. **Manage the PG replication slot** (recovery/diagnostics). `sluice slot list --source-driver postgres --source "$SLUICE_SOURCE"` shows every slot (name/active/wal_status/LSNs). `sluice slot drop <name>` removes an abandoned slot (`--if-exists`, `--force` if a consumer is attached, `--yes` to skip the prompt) — dropping an in-use slot breaks that stream, so treat it as gated.

7. **Cut over.** Sequence: `sluice sync stop --target-driver <drv> --target "$SLUICE_TARGET" --stream-id <id> --wait` (drains in-flight changes and clears the stop signal; `--timeout` bounds the wait) → flip application traffic to the target → `sluice cutover --format json --source-driver <drv> --source … --target-driver <drv> --target …` (re-reads source sequence/AUTO_INCREMENT state and applies it with `--sequence-margin` headroom, default 1000). Cutover is idempotent (re-run reports "noop"); it **refuses loudly (exit non-zero)** if the target sequence is already ahead — that means a re-snapshot decision, not a retry.

8. **Decommission a finished stream** (destructive — gated like `slot drop`). `sluice sync decommission --source-driver <drv> --source "$SLUICE_SOURCE" --target-driver <drv> --target "$SLUICE_TARGET" --stream-id <id> --yes` drops the stream's PG replication slot and its recorded per-stream publication on the source (never the shared `sluice_pub`) and clears its control row on the target — after this the stream can never warm-resume. A finished stream's slot otherwise pins WAL and (since v0.99.289) blocks later differently-scoped cold starts. Refuses with `SLUICE-E-DECOMMISSION-STREAM-ACTIVE` while the slot is active (run `sync stop --wait` first); `--dry-run` previews without `--yes`; MySQL-family sources have no source-side objects (control row only). Partial failures keep the control row; re-run to finish.

## What you return
- **Lifecycle state:** cold-start done? handed to CDC? current `sync health` verdict (+ exit code).
- **Freshness:** seconds-since-last-apply, byte-lag (PG), any breached threshold named.
- **Slot state (PG):** the stream's slot name + activity, any abandoned slot flagged.
- **Cutover result:** primed/noop/refused per the cutover report; any refusal surfaced as a decision point.
- **Destructive steps (if any):** `--reset-target-data --yes` / `--restart-from-scratch` / `--force-cold-start` / `slot drop` — named and flagged as needing explicit human approval.

Never pass `--reset-target-data --yes`, `--restart-from-scratch`, or `--force-cold-start` without approval for that specific invocation; on any `status:"refused"` / exit 3, surface `error.hint` and stop.

## References (canonical — don't duplicate)
`docs/operator/cdc-streaming.md` · `docs/operator/running-as-a-service.md` · `docs/cookbook/recipe-bidirectional-cutover.md` · `AGENTS.md` (taxonomy, envelope, destructive flags) · `sluice sync start --help` / `sluice sync health --help` / `sluice cutover --help` / `sluice slot --help`.
