# sluice v0.19.0

Logical backups Phase 4 lands. `sluice backup stream` is a single long-running process that produces rolling incrementals at a configured cadence — no per-incremental cron orchestration, no `for { sluice backup incremental }` shell loop, no slot churn between windows. Fits k8s "always-on protection" deployments naturally and pairs with continuous CDC + chain-restore for full disaster-recovery coverage. Implementation supplement: `docs/dev/design-logical-backups-phase-4.md`.

## Features

- **`sluice backup stream run --since=<full-id> --target=<url>`.** Long-running stream that drives a `for { rollover() }` loop. Each rollover is a bounded window (time / change-count / bytes, first-fired wins) that commits one new manifest at `manifests/incr-<unix-millis>-<seq>.json`. The CDC pump is opened ONCE for the lifetime of the stream and reused across rollovers — the load-bearing efficiency win over a tight `for { sluice backup incremental }` loop.

- **Rollover policy: hybrid time + size + bytes.** Three ceilings active in parallel, first-fired wins:
  - `--rollover-window=DURATION` (default `5m`) — wall-clock cadence.
  - `--rollover-max-changes=N` (default `100000`) — change-count ceiling.
  - `--rollover-max-bytes=BYTES` (default `64Mi`, mirrors `--max-buffer-bytes`) — buffered-bytes ceiling.

  Window extends to next `TxCommit` so the chain doesn't end mid-tx (mirrors Phase 3.1). Empty rollovers skipped by default; `--rollover-include-empty` opts in for heartbeat-shape monitoring.

- **Cross-machine stop via `sluice backup stream stop --target=<url>`.** Writes `stop_requested_at` to the destination's `manifests/stream_state.json`; the running stream observes the request on its next rollover-tick poll and exits cleanly. Cross-machine because the destination IS the rendezvous point — machine A's stream + machine B's stop command don't need to know about each other directly. Mirrors the `sync stop` pattern (ADR-0025).

- **Concurrent-writer protection.** `stream_state.json` carries `{pid, host, started_at, last_rollover_at, stop_requested_at}`. On startup, refuses to start a second stream when the file shows a recent (`< 2 × rollover-window`) `last_rollover_at` from a different (pid, host). `--force` bypasses with a WARN naming the conflict — operator-confirmed: "I'm taking over this destination from a previous stream that may still be running."

- **Signal handling.** SIGINT / SIGTERM via the existing `kongContext` notifier propagates as ctx.Done through the rollover loop. Mid-rollover cancel surfaces as a clean nil exit; the rollover's chunks may be partially-written but the manifest never finalises, so on restart the stream picks up at the previous rollover's EndPosition. Bounded by `stopDrainTimeout` (30 s) to keep wedged streams from holding container teardown indefinitely.

- **Operator-facing rollover hooks.** `--rollover-hook=<cmd>` runs a shell command after each rollover commits with env vars `SLUICE_ROLLOVER_MANIFEST_PATH`, `SLUICE_ROLLOVER_PARENT_BACKUP_ID`, `SLUICE_ROLLOVER_BACKUP_ID`, `SLUICE_ROLLOVER_CHANGES`, `SLUICE_ROLLOVER_BYTES`, `SLUICE_ROLLOVER_ELAPSED_MS`. Hook errors WARN-log but do NOT fail the stream — the rollover already committed. Examples: push to Prometheus pushgateway / send Slack notification / write to monitoring datastore. 30 s timeout.

- **`pipeline.RequestStreamStop(ctx, store, now)` exported helper.** Downstream tooling can stop a running stream without going through the CLI. Idempotent: re-issuing stop preserves the original `stop_requested_at` timestamp so drain-completion watchers don't see the clock reset.

## Compatibility

- **No CLI breaking changes.** `sluice backup full` / `sluice backup incremental` / `sluice backup verify` / `sluice restore` flag surfaces are unchanged.

- **No manifest format changes.** Stream rollovers write Phase-3-shape manifests at the same `manifests/incr-…json` path. Pre-v0.19.0 chains (single-shot incrementals + fulls) remain fully compatible; restore + verify walk stream-written chains identically.

- **`stream_state.json` is new and informational-only.** The chain itself remains the source of truth for restore + verify. Losing the state file (operator deletes it, object-store eventual-consistency lag) doesn't break the chain — only the concurrent-writer / cross-machine-stop signalling falls back to ctx-cancel and process signals. Restore + verify ignore the file entirely.

- **`sluice sync start --position-from-manifest`** keeps working unchanged against stream-written chains. The chain-walker doesn't care that the chain came from `backup stream` vs `backup incremental`.

## Who needs this

- **Anyone deploying sluice in k8s with `lifecycle.preStop` hooks or systemd's `KillSignal=SIGTERM`** — the long-running stream shape is the natural fit. Single deployment producing continuous incrementals with operator-controlled cadence; SIGTERM commits the in-flight rollover and exits cleanly.

- **Operators running cron-orchestrated `sluice backup incremental` loops today** — `backup stream run` replaces the cron with a single supervised process. Slot reuse across rollovers means lower WAL retention pressure on the source and one less moving piece for the operator to manage.

- **Anyone wanting per-rollover observability hooks** — `--rollover-hook=<cmd>` lets you push fresh-rollover events into the monitoring stack of your choice without sluice growing a metrics exporter.

## Operator notes

- **Rollover cadence tuning.** Defaults (`5m` / `100k` changes / `64Mi`) target a write-busy production workload. Quiet sources benefit from a longer window (`--rollover-window=30m`) so the chain doesn't accumulate tiny manifests. Write-heavy sources benefit from a tighter `--rollover-max-bytes=32Mi` so individual manifests stay small + restore-friendly. The chain itself is agnostic — restore walks any cadence transparently.

- **Concurrent-writer refusals.** The `< 2 × rollover-window` freshness check means a stream that crashes without cleanup leaves a state file that becomes stale within `2 × rollover-window`. A second stream started after that envelope takes over with a WARN; started inside the envelope is refused with `--force` as the override. If you crash a stream during testing and the next start refuses, either wait `2 × rollover-window` or pass `--force`.

- **Cross-machine stop**: `sluice backup stream stop --target=<url>` works regardless of whether the running stream is on the same host. The destination's `stream_state.json` is the rendezvous point. Useful for k8s-managed streams where the operator's shell isn't attached to the running pod.

- **Hook execution context** inherits the sluice process's environment (PATH and operator-exported vars are visible) plus the SLUICE_ROLLOVER_* vars. The hook runs through the OS default shell (`sh -c` on Unix, `cmd /C` on Windows). For complex hook logic, check the script into the repo and reference it from the flag: `--rollover-hook=/etc/sluice/rollover.sh`.

- **Crash recovery semantics.** Stream crash mid-rollover: in-flight chunks may be partially-written; the manifest never finalised. On restart, the stream picks up at the last committed manifest's `EndPosition`; the partial chunks are orphaned. Run `sluice backup verify` after a known-crash to identify garbage chunks. Stream crash AFTER manifest commit but BEFORE next rollover starts: clean restart, no orphan chunks.

- **Source CDC slot lost mid-stream** surfaces `ir.ErrPositionInvalid` (existing Item F sentinel from ADR-0022) as a FATAL error with operator-actionable next-step (chain is broken at this point; take a fresh full + start a new chain). Same loud-failure semantics as `backup incremental`.

## What's next

- **Phase 4.5 — `sluice sync from-backup`** — the watcher-side companion to `backup stream`. Polls the chain and replays incrementals into a target, decoupling source from target via the backup as the message log. Stream is the producer; from-backup is the consumer. Out of scope for v0.19.0; tracked on the roadmap.
- **Phase 5 — cross-engine chain restore** — Phase 4 streams produce same-engine chains; cross-engine replay needs the existing translate machinery extended for replay-of-changes-with-translation. Tracked on the roadmap.
- **Phase 6 — KMS-backed client-side encryption** — encryption-at-rest for backup chunks. Independent of stream and applicable to all backup verbs once it lands.
