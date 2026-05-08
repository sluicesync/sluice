# Logical Backups Phase 4 — Implementation Design

Supplement to [`design-logical-backups.md`](design-logical-backups.md), [`design-logical-backups-phase-2.md`](design-logical-backups-phase-2.md), and [`design-logical-backups-phase-3.md`](design-logical-backups-phase-3.md). This file covers Phase 4: **continuous-incremental long-running stream** (`sluice backup stream`).

The headline operator outcome: a single long-running process produces rolling incrementals at a configured cadence, no per-incremental cron orchestration. Fits k8s-style "always-on protection" deployments naturally; pairs with continuous CDC + chain-restore for full DR coverage.

## What's already in Phase 3 + 3.3 that this builds on

- `sluice backup incremental --since=<full-id-or-url>` — single-shot incremental writer; bounded by `--window=DURATION` or `--max-changes=N`
- Chain-linked manifests under `manifests/incr-…json`; per-Run change-chunk namespacing (Bug 35 fix)
- Snapshot-anchored EndPosition (v0.18.0)
- `sluice sync start --position-from-manifest` for chain → CDC handoff (Phase 3.3)

## Scope

**In scope (Phase 4):**

- `sluice backup stream --since=<full-id-or-url> --target=<url>` long-running process
- Rollover policy (time-bound + size-bound, first-fired wins; mirrors single-shot incremental's bound shape)
- Append-only manifest writes (one new manifest per rollover; never modify a written manifest)
- Signal handling: SIGTERM = clean shutdown after current rollover commits; SIGINT = graceful drain (drop in-flight, write whatever's been captured, exit cleanly)
- `sluice backup stream stop --target=<url>` companion for cross-machine control (mirrors `sync stop` pattern via control-table-style flag on the backup destination)
- Structured logging at INFO (rollover events, lifetime stats) + DEBUG (per-event detail)
- Operator-facing rollover hooks: optional `--rollover-hook=<cmd>` runs after each rollover commits (e.g. shell out to alert-manager / metrics-pusher)

**Deferred to Phase 4.5 (backup-as-broker, separate chunk):**

- `sluice sync from-backup` watcher that polls the chain and replays incrementals into a target

**Deferred to Phase 5+ (cross-engine chain restore):**

- Cross-engine chain restore (Phase 3.2 currently refuses cross-engine; this is a translation-pass scope, not a stream scope)

**Deferred to Phase 6 (KMS encryption):**

- Client-side encryption at-rest

## Open design questions — resolved decisions

### Q1: Rollover policy

**Decision: hybrid time-bound + size-bound, first-fired wins** (mirrors single-shot incremental's existing shape).

- `--rollover-window=DURATION` (default `5m`): commit a rollover after this much wall-clock time elapses since the last rollover, regardless of change volume.
- `--rollover-max-changes=N` (default `100k`): commit a rollover after this many change events queue up.
- `--rollover-max-bytes=BYTES` (default `64Mi`, mirrors `--max-buffer-bytes` from Phase 2 backup writer): commit when the in-flight buffer crosses this size.
- All three ceilings active; whichever fires first wins. Window extends to the next `TxCommit` so chain doesn't end mid-tx (same as Phase 3.1).

Justification: single threshold = surprises (chatty source → giant manifest, quiet source → stale chain). Hybrid catches both shapes without operator config gymnastics. Defaults pick "5 min OR 100k changes OR 64 MiB" which is reasonable for most workloads; operators tune via flags.

### Q2: Manifest update under concurrent writers

**Decision: append-only, one manifest per rollover, never modify a written manifest.**

- Each rollover writes a NEW manifest file at `manifests/incr-<unix-millis>-<seq>.json` (mirrors Phase 3.1 + Bug 35's per-Run namespace).
- The chain-walker (Phase 3.2) already builds the chain via `ParentBackupID` linkage; it doesn't depend on a single mutable manifest. So append-only writes work transparently.
- No concurrent-writer-to-same-manifest issue exists because each rollover has a distinct path.
- For "is the stream still running" liveness: introduce a small `stream_state.json` at the destination root with `last_rollover_at` + `pid` + `host`. This IS mutable — but only one stream process should be writing to it. Coordinate via:
  - **PID + host check**: refuse to start `backup stream` against a destination whose `stream_state.json` shows a recent (`< 2 × rollover-window`) `last_rollover_at` from a different (pid, host). Operator must `sluice backup stream stop` the previous run first.
  - **Override**: `--force` flag to bypass (operator-confirmed: "I'm taking over this destination").
- The `stream_state.json` is informational-only; the chain itself is the source of truth for restore + verify.

### Q3: Operator UX — long-running process

**Decision: standard slog-structured logs + cooperative signal handling + companion stop command.**

- INFO log line per rollover (`stream rollover committed manifest=incr-1778214632476-0007.json changes=8421 bytes=12_341_009 elapsed=4m51s`).
- DEBUG log line per change applied (gated behind `--log-level=debug`; production is INFO).
- WARN on rollover failures with exponential-backoff retry (3 attempts, 5s/15s/45s); after retry exhaustion, FATAL with a clear next-step (typically: object store unavailable, fix infra and restart).
- `SIGTERM` (default for k8s `lifecycle.preStop` and systemd `KillSignal`): commit current in-flight rollover, exit cleanly. Bounded by `stopDrainTimeout` (30s, mirrors Phase 3 streamer pattern). On timeout, force-cancel.
- `SIGINT` (Ctrl-C): same as SIGTERM but tighter — commits whatever's already been written, drops in-flight events, exits.
- `SIGHUP`: reload config (future; v1 logs WARN about unsupported signal and ignores).
- `sluice backup stream stop --target=<url>`: writes `stop_requested_at` to `stream_state.json`; the running stream polls it on each rollover-tick and exits cleanly when set.

### Q4: Rollover hooks

**Decision: optional `--rollover-hook=<cmd>` runs after each rollover commits.**

- Cmd executed via `os/exec` with environment vars: `SLUICE_ROLLOVER_MANIFEST_PATH`, `SLUICE_ROLLOVER_PARENT_BACKUP_ID`, `SLUICE_ROLLOVER_BACKUP_ID`, `SLUICE_ROLLOVER_CHANGES`, `SLUICE_ROLLOVER_BYTES`, `SLUICE_ROLLOVER_ELAPSED_MS`.
- Hook errors are WARN-logged but do NOT fail the stream (rollover already committed).
- Hook timeout = 30s (the stream pump waits for the hook before starting the next rollover-tick).
- Examples in docs: push to Prometheus pushgateway / send Slack notification / write to monitoring datastore.

## Sub-phasing

| Sub-phase | Scope | LOC est. |
|---|---|---|
| **4.1 — Stream pump + rollover policy** | New `internal/pipeline/stream.go` orchestrator. Reuses Phase 3.1's `incremental.go` writer; replaces the single-shot bounded-window driver with a `for { rollover() }` loop. Hybrid time/size/byte bounds via `time.Ticker` + counters. | 400-500 |
| **4.2 — Stream state + concurrent-writer protection** | New `manifests/stream_state.json` shape; `--force` override; PID/host conflict detection. | 100-150 |
| **4.3 — Signal handling + graceful drain** | SIGTERM/SIGINT handlers; bounded drain timeout; clean rollover commit on shutdown. | 100-150 |
| **4.4 — `sluice backup stream stop` command** | Companion CLI; writes `stop_requested_at` to stream_state. | 50-100 |
| **4.5 — Rollover hooks** | `--rollover-hook=<cmd>` wiring + env-var contract; bounded execution. | 100-150 |
| **CI integration** | Long-running stream test under testcontainer; concurrent writes during stream lifetime; rollover boundaries assertions; signal-handling assertions. | 200-300 |
| **Total Phase 4** | | ~950-1350 |

## CLI surface

| Command | Phase 4 work |
|---|---|
| `sluice backup full` | Unchanged. |
| `sluice backup incremental` | Unchanged. |
| `sluice backup stream --since=<full-id-or-url> --target=<url>` | NEW. Long-running process; rolling incrementals at configured cadence. |
| `sluice backup stream stop --target=<url>` | NEW. Companion stop command. |
| `sluice backup verify` | Unchanged (already walks chains; covers stream-written chains transparently). |
| `sluice restore` | Unchanged (chain-walker handles stream-written chains identically). |
| `sluice sync start --position-from-manifest` | Unchanged (Phase 3.3; reads chain's terminal position regardless of how it was written). |

## Acceptance criteria

A clean Phase 4 must:

1. **Long-running stream against PG and MySQL produces rolling incrementals.** Start `backup stream`; drive source writes; observe N rollovers committed at the configured cadence; stop cleanly.
2. **Time-bound rollover triggers.** Set `--rollover-window=30s`; with no source writes, observe an empty rollover (or skip-empty-rollover behavior, see below) at 30s intervals.
3. **Size-bound rollover triggers.** Set low `--rollover-max-changes=10`; drive 25 INSERTs; observe 3 rollovers (10 + 10 + 5 in the in-flight buffer).
4. **Restore + handoff from a stream-written chain works.** The chain-walker doesn't care that the chain was stream-written vs `incremental --since` written.
5. **SIGTERM commits in-flight rollover and exits cleanly.** No partial / corrupted manifest on disk.
6. **SIGINT graceful-drains** (drops in-flight changes if any, but commits already-written events).
7. **`sluice backup stream stop` cross-machine control works.** Run stream on machine A; run stop on machine B (against same destination); machine A's stream exits within 2 × rollover-window.
8. **Concurrent-writer protection refuses second stream against the same destination** unless `--force` supplied.
9. **Rollover hooks fire after each commit** with the documented env vars; hook errors WARN-log but don't fail the stream.

CI: long-running tests are awkward in `go test`; use timeouts + bounded windows. Most assertions can be made within 30-60s of stream lifetime via small `--rollover-window=2s` and `--rollover-max-changes=10` settings.

## Skip-empty-rollover policy

**Decision: skip empty rollovers by default; opt-in to write them via `--rollover-include-empty`.**

A quiet source can produce empty rollovers (no change events between ticks). Two questions:

- **Does the chain need an empty rollover?** No — restore + handoff work fine if the chain just has no manifests for quiet periods. The chain's terminal `EndPosition` advances only when there's something to record.
- **Does observability need empty rollovers?** Some operators want "I know my stream is alive because it wrote a manifest in the last hour" as a heartbeat. The `stream_state.json` (which IS updated on every tick, regardless of changes) covers this without polluting the chain.

So default: skip empty. `--rollover-include-empty` for operators who want the heartbeat manifest shape (typically: stale-chain alerting based on chain-tail manifest age).

## Error recovery + restart semantics

- **Stream crash mid-rollover.** The in-flight rollover's chunks may be partially-written; the manifest hasn't been finalized. On restart, `backup stream` detects the partial state and resumes from the last committed manifest's `EndPosition`. The partial chunks are orphaned — operator-visible warning + recommend running `backup verify` to identify any garbage chunks.
- **Stream crash AFTER manifest commit but BEFORE next rollover starts.** Restart picks up at the committed manifest's `EndPosition`. Clean.
- **Source CDC slot lost mid-stream.** Stream surfaces `ir.ErrPositionInvalid` (existing Item F sentinel from ADR-0022); FATAL with operator-actionable next-step (chain is broken at this point; take a fresh full + start a new chain).

## Tenet check

- **IR-first.** Stream pump consumes `ir.Change` events from the existing CDC reader; writes via existing chunk format. No new translation surface.
- **Contain Postgres complexity.** PG slot lifecycle stays explicit (operator manages chain-handoff slot); stream just keeps reading. The PG soft-warning preflights from Phase 3.3 already cover the slot-existence and `wal_keep_size` checks at stream startup.
- **Validate end-to-end.** Acceptance criteria 4 (restore + handoff from stream-written chain) is the load-bearing integration test.
- **Loud failure beats silent corruption.** Rollover failures retry then FATAL; partial manifests on crash mid-rollover surface a clear operator warning at restart.
- **Clean, elegant code.** Stream pump = small loop reusing Phase 3.1's machinery; no new abstractions beyond `streamState` + signal handlers.

## See also

- [`design-logical-backups.md`](design-logical-backups.md) — original proto-ADR
- [`design-logical-backups-phase-2.md`](design-logical-backups-phase-2.md) — cloud backends + resumable writer
- [`design-logical-backups-phase-3.md`](design-logical-backups-phase-3.md) — incrementals + chain-aware restore + CDC handoff + snapshot-anchored EndPosition
- ADR-0007 (per-target control table) — the conceptual cousin of `stream_state.json`
- ADR-0022 (slot-missing fall-through, Item F) — what happens when stream's slot is lost mid-run
- ADR-0025 (graceful-drain `sync stop`) — the conceptual cousin of `backup stream stop`
