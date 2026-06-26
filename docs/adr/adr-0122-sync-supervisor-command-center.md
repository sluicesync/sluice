# ADR-0122: Sync command center — supervise many syncs from one process

## Status

**Accepted (2026-06-26).** Roadmap item 47, staged MINIMAL-FIRST. Two deliverables,
both thin layers over the existing single-sync machinery:

- `sluice sync run --config syncs.yaml` — a supervisor that runs N independent
  `pipeline.Streamer` syncs in ONE process, each its own stream-id, each
  FAILURE-ISOLATED (one sync crashing/erroring must NOT take the others down),
  each restarted on a bounded backoff rather than aborting the fleet.
- `sluice sync status --all --config syncs.yaml` — rolls up the existing per-stream
  `sync status` output across every configured target into one fleet view.

A TUI / web dashboard is explicitly a LATER layer over the already-exported
`/metrics` + the aggregated status; it is NOT v1.

**Concurrency chunk.** The supervisor is N goroutines over shared status state.
Failure isolation is the load-bearing property and is pinned by a unit test with
stubbed runners. `-race` can't run locally (CGO off); the CI `-race` integration
job must pass before any tag.

## Context

Each `sluice sync start` is one source→target stream in its own process. The
observability primitives already exist per-stream (`sync status` / `sync-health` /
`/metrics`, and `sync status` already renders in terms of "N streams"), and the
`pipeline.Streamer` already does one full sync (cold-start → CDC) with its OWN
internal retry/restart machinery (`ApplyRetryAttempts` + the ADR-0093 reactive
re-snapshot + ADR-0038 backoff). What was missing is a layer to DRIVE and AGGREGATE
many syncs at once — what an operator keeping several ongoing cross-database syncs
alive actually wants. Today they run N `sync start` processes under N systemd units
/ k8s pods; the command center collapses that to one supervised process.

The tenet pressure here is the loud-failure / user-trust one: a supervisor is only
worth running if a single bad sync can NEVER corrupt or stall a healthy peer, and
if a config that WOULD corrupt data (two Postgres syncs sharing a replication slot)
is refused loudly at load rather than discovered at runtime.

## Decision

### 1. Supervisor model (`internal/pipeline.Supervisor`)

Engine-neutral, lives in the orchestrator package next to `Streamer` (it imports no
engine package — the CLI composition root resolves engines and hands the supervisor
ready-built `*Streamer`s). The supervisor manages a slice of `SupervisedSync{ID,
Runner}` where `Runner` is the one-method `SyncRunner` interface (`Run(ctx) error`)
that `*Streamer` already satisfies. The interface is the seam the failure-isolation
unit test injects a deterministic failing/healthy stub through — no real pipeline
boot needed.

Each sync runs in its OWN goroutine under an internal supervise loop:

- The runner is invoked through `runGuarded`, which `recover()`s a panic into an
  error (a panicking sync must not crash the process and take down peers — the
  single most important isolation guarantee), logging the stack at ERROR.
- A `nil` return means the Streamer drained cleanly (operator `sync stop` / ctx
  cancel) → the sync is `stopped`, not restarted.
- A non-nil return with a LIVE ctx is a crash/terminal error → the sync is logged
  loudly, backed off, and restarted (re-entering the Streamer's own cold-start /
  warm-resume-from-persisted-position path). Peers are untouched.
- A ctx cancel (Ctrl-C / SIGTERM) stops EVERY sync's loop cleanly; `Supervisor.Run`
  returns nil.

`Supervisor.Run` launches all goroutines and blocks until every one exits. On ctx
cancel it returns nil. If ctx is still live but every sync has ended on its own
(all permanently failed / drained), it returns an aggregated error of the failed
syncs (so a single-sync fleet that can't start exits non-zero).

### 2. Restart policy (bounded backoff + reset-on-healthy)

`RestartPolicy{BackoffBase=1s, BackoffCap=30s, HealthyRunThreshold=60s,
MaxConsecutiveFailures=0}`. Exponential backoff `base * 2^(n-1)` capped at
`BackoffCap`. The consecutive-failure counter RESETS when a sync ran longer than
`HealthyRunThreshold` before failing — mirroring ADR-0038's "counter resets when
the stream made progress" so a sync that ran for hours then died doesn't carry
restart debt. `MaxConsecutiveFailures=0` (the default, zero-value-safe) means
restart forever with the capped backoff — a sync whose source comes back recovers
on its own. A positive cap transitions the sync to a terminal `failed` state after
N consecutive failures (logged loudly, peers unaffected) — chosen as the default in
the failure-isolation TEST so the pin is deterministic, not as the production
default.

### 3. Config schema (`syncs.yaml`)

A typed YAML loader (koanf, mirroring `internal/config`), keys in kebab-case to
match the CLI flags operators already know:

```yaml
syncs:
  - stream-id: orders
    source-driver: postgres
    source: postgres://user:pass@src-a:5432/app
    target-driver: mysql
    target: mysql://user:pass@dst:3306/app
    slot-name: orders          # distinct per Postgres source — see §4
    apply-concurrency: 4
    apply-delay: 0s
    notify-slack: ""           # credential via env, as today
restart:
  backoff-base: 1s
  backoff-cap: 30s
  max-consecutive-failures: 0
```

Per-sync knobs are a CURATED SUBSET of `Streamer`'s fields — the ones that matter
for a fleet: source/target driver+DSN, stream-id, slot-name, target-schema,
table filters, type/expr overrides, apply-concurrency / apply-batch-size /
no-auto-tune / apply-delay / max-buffer-bytes / apply-exec-timeout / apply-retry-*,
metrics-listen, heartbeat-interval, poll-interval, schema-changes, and the
notify-webhook / notify-slack / notify-sync-lag-seconds / notify-cooldown + SMTP
sink fields. The spec→Streamer builder REUSES the exact `sync start` helpers
(`resolveEngine`, `resolveApplyBatchSize`, `NewTableFilter`, the `notify.SMTPConfig`
assembly) so a fleet sync behaves identically to the same flags on `sync start`.

Deliberately OUT of v1 (documented, not silently dropped): per-sync MySQL
process-global overrides (`--mysql-sql-mode` / `--zero-date` / VStream tuning are
set once per process via package setters and are shared by every sync in the
fleet); per-sync PlanetScale control-plane telemetry (the PS-util threshold alerts
need a telemetry provider the v1 fleet does not wire — `notify-sync-lag-seconds` is
ungated and DOES work); redaction keyset/dictionaries; and env-overlay over the
list. These are additive follow-ups, not correctness gaps.

### 4. Slot-name uniqueness guard (the data-corruption refusal)

A Postgres replication slot is a single-consumer resource on the SOURCE cluster.
Two Postgres-source syncs sharing a resolved slot name would fight over one slot —
silent data corruption. At config-load the loader resolves every Postgres-source
sync's slot name (via `pipeline.ResolveSlotName`, applying the `sluice_` prefix
convention; empty → the engine default `sluice_slot`) and REFUSES LOUDLY, naming
both colliding stream-ids and the slot, if any two collide.

The guard is intentionally CONSERVATIVE: it enforces global uniqueness of the
resolved slot name across all Postgres-source syncs, NOT (slot, source-server)
uniqueness. Two syncs from genuinely different PG servers reusing one slot name
would be safe, but distinguishing servers means parsing every DSN dialect; refusing
the rare safe case is the loud-failure-correct trade (the operator just sets a
distinct `slot-name`, which is exactly the convention's purpose, and the default
`sluice_slot` on two PG sources is overwhelmingly an error). Scoping it to source
endpoint identity is a noted future refinement. MySQL-source syncs have no slot
(the binlog stream is the slot) and are exempt.

A second config-load refusal guards duplicate stream-ids across the fleet: two
syncs with the same stream-id on the same target clobber each other's
`sluice_cdc_state` position row — the same corruption class — so duplicates are
refused regardless of engine.

### 5. Connection-budget accounting (WARN, not refuse)

When several syncs target one server they share that target's connection budget
(each sync opens cold-copy + per-lane apply connections). v1 does NOT probe the
shared budget — it WARNs loudly at load when two or more syncs resolve to the same
target endpoint (coarse host extraction; falls back to the full DSN string),
listing the colliding stream-ids, so the operator sizes `apply-concurrency` /
`max-target-connections` accordingly. A per-target shared budget prober is a noted
follow-up; over-engineering it for v1 is out of scope, but silently oversubscribing
is not acceptable — hence the WARN.

### 6. Status aggregation (`sync status --all`)

`sync status --all --config syncs.yaml` reads the fleet config and, for each
DISTINCT target (deduped by driver+DSN so a shared target is queried once), opens
the target applier and calls the existing `ListStreams`, merging every stream into
one `[]ir.StreamStatus` rendered through the EXISTING `renderStatus` (text +
`--summary` aggregate header, or `--json`). This reuses the per-stream rendering
verbatim — the fleet view is "every stream across every configured target, one row
each." It reads the control tables directly (no running supervisor process
required), exactly as single `sync status` does today. A target that can't be
reached is reported inline and skipped (a dead target must not blank the whole
fleet view) rather than aborting — the same failure-isolation discipline as the
supervisor itself.

## Consequences

- One process, N syncs, failure-isolated: the operational unit shifts from
  "N pods" to "one supervised fleet," which is the item-47 thesis. The Streamer's
  own retry is the inner loop; the supervisor's restart is the outer loop.
- The slot-uniqueness and duplicate-stream-id refusals turn two silent
  data-corruption classes into loud config-load errors.
- Config hot-reload is explicitly future: changing `syncs.yaml` requires a process
  restart (a clean ctx-cancel drains every sync, then re-run). Noted in §3.
- Because the supervisor is concurrency over shared state, the CI `-race`
  integration job is the gate before any tag (local CGO-off can't run `-race`).
