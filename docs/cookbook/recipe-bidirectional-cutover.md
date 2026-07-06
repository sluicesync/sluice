# Recipe — bidirectional cutover with sequence priming

Low-downtime migration. Snapshot the source, catch up changes via CDC,
then cut over from old → new without leaving the old database midway.

## When to use this recipe

- You're moving a production database from one infrastructure to another
  and can't afford the read-only window a one-shot `migrate` would
  require.
- You need to validate the target by running it in parallel with the
  source for hours or days before cutover.
- You're moving cross-engine (MySQL → Postgres or vice versa) and want
  to bring traffic to the new engine gradually.

If you can afford a read-only window — even just a few minutes —
[recipe-migrate-once.md](recipe-migrate-once.md) is simpler. Don't
reach for the cutover dance if `migrate` is enough.

## The flow at a glance

1. **`sluice sync start`** — one self-contained command. It captures a
   source CDC anchor, bulk-snapshots every table from that consistent
   anchor **while the source stays writable**, then resumes CDC from the
   exact anchor. This is the *only* data-movement step — there is no
   separate bulk-copy command to run first.
2. (verify, monitor, run dual-read tests, etc. — as long as you want;
   the stream keeps the target continuously caught up)
3. **`sluice cutover`** — prime the target's sequences past the source's
   highest IDs so application writes to the target don't collide.
4. **Cut application traffic over to the target** (DNS, connection
   string flip, etc.).
5. **`sluice sync stop --wait`** — drain the CDC stream cleanly and
   stop.

The whole sequence is operator-driven; sluice doesn't own the traffic
switch. That's deliberate — the cutover moment is application-specific
and the operator's call.

Note there is **no `migrate` step and no `--resume` flag** in this flow.
`sluice sync start` already does the bulk snapshot *and* the CDC handoff
in one process; the snapshot→CDC boundary is gapless by design (see
[`docs/snapshot-cdc-handoff.md`](../snapshot-cdc-handoff.md)). Running a
separate `sluice migrate` first would populate the target and make
`sync start` refuse to cold-start (see the pitfalls below).

## The commands

### Step 1: start the sync stream (cold-start snapshot → CDC)

```sh
sluice sync start \
    --source-driver mysql    --source ... \
    --target-driver postgres --target ... \
    --stream-id myapp-prod
```

This one command does everything the old two-step "migrate then resume"
dance tried to do — but correctly and without a gap. On an empty target
`sync start` **cold-starts**: it captures the source's current CDC
position into `sluice_cdc_state` *before* the bulk reader runs, snapshots
every table from that consistent anchor (the source stays fully writable
throughout — no read-only window), then resumes CDC from the exact anchor
it captured. The snapshot→CDC boundary is gapless by design; see
[`docs/snapshot-cdc-handoff.md`](../snapshot-cdc-handoff.md) for the
guarantee and the log lines to watch (`cold start; snapshot captured`
→ bulk copy → `cdc start; resuming from position_token=...`).

There is deliberately **no `--resume` flag** and no separate `migrate`
first — `sync start` owns both phases. (If you *do* run `sluice migrate`
into this target first, `sync start` will refuse to cold-start against a
non-empty target; see the pitfalls.)

The stream is **long-running**. Run it under systemd, k8s, or your
process supervisor of choice. Operationally it behaves like any other
replicator — restart-safe (a restarted `sync start` with the same
`--stream-id` *warm-resumes* from `sluice_cdc_state`, no re-snapshot),
drainable (`sluice sync stop --wait`), and observable (`sluice sync
status` and `sluice sync health`).

### Step 2: monitor

```sh
# How fresh is the target relative to the source?
sluice sync status --stream-id myapp-prod

# Is the stream within an SLO? Exits non-zero past the threshold.
sluice sync health \
    --target-driver postgres --target ... \
    --stream-id myapp-prod --max-stale-seconds=60

# Are all the rows there?
sluice verify --source-driver mysql --source ... \
              --target-driver postgres --target ... \
              --depth=sample
```

Run these on whatever cadence your operations procedures require.
Many teams run `sync health` from a cron job into a Slack channel;
`--max-stale-seconds=N` makes it exit non-zero once the target's last
apply falls more than N seconds behind.

### Step 3: prime the target's sequences

This is the load-bearing step. The source's identity columns (PG
`bigserial`, MySQL `AUTO_INCREMENT`) have been advancing in production
while CDC is running. The target's sequences are tracking the values the
cold-start snapshot copied, not the source's current high-water mark. If
you cut traffic over now, the target's auto-incremented inserts will
collide with rows the CDC stream is about to replicate.

```sh
sluice cutover \
    --target-driver postgres \
    --target ... \
    --source-driver mysql \
    --source ... \
    --sequence-margin=1000
```

This reads the source's current `MAX(id)` per identity column and
runs `setval(...)` on the target with a margin (1000 in the example).
After this, the target's next-inserted ID is guaranteed to be greater
than anything CDC has carried so far, even if more changes arrive
during the cutover window.

The margin is operator-tunable. 1000 is the default and a good fit for
human-paced traffic; pick higher if you have a high-write source. (The
old spelling `--cutover-sequence-margin` still works as a deprecated
alias.)

### Step 4: flip application traffic

This is **your** step, not sluice's. Switch your application's
connection string, flip your DNS record, or take whatever
cut-over-the-traffic action your environment uses.

After this moment, the source is **no longer being written to** (or
shouldn't be) — the application is writing to the target.

### Step 5: drain and stop the CDC stream

```sh
sluice sync stop \
    --target-driver postgres --target ... \
    --stream-id myapp-prod --wait
```

`--wait` blocks until sluice's drain logic catches the stream up to
its current end position (every change committed before the stop
signal is applied) and then exits cleanly. Without `--wait`, the stop
is asynchronous — the daemon shuts down but late-in-flight changes
may or may not have landed.

## Verification post-cutover

```sh
sluice verify \
    --source-driver mysql    --source ... \
    --target-driver postgres --target ... \
    --depth=sample
```

Run after the CDC drain completes. `--depth=sample` is a good
default; `--depth=count` is faster but only verifies row counts;
`--depth=full` is the strongest guarantee and the slowest.

## Common pitfalls

- **Forgot the cutover step.** Application writes to the target start
  generating PK collisions as CDC catches up — the inserts on the
  target's auto-incremented columns produce IDs that overlap with
  rows the CDC stream is about to apply. Always run `sluice cutover`
  before switching traffic.
- **Forgot `--wait` on `sync stop`.** The async stop signal sets
  `stop_requested_at` but doesn't drain in-flight changes. Late
  changes may be lost. Use `--wait`.
- **Running `sluice migrate` before `sync start`.** There is no
  two-step "migrate then resume" flow — `sync start` does the bulk
  snapshot itself. A prior `migrate` populates the target but writes no
  `sluice_cdc_state` row, so `sync start` refuses to cold-start against
  the now-non-empty target (`SLUICE-E-COLDSTART-TARGET-NOT-EMPTY`). Start
  with `sync start` on an empty target and let it own both phases.
- **Reaching for a `--resume` flag on `sync start`.** It doesn't exist
  (kong rejects the unknown flag). Warm-resume after a restart is
  automatic — re-run `sync start` with the same `--stream-id` and it
  picks up from the persisted position with no re-snapshot.

## What's NOT in this recipe

- **Mid-stream schema changes during the sync window.** Use `sluice
  schema add-table TABLE --stream-id ID --no-drain` for live add-table.
  See [`docs/schema-change-runbook.md`](../schema-change-runbook.md).
- **Backup chains.** See [recipe-backup-encrypted.md](recipe-backup-encrypted.md).
- **PII redaction during the sync.** See
  [recipe-redaction-keyset.md](recipe-redaction-keyset.md).

## See also

- [`docs/cutover.md`](../cutover.md) — the per-feature cutover
  documentation with all flag knobs.
- [`docs/snapshot-cdc-handoff.md`](../snapshot-cdc-handoff.md) — how
  sluice closes the gap between bulk-copy end-position and CDC start.
