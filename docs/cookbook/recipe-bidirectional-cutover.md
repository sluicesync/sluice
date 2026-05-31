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

1. **`sluice migrate`** — bulk copy the source to the target.
2. **`sluice sync start`** — start the CDC stream picking up where the
   migrate left off, replicating new changes from source to target.
3. (verify, monitor, run dual-read tests, etc. — as long as you want)
4. **`sluice cutover`** — prime the target's sequences past the source's
   highest IDs so application writes to the target don't collide.
5. **Cut application traffic over to the target** (DNS, connection
   string flip, etc.).
6. **`sluice sync stop --wait`** — drain the CDC stream cleanly and
   stop.

The whole sequence is operator-driven; sluice doesn't own the traffic
switch. That's deliberate — the cutover moment is application-specific
and the operator's call.

## The commands

### Step 1: bulk copy

```sh
sluice migrate \
    --source-driver mysql    --source ... \
    --target-driver postgres --target ... \
    --migration-id myapp-prod
```

Same as [recipe-migrate-once.md](recipe-migrate-once.md); the
`--migration-id` is what links this migrate to the CDC stream below.

### Step 2: start the CDC stream

```sh
sluice sync start \
    --source-driver mysql    --source ... \
    --target-driver postgres --target ... \
    --stream-id myapp-prod \
    --resume
```

`--stream-id myapp-prod` matches the `--migration-id` from step 1 so
sluice resumes from the bulk-copy's end-position rather than
re-snapshotting. `--resume` is the explicit acknowledgement that you're
continuing an existing migration; without it, sluice's cold-start
refusal kicks in (the target's `sluice_cdc_state` table already has a
row for this stream).

The CDC stream is **long-running**. Run it under systemd, k8s, or your
process supervisor of choice. Operationally it behaves like any other
replicator — restart-safe (resume from `sluice_cdc_state`), drainable
(`sluice sync stop --wait`), and observable (`sluice sync status` and
`sluice sync health --max-staleness=60s`).

### Step 3: monitor

```sh
# How fresh is the target relative to the source?
sluice sync status --stream-id myapp-prod

# Is the stream within an SLO? Exits non-zero past the threshold.
sluice sync health --stream-id myapp-prod --max-staleness=60s

# Are all the rows there?
sluice verify --source-driver mysql --source ... \
              --target-driver postgres --target ... \
              --depth=sample
```

Run these on whatever cadence your operations procedures require.
Many teams run `sync health` from a cron job into a Slack channel.

### Step 4: prime the target's sequences

This is the load-bearing step. The source's identity columns (PG
`bigserial`, MySQL `AUTO_INCREMENT`) have been advancing in production
while CDC is running. The target's sequences have been advancing too,
but they're tracking the bulk-copied values, not the source's current
high-water mark. If you cut traffic over now, the target's
auto-incremented inserts will collide with rows the CDC stream is about
to replicate.

```sh
sluice cutover \
    --target-driver postgres \
    --target ... \
    --source-driver mysql \
    --source ... \
    --cutover-sequence-margin=1000
```

This reads the source's current `MAX(id)` per identity column and
runs `setval(...)` on the target with a margin (1000 in the example).
After this, the target's next-inserted ID is guaranteed to be greater
than anything CDC has carried so far, even if more changes arrive
during the cutover window.

The margin is operator-tunable. 1000 is a good default for human-paced
traffic; pick higher if you have a high-write source.

### Step 5: flip application traffic

This is **your** step, not sluice's. Switch your application's
connection string, flip your DNS record, or take whatever
cut-over-the-traffic action your environment uses.

After this moment, the source is **no longer being written to** (or
shouldn't be) — the application is writing to the target.

### Step 6: drain and stop the CDC stream

```sh
sluice sync stop --stream-id myapp-prod --wait
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
- **Skipped the `--migration-id` / `--stream-id` link.** If they
  don't match, the CDC stream tries to cold-start (since it has no
  prior position for that stream-id), which the target's preflight
  refuses with a clear message.

## What's NOT in this recipe

- **Mid-stream schema changes during the sync window.** Use `sluice
  schema add-table TABLE --stream-id ID --no-drain` for live add-table.
  See [`docs/schema-change-runbook.md`](../schema-change-runbook.md).
- **Backup chains.** See [recipe-backup-encrypted.md](recipe-backup-encrypted.md).
- **PII redaction during the migrate / sync.** See
  [recipe-redaction-keyset.md](recipe-redaction-keyset.md).

## See also

- [`docs/cutover.md`](../cutover.md) — the per-feature cutover
  documentation with all flag knobs.
- [`docs/snapshot-cdc-handoff.md`](../snapshot-cdc-handoff.md) — how
  sluice closes the gap between bulk-copy end-position and CDC start.
