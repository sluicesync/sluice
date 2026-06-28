# Trigger-CDC change-log retention (`sluice trigger prune`)

The trigger-CDC source engines — `sqlite-trigger` (local SQLite file),
`d1-trigger` (Cloudflare D1 over HTTP), and `postgres-trigger` (PG) —
capture every source change as a row in `sluice_change_log`. The CDC
reader advances a watermark over those rows but **does not reap them**,
so on a long-running, write-heavy continuous sync the change-log grows
unbounded: it can dwarf the base tables and eventually fill disk (on D1
it is also billable rows-written / storage).

`sluice trigger prune` is the operator-run reaper. It deletes change-log
rows the **target has already durably applied**, so it is safe to run at
any time — including while a sync is live.

## The one safety rule (read this)

A change-log row may be reaped **only if it has been durably applied on
the target**. `sluice trigger prune` enforces this by construction: it
reads the durably-persisted CDC position from the **target** (the same
position `sluice sync status` shows), and deletes only rows at or below
that frontier (minus a margin).

It will **never** prune based on the source's own state (the reader's
read-cursor runs *ahead* of the durable frontier; the source `MAX(id)`
is further ahead still). Pruning on either would delete rows the target
has not yet applied — and a warm-resume after a crash would then look
for those rows and find them gone: **silent, permanent data loss.**

If the command cannot read the target's durable position (no sync has
applied anything yet, or the target is unreachable), it **refuses
loudly and deletes nothing**. There is no "prune blind" mode.

## Usage

```
sluice trigger prune \
  --source-driver sqlite-trigger \
  --source ./app.db \
  --target-driver postgres \
  --target 'postgres://user:pass@host/db' \
  --stream-id myapp-prod
```

The coordinates are the same ones the sync uses:

| Flag              | What it points at                                                        |
|-------------------|--------------------------------------------------------------------------|
| `--source-driver` | The trigger engine: `sqlite-trigger`, `d1-trigger`, or `postgres-trigger`. |
| `--source`        | Where `sluice_change_log` lives (SQLite file path / `d1://…` / PG DSN).   |
| `--target-driver` | The target engine (`postgres`, `mysql`) — where the durable position lives. |
| `--target`        | The target DSN (the same target the sync applies to).                    |
| `--stream-id`     | The stream whose durable position bounds the prune (same `--stream-id` as the sync). |

For `postgres-trigger`, `--schema` selects the schema holding
`sluice_change_log` (defaults to the DSN's `schema` parameter). It is
ignored for the SQLite/D1 engines (flat namespace).

## Flags

- **`--keep N`** (default `1000`) — keep the most-recent N change-log
  ids below the durable frontier unpruned, as a belt-and-suspenders
  margin. The frontier itself is already durably applied, so even
  `--keep 0` is safe; the default leaves a small buffer. The delete
  bound is `id <= (applied_last_id - N)`; if that is `<= 0` the command
  reports a no-op and exits 0.
- **`--vacuum`** (off by default; `sqlite-trigger` / `d1-trigger` only)
  — run `VACUUM` after the delete to reclaim file space. Off by default
  because `VACUUM` rewrites the entire database. Postgres reclaims the
  freed space via autovacuum, so `--vacuum` is rejected for
  `postgres-trigger`.
- **`--dry-run` / `-n`** — print the computed prune bound and the
  current change-log size without deleting anything.

## Scheduling it

Phase A is an explicit operator action. Run it on a cron / timer
alongside the continuous sync — e.g. hourly:

```
# crontab: prune the change-log every hour
0 * * * * /usr/local/bin/sluice trigger prune \
  --source-driver sqlite-trigger --source /var/lib/app/app.db \
  --target-driver postgres --target "$SLUICE_TARGET" \
  --stream-id myapp-prod >> /var/log/sluice-prune.log 2>&1
```

`--source` reads `$SLUICE_SOURCE` and `--target` reads `$SLUICE_TARGET`
if the flags are omitted, so the secrets can stay in the unit's
environment rather than the crontab line.

It is idempotent (a second run with the same durable frontier deletes
nothing) and safe to overlap with a live sync, so a simple fixed-cadence
schedule needs no coordination with the streamer.

## Deferred: automatic in-stream pruning

Having the streamer prune the change-log itself on a durable-checkpoint
cadence (so operators don't schedule anything) is **ADR-0137 Phase B**,
a deferred follow-up. Until it lands, bounding change-log growth on a
continuous sync is the explicit operator action documented here.

## See also

- `docs/adr/adr-0137-trigger-changelog-retention.md` — the design and
  the silent-loss-avoidance rationale.
- `docs/adr/adr-0135-sqlite-trigger-cdc.md`,
  `docs/adr/adr-0136-d1-trigger-cdc.md`,
  `docs/adr/adr-0066-postgres-trigger-engine-variant.md` — the trigger
  engines themselves.
