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
loudly and deletes nothing**. There is no "prune blind" mode. It also
refuses if the `--stream-id` resolves to a position that is not a
trigger-CDC watermark (e.g. you pointed it at a pgoutput / GTID stream).

## ⚠️ Pass the EXACT `--source` / `--stream-id` pair the sync uses

The frontier is read from the stream named by `--stream-id`, and the cut
it produces is applied to the change-log at `--source`. If you pair
`--source` (change-log A) with a `--stream-id` whose frontier belongs to
a **different** source B, the two are in different id spaces — and B's
cut applied to A can delete A's not-yet-applied rows: **silent loss.**

sluice cross-checks this where it can. Each stream records a source
*fingerprint* (host:port:database, ADR-0031); for a **PostgreSQL** source
(`postgres-trigger`) the command recomputes `--source`'s fingerprint and
**refuses loudly on a mismatch**. For a **SQLite file** (`sqlite-trigger`)
or a **D1** (`d1-trigger`) source there is no recorded fingerprint (only
host:port:db is fingerprinted), so the cross-check **cannot run** — the
command prints a `note:` saying so and trusts the pair you passed. For
those sources it is **your responsibility** to pass the exact
`--source`/`--stream-id` pair the sync runs with. (Extending the
fingerprint to file/D1 sources so the cross-check covers them too is a
tracked Phase-A.1 follow-up.)

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

## SQLite WAL bounding (automatic; `sqlite-trigger` only)

`trigger prune` bounds the change-log **row count** in the main database.
A separate concern on a local SQLite source is the **write-ahead log
(`<db>-wal`) file size**. Every captured change is a write, and the
continuous CDC poller holds a reader on the source; under sustained
churn that combination prevents SQLite's checkpoint from resetting the
WAL, so the `-wal` file (and the sync's resident memory, which tracks it)
can grow without bound even while the change-log row count stays bounded
by prune. (In a ~52-min endurance run the WAL reached 75 GB on a 20 GB
database before this was fixed — Bug 167.)

Since v0.99.152 the `sqlite-trigger` poller bounds the WAL automatically,
with **no operator action**:

- it does not hold an idle pooled read connection between polls (the idle
  connection's stale read-mark was what pinned the checkpoint), so your
  app's own auto-checkpoint can reset the WAL normally; and
- it issues `PRAGMA wal_checkpoint(TRUNCATE)` on a ~30 s cadence as a
  backstop, so the WAL stays bounded **even if your application sets
  `PRAGMA wal_autocheckpoint=0`** (disables SQLite's own checkpointing).

This is pure WAL-file management — it never changes what is read or
applied, and never moves the watermark, so exactly-once is unaffected.
The checkpoint runs between polls in the poller's own goroutine and a
momentary `BUSY` (another connection held the WAL) is simply retried on
the next cadence; it never blocks or fails the stream. The `d1-trigger`
engine polls Cloudflare D1 over HTTP and has no local WAL, so this does
not apply there.

If you want to inspect WAL behaviour, watch the `<db>-wal` file size next
to your source `.db` while a sync runs; with the fix it stays small
(roughly one checkpoint interval of churn) instead of climbing.

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
