# sluice v0.99.151

**`sluice trigger prune` — bound the trigger-CDC change-log. The `sqlite-trigger`/`d1-trigger`/`pgtrigger` engines append a change-log row per change and never reaped consumed ones, so a continuous sync grew the source change-log without limit (a test run bloated a 476 MB SQLite file to 1.06 GB in three minutes). The new command safely reaps rows the target has already durably applied.**

## Features

**`sluice trigger prune` (ADR-0137 Phase A, Bug 165).** Reaps consumed rows from the source `sluice_change_log`:

```
sluice trigger prune --source-driver sqlite-trigger --source ./app.db \
  --target-driver postgres --target <pg-dsn> --stream-id <id>
```

It reads the durably-persisted CDC position for the stream from the **target** (the same cdc-state row the applier writes and `sync status` reads), extracts the applied `last_id`, and `DELETE`s `sluice_change_log` rows with `id <= (applied_last_id - --keep)` on the source — engine-dispatched (SQLite over the file, D1 over the `/query` HTTP API, Postgres over SQL). It is idempotent and **safe to run while a sync is live** (it only removes rows the target already durably applied; the live reader is reading `id > applied_last_id >= cut`). Flags: `--keep N` (default 1000) keeps the most-recent N ids below the frontier as a margin; `--vacuum` (SQLite/D1, off by default) reclaims file space; `--dry-run` previews the bound.

## The safety invariant (why it can't lose data)

The prune bound is the **target's durable frontier**, never the source reader's read cursor — the reader runs *ahead* of what's been durably applied, so pruning on it would delete not-yet-applied rows and a warm-resume (`id > durable_watermark`) would find them gone (silent loss). So the command:
- refuses loudly if it cannot read the target's durable position (never prunes blind);
- refuses a `--stream-id` whose position isn't a trigger-CDC watermark (a foreign pgoutput/GTID/broker token that would otherwise decode to `last_id=0` and look like "nothing to prune");
- cross-checks `--source` against the stream's recorded source fingerprint (ADR-0031) and refuses on mismatch for PostgreSQL sources.

Pinned by unit tests (the prune-bound math; the inclusive `id <= cut` boundary; the refuse-on-garbled/foreign-token path) and cross-engine integration tests (`sqlite-trigger` and `pgtrigger` → Postgres) proving rows at/below the cut are reaped while the durable position is unchanged **and warm-resume after the prune still converges exactly-once** — including a `--keep 0` leg that deletes `id == applied_last_id` exactly and still resumes exactly-once.

## Compatibility

Additive: a new `trigger prune` subcommand; nothing else changes, and not running it leaves today's behavior (an append-only change-log). Known limit (tracked follow-up): the `--source`/`--stream-id` mis-pairing cross-check covers PostgreSQL sources; SQLite-file and `d1://` sources carry no `host:port:db` fingerprint, so for them the cross-check can't run — the command prints a `note:` and the operator doc warns to pass the exact `--source`/`--stream-id` pair the sync uses. Automatic in-stream pruning (the streamer prunes on a durable-checkpoint cadence, no manual scheduling) is the deferred ADR-0137 Phase B.

## Who needs this

Anyone running a long-lived continuous `sync` from a trigger-CDC source (`sqlite-trigger`, `d1-trigger`, `pgtrigger`) — schedule `sluice trigger prune` (cron/sidecar) to keep the source change-log (and, on D1, billable storage) bounded.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.151 · **Container:** ghcr.io/sluicesync/sluice:0.99.151
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
