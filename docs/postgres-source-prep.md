# Postgres source preparation

Before running a sluice CDC stream against a Postgres source, the cluster needs a small set of GUCs and (on managed Postgres products like PlanetScale) a few cluster-level settings. This guide is the practical checklist; the *why* is collected at the end so you can skim during incident response and read the rationale separately.

## tl;dr — what to check

- `wal_level = logical` — required (cluster restart to change)
- `max_replication_slots ≥ 2 × replicas` — required (cluster restart to change)
- `max_wal_senders ≥ 2 × replicas`, and ≥ `max_replication_slots` — required (cluster restart to change)
- `max_slot_wal_keep_size > 4GB` — strongly recommended (live)
- The connecting role must have the `REPLICATION` attribute
- The sluice slot name (default `sluice_slot`) must be listed in the cluster's "Logical slot name" / Patroni `slots:` / PG 17 `sync_replication_slots` configuration *before* failover or switchover events — see the failover section below
- For PG 17+ HA: `sync_replication_slots = on` and `hot_standby_feedback = on`

## Required GUCs

```sql
-- run as a superuser on the source
SHOW wal_level;                  -- must be 'logical'
SHOW max_replication_slots;      -- e.g. 20 on PlanetScale defaults
SHOW max_wal_senders;            -- e.g. 10 on PlanetScale defaults
SHOW max_slot_wal_keep_size;     -- '> 4GB' recommended; '-1' = unlimited (risky)
```

If `wal_level` is not `logical`, sluice's CDC reader fails the precondition check at startup with a clear error rather than touching the slot. You'll see something like:

```
postgres: cdc: wal_level is "replica"; must be 'logical' for logical replication
(set wal_level=logical in postgresql.conf and restart)
```

To change `wal_level`, edit `postgresql.conf` (or the managed-service equivalent) and restart the cluster. It cannot be changed live.

## Connecting role

The role sluice uses to read CDC needs the `REPLICATION` attribute:

```sql
ALTER ROLE sluice_user WITH REPLICATION;
```

Without it, sluice fails on the first replication-protocol command rather than mid-stream.

## Slot lifetime under failover

This is the part that bites people. **A logical replication slot is a primary-local object by default.** When the primary fails over, the slot does not automatically move to the new primary. If your CDC stream depends on a slot that's been left behind, your only recovery is to drop the dead slot, create a fresh one, and re-snapshot.

There are three orthogonal mechanisms that preserve slots across failover. You generally want *one* of them, and you need to confirm it's actually configured before betting your production CDC stream on it.

### On PlanetScale Postgres (Patroni-managed)

PlanetScale uses [Patroni](https://patroni.readthedocs.io/) for HA. Patroni's `slots:` config defines "permanent replication slots" that are preserved across switchover and failover. The dashboard surfaces this as the **"Logical slot name"** field under *Cluster configuration > Parameters > Failover*. **Add the sluice slot name (default `sluice_slot`) there** — values are comma-delimited if you have more than one consumer.

Critical: slots **not** listed in that field are **silently** lost on failover. No error, no warning — your CDC stream just begins to miss changes. Audit the field as part of your pre-production checklist.

For PG 17+ clusters, you can additionally enable native PG slot synchronization:

```
sync_replication_slots = on
hot_standby_feedback   = on
```

Both required for PG 17 native slot sync. `logical_slot_sync_timeout` (default 300s) bounds how long failover waits for slot sync to finish — relevant when your CDC client sleeps for long periods, which may otherwise let failover proceed before the slot is synchronized.

### On self-hosted Patroni

Same idea, set explicitly:

```yaml
slots:
  sluice_slot:
    type: logical
    database: <your_db>
    plugin: pgoutput
```

### On vanilla Postgres without HA

You don't need to do anything special — there's no failover to worry about. But you should monitor slot health (see below) because a slow consumer can still cause WAL bloat or slot invalidation.

## When a slot becomes unusable

A logical slot can transition through these states (visible in `pg_replication_slots.wal_status`):

| wal_status   | Meaning                                                                           |
| ------------ | --------------------------------------------------------------------------------- |
| `reserved`   | Healthy. All required WAL is on disk.                                             |
| `extended`   | Healthy but consumer is behind; slot is keeping more WAL than `max_wal_size`.     |
| `unreserved` | Required WAL has been removed from `pg_wal` but is still recoverable.             |
| `lost`       | Required WAL is gone. The slot still exists but cannot be used.                   |

The transition is driven by `max_slot_wal_keep_size`: when WAL backed up by a slot exceeds that cap, the slot is marked `unreserved` and, after the next checkpoint, `lost`. The default of `-1` means "unlimited" — slots will retain WAL until disk fills, which is its own bad day. The middle ground (PlanetScale's recommendation) is `> 4GB`: gives slots room to recover from short consumer outages without letting one stuck slot fill the disk.

When sluice sees a slot in `unreserved` or `lost` state, it refuses to start replication and surfaces a clear error pointing at the recovery path:

```
postgres: replication slot "sluice_slot" has wal_status="lost" — required WAL has been
permanently removed; the slot must be dropped and recreated. To recover:
`sluice slot drop sluice_slot --source-driver=postgres --source ...` then restart with
empty position (forces a fresh snapshot). To prevent recurrence, raise
max_slot_wal_keep_size on the source — PlanetScale recommends > 4GB
```

## Operator commands

List slots on the source:

```bash
sluice slot list --source-driver postgres --source 'postgres://...'
```

Drop a slot (prompts for confirmation; pass `--yes` to skip):

```bash
sluice slot drop sluice_slot --source-driver postgres --source 'postgres://...'
```

If the slot is currently in use by a CDC consumer, drop refuses unless `--force` is set. Equivalently in psql:

```sql
SELECT slot_name, plugin, active, wal_status, restart_lsn, confirmed_flush_lsn
FROM   pg_replication_slots;

SELECT pg_drop_replication_slot('sluice_slot');
```

## Auto-cleanup on failed cold-start

When you start a new sluice CDC stream and something goes wrong *during setup* (publication permissions, START_REPLICATION rejection, ctx cancellation), the freshly-created slot is auto-dropped before the error is returned. You should not see leftover `sluice_slot`-named slots on the source from failed setup attempts.

Auto-cleanup deliberately does not run when:

- The slot already existed before the call. It might carry someone else's progress.
- The pump goroutine fails *after* the channel is returned. At that point, changes may have been emitted whose positions reference the slot — that's user data, hands off.

For those cases, `sluice slot drop` is the explicit cleanup path.

## References

- [PlanetScale Postgres logical CDC integration guide](https://planetscale.com/docs/postgres/integrations/logical-cdc) — managed-service-specific guidance for parameters and the "Logical slot name" UI
- [PlanetScale: Postgres High Availability with CDC](https://planetscale.com/blog/postgres-ha-with-cdc) — failover mechanics, the three failure modes (quiet period, replica replacement, WAL pin growth)
- [Patroni dynamic configuration: slots](https://patroni.readthedocs.io/en/latest/dynamic_configuration.html) — for self-hosted Patroni clusters
- [PostgreSQL: pg_replication_slots view](https://www.postgresql.org/docs/current/view-pg-replication-slots.html) — official `wal_status` semantics
- [Mastering Postgres Replication Slots — Gunnar Morling](https://www.morling.dev/blog/mastering-postgres-replication-slots/) — practical walkthrough of slot lifecycle and pitfalls
