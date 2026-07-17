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
(self-managed: set wal_level=logical in postgresql.conf and restart; managed providers
expose it as a setting — e.g. Neon's enable_logical_replication toggle;
see docs/postgres-source-prep.md for the provider matrix)
```

How you change it depends on who runs the server — `wal_level` cannot be changed live anywhere, but managed providers wrap the restart (or pre-enable it) differently:

| Provider | How to get `wal_level=logical` | Notes |
|----------|-------------------------------|-------|
| Self-managed | `wal_level = logical` in `postgresql.conf`, then restart the cluster | The classic path; plan the restart window. |
| Neon | Project setting **`enable_logical_replication`** (console: Settings → Logical replication, or the project-update API) | **Irreversible**; takes effect in seconds with no visible downtime (validated live 2026-07-15). |
| Supabase | Nothing — `logical` is on by default | Role `postgres` already carries the REPLICATION attribute (validated live 2026-07-16). CDC needs the direct endpoint, which is IPv6-only unless the Pro-plan IPv4 add-on is enabled; `max_slot_wal_keep_size=512MB` is the platform default — a small detach window. See [managed-services](managed-services.md#supabase-postgres). |
| PlanetScale Postgres | On by default on current provisioning; verify with `SHOW wal_level` | Older/custom-provisioned databases may differ. |
| AWS RDS / Aurora | Parameter group: `rds.logical_replication = 1`, then reboot | The GUC itself is read-only on RDS; the rds.* parameter drives it. With `backup-retention-period 0` the baseline is `minimal` (not the usually-assumed `replica`) — the parameter flip forces `logical` either way; don't detour via "enable backups". |
| GCP CloudSQL | Flag `cloudsql.logical_decoding = on`, then restart | Validated live 2026-07-16: the restart is automatic inside `gcloud sql instances patch` (~1 min); `--database-flags` replaces the whole flag set, so include existing flags. Then grant the role: `ALTER ROLE <role> REPLICATION` — works as the default `postgres` user (Cloud SQL patches the check for `cloudsqlsuperuser` members). Baseline is `replica` regardless of backup settings (no RDS-style `minimal` trap). See [managed-services](managed-services.md#google-cloud-sql-for-postgresql). |
| Azure Database for PostgreSQL (Flexible) | `az postgres flexible-server parameter set --name wal_level --value logical`, then an **explicit** `az postgres flexible-server restart` (~1 min) | Validated live 2026-07-17: the parameter set alone does NOT apply it (contrast Cloud SQL's auto-restart). Then `ALTER ROLE <role> REPLICATION` — self-grantable by the admin (Azure patches the grant for `azure_pg_admin` members; there is no RDS-style membership model). See [managed-services](managed-services.md#azure-database-for-postgresql-flexible-server). |
| Vultr Managed Databases | Nothing — `logical` is on by default | Validated live 2026-07-16 (Aiven lineage): the master user `vultradmin` ships with the REPLICATION attribute **pre-granted** — `sync start` works with zero preparation. For custom roles `ALTER ROLE <role> REPLICATION` works as `vultradmin`. Leave the platform's `pghoard_local` slot alone. See [managed-services](managed-services.md#vultr-managed-databases-for-postgresql). |

### WAL volume cost of `wal_level = logical`

Flipping `wal_level` from `replica` to `logical` increases the WAL volume the source emits. Two things happen at logical level that don't at replica level:

1. **Full-page images (FPIs) are still emitted on every checkpoint-boundary first-touch** (this is independent of `wal_level`), but every page that carries a logically-decodable change *also* carries the full new tuple in WAL — not just the heap-page diff. The downstream decoder needs the full row to reconstruct the logical event.
2. **TOAST'd values for UPDATE/DELETE-target rows are written into WAL even when they didn't change** (`REPLICA IDENTITY FULL` amplifies this; the default `DEFAULT` setting limits it to the primary key, but logical-level still carries more than replica-level does).

In our internal measurements on a typical OLTP workload, the WAL byte-rate roughly **1.2×–1.6×** what the same workload generated under `wal_level = replica` — sometimes more on tables with large TEXT/JSONB columns under `REPLICA IDENTITY FULL`. The exact ratio is workload-dependent; the right way to size storage and replication-slot retention is to measure your own workload at `logical` before depending on it in production.

Operator-visible consequences:

- **Slot-retention disk pressure.** A slot that falls behind during a sluice outage holds WAL until sluice catches up. The 1.2×–1.6× multiplier applies to that retention too; budget `max_slot_wal_keep_size` accordingly.
- **Backup volume.** WAL-archiving solutions (pgBackRest, wal-g, managed-service WAL archives) ingest the larger WAL stream.
- **Network bandwidth to replicas / wal_receiver.** Physical replicas downstream of a `wal_level = logical` primary see the same larger stream.

This is fundamental to PG's logical-replication design — sluice cannot avoid it, only surface it. References: The Internals of PostgreSQL Ch 9.4 (WAL record format, FPI semantics under different wal_level settings), Ch 9.5 (asynchronous commit and WAL flush). Sluice's F6 PG-internals research finding (2026-05-22) documents the trade-off in more detail.

### Logical-decoding spill (large-transaction memory pressure)

When PG decodes a transaction through a logical-replication slot, the ReorderBuffer holds the decoded change records in memory until commit so it can emit them in commit order. If a transaction is large enough that the buffered changes exceed `logical_decoding_work_mem` (default **64 MB** as of PG 14+), PG spools the un-emitted records to disk under `pg_replslot/<slot>/snap/`.

Spill is not data-loss-class on its own — PG correctly serialises and replays the spilled records when emitting. But sustained spill puts disk pressure on the source's `pg_replslot/` directory, and **if that directory fills, PG can invalidate the slot** (the slot's `wal_status` flips to `lost`). A lost slot IS silent-loss-class for sluice — sluice has to drop and recreate it, and any unconsumed changes are gone.

Sluice surfaces spill stats from PG 14+'s `pg_stat_replication_slots` view in two places:

```
$ sluice sync health --source-driver postgres --source $SOURCE --target-driver postgres --target $TARGET --stream-id myapp ...
stream: myapp
state: healthy
...
spill_txns: 17
spill_bytes: 5_242_880
```

```
# Prometheus /metrics (when --metrics-listen is set on sluice sync start)
sluice_pg_slot_spill_txns_total{stream_id="myapp",slot="sluice_slot"} 17
sluice_pg_slot_spill_bytes_total{stream_id="myapp",slot="sluice_slot"} 5242880
```

Both counters are **cumulative since slot creation** — they accumulate over the slot's lifetime and only reset when the slot is dropped + recreated. Operators usually want to alert on the *rate of change* (`rate(sluice_pg_slot_spill_bytes_total[5m])`) rather than the absolute value.

**When the lines are absent:**

- The source is PG < 14 (the `pg_stat_replication_slots` view doesn't exist).
- The slot has never been used for decoding yet (no row in the view).
- The source engine isn't Postgres (MySQL has no analogue — binlog decoding doesn't spool the same way).

Sluice deliberately omits the fields rather than emitting `0` in these "no signal" cases, so a careless reader can't mistake "we can't tell" for "definitely no spill."

**Operator action when these grow:**

1. **Bump `logical_decoding_work_mem` on the source.** This is the cheapest fix and is live-reloadable:
   ```sql
   ALTER SYSTEM SET logical_decoding_work_mem = '256MB';
   SELECT pg_reload_conf();
   ```
   The right value is workload-dependent — pick something safely above your largest CDC transaction's decoded-change-buffer size. 256 MB to 1 GB is typical for OLTP workloads with occasional bulk loads.
2. **Split large application transactions.** Bulk `INSERT … SELECT` over millions of rows and large multi-statement `UPDATE`s are the usual culprits. Chunking the work into smaller transactions keeps each within the in-memory budget.
3. **Watch `pg_replslot/` disk usage** alongside the spill counters. If the directory fills, the slot will be invalidated and recovery requires a full re-snapshot.

References: The Internals of PostgreSQL Ch 9.4 (logical decoding, ReorderBuffer), `pg_stat_replication_slots` view documentation in the PG manual. Sluice's F2 PG-internals research finding (2026-05-22) documents the surface wire-up.

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

Both required for PG 17 native slot sync.

#### The idle-slot failover trap (load-bearing)

**Even with all three mechanisms configured — Patroni `slots:` / "Logical slot name", `sync_replication_slots = on`, and `hot_standby_feedback = on` — a slot that hasn't advanced during the slot-sync window can still be lost on failover.** Operator-confirmed in production.

The failure mode: Patroni's slot-sync (and PG 17's native equivalent, gated by `logical_slot_sync_timeout`, default 300s) is a primary→standby pull. The standby periodically copies slot state from the primary. If the primary's slot hasn't advanced for the duration of the sync window — e.g. the source database is quiet, the consumer is paused, or the consumer's host is down — the standby's replica copy stays at an old LSN. When failover then promotes the standby, the new primary's slot points at WAL that may already have been recycled. Result: slot is "present but invalid" and your CDC stream surfaces a `wal_status='lost'` error on resume.

**Mitigations, ranked:**

1. **Keep the slot consumer running.** Sluice's PG CDC reader sends `pg_send_standby_status_update` every 10 seconds whether or not events are flowing (see `internal/engines/postgres/cdc_reader.go`'s keepalive loop). As long as `sluice sync start` is the active consumer, the slot is "active" from the primary's perspective and the standby's sync will keep pace. **Don't run sluice as a one-shot during low-traffic windows; run it continuously.**
2. **For low-traffic source databases**, inject lightweight WAL activity from the source side — e.g. a periodic `INSERT INTO heartbeat (ts) VALUES (now())` against a small dedicated table, or a `SELECT pg_logical_emit_message(false, 'sluice-heartbeat', '')`. The latter writes to WAL without modifying any user data; sluice's CDC reader sees and discards it. This guarantees slot advancement even if the active consumer is briefly disconnected.
3. **Tune the sync window upward.** `logical_slot_sync_timeout` can be increased; on Patroni, the equivalent knobs are `loop_wait` (default 10s) and the `dcs.permanent_slots` consistency policy. Bigger windows = more tolerance for idle slots, but at the cost of slower failover detection.
4. **Accept the risk** and rely on sluice's slot-missing fall-through (Item F, ADR-0022) — `sync start --resume` detects the lost slot, drops it, and falls through to cold-start. Operationally heavy but always works. Pair with `--reset-target-data` if the target also needs to be wiped.

The trap is silent at the time of failover — there's no error in PG's logs naming the dropped slot. You only see it when the consumer reconnects and gets `wal_status='lost'` or `replication slot does not exist`. Audit your slot's `pg_replication_slots.confirmed_flush_lsn` advancement rate as part of pre-production checks; if it doesn't advance for hours at a time on a quiet workload, mitigation #2 (heartbeat writes) is the durable fix.

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

A running sync watches these states continuously (the ADR-0059 slot-health probe, 30s cadence): `unreserved` pages **critical** immediately — invalidation lands at the next checkpoint, the last window where catching the consumer up can still save the slot — and `lost` (or the slot being dropped out from under a sync that already had a health condition outstanding) is a **terminal** critical page: it fires exactly once and then latches for the rest of the stream — no repeats, and never a "condition cleared" — because the loss is unrecoverable and the only remaining action is the re-snapshot recovery below. If the slot-health probe itself starts failing (revoked role, killed connection), sluice warns and pages after 5 consecutive probe failures rather than going silently blind.

When sluice sees a slot in `unreserved` or `lost` state at startup, it refuses to start replication and surfaces a clear error pointing at the recovery path:

```
postgres: replication slot "sluice_slot" has wal_status="lost" — required WAL has been
permanently removed; the slot must be dropped and recreated. To recover:
`sluice slot drop sluice_slot --source-driver=postgres --source ...` then restart with
empty position (forces a fresh snapshot). To prevent recurrence, raise
max_slot_wal_keep_size on the source — PlanetScale recommends > 4GB
```

### One-command recovery via `--reset-target-data`

After dropping the slot on the source, the next `sluice sync start` will trip the cold-start pre-flight refusal because the target still has the partially-streamed dest data. Two paths past the refusal:

- **`sluice sync start --reset-target-data --yes ...`** (recommended for recovery): clears `sluice_cdc_state` and DROPs every source-schema table on the target, then runs cold-start. Confirmation prompt requires typing `reset` verbatim unless `--yes` is set. See [ADR-0023](adr/adr-0023-reset-target-data.md).
- **`sluice sync start --force-cold-start ...`**: bypasses the refusal but does *not* clean up. INSERTs into the populated dest collide on PRIMARY KEY. Use only when you have manually wiped or otherwise prepared the target.

`--reset-target-data` is destructive on the target — drops every table sluice manages on that DSN. Other tables on the target (sluice's bookkeeping tables aside) are untouched. The `slot drop` on the source is still a precondition; the flag handles dest-side cleanup only.

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
