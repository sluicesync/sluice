# Recipe — continuous replication via the backup chain (`sync from-backup`)

Replicate from a Postgres source to a target by reading the backup
chain instead of the source's CDC stream directly. Useful when you
have **decoupled transport** — the source and target can't (or
shouldn't) talk to each other directly, but they share access to the
same backup store.

Producer + consumer pattern: one `sluice` process emits a backup chain
from the source; another `sluice` process tails the chain and applies
the changes to the target. Both run continuously; the backup store
serves as the message log between them.

## When to use this recipe

- **Air-gapped target.** Source is in network A; target is in network
  B; the only thing crossing the boundary is the backup store.
- **Cross-region replication with shared backup store.** The chain is
  already crossing the region for DR; using it as the replication
  transport too avoids duplicate egress.
- **Compliance / audit-trail-driven replication.** The backup chain is
  the canonical record of change; the target derives from the chain
  so the audit trail and the replicated state are consistent
  by-construction.
- **Multi-target fan-out from one chain.** N consumers tail the same
  chain into N different targets. Any new target just starts a
  consumer; no producer-side reconfiguration.

If none of these apply — if your source and target can talk to each
other directly — use [`sluice sync start`](recipe-bidirectional-cutover.md)
instead. It's lower-latency and lower-overhead than the broker for
the direct-CDC case.

## Broker vs. `sync start` — decision matrix

| Property | `sluice sync start` | `sluice sync from-backup run` (broker) |
|---|---|---|
| Source-to-target connectivity required | Yes — direct CDC stream | No — backup store is the transport |
| Latency floor | Sub-second (poll + apply) | Poll cadence (default 30s) + apply |
| Throughput ceiling | Source's CDC emission rate (very high) | Bound by chunk-bytes-per-poll-tick (moderate) |
| Multi-target fan-out | One stream per target | One chain feeds N consumers |
| Backup chain as side-effect | Separate (run `backup stream` if you want one) | The chain *is* the input |
| High-volume friendly | Yes — direct CDC | No — broker is designed for moderate volumes |

The honest framing: the broker isn't trying to compete with
`sync start` on throughput or latency. It's trading those for the
**decoupled-transport** property. For high-volume workloads use
`sync start`; for decoupled-transport at moderate volumes, the broker
fits.

## What you need

- A sluice binary on your PATH (`sluice --version` works).
- The source DSN — a connection string with read access to all tables
  you want to replicate and the CDC prerequisites
  (`wal_level=logical` for Postgres; see [`docs/postgres-source-prep.md`](../postgres-source-prep.md)).
- The target DSN — a connection string with `CREATE TABLE` permission
  on the target database.
- A backup store both processes can reach — local filesystem for
  same-host testing, or S3/GCS/Azure Blob for the real
  decoupled-transport case.

## The flow

### Step 1: producer takes the full backup

```sh
sluice backup full \
    --source-driver postgres \
    --source 'postgres://...source...' \
    --output-dir /var/backups/myapp
```

This lands one full backup chain root in the store. The consumer's
`restore` step (step 3 below) uses this full as its cold-start
bulk-copy.

### Step 2: producer starts the continuous stream

```sh
# The producer has no --stream-id: it is identified by the chain it
# extends (the parent full in --output-dir, or --since to pick one).
sluice backup stream run \
    --source-driver postgres \
    --source 'postgres://...source...' \
    --output-dir /var/backups/myapp \
    --rollover-window 10s \
    --retain-rotate-at-chain-length 20
```

Operationally a long-running process — run it under systemd / k8s /
your supervisor of choice. The `--rollover-window` controls how often
the producer commits an incremental (chunks accumulated during the
window get bundled into one manifest); `--retain-rotate-at-chain-length`
controls when the chain rotates into a new segment (useful for
keeping individual segments compact for `backup prune` operations).
With rotation enabled the producer refuses to start while a source
table has no `PRIMARY KEY` and no `NOT NULL UNIQUE` index
(`SLUICE-E-BACKUP-ROTATED-KEYLESS-TABLE`): a chain restore applies each
later segment full over the earlier segments' rows, which such a table
cannot absorb. The broker refuses keyless tables on its own account
(`SLUICE-E-BROKER-KEYLESS-TABLE`), so a broker topology needs keys
either way.

### Step 3: consumer bulk-copies the full

```sh
sluice restore \
    --from-dir /var/backups/myapp \
    --target-driver postgres \
    --target 'postgres://...target...'
```

This bulk-copies the full's contents to the target. After this step
the target has the source's state as of the moment the full was
taken; the broker then tails the chain to apply changes from that
point forward.

### Step 4: consumer starts the broker

```sh
sluice sync from-backup run \
    --backup-dir /var/backups/myapp \
    --target-driver postgres \
    --target 'postgres://...target...' \
    --stream-id myapp-broker \
    --poll-interval 10s
```

The broker reads `lineage.json` at the configured `--poll-interval`,
lists manifests newer than the consumer's last-applied position, and
applies them in chain order. Position is persisted in the target's
`sluice_cdc_state` table — the same control table used by direct
`sync start`, with a different `position_engine` sentinel
(`"backup-broker"`) so the two surfaces don't collide on the same
stream-id.

## Cold-start gotcha — `--at-chain-id`

If you point the broker at a chain it hasn't seen before AND the
target's `sluice_cdc_state` has no row for the chosen
`--stream-id`, the broker refuses loudly because it doesn't know
where in the chain to start. The refusal names two recovery paths:

- `--reset-target-data` — truncate the target and replay the chain
  from the chain root. Suitable when the target is empty (after a
  fresh `restore`).
- `--at-chain-id=<backup-id>` — start tailing from after the named
  backup ID. Useful when the target's data was already brought up
  to a known checkpoint by some other path (a parallel `restore`,
  a manual `pg_dump`+`pg_restore`, etc.) and you want the broker to
  pick up incrementals from there forward.

The most common case is the post-`restore` cold-start: the operator
just ran `restore` (step 3 above), the target is at the chain root's
`end_position`, and they want the broker to tail forward from there.
Pass `--at-chain-id=<the-most-recent-full-manifest's-backup-id>`
once on first launch; subsequent broker restarts read from
`sluice_cdc_state` automatically and don't need the flag.

```sh
# First launch after a fresh restore:
sluice sync from-backup run \
    --backup-dir /var/backups/myapp \
    --target-driver postgres \
    --target 'postgres://...target...' \
    --stream-id myapp-broker \
    --poll-interval 10s \
    --at-chain-id 9b12b8ccdc3e7fa9725825ab032e6d6d41d3db09

# Subsequent restarts (warm-resume from sluice_cdc_state):
sluice sync from-backup run \
    --backup-dir /var/backups/myapp \
    --target-driver postgres \
    --target 'postgres://...target...' \
    --stream-id myapp-broker \
    --poll-interval 10s
```

## Rotation behaviour (multi-segment chains)

The producer in step 2 above is configured to rotate the chain into
a new segment when its incremental count crosses 20
(`--retain-rotate-at-chain-length=20`). The broker follows rotation
seams automatically — when segment N caps and segment N+1 opens,
the broker continues tailing into the new segment without operator
intervention.

Implementation detail: the broker's apply loop skips full manifests
unconditionally, so segment-N+1's rotation snapshot is auto-skipped.
ADR-0067's born-contiguous rotation guarantees that the new
segment's first incremental covers the `(P_N, S]` overlap from the
prior segment's end position, so no changes are lost across the
rotation seam. Nor is anything applied twice there: the broker never
applies a full, and the new segment's first incremental begins exactly
where the prior segment's last incremental ended (`P_N`). The
`(P_N, S]` overlap ADR-0067 describes is replayed only by a *restore*
that starts from the new segment's full. The one way a broker applies
a change twice is the crash-recovery re-application below.

Pre-v0.97.2 sluice deferred multi-segment broker following — the
broker refused loudly at the first rotation transition with the
message `Broker following a multi-segment lineage is deferred
(ADR-0046 Phase 4.5); point the broker at a single-segment backup,
or restore the multi-segment lineage with sluice restore instead`.
v0.97.2 closed that deferral; current versions follow rotation
cleanly. **If you're on a pre-v0.97.2 sluice and need this, upgrade.**

## Crash recovery

The broker is designed for restart resilience on both sides.

### Consumer crash

```sh
# Kill it however it died (oom, process restart, k8s eviction).
# Restart it with the same --stream-id:
sluice sync from-backup run \
    --backup-dir /var/backups/myapp \
    --target-driver postgres \
    --target 'postgres://...target...' \
    --stream-id myapp-broker \
    --poll-interval 10s
```

The broker reads its position from `sluice_cdc_state` on startup,
finds it in the chain, and resumes where it stopped. That position
can stand **inside** an incremental: it advances, in the same target
transaction as the work, to the last source transaction whose effects
are durable. So an incremental the crash interrupted is re-read, what
already landed is skipped, and only the source transaction that was
in flight is re-applied. A chain written by this sluice records each
change's apply identity (the source's transaction id and the change's
ordinal), and the target's apply marks then skip that transaction's
changes that already landed: the re-run is exactly-once, for tables
without a key and for key changes alike.

A chain written by an older sluice, or an incremental smart
compaction rewrote, records no identities. There the in-flight
transaction is re-applied without marks, which converges only on
tables where the re-applied INSERT collides on a key whose columns
the replayed rows carry: a table with no PRIMARY KEY and no NOT NULL
UNIQUE index would gain a duplicate of every row of it the
interrupted run had committed, and so would a target table keyed only
on a serial, identity or defaulted surrogate (`bigserial`,
`GENERATED BY DEFAULT AS IDENTITY`, `DEFAULT gen_random_uuid()`,
MySQL `AUTO_INCREMENT`) that the rows do not supply — it draws a
fresh key for every re-applied row, and the marks cannot key it
either. So the broker refuses such a table, for each incremental that
touches it, before it applies anything of that incremental, with
`SLUICE-E-BROKER-KEYLESS-TABLE` (exit 3), naming each table, whether
the chain's recorded schema or the target table lacks the key, and
why the replay cannot be exactly-once. For an older chain,
`--reset-target-data` restores it through its tail and the broker
then judges only the incrementals that follow; otherwise the remedy
is a key on the **source** and a new full backup (or, for a
target-only miss, the source's key on the target table), or
`sluice sync start` for those tables.

Without identities, even a keyed table converges only for inserts,
and for updates and deletes that keep each row's key. A transaction
that **changed a row's key value** does not: the re-run re-inserts a
row the interrupted run had already moved, and the move then collides
with the moved copy, so it fails on a duplicate key (MySQL 1062 /
Postgres 23505) on every re-run. And when a key value was moved off
one row and onto another inside that transaction, the re-run can
apply a change to the wrong row with no error at all. If your source
changes key values and the chain records no identities, recover an
interrupted incremental with `--reset-target-data`, not by
re-running. If the broker refuses to resume with
`SLUICE-E-BROKER-INCREMENTAL-REWRITTEN`, the incremental it was
inside was rewritten (smart compaction) since; recover with
`--reset-target-data`, and do not smart-compact a segment a broker is
still replaying.

**Stopping the broker.** `sluice sync from-backup stop` (or the
`stop_requested_at` field it writes) is observed only between ticks,
so it never interrupts an incremental and the broker exits 0. A
SIGINT/SIGTERM — or `q`/ctrl+c on the live panel — cancels the run
immediately: if it lands between incrementals the exit is still 0,
but if it lands while an incremental is being applied the broker
exits non-zero with an error carrying `BROKER-INCREMENTAL-PARTIAL`,
naming the incremental. Re-run the same command; it re-applies that
incremental, which converges unless the incremental changed key
values (see above; then use `--reset-target-data`). A cancel during a `--reset-target-data`
cold start, once it has begun dropping the target's tables, exits
non-zero with `BROKER-COLD-START-PARTIAL`: the target holds a partial
restore and no position, so re-run with `--reset-target-data` (never
`--at-chain-id`).

**If a broker on v0.156.10 or earlier was ever interrupted** and the
chain carries keyless tables, compare those tables with the source:
releases v0.99.222 through v0.156.10 re-applied an interrupted
incremental into them and duplicated its committed rows at exit 0
(v0.20.0 through v0.99.221 skipped the rest of the interrupted
incremental instead). The current release refuses to start on such a
chain, so the comparison is the only way to learn whether the target
already diverged.

### Producer crash

```sh
# Restart with --force to take over the prior PID's lease:
sluice backup stream run \
    --source-driver postgres \
    --source 'postgres://...source...' \
    --output-dir /var/backups/myapp \
    --rollover-window 10s \
    --retain-rotate-at-chain-length 20 \
    --force
```

The `--force` flag is the v0.67.0 concurrent-writer guard: it
surfaces the prior PID's lease loudly (so you know you're taking
over an unclean exit), then takes the lease and resumes from the
source's last persisted `confirmed_flush_lsn`. The source's
replication slot holds WAL across the producer's outage window —
when the producer comes back, the slot still has every change since
its last position-write available to replay.

If the producer's outage is shorter than the broker's poll interval
plus its apply window, the consumer doesn't even notice — it just
sees "no new manifest yet" for a few polls and then catches up
when the producer's first post-restart incremental lands.

## Verification post-soak

```sh
# Count check (fast, always run):
sluice verify \
    --source-driver postgres --source ... \
    --target-driver postgres --target ... \
    --depth=count

# Sampled content check (good default):
sluice verify ... --depth=sample
```

The broker preserves byte-equality the same way `sync start` does —
the apply path is shared. A divergence after a clean soak is a real
bug; file it.

## Common pitfalls

- **Cold-start without `--at-chain-id`.** The refusal names the
  recovery path; pass the flag once on first launch and don't
  pass it again on restart.
- **Keyless tables on an older chain.** The broker refuses an
  incremental that records no apply identities when it touches a
  table with no PRIMARY KEY and no NOT NULL UNIQUE index, and any
  incremental that touches a target table keyed only on a surrogate
  the backup's rows do not carry (`SLUICE-E-BROKER-KEYLESS-TABLE`),
  because it cannot re-apply an interrupted transaction into one
  without duplicating rows. There is no table filter on the broker:
  replay a chain this sluice wrote, add the key on the source and
  take a new full, or replicate those tables with `sync start`.
- **Two consumers with the same `--stream-id`.** They'll race on
  position writes and you'll see one make progress and the other
  appear stuck. Use distinct stream-ids for distinct targets.
- **Backup store on the same disk as the source.** Don't do this.
  The broker's whole point is decoupled transport; co-locating the
  store with the source removes the decoupling.
- **High write rate.** As noted in the decision matrix, the broker
  is for moderate volumes. If your source sustains tens of
  thousands of changes/sec, use `sync start` directly.

## What this recipe doesn't cover

- **Encrypted backup chains.** Compose with
  [recipe-backup-encrypted.md](recipe-backup-encrypted.md) — the
  broker accepts `--encrypt` + `--encryption-passphrase` just like
  the rest of the backup family.
- **Cross-engine broker** (PG-source backup chain → MySQL target).
  Supported when the chain doesn't contain PG-specific shapes
  (verbatim extension types, EXCLUDE constraints) the cross-engine
  refusal would block; see the cross-engine refusal docs.
- **Backup chain pruning while the broker is consuming.** Pruning
  via `sluice backup prune` is safe as long as the broker's
  `last_applied_backup_id` is in a segment newer than the pruned
  range. Pruning past the broker's position will cause the broker
  to refuse loudly when its position isn't found in the chain.

## See also

- [recipe-backup-encrypted.md](recipe-backup-encrypted.md) — backup
  chain encryption + verify-path + ingestion-path probes for
  rotated-passphrase detection.
- [recipe-bidirectional-cutover.md](recipe-bidirectional-cutover.md)
  — the direct `sync start` path for source-to-target replication
  when the topology supports it.
- ADR-0046 in [`docs/adr/`](../adr/) — the inline backup chain
  rotation model the broker walks.
- [`docs/backup-format-versioning.md`](../backup-format-versioning.md)
  — the manifest `FormatVersion` contract the broker honors on
  multi-segment chains.
