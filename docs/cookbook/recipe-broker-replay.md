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
sluice backup stream run \
    --source-driver postgres \
    --source 'postgres://...source...' \
    --output-dir /var/backups/myapp \
    --rollover-window 10s \
    --retain-rotate-at-chain-length 20 \
    --stream-id myapp-producer
```

Operationally a long-running process — run it under systemd / k8s /
your supervisor of choice. The `--rollover-window` controls how often
the producer commits an incremental (chunks accumulated during the
window get bundled into one manifest); `--retain-rotate-at-chain-length`
controls when the chain rotates into a new segment (useful for
keeping individual segments compact for `backup prune` operations).

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
rotation seam. ADR-0010's idempotent applier handles any brief
re-application of changes that landed between the broker's last
advance and the rotation moment.

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

The broker reads `last_applied_backup_id` from `sluice_cdc_state` on
startup, finds it in the chain, and resumes from the next manifest
forward. No re-application, no skipping. The idempotent applier
(ADR-0010) handles any overlap gracefully.

### Producer crash

```sh
# Restart with --force to take over the prior PID's lease:
sluice backup stream run \
    --source-driver postgres \
    --source 'postgres://...source...' \
    --output-dir /var/backups/myapp \
    --rollover-window 10s \
    --retain-rotate-at-chain-length 20 \
    --stream-id myapp-producer \
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
