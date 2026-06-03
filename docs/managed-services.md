# Managed-service compatibility

This document records what sluice has been verified to work against on
specific managed database services, and the operator-facing
preconditions each service requires. Sluice itself is engine-neutral —
the rules here are properties of the service, not the tool.

For local databases (a vanilla MySQL or Postgres install you run
yourself) sluice's defaults work without configuration. Read on if
you're targeting a managed offering.

## PlanetScale Postgres

**Status**: All sluice paths verified against PlanetScale Postgres
(PS-PG, late 2025 launch). No code changes were required — the
vanilla `postgres` engine handles PS-PG cleanly.

| Path | Verified |
|------|----------|
| Connectivity (pgx, TLS, pgwire) | ✅ |
| Schema reader (`SchemaReader.ReadSchema`) | ✅ |
| Simple-mode migration (`pipeline.Migrator`, PG→PG) | ✅ |
| CDC reader (`CDCReader.StreamChanges` from "now") | ✅ |
| Continuous sync (`pipeline.Streamer`, snapshot+CDC handoff) | ✅ |
| Cross-engine PlanetScale-MySQL → PS-PG (simple-mode) | ✅ |

Verified against PG 18.3 in May 2026. PS-PG advertises standard
Postgres compatibility built on a Vitess-like architecture; in
practice the verification tests passed without flavor-specific code
paths. Sluice does not declare a `FlavorPlanetScalePostgres` — the
vanilla `postgres` engine is the right driver.

### Operator preconditions

Sluice does not configure these for you; they're database-level
settings the PlanetScale operator needs to ensure are in place.

- **`wal_level=logical`** on the source database. Required for the
  CDC reader and continuous-sync streamer; not required for
  simple-mode migrations. PS-PG's defaults already enable it on the
  databases provisioned for sluice's verification, but if you
  provisioned earlier or with custom settings, double-check with:
  ```sql
  SHOW wal_level;
  ```
- **REPLICATION attribute** on the connecting role. Required for
  CDC and continuous sync. Check with:
  ```sql
  SELECT rolreplication FROM pg_roles WHERE rolname = current_user;
  ```
  PlanetScale's default user has it; custom roles may not.
- **`max_wal_senders` ≥ number of concurrent sluice streams**. PS-PG
  defaults are generous (10) — only a concern if you're running many
  streams against the same database.
- **`max_replication_slots` ≥ number of concurrent streams**. Same
  caveat.

Connectivity itself uses standard pgwire over TLS. PS-PG DSNs come
in URI form (`postgresql://…?sslmode=verify-full`) and pgx accepts
them as-is. No vendor-specific connection-string parameters are
required.

### Verification suite

Sluice has automated PlanetScale/Vitess coverage behind a `psverify`
build tag (requires PlanetScale credentials), run on-demand before
releases. Each phase that creates objects on PS-PG drops them at the
end so re-runs are idempotent.

### Operational notes

- **Schema-drop CASCADE on PS-PG can be slow.** During verification,
  the destination schema occasionally took longer than 30 seconds to
  drop after a streamer test, presumably due to PS-PG's
  Vitess-backed proxy doing more work on DDL than vanilla PG. The
  verification helpers cap the wait at 30 seconds and log a notice
  rather than blocking the test. Production migrations don't
  ordinarily drop schemas, so this hasn't surfaced as an operational
  concern — only as test cleanup flakiness.
- **Cancellation propagation can be slow over the proxy.**
  PostgreSQL cancel packets sometimes don't reach the backend
  quickly via PS-PG's proxy. Sluice's tests bound their cleanup
  helpers explicitly to avoid relying on it. In production this
  manifests as a multi-second delay when terminating a long-running
  query; harmless but worth knowing.
- **Replication-slot lifecycle on failed runs.** If a sluice
  streamer crashes between `CREATE_REPLICATION_SLOT` and clean
  shutdown, the slot persists on the source. The next streamer
  start will refuse with `replication slot "sluice_slot" already
  exists`. Drop it manually before retrying:
  ```sql
  SELECT pg_drop_replication_slot('sluice_slot');
  ```
  This is the same behaviour you'd see on vanilla PG; flagged here
  because the situation is more likely on a managed service where
  network blips can interrupt the streamer.

## PlanetScale MySQL (and other Vitess deployments)

**Status**: Supported via the `planetscale` engine (a flavor of the
MySQL engine that speaks Vitess's VStream gRPC protocol for CDC).
The same flavor handles three deployment shapes:

| Deployment | Transport | Auth | Shard convention |
|------------|-----------|------|------------------|
| PlanetScale (hosted) | TLS (default) | Basic (default; service-token name+value) | `-` (default) |
| Self-hosted Vitess + TLS + auth | TLS | Basic | varies |
| Self-hosted Vitess plaintext (vttestserver, dev) | `?vstream_transport=plaintext` | `?vstream_auth=none` | usually `?vstream_shards=0` |

Key constraints inherited from the Vitess platform:

- No `LOAD DATA INFILE` (sluice uses batched inserts instead).
- No direct binlog access — CDC goes through Vitess's VStream gRPC
  protocol. The flavor declares `CDCVStream` so the streamer's
  capability check accepts it.
- No user-defined PARTITION BY (Vitess sharding handles partitioning).
- Sharded keyspaces are supported on both the standalone CDC path
  and the snapshot+CDC handoff path: list the shards explicitly
  via `vstream_shards` or set `vstream_auto_discover_shards=true`
  to have sluice query `SHOW VITESS_SHARDS LIKE '<keyspace>/%'` at
  Open time. The reader streams from all shards concurrently
  (vtgate fans out the COPY phase per shard, then the same gRPC
  stream tails CDC across all shards). Per-scope `COPY_COMPLETED`
  events are progress markers; only the *global* COPY_COMPLETED
  (Keyspace and Shard both empty) marks the snapshot→CDC
  handoff. Reshards mid-stream surface as a typed
  `ShardLayoutChangedError`; on the standalone CDC path callers
  resume via `vstreamCDCReader.Reopen`, on the snapshot path v1
  asks the caller to drop and reopen the snapshot stream from
  scratch (in-place reshard recovery during COPY is a future
  chunk).
- Spatial types not supported in v1 (conservative default; flip the
  capability flag if you've confirmed otherwise).

Use `--source-driver=planetscale` (or the equivalent in
`sluice.yaml`) when targeting any Vitess deployment.

### VStream DSN flags

All optional, all default to PlanetScale-friendly behaviour. Ride
on the standard MySQL DSN as additional `?key=value` parameters:

- `vstream_endpoint=<host:port>` — override the vtgate gRPC
  endpoint. Default is `<sql-host>:443`, matching PlanetScale's
  connect-host convention.
- `vstream_transport={tls|plaintext}` — default `tls`. Plaintext
  opts out of TLS entirely; only sensible for localhost vttestserver
  and dev setups.
- `vstream_insecure_tls=true` — keeps TLS but skips certificate
  verification. Useful for self-signed certs in tests.
- `vstream_auth={basic|none}` — default `basic`. None skips the
  Authorization header entirely; matches vanilla Vitess
  deployments that don't authenticate VStream calls.
- `vstream_shards=<comma-separated>` — default `-`. PlanetScale
  convention; vttestserver typically uses `0` for an unsharded
  keyspace. List every shard for a sharded keyspace
  (`vstream_shards=-80,80-`).
- `vstream_auto_discover_shards=true` — discover the keyspace's
  shard layout at Open time via `SHOW VITESS_SHARDS LIKE
  '<keyspace>/%'`. Default `false`. Mutually exclusive with
  `vstream_shards`. Recommended for sharded sources where the
  shard layout isn't known statically; for unsharded keyspaces
  the explicit-default path (`-`) is cheaper.

Reshards are surfaced as `ShardLayoutChangedError` (carries the
new shard layout). Callers can match it with `errors.Is(err,
ErrShardLayoutChanged)` and call `vstreamCDCReader.Reopen` to
resume from the new layout. The continuous-sync streamer's outer
loop owns this retry policy; the reader does not auto-reopen on
its own.

Auth is HTTP Basic over gRPC metadata. Username/password come from
the standard MySQL DSN's `User:Passwd` fields; for PlanetScale,
those are the service-token name and value.

### Verification

Default VStream coverage runs against a container-based Vitess
deployment (vttestserver) via testcontainers, under the
`integration && vstream` build tag:

```bash
go test -tags='integration vstream' -v -count=1 -timeout=15m \
  -run 'TestVStream_VTTestServer' ./internal/engines/mysql/...
```

The vttestserver image is heavier (~700 MB) than the plain
`mysql:8.0` the default integration suite uses, so the standard
`make test-it` doesn't pull it. The split build tag pattern
mirrors the `postgis` tag for the PostGIS integration test.

Sluice additionally has a `psverify` build tag that exercises a real
PlanetScale endpoint (requires PlanetScale credentials), run
on-demand before releases for vendor-specific coverage beyond what
container Vitess exhibits.

## Other managed services

The following haven't been formally verified but should work on the
basis of vendor compatibility statements. If you migrate against one
of these and hit anything sluice-side, please open an issue.

- **AWS RDS for MySQL / Aurora MySQL** — uses the vanilla `mysql`
  engine.
- **AWS RDS for Postgres / Aurora Postgres** — uses the vanilla
  `postgres` engine. Aurora's "logical replication" needs explicit
  parameter-group settings (`rds.logical_replication=1`).
- **GCP CloudSQL for MySQL / Postgres** — uses the vanilla engines.
  CloudSQL's IAM-based connections require the cloud-sql-proxy
  alongside sluice.
- **Azure Database for MySQL / Postgres** — uses the vanilla engines.

The rule of thumb: anything advertising standard pgwire (Postgres)
or standard MySQL-protocol (MySQL) wire compatibility should work.
Vendor quirks land as separate flavor declarations only when 3+
divergences cluster (the criterion sluice's MySQL flavor pattern
follows; see [`docs/dev/notes/prep-planetscale-postgres.md`](dev/notes/prep-planetscale-postgres.md)
for the rule's first statement).
