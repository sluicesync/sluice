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

`internal/engines/postgres/planetscale_verify_test.go` and
`internal/pipeline/planetscale_verify_test.go` are gated behind the
`psverify` build tag. They consume credentials from env vars
(`SLUICE_POSTGRES_SOURCE`, `SLUICE_POSTGRES_DESTINATION`,
`SLUICE_MYSQL_SOURCE`, `SLUICE_MYSQL_DESTINATION`) and fall back to
a repo-root `PLANETSCALE_CREDENTIALS.env` file for local runs.

Run from a shell:

```bash
go test -tags=psverify -v -count=1 -timeout=10m \
  -run 'TestPSPG' ./internal/engines/postgres/...
go test -tags=psverify -v -count=1 -timeout=15m \
  -run 'TestPSPipeline' ./internal/pipeline/...
```

Each phase that creates objects on PS-PG drops them at the end so
re-runs are idempotent. The streamer test additionally drops the
`sluice_slot` replication slot before and after, in case a previous
failed run left it behind.

In CI, see `.github/workflows/psverify.yml` — manual-trigger only
(workflow_dispatch). The required secrets are listed in the file
header.

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

## PlanetScale MySQL

**Status**: Supported via the `planetscale` engine (a flavor of the
MySQL engine). Documented separately in
[../internal/engines/mysql/flavor.go](../internal/engines/mysql/flavor.go);
key constraints inherited from PlanetScale's Vitess platform are:

- No `LOAD DATA INFILE` (sluice uses batched inserts instead).
- No direct binlog access (sluice's CDC capability is reported as
  `None` for this flavor; continuous sync via VStream is a planned
  future chunk).
- No user-defined PARTITION BY (Vitess sharding handles partitioning).
- Spatial types not supported in v1 (conservative default; flip the
  capability flag if you've confirmed otherwise).

Use `--source-driver=planetscale` (or the equivalent in
`sluice.yaml`) when targeting a PS-MySQL database.

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
