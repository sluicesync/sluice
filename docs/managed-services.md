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

### Target table ownership — connect as a stable role

On PlanetScale Postgres a *user-defined role* (name `pscale_api_*`,
which *inherits* the `postgres` role) is distinct from the **Default
role** (the actual `postgres` user). Whichever role sluice connects
as **owns every table it creates on the target**, and a `pscale_api_*`
role is ephemeral — so having your migrated tables owned by one is a
latent hazard:

- If that `pscale_api_*` role is later deleted, the tables' owner is
  gone → ownership/permission problems.
- If a *different* `pscale_api_*` role later runs DDL (`ALTER`, etc.)
  against those tables, it hits permission errors — it isn't the
  owner, even though both inherit `postgres`.

**Recommendation: connect the sluice target as the Default `postgres`
role**, so created tables are owned by a stable role. On PlanetScale
you can make the Default role's password available with:

```sh
pscale role reset-default
```

sluice surfaces this rather than auto-handling it (the *contain
Postgres complexity* tenet — it never silently `ALTER … OWNER`s your
tables). When the target connection authenticates as a `pscale_api_*`
role, `migrate` and `sync start` emit a preflight **WARN** naming the
pitfall and the recovery paths below.

**Already ran as a `pscale_api_*` role?** The ownership is recoverable
after the fact — reassign the objects to a stable role with:

```sh
pscale role reassign
```

or use the PlanetScale UI (**Settings → Roles → "Reassign objects"**),
which works even for a `pscale_api_*` role that has since expired.

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

### Coming from ps-discovery?

PlanetScale's `ps-discovery` tool is a metadata census — it inventories what your source contains; sluice is the execution engine that enforces the hazards at run time. If ps-discovery flagged your schema, note what sluice checks automatically before any data moves: declaratively-partitioned tables (loud refusal), old-style `INHERITS` hierarchies (loud refusal — silent-duplication class), FDW foreign tables (loud WARN naming each skipped table and its server), RLS-filtered snapshots, XID-wraparound proximity, and replication-role/slot preconditions.

## PlanetScale MySQL (and other Vitess deployments)

**Status**: Supported via the `planetscale` engine (a flavor of the
MySQL engine that speaks Vitess's VStream gRPC protocol for CDC).
The same flavor handles three deployment shapes:

| Deployment | Transport | Auth | Shard convention |
|------------|-----------|------|------------------|
| PlanetScale (hosted) | TLS (default) | Basic (default; service-token name+value) | auto-discovered (default) |
| Self-hosted Vitess + TLS + auth | TLS | Basic | auto-discovered (or pin `vstream_shards`) |
| Self-hosted Vitess plaintext (vttestserver, dev) | `?vstream_transport=plaintext` | `?vstream_auth=none` | usually `?vstream_shards=0` |

Key constraints inherited from the Vitess platform:

- No `LOAD DATA INFILE` (sluice uses batched inserts instead).
- No direct binlog access — CDC goes through Vitess's VStream gRPC
  protocol. The flavor declares `CDCVStream` so the streamer's
  capability check accepts it.
- No user-defined PARTITION BY (Vitess sharding handles partitioning).
- Sharded keyspaces are supported on both the standalone CDC path
  and the snapshot+CDC handoff path with no extra configuration:
  unless you pin the layout with `vstream_shards`, sluice
  auto-discovers the keyspace's shards at Open time by querying
  `SHOW VITESS_SHARDS` (a single `-` for an unsharded keyspace, the
  real shards for a sharded one). The reader streams from all shards concurrently
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

### Foreign keys on Vitess/PlanetScale targets: `--skip-foreign-keys`

Vitess/PlanetScale keyspaces have limited FK support (sharded
keyspaces reject cross-shard FKs outright; hosted PlanetScale
requires an explicit settings toggle even for unsharded ones), so
migrating an FK-bearing source often fails at the constraint
phase. `--skip-foreign-keys` (on `migrate` and `sync start`)
skips creating FK constraints on the target and instead ensures
each skipped FK's referencing column tuple is indexed — an index
is synthesized only if an existing target index doesn't already
cover those columns as a left-prefix (on a MySQL target this also
preserves the backing index MySQL would otherwise create only
alongside the FK). Engine-agnostic; use it to transition an
FK-bearing source without stripping FKs from it first, or when
FKs are managed out-of-band. Mutually exclusive with
`--allow-degraded-fks` (opposite intents: one skips FK creation,
the other creates FKs and tolerates dirty rows by retrying as
`NOT VALID` — PG-target only).

### FLOAT precision on VStream sources: exact by default (`--strict-float` / `--no-float-exact-reread`)

A VStream cold-start COPY streams over vttablet's rowstreamer, whose
bare-column SELECT renders single-precision `FLOAT` at MySQL's
6-significant-digit text precision (`8388608` → `8388610`). sluice
**repairs** this by default on both paths:

- **`sluice sync start`** re-reads each single-precision `FLOAT` column
  exactly from the source (the `(col * 1E0)` projection) and UPDATEs the
  copied rows by primary key after the copy and before CDC begins.
  Exact, and eventually-consistent (CDC replays from the copy anchor
  forward, so any value that changed during the re-read is re-applied to
  its final value).
- **`sluice backup full`** re-reads the same columns and patches the
  archived rows, so backups store exact float32 by default. The cost is a
  bounded **within-row temporal skew**: the exact `FLOAT` reflects a read
  instant slightly after the snapshot VGTID, so a row whose `FLOAT`
  changed during the read window carries a `FLOAT` newer than the rest of
  its (VGTID-snapshot) columns. This skew is **zero** on a quiescent
  source; it **self-heals** on a chain restore, because the incrementals
  replay from the full's recorded position forward and re-apply every
  post-VGTID change; and it persists only for a **standalone-full
  restore** of a source with concurrent `FLOAT` writes (where a logical
  VStream snapshot is already per-shard-fuzzy, not a global instant).

  The backup re-read is **bounded-memory**: unlike the sync path (which
  streams the re-read cursor-paginated), the backup buffers a per-table
  primary-key → `FLOAT` map to patch rows the VStream COPY delivers out of
  primary-key order (Vitess scans by a cheaper unique key and re-emits
  rows during catchup — a merge-join can't rely on ordering). That map is
  capped at **`--float-reread-max-rows`** (default 2,000,000 rows ≈ a few
  hundred MB worst case; the VStream backup sweep is serial, so only one
  table's map is held at a time). A `FLOAT`-bearing table **larger than
  the cap** falls back loudly — it is **never** buffered unbounded (no
  silent OOM): by default that table is archived rounded with a WARN;
  raise `--float-reread-max-rows` to repair a bigger table exactly.

Escape hatches, on both `sync start` and `backup full`:

- **`--no-float-exact-reread`** keeps the display-rounded `FLOAT` values
  (a rounded-but-**perfectly-consistent** snapshot — every column at one
  instant). Choose this if within-row consistency matters more than
  sub-6-significant-digit `FLOAT` precision.
- **`--strict-float`** (backup) means **exact-or-fail**: it keeps the
  exact archive for every table it can repair, but refuses loudly
  (`SLUICE-E-VSTREAM-FLOAT-LOSSY`, exit 3) for any `FLOAT` column it
  **can't** make exact — a keyless / float-PK-only table (refused upfront)
  or a table over `--float-reread-max-rows` (refused when reached). For
  operators who'd rather fail than store a rounded or skewed value.

`DOUBLE` columns and the CDC/binlog leg are exact and untouched either
way. A **keyless** `FLOAT` table (no primary key to target the re-read)
retains the rounding under the default (WARNed) and is refused under
`--strict-float`. A target-side `--type-override` to `DOUBLE` does **not**
help — the source value is already rounded on the wire.

### Safe migrations: shipping DDL with `sluice deploy-ddl`, and bootstrapping sluice's control tables

A PlanetScale branch with **safe migrations** enabled refuses every direct DDL statement (Error 1105 `direct DDL is disabled`) — including the `CREATE TABLE IF NOT EXISTS` for sluice's own control tables (`sluice_migrate_state`, `sluice_cdc_state`, and siblings). sluice's ensure paths are detect-first (no DDL at all when the tables are current), and when the DDL is genuinely needed the refusal is the coded `SLUICE-E-PS-DIRECT-DDL-BLOCKED`, naming the way through.

That way through is one command per statement ([ADR-0165](adr/adr-0165-deploy-ddl-and-control-table-bootstrap.md)):

```
sluice control-tables ddl        # prints the exact CREATE statements (mysql dialect)
sluice deploy-ddl --org <org> --database <db> --ddl '<one statement>'
```

`deploy-ddl` drives the full governed flow for ONE verbatim statement — dev branch (with the ADR-0162 **stale-base freshness gate**: a fresh PS branch can silently propose *reverting* recent production schema, and sluice's schema comparison + one-shot backup-rebase is the guard), apply the DDL on the branch, deploy request, deploy, skip-revert finalize, cleanup. `--dry-run` prints the plan with zero control-plane calls. It reuses the expand-contract machinery and error codes; safe migrations must be ON (that is the deploy-request prerequisite — without it, direct DDL works and you don't need this command). Ship the five printed control-table statements once, and `backfill` runs normally against the branch; `sync` additionally needs the **user-table** schema pre-created (ship each CREATE via `deploy-ddl` — `sluice schema preview` prints the target DDL) and `--schema-already-applied`, because safe migrations refuses sluice's user-table CREATEs too (the coded `SLUICE-E-PS-DIRECT-DDL-BLOCKED` names this). A fresh `migrate` into a safe-migrations branch cannot skip pre-created tables yet (roadmap item 71b) — for migrate, disable safe migrations for the migration window instead.

It is also the general escape hatch for any ad-hoc schema change on a safe-migrations branch — the safety wrapper (freshness gate, tolerant deploy poller, cleanup) applies to whatever single statement you pass.

### Sharded targets: control tables and `--control-keyspace`

A continuous sync stores three control tables on the target
(`sluice_cdc_state`, `sluice_cdc_schema_history`,
`sluice_shard_consolidation_lease`). A SHARDED Vitess/PlanetScale
target keyspace requires a primary vindex on every table, which
the control tables don't have — so a sync against a sharded
target otherwise dies with `VT09001: table sluice_cdc_state does
not have a primary vindex`. `--control-keyspace` (on `sync
start`, `sync stop`, `sync status`, and `restore`; also the
per-sync `control-keyspace:` key in the `sync run` fleet config,
[ADR-0122](adr/adr-0122-sync-supervisor-command-center.md))
places them in an unsharded sidecar keyspace instead. Usually you
can omit it: against a sharded target sluice auto-detects the
sole unsharded sidecar keyspace and refuses loudly if there are
zero or several. Empty + unsharded/non-Vitess target = unchanged
(bare table names in the default keyspace); inert on non-MySQL
targets.

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
- `vstream_shards=<comma-separated>` — explicit override that pins
  the shard layout and skips discovery. PlanetScale convention is
  `-`; vttestserver typically uses `0` for an unsharded keyspace.
  List every shard for a sharded keyspace (`vstream_shards=-80,80-`).
  Absent, sluice auto-discovers (below).
- `vstream_auto_discover_shards=true` — discovery is now the
  DEFAULT: whenever `vstream_shards` is unset, sluice queries the
  keyspace's shard layout at Open time via `SHOW VITESS_SHARDS`
  (single `-` for unsharded, every shard for sharded). This flag is
  retained for back-compat — it names the default behavior — and is
  mutually exclusive with `vstream_shards`. Discovery-by-default is
  what makes a sharded source cold-copy correctly; a silent `-`
  default made the cold-copy build a keyspace-wide VGTID that vtgate
  rejects, copying zero rows.

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

## Storage auto-grow & primary-reparent resilience (PlanetScale and other Vitess/managed targets)

Managed targets that auto-grow storage — most visibly a non-Metal
PlanetScale instance crossing a storage boundary, but the same shape
applies to any service that reparents a primary during normal operation
— briefly disrupt in-flight writes while the volume grows and a new
primary is promoted. Sluice rides these windows automatically; **no
flags are required**, and the behaviour is bounded and loud on genuine
exhaustion (a target that never recovers still fails rather than hanging
forever). What's covered:

| Phase | Behaviour during a grow/reparent | Reference |
|-------|----------------------------------|-----------|
| Cold-copy **write** | Per-batch flush retries on a fresh connection (the reparented primary) | ADR-0108 |
| Cold-copy **source read** | Reconnect-and-resume from the durable chunk cursor | ADR-0109 |
| Cold-copy coordination | All concurrent copy lanes quiesce together for the grow window, then resume | ADR-0110 |
| Restore (parallel) | Any reparent-touched table is re-derived from its immutable backup chunks so it matches the manifest exactly | ADR-0113 |
| **Post-copy DDL** (index / constraint / view / identity-sequence build) | The phase retries through the reparent instead of aborting after a correct data copy | ADR-0114 (v0.99.118) |

During a grow window you'll see `WARN` lines naming the transient
(e.g. PG `57P01`/`57P03`/`53100`, MySQL/Vitess `1105 "not serving"` /
read-only / disk-full) and the retry; these are expected and
self-clearing. A real, non-transient failure (a genuine type error, an
unrecoverable target) still surfaces loudly and promptly — only the
classified grow/reparent transients are ridden out.

## Control-plane metrics, proactive adaptivity & alerting (PlanetScale)

Sluice can consume PlanetScale's **control-plane metrics** (target CPU,
memory, storage, replication lag, connections) to adapt proactively and
to surface operator alerts. This is opt-in and uses a metrics token
distinct from the database DSN — it reads the PlanetScale metrics API,
not the database.

Enable it on `sync start` (or `diagnose`) with the telemetry flags; the
token is supplied via environment variables, never on the command line:

```bash
export PLANETSCALE_METRICS_TOKEN_ID=...
export PLANETSCALE_METRICS_TOKEN=...
sluice sync start \
  --source-driver planetscale --source "$SOURCE" \
  --target-driver postgres   --target "$TARGET" \
  --planetscale-org acme \
  --planetscale-metrics-db app          # defaults to the target DSN's database \
  --notify-storage-util 0.85 --notify-cpu-util 0.90 \
  --notify-slack "$SLACK_WEBHOOK"
```

- **Proactive adaptivity** — live CPU/memory headroom clamps the
  startup apply-lane count (see `--apply-concurrency` below) and damps
  the apply AIMD high-water during pressure; a storage-headroom signal
  coordinates the cold-copy grow-gate.
- **Threshold alerts** (`--notify-*`) — edge-triggered, cooldown'd
  (`--notify-cooldown`, default 15m), hysteresis-armed alerts to a
  generic webhook (`SLUICE_NOTIFY_WEBHOOK`), Slack
  (`SLUICE_NOTIFY_SLACK` / `--notify-slack`), or email/SMTP
  (`--notify-smtp-host` / `-port` / `-from` / `-to`, TLS mode
  `--notify-smtp-tls` = starttls|implicit|none, auth
  `--notify-smtp-auth` = none|plain|login + `--notify-smtp-username`,
  password via `SLUICE_NOTIFY_SMTP_PASSWORD` only). One SMTP sink covers
  every transactional provider (SendGrid/Mailgun/SES/Postmark) and
  self-hosted relays. Rules: `--notify-storage-util` / `-cpu-util` /
  `-mem-util` (fractions 0–1), `--notify-lag-seconds`, and the
  rate-of-change `--notify-storage-growth-per-min`. Advisory and
  failure-isolated — a dead sink is logged and swallowed; an unobserved
  metric never fires.
- **Metrics history** — when telemetry is configured, sluice persists a
  rolling history (current / 1m / 5m / 10m) to a
  `sluice_target_metrics_history` table on the target (7-day retention),
  surfaced in `sluice diagnose`. Disable with
  `--suppress-target-metrics-history`.
- **Own `/metrics` export** — every sluice `/metrics` endpoint also
  emits `sluice_build_info{version,commit,go_version}`, a Go-runtime
  block, and the `sluice_target_*` gauge family (CPU/mem/storage/lag)
  when telemetry is on.

### Standalone `sluice metrics-watch` daemon

To watch a PlanetScale database's health **without** running a
sync — for dashboards or alert-only operation — `metrics-watch` polls
the control-plane metrics with no database connection attached:

```bash
sluice metrics-watch \
  --engine planetscale --planetscale-org acme --planetscale-metrics-db app \
  --notify-storage-util 0.85 --notify-slack "$SLACK_WEBHOOK" --quiet
```

It prints a live `cpu= mem= storage= lag= conns=` line (suppressed with
`--quiet`), fires the **same** `--notify-*` alerts as `sync start`,
supports `--once` (single sample, for scripts) and `--interval`
(default 60s), and with `--metrics-listen ADDR` becomes a standalone
PlanetScale-metrics Prometheus exporter — needing only the metrics
token, no DB credential.

### Fast-by-default concurrent CDC apply

`--apply-concurrency` controls the key-hash CDC apply lane count and is
**adaptive-by-default** (`0`/unset → an auto value bounded by the
target's measured headroom; engine-general across MySQL and Postgres).
Each lane runs its own AIMD controller and recovers in-lane from a
PlanetScale tx-killer / deadlock (shrink + idempotent split-retry, no
stream restart). Exactly-once is preserved for keyed tables. Pass
`--apply-concurrency 1` to force the legacy serial apply.

## Neon (Postgres)

**Status**: Live-validated 2026-07-15 (v0.99.249) as a migration + CDC **source** (Neon free tier → PlanetScale Postgres). Fidelity was byte-identical on md5 ground truth, including the hard value families: `NaN` inside `numeric[]`, ±Infinity, denormal floats, and 2-D arrays with NULL elements. The snapshot→CDC handoff and post-handoff convergence were clean. The vanilla `postgres` engine is the right driver.

### Direct vs pooler endpoints

Neon gives every branch two hostnames: the **direct** endpoint (`ep-…<id>.<region>.aws.neon.tech`) and the **pooled** endpoint (same name with a `-pooler` suffix on the first label), which is pgbouncer in transaction mode.

- **CDC requires the direct endpoint.** A pooler cannot proxy replication-protocol commands (it strips `replication=database`); `sync start` against the pooled host fails at slot creation with the coded `SLUICE-E-CDC-POOLER-ENDPOINT` refusal.
- **Bulk migrate through the pooler works** — a full snapshot-pinned parallel migrate passed through it at the validation scale — but sluice's parallel copy pins server connections inside long-lived snapshot transactions, which risks pool exhaustion mid-copy at higher parallelism/scale with a confusing failure. sluice emits a preflight **WARN** when the source host matches the `-pooler` pattern; prefer the direct endpoint.

### Enabling logical replication (`wal_level`)

Neon defaults to `wal_level=replica`; sluice's CDC preflight refuses loudly. The fix is **not** postgresql.conf — it's Neon's project setting `enable_logical_replication` (console: Settings → Logical replication; also settable via the project-update API). Two things to know: the toggle is **irreversible**, and it takes effect in seconds with no visible downtime (validated live). See [postgres-source-prep](postgres-source-prep.md) for the provider matrix.

### Operational notes

- **`wal_proposer_slot` is Neon-internal.** Every Neon endpoint carries an always-present *physical* replication slot named `wal_proposer_slot` (part of Neon's safekeeper architecture). sluice's slot-health monitoring correctly ignores it; any external slot-enumeration tooling (or future sluice tooling) must whitelist it rather than flagging it as a leaked consumer.
- **TLS**: Neon DSNs work with `sslmode=require`; `verify-full` also works with the standard system roots.
- **Region co-location matters.** The validation runs were cross-provider; co-locating the sluice process (or the target) with the Neon region measurably reduces snapshot wall-clock.
- **Autosuspend / cold-start is unprobed.** The validation project stayed active throughout, so scale-to-zero resume latency under a sluice snapshot has not been characterized — if you run against an autosuspending endpoint and see slow first-connection behaviour, that's the place to look.

## Supabase (Postgres)

**Status**: Live-validated 2026-07-15 as a bulk-migration **source** — bit-exact fidelity through **both** Supavisor pooler modes. CDC from Supabase was environment-blocked in the validation (IPv4-only network + IPv6-only direct endpoint — see below), not a sluice defect: `wal_level=logical` is on out of the box (contrast Neon).

### The IPv6-only direct endpoint (the thing that bites first)

Supabase free-tier **direct** endpoints (`db.<ref>.supabase.co`) have **only an AAAA record** — IPv4 connectivity to the direct endpoint is a paid add-on. From an IPv4-only machine the connection fails in about a second with the platform resolver's cryptic no-data error. sluice detects this class: on a resolve failure it probes for an AAAA record and, when the host is IPv6-only, extends the error with the remedy (coded `SLUICE-E-CONNECT-IPV6-ONLY`).

- **Bulk migrate**: use the pooler endpoint (`aws-…pooler.supabase.com` — it has an A record).
- **CDC**: the direct endpoint is required (a pooler cannot proxy replication), so from an IPv4-only network you need the IPv4 add-on or an IPv6-capable network. `sync start` through Supavisor fails at slot creation with the coded `SLUICE-E-CDC-POOLER-ENDPOINT` refusal explaining exactly this.

### Session vs transaction pooler modes

Supavisor exposes two ports on the pooler hostname:

- **Session mode (`:5432`)** — bulk migrate works, including parallel copy. Validated bit-exact.
- **Transaction mode (`:6543`)** — server connections rotate per transaction, which trips pgx's statement cache (SQLSTATE 42P05, "prepared statement already exists"). sluice WARNs and falls back to the single-reader copy path, which completes correctly — but **parallel copy is silently unavailable** in this mode. Prefer session mode (or the direct endpoint) for large copies.

sluice's pooler-host preflight WARN fires for both (the hostname matches the `pooler.supabase.com` pattern).

### Float display is not float identity

Supabase servers default `extra_float_digits=0`, so **text-level** float comparisons against a Supabase source lie (a value can print rounded while the stored bits are exact). sluice's copy was proven bit-exact via `float8send` ground truth; if you diff sluice's output with external tooling that compares text, pin `SET extra_float_digits = 1` (or better, compare `float8send`/`float4send` bytes).

### Platform schemas

Supabase ships its platform schemas (`auth`, `storage`, `realtime`, …) alongside `public`. sluice's default `public` scoping ignores them correctly; nothing to exclude manually.

## DigitalOcean Managed MySQL

**Status**: cold copy + CDC handoff validated live 2026-07-15 (throwaway `db-s-1vcpu-1gb`, MySQL 8.4). Uses the vanilla `mysql` engine. One platform behaviour is dangerous enough to headline:

### The lying binlog-retention window (read this before `sync start`)

On DO Managed MySQL **defaults**, an out-of-band platform reaper purges **every binlog file ~13–16 minutes after creation** — while `@@binlog_expire_logs_seconds` reads **259200 (3 days)** and the DO config API shows no retention field until you first set one. The server variable **lies**: no SQL-level check can see the real window, so the DSN host pattern (`*.db.ondigitalocean.com`) is the only reliable preflight signal. sluice emits a loud **WARN** at `sync`/backup start on that host pattern.

Why it matters: a CDC position older than the window is unrecoverable (`ErrPositionInvalid`, "binlog purged"), and a cold copy that takes longer than the window can **livelock auto-resnapshot** — each retry re-copies, exceeds the window again, and loses its position again.

The fix (confirmed working): set the retention knob through DO's database config API —

```
PATCH /v2/databases/{id}/config
{"config": {"binlog_retention_period": 86400}}
```

Seconds, accepted range 600–86400; **86400 (24 h) is the right value for migrations**. It takes effect immediately, no restart, and pre-existing binlogs stop being purged. (Aiven-hosted MySQL likely shares the purger behaviour — same platform lineage — but has not been probed.)

Open question (unprobed): whether an *attached* binlog-dump connection holds the purger back — i.e. whether a live caught-up stream is safe indefinitely at default retention or only between purge ticks. Until answered, treat the config-API knob as required for any DO CDC use.

### Connection + schema gotchas

- **TLS CA is required.** DO clusters use a private CA; fetch it from `GET /v2/databases/{id}/ca` and pass it via `--source-tls-ca` (custom-CA support shipped in ADR-0158).
- **`doadmin` has the replication grants** sluice's binlog CDC needs; no extra GRANTs required on defaults.
- **Default `sql_mode` includes `ANSI`** — double-quoted strings are *identifiers* on this server. Anything you run manually against the source with `"double quotes"` behaves differently than on a stock MySQL.
- **`sql_require_primary_key=true`** by default — keyless tables cannot be created on a DO target, and restoring keyless-table dumps there fails until the setting is relaxed.

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
