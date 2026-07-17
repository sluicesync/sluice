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

`deploy-ddl` drives the full governed flow for ONE verbatim statement — dev branch (with the ADR-0162 **stale-base freshness gate**: a fresh PS branch can silently propose *reverting* recent production schema, and sluice's schema comparison + one-shot backup-rebase is the guard), apply the DDL on the branch, deploy request, deploy, skip-revert finalize, cleanup. `--dry-run` prints the plan with zero control-plane calls. It reuses the expand-contract machinery and error codes; safe migrations must be ON (that is the deploy-request prerequisite — without it, direct DDL works and you don't need this command). Ship the five printed control-table statements once, and `backfill` runs normally against the branch; `sync` and `migrate` additionally need the **user-table** schema pre-created (ship each CREATE via `deploy-ddl` — `sluice schema preview` prints the target DDL), because safe migrations refuses sluice's user-table CREATEs too (the coded `SLUICE-E-PS-DIRECT-DDL-BLOCKED` names this). From there the two commands diverge: `sync` skips its schema-apply phase with `--schema-already-applied`, while a fresh `migrate` needs no flag at all — its pre-create gate (ADR-0166) detects each pre-created table, verifies its column shape matches what the migration would create (names/types/nullability; the deploy-ddl-shipped indexes are fine — they're outside the compare), and skips the refused CREATE with an INFO. A pre-existing table whose shape does NOT match refuses upfront with `SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH` before any data moves.

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

- **CDC requires the direct endpoint.** Neon's pooler strips the `replication=database` startup parameter — the behavior of most transaction/statement-mode poolers, though not a law of nature (some session-mode/modern-pgbouncer setups forward replication; Vultr's managed pools do) — so `sync start` against the pooled host fails at slot creation with the coded `SLUICE-E-CDC-POOLER-ENDPOINT` refusal.
- **Bulk migrate through the pooler works** — a full snapshot-pinned parallel migrate passed through it at the validation scale — but sluice's parallel copy pins server connections inside long-lived snapshot transactions, which risks pool exhaustion mid-copy at higher parallelism/scale with a confusing failure. sluice emits a preflight **WARN** when the source host matches the `-pooler` pattern; prefer the direct endpoint.

### Enabling logical replication (`wal_level`)

Neon defaults to `wal_level=replica`; sluice's CDC preflight refuses loudly. The fix is **not** postgresql.conf — it's Neon's project setting `enable_logical_replication` (console: Settings → Logical replication; also settable via the project-update API). Two things to know: the toggle is **irreversible**, and it takes effect in seconds with no visible downtime (validated live). See [postgres-source-prep](postgres-source-prep.md) for the provider matrix.

### Operational notes

- **`wal_proposer_slot` is Neon-internal.** Every Neon endpoint carries an always-present *physical* replication slot named `wal_proposer_slot` (part of Neon's safekeeper architecture). sluice knows it: the slot-health probe (scoped to sluice's own slot) never touches it, `sluice slot list` labels it platform-internal, and `sluice slot drop` refuses it without `--force`. Any external slot-enumeration tooling must whitelist it the same way rather than flagging it as a leaked consumer.
- **TLS**: Neon DSNs work with `sslmode=require`; `verify-full` also works with the standard system roots.
- **Region co-location matters.** The validation runs were cross-provider; co-locating the sluice process (or the target) with the Neon region measurably reduces snapshot wall-clock.
- **Autosuspend / cold-start is unprobed.** The validation project stayed active throughout, so scale-to-zero resume latency under a sluice snapshot has not been characterized — if you run against an autosuspending endpoint and see slow first-connection behaviour, that's the place to look.

## Supabase (Postgres)

**Status**: Live-validated as a bulk-migration **source** (2026-07-15, bit-exact through **both** Supavisor pooler modes) and — 2026-07-16, over the IPv4 add-on from an IPv4-only host — as a slot-based **CDC source** end-to-end: cold snapshot → CDC handoff → live I/U/D convergence (2-D arrays with exact dims, NaN-in-`numeric[]`, NULL elements) → clean stop → warm resume → slot drop. `wal_level=logical` is on out of the box (contrast Neon); the `postgres` role carries the REPLICATION attribute directly (no RDS-style membership model), so there is nothing to grant.

### The IPv6-only direct endpoint (the thing that bites first)

Supabase free-tier **direct** endpoints (`db.<ref>.supabase.co`) have **only an AAAA record** — IPv4 connectivity to the direct endpoint is a paid add-on. From an IPv4-only machine the connection fails in about a second with the platform resolver's cryptic no-data error. sluice detects this class: on a resolve failure it probes for an AAAA record and, when the host is IPv6-only, extends the error with the remedy (coded `SLUICE-E-CONNECT-IPV6-ONLY`).

- **Bulk migrate**: use the pooler endpoint (`aws-…pooler.supabase.com` — it has an A record; session mode, see below).
- **CDC**: the direct endpoint is required (Supavisor strips the replication parameter), so from an IPv4-only network you need the IPv4 add-on or an IPv6-capable network. `sync start` through Supavisor fails at slot creation with the coded `SLUICE-E-CDC-POOLER-ENDPOINT` refusal explaining exactly this.

### The IPv4 add-on (validated end-to-end)

- **Plan-gated**: the org must be on **Pro or higher** — a payment method alone is not enough (the API refuses with "Project addons cannot be edited on the free tier"), and the plan upgrade itself is dashboard-only.
- **$4/month, prorated hourly** (≈ $0.006/h) — cheap enough to enable for a migration window and disable after.
- Enable/disable via the management API (`PATCH /v1/projects/<ref>/billing/addons` with `addon_type=ipv4`; the DELETE takes the **variant** id `ipv4_default`, not the type). DNS flips in ~10 s each way.
- **Enabling REPLACES the AAAA record with an A record** — the direct endpoint is single-family at any moment, so dual-stack clients get IPv4 while the add-on is on, and the endpoint reverts to IPv6-only ~10 s after disable. Don't disable it under a running stream from an IPv4-only host.

### CDC preflight facts

- `max_replication_slots=10`, `max_wal_senders=10` on a fresh Micro project — plenty for one stream, but a shared budget if other consumers (Realtime, ETL) also use the project. (An earlier probe recorded 5/5; Supabase raised the platform default to 10, so treat the exact number as observed-in-band, not fixed.)
- **`max_slot_wal_keep_size` scales with the COMPUTE tier, not PITR** — 512 MB on Micro, ~2048 MB on Small+. This is Supabase's analogue of the managed-MySQL retention warnings: a detached or badly lagging stream on a busy database has only that much WAL runway before Postgres invalidates the slot (`wal_status='lost'`, re-snapshot required). sluice's ADR-0059 slot-health probe pages at 70/85% of whatever the live bound is; keep detach windows short. Note the lever for a WIDER retention window is a compute-tier bump (~$15/mo Small), **not** the PITR add-on (~$100/mo) — PITR only reaches 2 GB transitively because it *requires* Small compute.
- **PITR is CDC-benign** (unlike Cloud SQL's toggle, which destroys positions): enabling it leaves `wal_level`, `max_wal_senders`, `max_replication_slots`, and the logical-slot LSN untouched, adds no platform slot, and never rewinds the position a slot reads — WAL archiving is already `archive_mode=on` at baseline, so PITR only extends archived-WAL *retention duration*. A sluice CDC stream behaves identically with PITR on or off.
- **Platform slots**: a fresh/idle project has none (the empty `supabase_realtime` *publication* exists, but no slot until Realtime is used). Projects actively using Realtime hold `supabase_realtime*` slots — those are platform-owned; don't drop them and don't count them as leaked consumers.

### Session vs transaction pooler modes

Supavisor exposes two ports on the pooler hostname:

- **Session mode (`:5432`)** — bulk migrate works, including parallel copy. Validated bit-exact.
- **Transaction mode (`:6543`)** — server connections rotate per transaction, which trips pgx's statement cache (SQLSTATE 42P05, "prepared statement already exists"). sluice WARNs and falls back to the single-reader copy path, which completes correctly — but **parallel copy is silently unavailable** in this mode. Prefer session mode (or the direct endpoint) for large copies.

sluice's pooler-host preflight WARN fires for both (the hostname matches the `pooler.supabase.com` pattern).

### TLS

**Plaintext is accepted on the direct endpoint** — Supabase does not force TLS there, so always put `sslmode=require` (or stronger) on the DSN yourself. `require` negotiates TLS 1.3. `verify-full` fails against system roots (private CA: `Supabase Root 2021 CA` with a per-project leaf) and works with the Supabase CA pinned via the DSN: `sslmode=verify-full&sslrootcert=<path>` (download the CA from the dashboard). Note `--source-tls-ca` is the MySQL-side flag — for Postgres the CA rides the DSN's `sslrootcert` parameter, and sluice's refusal says so.

### Read replicas: bulk-migrate source yes, CDC source no

Live-probed 2026-07-17 (PG 17 replica, same region). A Supabase read replica works as a **bulk `sluice migrate` source** with the full consistency story: `pg_export_snapshot()` is legal on PG ≥ 16 standbys, so sluice's parallel snapshot-pinned copy engages **unreduced** on the replica — one shared snapshot across all table/chunk readers, byte-exact (`float8send`-proven), evaluated at the replica's replay position. Use it to offload the copy's read load from the production primary.

**DSN shapes:**

- **Direct endpoint**: `db.<ref>-rr-<region>-<suffix>.supabase.co:5432` — its own hostname, IPv6-only exactly like the primary. The IPv4 add-on covers replicas too (one PATCH gives both endpoints A records), but Supabase bills IPv4 **per database**, so it costs 2× while a replica exists.
- **Pooler**: same host/port as the primary — routing is **by username**: `postgres.<ref>` reaches the primary, `postgres.<ref>-rr-<region>-<suffix>` reaches the replica (session mode `:5432` for sluice).
- Password is identical to the primary (the role catalog replicates physically). Sanity check which end you're on with `SELECT pg_is_in_recovery()` — `t` means replica.

**CDC: point `sync start` (and `backup` CDC chains) at the PRIMARY, never the replica.** CDC manages the sluice publication on the source and `CREATE`/`ALTER PUBLICATION` cannot run on a standby; sluice refuses up front with the coded `SLUICE-E-CDC-STANDBY-SOURCE` steer (pre-fix this surfaced as a raw SQLSTATE 25006 at publication ensure). PG 16+ standbys can technically host logical slots, but creation blocks on the primary's next running-xacts record and Supabase denies the `pg_log_standby_snapshot()` nudge — the primary is the supported CDC source.

**Verifying against a replica: gate on replay lag.** `sluice verify` against a lagging replica compares the target with the replica's *past* — it can false-flag rows the copy correctly took from the primary moments earlier, or false-clean a stale target. Prefer verifying against the endpoint you copied from (self-consistent) and treat the primary as the authoritative sign-off target; if you must verify against a replica, first confirm `pg_stat_replication.replay_lag` on the primary is ≈ 0 (or receive LSN == replay LSN on the replica). Two operator traps: `now() - pg_last_xact_replay_timestamp()` on the replica reads minutes of "lag" on an **idle** primary (it timestamps the last replayed transaction — check `replay_lag` on the primary instead), and a **long** copy from a replica under primary write load can be cancelled by WAL-replay recovery conflicts once it outlives `max_standby_streaming_delay` — sluice fails loudly; re-run or copy from the primary.

### Float display is not float identity

Supabase servers default `extra_float_digits=0`, which makes **text-rendered** floats lie (a value prints rounded while the stored bits are exact). As of v0.99.265 sluice pins shortest-exact float rendering on every session it opens — the raw-copy text lane, the CDC walsender session, the trigger capture function, and `verify --depth=sample` hashes — so the server default no longer affects sluice's own fidelity (earlier releases were affected; re-verify data moved by them, and re-run `sluice trigger setup` on trigger-CDC sources after upgrading). External tooling that diffs text against a Supabase source still needs its own `SET extra_float_digits = 1` (or better, compare `float8send`/`float4send` bytes).

### Platform schemas

Supabase ships its platform schemas (`auth`, `storage`, `realtime`, …) alongside `public`. sluice's default `public` scoping ignores them correctly; nothing to exclude manually.

## Managed MySQL: the binlog-retention story at a glance

The managed-MySQL sections below (DigitalOcean, AWS RDS, Google Cloud SQL, Azure Flexible Server, Vultr — each validated live) share one theme: on managed MySQL, the CDC window a detached or slow-starting stream can survive is a **platform property**, and on most of the platforms the server variable that claims to describe it lies. The five-point comparison, measured 2026-07-15/17:

| | DigitalOcean | AWS RDS | Google Cloud SQL | Azure Flexible | Vultr |
|---|---|---|---|---|---|
| Default effective window | ~13–16 min (out-of-band reaper) | ~5–11 min (post-backup-upload purge) | **1 day** (variable-governed) | **unbounded observed** (no purge in 85 min; variable = 0) | ~10–16 min (out-of-band reaper — same Aiven lineage as DO) |
| Does `@@binlog_expire_logs_seconds` tell the truth? | No (reads 3 d) | No (reads 30 d) | **Yes (reads 1 d)** | **Yes-and-then-some** (reads 0 = never; even a set value purges lazily) | No (reads 3 d — the identical DO value) |
| Where's the real knob | DO config API (`binlog_retention_period`, 600–86400 s) | SQL: `mysql.rds_set_configuration('binlog retention hours', N)`, cap 168 h | gcloud database flag `binlog_expire_logs_seconds`, floor 86400 s, no practical cap | `az … parameter set binlog_expire_logs_seconds` (0–2³², no floor, live ~20 s) | **NO KNOB EXISTS — the window is unconfigurable** |
| Knob change restarts? | No | No | No (but the PITR *toggle* does, twice, and wipes positions) | No | n/a (no knob) |
| Detect signal | host suffix only | host suffix + SQL-visible setting | **no host suffix; `@@version` ends in `-google`** | host suffix + `@@version` ends in `-azure` | host suffix `*.vultrdb.com` only (`@@version_comment` is a bare `Source distribution`) |
| `binlog_row_image` default | FULL | FULL | FULL | **MINIMAL — silent UPDATE loss (Bug 193) until set FULL** | FULL |
| `FLUSH TABLES WITH READ LOCK` | allowed | platform-blocked | allowed | allowed | allowed |
| Defaults CDC-safe? | No | No | **Yes** (for gaps < 24 h) | **No — retention yes, but the row image silently loses UPDATEs** | **No — and unfixable** |

sluice's `sync`/backup preflight advises per platform: unconditional WARNs on the DO and Vultr host patterns (the truth is API-only on DO and nonexistent on Vultr), a detect-then-read probe on RDS hosts (silent when correctly configured), and an in-band `@@version` fingerprint for Cloud SQL (silent at its safe defaults). Azure needs no retention advisory at all — its defaults hold binlogs — but carries the row-image trap instead (see its section). Details in each section.

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

Seconds, accepted range 600–86400; **86400 (24 h) is the right value for migrations**. It takes effect immediately, no restart, and pre-existing binlogs stop being purged. (The purger is an Aiven-platform behaviour, now cross-confirmed: Vultr — the same Aiven-derived platform — exhibits the identical reaper class, minus DO's knob; expect it on any Aiven-lineage managed MySQL, including Aiven proper.)

One earlier open question is now answered (via the Vultr probe of the same platform lineage, 2026-07-17): an *attached*, caught-up binlog-dump connection does **NOT** hold the purger back — files behind a live stream purge on schedule, and a caught-up stream survives only because it sits on the active file. Treat the config-API knob as required for any DO CDC use.

### Connection + schema gotchas

- **TLS CA is required.** DO clusters use a private CA; fetch it from `GET /v2/databases/{id}/ca` and pass it via `--source-tls-ca` (custom-CA support shipped in ADR-0158).
- **`doadmin` has the replication grants** sluice's binlog CDC needs; no extra GRANTs required on defaults.
- **Default `sql_mode` includes `ANSI`** — double-quoted strings are *identifiers* on this server. Anything you run manually against the source with `"double quotes"` behaves differently than on a stock MySQL.
- **`sql_require_primary_key=true`** by default — keyless tables cannot be created on a DO target, and restoring keyless-table dumps there fails until the setting is relaxed.

## AWS RDS for MySQL

**Status**: cold copy + CDC handoff validated live 2026-07-16 (throwaway db.t4g.micro, MySQL 8.4.9). Uses the vanilla `mysql` engine. Aurora MySQL shares the endpoint suffix and the retention procedure below.

### Binlogs purge in ~5-11 minutes on defaults (but the truth is SQL-visible)

With no retention configured, RDS purges each binlog file on a ~5-minute sweep once it has been uploaded by automated backups — observed lifetime **~5-11 minutes per file** — while `@@binlog_expire_logs_seconds` reads 30 days. The variable lies (same class as DigitalOcean), but unlike DO the real setting is visible in SQL: `CALL mysql.rds_show_configuration` → `binlog retention hours`, default NULL ("as soon as possible"). A CDC position older than the window is unrecoverable (`ErrPositionInvalid`), and a cold copy longer than it can livelock auto-resnapshot — RDS's window is *tighter* than DO's.

Before `sync start` / `backup`:

```sql
CALL mysql.rds_set_configuration('binlog retention hours', 24);  -- max 168 (7 days)
```

Effective immediately, no restart. **An attached caught-up stream does NOT hold the purger back** (live-proven: files the stream had already read were purged on schedule while it ran) — set the knob first, and note the 168 h cap means paused streams beyond 7 days are impossible on RDS MySQL. Retained binlogs count against allocated storage; on tiny instances set it back to NULL (or lower) after cutover. Automated backups must be ON (retention ≥ 1 day) or RDS disables binary logging entirely.

sluice checks this for you: on `sync`/`backup` runs against an `*.rds.amazonaws.com` host it queries the retention setting and WARNs when it is NULL or under 24 h — a correctly configured source stays silent (detection beats DO's pattern-only advisory, which is the best DO's API-only setting allows).

### Version + parameter-group gotchas

- **MySQL 8.4 default parameter group is CDC-ready** (`binlog_format=ROW`). **MySQL 8.0 defaults to `binlog_format=MIXED`** — create a custom parameter group with `binlog_format=ROW` (dynamic; reconnect, no reboot).
- `gtid_mode=OFF_PERMISSIVE` by default; sluice's file/pos CDC works as-is.

### Connection + privilege gotchas

- **TLS**: defaults allow plaintext (`require_secure_transport=OFF`). `?tls=true` fails — the RDS CA is not in system roots; pass the public regional bundle via `--source-tls-ca` (`https://truststore.pki.rds.amazonaws.com/<region>/<region>-bundle.pem`). No API call needed (contrast DO, whose CA comes from an authenticated API endpoint).
- **`FLUSH TABLES WITH READ LOCK` is blocked by the platform even though the master user holds RELOAD** — sluice falls back to serial cold copy and a no-freeze snapshot (WARNs; the messages name the RDS reality on RDS hosts). No grant fixes it; quiesce writers during the snapshot if exactness of the handoff position matters, or accept the WARN on idle sources.
- The master user has the replication grants CDC needs out of the box.

## AWS RDS for Postgres

**Status**: Live-validated 2026-07-16 (v0.99.261 validation run, RDS for PostgreSQL 16.14) as a **bulk-migration source** — byte-identical on md5 ground truth including NaN-in-`numeric[]`, ±Infinity, denormal floats, and 2-D arrays with NULL elements. Trigger-CDC (`postgres-trigger`) was validated end-to-end in the same run (that run also surfaced a provider-independent trigger-engine defect — array-column CDC payloads crash-looping the apply — fixed since, with the full array-family matrix pinned). Slot-based CDC was blocked in that run by a sluice-side false refusal — the replication-capability preflight only understood `rolsuper OR rolreplication`, while RDS grants slot creation via `rds_replication` role *membership* (the platform itself was proven slot-capable); the preflight has since been taught the membership model, so slot CDC is expected to work with the master user. Aurora Postgres uses the same role model.

### Enabling logical replication

Not postgresql.conf and not a console toggle: attach a **custom parameter group** with `rds.logical_replication = 1` (static → **reboot required**, ~2 min). The GUC `wal_level` itself is read-only on RDS. Two gotchas: (1) with `backup-retention-period 0` — the cheapest possible instance shape — the instance runs `wal_level=minimal`, not the `replica` most docs assume as the RDS baseline, so a cost-minimized instance is two steps from CDC-ready, not one; the parameter flip forces `logical` either way (the remedy is the parameter group + reboot, **not** "enable backups"); (2) after the flip RDS provisions `max_replication_slots=20` / `max_wal_senders=35` automatically.

### Roles

The master user is **not** a superuser and never has the REPLICATION attribute (`rolreplication=f`); it is a member of `rds_superuser` and `rds_replication`, which is what actually gates slot creation — sluice's replication preflight recognizes that membership. `ALTER ROLE ... REPLICATION` is not available on RDS; for a custom role, `GRANT rds_replication TO <role>;` is the equivalent. Nothing to grant on defaults — the master user has everything sluice needs, including `CREATE EVENT TRIGGER` (via `rds_superuser`) for `sluice trigger setup`.

### TLS

`rds.force_ssl=1` is the default on PG 15+ engines — plaintext connections are refused at pg_hba (`no pg_hba.conf entry ... no encryption`). `sslmode=require` works out of the box; `verify-full` needs the AWS RDS trust bundle (`https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem`) via `?sslrootcert=<path>`.

### RDS Proxy

Untested. Expected CDC-incompatible (a transaction-mode pooler, expected to strip the replication parameter — the Neon/Supavisor-class behaviour; some session-mode/modern-pgbouncer setups forward replication, but RDS Proxy is not known to be one). Connect sluice to the **instance** endpoint (`*.<id>.<region>.rds.amazonaws.com`), not a proxy endpoint (`*.proxy-*.rds.amazonaws.com`).

## Google Cloud SQL for MySQL

**Status**: cold copy + CDC handoff + 35-min-detach warm resume validated live 2026-07-16 (throwaway db-f1-micro Enterprise, MySQL 8.0.45). Uses the vanilla `mysql` engine, connecting to the instance's public IP directly (no proxy required — see the connection notes below).

### Retention: truthful and safe by default (1-day floor)

Unlike DigitalOcean and RDS, Cloud SQL's `@@binlog_expire_logs_seconds` (default **86400 = 1 day**) is the real on-disk retention and no out-of-band reaper purges ahead of it — a stream detached for well under a day warm-resumes on pure defaults. The platform refuses to set it below 86400 (allowed: 0 = never, or 1–49,710 days), so the CDC window can't be misconfigured short. To stretch it past a day: `gcloud sql instances patch INSTANCE --database-flags=binlog_expire_logs_seconds=604800` — applied live, no restart; `--database-flags` replaces the whole flag set, so include existing flags. **`--retained-transaction-log-days` is NOT this knob**: it governs the PITR log copies in Cloud Storage (Enterprise caps it at 7 days), which the replication protocol can't read. Binlogs count against disk; watch storage if you raise the flag with auto-grow off.

sluice fingerprints Cloud SQL in-band on `sync`/backup runs (there is no hostname to pattern-match — the reliable signal is `@@version` ending in `-google`, which also works through the auth proxy) and reads the retention variable, which is honest here; at the platform's safe defaults it stays silent.

### The PITR toggle destroys positions (both directions)

`--no-enable-bin-log` restarts the instance (~10 min operation), sets `log_bin=0` and deletes every binlog (a running stream dies loudly with `ERROR 1236: Binary log is not open`); re-enabling restarts it again and **resets numbering to `mysql-bin.000001`**, so positions persisted before the toggle are permanently invalid — expect sluice's loud WARN + auto-resnapshot (sluice's position-loss error names this Cloud SQL failure mode when the source fingerprints as Cloud SQL). Binary logging also requires automated backups: disabling backups with binlog on is refused (HTTP 400), and creating with `--enable-bin-log` implies backups.

### Connection + privilege gotchas

- **No hostname**: Cloud SQL is bare-IP (or the auth-proxy at localhost) — nothing for host-pattern advisories to match; the reliable fingerprint is `@@version` ending in `-google`.
- **TLS**: defaults accept plaintext (`ALLOW_UNENCRYPTED_AND_ENCRYPTED`). `?tls=true` fails — per-instance private CA; fetch it with `gcloud sql instances describe INSTANCE --format='value(serverCaCert.cert)'` and pass `--source-tls-ca` (same recipe as DO; RDS's public-URL bundle has no analog here).
- **`FLUSH TABLES WITH READ LOCK` works** — root has effective `RELOAD`/`FLUSH_TABLES`, so sluice's concurrent frozen-snapshot cold copy runs with no fallback WARNs (the RDS platform-block does not apply).
- Replication grants present on root out of the box; `binlog_format=ROW` + `binlog_row_image=FULL` + `gtid_mode=ON` by default even on 8.0 (no parameter-group dance); `sql_require_primary_key=OFF`; stock-strict `sql_mode` (no DO-style ANSI surprise).
- `SET GLOBAL` / `PURGE BINARY LOGS` are denied (no SUPER) — all retention/config changes go through gcloud/API database flags.

## Google Cloud SQL for PostgreSQL

**Status**: Live-validated 2026-07-16 (v0.99.263, Cloud SQL PG 16.14) as a migration + slot-based CDC **source** — byte-identical bulk migrate on md5 ground truth (NaN-in-`numeric[]`, ±Infinity, denormal floats, 2-D arrays with NULL elements) and exact CDC convergence with a clean snapshot→CDC handoff. The vanilla `postgres` engine is the right driver, connecting to the instance's public IP directly (authorized-network allowlist; no proxy required).

### Enabling logical replication

Two steps, both self-service, no ticket:

1. Database flag **`cloudsql.logical_decoding = on`** — `gcloud sql instances patch <instance> --database-flags=cloudsql.logical_decoding=on`. The patch performs the required restart inline (~1 min end-to-end; validated live). Careful: `--database-flags` replaces the entire flag set — include any existing flags.
2. Grant the connecting role the REPLICATION attribute: **`ALTER ROLE <role> WITH REPLICATION;`** — this WORKS on Cloud SQL as the (non-superuser) default `postgres` user: Cloud SQL patches the grant for `cloudsqlsuperuser` members. This is exactly recovery path (a) in sluice's replication-capability refusal, and the refusal names Cloud SQL for that reason. (Membership in `cloudsqlreplica`/`cloudsqllogical` does NOT confer slot creation — those are platform-internal; the attribute is the mechanism, unlike RDS's `rds_replication` membership model.)

Baseline before the flip: `wal_level=replica` regardless of backup settings (no RDS-style retention-0 ⇒ `minimal` trap), `max_replication_slots=10` / `max_wal_senders=10` (unchanged by the flip).

### TLS

**The default accepts plaintext** (`sslMode=ALLOW_UNENCRYPTED_AND_ENCRYPTED`) — always put `sslmode=require` (or stronger) on Cloud SQL DSNs yourself; nothing server-side will refuse a plaintext connection unless the instance was created/patched to `ENCRYPTED_ONLY`. `verify-ca` works with the per-instance CA (`gcloud sql instances describe <instance> --format='value(serverCaCert.cert)'`) via `?sslrootcert=`. `verify-full` does NOT work on default instances (the cert names a `.sql.goog` DNS name the default CA mode doesn't publish; use `--server-ca-mode=GOOGLE_MANAGED_CAS_CA` at create time for a resolvable name — unvalidated).

### Cloud SQL Auth Proxy

Untested. It is an authenticating TCP tunnel, not a transaction pooler, so CDC may traverse it (unlike the Supavisor class) — unvalidated; connect sluice to the instance IP directly. Cloud SQL's *Managed Connection Pooling* feature IS a pooler and is expected to strip replication like the Supavisor class (also untested).

### Decommissioning

A cleanly stopped sluice stream leaves its (resumable) replication slot in place; when done for good, `sluice slot drop --yes <slot>` — an abandoned slot retains WAL and will eventually fill the instance disk.

## Azure Database for MySQL (Flexible Server)

**Status**: cold copy + CDC handoff + 35-min-detach warm resume validated live 2026-07-17 (throwaway Standard_B1ms, MySQL 8.0.45). Uses the vanilla `mysql` engine. **One required knob — see the row-image warning below.**

### REQUIRED: set `binlog_row_image=FULL` before any sync

Azure's platform default is `binlog_row_image=MINIMAL` — the only major managed-MySQL platform that defaults to it. Under MINIMAL a binlog UPDATE carries only the changed columns, which would **lose UPDATEs silently** (Bug 193; INSERT/DELETE are unaffected, counts stay equal, only content diverges). Since v0.99.266 sluice's CDC preflight **refuses a non-FULL row image at stream start** with the coded `SLUICE-E-CDC-ROW-IMAGE-PARTIAL` (and re-checks on resume), so a fresh `sync start` under MINIMAL fails loudly, pre-data, rather than corrupting silently. Set the knob to FULL regardless — and if a stream already ran under MINIMAL before you set it, re-verify with full-table sampling (see below). Before `sync start`:

```
az mysql flexible-server parameter set --resource-group <rg> --server-name <server> \
  --name binlog_row_image --value FULL
```

Dynamic — applies in ~20 s with no restart. Verify with `SELECT @@binlog_row_image;`. If a stream already ran under MINIMAL, verify with full-table sampling (`--sample-rows-per-table` sized to the table), not the default sample depth — a 0.2% divergence sails under the default sampling design point (demonstrated live).

### Retention: the safest defaults of any managed MySQL probed

`binlog_expire_logs_seconds` defaults to **0 (no time-based expiry)** and — unlike DigitalOcean, Vultr, and RDS — no out-of-band reaper was observed: files survived 85+ minutes, multiple rotations, and an on-demand full backup without a single purge. Detached streams warm-resume after long gaps on pure defaults (a 35-minute detach — fatal on RDS/DO defaults — replayed its backlog exactly). The inverse concern applies: binlogs accrue against your storage until you set the knob (`--name binlog_expire_logs_seconds --value 604800`, live, no restart; note purge appears platform-scheduled and lazy — files can outlive the window by tens of minutes). Manual `PURGE BINARY LOGS` is denied (no SUPER/BINLOG_ADMIN).

### Connection + privilege notes

- **TLS is mandatory AND zero-setup**: plaintext is refused (`require_secure_transport=ON` — the only one of the probed managed-MySQL platforms that refuses it), and `?tls=true` just works — the server chain validates against public roots already in your system store. No CA download, no `--source-tls-ca`.
- **FTWRL works** (RELOAD honored) — sluice's concurrent frozen-snapshot cold copy runs with no fallback WARNs.
- Replication grants (`REPLICATION SLAVE`/`REPLICATION CLIENT`) present on the admin user out of the box; `binlog_format=ROW` is read-only at the platform (no MIXED trap); `gtid_mode=OFF` by default (file/pos CDC fine); `sql_require_primary_key=OFF`; stock-strict `sql_mode` (no DO-style ANSI surprise).
- Host pattern `*.mysql.database.azure.com`; in-band fingerprint `@@version` ending `-azure`. One-time subscription step: `az provider register --namespace Microsoft.DBforMySQL` must have completed before instance creation works.

## Azure Database for PostgreSQL (Flexible Server)

**Status**: Live-validated 2026-07-17 (Flexible Server PG 16.14) as a migration + slot-based CDC **source** — byte-identical bulk migrate on md5 ground truth (NaN-in-`numeric[]`, ±Infinity, denormal floats, 2-D arrays with NULL elements) and exact CDC convergence with a clean snapshot→CDC handoff. The vanilla `postgres` engine is the right driver.

### Enabling logical replication

Three self-service steps, no ticket:

1. `az postgres flexible-server parameter set --resource-group <rg> --server-name <server> --name wal_level --value logical` — Azure exposes the GUC directly (no provider-specific alias). It is static: the command returns with the change pending.
2. `az postgres flexible-server restart --resource-group <rg> --name <server>` — the parameter does NOT take effect until this explicit restart (~1 min; contrast Cloud SQL, whose patch restarts for you). Verify with `SHOW wal_level;`.
3. Grant the connecting role the REPLICATION attribute: `ALTER ROLE <role> WITH REPLICATION;` — this WORKS as the (non-superuser) admin user; Azure patches the grant for `azure_pg_admin` members. This is exactly recovery path (a) in sluice's replication-capability refusal, the same self-service model as Cloud SQL. (The platform `replication` role is grant-restricted and irrelevant; there is no RDS-style membership model.)

Baseline before the flip: `wal_level=replica` regardless of backup settings (no RDS-style retention-0 ⇒ `minimal` trap), `max_replication_slots=10` / `max_wal_senders=10` (unchanged by the flip), zero platform slots.

### TLS

TLS is mandatory (plaintext refused at pg_hba) and the certificate chain is **public** (Microsoft/DigiCert roots): `sslmode=verify-full` works with no CA download and no `sslrootcert` — use it on every Azure DSN, it is both the strictest and the zero-config mode. (If a client stack with its own CA file fails verification, it's missing an OS trust store, not an Azure quirk.)

### Pooler

The built-in PgBouncer requires General Purpose or higher (port 6432 on the same hostname when enabled; it cannot be enabled at all on Burstable) and is expected to strip replication like the Supavisor class — untested; connect sluice to port 5432.

### Provisioning friction

`Microsoft.DBforPostgreSQL` provider registration is a one-time subscription step, and region availability differs per subscription even from the MySQL flexible service (a region that provisions MySQL can refuse PG with "The location is restricted" — plan a region fallback).

### Decommissioning

A cleanly stopped sluice stream leaves its (resumable) replication slot in place; when done for good, `sluice slot drop --yes <slot>` — an abandoned slot retains WAL and will eventually fill the instance disk.

## Vultr Managed Databases for MySQL

**Status**: cold copy + CDC handoff validated live 2026-07-17 (throwaway hobbyist single-node, MySQL 8.4.8). Uses the vanilla `mysql` engine. Vultr's DBaaS is the same Aiven-derived platform as DigitalOcean's, and it shares DO's headline hazard — without DO's fix:

### The lying binlog window, with NO retention knob (read this before `sync start`)

An out-of-band platform reaper purges **every binlog file ~10–16 minutes after creation** — while `@@binlog_expire_logs_seconds` reads **259200 (3 days)**. The variable lies exactly as it does on DigitalOcean (same platform lineage), but where DO's config API accepts `binlog_retention_period`, **Vultr exposes no retention control at all**: the advanced-options API rejects the option by name, the database-update API ignores it, and `SET GLOBAL`/`SET PERSIST`/`PURGE BINARY LOGS` are denied to `vultradmin`. There is nothing to configure — the ~10-minute floor is permanent. sluice emits a loud **WARN** at `sync`/backup start on the `*.vultrdb.com` host pattern (the only reliable signal — `@@version_comment` is a bare `Source distribution`).

What that means in practice: a CDC position older than ~10 minutes is unrecoverable (`ErrPositionInvalid`, auto-resnapshot), an attached caught-up stream is safe **only while it stays caught up** (files behind a live stream purge on schedule; the active file alone is immune — live-demonstrated), and a cold copy or restart gap longer than ~10 minutes can livelock auto-resnapshot **with no remedy**. Treat Vultr MySQL as a migrate-and-cut-over source: keep the sync stream attached and caught up from snapshot to cutover, and keep any planned pause well under 10 minutes. For long-running or pausable replication, this platform's defaults cannot support it.

### Connection + schema gotchas (the DO list, almost verbatim)

- Host pattern `*.vultrdb.com`, nonstandard high port; **plaintext is accepted** (`require_secure_transport=OFF`) but the unencrypted-binlog WARN applies. `?tls=true` fails — per-cluster private CA (Aiven "Project CA"); it is embedded in the create/GET API response's `ca_certificate` field — save it and pass `--source-tls-ca` (no separate CA endpoint call needed, unlike DO).
- **`vultradmin` has the replication grants** CDC needs, plus RELOAD — FTWRL works, so sluice runs the concurrent frozen-snapshot cold copy with no fallback WARNs.
- **Default `sql_mode` includes `ANSI`** — double-quoted strings are *identifiers*. Manual SQL against the source behaves differently than stock MySQL.
- **`sql_require_primary_key=ON`** — keyless tables cannot be created on a Vultr target.
- `local_infile=OFF` (Vultr-as-target takes the batched-INSERT fallback); `max_binlog_size` lowered to 64 MB; MySQL **8.4 is the only offered version**.

## Vultr Managed Databases for PostgreSQL

**Status**: Live-validated 2026-07-16 (Vultr PG 17.10) as a migration + slot-based CDC **source** — byte-identical bulk migrate on md5 ground truth (NaN-in-`numeric[]`, ±Infinity, denormal floats, 2-D arrays with NULL elements) and exact CDC convergence with a clean snapshot→CDC handoff. The vanilla `postgres` engine is the right driver.

### Enabling logical replication: nothing to do

Vultr (an Aiven-lineage platform) ships CDC-ready: `wal_level=logical` out of the box and the master user (`vultradmin`) carries the REPLICATION attribute from first boot — `sync start` works with zero preparation, the only provider validated so far where that is true. `max_replication_slots`/`max_wal_senders` default 20/20 and are raisable to 64 via the database's advanced options. For custom roles, `ALTER ROLE <role> WITH REPLICATION` works as `vultradmin` (no superuser needed — the platform patches the grant, like Cloud SQL and Azure).

### The `pghoard_local` slot is platform-internal

Every Vultr PG instance carries an always-active *physical* replication slot named `pghoard_local` (Aiven's pghoard backup daemon). `sluice slot list` shows it labeled platform-internal, `sluice slot drop` refuses it without `--force`, and the slot-health probe (scoped to sluice's own slot) never flags it. Leave it alone — never drop it, and don't count it as a leaked consumer. (It is the Aiven-lineage sibling of Neon's `wal_proposer_slot`, and likely present on Aiven proper and DO Managed PG too — unprobed.)

### TLS

Plaintext is refused server-side; `sslmode=require` works out of the box, and `sslmode=verify-full` works with the project CA, which is delivered inline in the `database create`/`get` API response (`ca_certificate` field) — pass it via `?sslrootcert=<path>` on the DSN. System roots do not verify (private per-project CA).

### Connection pooler: CDC actually traverses it

Vultr's managed pgbouncer pools listen on the **primary hostname at port + 1** with `dbname=<poolname>` (the API does not expose this — it is the platform convention). Unlike the Neon/Supavisor/RDS-Proxy pooler class, **replication connections pass through** (modern pgbouncer ≥ 1.24 forwards them 1:1 to the server): both bulk migrate (parallel, snapshot-pinned, no statement-cache trip) and slot-based CDC — slot creation, streaming, warm resume, clean stop — were validated end-to-end through a transaction-mode pool. This is the live counter-example that makes the pooler-strip claim provider-dependent. Prefer the direct port anyway: a pool sized N permanently holds N of the plan's small connection budget (22 on the cheapest plan). Note sluice's pooler-host WARN does not fire here (the pool hostname equals the primary's; only the port differs), which on Vultr is harmless — but it also means "no WARN" is not evidence of "not a pooler" on this provider.

### Decommissioning

A cleanly stopped sluice stream leaves its (resumable) replication slot in place; when done for good, `sluice slot drop --yes <slot>` — an abandoned slot retains WAL against the instance disk. (`pghoard_local` stays; see above.)

## Other managed services

The following haven't been formally verified but should work on the
basis of vendor compatibility statements. If you migrate against one
of these and hit anything sluice-side, please open an issue.

- **Aurora MySQL** — uses the vanilla `mysql` engine. Shares RDS for
  MySQL's endpoint suffix and `mysql.rds_set_configuration` retention
  procedure; see the validated RDS MySQL section above.
- **Aurora Postgres** — uses the vanilla `postgres` engine. Shares
  RDS for Postgres's role model and parameter-group settings
  (`rds.logical_replication=1`); see the validated RDS section above.
- **Aiven (MySQL / Postgres)** — uses the vanilla engines. DO and
  Vultr are Aiven-derived, so expect the same shapes: on MySQL the
  out-of-band binlog reaper (Aiven exposes the `binlog_retention_period`
  config knob DO surfaces), on Postgres the `pghoard_local` platform
  slot and CDC-ready defaults. Unprobed.

The rule of thumb: anything advertising standard pgwire (Postgres)
or standard MySQL-protocol (MySQL) wire compatibility should work.
Vendor quirks land as separate flavor declarations only when 3+
divergences cluster (the criterion sluice's MySQL flavor pattern
follows; see [`docs/dev/notes/prep-planetscale-postgres.md`](dev/notes/prep-planetscale-postgres.md)
for the rule's first statement).
