# Production readiness

This page is the one-stop answer to "is sluice ready for my workload?" — the support matrix, the CDC modes per source, the honest list of what sluice does *not* do today, and a pre-production checklist. It is maintained against the code, not aspirations; every limitation below names its trigger condition and its workaround. Status as of v0.124.0.

The posture behind this page: sluice has **no known production users today**, and treats that as the reason to gate on correctness rather than a caveat to bury — the project's tenet is that the first migration that silently corrupts data ends its credibility permanently. What "battle-tested" means here is therefore specific and checkable: every silent-loss class ever caught has a permanent class-pin test (per-family × per-shape matrices ground-truthed on real servers, not representative pins); six required CI checks run real-database integration suites — including cross-engine migrate + sync + backup — on every PR; repeated full blind multi-agent audits of the codebase (three in July 2026 alone) each ran to remediated closure; the PlanetScale filtered-sync path was validated end-to-end against real PlanetScale at 5M rows; and a multi-week soak fleet runs continuous syncs against live managed services between releases. What it does *not* mean: an install base, or a stability guarantee — v0.x minor releases may still include opt-in behavior changes ([README § Project state](../README.md#project-state)).

## Supported engines and directions

Fourteen engines are registered (`sluice engines` lists them): `mysql`, `mariadb`, `planetscale`, `vitess`, `postgres`, `sqlite`, `d1`, `csv`, `tsv`, `ndjson`, `mydumper`, `postgres-trigger`, `sqlite-trigger`, `d1-trigger`.

### Live databases — migrate and continuous sync

| Source ↘ Target → | MySQL | MariaDB | PostgreSQL | PlanetScale MySQL | PlanetScale PG |
|---|---|---|---|---|---|
| **MySQL** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **MariaDB** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **PostgreSQL** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **PlanetScale MySQL** | ✓ (VStream CDC) | ✓ (VStream CDC) | ✓ (VStream CDC) | ✓ | ✓ |
| **PlanetScale PG** | ✓ | ✓ | ✓ | ✓ | ✓ |

Every cell supports one-shot `migrate`; every cell whose source has a CDC mode (next section) also supports continuous `sync`. MySQL flavors (vanilla, MariaDB, PlanetScale, Vitess) share one engine implementation with per-flavor `Capabilities` declarations; MariaDB is first-class since v0.99.268 (native `uuid`/`inet6`/`inet4` carry, domain-GTID CDC, JSON-via-`json_valid`).

A PlanetScale MySQL host needs a VStream driver. `migrate` and `sync` **refuse** such a host under `--source-driver mysql` or `--source-driver mariadb` (and the `--target-driver` equivalents) with `SLUICE-E-DRIVER-HOST-MISMATCH`, from the DSN string alone and before any connection: those flavors drive binlog CDC and `LOAD DATA` cold-copy, both of which Vitess blocks, so the run would otherwise fail obscurely partway through. Use `--source-driver planetscale` — or `vitess` for a self-hosted Vitess — which gets VStream CDC with the Vitess `_vt_*` shadow tables auto-excluded. That preflight runs on `migrate` and `sync` only; the schema-read and backup surfaces (`schema diff`, `schema preview`, `backup`) do not consult it, and a non-VStream driver still gets the `_vt_*` auto-exclusion there.

<!-- psdb-mysql-host-suffixes: *.connect.psdb.cloud, *.private-connect.psdb.cloud -->
<!-- psdb-mysql-host-drivers: mariadb=refused, mysql=refused, planetscale=ok, vitess=ok -->

The two markers above are derived, not maintained by hand: `TestPSDBMySQLHostDriverClaimMatchesTheCode` reads the endpoint suffixes out of the engine source and then asks every registered driver what it actually does with a host under each one, failing the build when this page, the README, or the error-code reference disagrees. This page told operators the opposite — that such a host "also works under `--source-driver mysql`" — for every release from v0.100.0 on, because the same sentence lives in three places and each of the two previous corrections fixed one of them. The gate is the difference between correcting it a third time and it staying correct.

Cross-engine type translation covers the common surfaces (PG `UUID`/`INET`/`MACADDR`/`ARRAY` ↔ MySQL equivalents, `TINYINT(1)` ↔ `BOOLEAN`, `ENUM`/`SET`, PostGIS geometry with SRID, generated-column and `CHECK` idioms — see [translator-catalog](translator-catalog.md)); anything it cannot translate refuses loudly, with `--type-override` and `--expr-override` as the per-column escape hatches.

### SQLite & Cloudflare D1

| Engine name | Role | Notes |
|---|---|---|
| `sqlite` | **source** (file or `.sql` dump) **and target** | Pure-Go, no CGO. Imports a binary `.db` or a `wrangler d1 export` dump into Postgres / MySQL; as a target emits a `.db` from any source (decimals byte-exact as TEXT). Migrate only. |
| `d1` | **source** (live, lossless) | Reads a live Cloudflare D1 over its HTTP query API; integers above 2⁵³ and BLOBs round-trip exactly ([ADR-0132](adr/adr-0132-d1-query-api-reader.md)). |
| `sqlite-trigger` | **CDC source** | Trigger-based continuous sync from a local SQLite file ([ADR-0135](adr/adr-0135-sqlite-trigger-cdc.md)). |
| `d1-trigger` | **CDC source** | The same trigger-CDC design over a live D1's HTTP query API ([ADR-0136](adr/adr-0136-d1-trigger-cdc.md)). |

D1 is not a target (emit a SQLite `.db` and `wrangler d1 import` it). Operator guide: [sqlite-d1-import](operator/sqlite-d1-import.md).

### Flat-file sources (migrate only — a file doesn't change)

| Engine name | Role | Notes |
|---|---|---|
| `csv` / `tsv` | **source** | Staged with validated type inference; NULL/header conventions declared, never sniffed ([ADR-0163](adr/adr-0163-flatfile-csv-tsv-ndjson-sources.md)). |
| `ndjson` | **source** | Numbers carried as raw text end-to-end — no float64 transit, big integers land exact. |
| `mydumper` | **source** | A mydumper or `pscale database dump` directory; recorded row counts cross-checked after every table read ([ADR-0161](adr/adr-0161-mydumper-source-engine.md)). |

All four migrate into any target; operator guide: [flat-file-sources](operator/flat-file-sources.md).

### Which engines can be a target

Most of the fourteen are source-only. These are the engine names `--target-driver` accepts for a `migrate` or a `sync` cold-start copy:

<!-- target-engines: mariadb, mysql, planetscale, postgres, postgres-trigger, sqlite, vitess -->

`mariadb`, `mysql`, `planetscale`, `vitess`, `postgres`, `postgres-trigger`, `sqlite`. Every other registered engine — `d1`, `d1-trigger`, `sqlite-trigger`, `csv`, `tsv`, `ndjson`, `mydumper` — refuses `OpenSchemaWriter`/`OpenRowWriter` and cannot be selected as a target at all.

The marker above is not decoration: `TestTargetEngineListMatchesTheCode` derives the set twice — once from the shape of each engine's writer doors in the source, once by actually calling those doors and reading the error — and fails the build if the two disagree with each other or with this list. It exists because sluice published "fires on SQLite and D1 targets" across two releases, for a D1 target that has never existed. If you are writing release notes about a change to a target-side path, take the engine list from here rather than from the shape of the fix.

### Adjacent surfaces

Encrypted logical backups with restore: `backup full`, restore, and point-in-time chain replay work on every engine that migrates — the restore side reads the stored chain and never touches the source. **Creating** the incremental side of a chain needs a **CDC-capable source**: `backup incremental` and the continuous `backup stream` producer capture the changes since the parent, and capturing changes is what CDC is, so both refuse a CDC-less source at validate. That is the engine set below (the CDC table's rows with a mechanism). `sluice backup export-as-parquet` transcodes any backup chain into Parquet for DuckDB / warehouse ingestion ([ADR-0164](adr/adr-0164-backup-export-as-parquet.md)).

<!-- incremental-backup-engines: d1-trigger, mariadb, mysql, planetscale, postgres, postgres-trigger, sqlite-trigger, vitess -->

The online schema-change family splits by engine: `backfill` covers MySQL-family + Postgres in place; `expand-contract` and `deploy-ddl` are **PlanetScale-only** — they drive PlanetScale deploy requests and refuse without a PlanetScale service token ([schema-change-runbook](schema-change-runbook.md) has the per-command table).

<!-- planetscale-only-commands: deploy-ddl, expand-contract -->

## CDC modes per source engine

Continuous sync (`sluice sync start`) needs a change stream from the source. Which transport sluice uses — and what the source must provide — depends on the engine:

| Source engine | CDC transport | Source requirements | Notes |
|---|---|---|---|
| `mysql` | Row-based binary log (binlog) | `binlog_format=ROW` — anything else (STATEMENT, MIXED) is **refused up front** with `SLUICE-E-CDC-BINLOG-FORMAT-NOT-ROW`, because a non-ROW stream is a fully silent empty-stream mode (verified on a real server before the gate shipped, v0.99.292). Full row images are required too — `binlog_row_image=MINIMAL`/`NOBLOB` (Azure MySQL's platform default is MINIMAL) refuses at CDC start with `SLUICE-E-CDC-ROW-IMAGE-PARTIAL`. | GTID-based resume; covers RDS / CloudSQL / Azure / Percona — anything wire-compatible with upstream MySQL. |
| `mariadb` | Binlog with MariaDB domain GTIDs ([ADR-0170](adr/adr-0170-mariadb-flavor-phase3-cdc.md)) | Same ROW-format gate — note MIXED is MariaDB's *platform default*, so an untuned MariaDB source hits the refusal out of the box; the fix is one `SET GLOBAL binlog_format=ROW`. | Native `uuid`/`inet6`/`inet4` values carry faithfully on both bulk copy and the binlog tail ([ADR-0171](adr/adr-0171-mariadb-native-uuid-inet-cdc-decode.md)). |
| `planetscale` / `vitess` | Vitess VStream gRPC via vtgate | A vtgate endpoint; no binlog access needed. | VGTID positions, multi-shard, reshard-aware. A source-side throttle (typically a co-tenant OnlineDDL/MoveTables migration) can pause delivery — sluice WARNs on the throttle/idle signature and names `SHOW VITESS_THROTTLED_APPS`; latency only, never loss ([vitess-vstream-troubleshooting](vitess-vstream-troubleshooting.md)). |
| `postgres` | Logical replication (pgoutput) | `wal_level=logical`, a role with `REPLICATION`, headroom for one replication slot per stream — preflighted with an inventory refusal (`SLUICE-E-CDC-REPLICATION-HEADROOM`) rather than a mid-run failure. | Per-stream publications with a scope-conflict guard (`SLUICE-E-CDC-PUBLICATION-SCOPE-CONFLICT`) so concurrent differently-scoped streams can't silently de-scope each other; `--where` row filters push down into the publication on PG 15+; pre-emptive slot-retention warnings; `sluice sync decommission` retires a finished stream's slot + publication + control row. Prep guide: [postgres-source-prep](postgres-source-prep.md). |
| `postgres-trigger` | Trigger-based change log on the source | Plain DDL privileges — **no replication slot, no `REPLICATION` role, no `wal_level` change**. | The path for managed Postgres that blocks logical replication (e.g. Heroku). `trigger setup` installs, `trigger teardown` removes every trace. |
| `sqlite-trigger` / `d1-trigger` | Trigger-based change log | Write access to the file / D1 database. | Exactly-once resume via a change-log watermark. |
| `sqlite`, `d1`, `csv`, `tsv`, `ndjson`, `mydumper` | none | — | Migrate-only sources. |

<!-- cdc-modes: csv=none, d1=none, d1-trigger=triggers, mariadb=binlog, mydumper=none, mysql=binlog, ndjson=none, planetscale=vstream, postgres=logical-replication, postgres-trigger=triggers, sqlite=none, sqlite-trigger=triggers, tsv=none, vitess=vstream -->

The marker above is not decoration: `TestCDCModeTableMatchesTheRegistry` derives this pairing from the engine registry's own capability declarations and fails the build if the table drifts. It exists because "which engines does this CDC change affect" is a sentence this project has gotten wrong four releases running — most recently a fix to the **binlog** reader described as reaching PlanetScale and Vitess, which stream through VStream and were never on that path. If you are writing release notes about a CDC-path change, take the engine list from this table rather than from the shape of the fix.

CDC **apply** targets are MySQL-family and Postgres (concurrent key-hash apply on both). SQLite is a migrate-only target. The operator guide for running CDC day-to-day is [operator/cdc-streaming](operator/cdc-streaming.md).

## Known limitations

The honest list. Every entry is loud, off-default, or a schema-object (not value) gap — with **one known exception, the trigger-CDC REPLACE blind spot below**, which is silent at loss time and is therefore WARNed at setup time — and each is stated with its exact trigger so you can check it against your workload. Items marked *queued* have fixes scheduled.

**Trigger-CDC (`sqlite-trigger`/`d1-trigger`): OR-REPLACE implicit deletes across a non-PK UNIQUE conflict are not captured.** With SQLite's default `PRAGMA recursive_triggers=OFF` — a per-connection setting of *your application's* writing connections, which sluice cannot set for them — the row an `INSERT OR REPLACE`/`UPDATE OR REPLACE` implicitly deletes to satisfy a **non-PK UNIQUE** conflict fires no DELETE trigger: the delete is never captured and the target keeps the row, at exit 0. A PK-conflict REPLACE converges (the captured insert upserts the same key); a `UNIQUE ... ON CONFLICT REPLACE` clause gives even a plain INSERT the same blind spot. Since v0.122.0 `trigger setup` WARNs per affected table, and the SQLite behaviour itself is premise-pinned in both directions. Remedies: run every writing connection with `recursive_triggers=ON`, or write upserts as `INSERT ... ON CONFLICT DO UPDATE` (never implicitly deletes; on D1 prefer this — whether the pragma is settable through D1's bindings is unmeasured). pgtrigger and the log-based CDC engines are exempt (their capture sees the delete regardless). Details: [ADR-0135](adr/adr-0135-sqlite-trigger-cdc.md).

**PostGIS Z/M-dimensional columns toward MySQL refuse at preflight (v0.124.0).** MySQL 8 supports no Z/M geometry; a `geometry(POINTZ, …)` or Z/M-flagged `geography` column toward a MySQL-family target now refuses at the shared pre-DDL chokepoint (migrate, restore, chain restore), naming the dimension and the mid-run error 1416 the refusal prevents — previously it passed every preflight and the copy died mid-run on the raw 1416. Remedies: `ST_Force2D` the column on the source if losing the extra dimension is acceptable, `--exclude-table`, or `--type-override`. Plain 2D geometry and geography stay supportable.

**`postgres-trigger` sources with PostGIS columns: `trigger setup` refuses (v0.124.0).** The trigger capture renders a spatial value as GeoJSON, which the apply path cannot decode — before v0.124.0 setup and cold copy succeeded and the FIRST spatial DML wedged the stream mid-incident. Setup now refuses spatial columns up front (refusal reason `postgis-spatial-column`; detected at every wrapping — direct column, array element, DOMAIN over geometry, DOMAIN over a geometry array). Remedies: the native `postgres` engine where logical replication is available (it carries spatial values as EWKB faithfully), or `--exclude-table`. Existing installs: a setup re-run now refuses; an already-running stream's failure mode is unchanged-loud until then. An ST_AsEWKB capture that would carry spatial columns is the recorded, demand-gated alternative.

**MySQL XA (distributed) transactions on replicated tables refuse loudly mid-stream (`SLUICE-E-CDC-XA-UNSUPPORTED`).** sluice applies CDC rows at read time, but an XA body's rows are invisible on the source until a later `XA COMMIT` — applying them would fabricate rows on the target if the coordinator rolls back, and mid-body positions are not valid restart points. XA traffic touching only non-replicated tables streams past unaffected, so a filtered sync sharing a server with an XA-using application keeps working. PostgreSQL two-phase (`PREPARE TRANSACTION`) has no equivalent exposure: sluice's slots never opt into two-phase decoding, so PG delivers prepared transactions only at `COMMIT PREPARED`, as ordinary transactions. Remedy: keep XA off the replicated tables or exclude them; faithful XA replication (buffering prepared transactions) is demand-gated.

**Secondary/index-only DDL is not forwarded mid-sync (all CDC sources).** An `ALTER TABLE … ADD INDEX` executed on the source *during* continuous sync does not cross to the target — no CDC wire protocol carries secondary-index metadata, so sluice never sees the delta. Data is unaffected, and the cold-start copy carries all source indexes; only indexes added *after* the copy are missed, and today there is no WARN when it happens. Workaround: add the index on the target out-of-band (`CREATE INDEX CONCURRENTLY` on PG; a deploy request on PlanetScale). Design exists ([ADR-0103](adr/adr-0103-forward-index-ddl-during-cdc.md)); building per-source detection is demand-gated.

**User-defined triggers and event triggers are not carried (any path, including PG→PG).** The IR has no trigger model; the PG reader never reads `pg_trigger`/`pg_event_trigger`, so triggers are silently absent on the target after migrate, backup-restore, and sync. This deserves a specific callout: **an event trigger installed as a guard against accidental `DROP`/`TRUNCATE` vanishes silently too** — a guard whose whole job is safety should not be assumed to have crossed. Workaround: script trigger re-creation into your cutover runbook (`pg_dump --section=post-data` extracts them). Same-engine carry is a designed-but-not-built follow-up (roadmap item 50).

**Relaxed `sql_mode`: silent coercion WARNs on every write path.** If you opt into `--mysql-sql-mode=''` on a MySQL-family *target*, out-of-range or over-long values are coerced by the server (300→127, `'toolong'`→`'too'`) instead of refused. Every write path detects this via the server's warning list and emits a loud one-time-per-table WARN — the three bulk-copy paths since v0.99.28, and the steady-state CDC apply path (serial, coalesced-batch, and concurrent lanes) since the v0.100.0 line. Not reachable on the default path — strict mode (the default) refuses these loudly. Remedy: keep strict `sql_mode`, or `--type-override` the column to a type that fits. See [migrating-legacy-mysql](operator/migrating-legacy-mysql.md).

**UNIQUE-constraint attributes land weaker — now with a loud WARN.** `DEFERRABLE`, PG-15 `UNIQUE NULLS NOT DISTINCT`, and PG-18 `WITHOUT OVERLAPS` constraints are created on the target in their default (weaker) form — a NULLS-NOT-DISTINCT source admits duplicate NULLs on the target going forward. Since the v0.100.0 line the PG schema reader detects all three and WARNs once per constraint at read time, naming the attribute and the weaker landing, on every path that reads the schema (migrate, sync, backup, preview/diff). Faithful same-engine carry is a filed follow-up, as is the sibling gap the fix uncovered (PG 15+ also allows NULLS NOT DISTINCT on a plain `CREATE UNIQUE INDEX`, which still lands weaker un-WARNed). Remedy: re-apply the attribute on the target after migrate and confirm with `sluice schema diff`.

**Empty-but-drifted pre-existing target tables: one residual path.** The shape gate that refuses copying into a pre-existing target table whose schema drifted from what sluice would emit (`SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH`) covers `migrate` (v0.99.258, [ADR-0166](adr/adr-0166-migrate-precreate-shape-gate.md)) and the single-database `sync` cold start (v0.99.292). The **multi-database (`--all-databases`) sync cold-start path still creates tables ungated** — filed. Workaround there: `--reset-target-data` for a clean re-create, or pre-verify with `sluice schema diff`.

**Five expression-translator rules stay deferred** (`GREATEST`/`LEAST`, `REGEXP_LIKE`, `FIND_IN_SET`, `CONVERT_TZ`, `INET_ATON`/`INET_NTOA`) — each has a semantic divergence that makes auto-rewrite a masking risk; each fails loudly and has an escape hatch: `--expr-override TABLE.COLUMN=EXPR` for generated columns, `--exclude-table` or a source-side rewrite for CHECK/DEFAULT sites (the override rewrites only generated-column bodies). Full per-rule analysis: [translator-catalog](translator-catalog.md).

**Cross-engine view bodies are emitted verbatim.** A view definition that doesn't parse on the target surfaces as a loud target-side rejection at apply time; `--view-override` supplies the translated body. PG materialized views refresh via `sluice matview refresh` (PG-only).

**Multi-source MySQL fan-in has no per-table rename.** Aggregating N MySQL sources into one target database relies on DSN/database choice for namespacing; there is no `--rename-table SOURCE=TARGET` flag (PG multi-source uses `--target-schema`). Zero demand to date; tracked as roadmap item 9.

**No Arrow / columnar in-flight format.** The analytics exit is `backup export-as-parquet` (one Parquet file per table); Arrow as an IR row representation is deliberately deferred with zero current demand.

**PG 19 native sequence sync: tests are time-gated on GA (~2026-09).** sluice runs its own CDC and is unaffected by PG19's `FOR ALL SEQUENCES` publications; a PG19-beta canary already runs the full PG suite weekly. `sluice cutover` is today's engine-neutral, any-version equivalent of PG19's sequence sync (works PG 12–18 and cross-engine). Two sequence-carry residuals, both loud-not-silent: a sequence whose `INCREMENT` exceeds the cutover margin can fail the prime with a raw PG bounds error, and mid-stream `ALTER SEQUENCE` option changes are recorded in backups but not re-applied on chain restore.

**Cloud KMS signing/wrapping is live-validated on all three providers.** AWS KMS runs as a standing per-CI `kmsverify` localstack leg; GCP Cloud KMS and Azure Key Vault were live-validated against the real services on 2026-07-24 (sign/verify round-trips, tamper refusal, and — for Azure — the key-version-pinned rotate-then-restore-old-chain recovery). The GCP/Azure live runs are operator-dispatched rather than per-PR (no free local emulator). Failure modes throughout are loud, never silent.

**VStream throttle stalls are latency, not loss.** A Vitess-side throttle (most commonly a co-tenant online DDL on the same keyspace) pauses delivery with no error on the wire; sluice stays position-safe and converges on unthrottle, and WARNs on the throttle/idle signature. Budget for it in cutover timing on busy shared keyspaces.

**`--force-cold-start` bypasses the populated-target preflight.** The default slot-loss recovery path refuses loudly on a populated target; the bypass flag's name announces what it does. Don't script it.

**PG large objects are not copied, and an `oid`/`lo` column is refused.** `pg_largeobject` contents live outside every user table, so sluice does not carry them — and the referencing `oid`/`lo` column is itself an unsupported column type, so a source with one in scope does not migrate: the census WARN fires first naming every suspect `table.column`, then the schema read refuses loudly (`postgres: unsupported data_type "oid"`), on both `migrate` and `sync start`. To proceed without the table, `--exclude-table` it; to carry the blobs, convert the columns to inline `bytea` on the source first (`ALTER TABLE t ALTER COLUMN c TYPE bytea USING lo_get(c)`) so the bytes copy like any other column, or export them separately (`lo_export` / `pg_dump --blobs`). Full detail: [type-mapping § Postgres large objects](type-mapping.md#postgres-large-objects-pg_largeobject--oid--lo).

**The Heroku migrator wrapper is Postgres-target only.** sluice's core supports Heroku PG → MySQL-family via `postgres-trigger` (integration-tested); the convenience wrapper hasn't grown the target switch. Drive the core commands directly for that direction.

### QA-posture notes (what the test suite honestly covers)

- **Two hard test quarantines exist, both on non-required extended-suites legs:** (1) four reshard *skew* A/B tests are skipped — a harness incompatibility between the non-default `MinimizeSkew` mode and an intentionally-drained post-reshard shard; the core reshard exactly-once oracles (`ProofOfReshardability`, `RelaxSkewReshardMidStream`) remain active and green in both A/B runs, and the exposed mode is non-default. (2) The chaos `RollingUpgrade` leg is quarantined on a cluster bring-up infra flake that never reaches a sluice assertion. Neither skip hides an untested product path on defaults.
- **Live-PlanetScale verification (`psverify`) is operator-run, not CI.** The per-PR gates run real Vitess (vttestserver + multi-process clusters); the credentialed live-PS suites run on demand before releases ([dev/notes/ps-release-checklist](dev/notes/ps-release-checklist.md)). Tag publishes additionally require a real-cluster filtered move-OUT gate.
- **The dump-parity oracle is the machine-checked version of this page's schema-fidelity claims:** sluice's PG→PG and MySQL→MySQL output is diffed against `pg_dump`/`mysqldump` on every PR, and every allowlisted divergence must cite the doc that declares it — "what do we knowingly not carry?" is a reviewable file, not tribal knowledge.

## Pre-production checklist

The suggested path from evaluation to a production cutover. Each step is cheap relative to discovering its failure mode mid-migration.

1. **Preview the schema translation:** `sluice schema preview` against your real source. Translator gaps, type-mapping decisions, and refusals are visible here, before anything runs. Diff a scratch target with `sluice schema diff`.
2. **Dry-run the migration:** `sluice migrate --dry-run` prints the full plan (tables, types, indexes, constraints, notices) without touching the target.
3. **Rehearse on a non-production target:** run the real `migrate`, then `sluice verify` (`--depth count`, then `--depth sample` for content-hash spot checks). Investigate any delta before going further.
4. **Prep the source for CDC:** [postgres-source-prep](postgres-source-prep.md) (GUCs, slot lifecycle, failover survival) for PG sources; `binlog_format=ROW` + retention for MySQL/MariaDB ([operator/cdc-streaming](operator/cdc-streaming.md)); [managed-services](managed-services.md) for provider-specific preconditions. The preflights will refuse loudly on what's missing, but reading first avoids the round-trips.
5. **Rehearse the sync:** `sync start` against the non-production target with `--metrics-listen` (scrape `/metrics`, gate on `/readyz` — see [running-as-a-service](operator/running-as-a-service.md)) and `--source-heartbeat-interval=30s` on quiet sources. Watch the slot-health warnings (automatic on PG sources) and `sluice sync health --max-stale-seconds N` as the cron-able freshness probe.
6. **Wire alerting before cutover, not after:** the `--notify-*` threshold rules page webhook/Slack/SMTP sinks on slot retention, sync lag, storage, and target vacuum health. Alert on `sluice_seconds_since_last_apply` and `sluice_sync_lag_seconds` at minimum.
7. **Know the failure surface:** every recognized refusal carries a stable `SLUICE-E-*` code with a remedy ([operator/error-codes](operator/error-codes.md) — 61 codes, CI-synced with the code); exit codes are contractual (0/1/2/3/80). First move on anything unexplained: `sluice diagnose --output bundle.zip` for a redacted, shareable state bundle.
8. **Cut over deliberately:** drain with `sync stop --wait`, prime sequences with `sluice cutover` (prevents PK collisions on the first post-cutover INSERT), verify once more, then move traffic.
9. **Decommission finished streams:** `sluice sync decommission` drops the stream's replication slot, per-stream publication, and control row. A leftover PG slot pins WAL on the source indefinitely and blocks later differently-scoped streams — this is the step multi-wave migrations forget ([operator/staged-wave-migration](operator/staged-wave-migration.md)).

## Where to go deeper

[architecture](architecture.md) · [type-mapping](type-mapping.md) · [value-types](value-types.md) · [testing](testing.md) (the class-pin discipline) · [operator/cdc-streaming](operator/cdc-streaming.md) · [snapshot-cdc-handoff](snapshot-cdc-handoff.md) · [throughput-tuning](throughput-tuning.md) · [CHANGELOG](../CHANGELOG.md)
