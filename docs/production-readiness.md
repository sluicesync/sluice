# Production readiness

This page is the one-stop answer to "is sluice ready for my workload?" — the support matrix, the CDC modes per source, the honest list of what sluice does *not* do today, and a pre-production checklist. It is maintained against the code, not aspirations; every limitation below names its trigger condition and its workaround. Status as of v0.99.292.

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

Every cell supports one-shot `migrate`; every cell whose source has a CDC mode (next section) also supports continuous `sync`. MySQL flavors (vanilla, MariaDB, PlanetScale, Vitess) share one engine implementation with per-flavor `Capabilities` declarations; MariaDB is first-class since v0.99.268 (native `uuid`/`inet6`/`inet4` carry, domain-GTID CDC, JSON-via-`json_valid`). A `*.connect.psdb.cloud` host also works under `--source-driver mysql` (binlog CDC) with the Vitess `_vt_*` shadow tables auto-excluded.

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

### Adjacent surfaces

Encrypted logical backup chains (full + incremental) with restore, point-in-time chain replay, and a continuous broker work on every engine that migrates; `sluice backup export-as-parquet` transcodes any backup chain into Parquet for DuckDB / warehouse ingestion ([ADR-0164](adr/adr-0164-backup-export-as-parquet.md)). The online schema-change family (`backfill`, `expand-contract`, `deploy-ddl`) covers MySQL-family + Postgres in place ([schema-change-runbook](schema-change-runbook.md)).

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

CDC **apply** targets are MySQL-family and Postgres (concurrent key-hash apply on both). SQLite is a migrate-only target. The operator guide for running CDC day-to-day is [operator/cdc-streaming](operator/cdc-streaming.md).

## Known limitations

The honest list. Every entry is loud, off-default, or a schema-object (not value) gap — there is no known default-path silent data-loss vector — but each is stated with its exact trigger so you can check it against your workload. Items marked *queued* have fixes scheduled.

**Secondary/index-only DDL is not forwarded mid-sync (all CDC sources).** An `ALTER TABLE … ADD INDEX` executed on the source *during* continuous sync does not cross to the target — no CDC wire protocol carries secondary-index metadata, so sluice never sees the delta. Data is unaffected, and the cold-start copy carries all source indexes; only indexes added *after* the copy are missed, and today there is no WARN when it happens. Workaround: add the index on the target out-of-band (`CREATE INDEX CONCURRENTLY` on PG; a deploy request on PlanetScale). Design exists ([ADR-0103](adr/adr-0103-forward-index-ddl-during-cdc.md)); building per-source detection is demand-gated.

**User-defined triggers and event triggers are not carried (any path, including PG→PG).** The IR has no trigger model; the PG reader never reads `pg_trigger`/`pg_event_trigger`, so triggers are silently absent on the target after migrate, backup-restore, and sync. This deserves a specific callout: **an event trigger installed as a guard against accidental `DROP`/`TRUNCATE` vanishes silently too** — a guard whose whole job is safety should not be assumed to have crossed. Workaround: script trigger re-creation into your cutover runbook (`pg_dump --section=post-data` extracts them). Same-engine carry is a designed-but-not-built follow-up (roadmap item 50).

**Relaxed `sql_mode`: the steady-state CDC apply path does not yet WARN on silent coercion (open today, fix queued).** If you opt into `--mysql-sql-mode=''` on a MySQL-family *target*, out-of-range or over-long values are coerced by the server (300→127, `'toolong'`→`'too'`). All three bulk-copy write paths detect this via the server's warning list and emit a loud one-time-per-table WARN (since v0.99.28); the **CDC change-applier path does not run that check yet**, so a steady-state out-of-range UPDATE under relaxed mode clamps with no signal. Not reachable on the default path — strict mode (the default) refuses these loudly. Remedy: keep strict `sql_mode`, or `--type-override` the column to a type that fits. See [migrating-legacy-mysql](operator/migrating-legacy-mysql.md).

**UNIQUE-constraint attributes land weaker (open today, fix queued).** `DEFERRABLE`, PG-15 `UNIQUE NULLS NOT DISTINCT`, and PG-17 `WITHOUT OVERLAPS` are not read from `pg_constraint`, so a constraint carrying them is created on the target in its default (weaker) form — a NULLS-NOT-DISTINCT source admits duplicate NULLs on the target going forward; WITHOUT OVERLAPS fails loudly at `ADD CONSTRAINT` only if the data happens to overlap. Trigger condition: a PG source actually using one of these three attributes. Remedy until the fix lands: re-apply the attribute on the target after migrate (`ALTER TABLE … DROP CONSTRAINT`/`ADD CONSTRAINT … NULLS NOT DISTINCT`) and confirm with `sluice schema diff`.

**Empty-but-drifted pre-existing target tables: one residual path.** The shape gate that refuses copying into a pre-existing target table whose schema drifted from what sluice would emit (`SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH`) covers `migrate` (v0.99.258, [ADR-0166](adr/adr-0166-migrate-precreate-shape-gate.md)) and the single-database `sync` cold start (v0.99.292). The **multi-database (`--all-databases`) sync cold-start path still creates tables ungated** — filed. Workaround there: `--reset-target-data` for a clean re-create, or pre-verify with `sluice schema diff`.

**Five expression-translator rules stay deferred** (`GREATEST`/`LEAST`, `REGEXP_LIKE`, `FIND_IN_SET`, `CONVERT_TZ`, `INET_ATON`/`INET_NTOA`) — each has a semantic divergence that makes auto-rewrite a masking risk; each fails loudly at apply time and has the `--expr-override TABLE.COLUMN=EXPR` escape hatch. Full per-rule analysis: [translator-catalog](translator-catalog.md).

**Cross-engine view bodies are emitted verbatim.** A view definition that doesn't parse on the target surfaces as a loud target-side rejection at apply time; `--view-override` supplies the translated body. PG materialized views refresh via `sluice matview refresh` (PG-only).

**Multi-source MySQL fan-in has no per-table rename.** Aggregating N MySQL sources into one target database relies on DSN/database choice for namespacing; there is no `--rename-table SOURCE=TARGET` flag (PG multi-source uses `--target-schema`). Zero demand to date; tracked as roadmap item 9.

**No Arrow / columnar in-flight format.** The analytics exit is `backup export-as-parquet` (one Parquet file per table); Arrow as an IR row representation is deliberately deferred with zero current demand.

**PG 19 native sequence sync: tests are time-gated on GA (~2026-09).** sluice runs its own CDC and is unaffected by PG19's `FOR ALL SEQUENCES` publications; a PG19-beta canary already runs the full PG suite weekly. `sluice cutover` is today's engine-neutral, any-version equivalent of PG19's sequence sync (works PG 12–18 and cross-engine). Two sequence-carry residuals, both loud-not-silent: a sequence whose `INCREMENT` exceeds the cutover margin can fail the prime with a raw PG bounds error, and mid-stream `ALTER SEQUENCE` option changes are recorded in backups but not re-applied on chain restore.

**Azure Key Vault key-version unwrap is unit-faked, not live-validated.** The wrap-time key version is pinned and recorded (so key auto-rotation shouldn't break restores), but this leg has never been exercised against a real AKV — AWS and GCP KMS have live-validated equivalents. Failure mode is loud ("wrong key or tampered"), never silent. If you rely on AKV encryption with rotation, validate a restore before trusting the chain.

**VStream throttle stalls are latency, not loss.** A Vitess-side throttle (most commonly a co-tenant online DDL on the same keyspace) pauses delivery with no error on the wire; sluice stays position-safe and converges on unthrottle, and WARNs on the throttle/idle signature. Budget for it in cutover timing on busy shared keyspaces.

**`--force-cold-start` bypasses the populated-target preflight.** The default slot-loss recovery path refuses loudly on a populated target; the bypass flag's name announces what it does. Don't script it.

**PG large objects are not copied.** `pg_largeobject` contents live outside every user table; referencing `oid`/`lo` columns copy as plain integers and a preflight WARN names the suspect columns. Carry recipes (export separately, or convert to inline `bytea` first): [type-mapping § Postgres large objects](type-mapping.md#postgres-large-objects-pg_largeobject--oid--lo).

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
