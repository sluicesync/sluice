# sluice vs. alternatives — deep dive

The README carries the headline comparison matrix; this page is the longer-form companion. Each row of the matrix gets a paragraph here explaining *what the capability means in operator terms*, *what failure shape it closes*, and *why an alternative does or doesn't have it*.

This is the page to send a skeptical evaluator who's already seen the matrix and wants the "why does that row matter to me" version.

If a row in the README's matrix doesn't land for your scenario, the answer is in this doc — or it's a sign sluice isn't the right pick (see also the README's "When NOT to use sluice" section and [`docs/use-cases.md`](use-cases.md)).

For the **canonical open-source PG → PG comparison point (Bucardo)**, see the standalone [`docs/comparison-bucardo.md`](comparison-bucardo.md). That page carries measured head-to-head numbers from a one-on-one benchmark and the honest "which one should you pick" framing.

For **measured initial-copy throughput head-to-heads**, see [`docs/comparison-pgcopydb.md`](comparison-pgcopydb.md) (PG → PG bulk copy, the tool whose parallel-COPY tactics sluice borrowed) and [`docs/comparison-pgloader.md`](comparison-pgloader.md) (sluice vs. pgloader for MySQL → PG, plus the cross-engine + Vitess throughput matrix).

---

## Cross-engine MySQL ↔ Postgres in all four directions

**Capability claim.** sluice handles `MySQL → MySQL`, `MySQL → Postgres`, `Postgres → Postgres`, and `Postgres → MySQL`, plus the PlanetScale-MySQL flavor as a registered engine variant.

**Why this matters.** Most CDC tooling pivots on a single direction: Debezium needs a sink connector to land somewhere non-Kafka, AWS DMS only reads into AWS, Fivetran assumes a SaaS warehouse target, pgcopydb is PG → PG only. The cross-engine + cross-direction combination is the operational shape that lets one tool cover both the "moving in" and "moving out" sides of a multi-year platform migration.

**Where the alternatives land.**
- **Debezium:** requires a sink connector (JDBC sink, dedicated Postgres sink, etc.). Adds operational surface (Kafka + connector versions + serialization format).
- **AWS DMS:** all four directions in principle, but target must be in AWS or AWS-managed; bringing data *out* of AWS isn't the supported direction.
- **Fivetran / Stitch / Airbyte:** source-to-warehouse shape; not a peer-to-peer DB-to-DB story.
- **pgcopydb:** PG → PG only. The fast-path tactics (parallel COPY, deferred indexes) are the inspiration for sluice's bulk-copy implementation.
- **HVR / Striim / Qlik:** all four directions. The enterprise tier is where sluice positions against.

---

## `ADD COLUMN` auto-forwarding (since v0.79.0)

**Capability claim.** When the source executes `ALTER TABLE … ADD COLUMN …` mid-stream, sluice detects the DDL on the binlog (MySQL) or pgoutput stream (PG), applies the matching `ADD COLUMN` on the target, and continues replicating without an operator-driven stop/restart cycle.

**Why this matters.** Schema evolution is the #1 reason CDC pipelines break in production. The naive shape — stop replication, run the DDL by hand on both ends, restart — leaks application work onto the platform team. Operators want the platform to handle the safe cases; sluice does, while refusing loudly on the unsafe ones (computed `DEFAULT`s with side effects, etc.).

**Where the alternatives land.**
- **Debezium:** has it via the schema-history connector; configuration complexity is the trade.
- **AWS DMS:** partial — some DDL is forwarded, some is skipped by default; behavior varies by configuration.
- **Fivetran:** has it for SaaS targets.
- **pgcopydb:** snapshot-only; out of scope.
- **HVR:** has it, with operator-tunable refuse/auto-apply policies.

---

## Refuse-loudly on unsafe DDL with structured diff (F11, v0.81.0)

**Capability claim.** When the source emits DDL sluice can't safely translate or apply (a structural change outside the recognized-shape catalog, or a translation gap), the CDC stream halts with a structured error message naming the table, the unrecognized clause, and an operator-actionable hint (often "refuse-loudly with the drained-model recovery hint" from ADR-0054).

**Why this matters.** The alternative posture is to continue the stream with the unsafe DDL un-applied, which some tools do by default and some do depending on connector and configuration. When that happens, target and source schemas can diverge, and the drift surfaces later — usually as a row-apply failure with a confusing error, or as value truncation. Loud refusal at the moment of the unsafe DDL is sluice's tenet (`CLAUDE.md`); operators get an actionable error at the right time.

**Where the alternatives land.**
- **Debezium:** varies by connector. The PG connector has decent surface here; MySQL connectors with strict mode behave better.
- **AWS DMS:** skips unsafe DDL by default; schema-evolution auditing is handled out of band.
- **Fivetran:** typically resyncs the affected table rather than applying the DDL in place; exact behavior varies by connector and configuration.
- **HVR:** loud refuse + operator-tunable policy.

---

## Pre-emptive slot-retention warnings (F13, v0.80.0)

**Capability claim.** sluice's PG source-side stream watches `pg_stat_replication_slots`. When the slot's `restart_lsn` retention hits 70% of `max_slot_wal_keep_size`, sluice emits a WARN. At 85%, escalates. If the slot is `inactive` for ≥30 minutes, separate WARN. Operators learn *before* the slot evicts and the stream goes cold-start.

**Why this matters.** PG's slot eviction is the silent-failure case sluice's slot-health work was built around. The first sign an operator gets without sluice's monitoring is the cold-start fall-through (sluice surfaces it loudly per ADR-0022) or, worse, a `wal_keep_size`-shaped surprise: the operator's cluster fills disk because the slot is holding WAL the consumer never claimed. F13's pre-emptive warnings give a window for action.

**Where the alternatives land.**
- **Debezium:** operator monitors `pg_stat_replication` themselves. Some monitoring stacks (Datadog, Grafana) ship dashboards for this; sluice ships the warning logic in-process.
- **AWS DMS / Fivetran:** SaaS-managed; the operator doesn't see the slot at all (until disk fills).
- **HVR:** has it.

---

## Source-side heartbeat writer (F17, v0.82.0)

**Capability claim.** Opt-in `--source-heartbeat-interval=30s`. Sluice writes a tiny periodic row to a sluice-owned table on the source; the INSERT generates WAL (PG) / binlog (MySQL) so the CDC consumer's position advances even against a quiet source. Default OFF (it's a behavioural change on the source DB).

**Why this matters.** The complement to F13: F13 *detects* slot-retention pressure; F17 *prevents* it. On low-traffic source DBs (off-hours, weekends, dev environments), the slot's `restart_lsn` stagnates and the slot eventually evicts. F17 keeps the heartbeat moving so the consumer's claim stays current.

**Where the alternatives land.**
- **Debezium:** PG connector has a source-side heartbeat option (similar shape).
- **AWS DMS / Fivetran:** SaaS-managed; the operator can't add a heartbeat directly.
- **pgcopydb:** snapshot-only; no slot to manage.
- **HVR:** has it.

---

## Cutover sequence priming as one command (F10, v0.83.0)

**Capability claim.** `sluice cutover` — after sync stop, before traffic flip — reads source sequences and applies them to target with a safety margin. Closes the PK-collision-on-first-INSERT class without per-table `setval` by hand. See [`docs/cutover.md`](cutover.md).

**Why this matters.** Pre-v0.83.0 the manual procedure was "list every BIGSERIAL/AUTO_INCREMENT table, write `SELECT setval(seq, source_max + 1000)` for each, run them as the operator." A dozen-table migration was tractable; a 200-table migration was a 200-line bash script with no idempotency guarantees. F10 replaces it with one idempotent, refuse-loudly-on-target-ahead command.

**Where the alternatives land.**
- **Debezium / DMS / Fivetran / pgcopydb:** no equivalent. The operator handles cutover sequence priming themselves, typically with the same hand-rolled `setval` script.
- **partial in pgcopydb:** has primitives for sequence sync as part of its `pgcopydb clone` flow.
- **HVR:** has it.

---

## Inline PII redaction (bulk + CDC)

**Capability claim.** `--redact 'table.column=STRATEGY'` masks / hashes / tokenizes / randomizes PII inline — on both the cold-start bulk copy and the live CDC stream — so the data lands already-redacted on the target and PII never sits in plaintext on disk (backup chunks included). 26 strategies across 5 phases, including format-preserving presets (SSN / PAN / email / IBAN) and keyset-backed deterministic tokenization for cross-stream surrogate stability. See [`docs/redaction.md`](redaction.md).

**Why this matters.** The common shape is "migrate prod → a staging / analytics / vendor target that must not hold raw PII." A post-load `UPDATE` leaves a plaintext window; redacting in the application leaves the migration path uncovered. Redacting in the replication path closes both.

**Where the alternatives land.**
- **Debezium:** partial — SMTs (single-message transforms) can mask or drop fields, but it's per-connector wiring, not a first-class strategy catalog.
- **AWS DMS:** partial — transformation rules remove / rename columns and do limited value rewriting; not a redaction-strategy library.
- **Fivetran:** partial — column hashing and column blocking are supported; format-preserving masks and tokenization are a different surface.
- **pgcopydb:** none.
- **HVR:** yes — agent-side data transformation can mask in-flight.

---

## Slot-less CDC for locked-down Postgres

**Capability claim.** The `postgres-trigger` engine (`--source-driver=postgres-trigger`) captures changes via triggers + a change-log table instead of a logical-replication slot, so sluice can stream CDC from managed Postgres that doesn't grant `REPLICATION` or expose logical decoding (Heroku Postgres is the canonical case). `sluice trigger setup` installs the source-side state; `--allow-polled-fingerprint` covers DDL detection on a non-superuser role. See the slot-less recipe in [`docs/cookbook/`](cookbook/).

**Why this matters.** Slot-based CDC is the default everywhere, but a whole tier of managed Postgres simply won't let a customer create a slot. Without a trigger fallback the answer is "you can't replicate continuously from that source" — exactly the gap operators hit on Heroku.

**Where the alternatives land.**
- **Debezium / AWS DMS:** no — both require logical decoding (a slot) for Postgres CDC; a source that blocks slots blocks them.
- **Fivetran:** partial — offers non-log incremental sync by polling, with the usual polling caveats (delete fidelity, latency).
- **pgcopydb:** none — snapshot copy only, no CDC.
- **HVR:** yes — trigger-based capture has long been a first-class option for sources without log access; it's the closest analogue, and the model sluice's trigger engine follows.

---

## Encrypted logical backups + continuous-backup broker

**Capability claim.** `sluice backup` writes encrypted logical backup chains (a full snapshot plus incremental CDC segments) to local disk or a blob store, under a versioned `FormatVersion` manifest contract that refuses-before-touch on an older binary rather than silently dropping metadata. `sluice restore` replays a chain into a target, and `sluice sync from-backup run` runs a long-lived **broker** that follows a growing chain and applies incrementals into a target continuously — backup-as-replication-source. See [`docs/backup-format-versioning.md`](backup-format-versioning.md).

**Why this matters.** It decouples capture from apply: take encrypted backups on a schedule, then restore or broker them into one or many targets (cross-region, air-gapped, compliance-driven audit trail) without holding a live source connection the whole time.

**Where the alternatives land.**
- **Debezium / AWS DMS / Fivetran / pgcopydb:** none ship this — they are live source-to-sink paths (or, for DMS / Fivetran, managed services), not an encrypted logical-backup-chain format with a broker replay loop. Physical provider snapshots (RDS, etc.) are a different, non-portable shape.
- **HVR:** no direct equivalent; its file-integrate targets are not an encrypted, restorable logical-backup chain.

---

## Schema fidelity: RLS, PostGIS, DOMAIN / CHECK, generated columns

**Capability claim.** sluice carries schema constructs most data-movement tools drop on the floor. PG Row-Level Security policies are captured into the IR and re-emitted on a same-engine target (and the cross-engine PG → MySQL case refuses loudly rather than silently shipping unprotected rows). PostGIS geometry round-trips with SRID. PG `DOMAIN` `CHECK` constraints translate to MySQL 8.0.16+ table-level `CHECK` where the shape is safely translatable, and refuse loudly where it isn't. Generated columns and many `CHECK` idioms translate via the ADR-0016 expression translator, with `--expr-override` as the escape hatch.

**Why this matters.** The failure these close is the *silent* one: a migration that "succeeds" but quietly leaves the target without the RLS policy, the geometry SRID, or the constraint that was enforcing integrity on the source. The operator finds out when bad data lands — or when one tenant sees another tenant's rows.

**Where the alternatives land.**
- **Debezium / AWS DMS / Fivetran:** generally do not migrate RLS policies, `DOMAIN` / `CHECK` semantics, or PostGIS SRID metadata as first-class objects; schema handling centers on column types. Constraint / policy fidelity is the operator's separate problem.
- **pgcopydb:** same-engine PG → PG, so native objects copy faithfully — but it's PG-only, so the cross-engine translation question doesn't arise.
- **HVR:** strong schema handling within its supported matrix; RLS / cross-engine constraint-translation specifics vary by configuration.

---

## SQLite & Cloudflare D1 as first-class sources

**Capability claim.** sluice imports a SQLite file, a `wrangler d1 export`
`.sql` dump, or a **live Cloudflare D1** (over its HTTP query API) into
Postgres or MySQL in one command — and emits a SQLite `.db` as a target.
The lossless path matters: D1's query API and its export both serialise
through JSON float64, so integers above 2⁵³ round on the way out; sluice's
`d1` reader projects `typeof()` + `CAST(... AS TEXT)` / `hex()` per column
so big integers and BLOBs round-trip exactly ([ADR-0132](adr/adr-0132-d1-query-api-reader.md)).
Continuous sync off SQLite/D1 is trigger-based (`sqlite-trigger` /
`d1-trigger`) since SQLite's WAL is a physical page-log, not a logical
change stream.

**Why this matters.** The serverless / edge path (D1, embedded SQLite)
into a "real" managed database is otherwise hand-rolled export scripts —
exactly where the silent big-integer rounding bites.

**Where the alternatives land.**
- **pgloader:** the usual SQLite → PG tool, but PG-target only and a
  separate toolchain; no live-D1 or MySQL-target path.
- **Debezium / AWS DMS / Fivetran / HVR:** centered on MySQL / Postgres /
  Oracle / SQL Server sources; SQLite and Cloudflare D1 are not in scope.
- **pgcopydb:** PG → PG only.

---

## Single static binary, no daemon, no Kafka

**Capability claim.** sluice is one Go binary (cross-platform via GoReleaser). No coordinator process, no Kafka cluster, no manifest server, no daemon. Run it from a bastion host, a build agent, or a laptop with network reach to both endpoints. State lives in target's `sluice_*` control tables; no external state store.

**Why this matters.** Operational complexity is a real cost. Standing up Kafka just to use Debezium adds days of setup work and ongoing operational burden (broker quorum, retention, partition counts, ACLs). Sluice's single-binary shape mirrors what `pg_dump`, `mysqldump`, and `pgcopydb` get right: low ceremony, predictable failure modes, easy to reason about.

**Where the alternatives land.**
- **Debezium:** Kafka + connector framework. Operational surface is significant.
- **AWS DMS / Fivetran:** SaaS, so no operator-side daemon — but operator gives up the "run anywhere" property.
- **pgcopydb:** single binary, same shape. The shared philosophy.
- **HVR:** dedicated coordinator process.

---

## Open-source (Apache 2.0)

**Capability claim.** sluice is Apache 2.0 licensed; full source on GitHub; no enterprise feature gating; no telemetry phone-home.

**Why this matters.** For operators in regulated industries (healthcare, finance, government), no-vendor-dependency replication is sometimes the only compliant shape. For self-hosted / air-gapped deployments, an OSS tool is the only option. For organizations where the platform team's tools must be auditable end-to-end, sluice's source is the auditor's source.

**Where the alternatives land.**
- **Debezium:** Apache 2.0.
- **AWS DMS:** proprietary.
- **Fivetran:** proprietary, SaaS-only.
- **pgcopydb:** BSD.
- **HVR / Striim / Qlik:** proprietary, commercial.

---

## Per-row pricing: none

**Capability claim.** sluice has no usage-based pricing. The cost is whatever compute and egress the operator already pays for. A 50TB migration costs the same in licensing as a 50GB one (zero).

**Why this matters.** SaaS CDC tools price on Monthly Active Rows (Fivetran's MAR), DMS instance-hours (AWS), or per-source connection (HVR). For a one-time migration of a large dataset, those line items can dominate the engineering budget. For an *ongoing* CDC sync of a high-volume source, the cost crossover with running sluice yourself happens early.

**Where the alternatives land.**
- **Debezium:** no per-row cost (you pay for Kafka + compute).
- **AWS DMS:** per-DMS-instance-hour.
- **Fivetran:** per-MAR (Monthly Active Rows) tier.
- **pgcopydb:** no cost.
- **HVR:** per-source license. Multi-year contracts in the six-figure range are common.

---

## The honest take

For each row above, an operator's decision usually breaks down to:

1. **What's already in your stack?** If you have Kafka, Debezium is the path of least resistance — you've already paid the operational cost.
2. **What does the destination look like?** AWS-targeted? DMS. Warehouse-targeted? Fivetran or Airbyte. Cross-engine peer-to-peer? sluice is built for this.
3. **What's your tolerance for SaaS dependency?** SaaS shifts the operational burden to the vendor; OSS keeps it on your team. The trade is real and personal.
4. **What's the failure-mode discipline you need?** sluice's loud-failure tenet is opinionated — operators who want auto-remediation may find sluice's refusal-on-uncertainty grating. Operators who've been burned by silent-skip surprises tend to prefer it.

sluice is not the right tool for every shape — see the README's "When NOT to use sluice" section. But for the cross-engine, on-prem-friendly, OSS-licensed, no-Kafka shape, it's the most-direct fit on the spectrum.

---

## Cross-references

- [`README.md`](../README.md) — the headline matrix
- [`docs/use-cases.md`](use-cases.md) — concrete operator scenarios
- [`docs/cutover.md`](cutover.md) — the `sluice cutover` shipped capability
- [`docs/architecture.md`](architecture.md) — IR + engine pattern that makes cross-engine work
