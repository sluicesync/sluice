# sluice

## Open-source HVR-class CDC for MySQL ↔ Postgres

Continuous sync between MySQL and Postgres in all four directions, with the schema-evolution, cutover-priming, and slot-health capabilities that have lived behind enterprise-tier paywalls (HVR, Striim, Qlik Replicate). Initial snapshot, CDC catch-up, and operator-driven cutover in one tool, opinionated about correctness.

- 🔄 **Bidirectional** — MySQL → Postgres, Postgres → MySQL, same-engine in both directions, PlanetScale flavors included
- 🪶 **Schema evolution** — `ADD COLUMN` forwards automatically; every other shape refuses loudly with a structured drift diff naming the column that changed
- 🩺 **Operational telemetry** — pre-emptive PG slot-health warnings (70 % / 85 % retention + 30 min inactivity); source-side heartbeat writer keeps slots alive against quiet sources
- 🔁 **Cutover** — one-command sequence priming (`sluice cutover`) prevents PK collisions on the first post-cutover `INSERT`
- 🛑 **Loud failure by default** — every silent-loss class we have caught has a structured refuse-loudly message with an operator-action recovery hint. Paste into Slack and the on-call DBA knows what to fix.

Pre-built Linux / macOS / Windows binaries on every tagged release: [latest release](https://github.com/orware/sluice/releases/latest). Apache 2.0, single static binary, no daemon, no SaaS dependency.

---

## Quick start

```bash
# Install
go install github.com/orware/sluice/cmd/sluice@latest

# One-shot migration: MySQL → Postgres
sluice migrate \
    --source-driver mysql    --source 'root:rootpw@tcp(localhost:3306)/app' \
    --target-driver postgres --target 'postgres://postgres:pgpw@localhost:5432/app?sslmode=disable'

# Continuous sync: snapshot + CDC catch-up, resumable on restart
sluice sync start \
    --source-driver mysql    --source ... \
    --target-driver postgres --target ... \
    --stream-id myapp-prod

# Cutover-time sequence priming (post-snapshot, pre-traffic-switch)
sluice cutover --config sluice.yaml --cutover-sequence-margin=1000
```

A 10-minute walkthrough against real MySQL 8.0 + Postgres 16 containers, loading the Sakila sample database, lives at [`docs/examples/quickstart.md`](docs/examples/quickstart.md).

---

## What sluice does

sluice is built around three product surfaces, each independently runnable:

| You want to… | Run |
|---|---|
| Move data **once** between MySQL and Postgres, then stop | `sluice migrate` |
| Move data **once** with low downtime — snapshot + CDC catch-up + cutover | `sluice migrate` → `sluice sync start --resume` → `sluice cutover` |
| **Replicate continuously** for analytics, geo-locality, or hot-standby | `sluice sync start` |
| **Preview** the target DDL before running anything | `sluice schema preview` |
| **Diff** a target against what sluice would produce | `sluice schema diff` |
| **Verify** that every row made it across | `sluice verify` |
| **Probe** a running sync's freshness against a staleness threshold | `sluice sync health` |
| Do all of the above against **PlanetScale** | Same commands; PS-MySQL uses VStream automatically when the DSN host matches `*.connect.psdb.cloud` |

### The four HVR-class features that landed in v0.80.0 – v0.83.0

These are the operator-pain features Reddit's `/r/PostgreSQL`, `/r/mysql`, and `/r/dataengineering` keep flagging as the reason teams reach for paid CDC tooling. Each one closed a catalogued silent-loss or silent-under-information class.

| Feature | Shipped in | What it does |
|---|---|---|
| **F13 — Pre-emptive PG slot-health warnings** | [v0.80.0](https://github.com/orware/sluice/releases/tag/v0.80.0) | A 30-second background probe per PG-sourced stream emits structured WARNs when `pg_replication_slots` retention crosses 70 % / 85 % of `max_slot_wal_keep_size`, or when a slot has been inactive for ≥30 min. De-duplicates within a 5-min window; severity transitions and clears emit immediately. Surfaces the slow burn *before* Postgres silently evicts the slot. ([ADR-0059](docs/adr/adr-0059-pg-slot-health-prewarning.md)) |
| **F11 — Per-table schema-drift diff in refuse messages** | [v0.81.0](https://github.com/orware/sluice/releases/tag/v0.81.0) | When a non-`ADD COLUMN` source DDL arrives over CDC, the refusal now names the specific columns, indexes, and constraints that drifted plus an operator-action hint per category (`[column-added] foo TIMESTAMP NULL — drained schema migrate ...`). Greppable prefixes for Slack / ticket workflows. Pre-F11, operators ran `pg_dump`-diff by hand to find out *what* changed. ([ADR-0060](docs/adr/adr-0060-cdc-schema-drift-diff.md)) |
| **F17 — Source-side heartbeat writer** | [v0.82.0](https://github.com/orware/sluice/releases/tag/v0.82.0) | Optionally writes a tiny periodic row to a sluice-owned table on the source. The `INSERT` generates WAL / binlog so the consumer's position advances even against a quiet source, preventing silent slot eviction / binlog rotation past the consumer on low-traffic sources. Default-off; opt in with `--source-heartbeat-interval=30s`. Pairs with F13: F13 detects the symptom, F17 prevents the cause. ([ADR-0061](docs/adr/adr-0061-source-side-heartbeat-writer.md)) |
| **F10 — Cutover sequence priming** | [v0.83.0](https://github.com/orware/sluice/releases/tag/v0.83.0) | `sluice cutover` reads source PG sequences (`pg_sequences.last_value`) / MySQL `AUTO_INCREMENT` values and bumps the target by `--cutover-sequence-margin=N` (default 1000). Closes the PK-collision-on-first-post-cutover-`INSERT` class. Idempotent; refuses loudly when target value is already above the safety margin (signal that traffic landed before cutover priming ran). Skips composite-PK / UUID / no-sequence tables gracefully. ([ADR-0062](docs/adr/adr-0062-cutover-sequence-priming.md)) |

### Engines and directions

| Source ↘ Target → | MySQL | PostgreSQL | PlanetScale MySQL | PlanetScale PG |
|---|---|---|---|---|
| **MySQL** | ✓ | ✓ | ✓ | ✓ |
| **PostgreSQL** | ✓ | ✓ | ✓ | ✓ |
| **PlanetScale MySQL** | ✓ (VStream CDC) | ✓ (VStream CDC) | ✓ | ✓ |
| **PlanetScale PG** | ✓ | ✓ | ✓ | ✓ |

Cross-engine type translation handles the common surfaces (PG `UUID` / `INET` / `MACADDR` / `ARRAY` ↔ MySQL `CHAR(36)` / `VARCHAR` / `JSON`; MySQL `TINYINT(1)` ↔ PG `BOOLEAN`; MySQL `ENUM` / `SET` → PG enum / `TEXT[] + CHECK`; PostGIS `GEOMETRY` round-trips with SRID; many idioms in generated columns and `CHECK` constraints translate automatically — see [`docs/dev/translator-coverage.md`](docs/dev/translator-coverage.md)). When the default doesn't fit, `--type-override TABLE.COLUMN=TYPE` and `--expr-override TABLE.COLUMN=EXPR` cover one-off cases without writing a config file.

---

## Why "opinionated about correctness"

sluice has **zero production users today.** That is not a problem to rush past — it is the entire reason the tool's tenet hierarchy puts user-trust above feature throughput. The first real migration that silently corrupts data ends the project's credibility permanently. There is no install base to be impressed by breadth, so the calculus tilts toward refusing to ship a happy path that has not been pinned against the silent-loss class behind it.

### Loud failure by default

Every recognized failure mode has a structured refuse-loudly message that names the offending object (table, column, slot, role) and an operator-action recovery hint. The refusal text is greppable, paste-friendly, and stable enough that a Slack-posted refusal is enough for an on-call DBA to start work. A few representative classes the catalog covers today:

- **CDC schema drift** — non-`ADD COLUMN` source DDL arrives in the stream → refusal names every drifted column / index / constraint plus the drained-model recovery hint (F11, v0.81.0).
- **PG Row-Level Security without `BYPASSRLS`** — source role would silently filter the snapshot through `USING` policies → preflight refuses with three recovery paths (grant `BYPASSRLS`, run as superuser/owner, or `--exclude-table` the tenant-scoped data) (v0.78.4).
- **`ADD COLUMN` with a computed `DEFAULT`** — `NOW()`, `nextval()`, `gen_random_uuid()`, `random()`, `UUID()`, `RAND()` would cause source-target row-by-row divergence on already-shipped rows → refusal names the column, table, expression text, and the volatility reason; literal defaults continue to forward (Bug 90 / 91, v0.79.1).
- **Cutover sequence collision** — target sequence already above safety margin → refusal signals that traffic landed before cutover priming ran; suggests manual re-snapshot (v0.83.0).
- **`information_schema_stats_expiry` cache effects, table-name folding, identifier-quoting asymmetries** — every silent-loss class caught in v0.78.x has a permanent unit-test pin against the class, not the representative.

### Bug 86–91: the silent-loss arc the tenet caught

Between v0.78.0 and v0.79.1, six bugs surfaced through battle-testing that — taken together — make the case for why this tenet hierarchy is non-negotiable. Each one was a class of *silent* divergence the happy path papered over, and each one shipped with a class-pin test that would have caught the next variant in CI:

- **Bug 86** (v0.78.1) — pgoutput's `RelationMessage` carries no `attnotnull` or default-precision typmod, so post-DDL CDC snapshots had `Nullable=false` and `Precision=0` for every column regardless of source state. RENAME refusal then fired a phantom "alter column nullability" combo refusal against any schema with a nullable `NUMERIC` / `TEXT` / `TIMESTAMP`. Fix extended `NormalizeForCDCComparison` to zero what CDC can't carry; the pin matrix now exercises six type cells at the boundary.
- **Bug 87** (v0.78.2) — `pg_get_serial_sequence($1, $2)` parses its first argument as identifier text, which means unquoted mixed-case names fold to lowercase. A target table `"Widgets"` lookup became `public.widgets`, raised `42P01` on Migrator-side (loud) — and silently stalled the CDC streamer in retry-backoff (silent-loss-shape, never transitioned to CDC mode). The pin is now a 4-direction × 4-shape × 2-path matrix (32 scenarios).
- **Bug 88** (v0.78.3) — under MySQL `binlog_row_image=MINIMAL`, the DELETE Before-image carries `nil` for non-PK columns. The applier emitted `col IS NULL` predicates against those nils, matched zero rows on the target, ADR-0010 idempotency absorbed the miss, position advanced, and the source DELETE silently did not propagate. Mirrors the Bug 8 pattern PG had already fixed via `filterDeleteBefore`. Fix narrows the DELETE Before-image to PK columns only at the CDC reader before emit.
- **Bug 89** (v0.79.0) — MySQL fold-points in the ADR-0058 ADD COLUMN forwarding path.
- **Bug 90** (v0.79.1) — `--forward-schema-add-column` did not fire the §2a computed-DEFAULT refusal in production because the CDC reader's `RelationMessage` / `TableMapEvent` projection drops the `DEFAULT` clause on every column (pgoutput has no `attdefault` slot; MySQL's `TableMapEvent` has no `COLUMN_DEFAULT`), so the post-DDL `SchemaSnapshot` always arrived with `Default == nil`. Operators turning on the flag for a routine `created_at TIMESTAMPTZ DEFAULT NOW()` saw the happy-path log, the ALTER landed, and every pre-existing target row silently diverged from source. **Severity-A silent-loss** on the marquee schema-evolution feature. Fix wires a source-side `SchemaReader` probe through the intercept and runs a text-based volatility classifier against a deny-list of time-volatile / sequence-stateful / random / session-state functions.
- **Bug 91** (v0.79.1) — follow-on: PG `nextval` was classified as `DefaultNone{}` by the SERIAL auto-increment heuristic, so the Bug 90 classifier never saw the volatile expression. Fix added a `RawDefaultReader` interface that bypasses the heuristic and reads `information_schema.columns.column_default` directly.

The lesson all six share: **the integration test was green** at the surface that shipped. The bugs lived one level below the surface the happy path exercised, in a different driver codec path, a different IR canonicalization asymmetry, a different binlog projection slot. So sluice's testing discipline is to pin **the class, not the representative** — every encoder, decoder, or family-dispatched codec has a per-family × per-shape matrix, with `src==dst` ground-truthed on the real target. See [`docs/testing.md`](docs/testing.md) and the "Bug 74 lesson" section in [`CLAUDE.md`](CLAUDE.md).

---

## Architecture in one paragraph

[`internal/ir`](internal/ir) defines a typed dialect-neutral schema and value model plus the `Engine`, `SchemaReader`, `SchemaWriter`, `RowReader`, `RowWriter`, `CDCReader`, `ChangeApplier` interfaces. Each engine package (`internal/engines/mysql`, `internal/engines/postgres`) implements those interfaces and self-registers via `init()`. `internal/pipeline.Migrator` is the simple-mode orchestrator: read source schema → optional dry-run plan → create target tables (no constraints) → bulk-copy rows → create indexes → create constraints. `cmd/sluice` is a [kong](https://github.com/alecthomas/kong)-based CLI; config loading is via [koanf](https://github.com/knadh/koanf) YAML + env. Engines are looked up by name from `engines.Get(...)`; the pipeline package never imports specific engine packages. MySQL has flavors (Vanilla, PlanetScale) — same engine code, different `Capabilities` declarations, registered under different names. Additional engines slot in without touching the orchestrator.

The longer story lives in [`docs/architecture.md`](docs/architecture.md).

---

## Where sluice fits (vs. alternatives)

This is the category claim: sluice is the open-source tool whose feature set most directly overlaps the enterprise-tier CDC products operators reach for when DMS / Debezium / Fivetran fall short on schema evolution, cutover priming, or slot health.

|  | sluice | Debezium | AWS DMS | Fivetran | pgcopydb | HVR (commercial) |
|---|---|---|---|---|---|---|
| **Cross-engine MySQL ↔ Postgres** | ✓ all 4 directions | requires sink connector | ✓ | ✓ | PG → PG only | ✓ |
| **`ADD COLUMN` auto-forwards** | ✓ (opt-in, since v0.79.0) | ✓ via schema-history connector | partial | ✓ | ✗ (snapshot only) | ✓ |
| **Refuse-loudly on unsafe DDL with structured diff** | ✓ (F11, v0.81.0) | varies by connector | ✗ (silent skip) | ✗ | n/a | ✓ |
| **Pre-emptive slot-retention warnings** | ✓ (F13, v0.80.0) | ✗ (operator monitors `pg_stat_replication`) | ✗ | n/a (SaaS) | ✗ | ✓ |
| **Source-side heartbeat writer** | ✓ (F17, v0.82.0) | ✓ (PG only) | ✗ | n/a | ✗ | ✓ |
| **Cutover sequence priming as one command** | ✓ (F10, v0.83.0) | ✗ (manual `setval`) | ✗ | ✗ | partial | ✓ |
| **Single static binary, no daemon, no Kafka** | ✓ | requires Kafka + connector | managed service | SaaS | ✓ | ✓ |
| **Open-source** | Apache 2.0 | Apache 2.0 | proprietary | proprietary | BSD | proprietary |
| **Per-row pricing** | none | none | per-DMS-instance | per-MAR | none | per-source |

**Sluice's posture vs. each:**

- **vs. `mysqldump` / `pg_dump`:** Same-engine and snapshot-only. sluice handles cross-engine, schema translation, and continuous CDC.
- **vs. AWS DMS / GCP DataStream:** sluice is a single binary you run anywhere — no managed service, no cloud account, no per-row billing. The tradeoff: you bring your own monitoring.
- **vs. Debezium + Kafka + sink connector:** sluice covers the source-to-target path directly without Kafka in the middle. Useful when you don't already have a Kafka deployment to leverage and don't want to stand one up just for replication.
- **vs. Fivetran / Stitch / Airbyte:** sluice is open-source, single-binary, no SaaS dependency, no row-count billing. Operators run it where they want — on-prem, air-gapped, behind a private VPC.
- **vs. pgcopydb:** Same-engine PG → PG only; excellent for that case and an explicit tactical reference for sluice's bulk-copy fast path. sluice generalizes the same parallel-COPY + deferred-index pattern across engines.
- **vs. HVR / Striim / Qlik Replicate:** the enterprise tools are the category sluice positions against — multi-engine CDC with schema evolution, slot-health, cutover. sluice ships the same operator-facing capabilities (F10 / F11 / F13 / F17) on the OSS tier, with an opinionated "refuse loudly on uncertainty" discipline the paid tools tend to soften in favor of auto-remediation.

---

## When NOT to use sluice

Calling out the gaps explicitly so operators don't waste a discovery cycle:

- **Heroku Postgres as a source.** Heroku Postgres does not grant the `REPLICATION` role attribute, does not allow `CREATE_REPLICATION_SLOT`, and does not expose logical replication to customers ([Heroku Help — third-party replication](https://help.heroku.com/E10ZZ6IJ/why-can-t-i-use-third-party-tools-to-replicate-my-heroku-postgres-database-to-a-non-heroku-database), [Heroku Help — logical replication](https://help.heroku.com/TVS8OHTR/does-heroku-postgres-support-logical-replication)). sluice's PG source path requires all three. For Heroku migrations today, use trigger-based tools like [Bucardo](https://bucardo.org/) or PlanetScale's [heroku-migrator](https://github.com/planetscale/heroku-migrator). A `postgres-trigger` engine variant for sluice is on the roadmap (Go-based alternative to Perl-based Bucardo) but not in the current release.
- **One-off snapshot, same engine, no CDC catch-up needed.** Just use `pg_dump` / `mysqldump`. sluice's value is the schema translation and the continuous-sync lifecycle; for trivial same-engine snapshots, the native tools are simpler.
- **Logical decoding to applications.** sluice writes to a target database, not a Kafka topic or application stream. If you want raw decode events going to your own consumer, Debezium + Kafka is the right shape.
- **Schema migration tooling.** sluice translates schemas between engines as part of a data migration, but it's not a versioned-migration tool like Atlas, Flyway, or Bytebase. Use those for application-driven schema evolution; use sluice when the goal is moving data between systems.

---

## What to do when something looks wrong

| Symptom | First-look |
|---|---|
| `sluice migrate` failed mid-phase | Re-run with `--resume` (per-table-progress checkpointing, ADR-0018). State row in `sluice_migrate_state` survives the crash. |
| `sluice sync start` won't resume — slot lost / WAL gone | Per [`docs/postgres-source-prep.md`](docs/postgres-source-prep.md). Recovery: `sluice slot drop --source ...`, then `sync start --reset-target-data` to redo from scratch. |
| `sluice verify` reports row-count mismatch | The target has drifted from source. Investigate via `--format json` for delta-per-table, then run `sluice schema diff` to confirm structural drift didn't also happen. Re-run `migrate` (with `--reset-target-data` if necessary) or fix source-side data. |
| `sluice sync health --max-stale-seconds N` exits 1 | Stream stopped or fell behind. Check `sluice sync status` for the position; check the source-side CDC reader (PG `pg_stat_replication`, MySQL `SHOW REPLICA STATUS`); if PlanetScale-MySQL, see [`docs/vitess-vstream-troubleshooting.md`](docs/vitess-vstream-troubleshooting.md). |
| `schema diff` reports drift after migrate | Either sluice didn't translate something cleanly (see ADR-0016 + `--expr-override`), or the target is being modified outside sluice's scope. Each diff entry has a copy-paste DDL suggestion; run them at your discretion. |
| F13 emits `slot retention ≥85 %` WARN | Consumer (sluice or otherwise) is falling behind. Check `sluice sync status` first; if sluice is the consumer and is healthy, the source side has more writes than the consumer can drain — see [`docs/throughput-tuning.md`](docs/throughput-tuning.md). |
| Cross-engine translation surfaces a bug | File against the issue tracker, or use the [sluice-testing](https://github.com/orware/sluice-testing) companion repo's `BUG-CATALOG.md`. Workaround in the meantime: `--expr-override TABLE.COLUMN=EXPRESSION` or `--type-override TABLE.COLUMN=TYPE`. |

---

## Project state

**Pre-1.0** (`v0.83.x` series at time of writing). sluice has shipped 150+ tagged releases across the v0.x line. The feature surface is settled enough that the recent v0.79 → v0.83 arc was all severity-a operator-pain features (F10 / F11 / F13 / F17) rather than core-engine work. **No known production users today.**

This is a deliberate posture, not an accident:

- Same-engine and cross-engine integration tests run against real MySQL + Postgres containers on every PR.
- Every silent-loss class caught has a permanent class-pin test (not a representative-pin), per the Bug 74 lesson.
- Every recognized failure mode has a structured refuse-loudly message with an operator-action recovery hint.
- Versioning follows [SemVer](https://semver.org/). v0.x minor releases may include opt-in behavior changes; the API and CLI surface are still settling. v1.0.0 will mark the API-frozen line, but no timeline is committed.

If you're evaluating sluice for a real workload, the suggested path is:

1. Read [`docs/architecture.md`](docs/architecture.md) for what's in scope and what isn't.
2. Walk through [`docs/examples/quickstart.md`](docs/examples/quickstart.md) against containers (~10 min).
3. Run `sluice schema preview` + `sluice schema diff` against your real source. The translator-coverage gaps are operator-visible at this step, before you commit to a migration.
4. Run `sluice migrate --dry-run` against a non-production target.
5. For continuous sync: prep the source per [`docs/postgres-source-prep.md`](docs/postgres-source-prep.md) (PG) or the MySQL binlog section of [`docs/architecture.md`](docs/architecture.md), then drive a `sync start` against a non-production target with `--source-heartbeat-interval=30s` and the slot-health probe (automatic) for telemetry.

For sluice-the-project's perspective on its own maturity, see the "Tenets" and "State of play" sections of [`CLAUDE.md`](CLAUDE.md).

---

## CLI

```
$ sluice --help
Open-source database migration and continuous-sync tool.

Usage: sluice <command> [flags]

Commands:
  engines                  List registered database engines.
  migrate                  Run a one-time schema + data migration.
  sync start               Start a continuous-sync stream.
  sync status              Show status of a running sync stream.
  sync stop                Gracefully drain and stop a running stream.
  sync health              Probe a stream's freshness; cron-friendly exit code.
  cutover                  Prime target sequences from source (post-snapshot, pre-traffic-switch).
  schema preview           Print the target DDL sluice would emit.
  schema diff              Diff a target against what sluice would produce.
  verify                   Compare row counts (and forthcoming sampled / full content checks) between source and target.
  slot list / slot drop    Manage Postgres replication slots.
  diagnose                 Bundle source/target capability + role state for operator handoff.
```

Run `sluice <command> --help` for per-command flags. DSNs can also be passed via `SLUICE_SOURCE` / `SLUICE_TARGET` env vars.

---

## Terminology

A few terms recur in the codebase and docs:

- **IR** — the **internal representation**, sluice's typed dialect-neutral schema + value model in `internal/ir`. Every cross-engine translation passes through it: source-engine readers populate the IR, target-engine writers consume it. The IR is the only shared contract between engines, which keeps source-specific knowledge out of writers and target-specific knowledge out of readers. See [`docs/architecture.md`](docs/architecture.md) for the longer story.
- **Engine** — a registered database integration (`mysql`, `postgres`, `planetscale`). Each engine implements the same interface set (`SchemaReader`, `SchemaWriter`, `RowReader`, `RowWriter`, `CDCReader`, `ChangeApplier`, plus optional surfaces like `SlotHealthReporter`, `HeartbeatWriter`, `SequencePrimer`); the orchestrator never imports an engine package directly.
- **Stream** — a continuous-sync flow with persisted position, identified by a `--stream-id`. Distinct from a one-shot `migrate` which doesn't persist resume state on the target.
- **Shape** — the classification of a CDC-observed source DDL (`ShapeKindAddColumn`, `ShapeKindRenameColumn`, `ShapeKindUnrecognized`, …) that drives whether the streamer auto-forwards, coordinates live, or refuses loudly. See ADR-0054 / ADR-0058.
- **Refuse loudly** — sluice's posture when the safe forward path is ambiguous or the operator-action recovery hint matters more than continuing. Always emits a structured message naming the offending object and the recovery path.

---

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — IR, engine pattern, orchestrator, what sluice is and isn't
- [`docs/comparison.md`](docs/comparison.md) — sluice vs. Debezium / AWS DMS / Fivetran / pgcopydb / HVR / Striim / Qlik (deep dive)
- [`docs/comparison-bucardo.md`](docs/comparison-bucardo.md) — sluice vs. Bucardo (the canonical open-source PG → PG comparison; honest measured numbers, where each tool wins)
- [`docs/use-cases.md`](docs/use-cases.md) — operator-persona-by-persona breakdown of "which sluice surface do I need?"
- [`docs/cookbook/`](docs/cookbook/) — task-shaped recipes: one-shot migrate, bidirectional cutover, Heroku-style slot-less migration, encrypted backup chains, PII redaction, PostGIS round-trip, GitLab-shape case study, and the `pg_dump` comparison
- [`docs/translator-catalog.md`](docs/translator-catalog.md) — consolidated cross-engine expression translator reference: shipped translations + deferred rules + escape hatches
- [`docs/adr/README.md`](docs/adr/README.md) — index of all 70+ ADRs with one-line summary per decision
- [`docs/managed-services.md`](docs/managed-services.md) — PlanetScale-specific notes, operator preconditions
- [`docs/postgres-source-prep.md`](docs/postgres-source-prep.md) — required PG GUCs, slot lifecycle, failover-survival mechanisms
- [`docs/vitess-vstream-troubleshooting.md`](docs/vitess-vstream-troubleshooting.md) — operator runbook for PlanetScale-MySQL VStream lag (throttler, replication lag, deploy requests)
- [`docs/throughput-tuning.md`](docs/throughput-tuning.md) — knobs that matter at scale
- [`docs/redaction.md`](docs/redaction.md) — PII redaction operator guide: 27 strategies, determinism contracts, dictionary loader
- [`docs/snapshot-cdc-handoff.md`](docs/snapshot-cdc-handoff.md) — operator reference for the cold-start → CDC handoff
- [`docs/schema-change-runbook.md`](docs/schema-change-runbook.md) — `ADD COLUMN` / `DROP COLUMN` / `MODIFY` against a running stream
- [`docs/type-mapping.md`](docs/type-mapping.md), [`docs/value-types.md`](docs/value-types.md) — type translation policies and runtime row contract
- [`docs/testing.md`](docs/testing.md) — testing strategy, the Bug 74 class-pin lesson
- [`docs/adr/`](docs/adr/) — Architecture Decision Records (ADR-0001 through ADR-0062)
- [`docs/dev/`](docs/dev/) — local development setup, roadmap, design proto-ADRs
- [`docs/examples/`](docs/examples/) — runnable quickstart, sample `sluice.yaml` config

## Recent releases

The severity-a feature arc lives in:

- [v0.83.0](https://github.com/orware/sluice/releases/tag/v0.83.0) — F10: cutover sequence priming
- [v0.82.0](https://github.com/orware/sluice/releases/tag/v0.82.0) — F17: source-side heartbeat writer
- [v0.81.0](https://github.com/orware/sluice/releases/tag/v0.81.0) — F11: schema-drift diff in refuse messages
- [v0.80.0](https://github.com/orware/sluice/releases/tag/v0.80.0) — F13: pre-emptive PG slot-health warnings
- [v0.79.0](https://github.com/orware/sluice/releases/tag/v0.79.0) — Online `ADD COLUMN` forwarding through CDC apply

Full history: [CHANGELOG.md](CHANGELOG.md).

---

## Why "sluice"

A sluice is a gate that controls the flow of water through a canal — it doesn't generate the flow, it regulates and directs it. That's the posture this tool takes toward your data: it doesn't transform what's flowing, it manages how, when, and where it moves.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Inspiration

- [PlanetScale's pgcopydb fork](https://github.com/planetscale/pgcopydb) — reference for fast PG → PG bulk copy. Tactics borrowed: parallel `COPY` per table, deferred index / constraint creation, snapshot-based consistency.
- [pscale dumper](https://github.com/planetscale/pscale) — battle-tested batch sizes and session variables for high-throughput PlanetScale MySQL reads.
- [Vitess](https://vitess.io/) — VStream gRPC protocol for PlanetScale MySQL CDC.
- The category-defining commercial CDC tools — HVR (now Fivetran HVR), Striim, Qlik Replicate — for setting the operator-feature bar sluice positions against on the OSS tier.
