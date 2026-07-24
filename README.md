<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="branding/sluice-logo-dark.png">
  <img alt="sluice" src="branding/sluice-logo.png" width="340">
</picture>

### Open-source enterprise-class CDC for MySQL&nbsp;↔&nbsp;Postgres — plus SQLite & Cloudflare D1

[**Website**](https://sluicesync.com) · [**Documentation**](https://sluicesync.com/docs/) · [**Releases**](https://github.com/sluicesync/sluice/releases/latest)

</div>

Continuous sync between MySQL and Postgres in all four directions, with the schema-evolution, cutover-priming, and slot-health capabilities usually found only in commercial/enterprise CDC tools. Initial snapshot, CDC catch-up, and operator-driven cutover in one tool, opinionated about correctness. SQLite files and live Cloudflare D1 databases also migrate into Postgres or MySQL (with trigger-based continuous sync), and SQLite is a migrate target. Flat files migrate too: CSV / TSV / NDJSON files and mydumper / `pscale database dump` directories are first-class migrate sources — no live source database needed.

- 🔄 **Bidirectional** — MySQL → Postgres, Postgres → MySQL, same-engine in both directions, MariaDB and the PlanetScale flavors included
- 🗃️ **SQLite & Cloudflare D1** — import a SQLite file or a `wrangler d1 export` `.sql` dump (`--source-driver sqlite`), or read a live D1 over its HTTP query API (`--source-driver d1`) into Postgres / MySQL; big integers above 2⁵³ round-trip exactly (no JS 52-bit rounding, [ADR-0132](docs/adr/adr-0132-d1-query-api-reader.md)). SQLite is also a migrate **target** (`--target-driver sqlite`, decimals stored byte-exact as TEXT), and `sqlite-trigger` / `d1-trigger` add trigger-based continuous CDC ([ADR-0135](docs/adr/adr-0135-sqlite-trigger-cdc.md) / [ADR-0136](docs/adr/adr-0136-d1-trigger-cdc.md))
- 📄 **Flat-file sources** — migrate straight from a CSV / TSV / NDJSON file (`--source-driver csv|tsv|ndjson`, staged with validated type inference, NULL conventions declared not sniffed — [ADR-0163](docs/adr/adr-0163-flatfile-csv-tsv-ndjson-sources.md)) or a mydumper / `pscale database dump` directory (`--source-driver mydumper`, [ADR-0161](docs/adr/adr-0161-mydumper-source-engine.md)) into any target
- 🧬 **Online schema changes** — `sluice expand-contract` drives the full expand→migrate→contract pattern on PlanetScale via deploy requests ([ADR-0162](docs/adr/adr-0162-planetscale-expand-contract-orchestration.md)); `sluice backfill` is the resumable, keyset-chunked, online-safe in-place data migration for MySQL-family and Postgres ([ADR-0159](docs/adr/adr-0159-standalone-backfill-command.md)); `sluice deploy-ddl` ships one verbatim DDL statement through the governed deploy-request channel ([ADR-0165](docs/adr/adr-0165-deploy-ddl-and-control-table-bootstrap.md))
- 📊 **Analytics export** — `sluice backup export-as-parquet` transcodes any engine's backup chain into one zstd Parquet file per table + an export manifest, ready for DuckDB / warehouse ingestion ([ADR-0164](docs/adr/adr-0164-backup-export-as-parquet.md))
- 🔌 **Slot-less Postgres sources** — managed Postgres that blocks logical replication (e.g. Heroku Postgres) still streams via a trigger-based CDC engine (`--source-driver=postgres-trigger`) — no replication slot or `REPLICATION` role required
- 🪶 **Schema evolution** — unambiguous source DDL (`ADD`/`DROP COLUMN`, type/nullability `ALTER`s, index and `CHECK` changes) forwards automatically through the live CDC apply path (`--schema-changes=forward`, the default); ambiguous shapes (`RENAME`, volatile `DEFAULT`s) refuse loudly with a structured drift diff naming the column that changed
- 🩺 **Operational telemetry** — pre-emptive PG slot-health warnings (70 % / 85 % retention + 30 min inactivity); source-side heartbeat writer keeps slots alive against quiet sources; `sync start --metrics-listen ADDR` serves Prometheus `/metrics` + a `/readyz` readiness probe (200 once the stream enters its apply phase) for k8s / load-balancer health checks
- 🔁 **Cutover** — one-command sequence priming (`sluice cutover`) prevents PK collisions on the first post-cutover `INSERT`
- 🛑 **Loud failure by default** — every silent-loss class we have caught has a structured refuse-loudly message with an operator-action recovery hint. Paste into Slack and the on-call DBA knows what to fix.

Apache 2.0, single static binary, no daemon, no SaaS dependency. Install via Homebrew, Scoop, WinGet, a `.deb`/`.rpm`, `go install`, or the container image — see **[Installation](#installation)**.

---

## Installation

| Platform | Command |
|----------|---------|
| **macOS / Linux** — Homebrew | `brew install sluicesync/tap/sluice` |
| **Windows** — Scoop | `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket`<br>`scoop install sluice` |
| **Windows** — WinGet | `winget install sluicesync.sluice` &nbsp;¹ |
| **Debian / Ubuntu** | grab the `.deb` from the [latest release](https://github.com/sluicesync/sluice/releases/latest) → `sudo dpkg -i sluice_*_linux_amd64.deb` |
| **RHEL / Fedora** | grab the `.rpm` → `sudo rpm -i sluice_*_linux_amd64.rpm` |
| **Go** | `go install sluicesync.dev/sluice/cmd/sluice@latest` |
| **Container** | `docker pull ghcr.io/sluicesync/sluice:latest` |

Self-contained binaries (Linux / macOS / Windows × amd64 / arm64) plus `.deb` / `.rpm` / `.apk` packages are attached to [every release](https://github.com/sluicesync/sluice/releases/latest).

<sub>¹ WinGet availability follows acceptance into the [microsoft/winget-pkgs](https://github.com/microsoft/winget-pkgs) community repo, which is submitted per release.</sub>

---

## Quick start

```bash
# Install
go install sluicesync.dev/sluice/cmd/sluice@latest

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

# Import a SQLite file (or a `wrangler d1 export` .sql dump) → Postgres
sluice migrate \
    --source-driver sqlite   --source ./app.db \
    --target-driver postgres --target 'postgres://...?sslmode=disable'

# Import a LIVE Cloudflare D1 → Postgres (token via CLOUDFLARE_API_TOKEN)
sluice migrate \
    --source-driver d1       --source 'd1://<account_id>/<database_id>' \
    --target-driver postgres --target 'postgres://...?sslmode=disable'
```

SQLite / D1 import (file, `.sql` dump, live query API), the lossless big-integer path, and the `sqlite-trigger` / `d1-trigger` continuous-CDC engines are covered in [`docs/operator/sqlite-d1-import.md`](docs/operator/sqlite-d1-import.md). Migrating many MySQL databases or PG schemas in one run is covered in [`docs/operator/multi-database-multi-schema.md`](docs/operator/multi-database-multi-schema.md).

A 10-minute walkthrough against real MySQL 8.0 + Postgres 16 containers, loading the Sakila sample database, lives at [`docs/examples/quickstart.md`](docs/examples/quickstart.md).

---

## What sluice does

sluice is built around three product surfaces, each independently runnable:

| You want to… | Run |
|---|---|
| Move data **once** between MySQL and Postgres, then stop | `sluice migrate` |
| Import a **SQLite file / `.sql` dump / live Cloudflare D1** into Postgres or MySQL | `sluice migrate --source-driver sqlite\|d1` |
| Migrate a **CSV / TSV / NDJSON file** or a **mydumper / `pscale database dump` directory** — no live source database | `sluice migrate --source-driver csv\|tsv\|ndjson\|mydumper` |
| Emit a **SQLite `.db`** from any source (e.g. for `wrangler d1 import`) | `sluice migrate --target-driver sqlite` |
| **Export a backup chain as Parquet** for DuckDB / warehouse analytics | `sluice backup export-as-parquet` |
| Run an **online schema change** (expand→migrate→contract) on PlanetScale | `sluice expand-contract` |
| **Backfill / transform a column in place** — resumable, keyset-chunked, online-safe | `sluice backfill` |
| Ship **one DDL statement** to a PlanetScale safe-migrations branch via a deploy request | `sluice deploy-ddl` (bootstrap: `sluice control-tables ddl`) |
| Move data **once** with low downtime — snapshot + CDC catch-up + cutover | `sluice sync start` (cold-start snapshot → CDC) → `sluice cutover` |
| **Replicate continuously** for analytics, geo-locality, or hot-standby | `sluice sync start` |
| **Preview** the target DDL before running anything | `sluice schema preview` |
| **Diff** a target against what sluice would produce | `sluice schema diff` |
| **Verify** that every row made it across | `sluice verify` |
| **Probe** a running sync's freshness against a staleness threshold | `sluice sync health` |
| Do all of the above against **PlanetScale** | Same commands; select `--source-driver planetscale` for VStream CDC. A `*.connect.psdb.cloud` host under `--source-driver mysql` also works — via binlog CDC — with the Vitess `_vt_*` shadow tables auto-excluded |

### The four enterprise-class features that landed in v0.80.0 – v0.83.0

These are the operator-pain features Reddit's `/r/PostgreSQL`, `/r/mysql`, and `/r/dataengineering` keep flagging as the reason teams reach for paid CDC tooling. Each one closed a catalogued silent-loss or silent-under-information class.

| Feature | Shipped in | What it does |
|---|---|---|
| **F13 — Pre-emptive PG slot-health warnings** | [v0.80.0](https://github.com/sluicesync/sluice/releases/tag/v0.80.0) | A 30-second background probe per PG-sourced stream emits structured WARNs when `pg_replication_slots` retention crosses 70 % / 85 % of `max_slot_wal_keep_size`, or when a slot has been inactive for ≥30 min. De-duplicates within a 5-min window; severity transitions and clears emit immediately. Surfaces the slow burn *before* Postgres silently evicts the slot. ([ADR-0059](docs/adr/adr-0059-pg-slot-health-prewarning.md)) |
| **F11 — Per-table schema-drift diff in refuse messages** | [v0.81.0](https://github.com/sluicesync/sluice/releases/tag/v0.81.0) | When a non-`ADD COLUMN` source DDL arrives over CDC, the refusal now names the specific columns, indexes, and constraints that drifted plus an operator-action hint per category (`[column-added] foo TIMESTAMP NULL — drained schema migrate ...`). Greppable prefixes for Slack / ticket workflows. Pre-F11, operators ran `pg_dump`-diff by hand to find out *what* changed. ([ADR-0060](docs/adr/adr-0060-cdc-schema-drift-diff.md)) |
| **F17 — Source-side heartbeat writer** | [v0.82.0](https://github.com/sluicesync/sluice/releases/tag/v0.82.0) | Optionally writes a tiny periodic row to a sluice-owned table on the source. The `INSERT` generates WAL / binlog so the consumer's position advances even against a quiet source, preventing silent slot eviction / binlog rotation past the consumer on low-traffic sources. Default-off; opt in with `--source-heartbeat-interval=30s`. Pairs with F13: F13 detects the symptom, F17 prevents the cause. ([ADR-0061](docs/adr/adr-0061-source-side-heartbeat-writer.md)) |
| **F10 — Cutover sequence priming** | [v0.83.0](https://github.com/sluicesync/sluice/releases/tag/v0.83.0) | `sluice cutover` reads source PG sequences (`pg_sequences.last_value`) / MySQL `AUTO_INCREMENT` values and bumps the target by `--cutover-sequence-margin=N` (default 1000). Closes the PK-collision-on-first-post-cutover-`INSERT` class. Idempotent; refuses loudly when target value is already above the safety margin (signal that traffic landed before cutover priming ran). Skips composite-PK / UUID / no-sequence tables gracefully. ([ADR-0062](docs/adr/adr-0062-cutover-sequence-priming.md)) |

Since that arc, the **v0.84 → v0.99 releases** widened the surface well beyond those four: encrypted logical backups with incremental chains, point-in-time restore, and a continuous-backup broker; PII redaction (26 strategies); the slot-less `postgres-trigger` CDC engine; PG Row-Level Security capture/emit; PostGIS geometry round-trips; multi-source aggregation; multi-database fan-out; connection-resilience tuning; the bulk-copy throughput arc (cross-table worker pool, index-build overlap on both engines, PG→PG raw `COPY` passthrough, fast `sync` cold-start); and most recently the flat-file source engines, the Parquet analytics export, and the online schema-change command family (`backfill` / `expand-contract` / `deploy-ddl`). See [Recent releases](#recent-releases) and the [CHANGELOG](CHANGELOG.md).

### Engines and directions

| Source ↘ Target → | MySQL | MariaDB | PostgreSQL | PlanetScale MySQL | PlanetScale PG |
|---|---|---|---|---|---|
| **MySQL** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **MariaDB** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **PostgreSQL** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **PlanetScale MySQL** | ✓ (VStream CDC) | ✓ (VStream CDC) | ✓ (VStream CDC) | ✓ | ✓ |
| **PlanetScale PG** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **CSV / TSV / NDJSON file** | ✓ (migrate) | ✓ (migrate) | ✓ (migrate) | ✓ (migrate) | ✓ (migrate) |
| **mydumper / `pscale database dump` dir** | ✓ (migrate) | ✓ (migrate) | ✓ (migrate) | ✓ (migrate) | ✓ (migrate) |

The long-form version of this matrix — CDC modes and source requirements per engine, the known-limitations list, and a pre-production checklist — lives in [`docs/production-readiness.md`](docs/production-readiness.md).

The flat-file rows are migrate **sources** only (no CDC — a file doesn't change); SQLite / D1 have their own table below. There is also an analytics **exit** direction: `sluice backup export-as-parquet` transcodes any engine's backup chain — whatever the source engine was — into Parquet files for DuckDB / warehouse ingestion ([ADR-0164](docs/adr/adr-0164-backup-export-as-parquet.md), recipe in [`docs/cookbook/duckdb-on-sluice-backups.md`](docs/cookbook/duckdb-on-sluice-backups.md)).

Cross-engine type translation handles the common surfaces (PG `UUID` / `INET` / `MACADDR` / `ARRAY` ↔ MySQL `CHAR(36)` / `VARCHAR` / `JSON`; MySQL `TINYINT(1)` ↔ PG `BOOLEAN`; MySQL `ENUM` / `SET` → PG enum / `TEXT[] + CHECK`; PostGIS `GEOMETRY` round-trips with SRID; many idioms in generated columns and `CHECK` constraints translate automatically — see [`docs/dev/translator-coverage.md`](docs/dev/translator-coverage.md)). When the default doesn't fit, `--type-override TABLE.COLUMN=TYPE` and `--expr-override TABLE.COLUMN=EXPR` cover one-off cases without writing a config file.

### Same-engine copies take a faster path

Cross-engine work flows through sluice's typed IR, where type translation, redaction, and value-fidelity checks live — every value decoded and re-encoded. On a *same-engine, no-transform* Postgres→Postgres copy there's nothing to translate, so sluice skips the round trip entirely and byte-pipes the native `COPY` stream source-to-target (the pgcopydb tactic, composed with parallel chunking — [ADR-0078](docs/adr/adr-0078-pg-pg-identity-passthrough-raw-copy.md)); MySQL→MySQL stays on the IR but writes through the native `LOAD DATA` loader with no translation to perform. This is *the same fidelity, less work* — not a more-exact copy. The Postgres fast lane engages only when a single auditable gate proves there's no transform to skip: add `--redact` / `--type-override` / a shard column, or hit an OID-sensitive type, and it falls back to the IR path automatically, per table.

### SQLite & Cloudflare D1

| Engine name | Role | Notes |
|---|---|---|
| `sqlite` | **source** (file or `.sql` dump) **and target** | Pure-Go `modernc.org/sqlite`, no CGO. As a source it imports a binary `.db` or a `wrangler d1 export` `.sql` dump (auto-detected) into Postgres / MySQL; as a target (`--target-driver sqlite`) it emits a `.db` from any source, decimals stored byte-exact as TEXT. Migrate only (no CDC). |
| `d1` | **source** (live, lossless) | Reads a live Cloudflare D1 over its HTTP query API (`--source-driver d1`, token via `CLOUDFLARE_API_TOKEN`); per-column `typeof()` + `CAST(... AS TEXT)` / `hex()` projection makes integers above 2⁵³ and BLOBs round-trip exactly, and reads don't take D1 offline ([ADR-0132](docs/adr/adr-0132-d1-query-api-reader.md)). |
| `sqlite-trigger` | **CDC source** | Trigger-based continuous sync from a local SQLite file: per-table AFTER triggers + a `sluice_change_log` watermark for exactly-once resume ([ADR-0135](docs/adr/adr-0135-sqlite-trigger-cdc.md)). |
| `d1-trigger` | **CDC source** | The same trigger-CDC design over a live D1's HTTP query API ([ADR-0136](docs/adr/adr-0136-d1-trigger-cdc.md)). |

SQLite / D1 are migrate **sources** into Postgres or MySQL, and SQLite is also a migrate **target**; D1 is not a target (use a `.db` SQLite target, then `wrangler d1 import`). Declared `DATE` / `DATETIME` / `BOOL` columns and the ambiguous value encoding are governed by `--sqlite-date-encoding` (`iso` default / `unixepoch` / `unixmillis` / `julian`), refusing loudly on a storage-class mismatch. Full operator walkthrough: [`docs/operator/sqlite-d1-import.md`](docs/operator/sqlite-d1-import.md).

### Flat files: CSV, TSV, NDJSON & mydumper dumps

| Engine name | Role | Notes |
|---|---|---|
| `csv` / `tsv` | **source** (migrate only) | Schema-less flat files staged into a temp SQLite database with validated type inference auto-engaged ([ADR-0163](docs/adr/adr-0163-flatfile-csv-tsv-ndjson-sources.md)). Producer conventions are **declared, never sniffed**: `--csv-null` names the unquoted text meaning SQL NULL, `--csv-header` / `--csv-no-header` declare header presence — undeclared ambiguity refuses loudly. `--csv-delimiter` customises `csv`; `tsv` is the same engine with TAB fixed. |
| `ndjson` | **source** (migrate only) | Newline-delimited JSON. Numbers are carried as raw source text end-to-end — never through a float64 — so integers above 2⁵³ and arbitrary-precision decimals land exact (the D1 lesson). Duplicate keys refuse loudly. |
| `mydumper` | **source** (migrate only) | A mydumper or `pscale database dump` output **directory** — PlanetScale's exporter writes the same format, so this is the provider-export and air-gapped migration path ([ADR-0161](docs/adr/adr-0161-mydumper-source-engine.md)). Values decode through the live MySQL engine's own decoder, ground-truthed against real dumps per value family; the dump's recorded row counts are cross-checked after every table read. |

All four are migrate sources into any target; `sluice verify --depth count` works against them (re-scan). Operator guide: [`docs/operator/flat-file-sources.md`](docs/operator/flat-file-sources.md).

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

[`internal/ir`](internal/ir) defines a typed dialect-neutral schema and value model plus the `Engine`, `SchemaReader`, `SchemaWriter`, `RowReader`, `RowWriter`, `CDCReader`, `ChangeApplier` interfaces. Each engine package (`internal/engines/mysql`, `internal/engines/postgres`, `internal/engines/sqlite`, the flat-file engines `internal/engines/{flatfile,mydumper}`, the trigger-CDC engines under `internal/engines/{pgtrigger,sqlite-trigger,d1-trigger}`) implements those interfaces and self-registers via `init()` — fourteen registered engines today (`sluice engines` lists them): `mysql`, `mariadb`, `planetscale`, `vitess`, `postgres`, `sqlite`, `d1`, `csv`, `tsv`, `ndjson`, `mydumper`, `postgres-trigger`, `sqlite-trigger`, `d1-trigger`. `internal/pipeline.Migrator` is the simple-mode orchestrator: read source schema → optional dry-run plan → create target tables (no constraints) → bulk-copy rows → create indexes → create constraints. `cmd/sluice` is a [kong](https://github.com/alecthomas/kong)-based CLI; config loading is via [koanf](https://github.com/knadh/koanf) YAML + env. Engines are looked up by name from `engines.Get(...)`; the pipeline package never imports specific engine packages. MySQL has flavors (Vanilla, MariaDB, PlanetScale, Vitess) — same engine code, different `Capabilities` declarations, registered under different names. Additional engines slot in without touching the orchestrator.

The longer story lives in [`docs/architecture.md`](docs/architecture.md).

---

## Where sluice fits (vs. alternatives)

This is the category claim: sluice is the open-source tool whose feature set most directly overlaps the commercial/enterprise CDC products operators reach for when they need schema evolution, cutover priming, or slot health. The table below maps the feature surface; it's a capability comparison, not a verdict on any one tool.

|  | sluice | Debezium | AWS DMS | Fivetran | pgcopydb | HVR (commercial) |
|---|---|---|---|---|---|---|
| **Cross-engine MySQL ↔ Postgres** | ✓ all 4 directions | requires sink connector | ✓ | ✓ | PG → PG only | ✓ |
| **`ADD COLUMN` auto-forwards** | ✓ (opt-in, since v0.79.0) | ✓ via schema-history connector | partial | ✓ | ✗ (snapshot only) | ✓ |
| **Refuse-loudly on unsafe DDL with structured diff** | ✓ (F11, v0.81.0) | varies by connector | ✗ | ✗ | n/a | ✓ |
| **Pre-emptive slot-retention warnings** | ✓ (F13, v0.80.0) | ✗ (operator monitors `pg_stat_replication`) | ✗ | n/a (SaaS) | ✗ | ✓ |
| **Source-side heartbeat writer** | ✓ (F17, v0.82.0) | ✓ (PG only) | ✗ | n/a | ✗ | ✓ |
| **Cutover sequence priming as one command** | ✓ (F10, v0.83.0) | ✗ (manual `setval`) | ✗ | ✗ | partial | ✓ |
| **Inline PII redaction (bulk + CDC)** | ✓ (26 strategies) | partial (SMT masking) | partial (transform rules) | partial (column hashing/blocking) | ✗ | ✓ (agent transform) |
| **Slot-less CDC for locked-down Postgres** | ✓ (`postgres-trigger` engine) | ✗ (needs logical replication) | ✗ (needs logical replication) | partial (polling-based) | ✗ (snapshot only) | ✓ (trigger capture) |
| **Encrypted logical backups + broker replay** | ✓ (full + incremental chains) | ✗ | ✗ | ✗ | ✗ | ✗ |
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

- **One-off snapshot, same engine, no CDC catch-up needed.** Just use `pg_dump` / `mysqldump`. sluice's value is the schema translation and the continuous-sync lifecycle; for trivial same-engine snapshots, the native tools are simpler.
- **Logical decoding to applications.** sluice writes to a target database, not a Kafka topic or application stream. If you want raw decode events going to your own consumer, Debezium + Kafka is the right shape.
- **Versioned schema-migration tooling.** sluice translates schemas between engines as part of a data migration, and it now ships the *online execution* half of a schema change — `sluice expand-contract`, `sluice backfill`, `sluice deploy-ddl` (see [`docs/schema-change-runbook.md`](docs/schema-change-runbook.md)) — but it's not a versioned-migration tool like Atlas, Flyway, or Bytebase: no migration history, no multi-environment promotion, no down-migrations. Use those for application-driven schema versioning; use sluice's commands to execute a change safely against a live database.

---

## What to do when something looks wrong

| Symptom | First-look |
|---|---|
| Anything you can't explain — or want to hand to another human | `sluice diagnose --stream-id X --output bundle.zip` — a privacy-tiered ZIP of the stream's state tables, engine health probes, and capabilities, safe to attach to an issue (ADR-0056). Long-running commands can also auto-write one on error via `--diagnose-on-crash-dir` (off by default). |
| `sluice migrate` failed mid-phase | Re-run with `--resume` (per-table-progress checkpointing, ADR-0018). State row in `sluice_migrate_state` survives the crash. |
| `sluice sync start` won't resume — slot lost / WAL gone | Per [`docs/postgres-source-prep.md`](docs/postgres-source-prep.md). Recovery: `sluice slot drop --source ...`, then `sync start --reset-target-data` to redo from scratch. |
| `sluice verify` reports row-count mismatch | The target has drifted from source. Investigate via `--format json` for delta-per-table, then run `sluice schema diff` to confirm structural drift didn't also happen. Re-run `migrate` (with `--reset-target-data` if necessary) or fix source-side data. |
| `sluice sync health --max-stale-seconds N` exits 1 | Stream stopped or fell behind. Check `sluice sync status` for the position; check the source-side CDC reader (PG `pg_stat_replication`, MySQL `SHOW REPLICA STATUS`); if PlanetScale-MySQL, see [`docs/vitess-vstream-troubleshooting.md`](docs/vitess-vstream-troubleshooting.md). |
| `schema diff` reports drift after migrate | Either sluice didn't translate something cleanly (see ADR-0016 + `--expr-override`), or the target is being modified outside sluice's scope. Each diff entry has a copy-paste DDL suggestion; run them at your discretion. |
| F13 emits `slot retention ≥85 %` WARN | Consumer (sluice or otherwise) is falling behind. Check `sluice sync status` first; if sluice is the consumer and is healthy, the source side has more writes than the consumer can drain — see [`docs/throughput-tuning.md`](docs/throughput-tuning.md). |
| Cross-engine translation surfaces a bug | File against the issue tracker. Workaround in the meantime: `--expr-override TABLE.COLUMN=EXPRESSION` or `--type-override TABLE.COLUMN=TYPE`. |

---

## Project state

**Pre-1.0** (`v0.99.x` series at time of writing, 200+ tagged releases across the v0.x line). The v0.84 → v0.99 arc kept widening the capability surface — encrypted logical backups + restore + a continuous-backup broker, PII redaction, the slot-less `postgres-trigger` engine, PG Row-Level Security, PostGIS round-trips, multi-source aggregation, multi-database fan-out, connection-resilience tuning, and the bulk-copy throughput arc (cross-table pool + index overlap + raw PG→PG passthrough) — each landing with the same class-pin test discipline rather than a happy-path-only ship. **No known production users today.**

### Maturity, concretely

"No production users" and "production-usable" are both true, and here is how they reconcile. What *is* battle-tested — in the strict sense of being exercised against real systems, automatically, on every change:

- Cross-engine migrate + sync + backup run against real MySQL, Postgres, and Vitess containers with the race detector on **every PR** (six required checks); every silent-loss class ever caught is pinned by a per-family × per-shape matrix ground-truthed on real servers, per the Bug 74 lesson.
- sluice's own PG→PG and MySQL→MySQL output is diffed against `pg_dump` / `mysqldump` on every PR — every knowingly-uncarried feature is an allowlist entry citing the doc that declares it.
- Three full blind multi-agent audits of the codebase ran in July 2026 alone, each remediated to closure and each closing with a permanent CI gate for the class it found.
- The PlanetScale filtered-sync path was validated end-to-end against **real PlanetScale at 5M rows**, and a multi-week soak fleet runs continuous syncs against live managed databases between releases; tag publishes additionally require a real-Vitess-cluster gate.

What has *not* happened yet: a stranger running sluice against a production workload nobody wrote a test for first. That is the honest gap behind the framing above — and the reason the tool refuses loudly rather than guessing when it is uncertain. The full support matrix and known-limitations list live in [`docs/production-readiness.md`](docs/production-readiness.md).

This is a deliberate posture, not an accident:

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
  sync run                 Supervise a fleet of syncs from one process (ADR-0122).
  sync status              Show status of a running sync stream.
  sync stop                Gracefully drain and stop a running stream.
  sync decommission        Retire a finished stream: drop its slot + publication (source) and control row (target).
  sync health              Probe a stream's freshness; cron-friendly exit code.
  sync tui                 Live terminal dashboard for a running fleet (ADR-0125).
  sync from-backup run     Replay a backup chain into a target as a long-running broker.
  cutover                  Prime target sequences from source (post-snapshot, pre-traffic-switch).
  backup                   Take and verify encrypted logical backups (full + incremental chains).
  backup export-as-parquet Transcode a backup into one Parquet file per table (analytics exit, ADR-0164).
  restore                  Restore a logical backup chain into a target database.
  backfill                 Backfill/transform a column in place — keyset-chunked, resumable, online-safe (ADR-0159).
  expand-contract          Drive the full expand→migrate→contract pattern on PlanetScale (ADR-0162).
  deploy-ddl               Ship ONE verbatim DDL statement to a PlanetScale branch via a deploy request (ADR-0165).
  control-tables ddl       Print sluice's control-table CREATE statements for safe-migrations bootstrap.
  trigger setup            Install trigger-CDC state (postgres-trigger / sqlite-trigger / d1-trigger).
  trigger teardown         Remove every trace of the trigger engine from the source database.
  trigger prune            Reap durably-applied rows from a trigger change-log (ADR-0137).
  schema preview           Print the target DDL sluice would emit.
  schema diff              Diff a target against what sluice would produce.
  schema add-table         Bring a new source table into an active stream's scope.
  verify                   Compare row counts between source and target.
  matview refresh          Refresh PostgreSQL materialized views (PG-only).
  slot list / slot drop    Manage Postgres replication slots.
  diagnose                 Bundle source/target capability + role state for operator handoff.
  metrics-watch            Watch a PlanetScale DB's control-plane metrics + fire alerts (ADR-0107).
```

Run `sluice <command> --help` for per-command flags. DSNs can also be passed via `SLUICE_SOURCE` / `SLUICE_TARGET` env vars.

**Migrating many namespaces in one run.** A MySQL server's databases or a Postgres source's schemas can be moved together: `--all-databases` / `--all-schemas` fan every non-system namespace out to a same-named target namespace (auto-created), and `--include-database` / `--exclude-database` (or the PG-source synonyms `--include-schema` / `--exclude-schema`) scope the set. These work on both `migrate` and `sync start` (CDC included). See [`docs/operator/multi-database-multi-schema.md`](docs/operator/multi-database-multi-schema.md) and [ADR-0074](docs/adr/adr-0074-multi-database-mysql-migration-and-sync.md) / [ADR-0075](docs/adr/adr-0075-postgres-source-multi-schema-migration-and-sync.md).

**Copying only some rows.** A per-table `--where TABLE=<predicate>` (native source SQL, source-keyed) filters at the **row** level — the granularity below `--include-table` and `--redact`. On `migrate` the predicate is pushed down into the source read (one-shot filtered extract, full source SQL); on `sync` the same predicate scopes both the cold-start snapshot and the CDC leg, which evaluates a restricted grammar client-side with full row-move semantics (a row updated out of scope becomes a target DELETE, not a leak). Filtering a *parent* table refuses loudly on the orphaned FK unless you reconcile or pass `--allow-degraded-fks`. See [`docs/operator/filtered-subset-migration.md`](docs/operator/filtered-subset-migration.md) and [ADR-0173](docs/adr/adr-0173-row-level-where-filter.md).

`trigger setup` is no longer Postgres-only: `--source-driver` selects `postgres-trigger` (default), `sqlite-trigger` (a local SQLite file), or `d1-trigger` (a live Cloudflare D1).

---

## Terminology

A few terms recur in the codebase and docs:

- **IR** — the **internal representation**, sluice's typed dialect-neutral schema + value model in `internal/ir`. Every cross-engine translation passes through it: source-engine readers populate the IR, target-engine writers consume it. The IR is the only shared contract between engines, which keeps source-specific knowledge out of writers and target-specific knowledge out of readers. See [`docs/architecture.md`](docs/architecture.md) for the longer story.
- **Engine** — a registered database integration. Fourteen are registered today (run `sluice engines`): `mysql`, `mariadb`, `planetscale`, `vitess`, `postgres`, `sqlite`, `d1`, `csv`, `tsv`, `ndjson`, `mydumper`, `postgres-trigger`, `sqlite-trigger`, `d1-trigger`. Each engine implements the same interface set (`SchemaReader`, `SchemaWriter`, `RowReader`, `RowWriter`, `CDCReader`, `ChangeApplier`, plus optional surfaces like `SlotHealthReporter`, `HeartbeatWriter`, `SequencePrimer`); the orchestrator never imports an engine package directly.
- **Stream** — a continuous-sync flow with persisted position, identified by a `--stream-id`. Distinct from a one-shot `migrate` which doesn't persist resume state on the target.
- **Shape** — the classification of a CDC-observed source DDL (`ShapeKindAddColumn`, `ShapeKindRenameColumn`, `ShapeKindUnrecognized`, …) that drives whether the streamer auto-forwards, coordinates live, or refuses loudly. See ADR-0054 / ADR-0058.
- **Refuse loudly** — sluice's posture when the safe forward path is ambiguous or the operator-action recovery hint matters more than continuing. Always emits a structured message naming the offending object and the recovery path.

---

## Documentation

- [`docs/production-readiness.md`](docs/production-readiness.md) — the support matrix, CDC modes per source, **known limitations**, and the pre-production checklist — start here when evaluating sluice for a real workload
- [`docs/architecture.md`](docs/architecture.md) — IR, engine pattern, orchestrator, what sluice is and isn't
- [`docs/comparison.md`](docs/comparison.md) — sluice vs. Debezium / AWS DMS / Fivetran / pgcopydb / HVR / Striim / Qlik (deep dive)
- [`docs/comparison-bucardo.md`](docs/comparison-bucardo.md) — sluice vs. Bucardo (the canonical open-source PG → PG comparison; honest measured numbers, where each tool wins)
- [`docs/use-cases.md`](docs/use-cases.md) — operator-persona-by-persona breakdown of "which sluice surface do I need?"
- [`docs/cookbook/`](docs/cookbook/) — task-shaped recipes: one-shot migrate, bidirectional cutover, Heroku-style slot-less migration, encrypted backup chains, PII redaction, PostGIS round-trip, GitLab-shape case study, and the `pg_dump` comparison
- [`docs/translator-catalog.md`](docs/translator-catalog.md) — consolidated cross-engine expression translator reference: shipped translations + deferred rules + escape hatches
- [`docs/backup-format-versioning.md`](docs/backup-format-versioning.md) — backup manifest `FormatVersion` contract: proportional version-stamp, refuse-before-touch on older binaries, how older sluice doesn't silently drop RLS / EXCLUDE metadata (Bug 116 closure reference)
- [`docs/adr/README.md`](docs/adr/README.md) — index of all ADRs, one-line summary per decision
- [`docs/managed-services.md`](docs/managed-services.md) — PlanetScale-specific notes, operator preconditions
- [`docs/postgres-source-prep.md`](docs/postgres-source-prep.md) — required PG GUCs, slot lifecycle, failover-survival mechanisms
- [`docs/vitess-vstream-troubleshooting.md`](docs/vitess-vstream-troubleshooting.md) — operator runbook for PlanetScale-MySQL VStream lag (throttler, replication lag, deploy requests)
- [`docs/throughput-tuning.md`](docs/throughput-tuning.md) — knobs that matter at scale
- [`docs/redaction.md`](docs/redaction.md) — PII redaction operator guide: 26 strategies, determinism contracts, dictionary loader
- [`docs/operator/filtered-subset-migration.md`](docs/operator/filtered-subset-migration.md) — copying only the rows you want with a per-table `--where` predicate (`migrate` one-shot + `sync` continuous filtered replication), the FK-orphan caveat, and the CDC row-move semantics
- [`docs/snapshot-cdc-handoff.md`](docs/snapshot-cdc-handoff.md) — operator reference for the cold-start → CDC handoff
- [`docs/schema-change-runbook.md`](docs/schema-change-runbook.md) — the schema-change command family (`expand-contract`, `backfill`, `deploy-ddl`, `control-tables ddl`) plus coordinating DDL against a running stream
- [`docs/operator/sqlite-d1-import.md`](docs/operator/sqlite-d1-import.md) — importing SQLite files / `.sql` dumps / live Cloudflare D1 into Postgres or MySQL, the lossless big-integer path, and the SQLite/D1 trigger-CDC engines
- [`docs/operator/flat-file-sources.md`](docs/operator/flat-file-sources.md) — migrating CSV / TSV / NDJSON files and mydumper / `pscale database dump` directories, the declared-not-sniffed CSV conventions, and type inference
- [`docs/cookbook/duckdb-on-sluice-backups.md`](docs/cookbook/duckdb-on-sluice-backups.md) — querying `backup export-as-parquet` output with DuckDB (local + S3), footer metadata, GeoParquet
- [`docs/operator/multi-database-multi-schema.md`](docs/operator/multi-database-multi-schema.md) — migrating many MySQL databases / Postgres schemas in one run (`--all-databases` / `--all-schemas`), fan-IN consolidation, and the documented edges
- [`docs/operator/staged-wave-migration.md`](docs/operator/staged-wave-migration.md) — moving a database a few tables at a time: one growing stream (`schema add-table`) vs. independent per-wave streams, FK-driven wave ordering, per-wave cutover, and why write-back to the source isn't supported
- [`docs/type-mapping.md`](docs/type-mapping.md), [`docs/value-types.md`](docs/value-types.md) — type translation policies and runtime row contract
- [`docs/testing.md`](docs/testing.md) — testing strategy, the Bug 74 class-pin lesson
- [`docs/adr/`](docs/adr/) — Architecture Decision Records; the full index lives in [`docs/adr/README.md`](docs/adr/README.md)
- [`docs/dev/`](docs/dev/) — local development setup, roadmap, design proto-ADRs
- [`docs/examples/`](docs/examples/) — runnable quickstart, sample `sluice.yaml` config

## Recent releases

Selected highlights from the **v0.94 → v0.99** arc:

- **The online schema-change command family** (v0.99.244 → v0.99.258) — `sluice backfill` (resumable keyset-chunked in-place data migration with the `--verify` completion gate, [ADR-0159](docs/adr/adr-0159-standalone-backfill-command.md)), `sluice expand-contract` (the full expand→migrate→contract pattern on PlanetScale via deploy requests, live-validated, [ADR-0162](docs/adr/adr-0162-planetscale-expand-contract-orchestration.md)), `sluice deploy-ddl` + `sluice control-tables ddl` (the safe-migrations DDL channel and bootstrap, [ADR-0165](docs/adr/adr-0165-deploy-ddl-and-control-table-bootstrap.md)), the automatic deploy-request index-build fallback ([ADR-0148](docs/adr/adr-0148-planetscale-deploy-request-index-build.md)), and `migrate`'s pre-create shape gate so a deploy-request-bootstrapped schema feeds a fresh migrate end to end ([ADR-0166](docs/adr/adr-0166-migrate-precreate-shape-gate.md))
- **Flat-file source engines** (v0.99.247 → v0.99.250) — `--source-driver mydumper` reads a mydumper / `pscale database dump` directory ([ADR-0161](docs/adr/adr-0161-mydumper-source-engine.md)); `--source-driver csv|tsv|ndjson` migrates schema-less flat files with validated type inference and declared-not-sniffed NULL/header conventions ([ADR-0163](docs/adr/adr-0163-flatfile-csv-tsv-ndjson-sources.md))
- [v0.99.251](https://github.com/sluicesync/sluice/releases/tag/v0.99.251) — **`sluice backup export-as-parquet`**: the analytics exit surface — any engine's backup chain transcoded to zstd Parquet + an export manifest, faithful-or-loud type mapping, DuckDB cookbook ([ADR-0164](docs/adr/adr-0164-backup-export-as-parquet.md))
- [v0.99.246](https://github.com/sluicesync/sluice/releases/tag/v0.99.246) — Slot-health alerts page the notification sinks (webhook/Slack/SMTP) + the backup-chain concurrent-writer guard (ADR-0160)
- **SQLite & Cloudflare D1 engine family** (v0.99.141 → v0.99.148) — SQLite/D1 migrate source (file + `.sql` dump + lossless live-D1 query-API reader, [ADR-0128](docs/adr/adr-0128-sqlite-d1-migrate-source.md)/[0130](docs/adr/adr-0130-sqlite-sql-dump-ingest.md)/[0132](docs/adr/adr-0132-d1-query-api-reader.md)), generated/CHECK/expression-index carry ([ADR-0133](docs/adr/adr-0133-sqlite-schema-feature-detection.md)), within-table parallel chunking, SQLite as a migrate **target** (decimals byte-exact as TEXT, [ADR-0134](docs/adr/adr-0134-sqlite-target-engine.md)), and the `sqlite-trigger` / `d1-trigger` continuous-CDC engines ([ADR-0135](docs/adr/adr-0135-sqlite-trigger-cdc.md)/[0136](docs/adr/adr-0136-d1-trigger-cdc.md))
- **MySQL CDC apply-over-WAN coalescing** ([ADR-0139](docs/adr/adr-0139-mysql-multirow-insert-apply.md)/[0140](docs/adr/adr-0140-mysql-coalesce-update-delete-apply.md)) — consecutive same-shape INSERTs fold into one multi-row `INSERT … ON DUPLICATE KEY UPDATE`, UPDATEs apply as the same keyed upsert, and DELETEs coalesce into one `DELETE … WHERE pk IN (…)`, turning N round trips into one and lifting cross-region / PlanetScale-MySQL apply off the per-RTT floor; a rate-limited INFO line reports the rows-per-statement coalescing ratio
- [v0.99.30](https://github.com/sluicesync/sluice/releases/tag/v0.99.30) — Index-build overlap extended to MySQL targets (ADR-0080) + within-table chunking on the PG fast `sync` cold-start + the `SPATIAL`/`FULLTEXT` `Error 1089` fix
- [v0.99.29](https://github.com/sluicesync/sluice/releases/tag/v0.99.29) — The bulk-copy throughput arc: cross-table worker pool (`--table-parallelism`, ADR-0076), index-build overlap (ADR-0077), PG→PG raw `COPY` passthrough (ADR-0078), fast `sync` cold-start (ADR-0079)
- [v0.99.16](https://github.com/sluicesync/sluice/releases/tag/v0.99.16) — Multi-database fan-out (`--all-databases` / `--include-database`, migrate + sync) + the FTWRL binlog-snapshot boundary-gap silent-loss fix
- [v0.99.0](https://github.com/sluicesync/sluice/releases/tag/v0.99.0) — `sluicesync.dev/sluice` vanity module + first GHCR runtime image
- [v0.98.0](https://github.com/sluicesync/sluice/releases/tag/v0.98.0) — Connection-resilience (target connection-budget cap, stale-backend reaping, AIMD copy-pool backoff) + Postgres deferred-index build tuning
- [v0.97.0](https://github.com/sluicesync/sluice/releases/tag/v0.97.0) — Inline MySQL `CHECK` enforcement for translatable PG `DOMAIN` checks; multi-segment backup-broker following (v0.97.2)
- [v0.96.0](https://github.com/sluicesync/sluice/releases/tag/v0.96.0) — Redaction config-precedence hardening; backup-chain passphrase-rotation probes (Bug 116 / 117)
- [v0.94.0](https://github.com/sluicesync/sluice/releases/tag/v0.94.0) — Encrypted logical backup chains (full + incremental) with the `FormatVersion` refuse-before-touch contract + restore

The slot-less `postgres-trigger` engine, PG Row-Level Security, PostGIS round-trips, and multi-source aggregation landed across **v0.84 → v0.93**; the original HVR-feature arc (F10 / F11 / F13 / F17) is **v0.80 → v0.83**.

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
