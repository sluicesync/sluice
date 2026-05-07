# sluice

**Continuous data sync between MySQL and PostgreSQL** — including PlanetScale MySQL (via VStream) and PlanetScale Postgres. One binary, four engines, all four directions covered. Snapshot + CDC handoff is gapless; resume after restart is automatic.

```bash
# Install
go install github.com/orware/sluice/cmd/sluice@latest

# Migrate MySQL → PostgreSQL (one-shot)
sluice migrate \
    --source-driver mysql    --source 'root:rootpw@tcp(localhost:3306)/app' \
    --target-driver postgres --target 'postgres://postgres:pgpw@localhost:5432/app?sslmode=disable'

# OR continuous sync (snapshot + CDC, runs until Ctrl-C; resume on restart)
sluice sync start \
    --source-driver mysql    --source ... \
    --target-driver postgres --target ... \
    --stream-id myapp-prod
```

Pre-built Linux/macOS/Windows binaries: [latest release](https://github.com/orware/sluice/releases/latest).

## Does this fit my use case?

| You want to… | sluice does this |
|---|---|
| Move data **once** between MySQL and Postgres (or PS variants), then stop | `sluice migrate` |
| Move data **once** with low downtime — snapshot first, catch up via CDC, then cut over | `sluice migrate` then `sluice sync start --resume` |
| **Replicate continuously** for analytics, geo-locality, or hot-standby | `sluice sync start` |
| **Diff** a target against what sluice would produce | `sluice schema diff` |
| **Verify** that every row made it across (row counts, sampled / full content forthcoming) | `sluice verify` |
| **Probe** a running sync's freshness against a staleness threshold | `sluice sync health` |
| **Preview** the target DDL before running anything | `sluice schema preview` |
| Do all of the above against **PlanetScale** | Same commands; PS-MySQL uses VStream automatically when DSN host matches `*.connect.psdb.cloud` |

If you want self-hosted physical backups, query-level replication (logical decoding to apps), or schema migration tooling — that's not sluice. See [docs/architecture.md §"What sluice is not"](docs/architecture.md) for boundaries.

## Engines + directions

| Source ↘ Target → | MySQL | PostgreSQL | PlanetScale MySQL | PlanetScale PG |
|---|---|---|---|---|
| **MySQL** | ✓ | ✓ | ✓ | ✓ |
| **PostgreSQL** | ✓ | ✓ | ✓ | ✓ |
| **PlanetScale MySQL** | ✓ (VStream CDC) | ✓ (VStream CDC) | ✓ | ✓ |
| **PlanetScale PG** | ✓ | ✓ | ✓ | ✓ |

Cross-engine type translation handles the common surfaces (PG `UUID`/`INET`/`MACADDR`/`ARRAY` ↔ MySQL `CHAR(36)`/`VARCHAR`/`JSON`; MySQL `TINYINT(1)` ↔ PG `BOOLEAN`; MySQL `ENUM`/`SET` → PG enum/`TEXT[]+CHECK`; PostGIS `GEOMETRY` round-trips with SRID; many idioms in generated columns and CHECK constraints translate automatically — see [`docs/dev/translator-coverage.md`](docs/dev/translator-coverage.md)). When the default doesn't fit, `--type-override TABLE.COLUMN=TYPE` and `--expr-override TABLE.COLUMN=EXPR` cover one-off cases without writing a config file.

## Try it in 10 minutes

[`docs/examples/quickstart.md`](docs/examples/quickstart.md) walks through MySQL 8.0 + Postgres 16 via Docker Compose, loads the sakila sample database, runs a one-shot migration, then a continuous-sync stream. ~10 minutes start to finish.

## Why sluice (vs. alternatives)

- **vs. mysqldump / pg_dump:** sluice handles cross-engine, schema translation, and continuous CDC; dump tools are same-engine and snapshot-only.
- **vs. AWS DMS / GCP DataStream:** sluice is a single binary you run anywhere — no managed service, no cloud account, no per-row billing. The tradeoff: you bring your own monitoring.
- **vs. Debezium + Kafka + sink connector:** sluice covers the source-to-target path directly without Kafka in the middle. Useful when you don't already have a Kafka deployment to leverage.
- **vs. Fivetran / Stitch / Airbyte:** sluice is open-source, single-binary, no SaaS dependency, no row-count billing. Operators run it where they want, including on-prem and air-gapped.

The throughline: sluice keeps the surface small and the operator in control. No silent retry loops, no proprietary state machines, no per-row pricing. When the tool can't translate something, it fails loud at apply time so you know — silent corruption is worse than a clear error.

## Status

Pre-1.0 (`v0.11.x` series). Schema translation, snapshot, continuous-sync, schema diff, schema preview, type/expression overrides, batched apply, parallel within-table bulk copy, slot lifecycle management, graceful drain, resumable migrations — all working end-to-end against real database containers in CI on every PR. Cross-engine integration tests cover all four directions. Pre-built binaries via goreleaser on every tagged release.

Versioning follows [SemVer](https://semver.org/). v0.x minor releases may include breaking changes — the API and CLI surface are still settling. v1.0.0 marks the API-frozen line.

See [CHANGELOG.md](CHANGELOG.md) for what's landed; [docs/dev/roadmap.md](docs/dev/roadmap.md) for what's next.

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
  schema preview           Print the target DDL sluice would emit.
  schema diff              Diff a target against what sluice would produce.
  verify                   Compare row counts (and forthcoming sampled / full
                           content checks) between source and target.
  slot list / slot drop    Manage Postgres replication slots.
```

Run `sluice <command> --help` for per-command flags. DSNs can also be passed via `SLUICE_SOURCE` / `SLUICE_TARGET` env vars.

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — IR, engine pattern, orchestrator, what sluice is and isn't
- [`docs/managed-services.md`](docs/managed-services.md) — PlanetScale-specific notes, operator preconditions
- [`docs/postgres-source-prep.md`](docs/postgres-source-prep.md) — required PG GUCs, slot lifecycle, failover-survival mechanisms
- [`docs/vitess-vstream-troubleshooting.md`](docs/vitess-vstream-troubleshooting.md) — operator runbook for diagnosing PlanetScale-MySQL VStream lag (throttler, replication lag, deploy requests), plus the Vitess 24 binlog streaming roadmap
- [`docs/throughput-tuning.md`](docs/throughput-tuning.md) — knobs that matter at scale
- [`docs/schema-change-runbook.md`](docs/schema-change-runbook.md) — `ADD COLUMN` / `DROP COLUMN` / `MODIFY` against a running stream
- [`docs/type-mapping.md`](docs/type-mapping.md), [`docs/value-types.md`](docs/value-types.md) — type translation policies and runtime row contract
- [`docs/adr/`](docs/adr/) — Architecture Decision Records
- [`docs/dev/`](docs/dev/) — local development setup, roadmap, design proto-ADRs
- [`docs/examples/`](docs/examples/) — runnable quickstart, sample `sluice.yaml` config

## Why "sluice"

A sluice is a gate that controls the flow of water through a canal — it doesn't generate the flow, it regulates and directs it. That's the posture this tool takes toward your data: it doesn't transform what's flowing, it manages how, when, and where it moves.

The name has a personal connection too. The author grew up in California's Imperial Valley, where the Imperial Irrigation District manages an extensive canal system. Sluice gates are a familiar sight there — unassuming pieces of infrastructure that raise and lower water levels and let the right amount through at the right time. That's the spirit the project aims for.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Inspiration

- [PlanetScale's pgcopydb fork](https://github.com/planetscale/pgcopydb) — reference for fast PG→PG bulk copy. Tactics borrowed: parallel `COPY` per table, deferred index/constraint creation, snapshot-based consistency.
- [pscale dumper](https://github.com/planetscale/pscale) — battle-tested batch sizes and session variables for high-throughput PlanetScale MySQL reads.
- [Vitess](https://vitess.io/) — VStream gRPC protocol for PlanetScale MySQL CDC.
