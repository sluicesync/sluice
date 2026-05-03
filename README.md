# sluice

An open-source tool for migrating and continuously syncing data between relational databases. The initial release ships with MySQL and PostgreSQL support in all four directions; the architecture is deliberately engine-neutral so additional engines can be added later.

## Quickstart

The fastest way to see what sluice does is to run it. The walkthrough at [docs/examples/quickstart.md](docs/examples/quickstart.md) sets up a MySQL 8.0 + Postgres 16 pair via Docker Compose, loads the sakila sample database, and runs both a one-shot bulk migration and a continuous-sync stream — about 10 minutes start to finish.

## Status

Pre-1.0. Both engines (MySQL and Postgres) are implemented and tested against real database containers; the simple-mode orchestrator and continuous-sync streamer both work end-to-end. Architecture, design decisions, and roadmap are documented in [docs/architecture.md](docs/architecture.md), [docs/adr/](docs/adr/), and [docs/dev/roadmap.md](docs/dev/roadmap.md).

Versioning follows [SemVer](https://semver.org/). While the project is in `v0.x`, minor releases may include breaking changes — the API and CLI surface are still settling. Once `v1.0.0` ships, breaking changes will only land in major versions.

## What it does

`sluice` supports four migration directions out of the box:

- MySQL → MySQL
- MySQL → PostgreSQL
- PostgreSQL → PostgreSQL
- PostgreSQL → MySQL

…and two modes of operation:

**Simple mode** — translate the source schema, apply it to the target, bulk-copy the data, done. Designed for smaller databases where a brief downtime window is acceptable. Single command, predictable outcome, easy to reason about.

**Continuous sync mode** — establish an initial copy, then stream ongoing changes from source to target via change-data-capture (MySQL binlog, PostgreSQL logical replication, or Vitess VStream for PlanetScale MySQL). Useful for low/zero-downtime migrations *and* for ongoing replication scenarios such as reporting replicas, geographic data locality, or feeding downstream systems.

### Managed-service support

The vanilla MySQL and Postgres engines work directly against most managed offerings (AWS RDS / Aurora, GCP CloudSQL, Azure Database). PlanetScale gets specific treatment:

- **PlanetScale Postgres** — the vanilla `postgres` engine handles PS-PG without code changes. Verified end-to-end against a real PS account.
- **PlanetScale MySQL** — uses the `planetscale` engine flavor (a variant of the MySQL engine), with CDC delivered via Vitess's VStream gRPC protocol instead of binlog (PlanetScale doesn't expose binlog directly).
- **Self-hosted Vitess** — the same `planetscale` flavor covers any Vitess deployment. DSN flags opt out of PlanetScale-specific defaults (TLS, basic auth, shard naming) for vttestserver and other self-hosted setups.

See [docs/managed-services.md](docs/managed-services.md) for the full compatibility matrix and operator preconditions.

### Type-translation overrides

The simple-mode and continuous-sync paths both honour a `mappings:` block in `sluice.yaml` that lets operators override per-column target types. Useful when the default policy is lossy or when targeting types the source can't natively express (PG `JSONB`, PG `TEXT[]` from a MySQL `SET`, PostGIS `geometry(POINT, 4326)` from a MySQL spatial column). See [docs/examples/sluice.yaml](docs/examples/sluice.yaml).

## Design principles

These are the tenets the project is built around. They take precedence over feature throughput.

**Clean, elegant code.** The codebase should read like a story. Composable interfaces, small surface areas, named concepts over scattered conditionals. When pragmatism requires a wart, the wart gets a name, a test, and a comment that explains why it exists.

**IR-first.** All translation passes through a typed internal representation of schema and data. Source-specific knowledge lives in readers; target-specific knowledge lives in writers; the IR is the single source of truth in between. No regex over DDL strings.

**Contain Postgres complexity.** Roles, permissions, and extensions are surfaced (via reports and explicit allowlists) rather than silently auto-handled. Users are told what the tool will and won't do.

**Test what matters.** Schema translation, data fidelity, and continuous-sync correctness are exercised against real database engines in containers, not mocks. See [docs/testing.md](docs/testing.md).

**Cross-platform single binary.** Implemented in Go. One `go install` should produce a working tool on Linux, macOS, and Windows.

## CLI

```
$ sluice --help
Open-source database migration and continuous-sync tool.

Usage: sluice <command> [flags]

Flags:
  -c, --config=PATH        Path to a YAML config file.
  -l, --log-level=info     Log verbosity. (debug,info,warn,error)
  -V, --version            Print version and exit.

Commands:
  engines                  List registered database engines.
  migrate                  Run a one-time schema + data migration (simple mode).
  sync start               Start a continuous-sync stream from source to target.
  sync status              Show status of a running sync stream.
```

Quick examples:

```bash
# List the engines this binary was built with.
sluice engines

# Run a one-time MySQL → Postgres migration.
sluice migrate \
    --source 'user:pass@tcp(localhost:3306)/myapp' \
    --target 'postgres://user:pass@localhost/myapp?sslmode=disable'

# Same, but with a config file overriding type mappings.
sluice --config sluice.yaml migrate --source ... --target ...
```

See [docs/examples/sluice.yaml](docs/examples/sluice.yaml) for a commented sample config.

## End-to-end walkthrough

A copy-pasteable run against two throwaway containers — useful for verifying a fresh build, demoing the tool, or kicking the tires on a new engine pair. The example migrates a small MySQL database into a fresh Postgres database.

Boot the two databases:

```bash
docker run -d --rm --name sluice-mysql \
    -e MYSQL_ROOT_PASSWORD=rootpw \
    -e MYSQL_DATABASE=app \
    -p 3306:3306 mysql:8.0

docker run -d --rm --name sluice-pg \
    -e POSTGRES_PASSWORD=pgpw \
    -e POSTGRES_DB=app \
    -p 5432:5432 postgres:16
```

Seed the MySQL source with a tiny schema and a few rows:

```bash
docker exec -i sluice-mysql mysql -uroot -prootpw app <<'SQL'
CREATE TABLE users (
    id         BIGINT       NOT NULL AUTO_INCREMENT,
    email      VARCHAR(255) NOT NULL,
    active     TINYINT(1)   NOT NULL DEFAULT 1,
    created_at TIMESTAMP(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY users_email_unique (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO users (email, active) VALUES
    ('alice@example.com', 1),
    ('bob@example.com',   0);
SQL
```

Run the migration. The `--dry-run` (`-n`) flag prints the plan without writing — useful for sanity-checking before letting it loose:

```bash
sluice migrate \
    --source-driver mysql    --source 'root:rootpw@tcp(localhost:3306)/app' \
    --target-driver postgres --target 'postgres://postgres:pgpw@localhost:5432/app?sslmode=disable' \
    --dry-run

sluice migrate \
    --source-driver mysql    --source 'root:rootpw@tcp(localhost:3306)/app' \
    --target-driver postgres --target 'postgres://postgres:pgpw@localhost:5432/app?sslmode=disable'
```

Verify the Postgres target:

```bash
docker exec -i sluice-pg psql -U postgres app <<'SQL'
\d users
SELECT id, email, active FROM users ORDER BY id;
SQL
```

Expected: a `users` table with `id BIGINT … IDENTITY`, `email VARCHAR(255)`, `active BOOLEAN`, the unique index on `email`, and two rows where alice's `active` is `t` and bob's is `f` — i.e. MySQL's `TINYINT(1)` came across as Postgres `BOOLEAN`.

Tear down:

```bash
docker stop sluice-mysql sluice-pg
```

DSNs can also be passed via the `SLUICE_SOURCE` and `SLUICE_TARGET` environment variables instead of `--source`/`--target`.

## Documents

- [docs/architecture.md](docs/architecture.md) — IR, reader/writer pattern, the two engines, module layout
- [docs/type-mapping.md](docs/type-mapping.md) — internal type model and dialect-specific translation policies
- [docs/value-types.md](docs/value-types.md) — the runtime contract for `ir.Row` values that flow between readers, the translator, and writers
- [docs/managed-services.md](docs/managed-services.md) — supported managed services (PlanetScale Postgres, PlanetScale MySQL via VStream, Vitess) plus operator preconditions
- [docs/testing.md](docs/testing.md) — testing strategy and tooling
- [docs/adr/](docs/adr/) — Architecture Decision Records pinning load-bearing design choices
- [docs/dev/development.md](docs/dev/development.md) — local development workflow (gofumpt, Make targets, pre-commit hook)
- [docs/examples/sluice.yaml](docs/examples/sluice.yaml) — example configuration file
- [CHANGELOG.md](CHANGELOG.md) — capability-grouped log of what's landed since the initial design pass

## Why sluice?

A sluice is a gate that controls the flow of water through a canal — it doesn't generate the flow, it regulates and directs it. That's exactly the posture this tool takes toward your data: it doesn't transform what's flowing, it manages how, when, and where it moves.

The name also has a personal connection. The author grew up in California's Imperial Valley, where the Imperial Irrigation District manages an extensive canal system. Sluice gates are a familiar sight there — unassuming pieces of infrastructure that raise and lower water levels and let the right amount through at the right time. That's the spirit the project aims for.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Inspiration & references

- [PlanetScale's pgcopydb fork](https://github.com/planetscale/pgcopydb) — reference implementation for fast PostgreSQL → PostgreSQL bulk copy. Tactics worth borrowing: parallel `COPY` per table, deferred index/constraint creation, snapshot-based consistency.
- [DoltHub's sqllogictest corpus](https://docs.dolthub.com/sql-reference/benchmarks/correctness) — used as a semantic-equivalence yardstick for migrated data.
- [Vitess SQL parser](https://github.com/vitessio/vitess) and [pg_query_go](https://github.com/pganalyze/pg_query_go) — battle-tested parsers for the dump-file path.
