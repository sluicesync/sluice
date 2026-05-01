# sluice

An open-source tool for migrating and continuously syncing data between relational databases. The initial release ships with MySQL and PostgreSQL support in all four directions; the architecture is deliberately engine-neutral so additional engines can be added later.

## Status

Pre-release. The IR package, engine registry, and CLI skeleton are in place; engine implementations (MySQL and PostgreSQL) are next. See [docs/architecture.md](docs/architecture.md) for the design and roadmap.

## Stability

`sluice` follows [Semantic Versioning](https://semver.org/) from day one. While the project is in `v0.x`, however, *no API or CLI stability guarantees are made* — minor releases may include breaking changes as the design settles. Once `v1.0.0` ships, breaking changes will only land in major versions.

## What it does

`sluice` supports four migration directions out of the box:

- MySQL → MySQL
- MySQL → PostgreSQL
- PostgreSQL → PostgreSQL
- PostgreSQL → MySQL

…and two modes of operation:

**Simple mode** — translate the source schema, apply it to the target, bulk-copy the data, done. Designed for smaller databases where a brief downtime window is acceptable. Single command, predictable outcome, easy to reason about.

**Continuous sync mode** — establish an initial copy, then stream ongoing changes from source to target via change-data-capture (MySQL binlog, PostgreSQL logical replication). Useful for low/zero-downtime migrations *and* for ongoing replication scenarios such as reporting replicas, geographic data locality, or feeding downstream systems.

## Design principles

These are the tenets the project is built around. They take precedence over feature throughput.

**Clean, elegant code.** The codebase should read like a story. Composable interfaces, small surface areas, named concepts over scattered conditionals. When pragmatism requires a wart, the wart gets a name, a test, and a comment that explains why it exists.

**IR-first.** All translation passes through a typed internal representation of schema and data. Source-specific knowledge lives in readers; target-specific knowledge lives in writers; the IR is the single source of truth in between. No regex over DDL strings.

**Contain Postgres complexity.** Roles, permissions, and extensions are surfaced (via reports and explicit allowlists) rather than silently auto-handled. Users are told what the tool will and won't do.

**Test what matters.** Schema translation, data fidelity, and continuous-sync correctness are exercised against real database engines in containers, not mocks. See [docs/testing.md](docs/testing.md).

**Cross-platform single binary.** Implemented in Go. One `go install` should produce a working tool on Linux, macOS, and Windows.

## Documents

- [docs/architecture.md](docs/architecture.md) — IR, reader/writer pattern, the two engines, module layout
- [docs/type-mapping.md](docs/type-mapping.md) — internal type model and dialect-specific translation policies
- [docs/value-types.md](docs/value-types.md) — the runtime contract for `ir.Row` values that flow between readers, the translator, and writers
- [docs/testing.md](docs/testing.md) — testing strategy and tooling

## Why sluice?

A sluice is a gate that controls the flow of water through a canal — it doesn't generate the flow, it regulates and directs it. That's exactly the posture this tool takes toward your data: it doesn't transform what's flowing, it manages how, when, and where it moves.

The name also has a personal connection. The author grew up in California's Imperial Valley, where the Imperial Irrigation District manages an extensive canal system. Sluice gates are a familiar sight there — unassuming pieces of infrastructure that raise and lower water levels and let the right amount through at the right time. That's the spirit the project aims for.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Inspiration & references

- [PlanetScale's pgcopydb fork](https://github.com/planetscale/pgcopydb) — reference implementation for fast PostgreSQL → PostgreSQL bulk copy. Tactics worth borrowing: parallel `COPY` per table, deferred index/constraint creation, snapshot-based consistency.
- [DoltHub's sqllogictest corpus](https://docs.dolthub.com/sql-reference/benchmarks/correctness) — used as a semantic-equivalence yardstick for migrated data.
- [Vitess SQL parser](https://github.com/vitessio/vitess) and [pg_query_go](https://github.com/pganalyze/pg_query_go) — battle-tested parsers for the dump-file path.
