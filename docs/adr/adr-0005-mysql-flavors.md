# ADR-0005: MySQL flavors as capability variants

## Status

Accepted.

## Context

"MySQL" in 2026 is a family of related-but-not-identical products. Vanilla MySQL 8.0 (and the major cloud variants that mirror it — RDS, CloudSQL, Azure Database, Percona Server) supports `LOAD DATA INFILE`, full binlog access, and user-defined partitioning. PlanetScale (Vitess-backed) is wire-compatible but does not allow `LOAD DATA INFILE`, exposes its own change-feed in place of binlog, and replaces user-defined partitioning with sharding. MariaDB diverges further. AWS Aurora MySQL is somewhere in between.

A naive design has one engine per flavor: an `internal/engines/planetscale` package that duplicates 90% of `internal/engines/mysql` to differ in three or four `Capabilities` flags. Code drift between near-twin packages is a known maintenance hazard.

A second naive design treats the flavor as a runtime probe: connect, run a few `SELECT @@version` queries, infer the flavor, choose a strategy. That approach makes the engine's behavior dependent on probe order and timing, complicates testing, and silently misclassifies edge cases.

## Decision

There is one MySQL engine package (`internal/engines/mysql`). All schema-reading, row-reading, DDL emission, and value decoding is flavor-independent — the SQL surface MySQL exposes is the same across vanilla and PlanetScale. Differences live exclusively in the `Capabilities` declaration each flavor returns.

`Flavor` is a typed enum (`FlavorVanilla`, `FlavorPlanetScale`). Each flavor has an entry in `flavorCapabilities` declaring `BulkLoad`, `CDC`, `SchemaScope`, `SupportedTypes`, and the boolean feature flags. The same `Engine` struct registers itself once per flavor under a different name (`mysql`, `planetscale`), and the user picks a flavor by setting `--source-driver=planetscale` (or `mysql`) on the CLI.

## Consequences

Adding a new MySQL flavor (MariaDB, Aurora MySQL, TiDB-as-MySQL) is a five-line capability table entry, not a new package. The strategy-selection logic in readers and writers is reused unchanged: `Capabilities.BulkLoad == BulkLoadLoadDataInfile` selects one path, `BulkLoadBatchedInsert` selects another, and the engine code never asks "which flavor am I?"

The cost is a slight indirection: the user must pick the right flavor name. That tradeoff favors explicitness over convenience — silent misclassification of a PlanetScale instance as vanilla MySQL would lead to confusing failures (a `LOAD DATA INFILE` rejection mid-migration), and forcing the user to declare the flavor surfaces the choice up-front.
