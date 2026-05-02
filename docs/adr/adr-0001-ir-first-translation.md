# ADR-0001: IR-first translation

## Status

Accepted.

## Context

A migration tool that supports N source engines and M target engines has, in principle, N×M translation paths. Each path needs to know how to read schema and rows from the source dialect and emit them in the target dialect. If translation logic is written pairwise — MySQL→Postgres, Postgres→MySQL, MySQL→MariaDB, etc. — the surface area grows quadratically in engines and the same translation rule (e.g. "MySQL `TINYINT(1)` is a boolean") gets restated in each direction.

A second axis is type fidelity. Reading a column without a typed model means relying on regexes over `SHOW CREATE TABLE` strings or on engine-specific column metadata, both of which leak dialect quirks into business logic.

## Decision

All translation passes through a typed intermediate representation defined in `internal/ir`. Source-side knowledge lives in readers (`SchemaReader`, `RowReader`, `CDCReader`); target-side knowledge lives in writers (`SchemaWriter`, `RowWriter`, `ChangeApplier`); the IR is the only shared contract between them. The IR includes a sealed `Type` interface (see [ADR-0002](adr-0002-sealed-interfaces.md)), a tiered model (`TierCore` types every engine must support, `TierExtension` types declared per-engine via `Capabilities`), and `Schema`/`Table`/`Column`/`Index`/`ForeignKey` structures.

The orchestrator (`internal/pipeline.Migrator`) operates exclusively on IR values and never imports an engine package. Engines are looked up by name through the registry.

## Consequences

Translation logic now scales linearly: each engine implements one reader and one writer, and any reader can pair with any writer. Per-pair test coverage still matters (see `internal/pipeline/migrate_cross_integration_test.go`) but the underlying code paths are shared.

The cost is an upfront commitment to keeping the IR genuinely dialect-neutral. When a Postgres-only feature (e.g. `JSONB`, native arrays, custom enum types) needs representation, it lands as a `TierExtension` type with explicit capability gating, never as a Postgres-shaped leak in `internal/ir`. The MySQL `ForeignKey.ReferencedSchema` leak that surfaced in cross-engine validation is the canonical example of what this discipline guards against.
