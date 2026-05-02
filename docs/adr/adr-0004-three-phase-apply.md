# ADR-0004: Three-phase schema apply

## Status

Accepted.

## Context

Bulk-loading data into a target with all indexes and constraints in place is slow. Each `INSERT` (or `COPY`) row maintains every secondary index, validates every `CHECK`, and enforces every foreign key on every row. For a multi-million-row table, that overhead dominates the migration time, often by an order of magnitude.

The well-known optimization is to load data into the bare table first, then build indexes and add constraints once the data is in place. This is what `pgcopydb` does on the Postgres side, what `mysqldump --skip-add-keys` was designed to support, and what every database administrator does by hand for large restores.

A second concern is foreign-key ordering. Even without the performance argument, FK references between tables make a single-pass schema apply order-sensitive: the parent table must exist (and, for some validation flavors, be populated) before the child's FK can be added. Splitting constraints into a separate phase moots the ordering question.

## Decision

`internal/pipeline.Migrator.Run` applies a target schema in three phases:

1. **CreateTablesWithoutConstraints** — emit `CREATE TABLE` statements with columns, primary keys, and `NOT NULL`, but no secondary indexes and no foreign keys.
2. *(Bulk row copy phase happens between 1 and 3 — not part of `SchemaWriter` itself.)*
3. **SyncIdentitySequences** — for engines whose identity columns don't auto-bump on direct INSERT (Postgres), advance each sequence past `MAX(<col>)` so the next user-initiated INSERT doesn't collide with bulk-copied IDs. MySQL's InnoDB auto-bumps, so its implementation is a no-op.
4. **CreateIndexes** — emit `CREATE INDEX` for every secondary index.
5. **CreateConstraints** — emit `ALTER TABLE ... ADD FOREIGN KEY` for every FK.

These four methods form the `ir.SchemaWriter` interface. Engines implement them independently (MySQL, Postgres). Inline FK declarations in `CREATE TABLE` are deliberately *not* used even when they'd parse: keeping the contract uniform across engines and across simple-mode vs CDC-resume scenarios is worth more than minor SQL aesthetics.

The original three-phase design (this ADR's title) was extended with phase 3.5 (`SyncIdentitySequences`) during roadmap §7 once the cross-engine bug surface around identity columns surfaced in real migrations. The pattern is the same: each phase is an independent `SchemaWriter` method, the orchestrator calls them in order, and engines whose semantics make a phase unnecessary implement it as a no-op. The title stays "three-phase" because the addition is a sub-step of the original pattern rather than a fundamental restructure — and renaming would force every backlink to update without changing the underlying decision.

## Consequences

Bulk loads run at native speed. FK ordering is not an issue at apply time. Cross-engine `Migrator` code is engine-neutral: it calls the three phases in order without caring how each engine emits the underlying DDL.

The cost is more round-trips at apply time (three phases instead of one) and more network chatter for large schemas. For migrations this is irrelevant; if it ever matters for sync resume, the phases can be batched per table without changing the contract.
