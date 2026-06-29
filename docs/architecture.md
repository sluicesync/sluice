# Architecture

This document describes the internal architecture of `sluice`. The goal is to lay out abstractions that compose cleanly, contain dialect-specific knowledge at the edges, and remain easy to reason about as the surface area grows. The architecture is also designed to outgrow the initial MySQL/Postgres pairing — adding a new engine should be a drop-in operation against well-defined interfaces, not a refactor.

## Core abstraction: the IR

Every translation flows through a typed internal representation. The IR is a Go data model — not a string format, not a serialised protocol — that describes schemas and data in dialect-neutral terms.

```
Source DB → Reader → IR → Writer → Target DB
```

The IR has two layers:

**Schema IR.** Represents tables, columns, indexes, foreign keys, constraints, and extensions in a normalised form. Column types are values of a sealed type hierarchy (see [docs/type-mapping.md](type-mapping.md)) — `Integer{Width: 8, Unsigned: false}`, `Varchar{Length: 255, Charset: "utf8mb4"}`, `Boolean{}`, etc.

**Change IR.** Represents row-level events (`Insert`, `Update`, `Delete`, `Truncate`) and schema events (`AddColumn`, `DropColumn`, etc.) for the continuous-sync engine. Each event carries the IR-typed values of the affected columns plus durable position information.

Source-specific knowledge lives in **readers**. Target-specific knowledge lives in **writers**. The IR layer itself is pure — no I/O, no logging, no global state — which means the most correctness-critical code is exercised by sub-millisecond table-driven unit tests. (See [ADR-0001](adr/adr-0001-ir-first-translation.md) for the IR-first decision and [ADR-0002](adr/adr-0002-sealed-interfaces.md) for the sealed `ir.Type` hierarchy.)

## Two engines

The simple and continuous-sync modes share a translator and type model but are otherwise different runtime engines.

### Simple-mode engine

A linear pipeline:

```mermaid
flowchart LR
    A[Source DB] -->|metadata| B[Reader]
    B --> C[Schema IR]
    C --> D[Translator]
    D --> E[Writer: schema-only]
    E -->|CREATE TABLE w/o indexes| F[Target DB]
    A -->|rows| G[Reader: bulk]
    G --> H[Row IR]
    H --> I[Writer: bulk loader]
    I --> F
    F --> J[Writer: indexes/constraints/FKs]
```

Schema is created without indexes or constraints, data is bulk-loaded (`COPY` for Postgres, `LOAD DATA INFILE` for MySQL), and then indexes and constraints are added as a final step. This three-phase ordering — borrowed from `pgcopydb` — is typically 5–10× faster than loading into a fully-constrained schema. (See [ADR-0004](adr/adr-0004-three-phase-apply.md) for the phase model.)

Per-table parallelism is the default. Foreign-key dependency order matters only for the constraint-application phase, not for the data-load phase.

### Continuous-sync engine

A streaming pipeline driven by source-side change-data-capture:

```mermaid
flowchart LR
    A[Source DB] -->|binlog / logical repl| B[CDC Reader]
    B --> C[Change IR]
    C --> D[Translator]
    D --> E[Buffered Applier]
    E --> F[Target DB]
    G[Position Store] <--> B
    G <--> E
```

The CDC reader produces a stream of Change IR events. The translator maps them through the type model. The applier batches and applies them to the target, maintaining transactional boundaries where the source provides them. A durable position store records progress on both sides so a crash mid-stream resumes correctly.

The same engine serves two operator intents: bootstrapping a one-time migration (initial snapshot + tail of the change stream until cutover) and ongoing replication (initial snapshot + indefinite tail). (See [ADR-0006](adr/adr-0006-pgoutput.md) and [ADR-0008](adr/adr-0008-go-mysql.md) for the per-engine CDC library choices, [ADR-0007](adr/adr-0007-position-persistence.md) for the position store, [ADR-0009](adr/adr-0009-streamer-vs-mode-flag.md) for the orchestrator shape, and [ADR-0010](adr/adr-0010-idempotent-applier.md) for resume safety.)

## Module layout

```
sluice/
├── cmd/
│   └── sluice/                 # CLI entry point
├── internal/
│   ├── ir/                     # IR types: Schema, Column, Type, Change events
│   │   ├── schema.go
│   │   ├── types.go            # core IR types (universal)
│   │   ├── extension_types.go  # extension IR types (per-engine optional)
│   │   ├── capabilities.go     # engine capability declarations
│   │   ├── change.go
│   │   └── diff/               # pure-function schema diff + per-table drift reports
│   ├── translate/              # IR ↔ IR transformations and policies
│   │   ├── policy.go
│   │   └── translate.go
│   ├── engines/                # Engine registry — see "Adding a new engine"
│   │   ├── registry.go         # name → Engine lookup; engines self-register
│   │   ├── mysql/              # MySQL engine package: reader+writer+CDC+caps
│   │   └── postgres/           # Postgres engine package: reader+writer+CDC+caps
│   ├── apply/                  # Continuous-sync applier and position store
│   ├── pipeline/               # Simple-mode orchestrator
│   └── config/                 # Connection, mapping overrides, runtime options
├── docs/
└── test/
    ├── golden/                 # Schema-translation golden files
    ├── integration/            # Container-based end-to-end tests
    └── sqllogic/               # Curated semantic-equivalence corpus
```

The `internal/ir` package has no dependencies on any other package in the project. Pure-function helpers and feature-scoped contracts over the IR split into sub-packages under `internal/ir` (today `ir/diff` — the schema diff behind `sluice schema diff` and the per-table drift report behind CDC refuse-loudly messages — and `ir/backup` — the logical-backup manifest types, chain identity/fingerprint helpers, and the optional engine surfaces the backup orchestrator type-asserts on); sub-packages depend only on core `ir`, never the reverse. The sealed-interface JSON codec (`schema_wire.go`) stays in core `ir`: its `Column.MarshalJSON`/`UnmarshalJSON` hooks must be methods on the core type, and the CDC schema-history store shares the codec with the backup manifests. The `translate` package depends only on `ir`. Each engine package under `internal/engines/<name>/` depends on `ir` plus its database driver — never on another engine package, never on `pipeline` or `apply`. This dependency direction is enforced; anything else is a code smell and a review-flag.

Each engine package bundles everything that knows about a specific database: schema reader, schema writer, row reader, row writer, CDC reader (if applicable), change applier (if applicable), and a `Capabilities` declaration. Co-locating these means the knowledge of "what MySQL is and how to talk to it" lives in exactly one place.

## Interfaces

The interfaces are intentionally narrow. Each one is implementable independently for each engine, which is what makes the four-direction matrix tractable.

```go
// SchemaReader extracts an IR schema from a live database.
type SchemaReader interface {
    ReadSchema(ctx context.Context) (*ir.Schema, error)
}

// SchemaWriter applies an IR schema to a target database, in phases.
type SchemaWriter interface {
    CreateTablesWithoutConstraints(ctx context.Context, s *ir.Schema) error
    CreateIndexes(ctx context.Context, s *ir.Schema) error
    CreateConstraints(ctx context.Context, s *ir.Schema) error
}

// RowReader streams rows for bulk copy.
type RowReader interface {
    ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error)
}

// RowWriter performs bulk inserts using the target's native fast-load path.
type RowWriter interface {
    WriteRows(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error
}

// CDCReader streams Change IR events from a source database.
type CDCReader interface {
    StreamChanges(ctx context.Context, from ir.Position) (<-chan ir.Change, error)
}

// ChangeApplier applies Change IR events to a target database.
type ChangeApplier interface {
    Apply(ctx context.Context, changes <-chan ir.Change) error
}
```

Note that the `Schema*` and `Row*` interfaces are deliberately separate. A simple-mode run needs both; a continuous-sync run needs `SchemaReader` and `SchemaWriter` for bootstrap, then transitions to `CDCReader` and `ChangeApplier`. Keeping them distinct lets the simple-mode engine remain lean and easy to test in isolation. (The unexported sealing method on each variant interface is documented in [ADR-0002](adr/adr-0002-sealed-interfaces.md).)

## Engine capabilities

Different engines support different things. Postgres has arrays, MySQL doesn't. Postgres has logical replication, MySQL has binlog. SQLite's WAL is a *physical* page-log, not a logical change stream, so its CDC is trigger-based — the `sqlite-trigger` and `d1-trigger` engines install per-table AFTER triggers that write a change-log (ADR-0135 / ADR-0136), exactly as the `postgres-trigger` engine does for managed Postgres that blocks logical replication. The orchestrator should not hard-code these differences with `if engine == "mysql"` branches scattered through the code; it should ask each engine what it supports.

Every engine declares a `Capabilities` value:

```go
package ir

type Capabilities struct {
    // How bulk loading is performed.
    BulkLoad BulkLoadMethod    // Copy, LoadDataInfile, BatchedInsert

    // How change-data-capture is exposed (if at all).
    CDC CDCMethod              // Binlog, LogicalReplication, VStream, Triggers, None

    // The schema scope model.
    SchemaScope SchemaScope    // Flat (MySQL), Namespaced (Postgres), ...

    // Which extension IR types this engine supports natively.
    SupportedTypes TypeSet     // see internal/ir/capabilities.go

    // Type-system feature flags.
    SupportsCheckConstraint   bool
    SupportsGeneratedColumns  bool
    SupportsPartitioning      bool
    EnumSupport               EnumSupport      // None, ColumnLevel, TypeLevel
    JSONSupport               JSONSupport      // None, Json, Jsonb, Both
    UnsignedIntegers          bool
}

type Engine interface {
    Name() string
    Capabilities() Capabilities
    OpenSchemaReader(ctx context.Context, dsn string) (SchemaReader, error)
    OpenSchemaWriter(ctx context.Context, dsn string) (SchemaWriter, error)
    OpenRowReader(ctx context.Context, dsn string) (RowReader, error)
    OpenRowWriter(ctx context.Context, dsn string) (RowWriter, error)
    OpenCDCReader(ctx context.Context, dsn string) (CDCReader, error)   // nil if CDC == None
    OpenChangeApplier(ctx context.Context, dsn string) (ChangeApplier, error)
}
```

The translator and the simple-mode pipeline consult capabilities to pick a strategy. The Postgres writer's emission of an `Enum` IR value is decided by `cap.EnumSupport`. The MySQL writer's emission of an `Array` IR value is decided by `cap.SupportedTypes.Has(ArrayType)`. The bulk-load phase asks `cap.BulkLoad` rather than guessing.

This pattern means new engines slot in without touching the core. It also means *behaviour differences* between engines are documented as data — readable, diffable, easy to inspect — rather than scattered conditionals. (See [ADR-0005](adr/adr-0005-mysql-flavors.md) for the per-flavor capability variant model that proves out the pattern.)

## Adding a new engine

Once the core is built, adding a new engine is a contained operation. The engine package is the only thing that needs to be written, and the registry takes care of wiring.

The `sqlite` engine (`internal/engines/sqlite`, shipped) is the canonical worked example. Its shape was exactly:

1. Create `internal/engines/sqlite/`.
2. Implement the relevant interfaces — `SchemaReader`, `SchemaWriter`, `RowReader`, `RowWriter`. CDC interfaces are optional; the base `sqlite` engine declares `CDC: CDCNone` and skips them (continuous sync arrived later as the separate `sqlite-trigger` / `d1-trigger` engines, which *compose* the base engine and add the trigger-CDC surface).
3. Declare a `Capabilities` value that reflects what SQLite actually supports — a flat namespace, no extension types, `JSONSupport: None`, and the type-affinity behaviour documented in [type-mapping.md](type-mapping.md).
4. Register the engine in the package's `init()` function. `Register` takes the engine value; the registry reads its name from `Name()`:

```go
package sqlite

import "sluicesync.dev/sluice/internal/engines"

func (Engine) Name() string { return "sqlite" }

func init() {
    engines.Register(Engine{})
}
```

5. Add a blank import where engines are wired in so its `init()` runs.

That's the entire surface area of "add a new engine." No changes to `ir`, the translator, `pipeline`, or the appliers. No changes to the CLI parsing logic — `--source-driver sqlite` (or `driver: sqlite` in a config) now resolves through the registry. A hypothetical future engine — say DuckDB — would slot in the same way.

The contract for a new engine:

- The reader must produce IR that uses **only** types declared in its own `Capabilities.SupportedTypes`. If the underlying engine has types beyond what we model, the reader either maps them to existing IR types with a documented loss, or extends the IR with a new extension type (and updates other engines' capability declarations to either support or reject it).
- The writer must accept any IR schema and either emit valid DDL or return a clear, structured error explaining what's not supported and why. Silently dropping fields is not allowed.
- The capability declaration must be honest. If the writer claims `SupportsCheckConstraint: true`, it had better.
- The engine package owns its own integration tests (in `internal/engines/<name>/`); the cross-engine matrix tests in `test/integration/` automatically pick up new engines from the registry.

## Configuration

Configuration is layered. CLI flags override values in a YAML config file, which overrides defaults. The config file is the place where per-table or per-column type overrides live — never in code.

```yaml
source:
  driver: mysql
  dsn: "user:pass@tcp(localhost:3306)/myapp"

target:
  driver: postgres
  dsn: "postgres://user:pass@localhost/myapp"

mode: simple   # or "sync"

mappings:
  # Override the default IR-to-target translation for specific cases.
  - table: orders
    column: status
    target_type: text  # default would have been an enum
  - table: events
    column: payload
    target_type: jsonb

extensions:
  # Explicit allowlist for Postgres extensions encountered in the source.
  allow:
    - citext
    - pg_trgm
```

Configuration loading and CLI parsing details are discussed in [ADR-0003](adr/adr-0003-kong-koanf.md).

## Decisions deferred

These are real questions that affect architecture but don't need answers before code starts. Captured here so they aren't lost.

**Schema drift during continuous sync.** MySQL binlog carries DDL; PostgreSQL logical replication does not. Initial behaviour: pause sync on detected source-side DDL and require explicit operator action. Auto-application is a v2 feature.

**Active-active replication.** Out of scope for v1. Sync is unidirectional.

**Conflict detection.** The applier surfaces apply errors but does not attempt automatic resolution. Operators decide.

**Resumability granularity.** Per-stream position is required; per-table positions within a stream are a stretch goal.
