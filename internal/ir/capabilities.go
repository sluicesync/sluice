// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// BulkLoadMethod identifies how an engine supports bulk inserting data.
type BulkLoadMethod uint8

// Recognised BulkLoadMethod values.
const (
	BulkLoadNone           BulkLoadMethod = iota
	BulkLoadCopy                          // PostgreSQL COPY
	BulkLoadLoadDataInfile                // MySQL LOAD DATA INFILE
	BulkLoadBatchedInsert                 // Driver-batched parameterised INSERTs
)

func (m BulkLoadMethod) String() string {
	switch m {
	case BulkLoadCopy:
		return "copy"
	case BulkLoadLoadDataInfile:
		return "load-data-infile"
	case BulkLoadBatchedInsert:
		return "batched-insert"
	case BulkLoadNone:
		return "none"
	default:
		return "unknown"
	}
}

// CDCMethod identifies how an engine exposes change-data-capture.
type CDCMethod uint8

// Recognised CDCMethod values.
const (
	CDCNone               CDCMethod = iota
	CDCBinlog                       // MySQL row-based binary log
	CDCLogicalReplication           // PostgreSQL logical replication
	CDCTriggers                     // Trigger-based CDC (e.g. SQLite future)
	CDCVStream                      // Vitess VStream gRPC (PlanetScale MySQL)
)

func (m CDCMethod) String() string {
	switch m {
	case CDCBinlog:
		return "binlog"
	case CDCLogicalReplication:
		return "logical-replication"
	case CDCTriggers:
		return "triggers"
	case CDCVStream:
		return "vstream"
	case CDCNone:
		return "none"
	default:
		return "unknown"
	}
}

// SchemaScope describes whether an engine namespaces tables under
// schemas (PostgreSQL) or has a flat table namespace (MySQL).
type SchemaScope uint8

// Recognised SchemaScope values.
const (
	SchemaScopeFlat       SchemaScope = iota // MySQL-style: tables live in a single namespace
	SchemaScopeNamespaced                    // Postgres-style: schemas contain tables
)

func (s SchemaScope) String() string {
	switch s {
	case SchemaScopeFlat:
		return "flat"
	case SchemaScopeNamespaced:
		return "namespaced"
	default:
		return "unknown"
	}
}

// DDLDialect identifies the SQL dialect family used when sluice
// renders DDL *suggestions* for an engine (schema-diff ALTER hints,
// identifier quoting). It is a rendering concern only — actual schema
// writes go through the engine's own [SchemaWriter].
type DDLDialect uint8

// Recognised DDLDialect values. The zero value is the ANSI/Postgres
// idiom (double-quoted identifiers, `ALTER COLUMN ... TYPE`), so an
// engine that doesn't declare a dialect renders portable-ish SQL
// rather than silently inheriting MySQL's backtick syntax.
const (
	DDLDialectANSI  DDLDialect = iota // double-quoted identifiers, ALTER COLUMN ... TYPE
	DDLDialectMySQL                   // backtick identifiers, MODIFY COLUMN
)

func (d DDLDialect) String() string {
	switch d {
	case DDLDialectANSI:
		return "ansi"
	case DDLDialectMySQL:
		return "mysql"
	default:
		return "unknown"
	}
}

// Capabilities declares what a database engine can do natively.
// Each [Engine] implementation returns a Capabilities value so the
// translator and pipeline can pick a strategy without hard-coding
// per-engine branches.
type Capabilities struct {
	// BulkLoad is the engine's preferred fast-load mechanism.
	BulkLoad BulkLoadMethod
	// CDC is the change-data-capture mechanism the engine exposes.
	CDC CDCMethod
	// CDCPositionCommitsAfterRows reports that the engine stamps CDC
	// positions per-transaction-commit, AFTER the rows the commit covers,
	// so a schema-boundary snapshot and the row changes in the SAME
	// transaction share the SAME position (Vitess/VStream: the VGTID
	// arrives after its rows — see internal/engines/mysql/cdc_vstream.go
	// and cdc_vstream_snapshot.go). A logical-backup restore uses this to
	// decide whether "a schema-history snapshot is anchored exactly at
	// EndPosition" proves the window's data was applied: on such engines an
	// emptied-data window whose final transaction first-touched a table
	// leaves a snapshot at EndPosition and would spuriously satisfy that
	// completeness check (Bug 184), so restore must NOT trust the anchor
	// there. False for engines whose schema anchor strictly precedes its
	// rows (Postgres: RelationMessage WALStart < row LSNs; MySQL binlog: the
	// DDL query-event position < the row events it precedes).
	CDCPositionCommitsAfterRows bool
	// SchemaScope is the table-namespacing model.
	SchemaScope SchemaScope
	// NOTE: seven TYPE-FIDELITY fields were removed here on 2026-08-10
	// (SupportedTypes, SupportsCheckConstraint, SupportsGeneratedColumns,
	// SupportsPartitioning, EnumSupport, JSONSupport, UnsignedIntegers).
	//
	// Every one was declared by all eight engines and read by NOTHING for
	// three months. They were a second copy of a truth the engines already
	// hold structurally — whether an engine can represent a type is decided
	// by its own type dispatch, and a missing arm in translateType IS the
	// answer — so the copy drifted exactly as second copies do: three
	// extension kinds declared by no engine at all, and the Vitess flavor
	// recording a sluice bug (Bug 239) as a vendor limitation.
	//
	// The fields that REMAIN are strategy selectors — BulkLoad, CDC,
	// SchemaScope, DDLDialect and friends — facts about an engine's
	// operational shape that the orchestrator cannot derive by inspection.
	// That is the line: declare strategy, derive type fidelity.
	//
	// Where an EARLY answer is needed, use the emit-preflighter family: the
	// target dry-runs its own emit and reports what it cannot render.
	// [ColumnTypeEmitPreflighter] is the member that answers this question
	// for column TYPES — it runs the engine's own emitColumnType, so it is
	// the derived answer the deleted table was pretending to be.

	// DDLDialect is the SQL dialect family used when sluice renders
	// DDL suggestions for this engine (schema-diff ALTER hints,
	// identifier quoting). Rendering-only; schema writes go through
	// the engine's [SchemaWriter].
	DDLDialect DDLDialect

	// PostgresBackend reports whether the engine connects to a genuine
	// PostgreSQL server, regardless of capture mechanism — PG
	// system-catalog probes (pg_roles, datfrozenxid,
	// pg_partitioned_table), 32-bit XID wraparound, and PG declarative
	// partitioning semantics all apply. True for both the slot-based
	// `postgres` engine and the trigger-based `postgres-trigger`
	// engine; false for the MySQL family. Orchestrator preflights that
	// probe PG internals gate on this rather than on engine names, so
	// a future PG-family flavor inherits them by declaration.
	PostgresBackend bool

	// PGExtensionCatalog reports whether the engine natively hosts the
	// PostgreSQL extension ecosystem (ADR-0032): `--enable-pg-extension`
	// can resolve extension-owned column types (pgvector, hstore,
	// citext, ...) into IR [ExtensionType] on this engine's side of a
	// run. False for MySQL-family engines (they can only RECEIVE the
	// per-extension cross-engine translations) and, conservatively,
	// for `postgres-trigger` — extension passthrough through its
	// JSONB-mediated trigger capture path is unvalidated, so the
	// pre-capability refusal is preserved.
	PGExtensionCatalog bool

	// VerbatimExtensionTypes reports whether the engine can carry
	// UNCATALOGUED PG extension types verbatim (ADR-0047): its schema
	// surface records the raw type spelling and re-emits it exactly,
	// so verbatim columns round-trip only when BOTH sides of a run
	// declare this. True only for the vanilla `postgres` engine;
	// false (conservatively) for `postgres-trigger`, preserving the
	// pre-capability refusal until the trigger capture path is
	// validated against verbatim-typed columns.
	VerbatimExtensionTypes bool

	// TransactionKiller reports whether the engine's server side
	// enforces a wall-clock transaction killer (Vitess vtgate kills
	// transactions at ~20s by default). Drives conservative apply-path
	// defaults: the AIMD controller's p95 target latency (ADR-0052
	// DP-2: 5s = 20s with 4x headroom) and the startup warning when
	// `--apply-batch-size` exceeds the empirically-safe range
	// (GitHub #18: cross-region PlanetScale failed at batch=100,
	// worked at 25-50).
	TransactionKiller bool

	// BulkCopyBypassesForeignKeys reports that this engine's COLD/BULK
	// COPY write path loads rows with the server's foreign-key
	// enforcement OFF, so a copy into a table that ALREADY carries
	// foreign keys cannot fail on child-before-parent ordering.
	//
	// It is deliberately narrower than "the engine bypasses foreign
	// keys". On MySQL the CDC APPLIER bypasses them — `foreign_key_checks=0`
	// on every apply connection, Bug 164 — while the bulk-copy pool does
	// not, and it is the copy that roadmap item 140's preflight is about.
	// Today only the SQLite target declares this: every writable
	// connection opens with `_pragma=foreign_keys(0)` (ADR-0134) because
	// the writer emits foreign keys INLINE in CREATE TABLE and the copy
	// sweep is unordered, with the post-copy `PRAGMA foreign_key_check`
	// surfacing a genuine violation loudly on a fresh scan.
	//
	// The ZERO VALUE is the ENFORCING answer, which is the one that keeps
	// the item-140 preflight ARMED: a new target engine is fail-closed
	// (it gets the refusal) rather than silently un-gated. Which engines
	// declare what is kept honest by
	// TestEveryTargetCapableEngineDeclaresItsBulkCopyFKEnforcement in
	// internal/docsync.
	BulkCopyBypassesForeignKeys bool
}
