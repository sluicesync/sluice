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

// EnumSupport describes how an engine represents enumerations.
type EnumSupport uint8

// Recognised EnumSupport values.
const (
	EnumNone        EnumSupport = iota // engine has no native enum representation
	EnumColumnLevel                    // MySQL-style: ENUM declared on the column
	EnumTypeLevel                      // Postgres-style: CREATE TYPE ... AS ENUM
)

func (s EnumSupport) String() string {
	switch s {
	case EnumColumnLevel:
		return "column-level"
	case EnumTypeLevel:
		return "type-level"
	case EnumNone:
		return "none"
	default:
		return "unknown"
	}
}

// JSONSupport describes which JSON variants an engine supports.
type JSONSupport uint8

// JSONSupport variants:
//
//   - JSONNone:   no native JSON type
//   - JSONText:   only a textual JSON type
//   - JSONBinary: only a parsed/normalised JSON type
//   - JSONBoth:   both textual and binary variants
const (
	JSONNone JSONSupport = iota
	JSONText
	JSONBinary
	JSONBoth
)

func (s JSONSupport) String() string {
	switch s {
	case JSONText:
		return "text"
	case JSONBinary:
		return "binary"
	case JSONBoth:
		return "both"
	case JSONNone:
		return "none"
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

// TypeSet is a small fixed-size set of [ExtensionKind] values used by
// [Capabilities] to declare which extension types an engine supports.
//
// It is implemented as a bitset so capability checks are O(1) and cheap
// to copy. Up to 64 extension kinds are representable; the IR has far
// fewer.
type TypeSet uint64

// NewTypeSet returns a TypeSet containing the given kinds.
func NewTypeSet(kinds ...ExtensionKind) TypeSet {
	var s TypeSet
	for _, k := range kinds {
		s = s.With(k)
	}
	return s
}

// With returns a copy of s with k added.
func (s TypeSet) With(k ExtensionKind) TypeSet { return s | (1 << uint(k)) }

// Without returns a copy of s with k removed.
func (s TypeSet) Without(k ExtensionKind) TypeSet { return s &^ (1 << uint(k)) }

// Has reports whether k is present in s.
func (s TypeSet) Has(k ExtensionKind) bool { return s&(1<<uint(k)) != 0 }

// Capabilities declares what a database engine can do natively.
// Each [Engine] implementation returns a Capabilities value so the
// translator and pipeline can pick a strategy without hard-coding
// per-engine branches.
type Capabilities struct {
	// BulkLoad is the engine's preferred fast-load mechanism.
	BulkLoad BulkLoadMethod
	// CDC is the change-data-capture mechanism the engine exposes.
	CDC CDCMethod
	// SchemaScope is the table-namespacing model.
	SchemaScope SchemaScope
	// SupportedTypes lists the extension types the engine handles natively.
	SupportedTypes TypeSet
	// SupportsCheckConstraint reports whether CHECK constraints are honoured.
	SupportsCheckConstraint bool
	// SupportsGeneratedColumns reports whether generated/computed columns are supported.
	SupportsGeneratedColumns bool
	// SupportsPartitioning reports whether table partitioning is supported.
	SupportsPartitioning bool
	// EnumSupport describes how the engine represents enumerations.
	EnumSupport EnumSupport
	// JSONSupport describes which JSON variants the engine supports.
	JSONSupport JSONSupport
	// UnsignedIntegers reports whether the engine has native unsigned integer types.
	UnsignedIntegers bool

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
}
