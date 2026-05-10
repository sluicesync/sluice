// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// Schema is a dialect-neutral description of a database's structure:
// its tables, their columns, indexes, and foreign keys.
type Schema struct {
	// Tables in the schema. Order is not significant for correctness
	// but is preserved from the source for stable diffs and logs.
	Tables []*Table

	// Views in the schema. Empty on engines that don't support views or
	// when the source has none. Materialized views (Postgres) are
	// included with View.Materialized = true. View support is a
	// schema-only round-trip in v1: the writer creates the view from
	// View.Definition; cross-engine view-definition translation is a
	// future Phase 3 effort. See `docs/dev/design-schema-completeness.md`.
	Views []*View
}

// View is a dialect-neutral description of a SQL view (regular or
// materialized). The definition is the SQL body of the view's SELECT
// statement, captured verbatim from the source dialect.
//
// View support is intentionally minimal in v1 ("Phase 1" of the proto-
// ADR `docs/dev/design-schema-completeness.md`):
//
//   - Schema-only round-trip: source views are read from the catalog,
//     emitted to the target via `CREATE VIEW`/`CREATE MATERIALIZED VIEW`.
//   - Same-engine pairs work without translation.
//   - Cross-engine pairs emit the source-dialect definition verbatim
//     and rely on the loud-failure tenet — non-portable definitions
//     surface as a target rejection at apply time, not silent corruption.
//   - `--view-override` (future Phase 3) will be the operator escape
//     for cases the eventual translator can't handle.
//
// Materialized views (Postgres-only today): the writer emits
// `CREATE MATERIALIZED VIEW ... WITH DATA` so the target's view is
// populated immediately from the just-loaded target tables. Continuous
// refresh on CDC is a Phase 2 future enhancement.
type View struct {
	// Schema is the namespace the view belongs to. Empty for engines
	// with a flat scope (such as MySQL); set for engines with
	// namespaced schemas (such as PostgreSQL). Mirrors Table.Schema.
	Schema string

	// Name is the view's identifier within its schema.
	Name string

	// Definition is the SQL body of the view, e.g.
	//   `SELECT id, email FROM users WHERE active = 1`.
	// Captured from the source dialect's `pg_views.definition` /
	// `information_schema.views.view_definition`. Identifier quoting
	// is left as the source emitted it — the writer is responsible
	// for any normalization needed to apply on the target dialect.
	Definition string

	// DefinitionDialect is the dialect the Definition text came from,
	// in the readers' canonical form ("mysql" or "postgres"). Mirrors
	// the same convention as [Column.GeneratedExprDialect] and
	// [CheckConstraint.ExprDialect]. Cross-engine writers compare this
	// against their own dialect: equal → emit verbatim; differ → in
	// Phase 1 emit verbatim and rely on loud failure, in Phase 3 route
	// through a SELECT-grammar translator. Empty means the producer
	// didn't tag the dialect (older IR-construction paths, hand-built
	// test fixtures).
	DefinitionDialect string

	// Columns are the projected columns of the view, in declaration
	// order. May be empty when the source reader can't cleanly derive
	// them (deriving requires parsing the SELECT body). Useful for
	// `sluice schema diff` to compare expected-vs-actual column lists
	// when the reader does populate them.
	Columns []*Column

	// Materialized indicates a Postgres-style materialized view (one
	// whose query result is physically stored on disk and refreshed
	// explicitly via `REFRESH MATERIALIZED VIEW`). False for regular
	// views (the dynamic-SELECT form both engines support). MySQL
	// has no materialized-view concept; this field is always false on
	// MySQL sources.
	//
	// Phase 1 writer emits `WITH DATA` for materialized views (refresh
	// from source on creation); Phase 2 will add CDC-driven refresh.
	Materialized bool

	// Comment is the view-level comment, if any. Mirrors Table.Comment.
	Comment string
}

// Table describes a single relational table.
type Table struct {
	// Schema is the namespace the table belongs to. Empty for engines
	// with a flat scope (such as MySQL); set for engines with
	// namespaced schemas (such as PostgreSQL).
	Schema string
	// Name is the table's identifier within its schema.
	Name string
	// Columns in declaration order.
	Columns []*Column
	// PrimaryKey, if any. Nil for tables without a declared primary key.
	PrimaryKey *Index
	// Indexes excluding the primary key.
	Indexes []*Index
	// ForeignKeys declared on this table.
	ForeignKeys []*ForeignKey
	// CheckConstraints declared on this table. Both column-scoped
	// (e.g. `qty INT CHECK (qty >= 0)`) and table-scoped (e.g.
	// `CHECK (start_date <= end_date)`) CHECKs land here — both
	// engines normalize them into information_schema as table-level
	// entries, and the IR mirrors that.
	CheckConstraints []*CheckConstraint
	// Comment is the table-level comment, if any.
	Comment string
}

// CheckConstraint represents a single CHECK clause on a table.
//
// Translation is layered: identifier quoting is normalized at the
// read boundary, a small set of high-frequency operator/function
// translations runs at the writer boundary when ExprDialect != the
// target engine (see ADR-0016), and anything not covered by either
// pass falls through verbatim — non-portable expressions (e.g.
// MySQL's IF(...) versus PG's CASE) still fail loudly on the target
// rather than be guessed at.
type CheckConstraint struct {
	// Name is the constraint name (system-generated when not
	// explicitly named at source). Preserved through DDL emit so a
	// target's pg_dump shape stays diffable against the source.
	Name string
	// Expr is the constraint expression in the source dialect's
	// syntax, with engine-specific identifier quoting stripped.
	Expr string
	// ExprDialect is the dialect the Expr text came from, in the
	// readers' canonical form ("mysql" or "postgres"). Empty means
	// the producer didn't tag the dialect (older IR-construction
	// paths, hand-built test fixtures). Writers compare this against
	// their own dialect: equal → emit verbatim; differ → run the
	// cross-dialect translation pass first. See ADR-0016 for the
	// layered translation policy.
	ExprDialect string
}

// Column describes a single column.
type Column struct {
	// Name is the column identifier.
	Name string
	// Type is the IR type. It is required and never nil for a
	// well-formed Column.
	Type Type
	// Nullable reports whether the column accepts NULL.
	Nullable bool
	// Default is the column's DEFAULT clause, modelled as a typed sum
	// to keep "no default", "literal default", and "expression default"
	// distinguishable without sentinel strings.
	Default DefaultValue
	// Comment is the column-level comment, if any.
	Comment string

	// GeneratedExpr is the SQL expression a generated column is
	// computed from, in the source dialect's syntax. Empty when the
	// column is not generated. Translation is layered: identifier
	// quoting is normalized at the read boundary, a small set of
	// high-frequency operator/function translations runs at the
	// writer boundary when GeneratedExprDialect differs from the
	// target engine (see ADR-0016), and anything not covered by
	// either pass falls through verbatim — non-portable constructs
	// still surface as a target rejection rather than be guessed-at.
	GeneratedExpr string

	// GeneratedStored, when true, signals STORED (PG default; MySQL
	// explicit). False signals VIRTUAL (the implicit "computed at
	// read time" form). Generated columns with GeneratedExpr non-
	// empty must have this set; readers default to STORED when the
	// source's metadata is ambiguous.
	GeneratedStored bool

	// GeneratedExprDialect is the dialect the GeneratedExpr text
	// came from, in the readers' canonical form ("mysql" or
	// "postgres"). Empty means the producer didn't tag the dialect
	// (older IR-construction paths, hand-built test fixtures).
	// Writers compare this against their own dialect: equal → emit
	// verbatim; differ → run the cross-dialect translation pass
	// first. See ADR-0016 for the layered translation policy.
	GeneratedExprDialect string
}

// IsGenerated reports whether the column is a generated/computed
// column (its value is derived from an expression rather than written
// directly). Equivalent to checking GeneratedExpr != "" but reads
// better at call sites that gate INSERT/UPDATE column lists.
func (c *Column) IsGenerated() bool {
	return c.GeneratedExpr != ""
}

// DefaultValue is a sealed interface describing a column's DEFAULT
// clause. Use one of [DefaultNone], [DefaultLiteral], or
// [DefaultExpression]; new variants must be added to this package.
type DefaultValue interface {
	isDefaultValue()
}

// DefaultNone indicates the column has no DEFAULT clause.
type DefaultNone struct{}

// DefaultLiteral is a DEFAULT clause expressed as a literal value
// (the raw, dialect-neutral textual representation).
type DefaultLiteral struct {
	Value string
}

// DefaultExpression is a DEFAULT clause expressed as a SQL expression
// such as CURRENT_TIMESTAMP or nextval('seq').
//
// Dialect tags the source engine the Expr text was read from so that
// cross-engine writers can route the expression through their dialect
// translator (see ADR-0016). Mirrors the same pattern as
// [Column.GeneratedExprDialect] and [CheckConstraint.ExprDialect].
// Empty when the expression text is dialect-neutral or matches the
// writer's dialect (the verbatim-passthrough path).
type DefaultExpression struct {
	Expr    string
	Dialect string
}

func (DefaultNone) isDefaultValue()       {}
func (DefaultLiteral) isDefaultValue()    {}
func (DefaultExpression) isDefaultValue() {}

// IndexKind identifies the storage structure of an index. Engines may
// support a subset of these.
type IndexKind uint8

// Recognised IndexKind values.
const (
	IndexKindUnspecified IndexKind = iota
	IndexKindBTree
	IndexKindHash
	IndexKindGIN
	IndexKindGIST
	IndexKindFullText
	IndexKindSpatial
)

func (k IndexKind) String() string {
	switch k {
	case IndexKindBTree:
		return "btree"
	case IndexKindHash:
		return "hash"
	case IndexKindGIN:
		return "gin"
	case IndexKindGIST:
		return "gist"
	case IndexKindFullText:
		return "fulltext"
	case IndexKindSpatial:
		return "spatial"
	case IndexKindUnspecified:
		return "unspecified"
	default:
		return "unknown"
	}
}

// Index describes a table index.
type Index struct {
	// Name of the index. May be empty for primary keys on engines that
	// don't name them explicitly.
	Name string
	// Columns covered by the index, in order.
	Columns []IndexColumn
	// Unique reports whether the index enforces uniqueness.
	Unique bool
	// Kind is the storage structure (btree, hash, gin, etc.).
	Kind IndexKind
	// Method is the verbatim engine-specific access-method name when
	// it falls outside the [IndexKind] enum — typically extension-
	// introduced methods (PG's ivfflat / hnsw via pgvector). The IR
	// preserves the bareword so a same-engine writer can emit it
	// verbatim under the engine's extension-passthrough policy
	// (ADR-0032). Empty when Kind alone suffices (the common case).
	//
	// Engines without extension-aware index methods leave this empty;
	// the writer's emit dispatch defaults to Kind.
	Method string
}

// IndexColumn is a single entry within an index. Most entries name a
// column directly via Column; functional/expression indexes (MySQL
// 8.0.13+ functional indexes, Postgres expression indexes) instead
// carry their indexed expression in Expression with Column empty.
// Exactly one of Column / Expression is non-empty for a well-formed
// IndexColumn.
type IndexColumn struct {
	// Column is the indexed column's name. Empty when this entry is an
	// expression index entry (Expression non-empty).
	Column string
	// Expression is the SQL expression indexed by this entry, in the
	// source dialect's syntax with engine-specific identifier quoting
	// stripped at the read boundary (same normalization as
	// CheckConstraint.Expr / Column.GeneratedExpr). Empty for the
	// common case of a plain column entry. When non-empty, Column is
	// empty.
	Expression string
	// ExpressionDialect is the source dialect the Expression text came
	// from ("mysql" or "postgres"; both MySQL flavors share "mysql"
	// since the wire dialect is identical). Empty for plain column
	// entries. The DDL emitters compare this against their own
	// dialect and run the layered ADR-0016 translator only when they
	// differ — a MySQL `json_unquote(json_extract(...))` index
	// expression rewrites to PG `->>'...'` on emit; a same-dialect
	// expression passes through verbatim. See ADR-0016.
	ExpressionDialect string
	// Desc indicates the column is indexed in descending order.
	Desc bool
	// Length is a prefix length for prefix indexes (MySQL); zero means
	// the entire column value is indexed.
	Length int
}

// FKAction is the action to take on a referenced row's UPDATE or DELETE.
type FKAction uint8

// Recognised FKAction values.
const (
	FKActionNoAction FKAction = iota
	FKActionRestrict
	FKActionCascade
	FKActionSetNull
	FKActionSetDefault
)

func (a FKAction) String() string {
	switch a {
	case FKActionNoAction:
		return "NO ACTION"
	case FKActionRestrict:
		return "RESTRICT"
	case FKActionCascade:
		return "CASCADE"
	case FKActionSetNull:
		return "SET NULL"
	case FKActionSetDefault:
		return "SET DEFAULT"
	default:
		return "unknown"
	}
}

// ForeignKey describes a foreign-key constraint.
type ForeignKey struct {
	// Name of the constraint. May be empty if unnamed in the source.
	Name string
	// Columns in the referencing (child) table.
	Columns []string
	// ReferencedSchema and ReferencedTable identify the parent table.
	// ReferencedSchema is empty for engines with flat scope.
	ReferencedSchema string
	ReferencedTable  string
	// ReferencedColumns are the parent table columns this FK points at.
	ReferencedColumns []string
	// OnDelete and OnUpdate are the referential actions.
	OnDelete FKAction
	OnUpdate FKAction
}
