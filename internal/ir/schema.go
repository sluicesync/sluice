// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// Schema is a dialect-neutral description of a database's structure:
// its tables, their columns, indexes, and foreign keys.
type Schema struct {
	// Tables in the schema. Order is not significant for correctness
	// but is preserved from the source for stable diffs and logs.
	Tables []*Table
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
