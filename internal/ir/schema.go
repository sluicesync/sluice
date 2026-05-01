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
	// Comment is the table-level comment, if any.
	Comment string
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
type DefaultExpression struct {
	Expr string
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

// IndexColumn is a single column entry within an index.
type IndexColumn struct {
	// Column is the indexed column's name.
	Column string
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
