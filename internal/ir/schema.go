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
	// ExcludeConstraints declared on this table (PostgreSQL only).
	// Carried verbatim via pg_get_constraintdef; cross-engine targets
	// (MySQL) refuse loudly because no equivalent exists. ADR-0053.
	// MySQL sources never populate this slice (MySQL has no EXCLUDE
	// constraint type).
	ExcludeConstraints []*ExcludeConstraint

	// RLSEnabled mirrors PG's `pg_class.relrowsecurity` — true when
	// the table has `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` in
	// effect on the source side. PG-only: MySQL has no equivalent
	// concept and always leaves this false. Populated by the PG
	// SchemaReader; consumed by the PG SchemaWriter to re-emit the
	// ENABLE on the target so policies it creates are actually
	// enforced (without ENABLE, CREATE POLICY rows are inert). See
	// ADR-0063 and [Policy] for the full RLS-IR contract.
	RLSEnabled bool
	// RLSForced mirrors PG's `pg_class.relforcerowsecurity` — true
	// when the table has `ALTER TABLE ... FORCE ROW LEVEL SECURITY`
	// in effect, which extends RLS enforcement to the table owner
	// (without FORCE, the owner bypasses by default). PG-only.
	// Meaningful only when RLSEnabled is true; the writer emits FORCE
	// only when both are set. See ADR-0063.
	RLSForced bool
	// Policies are the `pg_policies` rows attached to this table on
	// the source side. The PG SchemaWriter re-emits each as a
	// `CREATE POLICY` after the table's `ENABLE ROW LEVEL SECURITY`,
	// so the target carries the same per-tenant filter/check rules
	// the source had (closes task #52's silent-security-regression
	// failure mode: a target schema arriving without policies).
	// PG-only: MySQL has no RLS surface; the MySQL writer warns once
	// per stream when this slice is non-empty (cross-engine PG →
	// MySQL drops policies — see ADR-0063). MySQL sources never
	// populate this slice.
	Policies []*Policy

	// Comment is the table-level comment, if any.
	Comment string
}

// Policy is a dialect-neutral description of a PostgreSQL row-level-
// security policy (`pg_policies` row). PG-only by definition — MySQL
// has no RLS surface. Field names mirror `pg_policies` columns so the
// reader / writer correspondence is direct; the writer renders these
// back as `CREATE POLICY <name> ON <table> ...` after the table's
// `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` (the ENABLE must come
// first or the CREATE POLICY rows land inert).
//
// Captured into the IR (rather than carried as a side-channel) so the
// existing IR-first translation contract holds: the PG SchemaReader
// emits one [*Policy] per source row, the PG SchemaWriter consumes
// them on emit, and cross-engine writers (today: MySQL) detect their
// presence and warn loudly per ADR-0063's loud-failure tenet. See
// ADR-0063 for the motivation and the bug-74-style test matrix the
// IR contract pins (Command × Permissive × USING/CHECK shape ×
// ENABLE/FORCE).
type Policy struct {
	// Name is the policy identifier within the table's namespace
	// (`pg_policies.policyname`). System-generated when not
	// explicitly named at source. Preserved verbatim so the
	// target's pg_dump shape stays diffable against the source.
	Name string

	// Command is the operation the policy applies to: one of "ALL",
	// "SELECT", "INSERT", "UPDATE", or "DELETE". Mirrors
	// `pg_policies.cmd`. "ALL" matches every command (the PG
	// default when `CREATE POLICY` omits the `FOR ...` clause).
	Command string

	// Permissive is true for permissive policies (the default —
	// rows the policy admits ride OR'd with other permissive
	// policies), false for restrictive (rows must satisfy AND'd
	// with permissive). Mirrors `pg_policies.permissive` (which
	// returns "PERMISSIVE" / "RESTRICTIVE" — readers map to bool
	// for compactness; the writer renders the keyword back).
	Permissive bool

	// Roles is the role-list the policy applies to. Mirrors
	// `pg_policies.roles`. PG stores `{public}` (a one-element
	// list) for the default "every role" case; readers carry that
	// through verbatim and the writer re-emits `TO public`. An
	// empty slice is a sluice-bug condition the writer refuses
	// loudly — PG always populates the list.
	Roles []string

	// Using is the `USING (...)` expression text, in PG dialect
	// (the only RLS dialect today). Empty for INSERT-scoped
	// policies that supply only a WITH CHECK. Mirrors
	// `pg_policies.qual`. The writer emits `USING (<text>)`
	// only when non-empty; the parentheses are added by the
	// emitter so the IR text doesn't need to carry them.
	Using string

	// Check is the `WITH CHECK (...)` expression text, in PG
	// dialect. Empty for SELECT-scoped policies that supply only
	// USING. Mirrors `pg_policies.with_check`. The writer emits
	// `WITH CHECK (<text>)` only when non-empty. PG falls back to
	// USING as the default WITH CHECK for INSERT/UPDATE policies
	// when WITH CHECK is omitted; the reader captures the
	// catalog's explicit value (which may be NULL on those
	// fallback-to-USING cases) so the round-trip stays faithful.
	Check string
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

// ExcludeConstraint represents a PostgreSQL EXCLUDE constraint
// (pg_constraint.contype = 'x'). PG-only by nature — MySQL has no
// equivalent. Carried verbatim via pg_get_constraintdef as opaque
// text: same shape as ir.VerbatimType (ADR-0051), sibling tier for
// the constraint surface. Cross-engine targets refuse loudly via
// checkCrossEngineSupportable; same-engine PG → PG re-emits the
// Definition string literally. ADR-0053.
type ExcludeConstraint struct {
	// Name is the constraint name (system-generated when not
	// explicitly named at source). Preserved through DDL emit so a
	// target's pg_dump shape stays diffable against the source.
	Name string
	// Definition is the verbatim pg_get_constraintdef output for
	// this constraint — the constraint body without the `ALTER
	// TABLE … ADD CONSTRAINT <name>` wrapper. Example values:
	//   "EXCLUDE USING gist (builds_id_range WITH &&) WHERE ((builds_id_range IS NOT NULL))"
	//   "EXCLUDE USING gist (rotation_id WITH =, tstzrange(starts_at, ends_at, '[)'::text) WITH &&)"
	//   "EXCLUDE USING gist (...) WHERE (...) DEFERRABLE INITIALLY DEFERRED"
	// Includes USING <index_method>, (col WITH op) pairs, optional
	// WHERE predicate, and DEFERRABLE modifiers — everything the PG
	// writer needs to re-emit identically. Empty Definition is a
	// sluice-bug condition (the reader should never populate the
	// slice with an empty entry); the PG writer refuses loudly if
	// seen.
	Definition string
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

	// SourceColumnType is the column's pre-override IR type, captured
	// when [translate.ApplyMappings] rewrites Type from a per-column
	// `--type-override`. Nil when no override fired (the common case).
	//
	// Writers consult this to disambiguate value shapes that are
	// indistinguishable from the post-override Type + bytes alone.
	// The load-bearing case is Bug 47: MySQL JSON source value `{}`
	// arrives as `[]byte("{}")` and must round-trip as JSON object
	// `{}`, while a PG empty-array `text[]` value with
	// `--type-override=col=jsonb` arrives as the same bytes but must
	// land as JSON array `[]`. Both paths converge at
	// `prepareValue([]byte("{}"), ir.JSON{...})`; the only way to
	// tell them apart is to know that the second came from an
	// `ir.Array` source — i.e. SourceColumnType is non-nil and an
	// [Array].
	//
	// Producers other than translate.ApplyMappings leave this nil;
	// readers should never populate it (the field carries
	// override-context, not the raw source-engine type).
	SourceColumnType Type

	// SluiceInjected marks a column that sluice itself added to the
	// IR (i.e. one that does NOT exist on the operator's source
	// schema). Today the only producer is
	// [translate.InjectShardColumn] — the Shape-A discriminator
	// column the operator opts into via `--inject-shard-column
	// NAME=VALUE` (ADR-0048). The marker is a provenance bit, not a
	// behaviour bit: it changes nothing about emit / values, only
	// about how *diff* / *verify* interpret the column on the
	// consolidated target.
	//
	// [DiffSchemas] treats a target-side column with
	// SluiceInjected=true as an *expected* extra rather than drift:
	// it is suppressed from `ColumnsExtra` when absent on the
	// source-side expected schema (Shape-A diff against a sharded
	// source whose schema doesn't carry the discriminator), but the
	// inverse check still fires — the column must remain NOT NULL
	// and present on the actual target side. Mirrors the
	// `SourceColumnType` precedent: a small single-purpose
	// provenance field, lighter than a sealed enum.
	//
	// Source-side schema readers leave this false; only translation
	// passes that *add* a column set it to true.
	SluiceInjected bool
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
//
// Append-only: enum values are persisted as uint8 in the backup
// envelope (manifest.IndexKind). Reordering or removing existing
// values silently corrupts older manifests; new kinds get appended.
const (
	IndexKindUnspecified IndexKind = iota
	IndexKindBTree
	IndexKindHash
	IndexKindGIN
	IndexKindGIST
	IndexKindFullText
	IndexKindSpatial
	IndexKindSPGist
	IndexKindBRIN
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
	case IndexKindSPGist:
		return "spgist"
	case IndexKindBRIN:
		return "brin"
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

	// IncludeColumns are the non-key payload columns of a covering
	// index (Postgres `INCLUDE (...)`; SQL Server's equivalent). These
	// are stored in the leaf pages but are NOT part of the index key —
	// they don't affect ordering, comparison, or uniqueness scope.
	// Flattening them into Columns silently changes index semantics
	// (catalog Bug 19b), so they get their own slot. Empty for the
	// common (non-covering) case and for engines without INCLUDE.
	IncludeColumns []string

	// Predicate is the WHERE clause of a partial index (Postgres /
	// SQLite), in the source dialect's syntax with engine-specific
	// identifier quoting stripped at the read boundary — same
	// normalization and layered-translation policy as
	// [CheckConstraint.Expr]. Empty for a full (non-partial) index.
	// Dropping it silently turns a partial index into a full one —
	// different size, different query plans, and a silently widened
	// uniqueness scope if the index is UNIQUE (catalog Bug 19a).
	Predicate string
	// PredicateDialect is the dialect Predicate was read from, in the
	// readers' canonical form ("mysql" or "postgres"). Writers compare
	// it against their own dialect: equal → emit verbatim; differ →
	// run the ADR-0016 cross-dialect translation pass first. Empty
	// when there is no predicate. Mirrors [CheckConstraint.ExprDialect].
	PredicateDialect string
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
	// NullsFirst overrides the default NULL ordering for this index
	// column (Postgres `NULLS FIRST` / `NULLS LAST`). nil means "engine
	// default" — PG's default is NULLS LAST for ASC and NULLS FIRST for
	// DESC, so the reader only sets this when the stored ordering
	// deviates from that default, keeping emitted DDL minimal and
	// diff-stable. *NullsFirst == true → `NULLS FIRST`; false →
	// `NULLS LAST`. Engines without per-column NULL ordering leave it
	// nil.
	NullsFirst *bool
	// Length is a prefix length for prefix indexes (MySQL); zero means
	// the entire column value is indexed.
	Length int
	// OperatorClass is the PG operator-class name attached to this
	// index column, when one is required for the access method (PG
	// only — MySQL has no equivalent surface). The load-bearing case
	// is pgvector's hnsw access method, which has no default operator
	// class: every column entry must specify one of `vector_l2_ops`
	// / `vector_ip_ops` / `vector_cosine_ops` / `vector_l1_ops`. The
	// IR carries the bareword verbatim; the same-engine writer emits
	// `<column> <opclass>` after the column reference. Empty when the
	// AM has a default opclass for the column type (the common case
	// for btree/gin/gist over built-in types).
	OperatorClass string
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
