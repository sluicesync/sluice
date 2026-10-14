// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Which tables a binlog DDL can reshape (Bug 301)
//
// Two questions about a DDL QueryEvent need the same answer:
//
//   - the reader's schema-cache invalidation: must this statement drop
//     the decode shapes we hold? It used to key on the event's DEFAULT
//     database alone, so `USE other; ALTER TABLE app.t ...` left app.t's
//     cached shape in place and every later row of app.t decoded against
//     the pre-DDL column list. A same-count, same-family reshape (a column
//     reorder, a rename swap) then landed its values under the wrong
//     column names with no error — at the binlog HEAD, no lag required.
//   - the look-ahead proof ([CDCReader.proveCatalogShape]): does a DDL
//     between the reader's position and the catalog read reshape THIS
//     table?
//
// [classifyDDLReach] answers both from a real SQL parser (the Vitess one
// the charset guard already uses), never a regex. Its failure direction is
// fixed: a statement it cannot read, or a statement shape it does not
// know, reaches EVERY table. "Neutral" — cannot change any table's column
// shape — is only ever a positive finding over a fully parsed statement
// (or a short list of statement leaders that by construction name no
// table's columns), because a wrong "neutral" is a silent mis-decode and a
// wrong "reaches" is at worst a cache reload or a loud refusal.

package mysql

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// ddlReach is what a DDL statement can reshape.
type ddlReach struct {
	// neutral: the statement changes no table's column list, column
	// types, or primary key (an index, a table option, a TRUNCATE, a
	// view or routine, a grant). Decode shapes survive it.
	neutral bool
	// all: the statement could not be bounded to named tables (it did
	// not parse, or its shape is not one this classifier knows, or it
	// drops a whole database). Treated as reaching every table.
	all bool
	// tables: the qualified tables it reshapes when neither of the
	// above holds. An unqualified name takes the event's default
	// database; with no default database it is unbounded (all).
	tables []ddlTableRef
	// databases: whole databases it reshapes (DROP DATABASE).
	databases []string

	// inPlace: a single-table ALTER that keeps every column's NAME and
	// POSITION and may change only some columns' types (MODIFY or CHANGE
	// without renaming, FIRST or AFTER; ALTER COLUMN; CONVERT TO
	// CHARACTER SET). retyped names those columns, or retypedAll is set
	// for CONVERT TO. A reader can still decode by the current column
	// list across such a DDL, provided each retyped column's recorded type
	// decodes the same way — checked per event (cdc_ddl_lookahead.go).
	inPlace    bool
	retyped    []string
	retypedAll bool
}

// ddlTableRef names one table a DDL reshapes. Compared case-insensitively
// by [ddlReach.reaches]: over-matching a name under a case-sensitive server
// costs a cache reload or a refusal, under-matching one costs a mis-decode.
type ddlTableRef struct {
	schema, table string
}

// reaches reports whether the statement can reshape table schema.table.
func (d ddlReach) reaches(schema, table string) bool {
	if d.neutral {
		return false
	}
	if d.all {
		return true
	}
	for _, t := range d.tables {
		if strings.EqualFold(t.schema, schema) && strings.EqualFold(t.table, table) {
			return true
		}
	}
	for _, db := range d.databases {
		if strings.EqualFold(db, schema) {
			return true
		}
	}
	return false
}

// reachesDatabase reports whether the statement can reshape any table of a
// database inScope admits.
func (d ddlReach) reachesDatabase(inScope func(string) bool) bool {
	if d.neutral {
		return false
	}
	if d.all {
		return true
	}
	for _, t := range d.tables {
		if inScope(t.schema) {
			return true
		}
	}
	for _, db := range d.databases {
		if inScope(db) {
			return true
		}
	}
	return false
}

// neutralStatementLeaders are statement prefixes (upper-cased, single
// spaces) that cannot name a table column by their grammar. They are
// consulted only when the parser cannot read a statement, so they are the
// one place a non-parse becomes "neutral": each entry is an account,
// privilege, routine, trigger, event or maintenance statement.
var neutralStatementLeaders = []string{
	"GRANT ", "REVOKE ",
	"CREATE USER ", "ALTER USER ", "DROP USER ", "RENAME USER ",
	"CREATE ROLE ", "DROP ROLE ", "SET PASSWORD ", "SET DEFAULT ROLE ",
	"FLUSH ", "ANALYZE TABLE ", "OPTIMIZE TABLE ", "REPAIR TABLE ",
	"ANALYZE LOCAL TABLE ", "ANALYZE NO_WRITE_TO_BINLOG TABLE ",
	"OPTIMIZE LOCAL TABLE ", "OPTIMIZE NO_WRITE_TO_BINLOG TABLE ",
	"CREATE TRIGGER ", "DROP TRIGGER ",
	"CREATE FUNCTION ", "DROP FUNCTION ", "ALTER FUNCTION ",
	"CREATE PROCEDURE ", "DROP PROCEDURE ", "ALTER PROCEDURE ",
	"CREATE EVENT ", "ALTER EVENT ", "DROP EVENT ",
	"CREATE AGGREGATE FUNCTION ",
	"INSTALL PLUGIN ", "UNINSTALL PLUGIN ",
}

// classifyDDLReach classifies one DDL statement text. defaultSchema is the
// event's default database (the QueryEvent schema, or the `use` clause of
// a SHOW BINLOG EVENTS Info).
func classifyDDLReach(stmt, defaultSchema string) ddlReach {
	stmt = stripLeadingSQLComments(stripLeadingSessionAssignments(stripLeadingSQLComments(stmt)))
	unbounded := ddlReach{all: true}
	if stmt == "" {
		return unbounded
	}
	p := charsetDDLParser()
	if p == nil {
		return unbounded
	}
	// MariaDB's ALTER prefix (ONLINE / IGNORE / IF EXISTS / WAIT n) is not
	// Vitess syntax; normalise it the same way the charset guard does.
	// The normaliser skips every comment in the prefix, so it is not used on
	// a statement carrying one the server may execute (`/*!…*/`, `/*+…*/`):
	// the table name may be inside it.
	executable := strings.Contains(stmt, "/*!") || strings.Contains(stmt, "/*+")
	if norm, _, _, isAlter := normalizeAlterPrefix(stmt); isAlter && !executable {
		stmt = norm
	}
	parsed, err := p.ParseStrictDDL(stmt)
	if err != nil {
		if hasNeutralLeader(stmt) {
			return ddlReach{neutral: true}
		}
		if r, ok := classifyUnparsedDDL(stmt, defaultSchema); ok {
			return r
		}
		return unbounded
	}
	ref := func(t sqlparser.TableName) (ddlTableRef, bool) {
		schema := t.Qualifier.String()
		if schema == "" {
			schema = defaultSchema
		}
		if schema == "" {
			return ddlTableRef{}, false
		}
		return ddlTableRef{schema: schema, table: t.Name.String()}, true
	}
	refs := func(names ...sqlparser.TableName) ddlReach {
		out := ddlReach{}
		for _, n := range names {
			r, ok := ref(n)
			if !ok {
				return unbounded
			}
			out.tables = append(out.tables, r)
		}
		return out
	}
	switch s := parsed.(type) {
	case *sqlparser.AlterTable:
		if !s.FullyParsed {
			return unbounded
		}
		shape := alterTableShape(s)
		if shape.neutral {
			return ddlReach{neutral: true}
		}
		out := refs(s.Table)
		if shape.inPlace && !out.all {
			out.inPlace, out.retyped, out.retypedAll = true, shape.retyped, shape.retypedAll
			return out
		}
		for _, opt := range s.AlterOptions {
			// ALTER TABLE a RENAME TO b reshapes b too: a row event for b
			// after it decodes against whatever b now is.
			if rn, ok := opt.(*sqlparser.RenameTableName); ok && !out.all {
				out.tables = append(out.tables, refs(rn.Table).tables...)
			}
		}
		return out
	case *sqlparser.CreateTable:
		return refs(s.Table)
	case *sqlparser.DropTable:
		return refs(s.FromTables...)
	case *sqlparser.RenameTable:
		var names []sqlparser.TableName
		for _, pair := range s.TablePairs {
			names = append(names, pair.FromTable, pair.ToTable)
		}
		return refs(names...)
	case *sqlparser.TruncateTable,
		*sqlparser.CreateView, *sqlparser.AlterView, *sqlparser.DropView,
		*sqlparser.CreateProcedure, *sqlparser.DropProcedure,
		*sqlparser.CreateDatabase, *sqlparser.AlterDatabase:
		// TRUNCATE empties a table without reshaping it; a view or routine
		// has no row events of its own; a database's DEFAULT charset
		// applies to columns created later, not to existing ones.
		return ddlReach{neutral: true}
	}
	switch s := parsed.(type) {
	case *sqlparser.DropDatabase:
		if db := s.DBName.String(); db != "" {
			return ddlReach{databases: []string{db}}
		}
		return unbounded
	case sqlparser.DDLStatement, sqlparser.DBDDLStatement:
		// Any DDL shape this classifier does not list is unbounded.
		return unbounded
	}
	// Not DDL at all: transaction control (SAVEPOINT, ROLLBACK TO), a
	// statement-logged DML, a SET. None names a column definition.
	return ddlReach{neutral: true}
}

// alterShape is what an ALTER TABLE's options do to the table's columns.
type alterShape struct {
	neutral    bool
	inPlace    bool
	retyped    []string
	retypedAll bool
}

// alterTableShape classifies an ALTER TABLE's options: neutral when every
// option leaves the column list, the column types and the primary key
// exactly as they were; in place when, beyond that, options only change
// some columns' definitions without renaming or moving them; neither
// otherwise.
func alterTableShape(s *sqlparser.AlterTable) alterShape {
	out := alterShape{neutral: true, inPlace: true}
	retype := func(name string) {
		out.neutral = false
		out.retyped = append(out.retyped, name)
	}
	// outOfPlace — generated: a definition carrying `AS (expr)` changes whether the
	// decoder keeps the column's value at all, which no per-event type
	// check sees.
	//
	// keyed: a definition carrying a column-level key (`PRIMARY KEY`,
	// `UNIQUE`) may change the primary key, which decides the before-image
	// narrowing.
	outOfPlace := func(def *sqlparser.ColumnDefinition) bool {
		return def.Type != nil && def.Type.Options != nil &&
			(def.Type.Options.As != nil || def.Type.Options.KeyOpt != sqlparser.ColKeyNone)
	}
	for _, opt := range s.AlterOptions {
		switch o := opt.(type) {
		case *sqlparser.AddIndexDefinition:
			if o.IndexDefinition == nil || o.IndexDefinition.Info == nil ||
				o.IndexDefinition.Info.Type == sqlparser.IndexTypePrimary {
				return alterShape{}
			}
		case *sqlparser.DropKey:
			// `DROP PRIMARY KEY`, and every other spelling of it: `DROP INDEX
			// `PRIMARY``, `DROP KEY `PRIMARY``, `DROP CONSTRAINT `PRIMARY``.
			if o.Type == sqlparser.PrimaryKeyType || strings.EqualFold(o.Name.String(), "PRIMARY") {
				return alterShape{}
			}
		case *sqlparser.AddConstraintDefinition, sqlparser.AlgorithmValue, *sqlparser.AlterCheck,
			*sqlparser.AlterIndex, *sqlparser.KeyState, *sqlparser.Force, *sqlparser.LockOption,
			*sqlparser.RenameIndex, *sqlparser.Validation, sqlparser.TableOptions,
			*sqlparser.OrderByOption, *sqlparser.TablespaceOperation:
			// Indexes, constraints, table options (a table's DEFAULT charset
			// included — CONVERT TO is AlterCharset, below), algorithm and
			// lock clauses, row ordering, tablespace moves.
		case *sqlparser.ModifyColumn:
			if o.NewColDefinition == nil || o.First || o.After != nil || outOfPlace(o.NewColDefinition) {
				return alterShape{}
			}
			retype(o.NewColDefinition.Name.String())
		case *sqlparser.ChangeColumn:
			if o.OldColumn == nil || o.NewColDefinition == nil || o.First || o.After != nil || outOfPlace(o.NewColDefinition) ||
				!o.OldColumn.Name.Equal(o.NewColDefinition.Name) {
				return alterShape{}
			}
			retype(o.NewColDefinition.Name.String())
		case *sqlparser.AlterColumn:
			// SET/DROP DEFAULT, SET VISIBLE/INVISIBLE: no type change, but
			// not neutral — the column's definition moved.
			if o.Column == nil {
				return alterShape{}
			}
			out.neutral = false
		case *sqlparser.AlterCharset:
			out.neutral = false
			out.retypedAll = true
		default:
			// Columns added, dropped or renamed; the table renamed.
			return alterShape{}
		}
	}
	if out.neutral {
		out.inPlace = false
	}
	return out
}

// hasNeutralLeader reports whether an unparsed statement starts with one of
// [neutralStatementLeaders], skipping a `DEFINER=...` clause the server
// writes into routine, trigger, event and view DDL.
func hasNeutralLeader(stmt string) bool {
	u := strings.ToUpper(strings.Join(strings.Fields(stmt), " ")) + " "
	if strings.HasPrefix(u, "CREATE DEFINER=") {
		rest := strings.TrimPrefix(u, "CREATE DEFINER=")
		if i := strings.IndexByte(rest, ' '); i >= 0 {
			u = "CREATE " + rest[i+1:]
		}
	}
	for _, l := range neutralStatementLeaders {
		if strings.HasPrefix(u, l) {
			return true
		}
	}
	return false
}
