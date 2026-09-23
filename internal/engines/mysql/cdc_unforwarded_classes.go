// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// The unforwarded-class door, binlog lane (GC-2's MySQL-family sibling;
// the Postgres member is postgres/cdc_unforwarded_classes.go).
//
// # The gap
//
// A DDL reaches this reader as a QueryEvent, and the reader's response is
// a blanket schemaCache clear followed by a lazy information_schema
// rebuild of each table on its next row event. What that rebuild
// PUBLISHES is narrow: [projectTableIR] carries columns and the primary
// key, [Engine.NormalizeForCDCComparison] strips CHECKs and indexes from
// the seed it is compared against, and pipeline.ClassifyShape compares
// neither foreign keys nor the primary key, and compares a changed
// column on type and nullability only — never its DEFAULT. So a source
// ADD/DROP FOREIGN KEY, ADD/DROP UNIQUE, ADD/DROP/ALTER CHECK (including
// ENFORCED ⇄ NOT ENFORCED), a PRIMARY KEY change, ALTER COLUMN SET/DROP
// DEFAULT, or an AUTO_INCREMENT / ON UPDATE / generation-expression
// change on an existing column was ignored at exit 0. The sharpest shape
// is one statement: `ALTER TABLE t ADD COLUMN x INT, ADD CONSTRAINT fk
// FOREIGN KEY (x) REFERENCES p(id)` forwarded the column and dropped the
// foreign key — a target weaker than the source behind a successful
// forward.
//
// # The door
//
// The rebuild is where a change becomes visible, exactly as pgoutput's
// re-sent RelationMessage is on the Postgres lane. At StreamChanges,
// before the binlog connection opens, the reader fingerprints every table
// in the in-scope database(s) from information_schema ([readTableFacts]
// with no table named) — the BASELINE. Whenever [CDCReader.tableFor] has
// to rebuild a table (its first row after a start, and its first row
// after every in-scope DDL), dispatchRows fingerprints that one table
// again and diffs ([diffTableFacts]). A delta ends the stream with the
// grep-stable marker UNFORWARDED-SCHEMA-CHANGE as a [terminalMySQLError]:
// an automatic retry would re-baseline and accept the change silently.
//
// What is compared: unique keys (PRIMARY and every UNIQUE index, from
// STATISTICS so a prefix length or a functional key part is part of the
// identity); foreign keys (columns, referenced table and columns, ON
// UPDATE / ON DELETE, MATCH); CHECKs (clause, and ENFORCED on MySQL —
// MariaDB has no NOT ENFORCED); and per existing column its DEFAULT, its
// EXTRA attributes (auto_increment, ON UPDATE CURRENT_TIMESTAMP, the
// generated kind) and its generation expression. Nullability is NOT
// compared: ADR-0091 already carries it (AlterColumnNullability), and
// comparing it here would refuse the change the forward handles.
//
// # Identity, and the rename rule
//
// MySQL has no attnum, so columns are keyed by NAME and a rename has to
// be inferred. The door infers it exactly the way pipeline.ClassifyShape
// does, so the two cannot disagree about which boundary is a rename:
// exactly one column absent from the current catalog and exactly one
// column new to it, with every other attribute identical (type,
// nullability, default, extra, generation expression) — then the old
// name maps to the new one. Every constraint's prior column list is
// mapped through that rename before it is compared, so a UNIQUE (a) that
// the server rewrote to UNIQUE (b) on `RENAME COLUMN a TO b` compares
// equal, and a renamed column's DEFAULT is compared against its old
// name's. Any other drop+add shape pairs nothing and each side is judged
// as a plain drop and a plain add — which ClassifyShape refuses as a
// combo anyway.
//
// # What the diff exempts, and why each is safe
//
//   - A column added this boundary: its DEFAULT and attributes ride the
//     ADD COLUMN forward, so new columns are not compared. A CONSTRAINT
//     added alongside it is NOT exempt — that is the sharp shape above.
//     (MariaDB's auto `json_valid(<col>)` CHECK on a JSON column is not a
//     constraint at all but the type's own marker, and is dropped at read
//     time exactly as [recoverMariaDBJSONColumns] drops it from the seed.)
//   - A key or foreign key that DISAPPEARED because one of its columns was
//     dropped: the target's own DROP COLUMN removes it too. A key that
//     SHRANK instead (measured on MySQL 8.0: DROP COLUMN b turns UNIQUE
//     (a, b) into UNIQUE (a); Postgres drops the whole index) is REFUSED: whether
//     the target matches depends on the target engine, which the reader
//     cannot see, and a silent divergence is the outcome this door exists
//     to prevent. On a MySQL target that refusal is a false one, and the
//     remedy (restart once the target is confirmed) costs one restart.
//   - A CHECK that disappeared on a boundary that dropped a column, and a
//     CHECK whose clause changed on a boundary that renamed one. COARSE,
//     stated so it cannot be read as precise: information_schema records
//     no column dependency for a CHECK, so the door cannot tell which
//     column it read. `DROP COLUMN a, DROP CHECK on_b` in one statement is
//     therefore exempt — a named residual. (Measured on MySQL 8.0: a
//     DROP COLUMN takes a single-column CHECK with it, and the server
//     refuses — error 3959 — to drop a column a multi-column CHECK reads
//     or to rename any column a CHECK reads, which narrows the residual
//     on MySQL; MariaDB is less strict.)
//   - A DEFAULT or generation expression re-rendered by a type change
//     (measured: MySQL 8.0 reports `0` as `0.00` after `MODIFY x
//     DECIMAL(5,2) DEFAULT 0` on an INT DEFAULT 0 column). Only the
//     VALUE is exempt: a type change that ADDS or REMOVES a default — the
//     classic `MODIFY x BIGINT` that silently discards `DEFAULT 5` — is
//     refused, and so is an EXTRA change riding a type change (`MODIFY id
//     BIGINT` without AUTO_INCREMENT removes it).
//   - A foreign key whose REFERENCED table or column changed because the
//     parent was renamed: exempt only when the old referenced table / each
//     old referenced column no longer exists on the server, i.e. the
//     server rewrote the reference because its target moved. A re-pointed
//     FK whose old target still exists is refused.
//
// # Scope, stated so it cannot be read as broader
//
//   - The baseline is taken at StreamChanges, so a change made while the
//     stream was stopped, during a cold start's bulk copy, or before an
//     ADR-0038 in-process RETRY re-opens the stream (a retry is a fresh
//     StreamChanges, and so is a restart) is IN the baseline and never
//     refused. That includes a change whose grading was interrupted by a
//     transient failure of the grading read itself. The refusal says so.
//     Closing this needs a source↔target comparison, not a source↔source
//     one (the periodic full-diff follow-up to GC-2).
//   - Detection needs a rebuild, which needs a row event on the table
//     after an in-scope DDL QueryEvent: a change on a table never written
//     again is not seen until it is. A DDL whose QueryEvent carries an
//     OUT-of-scope default database (`USE other; ALTER TABLE db.t …`) does
//     not clear the decode cache at all (the generic-DDL arm gates the
//     clear on the statement's schema), so it is not seen either.
//   - The catalog read is current, not position-anchored: a change made
//     after the event being decoded can be reported early (still a real
//     change), and a change reverted before the read is missed (net no
//     change).
//   - Plain (non-unique) indexes are deliberately NOT compared: index DDL
//     is the documented not-forwarded tradeoff (roadmap item 24,
//     ADR-0103), it never alters what a row may contain, and refusing on
//     every source-side CREATE INDEX would end streams on routine tuning.
//   - Collation / charset changes that leave COLUMN_TYPE unchanged are not
//     compared (they are not among GC-2's classes).
//
// # Siblings
//
// The VStream flavors (planetscale, vitess) are EXEMPT from this file:
// their reader ([vstreamCDCReader]) never runs dispatchRows or tableFor —
// it decodes against vtgate's FIELD events, which carry column names and
// types only. The VStream wire DOES deliver a VEventType_DDL with the
// statement text (dispatchDDL uses it for TRUNCATE and field-cache
// invalidation), so a catalog re-read at the next FIELD event is a
// buildable door there; it is a separate change (a vtgate
// information_schema read per keyspace, and PlanetScale's online-DDL
// shadow-table cutovers to classify), not an extension of this one.

// unforwardedChangeMarker is the grep-stable token every refusal from this
// door carries. The Postgres lane's door uses the same token.
const unforwardedChangeMarker = "UNFORWARDED-SCHEMA-CHANGE"

// Constraint kinds as the fact map keys them.
const (
	kindPrimaryKey = "PRIMARY KEY"
	kindUnique     = "UNIQUE"
	kindForeignKey = "FOREIGN KEY"
	kindCheck      = "CHECK"
)

// mysqlTableFacts is one table's fingerprint of the classes the binlog
// lane's boundary projection does not carry.
type mysqlTableFacts struct {
	schema, name string
	columns      map[string]mysqlColumnFact
	// constraints is keyed by kind + " " + name: a UNIQUE index and a
	// foreign key may legally share a name.
	constraints map[string]mysqlConstraintFact
}

// mysqlColumnFact is one information_schema.columns row. nullable is
// read for rename matching only; it is never diffed.
type mysqlColumnFact struct {
	typ      string
	nullable bool
	// def is the raw COLUMN_DEFAULT (plus the DEFAULT_GENERATED token, so
	// a literal ⇄ expression switch with identical text still differs);
	// hasDef is whether a default is declared, per the flavor's
	// reporting convention (MariaDB reports a nullable defaultless column
	// as the bare keyword NULL).
	def    string
	hasDef bool
	// extra is EXTRA minus DEFAULT_GENERATED (carried by def) and
	// INVISIBLE (visibility is not something the target mirrors).
	extra     string
	generated string
}

// mysqlConstraintFact is one unique key, foreign key or CHECK.
type mysqlConstraintFact struct {
	kind, name string
	// columns is the key's / foreign key's column list in order; empty
	// for a CHECK. A functional key part is "".
	columns []string
	// detail is the rest of the identity: per-part prefix length and
	// expression for a key, the rules for a foreign key, the clause and
	// ENFORCED for a CHECK.
	detail string
	// refTable (qualified) and refColumns are the foreign key's target.
	refTable   string
	refColumns []string
}

func (c mysqlConstraintFact) display() string {
	switch c.kind {
	case kindForeignKey:
		return fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s (%s) %s",
			strings.Join(c.columns, ", "), c.refTable, strings.Join(c.refColumns, ", "), c.detail)
	case kindCheck:
		return "CHECK " + c.detail
	default:
		return fmt.Sprintf("%s (%s)%s", c.kind, strings.Join(c.columns, ", "), c.detail)
	}
}

// unforwardedScopeFilter restricts a catalog query to the user schemas,
// and to one table when the query's (table, schema, table) arguments name
// one. prefix is the table alias ("" or "k." etc).
func unforwardedScopeFilter(prefix string) string {
	return prefix + `table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
		  AND (? = '' OR (` + prefix + `table_schema = ? AND ` + prefix + `table_name = ?))`
}

// readTableFacts fingerprints one table (schema, table), or with table ""
// every table in every user schema, keyed by [qualifiedName]. Four
// catalog queries regardless of how many tables are read. The caller
// filters the all-tables form by database scope, which is where the
// server's identifier-folding rule lives.
func readTableFacts(ctx context.Context, db *sql.DB, flavor Flavor, schema, table string) (map[string]*mysqlTableFacts, error) {
	out := map[string]*mysqlTableFacts{}
	args := []any{table, schema, table}
	get := func(s, t string) *mysqlTableFacts {
		qn := qualifiedName(s, t)
		f := out[qn]
		if f == nil {
			f = &mysqlTableFacts{schema: s, name: t, columns: map[string]mysqlColumnFact{}, constraints: map[string]mysqlConstraintFact{}}
			out[qn] = f
		}
		return f
	}
	if err := readColumnFacts(ctx, db, flavor, args, get); err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}
	if err := readKeyFacts(ctx, db, flavor, args, out); err != nil {
		return nil, fmt.Errorf("read unique keys: %w", err)
	}
	if err := readForeignKeyFacts(ctx, db, args, out); err != nil {
		return nil, fmt.Errorf("read foreign keys: %w", err)
	}
	if err := readCheckFacts(ctx, db, flavor, args, out); err != nil {
		return nil, fmt.Errorf("read check constraints: %w", err)
	}
	return out, nil
}

func readColumnFacts(ctx context.Context, db *sql.DB, flavor Flavor, args []any, get func(s, t string) *mysqlTableFacts) error {
	rows, err := db.QueryContext(ctx, `
		SELECT table_schema, table_name, column_name, column_type, is_nullable,
		       column_default, IFNULL(extra, ''), IFNULL(generation_expression, '')
		FROM   information_schema.columns
		WHERE  `+unforwardedScopeFilter(""), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			s, t, col, typ, nullable, extra, gen string
			def                                  sql.NullString
		)
		if err := rows.Scan(&s, &t, &col, &typ, &nullable, &def, &extra, &gen); err != nil {
			return err
		}
		get(s, t).columns[col] = columnFact(flavor, typ, nullable, def, extra, gen)
	}
	return rows.Err()
}

// columnFact folds one columns row into the compared form.
func columnFact(flavor Flavor, typ, nullable string, def sql.NullString, extra, gen string) mysqlColumnFact {
	generatedDefault := false
	var kept []string
	for _, tok := range strings.Fields(extra) {
		switch strings.ToUpper(tok) {
		case "DEFAULT_GENERATED":
			generatedDefault = true
		case "INVISIBLE":
		default:
			kept = append(kept, tok)
		}
	}
	f := mysqlColumnFact{
		typ:       typ,
		nullable:  strings.EqualFold(nullable, "YES"),
		hasDef:    def.Valid && (flavor != FlavorMariaDB || def.String != "NULL"),
		extra:     strings.Join(kept, " "),
		generated: gen,
	}
	if f.hasDef {
		f.def = def.String
		if generatedDefault {
			f.def = "(" + def.String + ")"
		}
	}
	return f
}

func readKeyFacts(ctx context.Context, db *sql.DB, flavor Flavor, args []any, out map[string]*mysqlTableFacts) error {
	expr := `IFNULL(expression, '')`
	if flavor == FlavorMariaDB {
		expr = `''` // no functional key parts (see indexesQuery)
	}
	rows, err := db.QueryContext(ctx, `
		SELECT table_schema, table_name, index_name, IFNULL(column_name, ''), `+expr+`, IFNULL(sub_part, 0)
		FROM   information_schema.statistics
		WHERE  non_unique = 0
		  AND  `+unforwardedScopeFilter("")+`
		ORDER  BY table_schema, table_name, index_name, seq_in_index`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			s, t, index, col, expression string
			subPart                      int64
		)
		if err := rows.Scan(&s, &t, &index, &col, &expression, &subPart); err != nil {
			return err
		}
		f := out[qualifiedName(s, t)]
		if f == nil {
			continue
		}
		kind := kindUnique
		if index == "PRIMARY" {
			kind = kindPrimaryKey
		}
		key := kind + " " + index
		c := f.constraints[key]
		c.kind, c.name = kind, index
		c.columns = append(c.columns, col)
		switch {
		case expression != "":
			c.detail += fmt.Sprintf(" part%d=(%s)", len(c.columns), expression)
		case subPart > 0:
			c.detail += fmt.Sprintf(" part%d-prefix=%d", len(c.columns), subPart)
		}
		f.constraints[key] = c
	}
	return rows.Err()
}

func readForeignKeyFacts(ctx context.Context, db *sql.DB, args []any, out map[string]*mysqlTableFacts) error {
	rows, err := db.QueryContext(ctx, `
		SELECT k.table_schema, k.table_name, k.constraint_name, k.column_name,
		       k.referenced_table_schema, k.referenced_table_name, k.referenced_column_name,
		       rc.update_rule, rc.delete_rule, IFNULL(rc.match_option, '')
		FROM   information_schema.key_column_usage k
		JOIN   information_schema.referential_constraints rc
		  ON   rc.constraint_schema = k.constraint_schema
		 AND   rc.constraint_name   = k.constraint_name
		 AND   rc.table_name        = k.table_name
		WHERE  k.referenced_table_name IS NOT NULL
		  AND  `+unforwardedScopeFilter("k.")+`
		ORDER  BY k.table_schema, k.table_name, k.constraint_name, k.ordinal_position`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var s, t, name, col, refSchema, refTable, refCol, upd, del, match string
		if err := rows.Scan(&s, &t, &name, &col, &refSchema, &refTable, &refCol, &upd, &del, &match); err != nil {
			return err
		}
		f := out[qualifiedName(s, t)]
		if f == nil {
			continue
		}
		key := kindForeignKey + " " + name
		c := f.constraints[key]
		c.kind, c.name = kindForeignKey, name
		c.columns = append(c.columns, col)
		c.refTable = qualifiedName(refSchema, refTable)
		c.refColumns = append(c.refColumns, refCol)
		c.detail = fmt.Sprintf("ON UPDATE %s ON DELETE %s MATCH %s", upd, del, match)
		f.constraints[key] = c
	}
	return rows.Err()
}

// readCheckFacts reads CHECK constraints with the flavor's join (the
// Bug 198 shape [checkConstraintsQuery] documents: MariaDB names are
// unique per TABLE, MySQL 8's per schema and its check_constraints has no
// table_name) and, on MySQL, ENFORCED. MariaDB's auto json_valid CHECK on
// a JSON column is dropped: it is the type, not a constraint.
func readCheckFacts(ctx context.Context, db *sql.DB, flavor Flavor, args []any, out map[string]*mysqlTableFacts) error {
	enforced, tableJoin := `IFNULL(tc.enforced, 'YES')`, ""
	if flavor == FlavorMariaDB {
		enforced, tableJoin = `'YES'`, "\n\t\t AND   cc.table_name        = tc.table_name"
	}
	rows, err := db.QueryContext(ctx, `
		SELECT tc.table_schema, tc.table_name, cc.constraint_name, cc.check_clause, `+enforced+`
		FROM   information_schema.check_constraints cc
		JOIN   information_schema.table_constraints tc
		  ON   tc.constraint_schema = cc.constraint_schema
		 AND   tc.constraint_name   = cc.constraint_name`+tableJoin+`
		WHERE  tc.constraint_type = 'CHECK'
		  AND  `+unforwardedScopeFilter("tc."), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var s, t, name, clause, isEnforced string
		if err := rows.Scan(&s, &t, &name, &clause, &isEnforced); err != nil {
			return err
		}
		f := out[qualifiedName(s, t)]
		if f == nil {
			continue
		}
		if flavor == FlavorMariaDB && isMariaDBJSONMarker(f, clause) {
			continue
		}
		detail := "(" + clause + ")"
		if strings.EqualFold(isEnforced, "NO") {
			detail += " NOT ENFORCED"
		}
		f.constraints[kindCheck+" "+name] = mysqlConstraintFact{kind: kindCheck, name: name, detail: detail}
	}
	return rows.Err()
}

// isMariaDBJSONMarker reports whether clause is MariaDB's auto
// json_valid(<col>) CHECK on a longtext column of f — the same match
// [jsonValidCheckTarget] applies to the seed.
func isMariaDBJSONMarker(f *mysqlTableFacts, clause string) bool {
	col, ok := isMariaDBAutoJSONValidCheck(normalizeExprForFlavor(FlavorMariaDB, clause))
	if !ok {
		return false
	}
	c, ok := f.columns[col]
	return ok && strings.EqualFold(c.typ, "longtext")
}

// columnRename infers the boundary's rename the way pipeline.ClassifyShape
// does: exactly one column gone and exactly one new, identical in every
// attribute but the name. ok=false means no rename.
func columnRename(prior, cur *mysqlTableFacts) (from, to string, ok bool) {
	var gone, added []string
	for name := range prior.columns {
		if _, still := cur.columns[name]; !still {
			gone = append(gone, name)
		}
	}
	for name := range cur.columns {
		if _, had := prior.columns[name]; !had {
			added = append(added, name)
		}
	}
	if len(gone) != 1 || len(added) != 1 || prior.columns[gone[0]] != cur.columns[added[0]] {
		return "", "", false
	}
	return gone[0], added[0], true
}

// diffTableFacts returns one human-readable line per change between prior
// and cur that the stream cannot carry, after the exemptions the file
// comment lists. tablesNow holds the CURRENT facts of every table a
// re-pointed foreign key referenced before or after (absent key: the
// table no longer exists); only the parent-rename exemption reads it.
// Empty means nothing to refuse.
func diffTableFacts(prior, cur *mysqlTableFacts, tablesNow map[string]*mysqlTableFacts) []string {
	var deltas []string

	renameFrom, renameTo, renamed := columnRename(prior, cur)
	mapName := func(name string) string {
		if renamed && name == renameFrom {
			return renameTo
		}
		return name
	}
	dropped := map[string]bool{}
	for name := range prior.columns {
		if _, still := cur.columns[mapName(name)]; !still {
			dropped[name] = true
		}
	}

	for _, name := range sortedFactKeys(prior.columns) {
		pc := prior.columns[name]
		curName := mapName(name)
		cc, ok := cur.columns[curName]
		if !ok {
			continue
		}
		retyped := pc.typ != cc.typ
		switch {
		case pc.hasDef && !cc.hasDef:
			deltas = append(deltas, fmt.Sprintf("ALTER COLUMN %q DROP DEFAULT (was %s)", curName, pc.def))
		case !pc.hasDef && cc.hasDef:
			deltas = append(deltas, fmt.Sprintf("ALTER COLUMN %q SET DEFAULT %s", curName, cc.def))
		case pc.def != cc.def && !retyped:
			deltas = append(deltas, fmt.Sprintf("ALTER COLUMN %q SET DEFAULT %s (was %s)", curName, cc.def, pc.def))
		}
		if pc.extra != cc.extra {
			deltas = append(deltas, fmt.Sprintf("ALTER COLUMN %q attributes %q -> %q", curName, pc.extra, cc.extra))
		}
		if pc.generated != cc.generated && !retyped {
			deltas = append(deltas, fmt.Sprintf("ALTER COLUMN %q generation expression (%s) -> (%s)", curName, pc.generated, cc.generated))
		}
	}

	touchesDropped := func(c mysqlConstraintFact) bool {
		return slices.ContainsFunc(c.columns, func(col string) bool { return dropped[col] })
	}
	for _, key := range sortedFactKeys(cur.constraints) {
		cc := cur.constraints[key]
		pc, ok := prior.constraints[key]
		switch {
		case !ok:
			deltas = append(deltas, addConstraintLine(cc))
		case sameConstraint(pc, cc, mapName):
		case pc.kind == kindCheck && renamed:
			// Coarse: a rename may re-render a CHECK's clause, and nothing
			// says which column the clause reads.
		case pc.kind == kindForeignKey && fkParentMoved(pc, cc, mapName, tablesNow):
		default:
			deltas = append(deltas, changedConstraintLine(pc, cc))
		}
	}
	for _, key := range sortedFactKeys(prior.constraints) {
		if _, ok := cur.constraints[key]; ok {
			continue
		}
		pc := prior.constraints[key]
		if touchesDropped(pc) || (pc.kind == kindCheck && len(dropped) > 0) {
			continue
		}
		deltas = append(deltas, dropConstraintLine(pc))
	}
	return deltas
}

// sameConstraint compares with prior's columns mapped through the
// boundary's rename.
func sameConstraint(pc, cc mysqlConstraintFact, mapName func(string) string) bool {
	if pc.detail != cc.detail || pc.refTable != cc.refTable || !slices.Equal(pc.refColumns, cc.refColumns) || len(pc.columns) != len(cc.columns) {
		return false
	}
	for i, col := range pc.columns {
		if mapName(col) != cc.columns[i] {
			return false
		}
	}
	return true
}

// fkParentMoved is the parent-rename exemption: everything but the
// reference is unchanged, and each part of the reference that changed
// points at something that no longer exists — the server rewrote the
// reference because its target was renamed.
func fkParentMoved(pc, cc mysqlConstraintFact, mapName func(string) string, tablesNow map[string]*mysqlTableFacts) bool {
	local := pc
	local.refTable, local.refColumns = cc.refTable, cc.refColumns
	if !sameConstraint(local, cc, mapName) || len(pc.refColumns) != len(cc.refColumns) {
		return false
	}
	if pc.refTable != cc.refTable {
		if _, stillThere := tablesNow[pc.refTable]; stillThere {
			return false
		}
	}
	parent := tablesNow[cc.refTable]
	if parent == nil {
		return false
	}
	for i, old := range pc.refColumns {
		if old == cc.refColumns[i] {
			continue
		}
		if _, stillThere := parent.columns[old]; stillThere {
			return false
		}
	}
	return true
}

// fkParentsToRead names the tables [fkParentMoved] needs current facts
// for: both references of every same-named foreign key whose reference
// changed. Usually empty, so the common rebuild reads nothing extra.
func fkParentsToRead(prior, cur *mysqlTableFacts) []string {
	var out []string
	for key, cc := range cur.constraints {
		pc, ok := prior.constraints[key]
		if !ok || cc.kind != kindForeignKey || (pc.refTable == cc.refTable && slices.Equal(pc.refColumns, cc.refColumns)) {
			continue
		}
		out = append(out, pc.refTable, cc.refTable)
	}
	return out
}

func addConstraintLine(c mysqlConstraintFact) string {
	if c.kind == kindPrimaryKey {
		return "ADD " + c.display()
	}
	return fmt.Sprintf("ADD CONSTRAINT %q %s", c.name, c.display())
}

func dropConstraintLine(c mysqlConstraintFact) string {
	if c.kind == kindPrimaryKey {
		return fmt.Sprintf("DROP PRIMARY KEY (was %s)", c.display())
	}
	return fmt.Sprintf("DROP CONSTRAINT %q (was %s)", c.name, c.display())
}

func changedConstraintLine(pc, cc mysqlConstraintFact) string {
	if cc.kind == kindPrimaryKey {
		return fmt.Sprintf("PRIMARY KEY changed: %s -> %s", pc.display(), cc.display())
	}
	return fmt.Sprintf("CONSTRAINT %q changed: %s -> %s", cc.name, pc.display(), cc.display())
}

func sortedFactKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// captureUnforwardedBaseline fingerprints every table in the stream's
// database scope at StreamChanges. A nil catalog pool leaves the door
// inert (unit-test readers built as struct literals).
func (r *CDCReader) captureUnforwardedBaseline(ctx context.Context) error {
	if r.db == nil {
		return nil
	}
	facts, err := readTableFacts(ctx, r.db, r.flavor, "", "")
	if err != nil {
		return fmt.Errorf("mysql: cdc: baseline the schema objects the binlog lane does not forward: %w", err)
	}
	baseline := make(map[string]*mysqlTableFacts, len(facts))
	for qn, f := range facts {
		if r.databaseInScope(f.schema) {
			baseline[qn] = f
		}
	}
	r.unforwardedBaseline = baseline
	return nil
}

// gradeUnforwardedClasses is the per-rebuild half of the door, called by
// dispatchRows AFTER its table-scope gate (a change on a table this
// stream emits nothing for must not end the stream — Bug 246's rule,
// held for every dispatchRows exit by
// TestStreamKillingRefusalsInDispatchAreScopeGated). A table with no
// baseline (created after StreamChanges) is baselined here; a table that
// passes is re-baselined so an exempt change (a forwarded DROP COLUMN's
// cascade) is not re-diffed at the next rebuild.
func (r *CDCReader) gradeUnforwardedClasses(ctx context.Context, qn string) error {
	if r.unforwardedBaseline == nil || r.db == nil {
		return nil
	}
	schema, table := splitQualified(qn)
	facts, err := readTableFacts(ctx, r.db, r.flavor, schema, table)
	if err != nil {
		return fmt.Errorf("mysql: cdc: table %s: read the schema objects the binlog lane does not forward: %w", qn, err)
	}
	cur := facts[qn]
	if cur == nil {
		return nil // dropped since the row was written; the DML path owns that
	}
	prior := r.unforwardedBaseline[qn]
	if prior == nil {
		r.unforwardedBaseline[qn] = cur
		return nil
	}
	tablesNow := map[string]*mysqlTableFacts{}
	for _, ref := range fkParentsToRead(prior, cur) {
		if _, done := tablesNow[ref]; done {
			continue
		}
		refSchema, refTable := splitQualified(ref)
		parent, err := readTableFacts(ctx, r.db, r.flavor, refSchema, refTable)
		if err != nil {
			return fmt.Errorf("mysql: cdc: table %s: read referenced table %s: %w", qn, ref, err)
		}
		if f := parent[ref]; f != nil {
			tablesNow[ref] = f
		}
	}
	if deltas := diffTableFacts(prior, cur, tablesNow); len(deltas) > 0 {
		return &terminalMySQLError{err: unforwardedChangeError(schema, table, deltas)}
	}
	r.unforwardedBaseline[qn] = cur
	return nil
}

func unforwardedChangeError(schema, table string, deltas []string) error {
	return fmt.Errorf("mysql: cdc: %s on %s.%s: the source changed schema objects that the binlog stream does not carry "+
		"and sluice cannot forward: %s. The target (or the backup chain) does not have this change, so continuing would leave it "+
		"silently weaker than the source. Remedy: "+
		"(1) apply the same change to the target yourself (for `backup stream`, take a new full backup instead); "+
		"(2) restart with the SAME --stream-id. The restart takes a fresh baseline of these objects, so restarting WITHOUT step 1 "+
		"accepts the difference permanently and nothing will report it again",
		unforwardedChangeMarker, schema, table, strings.Join(deltas, "; "))
}
