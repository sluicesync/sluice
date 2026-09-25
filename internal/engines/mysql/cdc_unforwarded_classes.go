// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
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
// a retry cannot succeed (the next reader is handed this baseline and
// refuses again; GC-32), and without the carry it would re-baseline and
// accept the change silently.
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
//     SPELLING is exempt, judged by [sameDefaultValue] (numerically or
//     textually equal): a changed VALUE riding a retype reaches a MySQL
//     target through MODIFY COLUMN but not a Postgres one, whose forwarded
//     ALTER TYPE carries no DEFAULT, so it refuses (2026-09-23 pre-tag
//     review F6). A type change that ADDS or REMOVES a default — the
//     classic `MODIFY x BIGINT` that silently discards `DEFAULT 5` — is
//     refused, and so is an EXTRA change riding a type change (`MODIFY id
//     BIGINT` without AUTO_INCREMENT removes it).
//   - BINARY/VARBINARY literal defaults are compared on their TRUE bytes,
//     re-read from SHOW CREATE TABLE ([binaryHexDefault]): information_schema
//     cuts them at the first NUL, so a change after it (0x610000 →
//     0x6100FF) read identically there (review F5, GC-29's sibling). On
//     MariaDB they are re-read by a DEFAULT() probe instead: its catalog
//     stores every non-UTF-8 byte as '?', so 0x00AB00 → 0x00CD00 read
//     identically there (GC-36, [recoverMariaDBBinaryDefaults]).
//     Both rules are grounded on a real server by
//     TestTableFacts_DiffPremisesOnARealServer.
//   - Character-column literal defaults are compared on their TRUE text
//     when the catalog shows a '?': information_schema stores a character
//     utf8mb3 cannot hold as '?' on both flavors, so '😀x' → '😁x' read
//     '?x' both times (GC-37 (h), [recoverTextDefaults]). Grounded on
//     MySQL and every MariaDB LTS line by
//     TestTextDefault_SupplementaryChars_OnARealServer.
//   - A foreign key whose REFERENCED table or column changed because the
//     parent was renamed: exempt only when the old referenced table / each
//     old referenced column no longer exists on the server, i.e. the
//     server rewrote the reference because its target moved. A re-pointed
//     FK whose old target still exists is refused.
//
// # Scope, stated so it cannot be read as broader
//
//   - The baseline is taken at StreamChanges, so a change made while the
//     stream was stopped, or during a cold start's bulk copy, is IN the
//     baseline and never refused; so is anything an operator restart
//     re-baselines, which the refusal says. An ADR-0038 in-process RETRY
//     does NOT re-baseline: the pipeline hands the next reader this one's
//     baseline (GC-32), which also covers a transient failure of the
//     grading read itself. (A killed binlog dump thread never reaches
//     that retry: go-mysql's syncer reconnects inside the same reader.)
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
func readTableFacts(ctx context.Context, db *sql.DB, flavor Flavor, schema, table string, inScope func(schema string) bool) (map[string]*mysqlTableFacts, error) {
	out := map[string]*mysqlTableFacts{}
	// drop forgets a table dropped between the catalog read and a SHOW CREATE
	// TABLE the expression recovery needed for it (see [recoverExprTexts]).
	drop := func(s string) func(t string) {
		return func(t string) { delete(out, qualifiedName(s, t)) }
	}
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
	if err := readColumnFacts(ctx, db, flavor, args, inScope, get, drop); err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}
	if err := readKeyFacts(ctx, db, flavor, args, out); err != nil {
		return nil, fmt.Errorf("read unique keys: %w", err)
	}
	if err := readForeignKeyFacts(ctx, db, args, out); err != nil {
		return nil, fmt.Errorf("read foreign keys: %w", err)
	}
	if err := readCheckFacts(ctx, db, flavor, args, out, drop); err != nil {
		return nil, fmt.Errorf("read check constraints: %w", err)
	}
	return out, nil
}

func readColumnFacts(ctx context.Context, db *sql.DB, flavor Flavor, args []any, inScope func(string) bool, get func(s, t string) *mysqlTableFacts, drop func(s string) func(t string)) error {
	rows, err := db.QueryContext(ctx, `
		SELECT c.table_schema, c.table_name, c.column_name, c.column_type, c.is_nullable,
		       c.column_default, IFNULL(c.extra, ''), IFNULL(c.generation_expression, ''),
		       IFNULL(c.character_set_name, '')
		FROM   information_schema.columns c
		JOIN   information_schema.tables tb
		  ON   tb.table_schema = c.table_schema
		 AND   tb.table_name   = c.table_name
		 AND   tb.table_type   = 'BASE TABLE'
		WHERE  `+unforwardedScopeFilter("c."), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	// BINARY/VARBINARY literal defaults: information_schema cuts them at the
	// first NUL byte, so a change after it is invisible there (0x610000 →
	// 0x6100FF both read "0x61", measured on MySQL 8.4). Collected here and
	// re-read from SHOW CREATE TABLE below — the same recovery the schema
	// readers run (GC-29, binary_default_recovery.go).
	type binaryDefault struct{ schema, table, column string }
	var pending []binaryDefault
	// MariaDB reports the same defaults quoted, with every non-UTF-8 byte
	// stored as '?' (GC-36), so 0x00AB00 → 0x00CD00 read '\0?\0' both
	// times; those are re-read by the schema readers' DEFAULT() probe.
	mariadbPending := map[string][]pendingMariaDBBinaryDefault{}
	// A character default holding a supplementary character reads back as
	// '?' on both flavors (GC-37 (h)), so '😀' → '😁' read '?' both times;
	// re-read by the schema readers' utf8mb4 DEFAULT() probe.
	textPending := map[string][]pendingTextDefault{}
	// An expression default or generation expression holding a character
	// beyond the BMP reads back as '?' on MariaDB (Bug 288), so '😀' → '😁'
	// inside one read identically both times; recovered from SHOW CREATE.
	exprPending := map[string][]pendingExprText{}
	for rows.Next() {
		var (
			s, t, col, typ, nullable, extra, gen, charset string
			def                                           sql.NullString
		)
		if err := rows.Scan(&s, &t, &col, &typ, &nullable, &def, &extra, &gen, &charset); err != nil {
			return err
		}
		// Out-of-scope schemas are dropped here, before any recovery reads
		// them: the all-tables baseline spans the whole server, and a table
		// the stream never emits must not be able to fail its start.
		if inScope != nil && !inScope(s) {
			continue
		}
		get(s, t).columns[col] = columnFact(flavor, typ, nullable, def, extra, gen)
		if flavor == FlavorMariaDB {
			facts := get(s, t)
			if text, ok := exprDefaultCatalogText(flavor, extra, def); ok && exprTextNeedsRecovery(flavor, text) {
				exprPending[s] = append(exprPending[s], pendingExprText{
					table: t, kind: exprSiteDefault, name: col, catalog: text,
					set: func(recovered string) {
						c := facts.columns[col]
						c.def = recovered
						facts.columns[col] = c
					},
				})
			}
			if gen != "" && exprTextNeedsRecovery(flavor, gen) {
				exprPending[s] = append(exprPending[s], pendingExprText{
					table: t, kind: exprSiteGenerated, name: col, catalog: gen,
					set: func(recovered string) {
						c := facts.columns[col]
						c.generated = recovered
						facts.columns[col] = c
					},
				})
			}
		}
		if binaryHexDefault(typ, extra, def) {
			pending = append(pending, binaryDefault{s, t, col})
		}
		if flavor == FlavorMariaDB && mariadbBinaryDefaultNeedsProbe(isBinaryColumnType(typ), def) {
			facts := get(s, t)
			mariadbPending[s] = append(mariadbPending[s], pendingMariaDBBinaryDefault{
				table: t, column: col, catalog: def.String,
				set: func(hexLiteral string) {
					c := facts.columns[col]
					c.def = hexLiteral
					facts.columns[col] = c
				},
			})
		}
		if text, ok := textDefaultCatalogText(flavor, charset, extra, def); ok {
			facts := get(s, t)
			textPending[s] = append(textPending[s], pendingTextDefault{
				table: t, column: col, catalog: text, charset: charset,
				set: func(recovered string) {
					c := facts.columns[col]
					c.def = textDefaultFact(flavor, recovered)
					facts.columns[col] = c
				},
			})
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, s := range sortedKeys(mariadbPending) {
		if err := recoverMariaDBBinaryDefaults(ctx, db, s, mariadbPending[s]); err != nil {
			return fmt.Errorf("recover binary defaults: %w", err)
		}
	}
	for _, s := range sortedKeys(textPending) {
		if err := recoverTextDefaults(ctx, db, s, flavor, textPending[s]); err != nil {
			return fmt.Errorf("recover text defaults: %w", err)
		}
	}
	for _, s := range sortedKeys(exprPending) {
		if err := recoverExprTexts(ctx, db, s, flavor, exprPending[s], drop(s)); err != nil {
			return fmt.Errorf("recover expression text: %w", err)
		}
	}
	showCreate := map[string]string{}
	for _, p := range pending {
		key := p.schema + "." + p.table
		stmt, ok := showCreate[key]
		if !ok {
			var name string
			q := "SHOW CREATE TABLE `" + mysqlQuoteIdent(p.schema) + "`.`" + mysqlQuoteIdent(p.table) + "`"
			if err := db.QueryRowContext(ctx, q).Scan(&name, &stmt); err != nil {
				return fmt.Errorf("recover binary defaults of %s: %w", key, err)
			}
			showCreate[key] = stmt
		}
		c := get(p.schema, p.table).columns[p.column]
		if raw, ok := parseShowCreateColumnDefault(stmt, p.column); ok {
			c.def = bytesToHexLiteral(raw)
		} else {
			// Unparseable: keep the truncated value but mark it, so it can
			// never compare equal to a recovered one by accident.
			c.def = "unrecovered:" + c.def
		}
		get(p.schema, p.table).columns[p.column] = c
	}
	return nil
}

// binaryHexDefault reports a BINARY/VARBINARY column whose literal default
// information_schema rendered as a (possibly NUL-truncated) hex literal —
// the columns whose true bytes only SHOW CREATE TABLE carries. MariaDB
// reports these quoted and escape-encoded, never as `0x…`, so it never
// matches there (its loss is different; see mariadbBinaryDefaultNeedsProbe).
func binaryHexDefault(columnType, extra string, def sql.NullString) bool {
	if !def.Valid || strings.Contains(strings.ToUpper(extra), "DEFAULT_GENERATED") {
		return false
	}
	return isBinaryColumnType(columnType) && hasHexLiteralPrefix(def.String)
}

// isBinaryColumnType reports a BINARY(n)/VARBINARY(n) COLUMN_TYPE — the
// catalog-string form of [isBinaryFamilyType].
func isBinaryColumnType(columnType string) bool {
	t := strings.ToLower(columnType)
	return strings.HasPrefix(t, "binary(") || strings.HasPrefix(t, "varbinary(")
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
func readCheckFacts(ctx context.Context, db *sql.DB, flavor Flavor, args []any, out map[string]*mysqlTableFacts, drop func(s string) func(t string)) error {
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
	// MariaDB's '?' for a character beyond the BMP (Bug 288); see
	// readColumnFacts. MySQL's double-encoded CHECK_CLAUSE is a bijection
	// of the stored text, so it compares faithfully as read.
	exprPending := map[string][]pendingExprText{}
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
		suffix := ""
		if strings.EqualFold(isEnforced, "NO") {
			suffix = " NOT ENFORCED"
		}
		key := kindCheck + " " + name
		f.constraints[key] = mysqlConstraintFact{kind: kindCheck, name: name, detail: "(" + clause + ")" + suffix}
		if flavor == FlavorMariaDB && exprTextNeedsRecovery(flavor, clause) {
			exprPending[s] = append(exprPending[s], pendingExprText{
				table: t, kind: exprSiteCheck, name: name, catalog: clause,
				set: func(recovered string) {
					c := f.constraints[key]
					c.detail = "(" + recovered + ")" + suffix
					f.constraints[key] = c
				},
			})
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, s := range sortedKeys(exprPending) {
		if err := recoverExprTexts(ctx, db, s, flavor, exprPending[s], drop(s)); err != nil {
			return fmt.Errorf("recover expression text: %w", err)
		}
	}
	return nil
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
		// A retype re-renders a default's SPELLING (INT 0 → DECIMAL(5,2)
		// `0.00`, measured on MySQL 8.0) and only that is exempt: a changed
		// VALUE riding a retype (`MODIFY x BIGINT DEFAULT 7` from INT DEFAULT 5)
		// reaches a MySQL target through MODIFY COLUMN but not a Postgres one,
		// whose forwarded ALTER TYPE carries no DEFAULT.
		case pc.def != cc.def && (!retyped || !sameDefaultValue(pc.typ, cc.typ, pc.def, cc.def)):
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

// mysqlUnforwardedBaseline is the door's baseline as it crosses from one
// reader to the next through the pipeline (GC-32). Opaque to the
// pipeline: it only moves the value between readers of this engine.
type mysqlUnforwardedBaseline map[string]*mysqlTableFacts

// unforwardedDoorState holds the door's baseline. The pump reads and
// writes facts; [CDCReader.UnforwardedBaseline] reads it from the
// pipeline between retry attempts, so it is guarded. A facts entry is
// never mutated in place — a re-baseline replaces the pointer — so a
// shallow map copy is a consistent snapshot.
type unforwardedDoorState struct {
	mu      sync.Mutex
	facts   map[string]*mysqlTableFacts
	carried mysqlUnforwardedBaseline
}

// UnforwardedBaseline returns a snapshot of this reader's baseline for
// the NEXT reader of the same run (GC-32; the Postgres twin's doc states
// why). nil before StreamChanges or when the door is inert.
func (r *CDCReader) UnforwardedBaseline() any {
	r.unforwarded.mu.Lock()
	defer r.unforwarded.mu.Unlock()
	if r.unforwarded.facts == nil {
		// Never captured: pass on what it was handed (see the Postgres twin).
		if r.unforwarded.carried == nil {
			return nil
		}
		return r.unforwarded.carried
	}
	out := make(mysqlUnforwardedBaseline, len(r.unforwarded.facts))
	for k, v := range r.unforwarded.facts {
		out[k] = v
	}
	return out
}

// SetUnforwardedBaseline hands this reader the previous reader's baseline
// (GC-32); StreamChanges then starts from it instead of the live catalog.
// Must be called before StreamChanges. A value from another engine is
// ignored and the reader takes its own baseline.
func (r *CDCReader) SetUnforwardedBaseline(b any) {
	carried, ok := b.(mysqlUnforwardedBaseline)
	if !ok {
		return
	}
	r.unforwarded.mu.Lock()
	defer r.unforwarded.mu.Unlock()
	r.unforwarded.carried = carried
}

// captureUnforwardedBaseline fingerprints every table in the stream's
// database scope at StreamChanges — or, on a retry within one run, adopts
// the previous reader's baseline (GC-32). A nil catalog pool leaves the
// door inert (unit-test readers built as struct literals).
func (r *CDCReader) captureUnforwardedBaseline(ctx context.Context) error {
	if r.db == nil {
		return nil
	}
	r.unforwarded.mu.Lock()
	carried := r.unforwarded.carried
	r.unforwarded.mu.Unlock()
	baseline := make(map[string]*mysqlTableFacts, len(carried))
	if carried != nil {
		for qn, f := range carried {
			baseline[qn] = f
		}
	} else {
		facts, err := readTableFacts(ctx, r.db, r.flavor, "", "", r.databaseInScope)
		if err != nil {
			return fmt.Errorf("mysql: cdc: baseline the schema objects the binlog lane does not forward: %w", err)
		}
		for qn, f := range facts {
			if r.databaseInScope(f.schema) {
				baseline[qn] = f
			}
		}
	}
	r.unforwarded.mu.Lock()
	r.unforwarded.facts = baseline
	r.unforwarded.mu.Unlock()
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
	r.unforwarded.mu.Lock()
	armed := r.unforwarded.facts != nil
	prior := r.unforwarded.facts[qn]
	r.unforwarded.mu.Unlock()
	if !armed || r.db == nil {
		return nil
	}
	schema, table := splitQualified(qn)
	facts, err := readTableFacts(ctx, r.db, r.flavor, schema, table, nil)
	if err != nil {
		return fmt.Errorf("mysql: cdc: table %s: read the schema objects the binlog lane does not forward: %w", qn, err)
	}
	cur := facts[qn]
	if cur == nil {
		return nil // dropped since the row was written; the DML path owns that
	}
	if prior != nil {
		tablesNow := map[string]*mysqlTableFacts{}
		for _, ref := range fkParentsToRead(prior, cur) {
			if _, done := tablesNow[ref]; done {
				continue
			}
			refSchema, refTable := splitQualified(ref)
			parent, err := readTableFacts(ctx, r.db, r.flavor, refSchema, refTable, nil)
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
	}
	r.unforwarded.mu.Lock()
	r.unforwarded.facts[qn] = cur
	r.unforwarded.mu.Unlock()
	return nil
}

func unforwardedChangeError(schema, table string, deltas []string) error {
	return markUnforwardedRefusal(fmt.Errorf("mysql: cdc: %s on %s.%s: the source changed schema objects that the binlog stream does not carry "+
		"and sluice cannot forward: %s. The target (or the backup chain) does not have this change, so continuing would leave it "+
		"silently weaker than the source. Remedy: "+
		"(1) apply the same change to the target yourself (for `backup stream`, take a new full backup instead); "+
		"(2) restart with the SAME --stream-id: sluice records this refusal, so that start refuses again and prints its "+
		"fingerprint; start once more with --accept-unforwarded-schema-change=<that fingerprint>. The flag takes a fresh baseline "+
		"of these objects, so passing it WITHOUT step 1 "+
		"accepts the difference permanently and nothing will report it again",
		unforwardedChangeMarker, schema, table, strings.Join(deltas, "; ")))
}

// sameDefaultValue reports whether two catalog renderings of a column
// default denote the same value — the only difference a retype is allowed
// to make to a default. Identical text is the same value. Otherwise only a
// NUMERIC column retyped to a NUMERIC column may differ in spelling, and
// only between plain decimal literals (`0` = `0.00`, measured on MySQL 8.0):
// `big.Rat` alone would also equate `007`/`7`, `0x61`/`0x0061`, `1/2`/`0.5`
// and `1_000`/`1000`, and those are different defaults on a VARCHAR or a
// VARBINARY (2026-09-23 second-pass review, finding 3). Anything it cannot
// prove equal is different, so the door refuses rather than exempts.
func sameDefaultValue(priorType, curType, a, b string) bool {
	if a == b {
		return true
	}
	if !numericColumnType(priorType) || !numericColumnType(curType) ||
		!plainDecimal.MatchString(a) || !plainDecimal.MatchString(b) {
		return false
	}
	ra, okA := new(big.Rat).SetString(a)
	rb, okB := new(big.Rat).SetString(b)
	return okA && okB && ra.Cmp(rb) == 0
}

// plainDecimal is an optionally signed decimal with an optional fraction,
// and nothing else — no base prefix, exponent, underscore or fraction bar.
var plainDecimal = regexp.MustCompile(`^-?\d+(\.\d+)?$`) // RE2's \d is ASCII-only

// numericColumnType reports a MySQL numeric COLUMN_TYPE (`int(11)`,
// `decimal(5,2) unsigned`, `double`, …). BIT is deliberately excluded: its
// defaults are bit-literals, not decimals.
func numericColumnType(columnType string) bool {
	base := strings.ToLower(strings.TrimSpace(columnType))
	if i := strings.IndexAny(base, "( "); i >= 0 {
		base = base[:i]
	}
	switch base {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint",
		"decimal", "numeric", "dec", "fixed", "float", "double", "real":
		return true
	}
	return false
}
