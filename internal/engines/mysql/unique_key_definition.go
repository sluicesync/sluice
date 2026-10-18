// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
)

// uniqueKeyDefinitionSep separates the fields of one line of a
// [SchemaReader.UniqueKeyDefinition]: a control character no catalog
// rendering contains, so two different field lists cannot join to the same
// text.
const uniqueKeyDefinitionSep = "\x1f"

// UniqueKeyDefinition implements [ir.UniqueKeyDefinitionReader] for the
// stream's unique-key door (round-7 review MEDIUM-1): the server's own
// description of the key and of every column it depends on.
//
//   - the key's line of SHOW CREATE TABLE, verbatim — its parts, prefix
//     lengths, directions and a functional part's expression with its
//     literals as the server keeps them (information_schema renders a
//     non-ASCII expression lossily, Bug 288, so it is not the source of the
//     text) — and its information_schema.STATISTICS rows (column, SUB_PART,
//     EXPRESSION, COLLATION A/D, NON_UNIQUE, INDEX_TYPE);
//   - for every column it depends on, in ordinal order, the column's
//     information_schema.COLUMNS row (COLUMN_TYPE, CHARACTER_SET_NAME,
//     COLLATION_NAME, IS_NULLABLE, GENERATION_EXPRESSION, EXTRA) and its
//     SHOW CREATE TABLE line. A key with a functional part depends on every
//     column of the table: which columns an expression reads is not
//     recorded, and taking them all is the superset;
//   - on vanilla MySQL, a functional part's hidden generated column as SHOW
//     EXTENDED FULL COLUMNS reports it (its type and the collation the part
//     compares under);
//   - the database and table as STATISTICS reports them (round-8 review
//     LOW-4): Postgres's pg_get_indexdef names its schema, and without the
//     database a fingerprint minted in one database of a multi-database
//     stream would accept the identical key in another.
//
// A functional part on PlanetScale or Vitess is
// [ir.ErrUniqueKeyDefinitionUnsupported] ([functionalPartSupported]). A key
// not found, not unique, or whose lines cannot be found exactly once is an
// error.
func (r *SchemaReader) UniqueKeyDefinition(ctx context.Context, table, key string) (string, error) {
	fail := func(err error) (string, error) {
		return "", fmt.Errorf("mysql: unique-key definition of %s.%s.%s: %w", r.schema, table, key, err)
	}
	where, lines, plain, functional, err := r.uniqueKeyStatistics(ctx, table, key)
	if err != nil {
		return fail(err)
	}
	if len(lines) == 0 {
		return fail(errors.New("no index of that name on the table"))
	}
	if err := functionalPartSupported(r.flavor, functional); err != nil {
		return fail(err)
	}
	lines = append([]string{where}, lines...)

	ddl, err := r.showCreateTable(ctx, table)
	if err != nil {
		return fail(err)
	}
	keyPrefix := "UNIQUE KEY " + quoteIdent(key) + " ("
	if key == "PRIMARY" {
		keyPrefix = "PRIMARY KEY ("
	}
	keyLine, err := showCreateLine(ddl, keyPrefix)
	if err != nil {
		return fail(err)
	}
	lines = append([]string{joinDefinitionFields("key", keyLine)}, lines...)

	columns, err := r.uniqueKeyDefinitionColumns(ctx, table, ddl, plain, functional)
	if err != nil {
		return fail(err)
	}
	lines = append(lines, columns...)
	if functional { // vanilla only: functionalPartSupported refused the rest
		hidden, err := r.uniqueKeyHiddenColumns(ctx, table, key)
		if err != nil {
			return fail(err)
		}
		lines = append(lines, hidden...)
	}
	return strings.Join(lines, "\n"), nil
}

// functionalPartSupported refuses a functional key part on a flavor that
// does not serve SHOW EXTENDED FULL COLUMNS (round-8 review LOW-1): the
// part compares under its hidden column's collation, which can come from the
// session at CREATE time and then leaves no trace in the expression or in
// any column, so no definition read there can bind it. Such a key cannot be
// accepted; dropping it is the remedy.
func functionalPartSupported(flavor Flavor, functional bool) error {
	if functional && flavor != FlavorVanilla {
		return fmt.Errorf("%w: a functional key part on %s compares under a hidden column's collation that the server "+
			"does not expose", ir.ErrUniqueKeyDefinitionUnsupported, flavor)
	}
	return nil
}

// uniqueKeyStatistics renders the key's information_schema.STATISTICS rows,
// in part order, and returns the database and table as the server reports
// them, its plain parts' columns and whether it has a functional part. A
// non-unique index of the name is an error.
func (r *SchemaReader) uniqueKeyStatistics(ctx context.Context, table, key string) (where string, lines, plain []string, functional bool, err error) {
	expr := `IFNULL(expression, '')`
	if r.flavor == FlavorMariaDB {
		expr = `''`
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT table_schema, table_name, seq_in_index, IFNULL(column_name, ''), `+expr+`, IFNULL(sub_part, 0),
		       IFNULL(collation, ''), non_unique, IFNULL(index_type, '')
		FROM   information_schema.statistics
		WHERE  table_schema = ? AND table_name = ? AND index_name = ?
		ORDER  BY seq_in_index`, r.schema, table, key)
	if err != nil {
		return "", nil, nil, false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var seq, nonUnique int
		var subPart int64
		var schema, tableName, column, expression, collation, indexType string
		if err := rows.Scan(&schema, &tableName, &seq, &column, &expression, &subPart, &collation, &nonUnique, &indexType); err != nil {
			return "", nil, nil, false, err
		}
		if nonUnique != 0 {
			return "", nil, nil, false, errors.New("index is not unique")
		}
		where = joinDefinitionFields("table", schema, tableName)
		if column == "" {
			functional = true
		} else {
			plain = append(plain, column)
		}
		lines = append(lines, joinDefinitionFields("part", fmt.Sprint(seq), column, expression, fmt.Sprint(subPart),
			collation, indexType))
	}
	return where, lines, plain, functional, rows.Err()
}

// uniqueKeyDefinitionColumns renders the columns a key depends on: the
// named plain parts, or every column when the key has a functional part.
func (r *SchemaReader) uniqueKeyDefinitionColumns(ctx context.Context, table, ddl string, plain []string, all bool) ([]string, error) {
	want := map[string]bool{}
	for _, c := range plain {
		want[strings.ToLower(c)] = true
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT ordinal_position, column_name, column_type, IFNULL(character_set_name, ''), IFNULL(collation_name, ''),
		       is_nullable, IFNULL(generation_expression, ''), IFNULL(extra, '')
		FROM   information_schema.columns
		WHERE  table_schema = ? AND table_name = ?
		ORDER  BY ordinal_position`, r.schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var pos int
		var name, typ, charset, collation, nullable, generation, extra string
		if err := rows.Scan(&pos, &name, &typ, &charset, &collation, &nullable, &generation, &extra); err != nil {
			return nil, err
		}
		if !all && !want[strings.ToLower(name)] {
			continue
		}
		delete(want, strings.ToLower(name))
		line, err := showCreateLine(ddl, quoteIdent(name)+" ")
		if err != nil {
			return nil, err
		}
		out = append(out, joinDefinitionFields("column", fmt.Sprint(pos), name, typ, charset, collation, nullable,
			generation, extra, line))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(want) > 0 {
		return nil, errors.New("a key column is not in information_schema.columns")
	}
	if len(out) == 0 {
		return nil, errors.New("the key depends on no column information_schema lists")
	}
	return out, nil
}

// uniqueKeyHiddenColumns renders, from SHOW EXTENDED FULL COLUMNS, the
// hidden generated columns behind the key's functional parts
// (`!hidden!<key>!<part>!<n>`, the name truncated at 64 characters, in
// which case every column the truncated prefix can name is taken). Finding
// none is an error: the part's collation would go unread.
func (r *SchemaReader) uniqueKeyHiddenColumns(ctx context.Context, table, key string) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, "SHOW EXTENDED FULL COLUMNS FROM "+quoteIdent(r.schema)+"."+quoteIdent(table))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range vals {
		dest[i] = &vals[i]
	}
	var out []string
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		field := vals[0].String
		name, ok := strings.CutPrefix(field, "!hidden!")
		if !ok {
			continue
		}
		if utf8.RuneCountInString(field) >= 64 {
			// Truncated: a prefix of `!hidden!<key>!…`, or one extending it.
			full := "!hidden!" + key + "!"
			if !strings.HasPrefix(field, full) && !strings.HasPrefix(full, field) {
				continue
			}
		} else {
			rest, _, ok1 := cutLast(name, "!")
			index, _, ok2 := cutLast(rest, "!")
			if !ok1 || !ok2 || index != key {
				continue
			}
		}
		fields := []string{"hidden"}
		for _, v := range vals {
			fields = append(fields, v.String)
		}
		out = append(out, joinDefinitionFields(fields...))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, errors.New("SHOW EXTENDED FULL COLUMNS lists no hidden column for the key's functional part")
	}
	return out, nil
}

// showCreateLine returns the one line of a SHOW CREATE TABLE statement that
// starts, after its indentation, with prefix, trailing comma dropped; an
// error when no line or more than one does.
func showCreateLine(ddl, prefix string) (string, error) {
	var found []string
	for _, line := range strings.Split(ddl, "\n") {
		if l := strings.TrimLeft(line, " "); strings.HasPrefix(l, prefix) {
			found = append(found, strings.TrimSuffix(strings.TrimRight(l, "\r"), ","))
		}
	}
	if len(found) != 1 {
		return "", fmt.Errorf("SHOW CREATE TABLE has %d lines starting %q, want exactly one", len(found), prefix)
	}
	return found[0], nil
}

func joinDefinitionFields(fields ...string) string {
	return strings.Join(fields, uniqueKeyDefinitionSep)
}
