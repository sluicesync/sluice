// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"strings"
)

// uniqueKeyDefinitionSep separates the fields of one line of a
// [SchemaReader.UniqueKeyDefinition]: a control character no catalog
// rendering contains, so two different field lists cannot join to the same
// text.
const uniqueKeyDefinitionSep = "\x1f"

// UniqueKeyDefinition implements [ir.UniqueKeyDefinitionReader] for the
// stream's unique-key door (round-7 review MEDIUM-1). The definition is
// Postgres's own, read under a search_path of pg_catalog alone so every
// user type, collation and operator class comes back schema-qualified the
// same way on every read:
//
//   - the index: pg_get_indexdef, which renders its parts, expressions,
//     per-part COLLATE and operator class, NULLS NOT DISTINCT and the
//     predicate with its literals verbatim; and whether it is enforced
//     (indisvalid AND indisready);
//   - every table column it depends on, in attnum order: the key and
//     INCLUDE columns (indkey) and every column an expression or the
//     predicate reads (pg_depend on the index, refobjsubid > 0). For each,
//     its attnum, name, format_type with typmod, a domain's base type, its
//     collation schema-qualified with collisdeterministic (an array's
//     attcollation is its element's), NOT NULL, and a generated column's
//     expression.
//
// A key not found on the table is an error.
func (r *SchemaReader) UniqueKeyDefinition(ctx context.Context, table, key string) (string, error) {
	const indexQ = `
		SELECT ix.indexrelid::bigint, ix.indrelid::bigint,
		       pg_catalog.pg_get_indexdef(ix.indexrelid),
		       ix.indisvalid AND ix.indisready
		FROM   pg_catalog.pg_index ix
		JOIN   pg_catalog.pg_class     cl ON cl.oid = ix.indrelid
		JOIN   pg_catalog.pg_class     i  ON i.oid  = ix.indexrelid
		JOIN   pg_catalog.pg_namespace n  ON n.oid  = cl.relnamespace
		WHERE  n.nspname = $1 AND cl.relname = $2 AND i.relname = $3 AND ix.indisunique`
	rows, err := catalogQueryUnderSearchPath(ctx, r.db, "pg_catalog", indexQ, r.schema, table, key)
	if err != nil {
		return "", fmt.Errorf("postgres: unique-key definition of %s.%s.%s: %w", r.schema, table, key, err)
	}
	var indexOID, tableOID int64
	var def string
	var enforced bool
	found := 0
	for rows.Next() {
		if err := rows.Scan(&indexOID, &tableOID, &def, &enforced); err != nil {
			_ = rows.Close()
			return "", fmt.Errorf("postgres: unique-key definition of %s.%s.%s: %w", r.schema, table, key, err)
		}
		found++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return "", fmt.Errorf("postgres: unique-key definition of %s.%s.%s: %w", r.schema, table, key, err)
	}
	_ = rows.Close()
	if found != 1 {
		return "", fmt.Errorf("postgres: unique-key definition of %s.%s.%s: no unique index of that name on the table",
			r.schema, table, key)
	}

	const columnsQ = `
		SELECT a.attnum, a.attname,
		       pg_catalog.format_type(a.atttypid, a.atttypmod),
		       CASE WHEN t.typtype = 'd' THEN pg_catalog.format_type(t.typbasetype, t.typtypmod) ELSE '' END,
		       COALESCE(pg_catalog.quote_ident(cn.nspname) || '.' || pg_catalog.quote_ident(co.collname), ''),
		       COALESCE(co.collisdeterministic::text, ''),
		       a.attnotnull,
		       a.attgenerated::text,
		       COALESCE(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '')
		FROM   pg_catalog.pg_attribute a
		JOIN   pg_catalog.pg_type t ON t.oid = a.atttypid
		LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
		LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = co.collnamespace
		LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum AND a.attgenerated <> ''
		WHERE  a.attrelid = $1::oid AND NOT a.attisdropped AND a.attnum > 0
		  AND  a.attnum IN (
		         SELECT k::int2 FROM pg_catalog.pg_index ix, pg_catalog.unnest(ix.indkey) AS k WHERE ix.indexrelid = $2::oid
		         UNION
		         SELECT d.refobjsubid::int2 FROM pg_catalog.pg_depend d
		         WHERE  d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.objid = $2::oid
		           AND  d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.refobjid = $1::oid
		           AND  d.refobjsubid > 0)
		ORDER  BY a.attnum`
	crows, err := catalogQueryUnderSearchPath(ctx, r.db, "pg_catalog", columnsQ, tableOID, indexOID)
	if err != nil {
		return "", fmt.Errorf("postgres: unique-key definition of %s.%s.%s: columns: %w", r.schema, table, key, err)
	}
	defer func() { _ = crows.Close() }()
	lines := []string{"index" + uniqueKeyDefinitionSep + def + uniqueKeyDefinitionSep + fmt.Sprint(enforced)}
	for crows.Next() {
		var attnum int
		var name, typ, base, coll, deterministic, generated, genExpr string
		var notNull bool
		if err := crows.Scan(&attnum, &name, &typ, &base, &coll, &deterministic, &notNull, &generated, &genExpr); err != nil {
			return "", fmt.Errorf("postgres: unique-key definition of %s.%s.%s: columns: %w", r.schema, table, key, err)
		}
		lines = append(lines, strings.Join([]string{
			"column", fmt.Sprint(attnum), name, typ, base, coll, deterministic, fmt.Sprint(notNull), generated, genExpr,
		}, uniqueKeyDefinitionSep))
	}
	if err := crows.Err(); err != nil {
		return "", fmt.Errorf("postgres: unique-key definition of %s.%s.%s: columns: %w", r.schema, table, key, err)
	}
	if len(lines) == 1 {
		return "", fmt.Errorf("postgres: unique-key definition of %s.%s.%s: the key depends on no column the catalog lists",
			r.schema, table, key)
	}
	return strings.Join(lines, "\n"), nil
}
