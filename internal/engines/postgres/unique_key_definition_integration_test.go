//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestUniqueKeyDefinition_EveryDimension pins [SchemaReader.UniqueKeyDefinition]
// on a real server (round-7 review MEDIUM-1): the definition the unique-key
// door's acceptance fingerprint hashes changes with each dimension the
// round-6 hand-built shape collided on — (a) a text[] key whose element
// collation turns nondeterministic, (b) an expression key whose column's
// collation changes, (c) a predicate literal differing in case and a
// trailing space, (d) an operator class — and with a size-only widening, a
// column read only by the predicate, and a domain column made a plain type;
// and it is
// the same on every read, through a fresh reader each time (a restart). A
// key the table does not have is an error, never an empty definition.
func TestUniqueKeyDefinition_EveryDimension(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "ukdef_db")
	defer cleanup()
	ctx := context.Background()
	applyPGApplier(t, dsn, `
		CREATE COLLATION IF NOT EXISTS ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
		CREATE DOMAIN short_text AS varchar(10);
		CREATE TABLE t_arr (id int, tags text[] NOT NULL);  CREATE UNIQUE INDEX uk_arr ON t_arr (tags);
		CREATE TABLE t_expr (id int, k varchar(20));        CREATE UNIQUE INDEX uk_expr ON t_expr ((lower(k)));
		CREATE TABLE t_pred (id int, k text, status varchar(10));
		CREATE UNIQUE INDEX uk_pred ON t_pred (k) WHERE status = 'Active';
		CREATE TABLE t_op (id int, k text);                 CREATE UNIQUE INDEX uk_op ON t_op (k);
		CREATE TABLE t_size (id int, k varchar(20));        CREATE UNIQUE INDEX uk_size ON t_size (k);
		CREATE TABLE t_dom (id int, k short_text);          CREATE UNIQUE INDEX uk_dom ON t_dom (k);`)
	define := func(table, key string) string {
		t.Helper()
		sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sr.(interface{ Close() error }).Close() }()
		def, err := sr.(ir.UniqueKeyDefinitionReader).UniqueKeyDefinition(ctx, table, key)
		if err != nil {
			t.Fatalf("definition of %s.%s: %v", table, key, err)
		}
		if def != "" && def == definitionOnce(t, ctx, dsn, table, key) {
			return def
		}
		t.Fatalf("definition of %s.%s is not the same on a second read", table, key)
		return ""
	}
	for _, c := range []struct {
		name, table, key, change, mustMention string
	}{
		{"(a) array element collation", "t_arr", "uk_arr", "ALTER TABLE t_arr ALTER COLUMN tags TYPE text[] COLLATE ci", "ci"},
		{"(b) expression's column collation", "t_expr", "uk_expr", "ALTER TABLE t_expr ALTER COLUMN k TYPE varchar(20) COLLATE ci", "ci"},
		{
			"(c) predicate literal case and space", "t_pred", "uk_pred",
			"DROP INDEX uk_pred; CREATE UNIQUE INDEX uk_pred ON t_pred (k) WHERE status = 'active '", "'active '",
		},
		{"(d) operator class", "t_op", "uk_op", "DROP INDEX uk_op; CREATE UNIQUE INDEX uk_op ON t_op (k text_pattern_ops)", "text_pattern_ops"},
		{"size-only widening", "t_size", "uk_size", "ALTER TABLE t_size ALTER COLUMN k TYPE varchar(30)", "30"},
		{"a column read only by the predicate", "t_pred", "uk_pred", "ALTER TABLE t_pred ALTER COLUMN status TYPE varchar(12)", "12"},
		{"a domain column made a plain type", "t_dom", "uk_dom", "ALTER TABLE t_dom ALTER COLUMN k TYPE varchar(10)", "character varying(10)"},
	} {
		t.Run(c.name, func(t *testing.T) {
			before := define(c.table, c.key)
			applyPGApplier(t, dsn, c.change)
			after := define(c.table, c.key)
			if before == after {
				t.Fatalf("the definition did not change:\n%s", after)
			}
			if !strings.Contains(after, c.mustMention) {
				t.Errorf("the new definition does not carry %q:\n%s", c.mustMention, after)
			}
		})
	}
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()
	if def, err := sr.(ir.UniqueKeyDefinitionReader).UniqueKeyDefinition(ctx, "t_arr", "nope"); err == nil {
		t.Fatalf("a missing key returned %q, want an error", def)
	}
}

// definitionOnce reads one definition through its own reader.
func definitionOnce(t *testing.T, ctx context.Context, dsn, table, key string) string {
	t.Helper()
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()
	def, err := sr.(ir.UniqueKeyDefinitionReader).UniqueKeyDefinition(ctx, table, key)
	if err != nil {
		t.Fatal(err)
	}
	return def
}
