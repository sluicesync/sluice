//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestUniqueKeyDefinition_EveryDimension pins [SchemaReader.UniqueKeyDefinition]
// on a real server (round-7 review MEDIUM-1): the definition the unique-key
// door's acceptance fingerprint hashes changes with a functional key's
// column collation (the round-6 shape skipped an expression part's
// collation; a MySQL `lower(k)` under `_bin` hashed like one under
// `_ai_ci`), with a literal inside a functional part differing only in case
// and a trailing space, with a non-ASCII literal information_schema renders
// lossily (Bug 288), with a prefix length, and with a size-only widening;
// it is the same on every read through a fresh reader (a restart), and a
// key the table does not have is an error.
func TestUniqueKeyDefinition_EveryDimension(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "ukdef_db")
	defer cleanup()
	ctx := context.Background()
	applyMySQLApplier(t, dsn, `
		CREATE TABLE t_expr (id INT PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, UNIQUE KEY uk_expr ((lower(k))));
		CREATE TABLE t_lit (id INT PRIMARY KEY, k VARCHAR(20), UNIQUE KEY uk_lit ((concat(k, 'Active'))));
		CREATE TABLE t_utf (id INT PRIMARY KEY, k VARCHAR(20), UNIQUE KEY uk_utf ((concat(k, 'é'))));
		CREATE TABLE t_pre (id INT PRIMARY KEY, k VARCHAR(20), UNIQUE KEY uk_pre (k(5)));
		CREATE TABLE t_size (id INT PRIMARY KEY, k VARCHAR(20), UNIQUE KEY uk_size (k));`)
	define := func(table, key string) string {
		t.Helper()
		read := func() string {
			sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sr.(interface{ Close() error }).Close() }()
			def, err := sr.(ir.UniqueKeyDefinitionReader).UniqueKeyDefinition(ctx, table, key)
			if err != nil {
				t.Fatalf("definition of %s.%s: %v", table, key, err)
			}
			return def
		}
		def := read()
		if def == "" || def != read() {
			t.Fatalf("definition of %s.%s is empty or not the same on a second read", table, key)
		}
		return def
	}
	for _, c := range []struct {
		name, table, key, change, mustMention string
	}{
		{
			"functional key's column collation", "t_expr", "uk_expr",
			"ALTER TABLE t_expr MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci",
		},
		{
			"a functional literal's case and space", "t_lit", "uk_lit",
			"ALTER TABLE t_lit DROP INDEX uk_lit, ADD UNIQUE KEY uk_lit ((concat(k, 'active ')))", "active ",
		},
		{
			"a non-ASCII functional literal", "t_utf", "uk_utf",
			"ALTER TABLE t_utf DROP INDEX uk_utf, ADD UNIQUE KEY uk_utf ((concat(k, 'è')))", "è",
		},
		{"a prefix length", "t_pre", "uk_pre", "ALTER TABLE t_pre DROP INDEX uk_pre, ADD UNIQUE KEY uk_pre (k(6))", "(6)"},
		{"size-only widening", "t_size", "uk_size", "ALTER TABLE t_size MODIFY k VARCHAR(30)", "varchar(30)"},
	} {
		t.Run(c.name, func(t *testing.T) {
			before := define(c.table, c.key)
			applyMySQLApplier(t, dsn, c.change)
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
	if def, err := sr.(ir.UniqueKeyDefinitionReader).UniqueKeyDefinition(ctx, "t_size", "nope"); err == nil {
		t.Fatalf("a missing key returned %q, want an error", def)
	}
}
