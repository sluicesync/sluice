// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestOpenForeignDumpRefusals pins the Phase 3 extension of the ADR-0130
// sniff (ADR-0163): a plain mysqldump/pg_dump .sql or a PGDMP archive
// handed to the sqlite driver refuses with the coded scratch-server recipe
// BEFORE the materializer runs (previously it died mid-materialize on a
// confusing SQL error); flat-file extensions and mydumper directories name
// the right driver.
func TestOpenForeignDumpRefusals(t *testing.T) {
	write := func(t *testing.T, name, content string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	wantCode := func(t *testing.T, err error, code sluicecode.Code, sub string) {
		t.Helper()
		if err == nil {
			t.Fatal("expected a refusal, got nil")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != code {
			t.Fatalf("error code = %v; want %s (err: %v)", ce, code, err)
		}
		if !strings.Contains(err.Error(), sub) {
			t.Errorf("error %q does not contain %q", err.Error(), sub)
		}
	}
	ctx := context.Background()

	t.Run("mysqldump sql", func(t *testing.T) {
		p := write(t, "dump.sql", "-- MySQL dump 10.13  Distrib 8.0.36\nCREATE TABLE t (a int);\n")
		_, err := Engine{}.OpenSchemaReader(ctx, p)
		wantCode(t, err, sluicecode.CodeSourceForeignDump, "scratch MySQL")
	})
	t.Run("pg_dump plain sql", func(t *testing.T) {
		p := write(t, "dump.sql", "--\n-- PostgreSQL database dump\n--\nSET statement_timeout = 0;\n")
		_, err := Engine{}.OpenSchemaReader(ctx, p)
		wantCode(t, err, sluicecode.CodeSourceForeignDump, "scratch PostgreSQL")
	})
	t.Run("pg_dump custom PGDMP", func(t *testing.T) {
		p := write(t, "dump.bin", "PGDMP\x01\x0ebinary")
		_, err := Engine{}.OpenSchemaReader(ctx, p)
		wantCode(t, err, sluicecode.CodeSourceForeignDump, "pg_restore")
	})
	t.Run("csv extension names the csv driver", func(t *testing.T) {
		p := write(t, "data.csv", "a,b\n1,2\n")
		_, err := Engine{}.OpenSchemaReader(ctx, p)
		wantCode(t, err, sluicecode.CodeSourceWrongDriver, "--source-driver csv")
	})
	t.Run("mydumper directory names the mydumper driver", func(t *testing.T) {
		dir := t.TempDir()
		for name, content := range map[string]string{
			"metadata":              "Started dump at: 2026-07-15\n",
			"shop.users-schema.sql": "CREATE TABLE `users` (id int);\n",
		} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		_, err := Engine{}.OpenSchemaReader(ctx, dir)
		wantCode(t, err, sluicecode.CodeSourceWrongDriver, "--source-driver mydumper")
	})
	t.Run("a real sqlite3 SQL dump still materializes", func(t *testing.T) {
		p := write(t, "dump.sql",
			"PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n"+
				"CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\nCOMMIT;\n")
		sr, err := Engine{}.OpenSchemaReader(ctx, p)
		if err != nil {
			t.Fatalf("a genuine sqlite dump must keep working: %v", err)
		}
		defer func() { _ = sr.(*SchemaReader).Close() }()
		schema, err := sr.ReadSchema(ctx)
		if err != nil || len(schema.Tables) != 1 {
			t.Fatalf("ReadSchema = %v tables, err %v; want 1 table", len(schema.Tables), err)
		}
	})
}

// TestSchemaReaderExactRowCount pins the new ir.Verifier surface on the
// file schema reader (ADR-0163: verify --depth count for sqlite and staged
// flat-file endpoints).
func TestSchemaReaderExactRowCount(t *testing.T) {
	p := filepath.Join(t.TempDir(), "dump.sql")
	dump := "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\nINSERT INTO t VALUES (2);\n"
	if err := os.WriteFile(p, []byte(dump), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sr, err := Engine{}.OpenSchemaReader(ctx, p)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	r := sr.(*SchemaReader)
	defer func() { _ = r.Close() }()
	schema, err := r.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	n, err := r.ExactRowCount(ctx, schema.Tables[0])
	if err != nil {
		t.Fatalf("ExactRowCount: %v", err)
	}
	if n != 2 {
		t.Errorf("ExactRowCount = %d; want 2", n)
	}
	if _, err := r.ExactRowCount(ctx, nil); err == nil {
		t.Error("ExactRowCount(nil) should refuse")
	}
}
