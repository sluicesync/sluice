// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestRegistryNames pins the three driver registrations.
func TestRegistryNames(t *testing.T) {
	for _, name := range []string{"csv", "tsv", "ndjson"} {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engine %q not registered", name)
		}
		if e.Name() != name {
			t.Errorf("engine %q reports Name()=%q", name, e.Name())
		}
		caps := e.Capabilities()
		if caps.CDC != ir.CDCNone || caps.BulkLoad != ir.BulkLoadNone {
			t.Errorf("engine %q capabilities = CDC %v / BulkLoad %v; want CDCNone/BulkLoadNone", name, caps.CDC, caps.BulkLoad)
		}
	}
}

// TestSourceOnlySurfaces pins the d1/mydumper posture: every write/CDC/
// snapshot opener refuses with ErrNotImplemented.
func TestSourceOnlySurfaces(t *testing.T) {
	e := Engine{format: formatCSV}
	ctx := context.Background()
	if _, err := e.OpenSchemaWriter(ctx, "x"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenSchemaWriter err = %v; want ErrNotImplemented", err)
	}
	if _, err := e.OpenRowWriter(ctx, "x"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenRowWriter err = %v; want ErrNotImplemented", err)
	}
	if _, err := e.OpenCDCReader(ctx, "x"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenCDCReader err = %v; want ErrNotImplemented", err)
	}
	if _, err := e.OpenChangeApplier(ctx, "x"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenChangeApplier err = %v; want ErrNotImplemented", err)
	}
	if _, err := e.OpenSnapshotStream(ctx, "x"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenSnapshotStream err = %v; want ErrNotImplemented", err)
	}
}

// TestForeignDumpRefusals pins Phase 3 on the flat-file drivers: a plain
// mysqldump/pg_dump .sql and a PGDMP archive refuse with the coded recipe;
// SQLite binaries, compressed files, and UTF-16 name their preparation step.
func TestForeignDumpRefusals(t *testing.T) {
	e := csvEngine(t, Options{HeaderDeclared: true, Header: true})
	cases := []struct {
		name, file, content string
		code                sluicecode.Code
		wantSub             string
	}{
		{
			"mysqldump sql", "dump.csv",
			"-- MySQL dump 10.13  Distrib 8.0.36\n--\n-- Host: localhost\nCREATE TABLE t (a int);\n",
			sluicecode.CodeSourceForeignDump, "scratch MySQL",
		},
		{
			"pg_dump plain sql", "dump.csv",
			"--\n-- PostgreSQL database dump\n--\nSET statement_timeout = 0;\n",
			sluicecode.CodeSourceForeignDump, "scratch PostgreSQL",
		},
		{
			"pg_dump custom PGDMP", "dump.csv",
			"PGDMP\x01\x0e\x00\x04\x08binary-junk",
			sluicecode.CodeSourceForeignDump, "pg_restore",
		},
		{
			"sqlite binary", "data.csv",
			"SQLite format 3\x00more-binary-header-bytes",
			sluicecode.CodeSourceWrongDriver, "--source-driver sqlite",
		},
		{
			"gzip", "data.csv",
			"\x1f\x8b\x08\x00compressed",
			sluicecode.CodeSourceWrongDriver, "decompress",
		},
		{
			"zstd", "data.csv",
			"\x28\xb5\x2f\xfdcompressed",
			sluicecode.CodeSourceWrongDriver, "decompress",
		},
		{
			"utf-16 BOM", "data.csv",
			"\xff\xfea\x00,\x00b\x00",
			sluicecode.CodeSourceWrongDriver, "UTF-8",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeSource(t, tc.file, tc.content)
			_, err := e.OpenSchemaReader(context.Background(), path)
			wantCode(t, err, tc.code)
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

// TestCrossDriverMisuse pins the wrong-driver refusals: a mydumper
// directory handed to csv, and the extension cross-checks (with the
// explicit-delimiter escape hatch).
func TestCrossDriverMisuse(t *testing.T) {
	t.Run("mydumper directory to the csv driver", func(t *testing.T) {
		dir := t.TempDir()
		for name, content := range map[string]string{
			"metadata":              "Started dump at: 2026-07-15\n",
			"shop.users-schema.sql": "CREATE TABLE `users` (id int);\n",
		} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		e := csvEngine(t, Options{HeaderDeclared: true, Header: true})
		_, err := e.OpenSchemaReader(context.Background(), dir)
		wantCode(t, err, sluicecode.CodeSourceWrongDriver)
		if !strings.Contains(err.Error(), "--source-driver mydumper") {
			t.Errorf("error %q should name the mydumper driver", err.Error())
		}
	})
	t.Run("plain directory to the csv driver", func(t *testing.T) {
		e := csvEngine(t, Options{HeaderDeclared: true, Header: true})
		_, err := e.OpenSchemaReader(context.Background(), t.TempDir())
		if err == nil || !strings.Contains(err.Error(), "is a directory") {
			t.Fatalf("want the directory refusal; got %v", err)
		}
	})
	t.Run(".tsv to the csv driver refuses without an explicit delimiter", func(t *testing.T) {
		e := csvEngine(t, Options{HeaderDeclared: true, Header: true})
		path := writeSource(t, "data.tsv", "a\tb\n1\t2\n")
		_, err := e.OpenSchemaReader(context.Background(), path)
		wantCode(t, err, sluicecode.CodeSourceWrongDriver)
	})
	t.Run(".tsv to the csv driver proceeds with an explicit delimiter", func(t *testing.T) {
		e := csvEngine(t, Options{HeaderDeclared: true, Header: true, Delimiter: ",", NullRepr: strp("")})
		path := writeSource(t, "data.tsv", "a,b\n1,2\n")
		_, rows := readStaged(t, e, path)
		wantText(t, rows[0], "b", "2")
	})
	t.Run(".ndjson to the csv driver refuses", func(t *testing.T) {
		e := csvEngine(t, Options{HeaderDeclared: true, Header: true})
		path := writeSource(t, "data.ndjson", `{"a":1}`+"\n")
		_, err := e.OpenSchemaReader(context.Background(), path)
		wantCode(t, err, sluicecode.CodeSourceWrongDriver)
	})
	t.Run(".csv to the ndjson driver refuses", func(t *testing.T) {
		e := ndjsonEngine(t)
		path := writeSource(t, "data.csv", "a,b\n1,2\n")
		_, err := e.OpenSchemaReader(context.Background(), path)
		wantCode(t, err, sluicecode.CodeSourceWrongDriver)
	})
}

// TestVerifyExactRowCount pins the count-depth verify surface on a staged
// source: the schema reader implements ir.Verifier and counts the staged
// rows authoritatively.
func TestVerifyExactRowCount(t *testing.T) {
	e := csvEngine(t, Options{HeaderDeclared: true, Header: true, NullRepr: strp("")})
	path := writeSource(t, "count.csv", "a\n1\n2\n3\n")
	ctx := context.Background()

	sr, err := e.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	v, ok := sr.(ir.Verifier)
	if !ok {
		t.Fatal("staged schema reader does not implement ir.Verifier")
	}
	n, err := v.ExactRowCount(ctx, schema.Tables[0])
	if err != nil {
		t.Fatalf("ExactRowCount: %v", err)
	}
	if n != 3 {
		t.Errorf("ExactRowCount = %d; want 3", n)
	}
}

// TestInferTypeValidatorSurface pins that the staged schema reader carries
// the ADR-0144 validator --infer-types rides, and that it promotes a
// conforming staged TEXT column (ISO timestamps) while keeping a
// non-conforming one (the cus_abc123 *_id case).
func TestInferTypeValidatorSurface(t *testing.T) {
	e := csvEngine(t, Options{HeaderDeclared: true, Header: true, NullRepr: strp("")})
	path := writeSource(t, "infer.csv",
		"created_at,customer_id\n"+
			"2024-01-02 03:04:05,cus_abc123\n"+
			"2024-06-07T08:09:10,cus_def456\n")
	ctx := context.Background()

	sr, err := e.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	if _, err := sr.ReadSchema(ctx); err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	val, ok := sr.(ir.InferredTypeValidator)
	if !ok {
		t.Fatal("staged schema reader does not implement ir.InferredTypeValidator")
	}
	conforms, resolved, n, err := val.ValidateInferredType(ctx, "infer", "created_at", ir.Timestamp{})
	if err != nil {
		t.Fatalf("ValidateInferredType(created_at): %v", err)
	}
	if !conforms || n != 2 {
		t.Errorf("created_at: conforms=%v n=%d; want true/2", conforms, n)
	}
	if ts, ok := resolved.(ir.Timestamp); !ok || ts.WithTimeZone {
		t.Errorf("created_at resolved = %#v; want naive ir.Timestamp", resolved)
	}
	conforms, _, _, err = val.ValidateInferredType(ctx, "infer", "customer_id", ir.UUID{})
	if err != nil {
		t.Fatalf("ValidateInferredType(customer_id): %v", err)
	}
	if conforms {
		t.Error("customer_id (cus_abc123) must NOT validate as uuid")
	}
}
