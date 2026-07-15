// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 188 pins: the ADR-0161 §5 unsupported-charset refusal is
// DEFERRED from ReadSchema to the per-table read preflight (and the
// row reader's own guard), so one legacy latin1 table no longer blocks
// migrating the rest of a dump — the pipeline's --exclude-table filter
// runs between ReadSchema and the preflight and can route around it.

package mydumper

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// newTwoTableDump stages a dump with one clean utf8mb4 table and one
// latin1 table, returning the opened dumpDir.
func newTwoTableDump(t *testing.T) *dumpDir {
	t.Helper()
	dir := t.TempDir()
	writeDumpFile(t, dir, "metadata", "")
	writeDumpFile(t, dir, "shop.ok-schema.sql",
		"CREATE TABLE `ok` (`id` bigint NOT NULL, `s` varchar(20)) DEFAULT CHARSET=utf8mb4;")
	writeDumpFile(t, dir, "shop.ok.00000.sql", "INSERT INTO `ok` VALUES (1,'a');")
	writeDumpFile(t, dir, "shop.legacy-schema.sql",
		"CREATE TABLE `legacy` (`id` bigint NOT NULL, `txt` varchar(20)) DEFAULT CHARSET=latin1;")
	writeDumpFile(t, dir, "shop.legacy.00000.sql", "INSERT INTO `legacy` VALUES (1,'b');")
	d, err := openDumpDir(dir)
	if err != nil {
		t.Fatalf("openDumpDir: %v", err)
	}
	return d
}

func TestReadSchema_CarriesUnsupportedCharsetTable(t *testing.T) {
	r := &SchemaReader{dir: newTwoTableDump(t)}
	s, err := r.ReadSchema(context.Background())
	if err != nil {
		t.Fatalf("ReadSchema must not fail on a deferred charset violation (Bug 188); got %v", err)
	}
	if len(s.Tables) != 2 {
		t.Fatalf("tables = %d; want both (the latin1 table rides along, refusal deferred)", len(s.Tables))
	}
}

func TestPreflightTableRead_ReturnsDeferredRefusal(t *testing.T) {
	r := &SchemaReader{dir: newTwoTableDump(t)}
	if err := r.PreflightTableRead("ok"); err != nil {
		t.Fatalf("clean table preflight = %v; want nil", err)
	}
	err := r.PreflightTableRead("legacy")
	var charsetErr *CharsetRefusalError
	if !errors.As(err, &charsetErr) {
		t.Fatalf("legacy preflight = %v; want *CharsetRefusalError", err)
	}
	for _, want := range []string{"legacy", "txt", "latin1", "--exclude-table", "ADR-0161"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err.Error(), want)
		}
	}
	if r.PreflightTableRead("no-such-table") != nil {
		t.Error("unknown table must preflight nil (not this reader's problem)")
	}
}

func TestReadRows_RefusesUnsupportedCharsetTable(t *testing.T) {
	d := newTwoTableDump(t)
	r := &SchemaReader{dir: d}
	s, err := r.ReadSchema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var legacy *ir.Table
	for _, tbl := range s.Tables {
		if tbl.Name == "legacy" {
			legacy = tbl
		}
	}
	if legacy == nil {
		t.Fatal("legacy table missing from schema")
	}
	rr := &RowReader{dir: d}
	_, err = rr.ReadRows(context.Background(), legacy)
	var charsetErr *CharsetRefusalError
	if !errors.As(err, &charsetErr) {
		t.Fatalf("ReadRows on the latin1 table = %v; want the charset refusal (defense in depth when the pipeline preflight is skipped)", err)
	}
}
