// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// writeDumpFile writes one file into dir, compressing per the filename
// suffix so fixtures exercise the same transparent-decompression path
// real compressed dumps take.
func writeDumpFile(t *testing.T, dir, name, content string) {
	t.Helper()
	data := []byte(content)
	switch {
	case strings.HasSuffix(name, ".gz"):
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		data = buf.Bytes()
	case strings.HasSuffix(name, ".zst"):
		var buf bytes.Buffer
		zw, err := zstd.NewWriter(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := zw.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		data = buf.Bytes()
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

const traditionalMetadata = `Started dump at: 2026-07-14 10:00:00
SHOW MASTER STATUS:
	Log: mysql-bin.000002
	Pos: 12345
	GTID:00000000-0000-0000-0000-000000000001:1-42

Finished dump at: 2026-07-14 10:00:05
`

const iniMetadata = `[config]
quote_character = BACKTICK

[master]
# Channel_Name = ''
File = binlog.000007
Position = 999
Executed_Gtid_Set = 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5
`

func TestOpenDumpDir_LayoutAndMetadata(t *testing.T) {
	dir := t.TempDir()
	writeDumpFile(t, dir, "metadata", traditionalMetadata)
	writeDumpFile(t, dir, "shop-schema-create.sql", "CREATE DATABASE `shop`;")
	writeDumpFile(t, dir, "shop.users-schema.sql", "CREATE TABLE `users` (`id` bigint NOT NULL);")
	writeDumpFile(t, dir, "shop.users.00000.sql", "INSERT INTO `users` VALUES (1);")
	writeDumpFile(t, dir, "shop.users.00001.sql", "INSERT INTO `users` VALUES (2);")
	writeDumpFile(t, dir, "shop.empty-schema.sql.gz", "CREATE TABLE `empty` (`id` bigint NOT NULL);")
	writeDumpFile(t, dir, "shop.users-metadata", "2\n")

	d, err := openDumpDir(dir)
	if err != nil {
		t.Fatalf("openDumpDir: %v", err)
	}
	if d.database != "shop" {
		t.Fatalf("database = %q", d.database)
	}
	if len(d.tableOrder) != 2 || d.tableOrder[0] != "empty" || d.tableOrder[1] != "users" {
		t.Fatalf("tableOrder = %v", d.tableOrder)
	}
	if got := len(d.tables["users"].chunks); got != 2 {
		t.Fatalf("users chunks = %d", got)
	}
	if got := len(d.tables["empty"].chunks); got != 0 {
		t.Fatalf("empty chunks = %d", got)
	}
	if d.binlogFile != "mysql-bin.000002" || d.binlogPos != "12345" ||
		d.gtidSet != "00000000-0000-0000-0000-000000000001:1-42" {
		t.Fatalf("metadata = %q %q %q", d.binlogFile, d.binlogPos, d.gtidSet)
	}
}

func TestParseMetadata_IniShape(t *testing.T) {
	dir := t.TempDir()
	writeDumpFile(t, dir, "metadata", iniMetadata)
	writeDumpFile(t, dir, "shop.t-schema.sql", "CREATE TABLE `t` (`id` bigint NOT NULL);")
	d, err := openDumpDir(dir)
	if err != nil {
		t.Fatalf("openDumpDir: %v", err)
	}
	if d.binlogFile != "binlog.000007" || d.binlogPos != "999" ||
		d.gtidSet != "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5" {
		t.Fatalf("metadata = %q %q %q", d.binlogFile, d.binlogPos, d.gtidSet)
	}
}

// TestOpenDumpDir_Refusals pins the detect-at-open contract: anything that
// is not a completed single-database mydumper dump refuses loudly.
func TestOpenDumpDir_Refusals(t *testing.T) {
	schema := "CREATE TABLE `t` (`id` bigint NOT NULL);"
	cases := []struct {
		name    string
		build   func(t *testing.T, dir string)
		wantErr string
	}{
		{"empty-dir", func(_ *testing.T, _ string) {}, "no `metadata` file"},
		{"no-metadata", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
		}, "no `metadata` file"},
		{"no-schema-files", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
		}, "no `<db>.<table>-schema.sql` files"},
		{"partial-dump", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "metadata.partial", "in flight")
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
		}, "did not complete"},
		{"stranger-file", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
			writeDumpFile(t, dir, "notes.txt", "hello")
		}, "does not recognise"},
		{"multi-database", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
			writeDumpFile(t, dir, "other.u-schema.sql", schema)
		}, "2 databases"},
		{"chunk-without-schema", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
			writeDumpFile(t, dir, "shop.ghost.00000.sql", "INSERT INTO `ghost` VALUES (1);")
		}, "no matching"},
		{"subdirectory", func(t *testing.T, dir string) {
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "shop.t-schema.sql", schema)
			if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
				t.Fatal(err)
			}
		}, "unexpected subdirectory"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.build(t, dir)
			_, err := openDumpDir(dir)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v; want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestOpenDumpDir_NotADirectory(t *testing.T) {
	// A mysqldump-signature file now gets the recipe-bearing foreign-dump
	// refusal instead (ADR-0163, pinned in dir_wrong_driver_test.go); an
	// unrecognised file keeps the generic not-a-directory shape.
	dir := t.TempDir()
	file := filepath.Join(dir, "dump.sql")
	if err := os.WriteFile(file, []byte("SELECT 1; -- not any recognised dump signature"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openDumpDir(file); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("err = %v; want a not-a-directory refusal", err)
	}
}

func TestSortChunks_NumericOrder(t *testing.T) {
	chunks := []string{
		"/d/shop.t.00010.sql",
		"/d/shop.t.00002.sql.gz",
		"/d/shop.t.100000.sql", // widened field past 99999
		"/d/shop.t.00000.sql",
	}
	sortChunks(chunks)
	want := []string{
		"/d/shop.t.00000.sql",
		"/d/shop.t.00002.sql.gz",
		"/d/shop.t.00010.sql",
		"/d/shop.t.100000.sql",
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Fatalf("chunks = %v; want %v", chunks, want)
		}
	}
}
