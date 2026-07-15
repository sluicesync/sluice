// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// buildFamilyDump writes a complete single-table dump directory whose data
// chunk carries every value family in BOTH binary shapes (hex-blob and
// backslash-escaped) — the on-disk end-to-end half of the ADR-0161 pin
// matrix. compression is "", ".gz", or ".zst" and applies to the schema
// file and the data chunks alike.
func buildFamilyDump(t *testing.T, compression string) string {
	t.Helper()
	dir := t.TempDir()
	writeDumpFile(t, dir, "metadata", traditionalMetadata)

	schema := "/*!40101 SET NAMES binary*/;\n" +
		"CREATE TABLE `corpus` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `u64` bigint unsigned DEFAULT NULL,\n" +
		"  `flag` tinyint(1) DEFAULT NULL,\n" +
		"  `price` decimal(20,4) DEFAULT NULL,\n" +
		"  `ratio` double DEFAULT NULL,\n" +
		"  `name` varchar(64) DEFAULT NULL,\n" +
		"  `bin` varbinary(32) DEFAULT NULL,\n" +
		"  `at` datetime(6) DEFAULT NULL,\n" +
		"  `doc` json DEFAULT NULL,\n" +
		"  `role` enum('a','b') DEFAULT NULL,\n" +
		"  `tags` set('x','y') DEFAULT NULL,\n" +
		"  `mask` bit(5) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
	writeDumpFile(t, dir, "shop.corpus-schema.sql"+compression, schema)

	// Chunk 0: vanilla-mydumper shape — SET NAMES header, hex-blob binary,
	// bare-VALUES extended INSERT.
	chunk0 := "/*!40101 SET NAMES binary*/;\n" +
		"INSERT INTO `corpus` VALUES\n" +
		"(1,18446744073709551615,1,'12345.6789',2.5,'it''s a \\'name\\'',0x00FF1A,'2026-01-02 03:04:05.123456','{\"k\": [1]}','a','x,y',b'10101'),\n" +
		"(2,NULL,0,NULL,NULL,'NULL',NULL,NULL,NULL,NULL,'',NULL);\n"
	writeDumpFile(t, dir, "shop.corpus.00000.sql"+compression, chunk0)

	// Chunk 1: pscale shape — column list, backslash-escaped binary
	// (NO hex-blob: binary fidelity rides on the escape decoder).
	chunk1 := "INSERT INTO `corpus` (`id`,`u64`,`flag`,`price`,`ratio`,`name`,`bin`,`at`,`doc`,`role`,`tags`,`mask`) VALUES " +
		"(3,9007199254740993,1,'-0.0001',-1.25,'line\\nbreak','\\0\\Z\\\\\\'','1999-12-31 23:59:59','[]','b','y',b'1');\n"
	writeDumpFile(t, dir, "shop.corpus.00001.sql"+compression, chunk1)
	return dir
}

// corpusTable reads the dump's schema so the reader test consumes the same
// IR the pipeline would.
func corpusTable(t *testing.T, dir string) *ir.Table {
	t.Helper()
	sr, err := Engine{}.OpenSchemaReader(context.Background(), dir)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	schema, err := sr.ReadSchema(context.Background())
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(schema.Tables) != 1 {
		t.Fatalf("tables = %d; want 1", len(schema.Tables))
	}
	return schema.Tables[0]
}

func readAllRows(t *testing.T, dir string, table *ir.Table) []ir.Row {
	t.Helper()
	rr, err := Engine{}.OpenRowReader(context.Background(), dir)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	ch, err := rr.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var rows []ir.Row
	for row := range ch {
		rows = append(rows, row)
	}
	if err := rr.Err(); err != nil {
		t.Fatalf("reader Err: %v", err)
	}
	return rows
}

// TestReadRows_FamilyMatrixEndToEnd drives the whole engine surface —
// layout detect, schema parse, chunk streaming, tuple lex, value decode —
// over the family corpus, uncompressed and in both compressed forms.
func TestReadRows_FamilyMatrixEndToEnd(t *testing.T) {
	for _, compression := range []string{"", ".gz", ".zst"} {
		name := compression
		if name == "" {
			name = "plain"
		}
		t.Run(name, func(t *testing.T) {
			dir := buildFamilyDump(t, compression)
			table := corpusTable(t, dir)
			rows := readAllRows(t, dir, table)
			if len(rows) != 3 {
				t.Fatalf("rows = %d; want 3", len(rows))
			}

			want0 := ir.Row{
				"id":    int64(1),
				"u64":   uint64(18446744073709551615),
				"flag":  true,
				"price": "12345.6789",
				"ratio": 2.5,
				"name":  "it's a 'name'",
				"bin":   []byte{0x00, 0xFF, 0x1A},
				"at":    time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC),
				"doc":   []byte(`{"k": [1]}`),
				"role":  "a",
				"tags":  []string{"x", "y"},
				"mask":  "10101",
			}
			if !reflect.DeepEqual(rows[0], want0) {
				t.Fatalf("row0 =\n%#v\nwant\n%#v", rows[0], want0)
			}

			// Row 1: NULL vs the string 'NULL' vs the empty SET.
			if rows[1]["u64"] != nil || rows[1]["price"] != nil || rows[1]["mask"] != nil {
				t.Fatalf("row1 NULLs = %#v", rows[1])
			}
			if rows[1]["name"] != "NULL" {
				t.Fatalf("row1 name = %#v; the string 'NULL' is data", rows[1]["name"])
			}
			if !reflect.DeepEqual(rows[1]["tags"], []string{}) {
				t.Fatalf("row1 tags = %#v; want the empty set", rows[1]["tags"])
			}

			// Row 2: the pscale shape — column list + escape-decoded binary
			// + int64 > 2^53 exactness.
			if rows[2]["u64"] != int64(9007199254740993) {
				t.Fatalf("row2 u64 = %#v", rows[2]["u64"])
			}
			if !reflect.DeepEqual(rows[2]["bin"], []byte{0x00, 0x1A, '\\', '\''}) {
				t.Fatalf("row2 bin = %#v", rows[2]["bin"])
			}
			if rows[2]["name"] != "line\nbreak" {
				t.Fatalf("row2 name = %#v", rows[2]["name"])
			}
			if rows[2]["mask"] != "00001" {
				t.Fatalf("row2 mask = %#v", rows[2]["mask"])
			}
		})
	}
}

func TestExactRowCount_ChunkRescan(t *testing.T) {
	dir := buildFamilyDump(t, "")
	table := corpusTable(t, dir)
	sr, err := Engine{}.OpenSchemaReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	verifier, ok := sr.(ir.Verifier)
	if !ok {
		t.Fatal("SchemaReader must implement ir.Verifier for count-depth verify")
	}
	n, err := verifier.ExactRowCount(context.Background(), table)
	if err != nil {
		t.Fatalf("ExactRowCount: %v", err)
	}
	if n != 3 {
		t.Fatalf("count = %d; want 3", n)
	}
}

// TestReadRows_ChunkRefusals pins the loud-failure side of the data path:
// a stray statement kind, a foreign charset header, a table-name mismatch,
// and a zero-date value each abort the stream with a named error via Err.
func TestReadRows_ChunkRefusals(t *testing.T) {
	cases := []struct {
		name    string
		chunk   string
		wantErr string
	}{
		{"foreign-charset", "SET NAMES latin1;\nINSERT INTO `t` VALUES (1);", "SET NAMES latin1"},
		{"stray-statement", "DROP TABLE `t`;\nINSERT INTO `t` VALUES (1);", "unexpected DROP"},
		{"wrong-table", "INSERT INTO `other` VALUES (1);", "INSERT into other"},
		{"arity", "INSERT INTO `t` VALUES (1,2);", "expects 1"},
		{"garbage-value", "INSERT INTO `t` VALUES (CURRENT_TIMESTAMP);", "unsupported value literal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "shop.t-schema.sql", "CREATE TABLE `t` (`id` bigint NOT NULL);")
			writeDumpFile(t, dir, "shop.t.00000.sql", tc.chunk)

			table := corpusTableNamed(t, dir)
			rr, err := Engine{}.OpenRowReader(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			ch, err := rr.ReadRows(context.Background(), table)
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			for range ch { //nolint:revive // drain to completion
			}
			if err := rr.Err(); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Err = %v; want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestReadRows_ZeroDateRefusesLoudly pins the temporal loud-failure: a
// relaxed-sql_mode zero date in a dump refuses (naming the value) instead
// of silently normalizing (ADR-0161 §7 — no --zero-date plumbing yet).
func TestReadRows_ZeroDateRefusesLoudly(t *testing.T) {
	dir := t.TempDir()
	writeDumpFile(t, dir, "metadata", traditionalMetadata)
	writeDumpFile(t, dir, "shop.t-schema.sql", "CREATE TABLE `t` (`d` date DEFAULT NULL);")
	writeDumpFile(t, dir, "shop.t.00000.sql", "INSERT INTO `t` VALUES ('0000-00-00');")

	table := corpusTableNamed(t, dir)
	rr, err := Engine{}.OpenRowReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := rr.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	for range ch { //nolint:revive // drain to completion
	}
	if err := rr.Err(); err == nil || !strings.Contains(err.Error(), "0000-00-00") {
		t.Fatalf("Err = %v; want a zero-date refusal naming the value", err)
	}
}

// corpusTableNamed reads the single table of a one-table fixture dir.
func corpusTableNamed(t *testing.T, dir string) *ir.Table {
	t.Helper()
	return corpusTable(t, dir)
}

func TestReadRows_EmptyTable(t *testing.T) {
	dir := t.TempDir()
	writeDumpFile(t, dir, "metadata", traditionalMetadata)
	writeDumpFile(t, dir, "shop.t-schema.sql", "CREATE TABLE `t` (`id` bigint NOT NULL);")
	table := corpusTableNamed(t, dir)
	rows := readAllRows(t, dir, table)
	if len(rows) != 0 {
		t.Fatalf("rows = %d; want 0 (no chunk files = empty table)", len(rows))
	}
}

func TestReadRows_ContextCancel(t *testing.T) {
	dir := buildFamilyDump(t, "")
	table := corpusTable(t, dir)
	rr, err := Engine{}.OpenRowReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	for range ch { //nolint:revive // drain to completion
	}
	// A pure ctx cancel is not a sticky decode error.
	if err := rr.Err(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Err = %v", err)
	}
}

func TestEngine_SourceOnlySurfaces(t *testing.T) {
	e := Engine{}
	ctx := context.Background()
	if _, err := e.OpenSchemaWriter(ctx, "x"); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("OpenSchemaWriter err = %v", err)
	}
	if _, err := e.OpenRowWriter(ctx, "x"); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("OpenRowWriter err = %v", err)
	}
	if _, err := e.OpenCDCReader(ctx, "x"); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("OpenCDCReader err = %v", err)
	}
	if _, err := e.OpenChangeApplier(ctx, "x"); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("OpenChangeApplier err = %v", err)
	}
	if _, err := e.OpenSnapshotStream(ctx, "x"); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("OpenSnapshotStream err = %v", err)
	}
	caps := e.Capabilities()
	if caps.CDC != ir.CDCNone || caps.BulkLoad != ir.BulkLoadNone || !caps.UnsignedIntegers {
		t.Fatalf("capabilities = %+v", caps)
	}
}
