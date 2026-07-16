// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// These tests pin audit 2026-07-15 CRITICAL-1: a statement whose keyword
// lexes empty is skipped ONLY when it is pure comment/whitespace. A severed
// INSERT tail, digit-leading garbage, a mid-file BOM, or an unterminated
// versioned comment refuses loudly naming the file — on BOTH the ReadRows
// path and the verify --depth count path (which rides the same processChunk
// switch, so before the fix it CONFIRMED the silent loss instead of catching
// it). A leading UTF-8 BOM is stripped losslessly with a WARN (the flatfile
// engines' posture) so the first INSERT is read, not dropped.
func TestChunkKeywordlessFragments(t *testing.T) {
	const (
		insertA = "INSERT INTO `t` VALUES (1),(2);\n"
		insertB = "INSERT INTO `t` VALUES (3);\n"
	)
	cases := []struct {
		name     string
		chunk    string
		wantRows int64  // asserted when wantErr == ""
		wantErr  string // both paths must refuse containing this
	}{
		{"leading-bom-stripped", "\xef\xbb\xbf" + insertA + insertB, 3, ""},
		{"severed-insert-fragment", insertA + "(4),(5);\n" + insertB, 0, "does not begin with a SQL keyword"},
		{"digit-leading-garbage", insertA + "40101 SET NAMES binary;\n" + insertB, 0, "does not begin with a SQL keyword"},
		{"mid-file-bom", insertA + "\xef\xbb\xbf" + insertB, 0, "does not begin with a SQL keyword"},
		{"unterminated-versioned-comment", insertA + "/*!40101 SET NAMES binary\n", 0, "does not begin with a SQL keyword"},
		{"block-comment-fragment-skipped", "/* header */;\n" + insertA + "/* trailer */", 2, ""},
		{"line-comment-fragment-skipped", "-- header\n;\n" + insertA + "# trailer\n", 2, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeDumpFile(t, dir, "metadata", traditionalMetadata)
			writeDumpFile(t, dir, "shop.t-schema.sql", "CREATE TABLE `t` (`id` bigint NOT NULL);")
			writeDumpFile(t, dir, "shop.t.00000.sql", tc.chunk)
			table := corpusTableNamed(t, dir)

			// ReadRows (migrate) path.
			rr, err := Engine{}.OpenRowReader(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			ch, err := rr.ReadRows(context.Background(), table)
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			var rows int64
			for range ch {
				rows++
			}
			readErr := rr.Err()

			// verify --depth count path.
			sr, err := Engine{}.OpenSchemaReader(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			count, countErr := sr.(ir.Verifier).ExactRowCount(context.Background(), table)

			if tc.wantErr == "" {
				if readErr != nil {
					t.Fatalf("ReadRows Err = %v; want clean read", readErr)
				}
				if rows != tc.wantRows {
					t.Fatalf("rows = %d; want %d", rows, tc.wantRows)
				}
				if countErr != nil {
					t.Fatalf("ExactRowCount err = %v; want clean count", countErr)
				}
				if count != tc.wantRows {
					t.Fatalf("count = %d; want %d", count, tc.wantRows)
				}
				return
			}
			for what, err := range map[string]error{"ReadRows Err": readErr, "ExactRowCount err": countErr} {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("%s = %v; want it to contain %q", what, err, tc.wantErr)
				}
				if !strings.Contains(err.Error(), "shop.t.00000.sql") {
					t.Fatalf("%s = %v; want it to name the chunk file", what, err)
				}
			}
		})
	}
}

// TestSchemaFileKeywordlessFragments pins the same class on the schema-file
// side: a leading BOM is stripped (the CREATE TABLE parses), while a severed
// non-SQL fragment refuses naming the file.
func TestSchemaFileKeywordlessFragments(t *testing.T) {
	t.Run("leading BOM stripped", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpFile(t, dir, "metadata", traditionalMetadata)
		writeDumpFile(t, dir, "shop.t-schema.sql", "\xef\xbb\xbfCREATE TABLE `t` (`id` bigint NOT NULL);")
		table := corpusTableNamed(t, dir)
		if table.Name != "t" || len(table.Columns) != 1 {
			t.Fatalf("table = %+v; want t with one column", table)
		}
	})
	t.Run("severed fragment refuses", func(t *testing.T) {
		dir := t.TempDir()
		writeDumpFile(t, dir, "metadata", traditionalMetadata)
		writeDumpFile(t, dir, "shop.t-schema.sql", "CREATE TABLE `t` (`id` bigint NOT NULL);\n(1);\n")
		sr, err := Engine{}.OpenSchemaReader(context.Background(), dir)
		if err != nil {
			t.Fatal(err)
		}
		_, err = sr.ReadSchema(context.Background())
		if err == nil || !strings.Contains(err.Error(), "does not begin with a SQL keyword") ||
			!strings.Contains(err.Error(), "shop.t-schema.sql") {
			t.Fatalf("ReadSchema err = %v; want a keyword-less fragment refusal naming the file", err)
		}
	})
}
