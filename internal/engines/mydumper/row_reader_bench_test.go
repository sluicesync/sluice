// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// buildGiantStatementDump writes a complete single-table dump directory
// whose data chunk is ONE multi-MiB INSERT statement of ~4 KiB
// quoted-string rows — the mydumper/pscale-dump `--statement-size 64M`
// shape Bug 191 was filed on. Returns the dump dir and the chunk size.
func buildGiantStatementDump(tb testing.TB, stmtBytes int) (dir string, chunkLen int) {
	tb.Helper()
	dir = tb.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			tb.Fatal(err)
		}
	}
	write("metadata", traditionalMetadata)
	write("shop.big-schema.sql", "CREATE TABLE `big` (\n"+
		"  `id` bigint NOT NULL,\n"+
		"  `payload` longtext DEFAULT NULL,\n"+
		"  PRIMARY KEY (`id`)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n")

	// ~4 KiB per value; one escaped quote per row keeps the escape-aware
	// decode branch in play alongside the escape-free fast path.
	plain := strings.Repeat("x", 4096)
	escaped := plain[:2048] + `\'` + plain[2048:]
	var sb strings.Builder
	sb.Grow(stmtBytes + 8192)
	sb.WriteString("INSERT INTO `big` VALUES ")
	for id := 0; sb.Len() < stmtBytes; id++ {
		if id > 0 {
			sb.WriteByte(',')
		}
		payload := plain
		if id%2 == 1 {
			payload = escaped
		}
		fmt.Fprintf(&sb, "(%d,'%s')", id, payload)
	}
	sb.WriteString(";\n")
	chunk := sb.String()
	write("shop.big.00000.sql", chunk)
	return dir, len(chunk)
}

// BenchmarkReadRows_GiantSingleStatement is the Bug-191 PIPELINE pin: the
// v0.99.259 splitter fix (MED-P1) was linear at the statement-split layer,
// but every quoted VALUE below it allocated a buffer sized to the remaining
// statement TAIL, so end-to-end ReadRows stayed O(rows × statement_size)
// (~350 s for a 49 MiB, 12k-row statement). This drives the WHOLE reader —
// layout detect, chunk streaming, tuple lex, value decode — so a
// regression anywhere in the stack shows up in wall time, not just in a
// layer microbenchmark.
func BenchmarkReadRows_GiantSingleStatement(b *testing.B) {
	for _, mib := range []int{4, 16} {
		b.Run(fmt.Sprintf("one_%dMiB_statement", mib), func(b *testing.B) {
			dir, chunkLen := buildGiantStatementDump(b, mib<<20)
			table := benchCorpusTable(b, dir)
			b.SetBytes(int64(chunkLen))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchDrainRows(b, dir, table)
			}
		})
	}
}

func benchCorpusTable(b *testing.B, dir string) *ir.Table {
	b.Helper()
	sr, err := Engine{}.OpenSchemaReader(context.Background(), dir)
	if err != nil {
		b.Fatalf("OpenSchemaReader: %v", err)
	}
	schema, err := sr.ReadSchema(context.Background())
	if err != nil {
		b.Fatalf("ReadSchema: %v", err)
	}
	if len(schema.Tables) != 1 {
		b.Fatalf("tables = %d; want 1", len(schema.Tables))
	}
	return schema.Tables[0]
}

func benchDrainRows(b *testing.B, dir string, table *ir.Table) {
	b.Helper()
	rr, err := Engine{}.OpenRowReader(context.Background(), dir)
	if err != nil {
		b.Fatalf("OpenRowReader: %v", err)
	}
	ch, err := rr.ReadRows(context.Background(), table)
	if err != nil {
		b.Fatalf("ReadRows: %v", err)
	}
	var n int
	for range ch {
		n++
	}
	if err := rr.Err(); err != nil {
		b.Fatalf("reader Err: %v", err)
	}
	if n == 0 {
		b.Fatal("no rows streamed")
	}
}
