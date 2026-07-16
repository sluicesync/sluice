// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// buildInsertStatement composes one mydumper-shaped INSERT statement of
// roughly size bytes: many ~1 KiB quoted-string tuples, the shape a
// --statement-size dump emits.
func buildInsertStatement(size int) string {
	var sb strings.Builder
	sb.Grow(size + 2048)
	sb.WriteString("INSERT INTO `t` VALUES ")
	payload := strings.Repeat("x", 1000)
	first := true
	for sb.Len() < size {
		if !first {
			sb.WriteByte(',')
		}
		first = false
		fmt.Fprintf(&sb, "(%d,'%s')", sb.Len(), payload)
	}
	sb.WriteString(";\n")
	return sb.String()
}

// benchmarkStatementStream streams input through the statement splitter
// with the production block size, draining every statement.
func benchmarkStatementStream(b *testing.B, input string) {
	b.Helper()
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream := newStatementStream(strings.NewReader(input), 0)
		for {
			if _, err := stream.Next(); err != nil {
				if !errors.Is(err, io.EOF) {
					b.Fatal(err)
				}
				break
			}
		}
	}
}

// BenchmarkStatementStream_LargeStatement is the audit MED-P1 shape: the
// SAME bytes as one giant statement vs many 1 MiB statements. Before the
// resume-offset fix the single-statement case was quadratic in statement
// size (the carry was re-lexed from byte 0 on every 1 MiB read block).
func BenchmarkStatementStream_LargeStatement(b *testing.B) {
	for _, mib := range []int{16, 64} {
		b.Run(fmt.Sprintf("one_%dMiB_statement", mib), func(b *testing.B) {
			benchmarkStatementStream(b, buildInsertStatement(mib<<20))
		})
		b.Run(fmt.Sprintf("%dx_1MiB_statements", mib), func(b *testing.B) {
			benchmarkStatementStream(b, strings.Repeat(buildInsertStatement(1<<20), mib))
		})
	}
}
