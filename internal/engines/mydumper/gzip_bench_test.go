// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"bytes"
	stdgzip "compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	kgzip "github.com/klauspost/compress/gzip"
)

// gzipBenchInput builds a compressed dump-shaped chunk (~32 MiB of INSERT
// text with realistic-entropy values, not a repeated filler — decompress
// speed depends on the match/literal mix) once per benchmark run.
func gzipBenchInput(b *testing.B) (compressed []byte, rawLen int64) {
	b.Helper()
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic bench input
	var sb strings.Builder
	size := 32 << 20
	sb.Grow(size + 2048)
	sb.WriteString("INSERT INTO `t` VALUES ")
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 -_@."
	val := make([]byte, 80)
	first := true
	for sb.Len() < size {
		if !first {
			sb.WriteByte(',')
		}
		first = false
		for i := range val {
			val[i] = alphabet[rng.Intn(len(alphabet))]
		}
		fmt.Fprintf(&sb, "(%d,'%s')", rng.Int63(), val)
	}
	sb.WriteString(";\n")
	raw := []byte(sb.String())
	var buf bytes.Buffer
	zw := stdgzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		b.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes(), int64(len(raw))
}

// BenchmarkChunkGzipDecompress compares the two gzip readers on the dump
// chunk decompress path (audit LOW / M3.3: dir.go used stdlib while
// klauspost was already a module dependency via zstd).
func BenchmarkChunkGzipDecompress(b *testing.B) {
	compressed, rawLen := gzipBenchInput(b)
	b.Run("stdlib", func(b *testing.B) {
		b.SetBytes(rawLen)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			zr, err := stdgzip.NewReader(bytes.NewReader(compressed))
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, zr); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("klauspost", func(b *testing.B) {
		b.SetBytes(rawLen)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			zr, err := kgzip.NewReader(bytes.NewReader(compressed))
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, zr); err != nil {
				b.Fatal(err)
			}
		}
	})
}
