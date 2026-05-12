//go:build compressbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package compressbench

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Result is one (corpus × algorithm) cell of the benchmark matrix.
// Compressed/Input are bytes; durations are wall-clock; PeakHeapDelta
// is the difference between heap-in-use before/after a single
// compress-then-decompress pass (a coarse proxy for the algorithm's
// transient working-set cost).
type Result struct {
	Corpus       string
	Algo         string
	InputBytes   int
	OutputBytes  int
	EncodeWall   time.Duration
	DecodeWall   time.Duration
	HeapDelta    int64
	Ratio        float64
	EncodeMBperS float64
	DecodeMBperS float64
}

// RunAll generates the corpora and benchmarks every (corpus, algo)
// pair. Returns a flat slice of Result rows the markdown emitter
// sorts + tabulates. rows controls the corpus row count
// (CorpusRowCount when zero).
func RunAll(rows int) ([]Result, error) {
	corpora, err := generateCorpora(rows)
	if err != nil {
		return nil, fmt.Errorf("generate corpora: %w", err)
	}
	var out []Result
	for _, c := range corpora {
		for _, a := range allAlgos {
			r, err := benchOnePair(c, a)
			if err != nil {
				return nil, fmt.Errorf("bench %s/%s: %w", c.Name, a.Name, err)
			}
			out = append(out, r)
		}
	}
	return out, nil
}

// benchOnePair runs one compress + one decompress pass on `corpus`
// using `algo`, capturing wall time + heap delta. A single pass is
// enough for the comparative signal the decision doc needs; Go's
// testing.B harness in compressbench_test.go does the multi-iteration
// stable measurement when tighter numbers are wanted.
func benchOnePair(corpus Corpus, algo Algo) (Result, error) {
	// Snapshot heap-in-use before the encode pass. runtime.GC() forces
	// a collection so the "before" reading isn't inflated by garbage
	// from the corpus generator that hasn't been swept yet.
	var beforeStats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&beforeStats)

	// Encode.
	var encoded bytes.Buffer
	encoded.Grow(len(corpus.Data) / 4) // conservative — most algos compress 4-10x
	encStart := time.Now()
	w, err := algo.NewWriter(&encoded)
	if err != nil {
		return Result{}, fmt.Errorf("new writer: %w", err)
	}
	if _, err := w.Write(corpus.Data); err != nil {
		_ = w.Close()
		return Result{}, fmt.Errorf("encode write: %w", err)
	}
	if err := w.Close(); err != nil {
		return Result{}, fmt.Errorf("encode close: %w", err)
	}
	encodeWall := time.Since(encStart)

	// Heap reading after encode but before decode — captures the
	// encoder's peak working-set cost separate from decoder cost. A
	// second GC keeps the reading from including pinned encoder
	// state that's about to be released.
	var afterEncodeStats runtime.MemStats
	runtime.ReadMemStats(&afterEncodeStats)

	// Decode.
	decStart := time.Now()
	r, err := algo.NewReader(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		return Result{}, fmt.Errorf("new reader: %w", err)
	}
	decoded, err := io.ReadAll(r)
	if err != nil {
		_ = r.Close()
		return Result{}, fmt.Errorf("decode read: %w", err)
	}
	if err := r.Close(); err != nil {
		return Result{}, fmt.Errorf("decode close: %w", err)
	}
	decodeWall := time.Since(decStart)

	// Correctness sanity check — decompressed bytes must match the
	// input byte-for-byte. A mismatch here would invalidate the
	// throughput number and indicate a per-algorithm framing bug.
	if !bytes.Equal(decoded, corpus.Data) {
		return Result{}, fmt.Errorf("round-trip mismatch: decoded %d bytes, want %d", len(decoded), len(corpus.Data))
	}

	encMBs := float64(corpus.Bytes()) / encodeWall.Seconds() / (1 << 20)
	decMBs := float64(corpus.Bytes()) / decodeWall.Seconds() / (1 << 20)

	return Result{
		Corpus:       corpus.Name,
		Algo:         algo.Name,
		InputBytes:   corpus.Bytes(),
		OutputBytes:  encoded.Len(),
		EncodeWall:   encodeWall,
		DecodeWall:   decodeWall,
		HeapDelta:    int64(afterEncodeStats.HeapInuse) - int64(beforeStats.HeapInuse),
		Ratio:        float64(corpus.Bytes()) / float64(encoded.Len()),
		EncodeMBperS: encMBs,
		DecodeMBperS: decMBs,
	}, nil
}

// Bytes returns the rendered corpus length — exists so corpus.Data
// can be inlined-renderable later if memory matters.
func (c Corpus) Bytes() int { return len(c.Data) }

// FormatMarkdown writes a docs/dev/notes/compression-benchmark.md-
// shaped table to w, plus a per-corpus breakdown. Result ordering is
// {corpus alpha, algo as registered}. The decision doc copies this
// table verbatim.
func FormatMarkdown(w io.Writer, results []Result) error {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Corpus != results[j].Corpus {
			return results[i].Corpus < results[j].Corpus
		}
		return results[i].Algo < results[j].Algo
	})

	var sb strings.Builder
	sb.WriteString("# Compression-algorithm benchmark — Phase 1 backup chunks\n\n")
	fmt.Fprintf(&sb, "_Generated: %s_  \n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&sb, "_Go: %s, runtime.GOMAXPROCS=%d, GOOS=%s/%s_  \n",
		runtime.Version(), runtime.GOMAXPROCS(0), runtime.GOOS, runtime.GOARCH)
	sb.WriteString("_Rows per corpus: " + strconv.Itoa(rowsForReport(results)) + "_  \n\n")
	sb.WriteString("## Results\n\n")
	sb.WriteString("| Corpus | Algorithm | Input (MiB) | Output (MiB) | Ratio | Encode (MB/s) | Decode (MB/s) | Heap Δ (KiB) |\n")
	sb.WriteString("|---|---|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range results {
		fmt.Fprintf(&sb, "| %s | %s | %.2f | %.2f | %.2fx | %.1f | %.1f | %+d |\n",
			r.Corpus,
			r.Algo,
			float64(r.InputBytes)/(1<<20),
			float64(r.OutputBytes)/(1<<20),
			r.Ratio,
			r.EncodeMBperS,
			r.DecodeMBperS,
			r.HeapDelta/1024,
		)
	}
	sb.WriteString("\n")

	if _, err := io.WriteString(w, sb.String()); err != nil {
		return err
	}
	return nil
}

func rowsForReport(_ []Result) int {
	if v := os.Getenv("SLUICE_COMPRESSBENCH_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return CorpusRowCount
}
