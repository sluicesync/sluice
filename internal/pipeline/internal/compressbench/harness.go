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
//
// EncodeWall / DecodeWall are the *median* single-pass wall times over
// benchIters warm iterations (a discarded warm-up pass precedes the
// timed set so JIT/allocator cold-start doesn't skew the first sample).
// Median (not mean) is reported because compression timing has a
// right-skewed tail under GC pressure; the median is the stable
// decision-grade number the doc renders.
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

// benchIters is the number of warm, timed iterations benchOnePair runs
// for each of the encode and decode phases; the reported wall time is
// the median of these. One additional warm-up pass per phase precedes
// the timed set and is discarded. Kept small (the corpora are large at
// production scale) but >1 so the decode number — the DR-relevant axis
// this harness exists to measure — isn't a single-shot reading.
const benchIters = 5

// medianDuration returns the median of ds. ds is sorted in place; the
// caller passes a scratch slice it doesn't need preserved.
func medianDuration(ds []time.Duration) time.Duration {
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	n := len(ds)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return ds[n/2]
	}
	return (ds[n/2-1] + ds[n/2]) / 2
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

// encodeOnce runs a single compress pass of src through algo, returning
// the wall time and the compressed bytes. Factored out so the warm
// multi-iteration loop and the heap-snapshot pass share one code path.
func encodeOnce(src []byte, algo Algo) (time.Duration, []byte, error) {
	var encoded bytes.Buffer
	encoded.Grow(len(src) / 4) // conservative — most algos compress 4-10x
	start := time.Now()
	w, err := algo.NewWriter(&encoded)
	if err != nil {
		return 0, nil, fmt.Errorf("new writer: %w", err)
	}
	if _, err := w.Write(src); err != nil {
		_ = w.Close()
		return 0, nil, fmt.Errorf("encode write: %w", err)
	}
	if err := w.Close(); err != nil {
		return 0, nil, fmt.Errorf("encode close: %w", err)
	}
	return time.Since(start), encoded.Bytes(), nil
}

// decodeOnce runs a single decompress pass of enc through algo,
// returning the wall time and the decompressed bytes.
func decodeOnce(enc []byte, algo Algo) (time.Duration, []byte, error) {
	start := time.Now()
	r, err := algo.NewReader(bytes.NewReader(enc))
	if err != nil {
		return 0, nil, fmt.Errorf("new reader: %w", err)
	}
	decoded, err := io.ReadAll(r)
	if err != nil {
		_ = r.Close()
		return 0, nil, fmt.Errorf("decode read: %w", err)
	}
	if err := r.Close(); err != nil {
		return 0, nil, fmt.Errorf("decode close: %w", err)
	}
	return time.Since(start), decoded, nil
}

// benchOnePair benchmarks one (corpus, algo) pair: a discarded warm-up
// pass then benchIters timed passes per phase, reporting the median
// wall time of each. Heap delta is captured around a single
// post-warm-up encode (separate from the timed loop) so the working-
// set proxy stays comparable to the original single-pass methodology.
// Go's testing.B harness in compressbench_test.go remains available for
// allocator-stable bench numbers; this path is the one the markdown
// decision doc renders.
func benchOnePair(corpus Corpus, algo Algo) (Result, error) {
	// --- Encode: warm-up (discarded) + timed median. ---
	if _, _, err := encodeOnce(corpus.Data, algo); err != nil {
		return Result{}, fmt.Errorf("encode warm-up: %w", err)
	}
	encDurs := make([]time.Duration, benchIters)
	var encoded []byte
	for i := 0; i < benchIters; i++ {
		d, enc, err := encodeOnce(corpus.Data, algo)
		if err != nil {
			return Result{}, err
		}
		encDurs[i] = d
		encoded = enc // keep the last encoded buffer for the decode phase
	}
	encodeWall := medianDuration(encDurs)

	// Heap delta around a dedicated encode pass, GC-bracketed — keeps
	// the transient-working-set proxy comparable with the harness's
	// original single-pass reading (the timed median loop above would
	// conflate steady-state allocator churn into the delta).
	var beforeStats, afterEncodeStats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&beforeStats)
	if _, _, err := encodeOnce(corpus.Data, algo); err != nil {
		return Result{}, fmt.Errorf("encode heap pass: %w", err)
	}
	runtime.ReadMemStats(&afterEncodeStats)

	// --- Decode: warm-up (discarded) + timed median. ---
	if _, _, err := decodeOnce(encoded, algo); err != nil {
		return Result{}, fmt.Errorf("decode warm-up: %w", err)
	}
	decDurs := make([]time.Duration, benchIters)
	var decoded []byte
	for i := 0; i < benchIters; i++ {
		d, dec, err := decodeOnce(encoded, algo)
		if err != nil {
			return Result{}, err
		}
		decDurs[i] = d
		decoded = dec
	}
	decodeWall := medianDuration(decDurs)

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
		OutputBytes:  len(encoded),
		EncodeWall:   encodeWall,
		DecodeWall:   decodeWall,
		HeapDelta:    int64(afterEncodeStats.HeapInuse) - int64(beforeStats.HeapInuse),
		Ratio:        float64(corpus.Bytes()) / float64(len(encoded)),
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
	sb.WriteString("_Rows per corpus: " + strconv.Itoa(rowsForReport(results)) + "_  \n")
	fmt.Fprintf(&sb, "_Timing: median of %d warm iterations per phase (1 discarded warm-up); decode = restore-time / DR axis_  \n\n", benchIters)
	sb.WriteString("## Results\n\n")
	sb.WriteString("| Corpus | Algorithm | Input (MiB) | Output (MiB) | Ratio | Encode (MB/s) | Decode (MB/s) | Heap Δ (KiB) |\n")
	sb.WriteString("|---|---|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range results {
		fmt.Fprintf(
			&sb, "| %s | %s | %.2f | %.2f | %.2fx | %.1f | %.1f | %+d |\n",
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
