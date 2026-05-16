//go:build jsonbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Result is one (corpus × library) cell of the benchmark matrix.
//
// EncodeWall / DecodeWall are the MEDIAN single-full-pass wall times
// over benchIters warm iterations (one discarded warm-up pass per phase
// precedes the timed set so allocator / code-cache cold-start doesn't
// skew the first sample). A "pass" encodes (or decodes) every record in
// the corpus once, record-by-record, exactly as the JSON-Lines chunk
// path does. Median (not mean) is reported because JSON marshal timing
// is right-skewed under GC pressure; median is the stable
// decision-grade number.
//
// AllocsPerOp / BytesPerOp are per-RECORD (one marshal or one unmarshal
// = one op), measured over a dedicated GC-bracketed pass separate from
// the timed loop so steady-state churn doesn't conflate into them.
// HeapDelta is the heap-in-use change across a single decode pass — a
// coarse transient-working-set proxy, weighted to decode because
// restore is the DR-critical axis.
type Result struct {
	Corpus     string
	Lib        string
	Surface    string
	Records    int
	JSONBytes  int // total encoded bytes for the corpus (one pass)
	EncodeWall time.Duration
	DecodeWall time.Duration

	EncAllocsPerOp float64
	EncBytesPerOp  float64
	DecAllocsPerOp float64
	DecBytesPerOp  float64
	DecodeHeapDiff int64

	EncodeMBperS float64
	DecodeMBperS float64

	// Fidelity is the gate verdict carried into the report so the
	// markdown table can mark a DISQUALIFIED candidate inline. Empty
	// string means "not yet evaluated"; "PASS" or a failure reason
	// string otherwise. RunAll fills this from runFidelity so the
	// emitted table never shows a speed number for a lossy library
	// without the disqualification next to it.
	Fidelity string
}

// benchIters is the warm, timed iteration count per phase; the reported
// wall time is the median. One discarded warm-up precedes the set.
// Small (corpora are large at decision scale) but >1 so decode — the
// axis this harness exists to measure — isn't single-shot.
const benchIters = 5

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

// encodePass marshals every record once, returning the wall time and
// the per-line encoded bytes (kept for the decode phase + size stat).
func encodePass(records []map[string]any, lib Lib) (dur time.Duration, encoded [][]byte, totalBytes int, err error) {
	encoded = make([][]byte, len(records))
	start := time.Now()
	for i := range records {
		b, merr := lib.Marshal(records[i])
		if merr != nil {
			return 0, nil, 0, fmt.Errorf("marshal record %d: %w", i, merr)
		}
		// Copy: some libs return a buffer they may reuse across calls.
		cp := make([]byte, len(b))
		copy(cp, b)
		encoded[i] = cp
		totalBytes += len(cp)
	}
	return time.Since(start), encoded, totalBytes, nil
}

// decodePass decodes every encoded line through sluice's REAL two-hop
// decode path (decodeLine: map[string]RawMessage probe + typed
// sub-unmarshal of each tagged payload), using the candidate library's
// own Unmarshal at every hop. This is the production-faithful decode —
// the same work `chunkReader.ReadRow` / `decodeValue` performs — so the
// throughput number reflects what restore actually pays, not a
// decode-into-`any` shortcut that would mis-measure (and would silently
// lose int64 precision, which sluice never does). Decode is the
// DR-critical axis this harness exists to weigh.
func decodePass(encoded [][]byte, lib Lib) (time.Duration, error) {
	start := time.Now()
	for i := range encoded {
		if _, err := decodeLine(encoded[i], lib); err != nil {
			return 0, fmt.Errorf("decode record %d: %w", i, err)
		}
	}
	return time.Since(start), nil
}

// benchOnePair benchmarks one (corpus, lib) pair: warm-up (discarded) +
// benchIters timed passes per phase (median reported), then a dedicated
// GC-bracketed pass for allocs/op + B/op + decode heap delta.
func benchOnePair(corpus Corpus, lib Lib) (Result, error) {
	// --- Encode: warm-up + timed median. ---
	if _, _, _, err := encodePass(corpus.Records, lib); err != nil {
		return Result{}, fmt.Errorf("encode warm-up: %w", err)
	}
	encDurs := make([]time.Duration, benchIters)
	var encoded [][]byte
	var totalBytes int
	for i := 0; i < benchIters; i++ {
		d, enc, tot, err := encodePass(corpus.Records, lib)
		if err != nil {
			return Result{}, err
		}
		encDurs[i] = d
		encoded = enc
		totalBytes = tot
	}
	encodeWall := medianDuration(encDurs)

	// --- Decode: warm-up + timed median. ---
	if _, err := decodePass(encoded, lib); err != nil {
		return Result{}, fmt.Errorf("decode warm-up: %w", err)
	}
	decDurs := make([]time.Duration, benchIters)
	for i := 0; i < benchIters; i++ {
		d, err := decodePass(encoded, lib)
		if err != nil {
			return Result{}, err
		}
		decDurs[i] = d
	}
	decodeWall := medianDuration(decDurs)

	n := len(corpus.Records)

	// --- Allocs/op + B/op, GC-bracketed, dedicated passes. ---
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	if _, _, _, err := encodePass(corpus.Records, lib); err != nil {
		return Result{}, fmt.Errorf("encode alloc pass: %w", err)
	}
	runtime.ReadMemStats(&m1)
	encAllocs := float64(m1.Mallocs-m0.Mallocs) / float64(n)
	encBytes := float64(m1.TotalAlloc-m0.TotalAlloc) / float64(n)

	var d0, d1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&d0)
	if _, err := decodePass(encoded, lib); err != nil {
		return Result{}, fmt.Errorf("decode alloc pass: %w", err)
	}
	runtime.ReadMemStats(&d1)
	decAllocs := float64(d1.Mallocs-d0.Mallocs) / float64(n)
	decBytes := float64(d1.TotalAlloc-d0.TotalAlloc) / float64(n)
	decHeapDiff := int64(d1.HeapInuse) - int64(d0.HeapInuse)

	encMBs := float64(totalBytes) / encodeWall.Seconds() / (1 << 20)
	decMBs := float64(totalBytes) / decodeWall.Seconds() / (1 << 20)

	return Result{
		Corpus:         corpus.Name,
		Lib:            lib.Name,
		Surface:        lib.Surface,
		Records:        n,
		JSONBytes:      totalBytes,
		EncodeWall:     encodeWall,
		DecodeWall:     decodeWall,
		EncAllocsPerOp: encAllocs,
		EncBytesPerOp:  encBytes,
		DecAllocsPerOp: decAllocs,
		DecBytesPerOp:  decBytes,
		DecodeHeapDiff: decHeapDiff,
		EncodeMBperS:   encMBs,
		DecodeMBperS:   decMBs,
	}, nil
}

// RunAll runs the fidelity gate first (so a lossy library is recorded
// as DISQUALIFIED before any speed number is attributed to it), then
// benchmarks every (corpus, lib) pair. A fidelity failure does NOT
// abort the run — the report must show the disqualification alongside
// whatever speed the library manages, framed as evidence.
func RunAll(rows int) ([]Result, error) {
	if rows <= 0 {
		rows = CorpusRowCount
	}
	fid := runFidelity()

	// Generate + benchmark ONE corpus at a time, releasing it before
	// the next. At the decision-grade 1M-row scale, materialising all
	// corpora up front peaks at tens of GB of resident records (the
	// first 1M-row attempt did so and blew the test timeout). Bounding
	// the working set to a single corpus keeps the decision-grade run
	// survivable without changing any measured number.
	var out []Result
	for _, g := range corpusGens {
		seed := [32]byte{byte(len(g.name))}
		rng := newCorpusRNG(seed)
		c := Corpus{Name: g.name, Records: g.fn(rows, rng)}
		for _, l := range allLibs {
			r, err := benchOnePair(c, l)
			if err != nil {
				return nil, fmt.Errorf("bench %s/%s: %w", c.Name, l.Name, err)
			}
			if v, ok := fid[l.Name]; ok {
				r.Fidelity = v
			} else {
				r.Fidelity = "PASS"
			}
			out = append(out, r)
		}
		c.Records = nil // release before generating the next corpus
		runtime.GC()
	}
	return out, nil
}

// FormatMarkdown writes the decision-grade tables: a fidelity-gate
// summary first (the load-bearing section — speed is secondary), then
// per-corpus encode/decode tables. Decode columns are placed before
// encode so the eye lands on the DR-critical axis first.
func FormatMarkdown(w io.Writer, results []Result) error {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Corpus != results[j].Corpus {
			return results[i].Corpus < results[j].Corpus
		}
		return results[i].Lib < results[j].Lib
	})

	var sb strings.Builder
	sb.WriteString("# JSON encode/decode benchmark — sluice backup chunk path\n\n")
	fmt.Fprintf(&sb, "_Generated: %s_  \n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&sb, "_Go: %s, GOMAXPROCS=%d, GOOS=%s/%s_  \n",
		runtime.Version(), runtime.GOMAXPROCS(0), runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(&sb, "_Rows per corpus: %d_  \n", rowsForReport(results))
	fmt.Fprintf(&sb, "_Timing: median of %d warm passes per phase (1 discarded warm-up); a pass = every record marshalled/unmarshalled once, as JSON-Lines does. Decode = restore-time / DR axis._  \n\n", benchIters)

	// --- Fidelity gate summary. ---
	sb.WriteString("## Fidelity gate (correctness — load-bearing)\n\n")
	sb.WriteString("Each library round-trips the sluice tagged-value envelope; a lossy or semantically divergent result is DISQUALIFIED regardless of speed.\n\n")
	sb.WriteString("| Library | Surface | HTML-escapes (matches stdlib?) | Fidelity |\n")
	sb.WriteString("|---|---|---|---|\n")
	seen := map[string]bool{}
	for _, l := range allLibs {
		if seen[l.Name] {
			continue
		}
		seen[l.Name] = true
		esc := "no — format-divergent bytes"
		if l.HTMLEscapes {
			esc = "yes"
		}
		verdict := "n/a"
		for _, r := range results {
			if r.Lib == l.Name {
				verdict = r.Fidelity
				break
			}
		}
		fmt.Fprintf(&sb, "| %s | %s | %s | %s |\n", l.Name, l.Surface, esc, verdict)
	}
	sb.WriteString("\n")

	// --- Per-corpus speed tables. ---
	corpora := []string{}
	cs := map[string]bool{}
	for _, r := range results {
		if !cs[r.Corpus] {
			cs[r.Corpus] = true
			corpora = append(corpora, r.Corpus)
		}
	}
	sort.Strings(corpora)

	sb.WriteString("## Throughput (decode-first — DR-critical axis)\n\n")
	for _, corpus := range corpora {
		fmt.Fprintf(&sb, "### %s\n\n", corpus)
		sb.WriteString("| Library | Decode MB/s | Decode allocs/op | Decode B/op | Decode heap Δ (KiB) | Encode MB/s | Encode allocs/op | Encode B/op | JSON (MiB) | Fidelity |\n")
		sb.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
		for _, r := range results {
			if r.Corpus != corpus {
				continue
			}
			fmt.Fprintf(&sb, "| %s | %.1f | %.1f | %.0f | %+d | %.1f | %.1f | %.0f | %.2f | %s |\n",
				r.Lib,
				r.DecodeMBperS, r.DecAllocsPerOp, r.DecBytesPerOp, r.DecodeHeapDiff/1024,
				r.EncodeMBperS, r.EncAllocsPerOp, r.EncBytesPerOp,
				float64(r.JSONBytes)/(1<<20),
				r.Fidelity)
		}
		sb.WriteString("\n")
	}

	if _, err := io.WriteString(w, sb.String()); err != nil {
		return err
	}
	return nil
}

func rowsForReport(results []Result) int {
	if v := os.Getenv("SLUICE_JSONBENCH_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	if len(results) > 0 {
		return results[0].Records
	}
	return CorpusRowCount
}
