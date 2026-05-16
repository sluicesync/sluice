//go:build jsonbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

// The extended, format-agnostic harness: it benchmarks every
// Serializer (JSON baselines + msgpack drop-in + msgpack-native) over
// the SAME logical corpora the JSON harness uses, and adds the axes
// the headline "msgpack vs JSON" comparisons omit:
//
//   - RAW serialized size (the number microbenchmarks usually quote)
//   - POST-ZSTD size — sluice chunks ship compressed (v0.67.0 default
//     zstd at SpeedDefault). klauspost/compress/zstd at SpeedDefault,
//     matching production exactly (codec.go). The thesis under test:
//     msgpack's raw-size win largely COLLAPSES after zstd; the durable
//     wins (if any) are decode speed, allocations, native-int
//     precision safety, and native-binary (no base64).
//   - encode-then-zstd combined throughput (the real write path)
//   - decode MB/s — weighted, the DR-critical restore axis
//   - allocs/op, B/op, decode heap delta
//
// Methodology mirrors harness.go exactly: warm median of benchIters
// passes per phase, one discarded warm-up; allocs measured in a
// dedicated GC-bracketed pass; one corpus materialised at a time so
// the decision-grade scale stays survivable.

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// SResult is one (corpus × serializer) cell of the extended matrix.
type SResult struct {
	Corpus  string
	Name    string
	Surface string
	Model   encModel
	Records int

	RawBytes  int // total serialized bytes, one pass
	ZstdBytes int // total bytes after zstd SpeedDefault (production codec)

	EncodeWall     time.Duration
	DecodeWall     time.Duration
	EncodeZstdWall time.Duration // encode + zstd-compress combined

	EncAllocsPerOp float64
	EncBytesPerOp  float64
	DecAllocsPerOp float64
	DecBytesPerOp  float64
	DecodeHeapDiff int64

	EncodeMBperS     float64
	DecodeMBperS     float64
	EncodeZstdMBperS float64

	Fidelity         string
	HumanInspectable bool
}

// zstdEncoderForBench is a process-wide zstd encoder at SpeedDefault —
// EXACTLY the level codec.go's newCodecWriter uses for the v0.67.0
// default. EncodeAll is used (whole-buffer) because the size question
// is "what lands on disk per chunk", and a chunk is a bounded buffer.
var zstdEncoderForBench, _ = zstd.NewWriter(nil,
	zstd.WithEncoderLevel(zstd.SpeedDefault))

// zstdSizeOf returns len(zstd(buf)) at the production codec level. The
// per-record encoded lines are concatenated (newline-joined, as the
// JSON-Lines chunk body is) before compression so the ratio reflects a
// real chunk, not per-line framing overhead.
func zstdSizeOf(lines [][]byte) (size int, compressed []byte) {
	var raw bytes.Buffer
	for _, l := range lines {
		raw.Write(l)
		raw.WriteByte('\n')
	}
	out := zstdEncoderForBench.EncodeAll(raw.Bytes(), nil)
	return len(out), out
}

// sEncodePass serialises every record once.
func sEncodePass(records []map[string]any, s Serializer) (dur time.Duration, encoded [][]byte, totalBytes int, err error) {
	encoded = make([][]byte, len(records))
	start := time.Now()
	for i := range records {
		b, merr := s.EncodeRecord(records[i])
		if merr != nil {
			return 0, nil, 0, fmt.Errorf("encode record %d: %w", i, merr)
		}
		cp := make([]byte, len(b))
		copy(cp, b)
		encoded[i] = cp
	}
	dur = time.Since(start)
	for _, e := range encoded {
		totalBytes += len(e)
	}
	return dur, encoded, totalBytes, nil
}

func sDecodePass(encoded [][]byte, s Serializer) (time.Duration, error) {
	start := time.Now()
	for i := range encoded {
		if _, err := s.DecodeRecord(encoded[i]); err != nil {
			return 0, fmt.Errorf("decode record %d: %w", i, err)
		}
	}
	return time.Since(start), nil
}

func benchOneSerializer(corpus Corpus, s Serializer) (SResult, error) {
	// Encode: warm-up + timed median.
	if _, _, _, err := sEncodePass(corpus.Records, s); err != nil {
		return SResult{}, fmt.Errorf("encode warm-up: %w", err)
	}
	encDurs := make([]time.Duration, benchIters)
	var encoded [][]byte
	var rawTotal int
	for i := 0; i < benchIters; i++ {
		d, enc, tot, err := sEncodePass(corpus.Records, s)
		if err != nil {
			return SResult{}, err
		}
		encDurs[i] = d
		encoded = enc
		rawTotal = tot
	}
	encodeWall := medianDuration(encDurs)

	// Encode + zstd combined (the real write path: serialise then
	// compress the chunk body).
	ezDurs := make([]time.Duration, benchIters)
	for i := 0; i < benchIters; i++ {
		start := time.Now()
		_, enc, _, err := sEncodePass(corpus.Records, s)
		if err != nil {
			return SResult{}, err
		}
		_, _ = zstdSizeOf(enc)
		ezDurs[i] = time.Since(start)
	}
	encodeZstdWall := medianDuration(ezDurs)

	zstdBytes, _ := zstdSizeOf(encoded)

	// Decode: warm-up + timed median.
	if _, err := sDecodePass(encoded, s); err != nil {
		return SResult{}, fmt.Errorf("decode warm-up: %w", err)
	}
	decDurs := make([]time.Duration, benchIters)
	for i := 0; i < benchIters; i++ {
		d, err := sDecodePass(encoded, s)
		if err != nil {
			return SResult{}, err
		}
		decDurs[i] = d
	}
	decodeWall := medianDuration(decDurs)

	n := len(corpus.Records)

	// Allocs/op + B/op, GC-bracketed dedicated passes.
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	if _, _, _, err := sEncodePass(corpus.Records, s); err != nil {
		return SResult{}, fmt.Errorf("encode alloc pass: %w", err)
	}
	runtime.ReadMemStats(&m1)
	encAllocs := float64(m1.Mallocs-m0.Mallocs) / float64(n)
	encBytes := float64(m1.TotalAlloc-m0.TotalAlloc) / float64(n)

	var d0, d1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&d0)
	if _, err := sDecodePass(encoded, s); err != nil {
		return SResult{}, fmt.Errorf("decode alloc pass: %w", err)
	}
	runtime.ReadMemStats(&d1)
	decAllocs := float64(d1.Mallocs-d0.Mallocs) / float64(n)
	decBytes := float64(d1.TotalAlloc-d0.TotalAlloc) / float64(n)
	decHeapDiff := int64(d1.HeapInuse) - int64(d0.HeapInuse)

	mb := float64(rawTotal) / (1 << 20)
	return SResult{
		Corpus:           corpus.Name,
		Name:             s.Name,
		Surface:          s.Surface,
		Model:            s.Model,
		Records:          n,
		RawBytes:         rawTotal,
		ZstdBytes:        zstdBytes,
		EncodeWall:       encodeWall,
		DecodeWall:       decodeWall,
		EncodeZstdWall:   encodeZstdWall,
		EncAllocsPerOp:   encAllocs,
		EncBytesPerOp:    encBytes,
		DecAllocsPerOp:   decAllocs,
		DecBytesPerOp:    decBytes,
		DecodeHeapDiff:   decHeapDiff,
		EncodeMBperS:     mb / encodeWall.Seconds(),
		DecodeMBperS:     mb / decodeWall.Seconds(),
		EncodeZstdMBperS: mb / encodeZstdWall.Seconds(),
		HumanInspectable: s.HumanInspectable,
	}, nil
}

// RunAllSerializers fidelity-gates every serializer first (so a lossy
// candidate is recorded DISQUALIFIED before any speed number is
// attributed to it), then benchmarks every (corpus, serializer) pair,
// one corpus materialised at a time.
func RunAllSerializers(rows int) ([]SResult, error) {
	if rows <= 0 {
		rows = CorpusRowCount
	}
	sers := allSerializers()
	fid := runSerializerFidelity(sers)

	var out []SResult
	for _, g := range corpusGens {
		seed := [32]byte{byte(len(g.name))}
		rng := newCorpusRNG(seed)
		c := Corpus{Name: g.name, Records: g.fn(rows, rng)}
		for _, s := range sers {
			r, err := benchOneSerializer(c, s)
			if err != nil {
				return nil, fmt.Errorf("bench %s/%s: %w", c.Name, s.Name, err)
			}
			if v, ok := fid[s.Name]; ok {
				r.Fidelity = v
			} else {
				r.Fidelity = "PASS"
			}
			out = append(out, r)
		}
		c.Records = nil
		runtime.GC()
	}
	return out, nil
}

// FormatSerializerMarkdown writes the extended decision-grade report:
// fidelity gate first (load-bearing), then a compression-aware size
// table, then decode-first throughput per corpus.
func FormatSerializerMarkdown(w io.Writer, results []SResult) error {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Corpus != results[j].Corpus {
			return results[i].Corpus < results[j].Corpus
		}
		if results[i].Model != results[j].Model {
			return results[i].Model < results[j].Model
		}
		return results[i].Name < results[j].Name
	})

	var sb strings.Builder
	sb.WriteString("# msgpack vs JSON — sluice backup chunk path (evidence)\n\n")
	fmt.Fprintf(&sb, "_Generated: %s_  \n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&sb, "_Go: %s, GOMAXPROCS=%d, %s/%s_  \n",
		runtime.Version(), runtime.GOMAXPROCS(0), runtime.GOOS, runtime.GOARCH)
	if len(results) > 0 {
		fmt.Fprintf(&sb, "_Rows per corpus: %d_  \n", results[0].Records)
	}
	fmt.Fprintf(&sb, "_Timing: median of %d warm passes/phase (1 discarded warm-up). Decode = restore / DR-critical axis. zstd = klauspost SpeedDefault — the v0.67.0 production default (codec.go)._  \n\n", benchIters)

	// Fidelity gate.
	sb.WriteString("## Fidelity gate (correctness — load-bearing)\n\n")
	sb.WriteString("Round-trips sluice's value contract (docs/value-types.md): int64 incl. 2^53+1, uint64 > MaxInt64, float64, []byte, decimal-as-string, RFC3339Nano time-as-string, bool, SQL NULL→nil, nested map. A lossy/divergent candidate is DISQUALIFIED regardless of speed.\n\n")
	sb.WriteString("| Serializer | Model | Human-inspectable | Fidelity |\n")
	sb.WriteString("|---|---|---|---|\n")
	seen := map[string]bool{}
	for _, r := range results {
		if seen[r.Name] {
			continue
		}
		seen[r.Name] = true
		hi := "no — binary"
		if r.HumanInspectable {
			hi = "yes — `head file | jq .`"
		}
		fmt.Fprintf(&sb, "| %s | %s | %s | %s |\n", r.Name, r.Model, hi, r.Fidelity)
	}
	sb.WriteString("\n")
	sb.WriteString("`PASS` = value contract met, format self-describing. `PASS*` = value contract met bit-exact (int64/uint64/[]byte/bool/nil exact; timestamp survives byte-exact as its RFC3339Nano string — no msgpack-timestamp-ext rewrite), BUT the native model drops the `_t` tag so timestamp/decimal Go-typing requires out-of-band schema (the column IR type) the wire no longer carries. That schema coupling is a format-redesign cost, not a data-loss one — surfaced here so the decision weighs it.\n\n")

	corpora := orderedCorpora(results)

	// Compression-aware size table — the thesis-testing section.
	sb.WriteString("## Size: raw vs post-zstd (the compression-reality axis)\n\n")
	sb.WriteString("Thesis under test: msgpack's raw-size advantage largely collapses after zstd (sluice chunks ship compressed). `zstd/raw` is the compression ratio; compare zstd columns ACROSS serializers for the real on-disk delta.\n\n")
	for _, corpus := range corpora {
		fmt.Fprintf(&sb, "### %s\n\n", corpus)
		sb.WriteString("| Serializer | Model | Raw (MiB) | Zstd (MiB) | zstd/raw | vs JSON raw | vs JSON zstd | Fidelity |\n")
		sb.WriteString("|---|---|---:|---:|---:|---:|---:|---|\n")
		var jsonRaw, jsonZstd float64
		for _, r := range results {
			if r.Corpus == corpus && r.Name == "stdlib_v1" {
				jsonRaw = float64(r.RawBytes)
				jsonZstd = float64(r.ZstdBytes)
				break
			}
		}
		for _, r := range results {
			if r.Corpus != corpus {
				continue
			}
			vsRaw, vsZstd := "baseline", "baseline"
			if r.Name != "stdlib_v1" && jsonRaw > 0 {
				vsRaw = fmt.Sprintf("%+.1f%%", 100*(float64(r.RawBytes)-jsonRaw)/jsonRaw)
				vsZstd = fmt.Sprintf("%+.1f%%", 100*(float64(r.ZstdBytes)-jsonZstd)/jsonZstd)
			}
			fmt.Fprintf(&sb, "| %s | %s | %.2f | %.2f | %.3f | %s | %s | %s |\n",
				r.Name, r.Model,
				float64(r.RawBytes)/(1<<20), float64(r.ZstdBytes)/(1<<20),
				float64(r.ZstdBytes)/float64(r.RawBytes),
				vsRaw, vsZstd, r.Fidelity)
		}
		sb.WriteString("\n")
	}

	// Throughput, decode-first.
	sb.WriteString("## Throughput (decode-first — DR-critical axis)\n\n")
	for _, corpus := range corpora {
		fmt.Fprintf(&sb, "### %s\n\n", corpus)
		sb.WriteString("| Serializer | Model | Decode MB/s | Dec allocs/op | Dec B/op | Dec heap Δ (KiB) | Encode MB/s | Enc+zstd MB/s | Enc allocs/op | Fidelity |\n")
		sb.WriteString("|---|---|---:|---:|---:|---:|---:|---:|---:|---|\n")
		for _, r := range results {
			if r.Corpus != corpus {
				continue
			}
			fmt.Fprintf(&sb, "| %s | %s | %.1f | %.1f | %.0f | %+d | %.1f | %.1f | %.1f | %s |\n",
				r.Name, r.Model,
				r.DecodeMBperS, r.DecAllocsPerOp, r.DecBytesPerOp, r.DecodeHeapDiff/1024,
				r.EncodeMBperS, r.EncodeZstdMBperS, r.EncAllocsPerOp,
				r.Fidelity)
		}
		sb.WriteString("\n")
	}

	if _, err := io.WriteString(w, sb.String()); err != nil {
		return err
	}
	return nil
}

func orderedCorpora(results []SResult) []string {
	var corpora []string
	cs := map[string]bool{}
	for _, r := range results {
		if !cs[r.Corpus] {
			cs[r.Corpus] = true
			corpora = append(corpora, r.Corpus)
		}
	}
	sort.Strings(corpora)
	return corpora
}
