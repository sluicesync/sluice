//go:build compressbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package compressbench

import (
	stdgzip "compress/gzip"
	"fmt"
	"io"

	kpgzip "github.com/klauspost/compress/gzip"
	kpsnappy "github.com/klauspost/compress/snappy"
	kpzstd "github.com/klauspost/compress/zstd"
)

// Algo is the per-algorithm adapter used by the benchmark. Each entry
// wraps a writer factory + reader factory that produce streams matching
// the algorithm's natural decompression API. The benchmark consumes
// these uniformly so it can swap algorithms without per-call
// type-switches.
type Algo struct {
	// Name is the human-readable identifier the markdown emitter uses.
	Name string

	// NewWriter wraps `dst` in a compressor. Implementations choose the
	// compression level via the constructor; the benchmark calibrates
	// at the level shown in the harness comments below.
	NewWriter func(dst io.Writer) (io.WriteCloser, error)

	// NewReader is the inverse of NewWriter. Most algorithms expose a
	// constructor that doesn't require explicit Close on the reader —
	// the harness still calls Close uniformly for safety; ReadCloser
	// is the matching shape.
	NewReader func(src io.Reader) (io.ReadCloser, error)
}

// allAlgos is the registry of algorithms the harness benchmarks.
// Ordered by maturity in Go's ecosystem: stdlib first, then klauspost
// drop-ins, then klauspost's own algorithms. The harness iterates this
// slice; adding an algorithm is one entry here.
//
// Compression levels chosen to match a fair "default" comparison —
// each algorithm at its respective level=default. The Phase 1 prod
// path uses `gzip.NewWriter(...)` which is level=DefaultCompression
// (=6); the klauspost wrappers default to the same logical level so
// comparisons stay apples-to-apples.
//
// Two additional rows could be added if operators want a Pareto
// curve: zstd at level=1 (speed-tier) and zstd at level=11 (ratio-
// tier). Not in v1 of the harness; one tier per algorithm keeps the
// table readable.
var allAlgos = []Algo{
	{
		Name: "stdlib_gzip",
		NewWriter: func(dst io.Writer) (io.WriteCloser, error) {
			return stdgzip.NewWriter(dst), nil
		},
		NewReader: func(src io.Reader) (io.ReadCloser, error) {
			return stdgzip.NewReader(src)
		},
	},
	{
		Name: "klauspost_gzip",
		NewWriter: func(dst io.Writer) (io.WriteCloser, error) {
			return kpgzip.NewWriter(dst), nil
		},
		NewReader: func(src io.Reader) (io.ReadCloser, error) {
			return kpgzip.NewReader(src)
		},
	},
	{
		Name: "klauspost_zstd_default",
		NewWriter: func(dst io.Writer) (io.WriteCloser, error) {
			return kpzstd.NewWriter(dst,
				kpzstd.WithEncoderLevel(kpzstd.SpeedDefault))
		},
		NewReader: func(src io.Reader) (io.ReadCloser, error) {
			r, err := kpzstd.NewReader(src)
			if err != nil {
				return nil, fmt.Errorf("zstd reader: %w", err)
			}
			// kpzstd.Decoder.Close returns nothing; wrap it.
			return zstdReadCloser{r}, nil
		},
	},
	{
		Name: "klauspost_zstd_better",
		NewWriter: func(dst io.Writer) (io.WriteCloser, error) {
			return kpzstd.NewWriter(dst,
				kpzstd.WithEncoderLevel(kpzstd.SpeedBetterCompression))
		},
		NewReader: func(src io.Reader) (io.ReadCloser, error) {
			r, err := kpzstd.NewReader(src)
			if err != nil {
				return nil, fmt.Errorf("zstd reader: %w", err)
			}
			return zstdReadCloser{r}, nil
		},
	},
	{
		Name: "klauspost_snappy",
		NewWriter: func(dst io.Writer) (io.WriteCloser, error) {
			return kpsnappy.NewBufferedWriter(dst), nil
		},
		NewReader: func(src io.Reader) (io.ReadCloser, error) {
			return io.NopCloser(kpsnappy.NewReader(src)), nil
		},
	},
}

// zstdReadCloser adapts klauspost's `*zstd.Decoder` (whose Close
// returns nothing) to the `io.ReadCloser` shape the harness expects.
// The Close call returns nil so callers' error paths stay symmetric
// with the gzip / snappy adapters.
type zstdReadCloser struct {
	*kpzstd.Decoder
}

func (z zstdReadCloser) Close() error {
	z.Decoder.Close()
	return nil
}
