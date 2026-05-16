// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Per-segment compression codec (ADR-0046 §5). Each lineage segment
// records its codec in lineage.json; restore reads the recorded codec
// for that segment and NEVER infers the codec from the chunk bytes.
// The codec wraps the JSON-Lines body the chunk writer/reader produce;
// encryption (when enabled) is applied to the codec output, exactly as
// the pre-ADR gzip-only path applied encryption to the gzip output.
//
// Why a recorded codec and not a sniffed one: a restore that guesses
// gzip-vs-none-vs-zstd from the first bytes is a latent corruption
// path — a `none` chunk whose first JSON byte happens to look like a
// gzip magic prefix would be mis-decoded. The codec is metadata, read
// from the segment, full stop. An unknown / garbled recorded codec is
// a loud refusal (DR data — loud-fail, never silent-assemble).
//
// zstd uses klauspost/compress/zstd at SpeedDefault and is the
// v0.67.0+ default ([DefaultCodec]). Grounded in
// docs/dev/notes/compression-benchmark.md's decode-inclusive evidence:
// zstd decodes 55–85% faster than gzip on every corpus (restore speed
// is the DR-critical axis) at a ~1–5% ratio cost vs klauspost gzip on
// representative chunk data. klauspost/compress is already a direct
// module dependency (no new dep). The gzip→zstd default flip is a
// clean break (pre-users; no back-compat shim — sluice's zero-users
// tenet); --compression=gzip remains available.

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Codec is the per-segment compression codec recorded in lineage.json.
// The zero value is invalid; callers resolve via [resolveCodec] /
// [parseCodec] so [DefaultCodec] (zstd) is applied consistently.
type Codec string

const (
	// CodecNone stores chunk bodies uncompressed. The on-disk file is
	// human-readable JSON-Lines on a local-FS target — the operator-
	// inspectability case (eyeball `.jsonl`). Object stores never
	// auto-compress, so compression is always sluice-side; `none` is
	// principled for local targets.
	CodecNone Codec = "none"

	// CodecGzip is the pre-v0.67.0 default, still operator-selectable
	// via --compression=gzip. No longer the default (see [DefaultCodec]).
	CodecGzip Codec = "gzip"

	// CodecZstd uses klauspost/compress/zstd at SpeedDefault. The
	// v0.67.0+ default ([DefaultCodec]); the codec surface is opened
	// once.
	CodecZstd Codec = "zstd"
)

// DefaultCodec is the codec applied when the operator does not pass
// --compression: zstd, chosen on the decode-inclusive compressbench
// evidence (faster restore — the DR-critical axis; see
// docs/dev/notes/compression-benchmark.md). Pre-v0.67.0 this was gzip;
// the flip is a clean break (zero-users tenet — no back-compat shim).
const DefaultCodec = CodecZstd

// resolveCodec applies the "empty → default" rule. An empty Codec
// (operator left --compression unset, or a segment recorded none)
// resolves to [DefaultCodec] (zstd). A v0.67.0+ backup always records
// its codec explicitly; the empty case is the operator-unset write
// default.
func resolveCodec(c Codec) Codec {
	if c == "" {
		return DefaultCodec
	}
	return c
}

// parseCodec validates an operator-supplied codec string. Empty
// resolves to [DefaultCodec] (zstd) so a programmatic
// ParseCompression("") agrees with the documented default. Unknown
// values are a loud refusal — never a silent fallback (DR data).
func parseCodec(s string) (Codec, error) {
	switch Codec(s) {
	case CodecNone:
		return CodecNone, nil
	case CodecGzip:
		return CodecGzip, nil
	case CodecZstd:
		return CodecZstd, nil
	case "":
		return DefaultCodec, nil
	default:
		return "", fmt.Errorf("unknown compression codec %q; supported: none, gzip, zstd", s)
	}
}

// ParseCompression is the exported CLI entry point for validating an
// operator-supplied --compression value. Unknown values are a loud
// refusal (never a silent fallback — DR data).
func ParseCompression(s string) (Codec, error) { return parseCodec(s) }

// validateRecordedCodec rejects an unknown / garbled codec recorded on
// a segment. The codec is read from lineage.json, never sniffed; an
// unrecognised recorded value means a tampered / corrupt catalog and
// the loud-failure tenet says refuse, never silently assemble.
func validateRecordedCodec(c Codec) error {
	switch resolveCodec(c) {
	case CodecNone, CodecGzip, CodecZstd:
		return nil
	default:
		return fmt.Errorf("backup: segment records unknown compression codec %q; refusing to restore (codec is recorded, never inferred — a corrupt/tampered lineage)", c)
	}
}

// codecWriteCloser is the compression-writer surface the chunk writers
// wrap. gzip.Writer and zstd.Encoder both satisfy it; CodecNone uses a
// pass-through that satisfies it too. Close MUST flush the codec.
type codecWriteCloser interface {
	io.WriteCloser
}

// nopCodecWriteCloser is the CodecNone writer: bytes pass straight
// through to the underlying writer, Close is a no-op (there is no
// codec buffer to flush). The chunk writer's own bufio + sha256
// machinery is unchanged; only the compression layer differs.
type nopCodecWriteCloser struct {
	w io.Writer
}

func (n nopCodecWriteCloser) Write(p []byte) (int, error) { return n.w.Write(p) }
func (nopCodecWriteCloser) Close() error                  { return nil }

// newCodecWriter returns a [codecWriteCloser] wrapping dst for codec c.
// Mirrors the pre-ADR `gzip.NewWriter(dst)` call shape so the chunk
// writers change by exactly one line. zstd is constructed at
// SpeedDefault per ADR-0046 §5.
func newCodecWriter(dst io.Writer, c Codec) (codecWriteCloser, error) {
	switch resolveCodec(c) {
	case CodecNone:
		return nopCodecWriteCloser{w: dst}, nil
	case CodecGzip:
		return gzip.NewWriter(dst), nil
	case CodecZstd:
		enc, err := zstd.NewWriter(dst, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			return nil, fmt.Errorf("zstd writer: %w", err)
		}
		return enc, nil
	default:
		return nil, fmt.Errorf("newCodecWriter: unknown codec %q", c)
	}
}

// codecReadCloser is the decompression-reader surface the chunk
// readers wrap. gzip.Reader satisfies it directly; zstd.Decoder is
// adapted (its Close has no error return) and CodecNone is a
// pass-through. Close releases codec resources.
type codecReadCloser interface {
	io.Reader
	Close() error
}

// nopCodecReadCloser is the CodecNone reader: bytes pass straight
// through from the underlying reader.
type nopCodecReadCloser struct {
	r io.Reader
}

func (n nopCodecReadCloser) Read(p []byte) (int, error) { return n.r.Read(p) }
func (nopCodecReadCloser) Close() error                 { return nil }

// zstdReadCloser adapts a *zstd.Decoder (whose Close() returns no
// error) to the codecReadCloser surface so the chunk readers can treat
// every codec uniformly.
type zstdReadCloser struct {
	d *zstd.Decoder
}

func (z zstdReadCloser) Read(p []byte) (int, error) { return z.d.Read(p) }
func (z zstdReadCloser) Close() error               { z.d.Close(); return nil }

// newCodecReader returns a [codecReadCloser] wrapping src for codec c.
// Mirrors the pre-ADR `gzip.NewReader(src)` call shape. The codec is
// the one RECORDED for the segment in lineage.json — callers pass it
// through from the segment metadata, never sniff it from the bytes.
func newCodecReader(src io.Reader, c Codec) (codecReadCloser, error) {
	switch resolveCodec(c) {
	case CodecNone:
		return nopCodecReadCloser{r: src}, nil
	case CodecGzip:
		gz, err := gzip.NewReader(src)
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		return gz, nil
	case CodecZstd:
		dec, err := zstd.NewReader(src)
		if err != nil {
			return nil, fmt.Errorf("zstd reader: %w", err)
		}
		return zstdReadCloser{d: dec}, nil
	default:
		return nil, fmt.Errorf("newCodecReader: unknown codec %q", c)
	}
}
