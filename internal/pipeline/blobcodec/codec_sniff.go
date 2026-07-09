// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

// Codec sniffing for catalog REBUILD (audit N-14). The per-segment
// codec is recorded in lineage.json and restore trusts the record
// (ADR-0046 §5 — see codec.go); but when lineage.json itself is LOST,
// the rebuild tool used to stamp [DefaultCodec] unconditionally, so a
// gzip- or none-compressed chain got a rebuilt catalog that LIED about
// its codec and restore failed with a bare zstd decode error — a
// wrong-heal on the DR path. [SniffCodec] re-derives the record from
// chunk magic bytes at rebuild time; restore then trusts the (now
// truthful) record exactly as before. This does NOT weaken the
// recorded-never-sniffed restore contract: no per-chunk decode path
// ever sniffs — the sniff runs once, at record-(re)creation time.
//
// Why the sniff is deterministic here (unlike the general case
// codec.go's comment warns about): the codec's INPUT is always a
// sluice chunk stream, whose first byte is the `{` of the header line
// `{"_h":1,...}`. The three codecs are therefore disjoint on their
// first bytes — zstd frame magic (28 B5 2F FD), gzip magic (1F 8B),
// or `{` for none — and anything else is not a sluice chunk in any
// known codec (loud refusal, never a guess).
//
// Encryption caveat (the layout fact that shapes the callers): the
// codec layer sits INSIDE the encryption envelope — [ChunkWriter]
// compresses first and encrypts the codec output on Close, so an
// encrypted chunk's on-disk bytes are `[nonce | ciphertext | authtag]`
// and begin with a RANDOM nonce. Sniffing those raw bytes is not just
// useless but dangerous (a random nonce matches gzip's 2-byte magic 1
// in 2^16 reads). Callers MUST gate on the chunk's recorded encryption
// metadata and decrypt before sniffing; the chain-level prober in
// internal/pipeline/lineage does exactly that.

import (
	"bytes"
	"errors"
	"fmt"
)

// SniffCodecPrefixLen is the number of leading plaintext-chunk bytes
// [SniffCodec] needs to classify the codec: 4, the length of the zstd
// frame magic (the longest of the three signatures).
const SniffCodecPrefixLen = 4

// Codec magic signatures. The zstd frame magic is 0xFD2FB528
// little-endian; gzip is the RFC 1952 two-byte ID; CodecNone has no
// framing at all, so its signature is the sluice chunk header's
// opening `{` (see the package comment above for why that is
// deterministic).
var (
	zstdFrameMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}
	gzipMagic      = []byte{0x1F, 0x8B}
)

// SniffCodec classifies the compression codec from the first bytes of
// a PLAINTEXT chunk stream (decrypt first for encrypted chunks — the
// codec layer is inside the encryption envelope; see the package
// comment in this file). prefix should carry at least
// [SniffCodecPrefixLen] bytes when the file has that many; shorter
// inputs are classified when a full signature fits and refused
// (truncated/corrupt chunk) otherwise. Bytes matching no signature are
// a loud refusal — the caller must never fall back to a guess.
func SniffCodec(prefix []byte) (Codec, error) {
	switch {
	case len(prefix) >= len(zstdFrameMagic) && bytes.Equal(prefix[:len(zstdFrameMagic)], zstdFrameMagic):
		return CodecZstd, nil
	case len(prefix) >= len(gzipMagic) && bytes.Equal(prefix[:len(gzipMagic)], gzipMagic):
		return CodecGzip, nil
	case len(prefix) >= 1 && prefix[0] == '{':
		return CodecNone, nil
	case len(prefix) == 0:
		return "", errors.New("sniff codec: chunk file is empty")
	default:
		return "", fmt.Errorf(
			"sniff codec: chunk bytes match no known compression codec (first %d byte(s): % X); "+
				"expected zstd (28 B5 2F FD), gzip (1F 8B), or an uncompressed sluice chunk header ('{') — "+
				"truncated, corrupt, or not a sluice chunk",
			len(prefix), prefix,
		)
	}
}
