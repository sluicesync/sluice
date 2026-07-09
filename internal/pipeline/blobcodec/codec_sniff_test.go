// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

import (
	"bytes"
	"crypto/rand"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
)

// TestSniffCodec_MagicTable pins the magic-byte classifier on synthetic
// prefixes: every codec signature, plus the refusal shapes (empty,
// truncated, garbage). Truncation matters because the prober reads only
// the first [SniffCodecPrefixLen] bytes — a 1-byte file that happens to
// start a gzip magic must refuse, not classify.
func TestSniffCodec_MagicTable(t *testing.T) {
	cases := []struct {
		name    string
		prefix  []byte
		want    Codec
		wantErr bool
	}{
		{"zstd frame magic", []byte{0x28, 0xB5, 0x2F, 0xFD}, CodecZstd, false},
		{"gzip magic", []byte{0x1F, 0x8B, 0x08, 0x00}, CodecGzip, false},
		{"gzip magic exactly 2 bytes", []byte{0x1F, 0x8B}, CodecGzip, false},
		{"none (chunk header brace)", []byte(`{"_h`), CodecNone, false},
		{"none single brace", []byte{'{'}, CodecNone, false},
		{"empty file", nil, "", true},
		{"truncated gzip 1 byte", []byte{0x1F}, "", true},
		{"truncated zstd 3 bytes", []byte{0x28, 0xB5, 0x2F}, "", true},
		{"garbage", []byte("row "), "", true},
		{"binary garbage", []byte{0x00, 0x01, 0x02, 0x03}, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SniffCodec(tc.prefix)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("SniffCodec(% X) = %q, nil; want loud refusal", tc.prefix, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("SniffCodec(% X): %v", tc.prefix, err)
			}
			if got != tc.want {
				t.Errorf("SniffCodec(% X) = %q; want %q", tc.prefix, got, tc.want)
			}
		})
	}
}

// TestSniffCodec_EveryCodecFamily_RealWriterBytes ground-truths the
// sniff against what [ChunkWriter] ACTUALLY emits, for EVERY codec
// family — none, gzip, zstd — not one representative (the Bug 74
// pin-the-class discipline: the sniff dispatches on the codec family,
// so a green test on one codec covers nothing about the others).
func TestSniffCodec_EveryCodecFamily_RealWriterBytes(t *testing.T) {
	for _, codec := range []Codec{CodecNone, CodecGzip, CodecZstd} {
		t.Run(string(codec), func(t *testing.T) {
			var buf bytes.Buffer
			cw, err := NewChunkWriter(&buf, []string{"a"}, nil, codec, nil)
			if err != nil {
				t.Fatalf("NewChunkWriter(%s): %v", codec, err)
			}
			if err := cw.WriteRow(ir.Row{"a": int64(1)}, []*ir.Column{{Name: "a"}}); err != nil {
				t.Fatalf("WriteRow: %v", err)
			}
			if err := cw.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			got, err := SniffCodec(buf.Bytes()[:min(len(buf.Bytes()), SniffCodecPrefixLen)])
			if err != nil {
				t.Fatalf("SniffCodec(real %s chunk): %v", codec, err)
			}
			if got != codec {
				t.Errorf("SniffCodec(real %s chunk) = %q; want %q", codec, got, codec)
			}
		})
	}
}

// TestSniffCodec_EncryptedChunkShape pins the LAYOUT FACT the callers
// are built around: the codec sits INSIDE the encryption envelope, so
// an encrypted chunk's on-disk bytes start with a random GCM nonce (no
// codec magic — a raw sniff would be a coin flip, which is exactly why
// the lineage prober gates on the chunk's RECORDED encryption metadata
// and decrypts first), while the DECRYPTED bytes sniff to the true
// codec. If a future format change moved the codec outside the
// encryption, this test fails and the prober's gating must be
// re-derived.
func TestSniffCodec_EncryptedChunkShape(t *testing.T) {
	cek := make([]byte, crypto.CEKLen)
	if _, err := rand.Read(cek); err != nil {
		t.Fatalf("rand: %v", err)
	}
	var buf bytes.Buffer
	cw, err := NewChunkWriter(&buf, []string{"a"}, cek, CodecGzip, nil)
	if err != nil {
		t.Fatalf("NewChunkWriter: %v", err)
	}
	if err := cw.WriteRow(ir.Row{"a": int64(1)}, []*ir.Column{{Name: "a"}}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	pt, err := crypto.DecryptChunk(buf.Bytes(), cek)
	if err != nil {
		t.Fatalf("DecryptChunk: %v", err)
	}
	got, err := SniffCodec(pt[:min(len(pt), SniffCodecPrefixLen)])
	if err != nil {
		t.Fatalf("SniffCodec(decrypted chunk): %v", err)
	}
	if got != CodecGzip {
		t.Errorf("SniffCodec(decrypted chunk) = %q; want %q (codec layer no longer inside the encryption envelope?)", got, CodecGzip)
	}
}
