// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0152 chunk-position-binding pins at the codec layer, exercising
// BOTH chunk families — the row-chunk codec and the change-chunk codec
// ride separate writer/reader pairs (the Bug-74 family discipline), so
// each gets the full accept/refuse matrix.

package blobcodec

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
)

func aadTestCEK(t *testing.T) []byte {
	t.Helper()
	cek, err := crypto.GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	return cek
}

// writeBoundRowChunk writes a one-row chunk under (cek, aad) and
// returns its bytes + hash.
func writeBoundRowChunk(t *testing.T, cek, aad []byte) (data []byte, hash string) {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewChunkWriter(&buf, []string{"id"}, cek, CodecGzip, aad)
	if err != nil {
		t.Fatalf("NewChunkWriter: %v", err)
	}
	cols := []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}
	if err := w.WriteRow(ir.Row{"id": int64(1)}, cols); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes(), w.Hash()
}

func writeBoundChangeChunk(t *testing.T, cek, aad []byte) (data []byte, hash string) {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewChangeChunkWriter(&buf, cek, CodecGzip, aad)
	if err != nil {
		t.Fatalf("NewChangeChunkWriter: %v", err)
	}
	if err := w.WriteChange(ir.Insert{Table: "t", Row: ir.Row{"id": int64(1)}}); err != nil {
		t.Fatalf("WriteChange: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes(), w.Hash()
}

// TestChunkCodecs_AADMatrix runs the accept/refuse matrix over both
// chunk families: same AAD opens; wrong AAD, stripped AAD, and
// retrofitted AAD refuse at open (before any row/event is emitted);
// plaintext chunks refuse an AAD at construction on BOTH sides (an
// unencrypted chunk cannot carry an authenticated binding — silently
// accepting one would fake integrity).
func TestChunkCodecs_AADMatrix(t *testing.T) {
	aadA := []byte("sluice-chunk-aad/v1\ncreated_at=X\nfile=a-0")
	aadB := []byte("sluice-chunk-aad/v1\ncreated_at=X\nfile=b-0")

	type family struct {
		name  string
		write func(t *testing.T, cek, aad []byte) ([]byte, string)
		open  func(data []byte, hash string, cek, aad []byte) error
	}
	families := []family{
		{
			name:  "row chunk",
			write: writeBoundRowChunk,
			open: func(data []byte, hash string, cek, aad []byte) error {
				r, err := NewChunkReader(io.NopCloser(bytes.NewReader(data)), hash, cek, CodecGzip, aad)
				if err != nil {
					return err
				}
				for {
					if _, err := r.ReadRow(); errors.Is(err, io.EOF) {
						break
					} else if err != nil {
						_ = r.Close()
						return err
					}
				}
				return r.Close()
			},
		},
		{
			name:  "change chunk",
			write: writeBoundChangeChunk,
			open: func(data []byte, hash string, cek, aad []byte) error {
				r, err := NewChangeChunkReader(io.NopCloser(bytes.NewReader(data)), hash, cek, CodecGzip, aad)
				if err != nil {
					return err
				}
				for {
					if _, err := r.ReadChange(); errors.Is(err, io.EOF) {
						break
					} else if err != nil {
						_ = r.Close()
						return err
					}
				}
				return r.Close()
			},
		},
	}

	for _, f := range families {
		t.Run(f.name, func(t *testing.T) {
			cek := aadTestCEK(t)
			bound, boundHash := f.write(t, cek, aadA)
			unbound, unboundHash := f.write(t, cek, nil)

			if err := f.open(bound, boundHash, cek, aadA); err != nil {
				t.Fatalf("same AAD must open cleanly: %v", err)
			}
			if err := f.open(bound, boundHash, cek, aadB); err == nil {
				t.Error("wrong AAD opened the chunk; the position-splice class is back")
			} else if !strings.Contains(err.Error(), "does not belong at this position") {
				t.Errorf("wrong-AAD error %q should name the spliced-chunk hypothesis", err.Error())
			}
			if err := f.open(bound, boundHash, cek, nil); err == nil {
				t.Error("stripped AAD opened a BOUND chunk; the downgrade class is back")
			}
			if err := f.open(unbound, unboundHash, cek, aadA); err == nil {
				t.Error("retrofitted AAD opened an UNBOUND (legacy) chunk; bound and unbound ciphertext classes must not mix")
			}
			if err := f.open(unbound, unboundHash, cek, nil); err != nil {
				t.Errorf("legacy unbound chunk must keep opening nil-AAD: %v", err)
			}

			// Plaintext + AAD is a caller bug on either side: loud.
			if _, _, err := func() (a []byte, b string, err error) {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("plaintext+aad writer panicked: %v", r)
					}
				}()
				var buf bytes.Buffer
				switch f.name {
				case "row chunk":
					_, err = NewChunkWriter(&buf, []string{"id"}, nil, CodecGzip, aadA)
				default:
					_, err = NewChangeChunkWriter(&buf, nil, CodecGzip, aadA)
				}
				return nil, "", err
			}(); err == nil {
				t.Error("plaintext writer accepted an AAD; must refuse loudly (a plaintext chunk cannot carry an authenticated binding)")
			}
			plain, plainHash := f.write(t, nil, nil)
			if err := f.open(plain, plainHash, nil, aadA); err == nil {
				t.Error("plaintext reader accepted an AAD; must refuse loudly")
			}
		})
	}
}
