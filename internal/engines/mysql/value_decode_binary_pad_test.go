// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestDecodeValue_BinaryPadStripReconstructed pins the binlog
// BINARY(N) pad reconstruction (adversarial-corpus finding,
// 2026-08-22): the binlog ROW image strips a fixed BINARY column's
// trailing 0x00 padding, so the CDC lane delivered fewer bytes than
// the column's semantic width (BINARY(8) 0xDEAD000000000000 arrived
// as 2 bytes via CDC, 8 via bulk copy — a silent per-lane
// divergence). decodeValue must re-pad to the declared width — and
// must NOT pad VARBINARY/BLOB, where a short value is a real value.
func TestDecodeValue_BinaryPadStripReconstructed(t *testing.T) {
	t.Run("short binary is re-padded to declared width", func(t *testing.T) {
		got, err := decodeValue([]byte{0xDE, 0xAD}, ir.Binary{Length: 8})
		if err != nil {
			t.Fatalf("decodeValue: %v", err)
		}
		want := []byte{0xDE, 0xAD, 0, 0, 0, 0, 0, 0}
		if !bytes.Equal(got.([]byte), want) {
			t.Errorf("decodeValue(BINARY(8), 2 bytes) = %x; want %x (binlog pad-strip must be reconstructed)", got, want)
		}
	})
	t.Run("string input (driver text protocol) is re-padded too", func(t *testing.T) {
		got, err := decodeValue("\xde\xad", ir.Binary{Length: 4})
		if err != nil {
			t.Fatalf("decodeValue: %v", err)
		}
		want := []byte{0xDE, 0xAD, 0, 0}
		if !bytes.Equal(got.([]byte), want) {
			t.Errorf("decodeValue(BINARY(4), string) = %x; want %x", got, want)
		}
	})
	t.Run("all-padding value stripped to empty re-pads to N zeros", func(t *testing.T) {
		// The all-zeros BINARY(N) value is entirely padding, so the
		// binlog strips every byte. An empty-but-non-nil value must
		// re-pad to N zeros; only a genuine SQL NULL (raw == nil,
		// handled before this arm) may produce nil. The empty-vs-nil
		// question for the actual go-mysql wire is answered empirically
		// by the corpus cell bin_zeros on the CDC round.
		got, err := decodeValue([]byte{}, ir.Binary{Length: 8})
		if err != nil {
			t.Fatalf("decodeValue: %v", err)
		}
		want := make([]byte, 8)
		if !bytes.Equal(got.([]byte), want) {
			t.Errorf("decodeValue(BINARY(8), empty) = %x; want %x (all-zeros value, not NULL)", got, want)
		}
	})
	t.Run("embedded NUL preserved, only the tail padded", func(t *testing.T) {
		got, err := decodeValue([]byte{0xDE, 0x00, 0xAD}, ir.Binary{Length: 8})
		if err != nil {
			t.Fatalf("decodeValue: %v", err)
		}
		want := []byte{0xDE, 0x00, 0xAD, 0, 0, 0, 0, 0}
		if !bytes.Equal(got.([]byte), want) {
			t.Errorf("decodeValue(BINARY(8), de00ad) = %x; want %x", got, want)
		}
	})
	t.Run("full-width binary passes through unchanged", func(t *testing.T) {
		full := []byte{1, 2, 3, 4, 5, 6, 7, 8}
		got, err := decodeValue(full, ir.Binary{Length: 8})
		if err != nil {
			t.Fatalf("decodeValue: %v", err)
		}
		if !bytes.Equal(got.([]byte), full) {
			t.Errorf("decodeValue(BINARY(8), full) = %x; want %x", got, full)
		}
	})
	t.Run("varbinary is never padded — a short value is a real value", func(t *testing.T) {
		got, err := decodeValue([]byte{0xDE, 0xAD}, ir.Varbinary{Length: 8})
		if err != nil {
			t.Fatalf("decodeValue: %v", err)
		}
		if len(got.([]byte)) != 2 {
			t.Errorf("decodeValue(VARBINARY(8), 2 bytes) = %x; want the 2 original bytes, unpadded", got)
		}
	})
	t.Run("blob is never padded", func(t *testing.T) {
		got, err := decodeValue([]byte{0xFF}, ir.Blob{Size: ir.BlobRegular})
		if err != nil {
			t.Fatalf("decodeValue: %v", err)
		}
		if len(got.([]byte)) != 1 {
			t.Errorf("decodeValue(BLOB, 1 byte) = %x; want the 1 original byte, unpadded", got)
		}
	})
}
