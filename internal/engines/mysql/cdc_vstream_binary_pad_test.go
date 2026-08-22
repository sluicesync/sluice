// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"testing"

	"vitess.io/vitess/go/vt/proto/query"
)

// TestDecodeVStreamCell_BinaryPadStripReconstructed pins the VStream
// sibling of the binlog BINARY(N) pad-strip fix (adversarial-corpus
// finding, 2026-08-22): a fixed BINARY column's semantic value is
// always exactly N bytes (MySQL right-pads with 0x00 at INSERT), and
// the binlog ROW image strips that trailing padding. Whether Vitess's
// rowstreamer re-pads before the VStream wire is UNVERIFIED locally
// (no real VStream on the rig — to be confirmed on the
// PlanetScale/vitess-cluster follow-on), so decodeVStreamCell now pads
// only-when-short: a no-op if Vitess already delivers the full width,
// the faithful reconstruction if it does not. Either way this pin
// holds.
//
// VARBINARY/BLOB must NEVER be padded — a short value there is a real
// value — and a missing/garbled column_type must pass bytes through
// unchanged (never worse than the pre-fix behavior).
func TestDecodeVStreamCell_BinaryPadStripReconstructed(t *testing.T) {
	cases := []struct {
		name string
		f    *query.Field
		raw  []byte
		want []byte
	}{
		{
			name: "short binary re-padded to declared width",
			f:    &query.Field{Type: query.Type_BINARY, ColumnType: "binary(8)"},
			raw:  []byte{0xDE, 0xAD},
			want: []byte{0xDE, 0xAD, 0, 0, 0, 0, 0, 0},
		},
		{
			name: "embedded NUL preserved, only the tail padded",
			f:    &query.Field{Type: query.Type_BINARY, ColumnType: "binary(8)"},
			raw:  []byte{0xDE, 0x00, 0xAD},
			want: []byte{0xDE, 0x00, 0xAD, 0, 0, 0, 0, 0},
		},
		{
			name: "all-padding value stripped to nothing re-pads to N zeros",
			f:    &query.Field{Type: query.Type_BINARY, ColumnType: "binary(8)"},
			raw:  []byte{},
			want: []byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name: "full-width binary passes through unchanged",
			f:    &query.Field{Type: query.Type_BINARY, ColumnType: "binary(8)"},
			raw:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
			want: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			name: "bare binary declaration is BINARY(1)",
			f:    &query.Field{Type: query.Type_BINARY, ColumnType: "binary"},
			raw:  []byte{},
			want: []byte{0},
		},
		{
			name: "absent column_type passes through unpadded (never worse than pre-fix)",
			f:    &query.Field{Type: query.Type_BINARY},
			raw:  []byte{0xDE, 0xAD},
			want: []byte{0xDE, 0xAD},
		},
		{
			name: "varbinary is never padded — a short value is a real value",
			f:    &query.Field{Type: query.Type_VARBINARY, ColumnType: "varbinary(8)"},
			raw:  []byte{0xDE, 0xAD},
			want: []byte{0xDE, 0xAD},
		},
		{
			name: "blob is never padded",
			f:    &query.Field{Type: query.Type_BLOB, ColumnType: "blob"},
			raw:  []byte{0xFF},
			want: []byte{0xFF},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := decodeVStreamCell(c.f, c.raw)
			gb, ok := got.([]byte)
			if !ok {
				t.Fatalf("decodeVStreamCell = %T(%v); want []byte", got, got)
			}
			if !bytes.Equal(gb, c.want) {
				t.Errorf("decodeVStreamCell = %x; want %x", gb, c.want)
			}
		})
	}
}
