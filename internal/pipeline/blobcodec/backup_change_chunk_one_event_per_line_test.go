// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

import (
	"bytes"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeChunk_OneEventPerRecordLine binds the premise the broker's
// mid-incremental frontier rests on (ADR-0191 §3.2, review item 4): the
// frontier's `through` is an ordinal over DECODED events, while the
// `chunks` digest it is checked against pins chunk BYTES. The two agree only
// while every record line decodes to exactly one event and every written
// event is exactly one line — a decoder that ever split a record into two
// events, or folded two into one, or skipped a kind, would shift every later
// ordinal under an unchanged digest, and a resume would skip the wrong
// events silently.
//
// The independent expected value is the raw line count of the uncompressed
// chunk, counted with bytes.Count rather than by the reader under test; it
// must equal the writer's ChangeCount (the manifest's RowCount) and the
// number of events the reader returns, for every change kind the codec
// carries, on every codec. A SchemaSnapshot rides the manifest, not the
// chunk: it adds no line, no count and no event.
func TestChangeChunk_OneEventPerRecordLine(t *testing.T) {
	id := ir.ApplyID{TxID: "pg:1:1:0/10", Seq: 1}
	changes := append(rowChangesWith(id), ir.SchemaSnapshot{})
	const wantEvents = 6 // every kind of rowChangesWith; the snapshot adds none
	for _, mode := range applyIDChunkModes {
		t.Run(mode.name, func(t *testing.T) {
			var cek []byte
			if mode.cek {
				cek = bytes.Repeat([]byte{7}, 32)
			}
			buf := &bytes.Buffer{}
			w, err := NewChangeChunkWriter(buf, cek, mode.codec, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range changes {
				if err := w.WriteChange(c); err != nil {
					t.Fatalf("WriteChange(%T): %v", c, err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if got := w.ChangeCount(); got != wantEvents {
				t.Errorf("ChangeCount = %d; want %d (one per event, none for the snapshot)", got, wantEvents)
			}
			if mode.codec == CodecNone && !mode.cek {
				// One header line, then one line per record.
				if lines := bytes.Count(buf.Bytes(), []byte{'\n'}) - 1; lines != wantEvents {
					t.Errorf("the uncompressed chunk holds %d record lines; want %d", lines, wantEvents)
				}
			}
			events := readChangeChunk(t, buf.Bytes(), w.Hash(), mode.codec, cek)
			if int64(len(events)) != w.ChangeCount() {
				t.Fatalf("the reader returned %d events for %d written records: an ordinal over decoded events no longer "+
					"names the record it counted", len(events), w.ChangeCount())
			}
			for i, ev := range events {
				if want := changes[i]; fmt.Sprintf("%T", ev) != fmt.Sprintf("%T", want) {
					t.Errorf("event %d decoded as %T; record %d was written as %T", i, ev, i, want)
				}
			}
		})
	}
}
