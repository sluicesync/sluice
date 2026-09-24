// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// fillSourceEngine serves OpenRowReader from a fixed row set and records the
// table shape the fill asked it to read. Every other Open* panics
// (stubEngine): the fill must touch nothing but the row reader.
type fillSourceEngine struct {
	stubEngine
	rows   []ir.Row
	opened int
	read   []*ir.Table
}

func (e *fillSourceEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	e.opened++
	return &fillRowReader{e: e}, nil
}

type fillRowReader struct{ e *fillSourceEngine }

func (r *fillRowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	r.e.read = append(r.e.read, table)
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for _, row := range r.e.rows {
			narrowed := ir.Row{}
			for _, c := range table.Columns {
				narrowed[c.Name] = row[c.Name]
			}
			select {
			case out <- narrowed:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (r *fillRowReader) Err() error { return nil }

// fillDelta is an alter_table delta on users(id PK, email) adding cols.
func fillDelta(pk bool, cols ...*ir.Column) *irbackup.SchemaDeltaEntry {
	before := &ir.Table{Schema: "public", Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Text{}},
	}}
	if pk {
		before.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}
	}
	after := *before
	after.Columns = append(slices.Clone(before.Columns), cols...)
	return &irbackup.SchemaDeltaEntry{Kind: irbackup.SchemaDeltaAlterTable, Schema: "public", Table: "users", Before: before, After: &after}
}

// runFill runs captureAddColumnFill over deltas and decodes every change it
// appended, in list order.
func runFill(t *testing.T, e *fillSourceEngine, m *irbackup.Manifest, chunkSize int) []ir.Change {
	t.Helper()
	stream := &BackupStream{segStore: newMemStore(), segCodec: blobcodec.CodecGzip}
	window := len(m.ChangeChunks)
	if _, err := captureAddColumnFill(context.Background(), e, "dsn", newFillChunkBuffer(stream, m, nil), chunkSize); err != nil {
		t.Fatalf("captureAddColumnFill: %v", err)
	}
	var out []ir.Change
	for i, ci := range m.ChangeChunks[window:] {
		src, err := stream.segStore.Get(context.Background(), ci.File)
		if err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
		cr, err := blobcodec.NewChangeChunkReader(src, ci.SHA256, nil, blobcodec.CodecGzip, nil)
		if err != nil {
			t.Fatalf("chunk %d reader: %v", i, err)
		}
		for {
			c, err := cr.ReadChange()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("chunk %d read: %v", i, err)
			}
			out = append(out, c)
		}
		if err := cr.Close(); err != nil {
			t.Fatalf("chunk %d close: %v", i, err)
		}
	}
	return out
}

func fillManifest(deltas ...*irbackup.SchemaDeltaEntry) *irbackup.Manifest {
	return &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     time.Unix(1, 0).UTC(),
		Kind:          irbackup.BackupKindIncremental,
		StartPosition: ir.Position{Engine: "postgres", Token: "start"},
		EndPosition:   ir.Position{Engine: "postgres", Token: "end"},
		// A chunk the window already wrote: the fill must append after it.
		ChangeChunks: []*irbackup.ChunkInfo{{File: "window-0", RowCount: 1}},
		SchemaDelta:  deltas,
	}
}

// TestCaptureAddColumnFill_EmitsKeyAddressedUpdatesAfterTheWindow pins the
// shape the replay depends on: one source transaction at the window's end
// position, one UPDATE per source row whose Before is the key alone and
// whose After is the key plus the added columns, appended after the
// window's own chunks and rolled on the chunk size — and a generated added
// column neither read nor carried, since the target computes it.
func TestCaptureAddColumnFill_EmitsKeyAddressedUpdatesAfterTheWindow(t *testing.T) {
	e := &fillSourceEngine{rows: []ir.Row{
		{"id": int64(1), "email": "a", "c": "v1", "g": "gen"},
		{"id": int64(2), "email": "b", "c": nil, "g": "gen"},
		{"id": int64(3), "email": "c", "c": "v3", "g": "gen"},
	}}
	d := fillDelta(
		true,
		&ir.Column{Name: "c", Type: ir.Text{}, Nullable: true},
		&ir.Column{Name: "g", Type: ir.Text{}, GeneratedExpr: "upper(email)", GeneratedStored: true},
	)
	m := fillManifest(d)
	changes := runFill(t, e, m, 2)

	if got := len(m.ChangeChunks); got != 1+3 { // 5 events at 2 per chunk
		t.Errorf("chunks = %d; want the window's 1 + 3 fill chunks", got)
	}
	if m.ChangeChunks[0].File != "window-0" {
		t.Errorf("the window's chunk moved: %+v", m.ChangeChunks[0])
	}
	if len(e.read) != 1 || len(e.read[0].Columns) != 2 || e.read[0].Columns[0].Name != "id" || e.read[0].Columns[1].Name != "c" {
		t.Fatalf("fill read %+v; want users(id, c) only", e.read)
	}
	if len(changes) != 5 {
		t.Fatalf("changes = %d (%+v); want TxBegin, 3 updates, TxCommit", len(changes), changes)
	}
	if _, ok := changes[0].(ir.TxBegin); !ok {
		t.Errorf("first change %T; want TxBegin", changes[0])
	}
	if _, ok := changes[4].(ir.TxCommit); !ok {
		t.Errorf("last change %T; want TxCommit", changes[4])
	}
	for i, c := range changes {
		if c.Pos() != m.EndPosition {
			t.Errorf("change %d at %+v; want the window's end position", i, c.Pos())
		}
	}
	for i, want := range e.rows {
		u, ok := changes[i+1].(ir.Update)
		if !ok {
			t.Fatalf("change %d is %T; want Update", i+1, changes[i+1])
		}
		if u.Schema != "public" || u.Table != "users" {
			t.Errorf("update %d addresses %s.%s", i, u.Schema, u.Table)
		}
		if len(u.Before) != 1 || u.Before["id"] != want["id"] {
			t.Errorf("update %d Before = %+v; want the key alone", i, u.Before)
		}
		if len(u.After) != 2 || u.After["id"] != want["id"] || u.After["c"] != want["c"] {
			t.Errorf("update %d After = %+v; want id and c", i, u.After)
		}
	}
	if d.AddColumnFill == nil || !slices.Equal(d.AddColumnFill.Columns, []string{"c"}) || d.AddColumnFill.Rows != 3 || d.AddColumnFill.Skipped != "" {
		t.Errorf("fill record = %+v; want columns [c], 3 rows, captured", d.AddColumnFill)
	}
}

// TestCaptureAddColumnFill_RecordsWhatItCannotCapture pins the shapes that
// record a fill without events: a table with no key to address rows by and
// a key that includes the added column are SKIPPED (the restore names every
// column); an empty table is captured with zero rows and writes nothing. A
// delta that adds no stored column, and a DDL-only window with no end
// position, are covered alongside.
func TestCaptureAddColumnFill_RecordsWhatItCannotCapture(t *testing.T) {
	t.Run("keyless", func(t *testing.T) {
		e := &fillSourceEngine{rows: []ir.Row{{"id": int64(1), "c": "v"}}}
		d := fillDelta(false, &ir.Column{Name: "c", Type: ir.Text{}, Nullable: true})
		m := fillManifest(d)
		if changes := runFill(t, e, m, 10); len(changes) != 0 || e.opened != 0 {
			t.Errorf("keyless fill wrote %d changes, opened %d readers; want none", len(changes), e.opened)
		}
		if d.AddColumnFill == nil || d.AddColumnFill.Skipped == "" {
			t.Errorf("fill record = %+v; want skipped", d.AddColumnFill)
		}
	})
	t.Run("key-includes-added-column", func(t *testing.T) {
		e := &fillSourceEngine{}
		d := fillDelta(true, &ir.Column{Name: "c", Type: ir.Text{}})
		d.After.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}, {Column: "c"}}}
		m := fillManifest(d)
		if changes := runFill(t, e, m, 10); len(changes) != 0 || d.AddColumnFill == nil || d.AddColumnFill.Skipped == "" {
			t.Errorf("fill = %+v, %d changes; want skipped with none", d.AddColumnFill, len(changes))
		}
	})
	t.Run("empty-table", func(t *testing.T) {
		e := &fillSourceEngine{}
		d := fillDelta(true, &ir.Column{Name: "c", Type: ir.Text{}, Nullable: true})
		m := fillManifest(d)
		if changes := runFill(t, e, m, 10); len(changes) != 0 || len(m.ChangeChunks) != 1 {
			t.Errorf("empty table wrote %d changes / %d chunks; want no chunk at all", len(changes), len(m.ChangeChunks))
		}
		if d.AddColumnFill == nil || d.AddColumnFill.Rows != 0 || d.AddColumnFill.Skipped != "" {
			t.Errorf("fill record = %+v; want captured, 0 rows", d.AddColumnFill)
		}
	})
	t.Run("no-stored-column-added", func(t *testing.T) {
		e := &fillSourceEngine{}
		gen := fillDelta(true, &ir.Column{Name: "g", Type: ir.Text{}, GeneratedExpr: "upper(email)", GeneratedStored: true})
		add := &irbackup.SchemaDeltaEntry{Kind: irbackup.SchemaDeltaAddTable, Table: "t2", After: gen.After}
		m := fillManifest(gen, add)
		if changes := runFill(t, e, m, 10); len(changes) != 0 || e.opened != 0 || gen.AddColumnFill != nil || add.AddColumnFill != nil {
			t.Errorf("wrote %d changes, opened %d readers, records %+v / %+v; want nothing touched",
				len(changes), e.opened, gen.AddColumnFill, add.AddColumnFill)
		}
	})
	t.Run("ddl-only-window", func(t *testing.T) {
		e := &fillSourceEngine{rows: []ir.Row{{"id": int64(1), "c": "v"}}}
		m := fillManifest(fillDelta(true, &ir.Column{Name: "c", Type: ir.Text{}, Nullable: true}))
		m.EndPosition = ir.Position{}
		changes := runFill(t, e, m, 10)
		if len(changes) != 3 {
			t.Fatalf("changes = %d; want TxBegin, 1 update, TxCommit", len(changes))
		}
		for i, c := range changes {
			if c.Pos() != m.StartPosition {
				t.Errorf("change %d at %+v; want the start position a DDL-only window resumed from", i, c.Pos())
			}
		}
	})
}
