// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// backupRecorderEngine is a fake [ir.Engine] tailored to backup tests:
// it returns a configurable schema and serves rows from a per-table
// in-memory map. The recordingEngine in migrate_test.go isn't enough
// because the row reader there returns an empty channel.
type backupRecorderEngine struct {
	name   string
	schema *ir.Schema
	rows   map[string][]ir.Row
}

func newBackupRecorderEngine(name string, schema *ir.Schema, rows map[string][]ir.Row) *backupRecorderEngine {
	return &backupRecorderEngine{name: name, schema: schema, rows: rows}
}

func (e *backupRecorderEngine) Name() string                  { return e.name }
func (e *backupRecorderEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *backupRecorderEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	return &recordingSchemaReader{schema: e.schema}, nil
}

func (e *backupRecorderEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return nil, errors.New("backupRecorderEngine: write side not implemented")
}

func (e *backupRecorderEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	return &fakeRowReader{rows: e.rows}, nil
}

func (e *backupRecorderEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	return nil, errors.New("backupRecorderEngine: write side not implemented")
}

func (*backupRecorderEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("not implemented")
}

func (*backupRecorderEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not implemented")
}

func (*backupRecorderEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not implemented")
}

type fakeRowReader struct {
	rows map[string][]ir.Row
}

func (r *fakeRowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for _, row := range r.rows[table.Name] {
			select {
			case <-ctx.Done():
				return
			case out <- row:
			}
		}
	}()
	return out, nil
}

func (*fakeRowReader) Err() error { return nil }

func TestBackup_Validate(t *testing.T) {
	cases := []struct {
		name string
		b    *Backup
		want string
	}{
		{"nil source", &Backup{SourceDSN: "x", Store: &LocalStore{}}, "Source engine is nil"},
		{"empty DSN", &Backup{Source: stubEngine{}, Store: &LocalStore{}}, "SourceDSN is empty"},
		{"nil store", &Backup{Source: stubEngine{}, SourceDSN: "x"}, "Store is nil"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.b.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v; want contains %q", err, c.want)
			}
		})
	}
}

func TestBackup_RoundTrip_SingleTable(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{
		Tables: []*ir.Table{
			{
				Name: "users",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
					{Name: "name", Type: ir.Varchar{Length: 100}},
					{Name: "active", Type: ir.Boolean{}},
				},
			},
		},
	}
	rows := map[string][]ir.Row{
		"users": {
			{"id": int64(1), "name": "Alice", "active": true},
			{"id": int64(2), "name": "Bob", "active": false},
			{"id": int64(3), "name": "Carol", "active": true},
		},
	}
	src := newBackupRecorderEngine("postgres", schema, rows)

	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	b := &Backup{
		Source:        src,
		SourceDSN:     "src",
		Store:         store,
		ChunkRows:     2, // force two chunks: 2 rows + 1 row
		SluiceVersion: "test",
		Now:           func() time.Time { return now },
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	manifest, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if manifest.SourceEngine != "postgres" {
		t.Errorf("SourceEngine = %q; want postgres", manifest.SourceEngine)
	}
	if !manifest.CreatedAt.Equal(now) {
		t.Errorf("CreatedAt = %v; want %v", manifest.CreatedAt, now)
	}
	if len(manifest.Tables) != 1 {
		t.Fatalf("Tables len = %d", len(manifest.Tables))
	}
	tableEntry := manifest.Tables[0]
	if tableEntry.RowCount != 3 {
		t.Errorf("RowCount = %d; want 3", tableEntry.RowCount)
	}
	if len(tableEntry.Chunks) != 2 {
		t.Fatalf("Chunks len = %d; want 2", len(tableEntry.Chunks))
	}
	if tableEntry.Chunks[0].RowCount != 2 || tableEntry.Chunks[1].RowCount != 1 {
		t.Errorf("chunk row counts = [%d, %d]; want [2, 1]",
			tableEntry.Chunks[0].RowCount, tableEntry.Chunks[1].RowCount)
	}

	// Verify each chunk's hash is non-empty hex.
	for i, c := range tableEntry.Chunks {
		if c.SHA256 == "" || len(c.SHA256) != 64 {
			t.Errorf("chunk[%d].SHA256 = %q; want 64-hex", i, c.SHA256)
		}
	}

	// Run VerifyBackup and confirm clean.
	total, mismatches, err := VerifyBackup(context.Background(), store)
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if total != 2 || mismatches != 0 {
		t.Errorf("VerifyBackup total/mismatches = %d/%d; want 2/0", total, mismatches)
	}
}

func TestBackup_EmptyTableProducesNoChunks(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "empty", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
	src := newBackupRecorderEngine("mysql", schema, map[string][]ir.Row{})

	b := &Backup{Source: src, SourceDSN: "src", Store: store}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	m, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if len(m.Tables) != 1 {
		t.Fatalf("Tables len = %d", len(m.Tables))
	}
	if m.Tables[0].RowCount != 0 || len(m.Tables[0].Chunks) != 0 {
		t.Errorf("empty table: rows=%d chunks=%d; want 0/0",
			m.Tables[0].RowCount, len(m.Tables[0].Chunks))
	}
}

func TestBackup_FilterPrunesTables(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			{Name: "audit_log", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
	rows := map[string][]ir.Row{
		"users":     {{"id": int64(1)}},
		"audit_log": {{"id": int64(99)}},
	}
	src := newBackupRecorderEngine("postgres", schema, rows)

	b := &Backup{
		Source: src, SourceDSN: "src", Store: store,
		Filter: TableFilter{Exclude: []string{"audit_*"}},
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	m, _ := readManifest(context.Background(), store)
	if len(m.Tables) != 1 || m.Tables[0].Name != "users" {
		t.Errorf("filter not applied: tables = %+v", m.Tables)
	}
}

// chunkFilePath is a stable construction; pin its shape so a future
// rename ripples through the manifest contract loudly.
func TestChunkFilePath(t *testing.T) {
	cases := []struct {
		name string
		t    *ir.Table
		idx  int
		want string
	}{
		{"flat scope", &ir.Table{Name: "users"}, 0, "chunks/users/users-0.jsonl.gz"},
		{"flat scope idx 7", &ir.Table{Name: "orders"}, 7, "chunks/orders/orders-7.jsonl.gz"},
		{"schema-qualified", &ir.Table{Schema: "public", Name: "events"}, 0, "chunks/public__events/public__events-0.jsonl.gz"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := chunkFilePath(c.t, c.idx)
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
}
