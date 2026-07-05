// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"io"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// failOnNthPutStore wraps a [blobcodec.LocalStore] and returns an injected
// error on the Nth Put call (1-indexed). Used to simulate a crash mid-backup
// so the resume path is exercised. Mirror of the carved-out backup package's
// test copy (duplicated across the two package test trees so neither imports
// the other's).
type failOnNthPutStore struct {
	*blobcodec.LocalStore

	failOn  int
	putN    int
	failErr error
}

func newFailOnNthPutStore(inner *blobcodec.LocalStore, failOn int) *failOnNthPutStore {
	return &failOnNthPutStore{
		LocalStore: inner,
		failOn:     failOn,
		failErr:    errors.New("injected failure for resume test"),
	}
}

func (s *failOnNthPutStore) Put(ctx context.Context, path string, r io.Reader) error {
	s.putN++
	if s.putN == s.failOn {
		return s.failErr
	}
	return s.LocalStore.Put(ctx, path, r)
}

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
