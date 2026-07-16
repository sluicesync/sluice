// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the audit-MED-A1 sync-cold-start threading of the ADR-0148
// deploy-request index-build fallback: coldStartOpenTargetWriters hands
// [Streamer.IndexBuildFallback] to the freshly-opened target
// SchemaWriter (via the optional [ir.IndexBuildFallbackSetter]) so the
// deferred cold-start CreateIndexes — the same walled build migrate's
// index phase runs — can route a walled failure to the deploy request;
// unarmed (the zero value every fleet/programmatic caller gets) never
// touches the setter. The engine half is pinned against the real MySQL
// writer in internal/engines/mysql/schema_writer_index_fallback_test.go;
// this covers the orchestrator seam that was missing. The multi-database
// sibling (coldStartCopyOneDatabase) threads through the same
// migcore.ApplyIndexBuildFallback one-liner right next to the
// IndexBuildMem/Parallelism knobs it already mirrors.

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// coldStartFallbackEngine is a minimal fake target engine whose schema
// writer records the threaded ir.IndexBuildFallback.
type coldStartFallbackEngine struct {
	sw *coldStartFallbackSchemaWriter
}

func (*coldStartFallbackEngine) Name() string                  { return "fake-target" }
func (*coldStartFallbackEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *coldStartFallbackEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return e.sw, nil
}

func (*coldStartFallbackEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return coldStartFallbackRowWriter{}, nil
}

func (*coldStartFallbackEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, errors.New("not used")
}

func (*coldStartFallbackEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("not used")
}

func (*coldStartFallbackEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("not used")
}

func (*coldStartFallbackEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not used")
}

func (*coldStartFallbackEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not used")
}

type coldStartFallbackSchemaWriter struct {
	got  ir.IndexBuildFallback
	sets int
}

func (w *coldStartFallbackSchemaWriter) SetIndexBuildFallback(f ir.IndexBuildFallback) {
	w.got = f
	w.sets++
}

func (*coldStartFallbackSchemaWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	return nil
}

func (*coldStartFallbackSchemaWriter) CreateIndexes(context.Context, *ir.Schema) error { return nil }

func (*coldStartFallbackSchemaWriter) CreateConstraints(context.Context, *ir.Schema) error {
	return nil
}

func (*coldStartFallbackSchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error {
	return nil
}
func (*coldStartFallbackSchemaWriter) CreateViews(context.Context, *ir.Schema) error { return nil }

type coldStartFallbackRowWriter struct{}

func (coldStartFallbackRowWriter) WriteRows(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	for range rows { //nolint:revive // drain
	}
	return nil
}

// coldStartFallback is an inert ir.IndexBuildFallback with an identity.
type coldStartFallback struct{ id string }

func (coldStartFallback) BuildIndexDDL(context.Context, string, []string, error) error { return nil }

// TestColdStartOpenTargetWriters_ThreadsIndexBuildFallback pins the armed
// and unarmed cold-start threading through the real
// coldStartOpenTargetWriters path (the writers-open seam every
// single-database cold start takes before runBulkCopyWithOpts builds
// indexes).
func TestColdStartOpenTargetWriters_ThreadsIndexBuildFallback(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}

	t.Run("armed threads the exact value", func(t *testing.T) {
		eng := &coldStartFallbackEngine{sw: &coldStartFallbackSchemaWriter{}}
		fb := coldStartFallback{id: "armed"}
		s := &Streamer{Target: eng, TargetDSN: "tgt", IndexBuildFallback: fb}
		sw, rw, err := s.coldStartOpenTargetWriters(context.Background(), schema, &ir.SnapshotStream{})
		if err != nil {
			t.Fatalf("coldStartOpenTargetWriters: %v", err)
		}
		_ = sw
		_ = rw
		if eng.sw.got != ir.IndexBuildFallback(fb) {
			t.Errorf("threaded fallback = %#v; want the armed value", eng.sw.got)
		}
	})

	t.Run("unarmed never touches the setter", func(t *testing.T) {
		eng := &coldStartFallbackEngine{sw: &coldStartFallbackSchemaWriter{}}
		s := &Streamer{Target: eng, TargetDSN: "tgt"}
		if _, _, err := s.coldStartOpenTargetWriters(context.Background(), schema, &ir.SnapshotStream{}); err != nil {
			t.Fatalf("coldStartOpenTargetWriters: %v", err)
		}
		if eng.sw.sets != 0 {
			t.Errorf("SetIndexBuildFallback called %d times on an unarmed cold start; want 0", eng.sw.sets)
		}
	})
}
