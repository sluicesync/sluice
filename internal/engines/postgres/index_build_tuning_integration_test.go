//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for index-build phase tuning (Phase A). Boots a
// real PG container and ground-truths two things the unit tests can't:
// (1) the pg_settings probe SQL parses + scans against the real catalog
// (a SQL fat-finger or a PG-version drift in pg_size_bytes / SHOW
// surfaces here), and that `SET maintenance_work_mem` actually takes on
// a dedicated connection; and (2) CreateIndexes still produces the
// correct indexes (names match) after the tuning runs on the build
// session. The pure autotune math is exhaustively unit-tested in
// index_build_tuning_test.go.

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestProbeIndexBuildTuning_SaneNumbers asserts the raw probe reads
// plausible values against a default container and that a SET derived
// from the autotune actually takes on a dedicated connection (read back
// via SHOW on the same connection).
func TestProbeIndexBuildTuning_SaneNumbers(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)
	sw := swHandle.(*SchemaWriter)

	conn, err := sw.db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire dedicated conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	probe, err := probeIndexBuildTuning(ctx, conn)
	if err != nil {
		t.Fatalf("probeIndexBuildTuning: %v", err)
	}
	if probe.sharedBuffersBytes <= 0 {
		t.Errorf("shared_buffers = %d bytes, want > 0", probe.sharedBuffersBytes)
	}
	if probe.maintenanceWorkMemBytes <= 0 {
		t.Errorf("maintenance_work_mem = %d bytes, want > 0", probe.maintenanceWorkMemBytes)
	}
	if probe.maxWorkerProcesses < 1 {
		t.Errorf("max_worker_processes = %d, implausibly low", probe.maxWorkerProcesses)
	}

	// Derive the SET values and apply them on the dedicated conn, then
	// read maintenance_work_mem back via pg_size_bytes(current_setting)
	// on the SAME conn to prove the session GUC took.
	memBytes, workers := computeIndexBuildTuning(probe, 0)
	memKB := memBytes / 1024
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET maintenance_work_mem = '%dkB'", memKB)); err != nil {
		t.Fatalf("SET maintenance_work_mem: %v", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET max_parallel_maintenance_workers = %d", workers)); err != nil {
		t.Fatalf("SET max_parallel_maintenance_workers: %v", err)
	}
	var gotMemBytes int64
	if err := conn.QueryRowContext(
		ctx, "SELECT pg_size_bytes(current_setting('maintenance_work_mem'))",
	).Scan(&gotMemBytes); err != nil {
		t.Fatalf("read back maintenance_work_mem: %v", err)
	}
	// The SET rounds to whole kB; allow a 1 kB slack on the round-trip.
	if delta := gotMemBytes - memKB*1024; delta < -1024 || delta > 1024 {
		t.Errorf("maintenance_work_mem after SET = %d bytes, want ~%d", gotMemBytes, memKB*1024)
	}
}

// TestCreateIndexes_TunedProducesCorrectIndexes drives the full
// CreateIndexes path with an explicit --index-build-mem override and
// asserts the indexes land with the expected names. The tuning runs on
// the dedicated build connection; the test confirms it doesn't perturb
// the index output.
func TestCreateIndexes_TunedProducesCorrectIndexes(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "widgets",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "sku", Type: ir.Varchar{Length: 64}},
				{Name: "name", Type: ir.Varchar{Length: 255}},
			},
			PrimaryKey: &ir.Index{
				Name:    "widgets_pkey",
				Unique:  true,
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
			Indexes: []*ir.Index{
				{
					Name:    "widgets_sku_unique",
					Unique:  true,
					Kind:    ir.IndexKindBTree,
					Columns: []ir.IndexColumn{{Column: "sku"}},
				},
				{
					Name:    "widgets_name_idx",
					Kind:    ir.IndexKindBTree,
					Columns: []ir.IndexColumn{{Column: "name"}},
				},
			},
		},
	}}

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)

	// Thread an explicit override through the optional tuner surface —
	// the same path the pipeline's applyIndexBuildMem uses.
	tuner, ok := swHandle.(ir.IndexBuildTuner)
	if !ok {
		t.Fatal("PG SchemaWriter must implement ir.IndexBuildTuner")
	}
	tuner.SetIndexBuildMem(256 * 1024 * 1024) // 256 MiB

	if err := swHandle.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes: %v", err)
	}

	// Assert both secondary indexes exist (the PK index is created
	// inline with the table; we check the two CreateIndexes emitted).
	sw := swHandle.(*SchemaWriter)
	for _, want := range []string{"widgets_sku_unique", "widgets_name_idx"} {
		var count int
		if err := sw.db.QueryRowContext(
			ctx,
			"SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1",
			want,
		).Scan(&count); err != nil {
			t.Fatalf("query pg_indexes for %q: %v", want, err)
		}
		if count != 1 {
			t.Errorf("index %q: got count %d, want 1", want, count)
		}
	}
}
