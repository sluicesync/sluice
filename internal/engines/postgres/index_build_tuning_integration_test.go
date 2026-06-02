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

// TestCreateIndexes_ConcurrentMatchesSerial drives CreateIndexes (Phase B)
// twice over a multi-table, multi-index schema — once forced serial
// (--index-build-parallelism=1) and once forced concurrent (=4) — on two
// separate databases, and asserts the resulting index set is byte-for-byte
// identical (same names, same definitions). The concurrent worker pool
// must not perturb the output, drop an index, or race two CREATE INDEXes
// onto one connection's session. The CI -race Integration job is the
// authoritative gate for the data-race side; this test pins the
// correctness/equivalence side.
func TestCreateIndexes_ConcurrentMatchesSerial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	schema := multiIndexSchema()

	serial := indexDefsAfterBuild(ctx, t, schema, 1)
	concurrent := indexDefsAfterBuild(ctx, t, schema, 4)

	if len(serial) == 0 {
		t.Fatal("serial build produced no indexes")
	}
	if len(serial) != len(concurrent) {
		t.Fatalf("index count: serial=%d concurrent=%d", len(serial), len(concurrent))
	}
	for name, serialDef := range serial {
		concDef, ok := concurrent[name]
		if !ok {
			t.Errorf("index %q present after serial build, missing after concurrent build", name)
			continue
		}
		if serialDef != concDef {
			t.Errorf("index %q definition differs:\n serial:     %s\n concurrent: %s", name, serialDef, concDef)
		}
	}
}

// indexDefsAfterBuild creates the schema's tables + indexes on a fresh
// database with the given --index-build-parallelism, then returns a map of
// index name → pg_get_indexdef() for every non-PK index. Two builds with
// different parallelism must yield identical maps.
func indexDefsAfterBuild(ctx context.Context, t *testing.T, schema *ir.Schema, parallelism int) map[string]string {
	t.Helper()
	dsn, cleanup := startPostgres(t)
	t.Cleanup(cleanup)

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	t.Cleanup(func() { closeIf(swHandle) })

	tuner, ok := swHandle.(ir.IndexBuildTuner)
	if !ok {
		t.Fatal("PG SchemaWriter must implement ir.IndexBuildTuner")
	}
	tuner.SetIndexBuildParallelism(parallelism)

	if err := swHandle.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints (parallelism=%d): %v", parallelism, err)
	}
	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes (parallelism=%d): %v", parallelism, err)
	}

	sw := swHandle.(*SchemaWriter)
	rows, err := sw.db.QueryContext(ctx, `
		SELECT indexname, indexdef
		FROM pg_indexes
		WHERE schemaname = 'public' AND indexname NOT LIKE '%_pkey'
		ORDER BY indexname`)
	if err != nil {
		t.Fatalf("query pg_indexes (parallelism=%d): %v", parallelism, err)
	}
	defer func() { _ = rows.Close() }()

	defs := map[string]string{}
	for rows.Next() {
		var name, def string
		if err := rows.Scan(&name, &def); err != nil {
			t.Fatalf("scan pg_indexes row: %v", err)
		}
		defs[name] = def
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pg_indexes rows: %v", err)
	}
	return defs
}

// multiIndexSchema returns a 3-table schema with enough secondary indexes
// (7 across the tables) that a parallelism=4 build genuinely interleaves
// workers — exercising the channel-fed worker pool, not a degenerate
// single-job run.
func multiIndexSchema() *ir.Schema {
	mkTable := func(name string, extraCols []string) *ir.Table {
		cols := []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
		}
		var idxs []*ir.Index
		for _, c := range extraCols {
			cols = append(cols, &ir.Column{Name: c, Type: ir.Varchar{Length: 128}})
			idxs = append(idxs, &ir.Index{
				Name:    name + "_" + c + "_idx",
				Kind:    ir.IndexKindBTree,
				Columns: []ir.IndexColumn{{Column: c}},
			})
		}
		return &ir.Table{
			Name:    name,
			Columns: cols,
			PrimaryKey: &ir.Index{
				Name:    name + "_pkey",
				Unique:  true,
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
			Indexes: idxs,
		}
	}
	return &ir.Schema{Tables: []*ir.Table{
		mkTable("alpha", []string{"a1", "a2", "a3"}),
		mkTable("bravo", []string{"b1", "b2"}),
		mkTable("charlie", []string{"c1", "c2"}),
	}}
}
