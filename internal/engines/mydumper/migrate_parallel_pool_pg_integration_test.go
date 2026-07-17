//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-table worker-pool verification for the mydumper source engine
// (perf-parity gap #13). The migrate cross-table copy pool (ADR-0076) is
// SOURCE-AGNOSTIC: it opens a dedicated source reader per concurrent table
// via the engine's OpenRowReader, so a multi-table mydumper dump is copied
// through the SAME bounded pool the live DB engines use — no flat-file
// special-casing. Row 1 of docs/dev/perf-parity-matrix.md said "all
// engines" but predated the flat-file sources, so whether the pool actually
// reaches them was UNVERIFIED. This pin closes that: a synthetic dump with
// many tables, each split across several chunk FILES, migrated to a real
// Postgres target at --table-parallelism 4, and every row from every file
// of every table asserted value-exact.
//
// Within-table parallelism stays a DELIBERATE absence for this source (the
// dump's chunk files carry no PK addressing, so the reader implements none
// of the batched/bounds surfaces the within-table chunker needs — see
// capabilities_assert.go and matrix row 2/24); this test exercises the
// CROSS-table axis only, which is the axis the pool provides source-
// agnostically.
//
// The bound itself (never more than --table-parallelism tables copying at
// once) is source-agnostically unit-pinned by
// TestRunBulkCopyTablePool_PeakBoundedByParallelism in internal/pipeline;
// here the correctness bar is that parallelism changes THROUGHPUT, never the
// data — so the same dump migrated at parallelism 4 and at parallelism 1
// must land byte-identical rows.

package mydumper

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
)

// parallelPoolDump describes the synthetic dump geometry: nTables tables,
// each dumped across nChunks data-chunk FILES of rowsPerChunk rows. Chosen
// wide enough that several tables genuinely copy concurrently at
// parallelism 4 (exercising concurrent OpenRowReader on the real reader),
// yet small enough to stay fast.
const (
	parallelPoolTables      = 6
	parallelPoolChunks      = 3
	parallelPoolRowsPerFile = 8
	parallelPoolDBPrefix    = "shop"
)

// TestMydumperMigrate_CrossTablePool_PG pins that a multi-table,
// multi-chunk-file mydumper dump copies EVERY row from EVERY file correctly
// through the source-agnostic cross-table pool at --table-parallelism 4,
// and that the result is byte-identical to the serial (--table-parallelism
// 1) copy — parallelism changes throughput, never the data (gap #13).
func TestMydumperMigrate_CrossTablePool_PG(t *testing.T) {
	pgAdminDSN := startPostgresIT(t)

	dumpDir := buildParallelPoolDump(t)
	want := expectedParallelPoolRows()

	pgEng := mustEngine(t, "postgres")
	dumpEng := mustEngine(t, "mydumper")

	// Parallel leg: the cross-table pool at width 4 opens dedicated mydumper
	// readers concurrently (the free pair + up to 3 dedicated pairs).
	parallelDSN := createPGDB(t, pgAdminDSN, "dump_pool_parallel")
	runMigratePool(t, dumpEng, pgEng, dumpDir, parallelDSN, 4)
	got := snapshotDatabase(t, pgEng, parallelDSN)
	assertSnapshotEquals(t, "parallel(--table-parallelism=4)", want, got)

	// Serial leg: the SAME dump at width 1 (the pre-ADR-0076 single-goroutine
	// behaviour). Data must be identical — the pool is a throughput axis only.
	serialDSN := createPGDB(t, pgAdminDSN, "dump_pool_serial")
	runMigratePool(t, dumpEng, pgEng, dumpDir, serialDSN, 1)
	gotSerial := snapshotDatabase(t, pgEng, serialDSN)
	assertSnapshotEquals(t, "serial(--table-parallelism=1)", want, gotSerial)
}

// runMigratePool runs a mydumper→target migrate at an explicit
// --table-parallelism. runMigrate (the shared helper) doesn't expose the
// knob, so the Migrator is built inline. BulkParallelism is pinned to 1 so
// the WITHIN-table axis never claims budget the cross-table axis under test
// needs — on a few-core runner an auto within-degree could otherwise
// collapse the resolved table parallelism to 1 and make the "parallel" leg
// vacuous. (mydumper declines within-table chunking regardless — no PK
// addressing — so pinning it costs nothing here.)
func runMigratePool(t *testing.T, source, target ir.Engine, srcDSN, tgtDSN string, tableParallelism int) {
	t.Helper()
	mig := &pipeline.Migrator{
		Source:           source,
		Target:           target,
		SourceDSN:        srcDSN,
		TargetDSN:        tgtDSN,
		TableParallelism: tableParallelism,
		BulkParallelism:  1,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run(%s → %s, table-parallelism=%d): %v", source.Name(), target.Name(), tableParallelism, err)
	}
}

// buildParallelPoolDump writes a synthetic mydumper dump directory:
// parallelPoolTables tables, each with a schema file and parallelPoolChunks
// data-chunk files carrying disjoint, per-row-distinct rows. The values are
// deliberately simple (bigint PK + a per-row-unique varchar) so a mis-copy
// under concurrency — a dropped chunk, a swapped table, a shared-reader
// clobber — surfaces as a row-set mismatch, not as a value-decode subtlety.
func buildParallelPoolDump(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	// A minimal, lenient-parse metadata file (openDumpDir requires it to
	// exist; the position parse is informational).
	writeDumpFile(t, dir, "metadata",
		"Started dump at: 2026-01-01 00:00:00\n"+
			"SHOW MASTER STATUS:\n"+
			"\tLog: mysql-bin.000001\n"+
			"\tPos: 4\n"+
			"\tGTID:\n\n"+
			"Finished dump at: 2026-01-01 00:00:01\n")

	for ti := 0; ti < parallelPoolTables; ti++ {
		table := fmt.Sprintf("t%02d", ti)
		writeDumpFile(t, dir, fmt.Sprintf("%s.%s-schema.sql", parallelPoolDBPrefix, table),
			fmt.Sprintf("CREATE TABLE `%s` (`id` bigint NOT NULL, `payload` varchar(64) NOT NULL, PRIMARY KEY (`id`));", table))
		for ci := 0; ci < parallelPoolChunks; ci++ {
			var b []byte
			b = append(b, fmt.Sprintf("INSERT INTO `%s` VALUES ", table)...)
			for r := 0; r < parallelPoolRowsPerFile; r++ {
				id := ci*parallelPoolRowsPerFile + r
				if r > 0 {
					b = append(b, ',')
				}
				b = append(b, fmt.Sprintf("(%d,'%s')", id, parallelPoolPayload(ti, ci, id))...)
			}
			b = append(b, ';')
			writeDumpFile(t, dir, fmt.Sprintf("%s.%s.%05d.sql", parallelPoolDBPrefix, table, ci), string(b))
		}
	}
	return dir
}

// parallelPoolPayload is the per-row-unique value; embedding the table,
// chunk, and id makes a cross-file or cross-table mixup detectable.
func parallelPoolPayload(ti, ci, id int) string {
	return fmt.Sprintf("t%02d-c%d-r%d", ti, ci, id)
}

// expectedParallelPoolRows renders the exact canonical row set the migrated
// target must hold, in the SAME shape snapshotDatabase produces (canonical
// per-column text, sorted per table).
func expectedParallelPoolRows() map[string][]string {
	out := map[string][]string{}
	for ti := 0; ti < parallelPoolTables; ti++ {
		table := fmt.Sprintf("t%02d", ti)
		var rows []string
		for ci := 0; ci < parallelPoolChunks; ci++ {
			for r := 0; r < parallelPoolRowsPerFile; r++ {
				id := ci*parallelPoolRowsPerFile + r
				rows = append(rows, fmt.Sprintf("id=int:%d|payload=str:%s", id, parallelPoolPayload(ti, ci, id)))
			}
		}
		// snapshotDatabase sorts each table's rendered rows; match it so the
		// slice compare is order-stable.
		sort.Strings(rows)
		out[table] = rows
	}
	return out
}

// assertSnapshotEquals fails the test if got (a snapshotDatabase result)
// differs from want in table set or in any table's row set.
func assertSnapshotEquals(t *testing.T, leg string, want, got map[string][]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: table count = %d; want %d (got %v)", leg, len(got), len(want), tableNames(got))
	}
	for name, wantRows := range want {
		gotRows, ok := got[name]
		if !ok {
			t.Errorf("%s: table %s missing on the migrated target", leg, name)
			continue
		}
		if len(gotRows) != len(wantRows) {
			t.Errorf("%s: table %s: rows got=%d want=%d", leg, name, len(gotRows), len(wantRows))
			continue
		}
		for i := range wantRows {
			if gotRows[i] != wantRows[i] {
				t.Errorf("%s: table %s row %d diverged:\n want: %s\n got:  %s", leg, name, i, wantRows[i], gotRows[i])
			}
		}
	}
}

func tableNames(m map[string][]string) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}
