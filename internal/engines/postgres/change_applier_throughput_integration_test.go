//go:build integration

// Throughput-comparison integration test: same workload applied
// with batchSize=1 (per-change, the v0.3.x default) vs batchSize=100
// (typical production tuning). Measures wall-clock to surface the
// commit-overhead amortisation. This is not asserted as a strict
// performance bound — CI host load is too variable — but it logs
// the numbers so operators can validate the claim against their
// own hardware.

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestChangeApplier_ApplyBatch_ThroughputComparison runs the same
// 200-row workload twice — once with batch=1, once with batch=100 —
// and logs the wall-clock difference. Skipped under -short.
func TestChangeApplier_ApplyBatch_ThroughputComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput comparison skipped under -short")
	}
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE t1 (
			id    BIGINT PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		CREATE TABLE t100 (
			id    BIGINT PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const totalRows = 200

	// Per-change apply (batchSize=1) — uses the existing Apply path.
	makeEvents := func(table string) []ir.Change {
		out := make([]ir.Change, 0, totalRows)
		for i := int64(1); i <= totalRows; i++ {
			out = append(out, ir.Insert{
				Position: ir.Position{Engine: engineNamePostgres, Token: fmt.Sprintf("token-%d", i)},
				Schema:   "public",
				Table:    table,
				Row:      ir.Row{"id": i, "email": fmt.Sprintf("u%d@x", i)},
			})
		}
		return out
	}

	t.Run("batch=1 (per-change baseline)", func(t *testing.T) {
		applier, err := eng.OpenChangeApplier(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenChangeApplier: %v", err)
		}
		defer func() {
			if c, ok := applier.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}()
		start := time.Now()
		pumpBatchedChanges(t, ctx, applier, makeEvents("t1"), 1)
		t.Logf("batch=1: applied %d rows in %v (%.1f rows/sec)",
			totalRows, time.Since(start), float64(totalRows)/time.Since(start).Seconds())
	})

	t.Run("batch=100 (typical tuning)", func(t *testing.T) {
		applier, err := eng.OpenChangeApplier(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenChangeApplier: %v", err)
		}
		defer func() {
			if c, ok := applier.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}()
		start := time.Now()
		pumpBatchedChanges(t, ctx, applier, makeEvents("t100"), 100)
		t.Logf("batch=100: applied %d rows in %v (%.1f rows/sec)",
			totalRows, time.Since(start), float64(totalRows)/time.Since(start).Seconds())
	})
}
