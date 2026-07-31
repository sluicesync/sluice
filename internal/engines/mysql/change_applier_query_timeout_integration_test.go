//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the sync-side ADR-0182 query-timeout raise
// crash-recovery record (item 111 phase 3): the MySQL ChangeApplier's dedicated
// sluice_cdc_query_timeout_raise table, keyed by stream_id. Persisted state is a
// codec — this pins the round-trip against a REAL MySQL server (a reader that is
// not the writer's own encoder): Record→Read value-exact, the EMPTY-previous
// case (raised-at-default) distinct from "no record", Clear→Read ok=false, and
// missing-table / missing-row both reading ok=false without error.

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestApplierQueryTimeoutRaise_RoundTrip(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	recorder, ok := applier.(ir.QueryTimeoutRaiseRecorder)
	if !ok {
		t.Fatal("MySQL ChangeApplier does not implement ir.QueryTimeoutRaiseRecorder")
	}

	const streamID = "qt-raise-sync"

	// Missing TABLE (before EnsureControlTable): read tolerates it as ok=false,
	// never an error. Clear on a missing table is a no-op, not an error.
	if _, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, streamID); err != nil || dangling {
		t.Fatalf("ReadQueryTimeoutRaise(missing table) = (_, %v, %v); want (_, false, nil)", dangling, err)
	}
	if err := recorder.ClearQueryTimeoutRaise(ctx, streamID); err != nil {
		t.Fatalf("ClearQueryTimeoutRaise(missing table): %v", err)
	}

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	// Missing ROW (table exists, nothing recorded): ok=false, no error.
	if _, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, streamID); err != nil || dangling {
		t.Fatalf("ReadQueryTimeoutRaise(missing row) = (_, %v, %v); want (_, false, nil)", dangling, err)
	}

	// Record a raise carrying a prior custom value. Unlike the migrate recorder,
	// NO row need pre-exist — the dedicated table upserts freely.
	if err := recorder.RecordQueryTimeoutRaise(ctx, streamID, "1200"); err != nil {
		t.Fatalf("RecordQueryTimeoutRaise: %v", err)
	}
	if prev, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, streamID); err != nil || !dangling || prev != "1200" {
		t.Fatalf("ReadQueryTimeoutRaise = (%q, %v, %v); want (1200, true, nil)", prev, dangling, err)
	}

	// Re-record (upsert onto the existing row) overwrites the previous value.
	if err := recorder.RecordQueryTimeoutRaise(ctx, streamID, "1800"); err != nil {
		t.Fatalf("RecordQueryTimeoutRaise(upsert): %v", err)
	}
	if prev, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, streamID); err != nil || !dangling || prev != "1800" {
		t.Fatalf("ReadQueryTimeoutRaise(after upsert) = (%q, %v, %v); want (1800, true, nil)", prev, dangling, err)
	}

	// An EMPTY previous is meaningful (the keyspace was at its default when we
	// raised it): it must read back dangling with an empty previous, distinct
	// from "no record".
	if err := recorder.RecordQueryTimeoutRaise(ctx, streamID, ""); err != nil {
		t.Fatalf("RecordQueryTimeoutRaise(empty): %v", err)
	}
	if prev, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, streamID); err != nil || !dangling || prev != "" {
		t.Fatalf("ReadQueryTimeoutRaise(empty) = (%q, %v, %v); want (\"\", true, nil)", prev, dangling, err)
	}

	// Clear on revert removes the row → ok=false.
	if err := recorder.ClearQueryTimeoutRaise(ctx, streamID); err != nil {
		t.Fatalf("ClearQueryTimeoutRaise: %v", err)
	}
	if _, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, streamID); err != nil || dangling {
		t.Fatalf("after Clear, raise = (_, %v, %v); want (_, false, nil)", dangling, err)
	}
	// Clear is idempotent.
	if err := recorder.ClearQueryTimeoutRaise(ctx, streamID); err != nil {
		t.Fatalf("second ClearQueryTimeoutRaise: %v", err)
	}

	// Two streams keyed independently: a raise on one is invisible to the other.
	const otherStream = "qt-raise-sync-2"
	if err := recorder.RecordQueryTimeoutRaise(ctx, streamID, "900"); err != nil {
		t.Fatalf("RecordQueryTimeoutRaise(stream 1): %v", err)
	}
	if _, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, otherStream); err != nil || dangling {
		t.Fatalf("ReadQueryTimeoutRaise(other stream) = (_, %v, %v); want (_, false, nil) — records must be per-stream", dangling, err)
	}
}
