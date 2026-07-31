//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0182 query-timeout raise crash-recovery
// record on the MySQL sluice_migrate_state header. The load-bearing property
// is that ordinary phase-transition writes (the generic header upsert) NEVER
// clobber a live raise record — recorded through the dedicated column
// statements, read back exact, and untouched across a Write.

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestMigrationStateQueryTimeoutRaise_RoundTripAndClobberSafety(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	store, err := eng.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	recorder, ok := store.(ir.QueryTimeoutRaiseRecorder)
	if !ok {
		t.Fatal("MySQL MigrationStateStore does not implement ir.QueryTimeoutRaiseRecorder")
	}

	const id = "qt-raise"

	// No header row yet: recording must refuse (the loadOrInitState-first
	// contract), never silently no-op.
	if err := recorder.RecordQueryTimeoutRaise(ctx, id, "1200"); err == nil {
		t.Fatal("RecordQueryTimeoutRaise with no header row must error")
	}
	// Fresh read on an absent row is ok=false, never an error.
	if _, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, id); err != nil || dangling {
		t.Fatalf("ReadQueryTimeoutRaise(absent) = (_, %v, %v); want (_, false, nil)", dangling, err)
	}

	// Establish the header (the pipeline's loadOrInitState shape).
	if err := store.Write(ctx, ir.MigrationState{MigrationID: id, Phase: ir.MigrationPhasePending}); err != nil {
		t.Fatalf("Write header: %v", err)
	}

	// Record a raise carrying a prior custom value.
	if err := recorder.RecordQueryTimeoutRaise(ctx, id, "1200"); err != nil {
		t.Fatalf("RecordQueryTimeoutRaise: %v", err)
	}
	if prev, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, id); err != nil || !dangling || prev != "1200" {
		t.Fatalf("ReadQueryTimeoutRaise = (%q, %v, %v); want (1200, true, nil)", prev, dangling, err)
	}

	// THE load-bearing property: a phase-transition header write must not
	// clobber the raise record (the generic upsert never touches the column).
	if err := store.Write(ctx, ir.MigrationState{MigrationID: id, Phase: ir.MigrationPhaseIndexes}); err != nil {
		t.Fatalf("Write phase transition: %v", err)
	}
	if prev, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, id); err != nil || !dangling || prev != "1200" {
		t.Fatalf("after phase-transition Write, raise = (%q, %v, %v); want (1200, true, nil) — the header upsert clobbered the record", prev, dangling, err)
	}

	// An EMPTY previous is meaningful (the keyspace was at its default): the
	// record must still read back dangling with an empty previous, distinct
	// from "no record".
	if err := recorder.RecordQueryTimeoutRaise(ctx, id, ""); err != nil {
		t.Fatalf("RecordQueryTimeoutRaise(empty): %v", err)
	}
	if prev, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, id); err != nil || !dangling || prev != "" {
		t.Fatalf("ReadQueryTimeoutRaise(empty) = (%q, %v, %v); want (\"\", true, nil)", prev, dangling, err)
	}

	// Clear on revert.
	if err := recorder.ClearQueryTimeoutRaise(ctx, id); err != nil {
		t.Fatalf("ClearQueryTimeoutRaise: %v", err)
	}
	if _, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, id); err != nil || dangling {
		t.Fatalf("after Clear, raise = (_, %v, %v); want (_, false, nil)", dangling, err)
	}
	// Clear is idempotent.
	if err := recorder.ClearQueryTimeoutRaise(ctx, id); err != nil {
		t.Fatalf("second ClearQueryTimeoutRaise: %v", err)
	}
}
