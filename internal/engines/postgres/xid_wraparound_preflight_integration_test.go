//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the Postgres XID-wraparound preflight prober
// (pgcopydb PR #17 adoption). Boots a real PG container and asserts the
// prober reads the catalog truth: a freshly-initialised database has a
// small `age(datfrozenxid)` (well below the orchestrator's refuse
// threshold) and the prober returns it without error along with the
// connecting database's name.
//
// The orchestrator-side gate + threshold + refusal-formatting (incl.
// the KEY behavior difference vs the REPLICATION preflight — XID
// fires for BOTH `postgres` and `postgres-trigger` sources) is
// unit-tested with a stub in
// internal/pipeline/xid_wraparound_preflight_test.go; this file
// ground-truths the engine-side probe the gate rides on so a SQL
// fat-finger or a PG-version drift around `pg_database` / `age()`
// surfaces here loudly.

package postgres

import (
	"context"
	"testing"
	"time"
)

// TestSourceXIDWraparoundHorizon_FreshContainerHealthyAge confirms the
// prober's positive control: a fresh PG container has a tiny age and
// the query returns the connecting database's name. The threshold the
// orchestrator gates on is 1.5B; a fresh container is in the thousands
// at most — many orders of magnitude clear.
func TestSourceXIDWraparoundHorizon_FreshContainerHealthyAge(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := (Engine{}).OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// The probe surface is optional — assert SchemaReader implements it
	// (regression of the contract this PR introduces).
	prober, ok := sr.(interface {
		SourceXIDWraparoundHorizon(ctx context.Context) (age int64, datname string, err error)
	})
	if !ok {
		t.Fatal("postgres SchemaReader does not expose SourceXIDWraparoundHorizon (regression of the prober surface introduced for the pipeline.xidWraparoundProber gate)")
	}

	age, datname, err := prober.SourceXIDWraparoundHorizon(ctx)
	if err != nil {
		t.Fatalf("SourceXIDWraparoundHorizon: %v", err)
	}
	if datname == "" {
		t.Errorf("datname is empty; want the connecting db's name (the orchestrator surfaces this in the refusal message)")
	}
	if age < 0 {
		t.Errorf("age = %d; want a non-negative value (age() should not return negative under any circumstances)", age)
	}
	// Be generous — the bound below is orders of magnitude under the
	// 1.5B refuse threshold even for an already-active test container.
	const sanityCeiling = int64(50_000_000) // 50M — autovacuum_freeze_max_age default is 200M
	if age >= sanityCeiling {
		t.Errorf("age = %d; want < %d on a fresh container (would the test container have churned that many transactions?)",
			age, sanityCeiling)
	}
}
