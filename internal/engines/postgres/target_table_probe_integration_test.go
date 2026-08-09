//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_TargetTableExists is the INDEPENDENT binding for the
// unmapped-table preflight (audit backlog C-11).
//
// internal/pipeline pins the preflight's decision against a stub applier that
// answers the probe by construction, so those tests prove the logic and nothing
// about any engine. This asserts a real Postgres, through the real applier,
// answers the same question the apply path asks — which is the property the
// whole preflight rests on: it must predict the runtime's behaviour, not merely
// have an opinion.
//
// The absent case is the load-bearing one. Postgres is where the silent skip
// lived: an unresolvable table meant the applier WARNed and dropped every
// change for it, for the life of the stream, at exit 0.
func TestChangeApplier_TargetTableExists(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE present (id BIGINT PRIMARY KEY, v TEXT);
		CREATE SCHEMA other;
		CREATE TABLE other.scoped (id BIGINT PRIMARY KEY);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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

	probe, ok := applier.(ir.TargetTableProbe)
	if !ok {
		t.Fatal("postgres ChangeApplier no longer implements ir.TargetTableProbe — " +
			"the unmapped-table preflight silently stops running for this engine")
	}

	cases := []struct {
		name          string
		schema, table string
		want          bool
	}{
		{"a table that exists", "public", "present", true},
		{"a table that does NOT exist — the whole point", "public", "absent", false},
		{"a table in another schema", "other", "scoped", true},
		{"a table in a schema that does not exist at all", "nosuch", "whatever", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := probe.TargetTableExists(ctx, tc.schema, tc.table)
			if err != nil {
				t.Fatalf("TargetTableExists(%s.%s): %v", tc.schema, tc.table, err)
			}
			if got != tc.want {
				t.Errorf("TargetTableExists(%s.%s) = %v; want %v", tc.schema, tc.table, got, tc.want)
			}
		})
	}
}
