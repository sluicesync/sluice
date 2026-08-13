//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The real-PostGIS premise leg of the SPAT-4 refusal (audit 2026-08-11).
//
// The §14 spatial-column refusal keys on pg_type.typname — the same key
// PostGIS's own geometry_columns view joins on and the postgres reader's
// udt_name dispatch uses. This test binds that premise to the real
// extension: PostGIS's actual geometry/geography types, at every
// wrapping the capture can meet (direct column, array element, DOMAIN
// over geometry — the wrapping the unrecognised-domain refusal does NOT
// catch, because geometry's typtype is 'b'), must each refuse setup up
// front. Before the refusal existed, setup + cold copy succeeded on
// these shapes and the FIRST spatial DML wedged the stream mid-incident
// (to_jsonb renders a spatial value as a GeoJSON object the shared
// apply path cannot decode).
//
// The negative half runs in the same test: an ordinary table in the
// same PostGIS-enabled database must still be accepted — a refusal
// keyed on "PostGIS is installed" rather than "this table has a spatial
// column" would satisfy the positive assertions while breaking every
// vanilla table on a Supabase-shaped host, exactly the audience this
// engine exists for.
//
// Naming/tagging contract (scripts/check-run-filter-coverage.sh): every
// test in a postgis-tagged file carries a `PostGIS_` name segment and
// lives in a package the CI "Integration (PostGIS)" job's path list
// covers — this file is why that list includes internal/engines/pgtrigger.
package pgtrigger

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPostGISForTrigger boots the pre-baked PostGIS image the CI job
// pre-pulls (byte-equivalent to postgis/postgis:16-3.4 with the datadir
// pre-initialised) and enables the extension. Mirrors startPGForTrigger's
// contract; the appended single-occurrence log+port wait is the strategy
// the pre-baked image needs (it logs "ready" once — no initdb restart).
func startPostGISForTrigger(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := pgtc.Run(
		ctx,
		"ghcr.io/sluicesync/sluice-postgis:16-3.4-prebaked",
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		testcontainers.WithWaitStrategyAndDeadline(
			3*time.Minute,
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections"),
				wait.ForListeningPort("5432/tcp"),
			),
		),
	)
	if err != nil {
		t.Fatalf("start postgis container: %v", err)
	}
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	conn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}
	return conn, terminate
}

func TestSetup_PostGIS_RefusesSpatialColumns(t *testing.T) {
	dsn, cleanup := startPostGISForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE EXTENSION IF NOT EXISTS postgis;
		CREATE DOMAIN parcel_shape AS geometry(POLYGON, 4326);
		CREATE TABLE sp_geom  (id BIGINT PRIMARY KEY, g geometry(POINT, 4326));
		CREATE TABLE sp_geog  (id BIGINT PRIMARY KEY, g geography(POINT, 4326));
		CREATE TABLE sp_arr   (id BIGINT PRIMARY KEY, gs geometry[]);
		CREATE TABLE sp_dom   (id BIGINT PRIMARY KEY, shape parcel_shape);
		CREATE TABLE sp_plain (id BIGINT PRIMARY KEY, name TEXT, tags TEXT[], doc JSONB);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for _, table := range []string{"sp_geom", "sp_geog", "sp_arr", "sp_dom"} {
		plan, err := Setup(ctx, dsn, SetupOptions{Tables: []string{table}, Schema: "public"})
		if err == nil {
			t.Fatalf("Setup(%s): expected the SPAT-4 refusal; got nil err (plan=%+v) — "+
				"the first spatial DML after cold start would wedge the stream", table, plan)
		}
		var sawSpatial bool
		for _, r := range plan.Refusals {
			if r.Reason == "postgis-spatial-column" {
				sawSpatial = true
				for _, want := range []string{"GeoJSON", "postgres", "--exclude-table"} {
					if !contains(r.Hint, want) {
						t.Errorf("Setup(%s): Refusal.Hint = %q; want contains %q", table, r.Hint, want)
					}
				}
			}
		}
		if !sawSpatial {
			t.Errorf("Setup(%s): Refusals = %+v; want a postgis-spatial-column refusal", table, plan.Refusals)
		}
	}

	// The negative half: PostGIS being installed must not refuse an
	// ordinary table.
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"sp_plain"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup(sp_plain): a non-spatial table in a PostGIS-enabled database must not be refused: %v", err)
	}
}
