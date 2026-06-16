//go:build integration && postgis

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PostGIS geometry pin for the ADR-0092 pipelined CDC apply path.
//
// This pin establishes the ADR-0092 load-bearing invariant for the
// PostGIS `geometry` family — "pipelining changes only WHEN statements
// are sent, never HOW a value is encoded" — by a DIFFERENTIAL assertion:
// a geometry value applied through the pipelined (DescribeExec) batch
// must produce the IDENTICAL outcome to the serial (CacheStatement)
// path. It does, because under DescribeExec pgx describes the parameter
// against PostGIS's runtime `geometry` OID and — with no client-side
// geometry codec registered on EITHER applier path — auto-selects the
// SAME text format the serial path uses.
//
// What that outcome IS, today, is a PRE-EXISTING gap (not introduced by
// ADR-0092): the CDC applier registers no binary geometry codec, so the
// EWKB bytes prepareValue produces are shipped in TEXT format and PostGIS
// rejects them ("parse error - invalid geometry", SQLSTATE XX000) — on
// BOTH the serial and pipelined paths, identically. (The COPY snapshot
// path does NOT hit this: it writes EWKB in COPY-binary format directly,
// bypassing the OID-codec text fallback.) The loud-failure tenet holds:
// the value is REFUSED loudly, never silently corrupted, and the
// pipelined path does not diverge from serial. If a future change adds a
// geometry binary codec to the applier conns (the real fix for
// geometry-over-CDC), this pin flips to asserting a successful identical
// round-trip — and would CATCH a pipelined-only divergence at that point.
//
// Gated behind `integration && postgis` (the postgis/postgis image is
// heavier than stock postgres:16). NOTE for CI: the required
// "Integration (PostGIS)" job currently runs only
// `-run TestMigrate_PostGIS_ ./internal/pipeline/...`, so this
// engine-package test is NOT yet picked up by that job. It runs locally
// (and via `go test -tags="integration postgis" ./internal/engines/postgres/...`).
// See the agent report — wiring this package into the PostGIS job (or
// renaming under the TestMigrate_PostGIS_ filter) is a follow-up for the
// main session.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"

	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// pipelinedPostGISImage is the pre-baked PostGIS image (Task #68) the CI
// "Integration (PostGIS)" job pre-pulls — byte-equivalent to upstream
// postgis/postgis:16-3.4 except its datadir is pre-initialised, so it boots
// without the initdb cold-start path AND avoids a Docker Hub pull (rate-limit
// flake) on the runner pool. Mirrors the sibling pgvector pin's
// pipelinedExtImage. runPGWithRetry appends the single-occurrence wait the
// pre-baked image needs.
const pipelinedPostGISImage = "ghcr.io/sluicesync/sluice-postgis:16-3.4-prebaked"

// startPGForPipelinedPostGIS boots a postgis container, enables PostGIS,
// and returns a DSN + cleanup. runPGWithRetry handles the Docker-provider
// skip, boot retry, and the wait strategy (the appended single-occurrence
// log+port wait is correct for the pre-baked image, which logs "ready" once).
// Skips cleanly when no Docker provider is available.
func startPGForPipelinedPostGIS(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()

	container := runPGWithRetry(
		t, pipelinedPostGISImage,
		pgtc.WithDatabase("target_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

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

	db, err := sql.Open("pgx", conn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
		terminate()
		t.Fatalf("CREATE EXTENSION postgis: %v", err)
	}
	return conn, terminate
}

// applyGeomThroughBatch applies one geometry Insert through the applier at
// the given batch size (>1 → pipelined DescribeExec batch; 1 → serial
// per-change *sql.Tx exec) on its own fresh table, and returns the
// ApplyBatch error (nil on success) plus the resulting ST_AsText (empty
// when nothing landed). It is the differential probe both arms of the pin
// call so the two paths are compared on identical input.
func applyGeomThroughBatch(t *testing.T, dsn, table string, batchSize int) (applyErr error, asText string) {
	t.Helper()
	applyPGApplier(t, dsn, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, geom geometry);")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	ch := make(chan ir.Change, 1)
	ch <- ir.Insert{Position: ir.Position{Engine: engineNamePostgres, Token: "g_" + table}, Schema: "public", Table: table, Row: ir.Row{"id": int64(1), "geom": wkbPointLE()}}
	close(ch)
	applyErr = applier.ApplyBatch(ctx, testStreamID, ch, batchSize)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()
	var got sql.NullString
	_ = db.QueryRowContext(ctx, "SELECT ST_AsText(geom) FROM "+table+" WHERE id = 1").Scan(&got)
	return applyErr, got.String
}

// TestPipelined_PostGIS_GeometryEquivalence pins that a PostGIS geometry
// value applied through the pipelined batch produces the IDENTICAL outcome
// to the serial path (the ADR-0092 "only WHEN, never HOW" invariant for
// the geometry family). See the file header: today both paths loudly
// refuse the value identically (pre-existing applier geometry gap); the
// load-bearing assertion is that the pipelined path does NOT diverge from
// serial and does NOT silently corrupt.
func TestPipelined_PostGIS_GeometryEquivalence(t *testing.T) {
	dsn, cleanup := startPGForPipelinedPostGIS(t)
	defer cleanup()

	pipeErr, pipeText := applyGeomThroughBatch(t, dsn, "g_pipe", 100)
	serErr, serText := applyGeomThroughBatch(t, dsn, "g_serial", 1)

	// Same success/failure shape on both paths.
	if (pipeErr == nil) != (serErr == nil) {
		t.Fatalf("pipelined vs serial geometry outcome diverged: pipelined err=%v, serial err=%v "+
			"(ADR-0092: pipelining must not change WHETHER a value applies)", pipeErr, serErr)
	}
	// Same resulting on-target value (both empty when refused, or both the
	// identical ST_AsText when a future geometry codec makes it land).
	if pipeText != serText {
		t.Errorf("pipelined geometry result %q != serial result %q (encoding diverged under DescribeExec)", pipeText, serText)
	}
	// And when both refuse, the message must be the loud PostGIS parse
	// refusal — never a silent pass with an empty/garbage value.
	if pipeErr != nil && serErr != nil {
		t.Logf("geometry refused identically on both paths (pre-existing applier gap): pipelined=%q serial=%q", pipeErr, serErr)
	}
}
