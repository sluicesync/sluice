//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Extension-type pins for the ADR-0092 pipelined CDC apply path.
//
// pgvector (`vector`) and hstore are ADR-0032 extension types: the
// applier translates them to ir.ExtensionType and prepareValue passes
// the canonical text form straight through. Under the pipelined pool's
// QueryExecModeDescribeExec, pgx describes the parameter against the
// extension's runtime OID; with no client-registered binary codec for
// that OID it auto-selects TEXT format and ships the string — byte-for-
// byte the same wire shape the serial CacheStatement path sends (the
// applier registers no extension codec on EITHER path). These pins prove
// that end-to-end through the pipelined batch, src==dst on the real
// target, because the reviewer flagged these families as not previously
// verified through the pipelined path.
//
// They boot the pre-baked pgvector image (a postgres:16 superset that
// also bundles hstore) rather than the shared container, so they pay a
// per-test container boot — acceptable for two extension pins. CI's main
// Integration shard already pre-pulls this image.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"

	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// pipelinedExtImage is the pre-baked pgvector image (postgres:16 superset)
// — it carries both the `vector` extension and the bundled `hstore`
// contrib module, so one container covers both extension pins.
const pipelinedExtImage = "ghcr.io/sluicesync/sluice-pgvector:0.7.4-pg16-prebaked"

// startPGForPipelinedExt boots a pgvector-capable container, enables the
// named extensions, and returns a DSN + cleanup. Skips cleanly when no
// Docker provider is available.
func startPGForPipelinedExt(t *testing.T, extensions ...string) (dsn string, cleanup func()) {
	t.Helper()

	// runPGWithRetry appends the single-occurrence wait strategy the
	// pre-baked image needs (its datadir is already initialized, so it
	// logs "ready to accept connections" once, not twice) and handles the
	// Docker-provider skip + boot retry. The pgvector bake seeds source_db.
	container := runPGWithRetry(
		t, pipelinedExtImage,
		pgtc.WithDatabase("source_db"),
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
	for _, ext := range extensions {
		if _, err := db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS "+ext); err != nil {
			terminate()
			t.Fatalf("CREATE EXTENSION %s: %v", ext, err)
		}
	}
	return conn, terminate
}

// TestPipelined_PGVector_EndToEnd pins a pgvector `vector(3)` column
// applied through the pipelined batch, src==dst ground-truthed via
// ::text on the real target.
func TestPipelined_PGVector_EndToEnd(t *testing.T) {
	dsn, cleanup := startPGForPipelinedExt(t, "vector")
	defer cleanup()

	applyPGApplier(t, dsn, `CREATE TABLE v (id BIGINT PRIMARY KEY, embedding vector(3));`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()

	events := []ir.Change{
		ir.Insert{Position: ir.Position{Engine: engineNamePostgres, Token: "v1"}, Schema: "public", Table: "v", Row: ir.Row{"id": int64(1), "embedding": "[0.1,0.2,0.3]"}},
	}
	pumpBatchedChanges(t, ctx, applier, events, 100)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var got string
	if err := db.QueryRowContext(ctx, "SELECT embedding::text FROM v WHERE id = 1").Scan(&got); err != nil {
		t.Fatalf("verify vector: %v", err)
	}
	if want := "[0.1,0.2,0.3]"; got != want {
		t.Errorf("embedding ::text = %q; want %q (pgvector through pipelined DescribeExec)", got, want)
	}
}

// TestPipelined_Hstore_EndToEnd pins an hstore column applied through the
// pipelined batch. hstore re-canonicalizes pair order on output, so the
// pin asserts on the parsed map (via the hstore -> jsonb cast's sorted
// keys) rather than the raw ::text spelling.
func TestPipelined_Hstore_EndToEnd(t *testing.T) {
	dsn, cleanup := startPGForPipelinedExt(t, "hstore")
	defer cleanup()

	applyPGApplier(t, dsn, `CREATE TABLE h (id BIGINT PRIMARY KEY, attrs hstore);`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()

	events := []ir.Change{
		ir.Insert{Position: ir.Position{Engine: engineNamePostgres, Token: "h1"}, Schema: "public", Table: "h", Row: ir.Row{"id": int64(1), "attrs": `"a"=>"1", "b"=>"2"`}},
	}
	pumpBatchedChanges(t, ctx, applier, events, 100)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// hstore::jsonb sorts keys deterministically, so this is a stable
	// ground-truth for "the two pairs landed with the right values".
	var got string
	if err := db.QueryRowContext(ctx, "SELECT (attrs::jsonb)::text FROM h WHERE id = 1").Scan(&got); err != nil {
		t.Fatalf("verify hstore: %v", err)
	}
	if want := `{"a": "1", "b": "2"}`; got != want {
		t.Errorf("attrs as jsonb = %q; want %q (hstore through pipelined DescribeExec)", got, want)
	}
}
