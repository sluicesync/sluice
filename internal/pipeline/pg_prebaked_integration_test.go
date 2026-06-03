//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pipeline-package pre-baked Postgres image constants + the
// single-occurrence wait-strategy override (task #68).
//
// The shared TestMain in internal/engines/postgres has its own copy
// of the image constant (sharedPGImage); this package's per-test
// helpers can't import that (it's a test-only symbol in a sibling
// package), so the constant is re-declared here. Both must move
// together when the base version changes — see docs/dev/ci-images.md.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// pgPrebakedImage is the task-#68 pre-baked postgres:16 image —
// byte-equivalent to upstream postgres:16 except /var/lib/postgresql/
// data has initdb already run + the `test` superuser and seed
// databases (source_db, warehouse, sluice_shared_seed) already
// created. Cold-start drops from 30-60s (under CI disk-I/O
// contention, up to 2-3 min) to ~5s. See docs/dev/ci-images.md.
const pgPrebakedImage = "ghcr.io/sluicesync/sluice-postgres:16-prebaked"

// pgPrebakedBootTimeout is the per-attempt budget passed to the
// wait-strategy override below. Generous because the pre-baked
// path still has to pay container creation + network setup +
// process start time; ~10-15s is typical, 60s gives headroom.
const pgPrebakedBootTimeout = 60 * time.Second

// pgPrebakedWaitStrategy returns the wait-strategy customizer
// callers append to their pgtc.Run opts so the pre-baked image's
// "init done once, server starts once" log pattern is matched.
//
// Why this exists separately from pgtc.BasicWaitStrategies:
//
//	BasicWaitStrategies waits for "ready to accept connections" with
//	`.WithOccurrence(2)` because the upstream postgres image's
//	docker-entrypoint.sh runs initdb, starts the server temporarily
//	to apply env-var-driven init (CREATE USER / CREATE DATABASE),
//	stops it, then restarts for real — logging the readiness line
//	twice. The pre-baked image's datadir is already initialized so
//	the entrypoint skips the init-then-restart cycle; the log line
//	appears only once. WithOccurrence(2) would hang.
//
// Callers should APPEND this customizer to their existing opts
// (after pgtc.BasicWaitStrategies if present) so it replaces the
// inner strategy at testcontainers' wait.ForAll outer.
func pgPrebakedWaitStrategy() testcontainers.CustomizeRequestOption {
	return testcontainers.WithWaitStrategyAndDeadline(
		pgPrebakedBootTimeout,
		wait.ForAll(
			wait.ForLog("database system is ready to accept connections"),
			wait.ForListeningPort("5432/tcp"),
		),
	)
}

// pgPrebakedEnsureDB connects to the pre-baked container's `postgres`
// admin database (which the bake always seeds) and runs
// `CREATE DATABASE IF NOT EXISTS dbName` so subsequent test connections
// against the test-specific dbName succeed.
//
// Why this exists: on the upstream postgres image,
// pgtc.WithDatabase(dbName) sets POSTGRES_DB which the entrypoint
// reads at init time to CREATE DATABASE dbName. On the pre-baked
// image the entrypoint skips init (the datadir is already
// initialized), so POSTGRES_DB is ignored. For test-specific dbNames
// not in the bake's seed list (source_db, warehouse, sluice_shared_seed)
// the caller has to create the db itself.
//
// Idempotent: PG's CREATE DATABASE IF NOT EXISTS doesn't exist as
// such — we use a DO block that checks pg_database first. Safe to
// call multiple times on the same container.
func pgPrebakedEnsureDB(ctx context.Context, container *pgtc.PostgresContainer, dbName string) error {
	// Connect to the admin db (the bake always has `postgres`
	// existing as the bootstrap db) regardless of what dbName the
	// container was created with.
	host, err := container.Host(ctx)
	if err != nil {
		return fmt.Errorf("container.Host: %w", err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return fmt.Errorf("container.MappedPort: %w", err)
	}
	adminDSN := fmt.Sprintf("postgres://test:test@%s:%s/postgres?sslmode=disable",
		host, port.Port())

	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return fmt.Errorf("open admin db: %w", err)
	}
	defer func() { _ = db.Close() }()

	// `CREATE DATABASE` can't run in a DO block (must be top-level),
	// so check existence first and conditionally create.
	var exists bool
	if err := db.QueryRowContext(
		ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, dbName,
	).Scan(&exists); err != nil {
		return fmt.Errorf("check db existence: %w", err)
	}
	if exists {
		return nil
	}
	// Bare-bones CREATE DATABASE — owner defaults to the connecting
	// role (`test`, superuser). Quoting the identifier handles dbNames
	// with special characters; the bare format string is safe because
	// dbName comes from test code, not external input.
	if _, err := db.ExecContext(
		ctx,
		fmt.Sprintf(`CREATE DATABASE %q OWNER test`, dbName),
	); err != nil {
		return fmt.Errorf("create db %q: %w", dbName, err)
	}
	return nil
}
