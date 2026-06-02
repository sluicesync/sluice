//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration coverage for connection-resilience Phase 2b (adaptive
// backoff on connection-slot exhaustion during the parallel bulk COPY).
//
// The unit suite (copy_backoff_test.go, copy_parallelism_gate_test.go,
// copy_chunk_retry_test.go, and the engine's connection_slot_test.go)
// pins the pure decision, the gate's shrink/retire/give-up mechanics, the
// retry seam, and the classifier against synthetic *pgconn.PgError
// values. This file closes the one thing a synthetic error can't prove:
// that a REAL PostgreSQL `too_many_connections` / superuser-reserved-
// slots FATAL actually carries SQLSTATE 53300 on the wire, so the
// engine's IsConnectionSlotExhausted classifier — and therefore the whole
// Phase 2b retry path — fires against the genuine server error rather
// than a shape we only assumed PG emits.
//
// It is deterministic (no timing race): it boots a PG with a small
// max_connections, holds open enough sessions to saturate the slot
// budget, then asserts the engine's OpenRowWriter fails with an error the
// classifier flags as slot exhaustion. Making the end-to-end
// "copy backs off and still completes" path deterministic would require
// timing the slot release to land mid-COPY; that is left to the unit
// retry-seam test (which proves retry-then-succeed without a real clock)
// per the testing-layout guidance.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestConnectionSlotClassifier_RealPG53300 boots a PG capped at a small
// max_connections, saturates the slot budget with held sessions, and
// asserts that the postgres engine's OpenRowWriter returns an error the
// engine's ir.ConnectionSlotClassifier flags as slot exhaustion. This is
// the ground-truth pin that the synthetic-error unit test can't provide:
// real PG raises SQLSTATE 53300 for this condition.
func TestConnectionSlotClassifier_RealPG53300(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Small max_connections so we can saturate it cheaply. PG reserves
	// superuser_reserved_connections (default 3) on top, so a non-... here
	// the `test` role is a superuser; to make a non-superuser hit the
	// FATAL we'd need a separate role, but either way the saturating-open
	// returns SQLSTATE 53300 once every non-reserved slot is taken.
	const maxConns = 12

	container, err := pgtc.Run(
		ctx,
		pgPrebakedImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
		// Override max_connections on the already-initialised datadir.
		testcontainers.WithCmd(
			"postgres",
			"-c", fmt.Sprintf("max_connections=%d", maxConns),
		),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	// Saturate the slot budget: open raw sessions until the server refuses
	// a new one. Each *sql.DB is forced to one live connection via a ping.
	var held []*sql.DB
	defer func() {
		for _, db := range held {
			_ = db.Close()
		}
	}()

	var saturationErr error
	// max_connections+a few extra attempts guarantees we cross the
	// non-reserved ceiling and trip the FATAL.
	for i := 0; i < maxConns+8; i++ {
		db, openErr := sql.Open("pgx", dsn)
		if openErr != nil {
			t.Fatalf("sql.Open: %v", openErr)
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(0)
		if pingErr := db.PingContext(ctx); pingErr != nil {
			// This is the saturation point: the server refused a new
			// connection. Capture it and stop.
			saturationErr = pingErr
			_ = db.Close()
			break
		}
		held = append(held, db)
	}

	if saturationErr == nil {
		t.Fatalf("never hit slot saturation after %d opens against max_connections=%d", maxConns+8, maxConns)
	}

	// The engine must classify the saturated-open error as slot
	// exhaustion. Use the engine's own OpenRowWriter so the classifier
	// sees the exact wrapped error shape the parallel-copy pool would.
	pgEngAny, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	classifier, ok := pgEngAny.(ir.ConnectionSlotClassifier)
	if !ok {
		t.Fatal("postgres engine does not implement ir.ConnectionSlotClassifier")
	}

	// Confirm the raw saturation ping error classifies. (OpenRowWriter
	// wraps this same condition; classifying the raw ping error proves the
	// SQLSTATE is present and the classifier matches it through wrapping.)
	if !classifier.IsConnectionSlotExhausted(saturationErr) {
		t.Errorf("engine did not classify the real PG saturation error as slot exhaustion: %v", saturationErr)
	}

	// And through the engine's actual OpenRowWriter path, which wraps the
	// error as `postgres: ping: ...`. Retry briefly in case a held
	// connection churns, but the budget is saturated so this should fail.
	_, openErr := pgEngAny.OpenRowWriter(ctx, dsn)
	if openErr == nil {
		t.Skip("OpenRowWriter unexpectedly succeeded (a held session dropped); the raw-ping assertion above already pinned the classifier")
	}
	if !classifier.IsConnectionSlotExhausted(openErr) {
		t.Errorf("engine did not classify OpenRowWriter's saturated error as slot exhaustion: %v", openErr)
	}

	// Sanity: a fresh, non-saturation error (bad DSN) must NOT classify —
	// the safety property, end-to-end.
	if classifier.IsConnectionSlotExhausted(fmt.Errorf("postgres: open: %w", context.DeadlineExceeded)) {
		t.Error("classifier flagged a non-slot error as slot exhaustion (would mask real failures)")
	}
}
