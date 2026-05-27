//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MySQL container-boot retry wrapper for pipeline-package integration
// tests. Task #63: complements task #60's retry on the shared TestMain
// in internal/engines/mysql/shared_container_integration_test.go.
//
// The pipeline package has 7 per-test startMySQL* helpers, each booting
// its own mysql:8.0 container via mysqltc.Run. On PR #54's first CI run
// the pipeline-migrate and pipeline-rest-streamer shards both failed
// with the same wait-until-ready flake the engines-mysql shard was
// already protected against: `container exited with code 1` and
// `port: 3306 MySQL Community Server matched 0 times, expected 1`,
// followed by `context deadline exceeded`. A rerun was clean.
//
// Retry shape mirrors task #60: 3 attempts with 30s/60s backoff
// between, per-attempt budget of 4 minutes. Bumped from 2 minutes
// in task #69 after CI logs showed every failed boot attempt was
// hitting the 2-minute testcontainers wait-until-ready deadline on
// the self-hosted runner pool — successful attempts in the same
// runs took ~50s but slow attempts could exceed 2min under disk-I/O
// contention from concurrent shards. 4min gives load-spike headroom
// without unbounded budget growth. Worst-case wall time:
//
//	3 * 4min + 30s + 60s = ~13.5min
//
// well under the CI shard timeout (75min go-test inner). Callers
// replace the previous pattern
//
//	testcontainers.SkipIfProviderIsNotHealthy(t)
//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
//	defer cancel()
//	container, err := mysqltc.Run(ctx, "mysql:8.0", opts...)
//	if err != nil { t.Fatalf("start container: %v", err) }
//
// with
//
//	container := runMySQLWithRetry(t, opts...)
//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
//	defer cancel()
//
// The new ctx is for post-boot setup (ConnectionString, CREATE DATABASE,
// etc.); it is no longer used for the boot itself.

package pipeline

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// mysqlBootAttempts: total per-helper attempts. Kept at 3 (NOT
	// raised in sync with engines/mysql.sharedMySQLBootAttempts =
	// 5) because pipeline-package helpers are *per-test* boots —
	// the cost multiplies by the test count in the shard. PR #62's
	// initial CI run captured a slow-runner case where bumping the
	// per-test cap to 5 attempts had multiple pipeline-* shards
	// hit the 75-minute go-test-binary timeout: each failing test
	// burned ~13 min of retry + backoff (worst-case 5 * 2min + 30s
	// + 60s + 120s + 240s ≈ 17.5 min) instead of ~7.5 min at 3
	// attempts. The shared TestMain in engines/mysql boots once
	// per shard, so its 5-attempt budget adds at most ~10 min to
	// the shard's wall-time — single boot, no multiplier. The
	// per-test wrappers here multiply by ~20 tests, so the 3-attempt
	// cap is the right setting for this layer.
	mysqlBootAttempts = 3
	mysqlBootTimeout  = 4 * time.Minute
)

// mysqlBootBackoff returns the sleep duration between a failed boot
// attempt and the next one. attempt is 1-indexed and refers to the
// attempt that JUST failed. Schedule: 30s, 60s. The default branch
// (120s) is defensive — never reached at 3 attempts but kept for
// the case where a future raise of mysqlBootAttempts past 3 reuses
// this function.
func mysqlBootBackoff(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 30 * time.Second
	case 2:
		return 60 * time.Second
	default:
		return 120 * time.Second
	}
}

// runMySQLWithRetry boots a mysql:8.0 testcontainer with the given
// customisers, retrying on transient wait-until-ready failures. Calls
// testcontainers.SkipIfProviderIsNotHealthy internally so callers don't
// need to; t.Fatalf on the final exhaustion mirrors the prior
// single-shot helpers' error path.
func runMySQLWithRetry(t *testing.T, opts ...testcontainers.ContainerCustomizer) *mysqltc.MySQLContainer {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	// Prepend a wait-strategy override BEFORE caller opts so testcontainers'
	// internal `wait until ready` deadline matches our per-attempt budget.
	// Without this, the testcontainers mysql module defaults to a 60-second
	// startup-timeout on its "port: 3306  MySQL Community Server" log-wait,
	// which fires before our outer mysqlBootTimeout under self-hosted runner
	// disk-I/O contention (task #69 follow-up: CI logs showed 3 attempts ×
	// ~100s each = ~300s wall-time exhaustion, not the 3 × 4min the outer
	// budget allowed for — the inner 60s wait was the binding constraint).
	// Prepending means a caller can still override if a specific test needs
	// a different wait shape.
	waitOpts := append(
		[]testcontainers.ContainerCustomizer{
			testcontainers.WithWaitStrategy(
				wait.ForLog("port: 3306  MySQL Community Server").
					WithStartupTimeout(mysqlBootTimeout),
			),
		},
		opts...,
	)

	var lastErr error
	for attempt := 1; attempt <= mysqlBootAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), mysqlBootTimeout)
		container, err := mysqltc.Run(ctx, "mysql:8.0", waitOpts...)
		cancel()
		if err == nil {
			if attempt > 1 {
				log.Printf("pipeline: mysql boot attempt %d/%d succeeded", attempt, mysqlBootAttempts)
			}
			return container
		}
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		lastErr = err
		if attempt < mysqlBootAttempts {
			backoff := mysqlBootBackoff(attempt)
			log.Printf("pipeline: mysql boot attempt %d/%d failed: %v; retrying in %s",
				attempt, mysqlBootAttempts, err, backoff)
			time.Sleep(backoff)
			continue
		}
		log.Printf("pipeline: mysql boot attempt %d/%d failed: %v; giving up",
			attempt, mysqlBootAttempts, err)
	}
	t.Fatalf("start container: %d attempts exhausted: %v", mysqlBootAttempts, lastErr)
	return nil
}
