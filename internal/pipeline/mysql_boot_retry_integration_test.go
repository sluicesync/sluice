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
// between, per-attempt budget of 2 minutes (matches the original
// single-shot timeout). Worst-case wall time:
//
//	3 * 2min + 30s + 60s = ~7.5min
//
// well under the CI shard timeout. Callers replace the previous
// pattern
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
)

const (
	// mysqlBootAttempts: total per-helper attempts. Bumped 3 → 5 by
	// task #12 Phase B; see engines/mysql/shared_container_integration_test.go
	// for the rationale (two captured runs where 3 attempts exhausted
	// under runner load). Worst-case wall time: 5 * 2min + 30s + 60s
	// + 120s + 240s = ~17.5 min, still under CI shard timeout.
	mysqlBootAttempts = 5
	mysqlBootTimeout  = 2 * time.Minute
)

// mysqlBootBackoff returns the sleep duration between a failed boot
// attempt and the next one. attempt is 1-indexed and refers to the
// attempt that JUST failed. Schedule mirrors
// engines/mysql.sharedMySQLBootBackoff: 30s, 60s, 120s, 240s.
func mysqlBootBackoff(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 30 * time.Second
	case 2:
		return 60 * time.Second
	case 3:
		return 120 * time.Second
	case 4:
		return 240 * time.Second
	default:
		return 480 * time.Second
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

	var lastErr error
	for attempt := 1; attempt <= mysqlBootAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), mysqlBootTimeout)
		container, err := mysqltc.Run(ctx, "mysql:8.0", opts...)
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
