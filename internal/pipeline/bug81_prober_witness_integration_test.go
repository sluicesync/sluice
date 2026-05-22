//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// Bug 81 compile-time witness: opens a real PG / MySQL writer via
// the engine registry and asserts the returned ir.RowWriter satisfies
// shardPreflightProber. v0.72.0 shipped the interface with no engine
// implementation — the type assertion at shard_preflight.go:136
// silently failed and the three-point preflight was a no-op. v0.72.1
// adds the implementations; this test pins them so a future regression
// shows up as a test failure rather than as a runtime no-op.
//
// The integration-tag is required because we need a real engine + DSN
// to open a RowWriter; the interface isn't usefully testable in
// isolation (it's deliberately unexported in the pipeline package).

func TestBug81_PGRowWriterImplementsShardPreflightProber(t *testing.T) {
	_, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rw, err := pgEng.OpenRowWriter(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer func() {
		if c, isC := rw.(interface{ Close() error }); isC {
			_ = c.Close()
		}
	}()

	if _, ok := rw.(shardPreflightProber); !ok {
		t.Fatal("PG RowWriter does NOT implement shardPreflightProber — Bug 81 regression " +
			"(v0.72.0 shipped the interface with no engine implementation; the ADR-0048 DP-2 " +
			"three-point preflight silently no-op'd because of this)")
	}
}

func TestBug81_MySQLRowWriterImplementsShardPreflightProber(t *testing.T) {
	_, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rw, err := myEng.OpenRowWriter(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer func() {
		if c, isC := rw.(interface{ Close() error }); isC {
			_ = c.Close()
		}
	}()

	if _, ok := rw.(shardPreflightProber); !ok {
		t.Fatal("MySQL RowWriter does NOT implement shardPreflightProber — Bug 81 regression")
	}
}
