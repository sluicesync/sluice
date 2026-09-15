//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
)

// TestStreamer_MultiSchema_PG_TargetRLSRefusedBeforeTheCopy is the real-
// server pin for the multi-namespace half of audit 2026-09-15
// A0915-ARCH-MEDIUM-2, reproducing the audit's measured shape on plain
// Postgres: the target holds pre-created EMPTY tables with ENABLE + FORCE
// ROW LEVEL SECURITY, and the role sluice connects to the target as is
// NOBYPASSRLS.
//
// `migrate` and the single-schema `sync start` refused that target up
// front, naming `ALTER ROLE … BYPASSRLS`. The fan-out opened its own
// writers, reached none of the target-side preflights, and died mid-copy
// on a raw `COPY FROM not supported with row-level security` (SQLSTATE
// 0A000) behind an `--exclude-table` hint that fixes nothing. It must now
// refuse with the same RLS preflight refusal, having copied nothing.
//
// The independent evidence is the target's own row count, read as the
// superuser: the refusal message alone could come from a later, partial
// failure.
func TestStreamer_MultiSchema_PG_TargetRLSRefusedBeforeTheCopy(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, pgSource, `
		CREATE SCHEMA sales;
		CREATE SCHEMA billing;
		CREATE TABLE sales.users   (id BIGINT PRIMARY KEY, email TEXT NOT NULL);
		CREATE TABLE billing.users (id BIGINT PRIMARY KEY, email TEXT NOT NULL);
		INSERT INTO sales.users   (id, email) VALUES (1, 'a@x'), (2, 'b@x');
		INSERT INTO billing.users (id, email) VALUES (1, 'c@x'), (2, 'd@x'), (3, 'e@x');
	`)

	// The target: a NOBYPASSRLS role that owns the pre-created, empty,
	// RLS-FORCED tables (FORCE, because an owner otherwise bypasses its
	// own table's policies) and may create sluice's control tables.
	applyPGDDL(t, pgTarget, `
		CREATE ROLE appu LOGIN PASSWORD 'app' NOBYPASSRLS NOSUPERUSER;
		DO $$ BEGIN EXECUTE format('GRANT CREATE ON DATABASE %I TO appu', current_database()); END $$;
		GRANT ALL ON SCHEMA public TO appu;
		CREATE SCHEMA sales AUTHORIZATION appu;
		CREATE SCHEMA billing AUTHORIZATION appu;
		CREATE TABLE sales.users   (id BIGINT PRIMARY KEY, email TEXT NOT NULL);
		CREATE TABLE billing.users (id BIGINT PRIMARY KEY, email TEXT NOT NULL);
		ALTER TABLE sales.users   OWNER TO appu;
		ALTER TABLE billing.users OWNER TO appu;
		ALTER TABLE sales.users   ENABLE ROW LEVEL SECURITY;
		ALTER TABLE sales.users   FORCE ROW LEVEL SECURITY;
		ALTER TABLE billing.users ENABLE ROW LEVEL SECURITY;
		ALTER TABLE billing.users FORCE ROW LEVEL SECURITY;
	`)
	u, err := url.Parse(pgTarget)
	if err != nil {
		t.Fatalf("parse target DSN: %v", err)
	}
	u.User = url.UserPassword("appu", "app")
	appuTarget := u.String()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	streamer := &Streamer{
		Source:         pgEng,
		Target:         pgEng,
		SourceDSN:      pgSource,
		TargetDSN:      appuTarget,
		StreamID:       "multischema-rls",
		DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()

	select {
	case err := <-runErr:
		if err == nil {
			t.Fatal("the fan-out cold start into an RLS-forced target under a NOBYPASSRLS role returned nil")
		}
		if !strings.Contains(err.Error(), "RLS preflight refused") || !strings.Contains(err.Error(), "BYPASSRLS") {
			t.Fatalf("want the RLS preflight refusal naming BYPASSRLS; got:\n%v", err)
		}
		if strings.Contains(err.Error(), "0A000") {
			t.Errorf("the refusal carries the COPY-time 0A000, so the copy was attempted before refusing:\n%v", err)
		}
	case <-time.After(110 * time.Second):
		cancel()
		t.Fatal("the fan-out cold start neither refused nor returned within 110s")
	}

	for _, schema := range []string{"sales", "billing"} {
		if n := pgScalarCount(pgTarget, `SELECT COUNT(*) FROM `+schema+`.users`); n != 0 {
			t.Errorf("target %s.users holds %d rows after the refusal; want 0 — the refusal must precede the copy", schema, n)
		}
	}
}
