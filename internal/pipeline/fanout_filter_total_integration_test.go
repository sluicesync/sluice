//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 317 follow-up: a Postgres multi-schema fan-out whose table filter
// matches nothing in ANY namespace. Each namespace pass skips a namespace
// its filter emptied (the push-down drops every table before the filter
// sees it), which is right for one namespace — --include-table naming one
// schema's table is a normal fan-out — but when every namespace skipped,
// the run ended "multi-database migrate complete" at exit 0 having copied
// nothing, and a sync started CDC over nothing. The independent expected
// value is the summed push-down count: the source holds tables, and the
// filter kept none anywhere.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

const fanoutTwoSchemas = `
	CREATE SCHEMA sales;
	CREATE SCHEMA billing;
	CREATE TABLE sales.orders   (id BIGINT PRIMARY KEY);
	CREATE TABLE billing.invoices (id BIGINT PRIMARY KEY);
	INSERT INTO sales.orders VALUES (1), (2);
	INSERT INTO billing.invoices VALUES (1);
`

func TestMigrate_FanoutTypoIncludeTableRefuses_Postgres(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	applyPGDDL(t, pgSource, fanoutTwoSchemas)
	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	err := (&Migrator{
		Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget,
		DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
		Filter:         mustTableFilter(t, []string{"ordres"}, nil),
	}).Run(ctx)
	if err == nil || !strings.Contains(err.Error(), filterRefusal) {
		t.Fatalf("fan-out with a filter matching no table in any schema: want %q; got %v", filterRefusal, err)
	}

	// Control: a filter matching ONE schema's table still skips the other
	// schema and succeeds — the per-namespace skip is intact.
	if err := (&Migrator{
		Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget,
		DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
		Filter:         mustTableFilter(t, []string{"orders"}, nil),
	}).Run(ctx); err != nil {
		t.Fatalf("fan-out with a filter matching one schema's table must skip the other and succeed: %v", err)
	}
	if got := pgScalarCount(pgTarget, `SELECT COUNT(*) FROM sales.orders`); got != 2 {
		t.Errorf("sales.orders on the target = %d; want 2", got)
	}
}

func TestStreamer_FanoutTypoIncludeTableRefuses_Postgres(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyPGDDL(t, pgSource, fanoutTwoSchemas)
	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	err := (&Streamer{
		Source: pgEng, Target: pgEng, SourceDSN: pgSource, TargetDSN: pgTarget,
		StreamID:       "fanout-typo-include",
		DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
		Filter:         mustTableFilter(t, []string{"ordres"}, nil),
	}).Run(ctx)
	if err == nil || !strings.Contains(err.Error(), filterRefusal) {
		t.Fatalf("sync fan-out with a filter matching no table in any schema: want %q; got %v", filterRefusal, err)
	}
}
