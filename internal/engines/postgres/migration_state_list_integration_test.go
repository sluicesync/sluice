//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The PostgreSQL twin of the MySQL lister pin (audit 2026-09-09
// A0909-H1-ESCAPE). The MySQL statement shipped as a parse error because
// two byte-identical strings were reviewed as "the same SQL" and one
// dialect disagreed; the pre-tag review then found the PostgreSQL
// statement changed in the same fix and executed by no test at all — the
// sibling-sweep gap in its purest form. What only a real server can pin
// here is that the text the engine renders is a statement THIS dialect
// accepts, and that its ESCAPE clause takes effect.

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestMigrationStateStore_ListAgainstRealPostgres(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	eng := Engine{}
	store, err := eng.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("open migration-state store: %v", err)
	}
	defer func() {
		if c, ok := store.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	lister, ok := store.(ir.MigrationStateLister)
	if !ok {
		t.Fatal("the PostgreSQL migration-state store does not implement ir.MigrationStateLister; `sync status` " +
			"silently shows no cold starts on this engine")
	}

	got, err := lister.List(ctx, "sync-")
	if err != nil {
		t.Fatalf("List against a server with no control table: %v\n"+
			"  A statement the dialect rejects surfaces here regardless of the table.", err)
	}
	if len(got) != 0 {
		t.Errorf("List returned %d row(s) from a server with no control table; want none", len(got))
	}

	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("ensure control table: %v", err)
	}
	for _, id := range []string{"sync-prod", "sync-prod_x", "syncXprod", "sync-a#b", "auto-deadbeef"} {
		if err := store.Write(ctx, ir.MigrationState{MigrationID: id, Phase: ir.MigrationPhaseBulkCopy}); err != nil {
			t.Fatalf("seed %q: %v", id, err)
		}
	}

	t.Run("the prefix matches the sync namespace and nothing else", func(t *testing.T) {
		rows, err := lister.List(ctx, "sync-")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		seen := map[string]bool{}
		for _, r := range rows {
			seen[r.MigrationID] = true
		}
		for _, want := range []string{"sync-prod", "sync-prod_x", "sync-a#b"} {
			if !seen[want] {
				t.Errorf("List(%q) did not return %q", "sync-", want)
			}
		}
		for _, notWant := range []string{"syncXprod", "auto-deadbeef"} {
			if seen[notWant] {
				t.Errorf("List(%q) returned %q — the prefix is over-matching", "sync-", notWant)
			}
		}
	})

	t.Run("an underscore in the prefix is escaped, not a wildcard", func(t *testing.T) {
		rows, err := lister.List(ctx, "sync-prod_")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		for _, r := range rows {
			if r.MigrationID == "sync-prod" {
				t.Errorf("List(%q) matched %q, so '_' was treated as a wildcard and the ESCAPE clause is not "+
					"taking effect on this server", "sync-prod_", r.MigrationID)
			}
		}
	})

	t.Run("the escape character itself is escaped", func(t *testing.T) {
		rows, err := lister.List(ctx, "sync-a#")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(rows) != 1 || rows[0].MigrationID != "sync-a#b" {
			t.Errorf("List(%q) = %+v; want exactly sync-a#b — a bare '#' in the pattern would be an escape "+
				"with nothing to escape, or would swallow the next character", "sync-a#", rows)
		}
	})
}
