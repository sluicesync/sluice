//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The migration-state LISTER against a real MySQL server.
//
// # Why this exists
//
// `Store.List` shipped in v0.148.0 with no test touching it on EITHER engine,
// and it was broken on this one from the first commit: the rendered SQL carried
// `ESCAPE '\'`, and MySQL treats backslash as a STRING escape by default, so
// that is an unterminated literal and the statement is a 1064 parse error. It
// failed whether or not the control table existed, which meant `sync status`
// errored outright on every MySQL-family target and the release's headline
// cold-start-visibility feature was simply absent there.
//
// PostgreSQL's standard_conforming_strings makes the byte-identical twin valid,
// which is precisely why it shipped: the two engines' SQL was reviewed as "the
// same string" and one dialect disagreed. Two independent audit workers found
// it on the same day; no unit test could have, because the defect is in what
// the SERVER makes of the text.
//
// The engines now both render `ESCAPE '#'`, a character neither dialect treats
// specially. This test is the thing that would have caught it: it runs the real
// statement against a real server.
func TestMigrationStateStore_ListAgainstRealMySQL(t *testing.T) {
	dsn, cleanup := startMySQL(t)
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
		t.Fatal("the MySQL migration-state store does not implement ir.MigrationStateLister; `sync status` " +
			"silently shows no cold starts on this engine")
	}

	// A missing control table is "no runs", not an error — a target that has
	// never migrated is the healthy shape. This also proves the statement
	// PARSES: a 1064 fires before the table is ever consulted, so a parse bug
	// surfaces here even on an empty server.
	got, err := lister.List(ctx, "sync-")
	if err != nil {
		t.Fatalf("List against a server with no control table: %v\n"+
			"  A parse error surfaces here regardless of the table, which is exactly how the ESCAPE '\\' bug "+
			"broke `sync status` on every MySQL target.", err)
	}
	if len(got) != 0 {
		t.Errorf("List returned %d row(s) from a server with no control table; want none", len(got))
	}

	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("ensure control table: %v", err)
	}
	for _, id := range []string{"sync-prod", "sync-prod_x", "syncXprod", "auto-deadbeef"} {
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
		for _, want := range []string{"sync-prod", "sync-prod_x"} {
			if !seen[want] {
				t.Errorf("List(%q) did not return %q", "sync-", want)
			}
		}
		// "syncXprod" is the load-bearing exclusion: an unescaped '_' in the
		// pattern would be a single-character wildcard and match it, silently
		// attributing an unrelated migration's progress to the sync namespace.
		for _, notWant := range []string{"syncXprod", "auto-deadbeef"} {
			if seen[notWant] {
				t.Errorf("List(%q) returned %q — the prefix is over-matching", "sync-", notWant)
			}
		}
	})

	t.Run("an underscore in the prefix is escaped, not a wildcard", func(t *testing.T) {
		// Operator-chosen stream ids contain underscores ("prod_cutover"), and
		// an unescaped pattern would also match "prodXcutover".
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
}
