//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the UNLOGGED-table capture census (capture-
// completeness G2) against real Postgres. Three doors share one census
// (roster on refuseUnloggedTables); each is pinned in BOTH directions —
// refuses the bad shape, passes the good one — because a door that only
// has its refusal pinned can silently widen, and one that only has its
// pass pinned can silently die:
//
//  1. the census itself (schema scoping + table allowlist + the
//     logged/unlogged discrimination),
//  2. Door 3: EnsurePublication over an explicit scope (the coded
//     upgrade of PG's own `cannot add relation … to publication`),
//  3. Door A (engine half): PreflightSpanningUnloggedTables with the
//     Bug 246 exclusion predicate,
//  4. Door B: OpenBackupSnapshot --chain-slot, scoped by
//     opts.InScopeTables, and NOT censused on the default
//     temporary-anchor shape (a one-shot full backup of an unlogged
//     table is fine — its rows are swept once, no chain),
//  5. Door 5 (audit 2026-08-27 A7): PreflightAddTableUnlogged, the
//     `schema add-table` registration census.
package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// requireUnloggedRefusal asserts err is the coded SLUICE-E-CDC-UNLOGGED-TABLE
// refusal naming each of wantNames.
func requireUnloggedRefusal(t *testing.T, err error, wantNames ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected the UNLOGGED-table refusal; got nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCUnloggedTable {
		t.Fatalf("expected code %s; got coded=%v err=%v", sluicecode.CodeCDCUnloggedTable, ok, err)
	}
	for _, name := range wantNames {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("refusal does not name %q: %v", name, err)
		}
	}
	if !strings.Contains(err.Error(), "--exclude-table") || !strings.Contains(err.Error(), "SET LOGGED") {
		t.Errorf("refusal is missing the two remedies (--exclude-table / SET LOGGED): %v", err)
	}
}

func TestUnloggedCensus_AndPublicationDoors(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "unlogged_census_db")
	defer cleanup()

	applyPGSnap(t, dsn, `
		CREATE SCHEMA s2;
		CREATE TABLE t_logged (id BIGINT PRIMARY KEY, v TEXT);
		CREATE UNLOGGED TABLE u_scratch (id BIGINT PRIMARY KEY, v TEXT);
		CREATE UNLOGGED TABLE s2.u_other (id BIGINT PRIMARY KEY, v TEXT);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	eng := Engine{}
	cfg, err := eng.parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	t.Run("census discriminates persistence, schema, and allowlist", func(t *testing.T) {
		got, err := unloggedTablesInSchemas(ctx, db, []string{"public", "s2"}, nil)
		if err != nil {
			t.Fatalf("census: %v", err)
		}
		if len(got) != 2 || got[0].String() != "public.u_scratch" || got[1].String() != "s2.u_other" {
			t.Fatalf("census = %v; want [public.u_scratch s2.u_other] (logged tables must not appear)", got)
		}
		// Schema scoping: s2 only.
		got, err = unloggedTablesInSchemas(ctx, db, []string{"s2"}, nil)
		if err != nil {
			t.Fatalf("census (s2): %v", err)
		}
		if len(got) != 1 || got[0].String() != "s2.u_other" {
			t.Fatalf("census (s2) = %v; want [s2.u_other]", got)
		}
		// Table allowlist: a scope that omits the unlogged table is clean.
		got, err = unloggedTablesInSchemas(ctx, db, []string{"public"}, []string{"t_logged"})
		if err != nil {
			t.Fatalf("census (allowlist): %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("census (allowlist) = %v; want empty — the allowlist must scope the census", got)
		}
	})

	t.Run("Door 3: EnsurePublication refuses an unlogged table in scope, coded and pre-DDL", func(t *testing.T) {
		err := eng.EnsurePublication(ctx, dsn, []string{"t_logged", "u_scratch"})
		requireUnloggedRefusal(t, err, "public.u_scratch")
		// Pre-DDL: the refusal must land before CREATE PUBLICATION.
		var exists bool
		if err := db.QueryRowContext(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", eng.publicationName()).Scan(&exists); err != nil {
			t.Fatalf("check publication: %v", err)
		}
		if exists {
			t.Fatal("publication was created despite the census refusal; the door must be pre-DDL")
		}
	})

	t.Run("Door 3 passes a logged-only scope (the operator's --exclude-table workaround works)", func(t *testing.T) {
		if err := eng.EnsurePublication(ctx, dsn, []string{"t_logged"}); err != nil {
			t.Fatalf("EnsurePublication (logged only): %v", err)
		}
		applyPGSnap(t, dsn, "DROP PUBLICATION IF EXISTS "+quoteIdent(eng.publicationName()))
	})

	t.Run("Door 5 (add-table registration): one-table census refuses the unlogged table, passes the logged one", func(t *testing.T) {
		// Audit 2026-08-27 A7: `sluice schema add-table` must refuse an
		// UNLOGGED table at registration (it would backfill once and
		// freeze forever). Both directions, plus the not-found pass (the
		// orchestrator's own refusal owns that case).
		err := eng.PreflightAddTableUnlogged(ctx, dsn, "u_scratch")
		requireUnloggedRefusal(t, err, "public.u_scratch")
		if err := eng.PreflightAddTableUnlogged(ctx, dsn, "t_logged"); err != nil {
			t.Fatalf("add-table census must pass a logged table; got %v", err)
		}
		if err := eng.PreflightAddTableUnlogged(ctx, dsn, "no_such_table"); err != nil {
			t.Fatalf("add-table census must pass a table it cannot find; got %v", err)
		}
	})

	t.Run("Door A engine half: spanning census refuses across schemas, honours the exclusion predicate", func(t *testing.T) {
		err := eng.PreflightSpanningUnloggedTables(ctx, dsn, []string{"public", "s2"}, nil)
		requireUnloggedRefusal(t, err, "public.u_scratch", "s2.u_other")

		// Bug 246: tables the predicate excludes must not trip the door.
		excluded := func(_, table string) bool { return table != "u_scratch" && table != "u_other" }
		if err := eng.PreflightSpanningUnloggedTables(ctx, dsn, []string{"public", "s2"}, excluded); err != nil {
			t.Fatalf("spanning census with both unlogged tables excluded must pass; got %v", err)
		}
	})
}

func TestUnloggedCensus_BackupChainSlotDoor(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "unlogged_backup_db")
	defer cleanup()

	applyPGSnap(t, dsn, `
		CREATE TABLE t_logged (id BIGINT PRIMARY KEY, v TEXT);
		CREATE UNLOGGED TABLE u_scratch (id BIGINT PRIMARY KEY, v TEXT);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	eng := Engine{}

	t.Run("--chain-slot with the unlogged table in scope refuses before creating anything", func(t *testing.T) {
		snap, err := eng.OpenBackupSnapshot(ctx, dsn, irbackup.SnapshotOptions{
			PersistChainSlot: true,
			InScopeTables:    []string{"t_logged", "u_scratch"},
		})
		if err == nil {
			_ = snap.Close()
			t.Fatal("expected the UNLOGGED-table refusal; got a snapshot")
		}
		requireUnloggedRefusal(t, err, "public.u_scratch")
	})

	t.Run("--chain-slot with the unlogged table excluded passes (Bug 246)", func(t *testing.T) {
		snap, err := eng.OpenBackupSnapshot(ctx, dsn, irbackup.SnapshotOptions{
			PersistChainSlot: true,
			InScopeTables:    []string{"t_logged"},
		})
		if err != nil {
			t.Fatalf("chain-slot open with the unlogged table excluded must pass; got %v", err)
		}
		// Uncommitted Close drops the chain slot + releases everything.
		if err := snap.Close(); err != nil {
			t.Fatalf("snapshot close: %v", err)
		}
	})

	t.Run("default temporary-anchor backup is NOT censused (a one-shot full of an unlogged table is sound)", func(t *testing.T) {
		snap, err := eng.OpenBackupSnapshot(ctx, dsn, irbackup.SnapshotOptions{})
		if err != nil {
			t.Fatalf("default backup snapshot over an unlogged table must open; got %v", err)
		}
		if err := snap.Close(); err != nil {
			t.Fatalf("snapshot close: %v", err)
		}
	})
}
