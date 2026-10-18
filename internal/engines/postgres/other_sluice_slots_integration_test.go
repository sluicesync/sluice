//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestOtherSluiceSlots_OnlyThisDatabase is Bug 321 F-B, in both directions,
// on one real server holding two databases. A sluice slot on ANOTHER database
// cannot read this database's publication, so it neither counts as a peer
// for the ADR-0175 guard nor marks the publication "shared"; a slot on THIS
// database still does both. Before the filter the other-database slot made
// the guard refuse and the INCLUDE-TABLE-SCOPE-DRIFT no-record fallback stand
// down.
func TestOtherSluiceSlots_OnlyThisDatabase(t *testing.T) {
	ctx := context.Background()
	here, _ := newSharedPGDB(t, "slots_here")
	there, _ := newSharedPGDB(t, "slots_there")
	open := func(dsn string) *sql.DB {
		t.Helper()
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	hereDB, thereDB := open(here), open(there)
	mustExec := func(db *sql.DB, q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec(hereDB, `CREATE TABLE fb_t (id int primary key, v text)`)
	mustExec(hereDB, `CREATE TABLE fb_u (id int primary key, v text)`)
	mustExec(hereDB, `CREATE PUBLICATION sluice_fb_pub FOR TABLE fb_t`)

	const elsewhere, peer = "sluice_fb_elsewhere", "sluice_fb_peer"
	mustExec(thereDB, `SELECT pg_catalog.pg_create_logical_replication_slot('`+elsewhere+`', 'pgoutput')`)
	t.Cleanup(func() { _, _ = thereDB.ExecContext(ctx, `SELECT pg_catalog.pg_drop_replication_slot('`+elsewhere+`')`) })

	names := func() string {
		t.Helper()
		slots, err := otherSluiceSlots(ctx, hereDB, "sluice_fb_self")
		if err != nil {
			t.Fatal(err)
		}
		return strings.Join(slots, "; ")
	}
	if got := names(); strings.Contains(got, elsewhere) {
		t.Fatalf("a slot on another database counted as a peer of this one's publication: %s", got)
	}
	checker := Engine{}.WithPublicationScope("sluice_fb_pub", "sluice_fb_self").(interface {
		UnpublishedTables(context.Context, string, []string) (ir.PublicationMembership, error)
	})
	m, err := checker.UnpublishedTables(ctx, here, []string{"fb_t"})
	if err != nil {
		t.Fatal(err)
	}
	if m.Shared {
		t.Fatal("the publication reads as shared because of a slot on another database")
	}

	// The other direction: a peer on this database still counts, and the
	// ADR-0175 narrowing guard still refuses for it.
	mustExec(hereDB, `SELECT pg_catalog.pg_create_logical_replication_slot('`+peer+`', 'pgoutput')`)
	t.Cleanup(func() { _, _ = hereDB.ExecContext(ctx, `SELECT pg_catalog.pg_drop_replication_slot('`+peer+`')`) })
	if got := names(); !strings.Contains(got, peer) {
		t.Fatalf("a slot on this database no longer counts as a peer: %q", got)
	}
	err = ensurePublication(ctx, hereDB, "sluice_fb_pub", "public", []string{"fb_u"}, "sluice_fb_self", nil)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeCDCPublicationScopeConflict {
		t.Fatalf("narrowing a publication a same-database peer reads: %v; want %s", err, sluicecode.CodeCDCPublicationScopeConflict)
	}
}
