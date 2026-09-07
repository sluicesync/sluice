//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestEnsureAllTablesPublication_GuardsTheWideningDrop is the
// behavioural half of the audit 2026-09-06 H2 fix.
//
// THE DEFECT. `ensureAllTablesPublication` promotes a scoped publication
// to FOR ALL TABLES, and ALTER cannot do that, so it DROPs and recreates.
// It did so with no probe for other streams reading through that
// publication, on a comment asserting the drop was "safe because the
// publication is metadata only — slots reference WAL by LSN". The
// narrowing direction has consulted [otherSluiceSlots] since ADR-0175;
// the widening direction never did.
//
// WHY THE DROP IS THE HARM, regardless of direction — measured
// previously and recorded at [ensurePublication]'s create arm: a peer
// whose resume lands after a write in the drop→recreate window dies
// non-zero asserting the publication does not exist (Bug 267); a peer
// with no write in that window resumes fine and is now silently
// database-wide, so every keyless table it can see refuses UPDATE
// (Bug 270).
//
// A REAL SERVER, because every fact this grades is a catalog fact: that
// a `sluice_%` slot exists, that a publication is scoped rather than
// FOR ALL TABLES, and that the guard reads both. A stub would be
// asserting my own mock's shape.
func TestEnsureAllTablesPublication_GuardsTheWideningDrop(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	scopedPub := func(name string) {
		t.Helper()
		mustExec(`DROP PUBLICATION IF EXISTS ` + quoteIdent(name))
		mustExec(`CREATE PUBLICATION ` + quoteIdent(name) + ` FOR TABLE wg_t`)
	}
	isAllTables := func(name string) bool {
		t.Helper()
		var all bool
		if err := db.QueryRowContext(
			ctx,
			`SELECT COALESCE((SELECT puballtables FROM pg_publication WHERE pubname = $1), false)`, name,
		).Scan(&all); err != nil {
			t.Fatalf("read puballtables: %v", err)
		}
		return all
	}

	mustExec(`CREATE TABLE IF NOT EXISTS wg_t (id int primary key, v text)`)

	t.Run("no peer slot: the widening proceeds", func(t *testing.T) {
		// THE FLOOR, and it matters more than the refusal: one operator
		// with one stream is the overwhelmingly common shape, and a guard
		// that refuses it has broken multi-schema sync for everybody.
		const pub = "sluice_wg_solo"
		scopedPub(pub)
		defer func() { _, _ = db.ExecContext(ctx, `DROP PUBLICATION IF EXISTS `+quoteIdent(pub)) }()

		if err := ensureAllTablesPublication(ctx, db, pub, "sluice_wg_self"); err != nil {
			t.Fatalf("widening refused with no peer slot present: %v", err)
		}
		if !isAllTables(pub) {
			t.Error("publication was not promoted to FOR ALL TABLES, so the happy path is broken " +
				"even though it returned nil")
		}
	})

	t.Run("a peer slot refuses the drop", func(t *testing.T) {
		const pub = "sluice_wg_peer"
		const peerSlot = "sluice_wg_peer_slot"
		scopedPub(pub)
		mustExec(`SELECT pg_catalog.pg_create_logical_replication_slot('` + peerSlot + `', 'pgoutput')`)
		defer func() {
			_, _ = db.ExecContext(ctx, `SELECT pg_catalog.pg_drop_replication_slot('`+peerSlot+`')`)
			_, _ = db.ExecContext(ctx, `DROP PUBLICATION IF EXISTS `+quoteIdent(pub))
		}()

		err := ensureAllTablesPublication(ctx, db, pub, "sluice_wg_self")
		if err == nil {
			t.Fatal("the widening DROP proceeded with another sluice slot present. That drop either " +
				"wedges the peer's next resume (Bug 267) or silently widens it to every table in the " +
				"database, after which a keyless table refuses UPDATE (Bug 270).")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCPublicationScopeConflict {
			t.Errorf("refusal does not carry %s: %v", sluicecode.CodeCDCPublicationScopeConflict, err)
		}
		if !strings.Contains(err.Error(), peerSlot) {
			t.Errorf("the refusal does not name the peer slot %q, so the operator cannot act on it: %v",
				peerSlot, err)
		}

		// THE HALF THAT MAKES THIS A GUARD AND NOT A LOG LINE: the
		// publication must be untouched. A refusal that fires after the
		// DROP has already run would leave the peer in exactly the state
		// the refusal describes.
		if isAllTables(pub) {
			t.Error("the publication was widened DESPITE the refusal — the guard runs after the drop, " +
				"which means it reports the harm rather than preventing it")
		}
		var stillExists bool
		if err := db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, pub).Scan(&stillExists); err != nil {
			t.Fatalf("re-check publication: %v", err)
		}
		if !stillExists {
			t.Error("the publication was DROPPED despite the refusal; the peer is now wedged and the " +
				"guard bought nothing")
		}
	})

	t.Run("the caller's own slot never refuses itself", func(t *testing.T) {
		// excludeSlot is the whole reason a lone stream re-running its own
		// opener proceeds. If this regresses, every multi-schema resume
		// refuses on its own slot.
		const pub = "sluice_wg_own"
		const ownSlot = "sluice_wg_own_slot"
		scopedPub(pub)
		mustExec(`SELECT pg_catalog.pg_create_logical_replication_slot('` + ownSlot + `', 'pgoutput')`)
		defer func() {
			_, _ = db.ExecContext(ctx, `SELECT pg_catalog.pg_drop_replication_slot('`+ownSlot+`')`)
			_, _ = db.ExecContext(ctx, `DROP PUBLICATION IF EXISTS `+quoteIdent(pub))
		}()

		if err := ensureAllTablesPublication(ctx, db, pub, ownSlot); err != nil {
			t.Fatalf("a stream was refused by its OWN slot, so no multi-schema stream can ever "+
				"re-run its opener: %v", err)
		}
		if !isAllTables(pub) {
			t.Error("publication was not promoted despite the call returning nil")
		}
	})

	t.Run("an already-FOR-ALL-TABLES publication is a no-op even with a peer", func(t *testing.T) {
		// The idempotent arm returns BEFORE the guard, deliberately: there
		// is no drop, so there is nothing for a peer to experience.
		// Without this, a peer slot would make every multi-schema resume
		// refuse on a publication that needs no change at all.
		const pub = "sluice_wg_noop"
		const peerSlot = "sluice_wg_noop_peer"
		mustExec(`DROP PUBLICATION IF EXISTS ` + quoteIdent(pub))
		mustExec(`CREATE PUBLICATION ` + quoteIdent(pub) + ` FOR ALL TABLES`)
		mustExec(`SELECT pg_catalog.pg_create_logical_replication_slot('` + peerSlot + `', 'pgoutput')`)
		defer func() {
			_, _ = db.ExecContext(ctx, `SELECT pg_catalog.pg_drop_replication_slot('`+peerSlot+`')`)
			_, _ = db.ExecContext(ctx, `DROP PUBLICATION IF EXISTS `+quoteIdent(pub))
		}()

		if err := ensureAllTablesPublication(ctx, db, pub, "sluice_wg_self"); err != nil {
			t.Fatalf("an idempotent no-op was refused because a peer slot exists; the guard belongs "+
				"on the DROP, not on the call: %v", err)
		}
	})
}
