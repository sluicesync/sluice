//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-07-23 D0-7 gate — the interleaved-ensure race pin.
//
// Cold start ensures the publication BEFORE its own replication slot
// exists, so two simultaneously cold-starting streams sharing one
// publication name can swap definitions past the ADR-0175 existence
// guard: when stream B's ensure probes for conflicting slots, stream A
// hasn't created its slot yet — zero other slots, so B's redefinition
// COMMITS. The post-slot re-verification ([Engine.VerifyPublicationScope],
// wired by the pipeline right after the snapshot open creates the slot)
// closes the window after the fact: A re-reads the publication and
// refuses loudly BEFORE any data moves, while B — whose definition the
// catalog now holds — verifies clean, so at least one stream always
// proceeds and none streams through the wrong filter.
//
// The interleaving here is the deterministic serialization of the race
// (ensure A → mutate as B would → A's post-slot verify), which is
// exactly the D0-7 window without goroutine timing flakiness.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestVerifyPublicationScope_InterleavedEnsureRace(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExecPG(
		t, db,
		`CREATE TABLE d07_orders (id int PRIMARY KEY, country text)`,
		`ALTER TABLE d07_orders REPLICA IDENTITY FULL`,
		`CREATE TABLE d07_audits (id int PRIMARY KEY, note text)`,
	)

	scoped := func(pub, ownSlot string, filters map[string]string) Engine {
		e := Engine{}.WithPublicationScope(pub, ownSlot).(Engine)
		if filters != nil {
			e = e.WithPublicationRowFilters(filters).(Engine)
		}
		return e
	}
	ctx := context.Background()

	t.Run("row-filter swap in the pre-slot window", func(t *testing.T) {
		const pub = "sluice_d07_filter"
		a := scoped(pub, "sluice_d07f_slot_a", map[string]string{"d07_orders": "id < 100"})
		b := scoped(pub, "sluice_d07f_slot_b", map[string]string{"d07_orders": "id < 50"})

		// ---- Stream A's cold start ensures its definition. No slot yet. ----
		if err := a.EnsurePublication(ctx, dsn, []string{"d07_orders"}); err != nil {
			t.Fatalf("A EnsurePublication: %v", err)
		}
		if qual, _ := pubMemberQual(t, db, pub, "d07_orders"); qual != "(id < 100)" {
			t.Fatalf("prqual after A's ensure = %q; want (id < 100)", qual)
		}

		// ---- Stream B's simultaneous cold start redefines it. This COMMITS:
		// A has no slot yet, so the existence guard sees zero peers — the
		// documented D0-7 blind spot this test exists to close post-hoc. ----
		if err := b.EnsurePublication(ctx, dsn, []string{"d07_orders"}); err != nil {
			t.Fatalf("B EnsurePublication (the pre-slot window) = %v; the D0-7 window closed at the ensure — update this pin to match the new guard shape", err)
		}
		if qual, _ := pubMemberQual(t, db, pub, "d07_orders"); qual != "(id < 50)" {
			t.Fatalf("prqual after B's ensure = %q; want B's (id < 50)", qual)
		}

		// ---- A's slot is created (snapshot open); the post-slot verify must
		// refuse — pre-fix A silently streamed through B's filter forever. ----
		err := a.VerifyPublicationScope(ctx, dsn, []string{"d07_orders"})
		wantScopeConflict(t, err, "A's post-slot verify after B's redefinition")
		// SEC-1: the refusal reaches A's operator, who does not own B's
		// predicate — B's filter text must not be echoed.
		if strings.Contains(err.Error(), "id < 50") {
			t.Errorf("the refusal echoed the peer's predicate text: %v", err)
		}
		// The verify is read-only: B's committed definition stays untouched.
		if qual, _ := pubMemberQual(t, db, pub, "d07_orders"); qual != "(id < 50)" {
			t.Errorf("VerifyPublicationScope mutated the publication: prqual = %q", qual)
		}

		// ---- B's own post-slot verify passes: the catalog holds ITS
		// definition, so at least one of the racers proceeds. ----
		if err := b.VerifyPublicationScope(ctx, dsn, []string{"d07_orders"}); err != nil {
			t.Errorf("B VerifyPublicationScope = %v; want nil (the catalog holds B's definition)", err)
		}
	})

	t.Run("member-set swap in the pre-slot window", func(t *testing.T) {
		const pub = "sluice_d07_set"
		a := scoped(pub, "sluice_d07s_slot_a", nil)
		b := scoped(pub, "sluice_d07s_slot_b", nil)

		if err := a.EnsurePublication(ctx, dsn, []string{"d07_orders", "d07_audits"}); err != nil {
			t.Fatalf("A EnsurePublication: %v", err)
		}
		// B narrows to one table — commits, zero other slots exist.
		if err := b.EnsurePublication(ctx, dsn, []string{"d07_orders"}); err != nil {
			t.Fatalf("B EnsurePublication (narrowing, pre-slot window) = %v", err)
		}

		err := a.VerifyPublicationScope(ctx, dsn, []string{"d07_orders", "d07_audits"})
		wantScopeConflict(t, err, "A's post-slot verify after B narrowed the member set")
		if !strings.Contains(err.Error(), "d07_audits") {
			t.Errorf("the refusal must name the diverged member; got %v", err)
		}
		if err := b.VerifyPublicationScope(ctx, dsn, []string{"d07_orders"}); err != nil {
			t.Errorf("B VerifyPublicationScope = %v; want nil", err)
		}
	})
}
