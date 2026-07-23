//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0176 — the publication row-filter EMIT path and its interplay with
// the ADR-0175 definition-widened scope guard, on real Postgres:
//
//   - EnsurePublication renders the classifier-approved predicate as a
//     per-table `WHERE (<pred>)` (pg_get_expr-verified via prqual);
//   - a filtered stream's own cold RESTART (same predicate, raw text vs
//     pg_get_expr rendering) is proven a no-op by the transactional probe
//     and SUCCEEDS even while unrelated sluice slots exist — the
//     false-refusal the probe exists to prevent;
//   - a SECOND stream's bare SET TABLE over the same publication REFUSES
//     (it would clear the first stream's row filter) — the ADR-0175/0176
//     layering pin — and the refusal leaves the filter untouched;
//   - a CHANGED predicate refuses while another slot exists, applies once
//     the operator is alone, and ADDING a filter to a bare publication is
//     guarded the same way.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// pubMemberQual reads the pg_get_expr-rendered row filter of one
// publication member (” when bare, "" with ok=false when the member is
// absent).
func pubMemberQual(t *testing.T, db *sql.DB, publication, table string) (string, bool) {
	t.Helper()
	const q = `
		SELECT COALESCE(pg_get_expr(pr.prqual, pr.prrelid), '')
		FROM   pg_publication p
		JOIN   pg_publication_rel pr ON pr.prpubid = p.oid
		JOIN   pg_class c            ON c.oid     = pr.prrelid
		WHERE  p.pubname = $1 AND c.relname = $2`
	var qual string
	err := db.QueryRowContext(context.Background(), q, publication, table).Scan(&qual)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("read prqual: %v", err)
	}
	return qual, true
}

func mustExecPG(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

func wantScopeConflict(t *testing.T, err error, context string) {
	t.Helper()
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCPublicationScopeConflict {
		t.Fatalf("%s: err = %v; want coded %s", context, err, sluicecode.CodeCDCPublicationScopeConflict)
	}
}

func TestEnsurePublication_RowFilterLifecycle(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExecPG(
		t, db,
		`CREATE TABLE orders (id int PRIMARY KEY, country text)`,
		`ALTER TABLE orders REPLICA IDENTITY FULL`,
	)

	const pub = "sluice_pushlc"
	scoped := func(ownSlot string, filters map[string]string) Engine {
		e := Engine{}.WithPublicationScope(pub, ownSlot).(Engine)
		if filters != nil {
			e = e.WithPublicationRowFilters(filters).(Engine)
		}
		return e
	}
	filterA := map[string]string{"orders": "id < 100"}

	// ---- 1. Fresh publication: the filter is emitted and PG records it ----
	if err := scoped("sluice_pushlc_a", filterA).EnsurePublication(context.Background(), dsn, []string{"orders"}); err != nil {
		t.Fatalf("initial EnsurePublication with filter: %v", err)
	}
	qual, ok := pubMemberQual(t, db, pub, "orders")
	if !ok || qual != "(id < 100)" {
		t.Fatalf("prqual after create = %q (present=%v); want (id < 100)", qual, ok)
	}

	// ---- 2. Same-definition re-assert (a filtered stream's cold restart),
	// ALONE on the source: the ambiguous raw-text-vs-pg_get_expr comparison
	// resolves via the transactional probe and commits as a no-op. ----
	if err := scoped("sluice_pushlc_a", filterA).EnsurePublication(context.Background(), dsn, []string{"orders"}); err != nil {
		t.Fatalf("same-filter re-assert (no peers): %v", err)
	}

	// ---- 3. Same re-assert WHILE an unrelated sluice slot exists: the
	// probe must prove the no-op and SUCCEED — refusing here would break
	// every filtered stream's restart on any multi-stream source. ----
	mustExecPG(t, db, `SELECT pg_create_logical_replication_slot('sluice_unrelated', 'pgoutput')`)
	// Slots are CLUSTER-wide on the shared container: drop tolerantly at
	// the end regardless of which arm the test stopped in, so a failure
	// here can't poison other tests' guard probes.
	defer func() {
		_, _ = db.ExecContext(context.Background(), `SELECT pg_drop_replication_slot('sluice_unrelated')`)
	}()
	if err := scoped("sluice_pushlc_a", filterA).EnsurePublication(context.Background(), dsn, []string{"orders"}); err != nil {
		t.Fatalf("same-filter re-assert with an unrelated slot present must be a proven no-op, got: %v", err)
	}
	if qual, _ := pubMemberQual(t, db, pub, "orders"); qual != "(id < 100)" {
		t.Fatalf("prqual after no-op re-assert = %q; want (id < 100)", qual)
	}

	// ---- 4. Guard interplay (the ADR-0175/0176 layering pin): a second
	// stream's BARE SET TABLE over this publication would silently clear
	// the first stream's row filter — refuse, and leave it untouched. ----
	err = scoped("sluice_pushlc_b", nil).EnsurePublication(context.Background(), dsn, []string{"orders"})
	wantScopeConflict(t, err, "bare second-stream rescope over a row-filtered publication")
	if qual, _ := pubMemberQual(t, db, pub, "orders"); qual != "(id < 100)" {
		t.Fatalf("refused bare rescope mutated the filter: prqual = %q", qual)
	}

	// ---- 5. A CHANGED predicate while another slot exists: genuine
	// definition change → the probe rolls back and refuses; untouched. ----
	err = scoped("sluice_pushlc_a", map[string]string{"orders": "id < 50"}).EnsurePublication(context.Background(), dsn, []string{"orders"})
	wantScopeConflict(t, err, "changed predicate with a peer slot present")
	if qual, _ := pubMemberQual(t, db, pub, "orders"); qual != "(id < 100)" {
		t.Fatalf("refused predicate change mutated the filter: prqual = %q", qual)
	}
	if !strings.Contains(err.Error(), "row filter") {
		t.Errorf("refusal should name the row-filter change; got %v", err)
	}

	// ---- 6. Alone again: the operator's own predicate change applies. ----
	mustExecPG(t, db, `SELECT pg_drop_replication_slot('sluice_unrelated')`)
	if err := scoped("sluice_pushlc_a", map[string]string{"orders": "id < 50"}).EnsurePublication(context.Background(), dsn, []string{"orders"}); err != nil {
		t.Fatalf("predicate change with no peers: %v", err)
	}
	if qual, _ := pubMemberQual(t, db, pub, "orders"); qual != "(id < 50)" {
		t.Fatalf("prqual after operator's own change = %q; want (id < 50)", qual)
	}
	// Re-create the unrelated slot so the remaining arms keep a peer.
	mustExecPG(t, db, `SELECT pg_create_logical_replication_slot('sluice_unrelated', 'pgoutput')`)

	// ---- 7. Clearing the filter (bare re-assert by the OWNING stream)
	// while a peer exists is still a definition change → refuse. ----
	err = scoped("sluice_pushlc_a", nil).EnsurePublication(context.Background(), dsn, []string{"orders"})
	wantScopeConflict(t, err, "owning stream clearing its own filter with a peer present")

	// ---- 8. ADDING a filter to a bare publication with a peer present is
	// the symmetric hard conflict. Build a bare publication first. ----
	mustExecPG(t, db, `SELECT pg_drop_replication_slot('sluice_unrelated')`)
	if err := scoped("sluice_pushlc_a", nil).EnsurePublication(context.Background(), dsn, []string{"orders"}); err != nil {
		t.Fatalf("clearing the filter with no peers: %v", err)
	}
	if qual, _ := pubMemberQual(t, db, pub, "orders"); qual != "" {
		t.Fatalf("prqual after clear = %q; want bare", qual)
	}
	mustExecPG(t, db, `SELECT pg_create_logical_replication_slot('sluice_unrelated', 'pgoutput')`)
	err = scoped("sluice_pushlc_a", filterA).EnsurePublication(context.Background(), dsn, []string{"orders"})
	wantScopeConflict(t, err, "adding a filter to a bare publication with a peer present")
	if qual, _ := pubMemberQual(t, db, pub, "orders"); qual != "" {
		t.Fatalf("refused filter-add mutated the publication: prqual = %q", qual)
	}
}

// TestEnsurePublication_RowFilterMixedTables pins the mixed rendering: one
// filtered member and one bare member in a single CREATE, each with the
// right catalog state — the shape a multi-table `--where` sync emits when
// only some predicates are classifier-eligible.
func TestEnsurePublication_RowFilterMixedTables(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExecPG(
		t, db,
		`CREATE TABLE orders (id int PRIMARY KEY, country text)`,
		`CREATE TABLE audits (id int PRIMARY KEY, flag boolean)`,
		`ALTER TABLE orders REPLICA IDENTITY FULL`,
		`ALTER TABLE audits REPLICA IDENTITY FULL`,
	)

	e := Engine{}.WithPublicationScope("sluice_pushmix", "sluice_pushmix_slot").(Engine)
	e = e.WithPublicationRowFilters(map[string]string{"orders": "country = 'US'"}).(Engine)
	if err := e.EnsurePublication(context.Background(), dsn, []string{"orders", "audits"}); err != nil {
		t.Fatalf("EnsurePublication: %v", err)
	}

	if qual, ok := pubMemberQual(t, db, "sluice_pushmix", "orders"); !ok || !strings.Contains(qual, "country") {
		t.Errorf("orders prqual = %q (present=%v); want a country filter", qual, ok)
	}
	if qual, ok := pubMemberQual(t, db, "sluice_pushmix", "audits"); !ok || qual != "" {
		t.Errorf("audits prqual = %q (present=%v); want present and bare", qual, ok)
	}
}
