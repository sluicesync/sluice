//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// nekiTopologyWriteSemanticsProbe answers the two questions the settled
// NK306 fix (self-enrol the control tables in the authoritative shard group)
// needs before `__neki.set_data_topology` can be called from PRODUCTION code
// rather than from a test:
//
//  1. IDEMPOTENCY. EnsureControlTable runs on every start. If writing an
//     UNCHANGED document still mints a revision, then "compare before
//     write" is load-bearing rather than an optimisation, and every sluice
//     start bumping the cluster's topology revision is the price of
//     forgetting it.
//  2. CONCURRENCY. Two sluice processes starting against one cluster are a
//     read-modify-write on a shared document. The vendor documents an
//     `expected_revision` option, but NOT how the current revision is READ
//     over SQL, and not what a mismatch looks like (error? success=false?).
//     Both are needed to write a retry loop that is correct rather than
//     hopeful.
//
// Like the control-table probes it passes on any CONCLUSIVE answer and fails
// only when it cannot get one. Everything it learns is logged verbatim so the
// production helper is written against measurements, not guesses.
func nekiTopologyWriteSemanticsProbe(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()

	t.Run("PROBE: set_data_topology write semantics (revision read, idempotency, expected_revision)", func(t *testing.T) {
		// (1) Every __neki function, with arguments AND return type. This is
		// how the revision-read question gets answered: if a function exposes
		// the current revision, it is in this list.
		sigs, err := nekiFunctionInventory(ctx, db)
		if err != nil {
			t.Fatalf("INCONCLUSIVE: could not enumerate __neki functions: %v", err)
		}
		t.Logf("the router registers %d __neki functions:\n      %s", len(sigs), strings.Join(sigs, "\n      "))

		// Every __neki relation too — a revision may be exposed as a view
		// rather than a function.
		if rels, err := nekiRelationInventory(ctx, db); err != nil {
			t.Logf("__neki relations: could not enumerate: %v", err)
		} else {
			t.Logf("__neki relations: %v", rels)
		}

		// (2) The document's own shape — does it carry a revision?
		t.Logf("get_data_topology document: %s", topologyDocShape(ctx, db))

		// (3) IDEMPOTENCY: write the UNCHANGED document twice.
		doc, err := currentTopologyDoc(ctx, db)
		if err != nil {
			t.Fatalf("INCONCLUSIVE: %v", err)
		}
		write := func(options string) (bool, int64, error) {
			var ok bool
			var rev int64
			err := db.QueryRowContext(ctx,
				`SELECT success, revision FROM __neki.set_data_topology($1, true, $2)`, doc, options).Scan(&ok, &rev)
			return ok, rev, err
		}
		ok1, rev1, err := write(`{"comment":"nekiverify write-semantics probe: unchanged doc, first"}`)
		if err != nil || !ok1 {
			t.Fatalf("INCONCLUSIVE: the first unchanged write failed (success=%v rev=%d): %v", ok1, rev1, err)
		}
		ok2, rev2, err := write(`{"comment":"nekiverify write-semantics probe: unchanged doc, second"}`)
		if err != nil || !ok2 {
			t.Fatalf("INCONCLUSIVE: the second unchanged write failed (success=%v rev=%d): %v", ok2, rev2, err)
		}
		switch {
		case rev2 == rev1:
			t.Logf("ANSWER (idempotency): an UNCHANGED document does NOT mint a revision (%d twice). "+
				"Compare-before-write is still the right shape, but forgetting it costs nothing on the platform.", rev1)
		case rev2 > rev1:
			t.Logf("ANSWER (idempotency): an UNCHANGED document DOES mint a revision (%d then %d). "+
				"The production helper MUST compare before writing, or every sluice start bumps the "+
				"cluster's topology revision.", rev1, rev2)
		default:
			t.Fatalf("INCONCLUSIVE: revisions went backwards (%d then %d)", rev1, rev2)
		}

		// (4) EXPECTED_REVISION: a STALE value must be refused, and the shape
		// of that refusal is what the retry loop keys on.
		okStale, revStale, staleErr := write(fmt.Sprintf(
			`{"comment":"nekiverify write-semantics probe: stale expected_revision","expected_revision":%d}`, rev2-1,
		))
		switch {
		case staleErr != nil:
			code := "(no SQLSTATE)"
			var pgErr *pgconn.PgError
			if errors.As(staleErr, &pgErr) {
				code = pgErr.Code
			}
			t.Logf("ANSWER (expected_revision mismatch): refused as an ERROR, SQLSTATE %s: %v", code, staleErr)
		case !okStale:
			t.Logf("ANSWER (expected_revision mismatch): refused as success=false, revision reported %d", revStale)
		default:
			t.Fatalf("ANSWER (expected_revision mismatch): NOT refused — success=true at revision %d with a "+
				"stale expected_revision of %d. The option is inert on this build; the concurrency argument "+
				"cannot rest on it.", revStale, rev2-1)
		}

		// And a CURRENT value must be accepted. The current revision is the
		// one the last successful write returned; if a read function exists,
		// the signatures above say so and the production helper uses that.
		okCur, revCur, curErr := write(fmt.Sprintf(
			`{"comment":"nekiverify write-semantics probe: current expected_revision","expected_revision":%d}`, rev2,
		))
		if curErr != nil || !okCur {
			t.Fatalf("INCONCLUSIVE: a write with the CURRENT expected_revision (%d) was refused (success=%v "+
				"rev=%d): %v — either the stale write above advanced the revision despite refusing, or the "+
				"option does not mean what the docs say", rev2, okCur, revCur, curErr)
		}
		t.Logf("a write with the current expected_revision (%d) succeeded → revision %d", rev2, revCur)

		// (5) wait_for_data_topology — both call forms this suite has used.
		for _, form := range []struct {
			q    string
			args []any
		}{
			{`SELECT __neki.wait_for_data_topology($1)`, []any{revCur}},
			{`SELECT __neki.wait_for_data_topology($1, '120 seconds')`, []any{revCur}},
		} {
			if _, err := db.ExecContext(ctx, form.q, form.args...); err != nil {
				t.Logf("wait form %q: %v", form.q, err)
			} else {
				t.Logf("wait form %q: OK", form.q)
			}
		}
	})
}

// nekiFunctionInventory lists every function in the __neki namespace as
// `__neki.name(args) RETURNS result`.
func nekiFunctionInventory(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT p.proname,
		       pg_catalog.pg_get_function_arguments(p.oid),
		       pg_catalog.pg_get_function_result(p.oid)
		  FROM pg_catalog.pg_proc p
		  JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		 WHERE n.nspname = '__neki'
		 ORDER BY 1, 2`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var sigs []string
	for rows.Next() {
		var name, args, ret string
		if err := rows.Scan(&name, &args, &ret); err != nil {
			return nil, err
		}
		sigs = append(sigs, "__neki."+name+"("+args+") RETURNS "+ret)
	}
	return sigs, rows.Err()
}

// nekiRelationInventory lists every relation in the __neki namespace as
// `name(relkind)`.
func nekiRelationInventory(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT c.relname, c.relkind
		  FROM pg_catalog.pg_class c
		  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = '__neki'
		 ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			return nil, err
		}
		names = append(names, name+"("+kind+")")
	}
	return names, rows.Err()
}
