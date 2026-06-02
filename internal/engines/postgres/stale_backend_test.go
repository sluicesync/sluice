// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestQualifiesAsStale_OrphanSignalMatrix pins the pure orphan-signal
// predicate. The SQL scope (ours / own-role / not-self) is applied by the
// query; this exercises the signal layer on top: idle-in-transaction (and
// its aborted variant) qualify on their own, a held in-scope lock
// qualifies on its own, and a backend with neither signal does NOT
// qualify even though it passed the scope.
func TestQualifiesAsStale_OrphanSignalMatrix(t *testing.T) {
	cases := []struct {
		name string
		row  staleBackendRow
		want bool
	}{
		{
			name: "idle in transaction qualifies",
			row:  staleBackendRow{pid: 1, state: "idle in transaction"},
			want: true,
		},
		{
			name: "idle in transaction (aborted) qualifies",
			row:  staleBackendRow{pid: 2, state: "idle in transaction (aborted)"},
			want: true,
		},
		{
			name: "holding an in-scope lock qualifies",
			row:  staleBackendRow{pid: 3, state: "active", lockRelation: "public.bench_events", lockMode: "AccessExclusiveLock"},
			want: true,
		},
		{
			name: "active with no lock does NOT qualify",
			row:  staleBackendRow{pid: 4, state: "active"},
			want: false,
		},
		{
			name: "idle (not in tx) with no lock does NOT qualify",
			row:  staleBackendRow{pid: 5, state: "idle"},
			want: false,
		},
		{
			name: "empty state with no lock does NOT qualify",
			row:  staleBackendRow{pid: 6, state: ""},
			want: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := qualifiesAsStale(c.row); got != c.want {
				t.Errorf("qualifiesAsStale(%+v) = %v, want %v", c.row, got, c.want)
			}
		})
	}
}

// TestStaleBackendScope_SafetyBound is the load-bearing test: the WHERE
// predicate that bounds BOTH detection and termination must constrain to
// sluice's own, own-role, non-self backends. This pins the exact clause
// so a future edit that loosens any of the three terms fails here.
func TestStaleBackendScope_SafetyBound(t *testing.T) {
	want := `application_name LIKE 'sluice/%' AND usename = current_user AND pid <> pg_backend_pid()`
	if staleBackendScope != want {
		t.Fatalf("staleBackendScope drifted from the safety bound.\n got: %s\nwant: %s", staleBackendScope, want)
	}

	// Both the detection query and the reap query must embed the scope
	// verbatim — neither may reach a backend the scope excludes.
	detect := detectStaleBackendsQuery()
	for _, frag := range []string{
		"application_name LIKE 'sluice/%'",
		"usename = current_user",
		"pid <> pg_backend_pid()",
	} {
		if !strings.Contains(detect, frag) {
			t.Errorf("detection query is missing safety-bound fragment %q", frag)
		}
	}
}

// TestReapStaleBackends_QueryReappliesScope asserts the termination query
// re-applies the full safety scope (never trusts the caller's pid list
// alone) and gates pg_terminate_backend behind it. The scoping lives
// entirely in the SQL text, so a query-builder assertion is the right
// unit; the integration test exercises the live behaviour end-to-end.
func TestReapStaleBackends_QueryReappliesScope(t *testing.T) {
	for _, frag := range []string{
		"application_name LIKE 'sluice/%'",
		"usename = current_user",
		"pid <> pg_backend_pid()",
		"pg_terminate_backend(a.pid)",
		"a.pid = ANY ($1)",
	} {
		if !strings.Contains(reapStaleBackendsQuery, frag) {
			t.Errorf("reap query missing required fragment %q\nquery: %s", frag, reapStaleBackendsQuery)
		}
	}
	// Safety: pg_terminate_backend must sit in the SELECT projection, which
	// is evaluated only for rows that already passed the WHERE scope — NOT
	// in the WHERE qual list, where the planner could (qual order is not
	// guaranteed, and the function is VOLATILE) evaluate the kill before the
	// safety predicates. Assert the terminate appears before the WHERE and
	// the scope lives inside the WHERE.
	whereIdx := strings.Index(reapStaleBackendsQuery, "WHERE")
	termIdx := strings.Index(reapStaleBackendsQuery, "pg_terminate_backend(a.pid)")
	scopeIdx := strings.Index(reapStaleBackendsQuery, "pid <> pg_backend_pid()")
	if termIdx < 0 || whereIdx < 0 || termIdx > whereIdx {
		t.Errorf("pg_terminate_backend must be in the SELECT projection, before WHERE (term@%d, where@%d)", termIdx, whereIdx)
	}
	if scopeIdx < 0 || scopeIdx < whereIdx {
		t.Errorf("safety scope must live inside the WHERE clause (scope@%d, where@%d)", scopeIdx, whereIdx)
	}
}

// TestStaleBackendFromRow_Age converts float-seconds to a Duration and
// leaves a NULL age at zero.
func TestStaleBackendFromRow_Age(t *testing.T) {
	r := staleBackendRow{
		pid:             42,
		applicationName: "sluice/snapshot/ps_import",
		state:           "idle in transaction",
		ageSeconds:      sql.NullFloat64{Float64: 125.5, Valid: true},
		lockRelation:    "public.bench_events",
		lockMode:        "AccessExclusiveLock",
	}
	got := staleBackendFromRow(r)
	if got.PID != 42 || got.ApplicationName != "sluice/snapshot/ps_import" {
		t.Errorf("identity fields not carried: %+v", got)
	}
	if got.Age != 125500*time.Millisecond {
		t.Errorf("age = %v, want 125.5s", got.Age)
	}
	if got.LockRelation != "public.bench_events" || got.LockMode != "AccessExclusiveLock" {
		t.Errorf("lock fields not carried: %+v", got)
	}

	nullAge := staleBackendFromRow(staleBackendRow{pid: 1, ageSeconds: sql.NullFloat64{Valid: false}})
	if nullAge.Age != 0 {
		t.Errorf("NULL age should map to 0, got %v", nullAge.Age)
	}
}

// TestWithControlSchema_DedupAndAppend asserts the control/DSN schema is
// always in scope, de-duplicated, with caller order preserved.
func TestWithControlSchema_DedupAndAppend(t *testing.T) {
	got := withControlSchema([]string{"analytics", "", "analytics", "sales"}, "public")
	want := []string{"analytics", "sales", "public"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	// Control already present: not appended twice.
	got2 := withControlSchema([]string{"public", "analytics"}, "public")
	if len(got2) != 2 {
		t.Errorf("control already present should not duplicate; got %v", got2)
	}

	// Empty control defaults to "public".
	got3 := withControlSchema(nil, "")
	if len(got3) != 1 || got3[0] != "public" {
		t.Errorf("empty control should default to public; got %v", got3)
	}
}

// TestFormatStaleBackend renders the operator-facing line, including and
// omitting the optional lock/state/age fields.
func TestFormatStaleBackend(t *testing.T) {
	full := formatStaleBackend(staleBackendFromRow(staleBackendRow{
		pid:             12345,
		applicationName: "sluice/snapshot/ps_import",
		state:           "idle in transaction",
		ageSeconds:      sql.NullFloat64{Float64: 90, Valid: true},
		lockRelation:    "public.bench_events",
		lockMode:        "AccessExclusiveLock",
	}))
	for _, frag := range []string{
		"pid=12345",
		`"sluice/snapshot/ps_import"`,
		"idle in transaction",
		"age=1m30s",
		"AccessExclusiveLock on public.bench_events",
	} {
		if !strings.Contains(full, frag) {
			t.Errorf("formatted line %q missing %q", full, frag)
		}
	}

	// No lock, no age: those clauses are omitted.
	bare := formatStaleBackend(staleBackendFromRow(staleBackendRow{
		pid:             7,
		applicationName: "sluice/applier/-",
		state:           "idle in transaction",
	}))
	if strings.Contains(bare, "holds") {
		t.Errorf("bare line should omit the lock clause; got %q", bare)
	}
}
