// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"database/sql"
	"strings"
	"testing"
)

// The A-5 half of the SEC-4 fix (audit 2026-08-31): probeRelayControlTable
// computes the registered-stream count's VALIDITY and both consumers threw
// it away, so a denied or lock-parked count(*) rendered "0 registered
// stream(s)" inside a refusal that fired precisely BECAUSE the table is
// there — a zero indistinguishable from a genuinely empty control table,
// pointing the operator at the "decommissioned residue, drop it" remedy on
// evidence nobody read. The renderers below are where that validity is now
// spent, so they are what the pin grades.
func TestRelayControlTableRendering_HonoursCountValidity(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		found      []relayControlTable
		wantDetail []string
		notDetail  []string
		wantList   string
	}{
		{
			name:       "one hit with a read count",
			found:      []relayControlTable{{schema: "public", streams: sql.NullInt64{Int64: 2, Valid: true}}},
			wantDetail: []string{"public.sluice_cdc_state, 2 registered stream(s)"},
			wantList:   "public.sluice_cdc_state",
		},
		{
			name:       "one hit whose detail read FAILED must not print a zero",
			found:      []relayControlTable{{schema: "public"}},
			wantDetail: []string{"public.sluice_cdc_state, stream count unavailable"},
			notDetail:  []string{"0 registered stream(s)"},
			wantList:   "public.sluice_cdc_state",
		},
		{
			name:       "a genuinely EMPTY control table still reports zero — the two must stay distinguishable",
			found:      []relayControlTable{{schema: "public", streams: sql.NullInt64{Int64: 0, Valid: true}}},
			wantDetail: []string{"public.sluice_cdc_state, 0 registered stream(s)"},
			notDetail:  []string{"unavailable"},
			wantList:   "public.sluice_cdc_state",
		},
		{
			name: "several schemas, mixed validity — the database-wide shape SEC-4 opens",
			found: []relayControlTable{
				{schema: "customer_svc", streams: sql.NullInt64{Int64: 1, Valid: true}},
				{schema: "public"},
			},
			wantDetail: []string{
				"customer_svc.sluice_cdc_state, 1 registered stream(s)",
				"public.sluice_cdc_state, stream count unavailable",
			},
			wantList: "customer_svc.sluice_cdc_state, public.sluice_cdc_state",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			detail := relayControlTableDetail(tc.found)
			for _, want := range tc.wantDetail {
				if !strings.Contains(detail, want) {
					t.Errorf("detail = %q; want it to contain %q", detail, want)
				}
			}
			for _, bad := range tc.notDetail {
				if strings.Contains(detail, bad) {
					t.Errorf("detail = %q; must NOT contain %q", detail, bad)
				}
			}
			if got := relayControlTableList(tc.found); got != tc.wantList {
				t.Errorf("list = %q; want %q", got, tc.wantList)
			}
		})
	}
}

// The existence half of the probe must not carry a schema predicate: the
// hazard is database-wide (SEC-4). Grading the query text is the cheapest
// check that survives a refactor of the caller, and it is deliberately
// paired with the real-PG cross-schema pin in
// capture_replicated_integration_test.go — this one alone would be a
// grep, not a gate.
func TestProbeRelayControlTable_ExistenceQueryIsDatabaseWide(t *testing.T) {
	t.Parallel()
	q := relayControlExistsQuery
	if strings.Contains(q, "nspname = $") {
		t.Errorf("the existence probe scopes to one schema again — the upstream applier's control table lives in the TARGET DSN's schema, which --target-schema does not move, so a database-wide echo loop would go unrefused:\n%s", q)
	}
	// Anti-vacuity: the query the gate read must actually be the one that
	// looks for the control table.
	if !strings.Contains(q, "c.relname = $1") || !strings.Contains(q, "relkind = 'r'") {
		t.Errorf("the gate matched something that is not the control-table existence query:\n%s", q)
	}
}
