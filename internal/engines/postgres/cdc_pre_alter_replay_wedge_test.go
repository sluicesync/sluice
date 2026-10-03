// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
)

// TestWithPreAlterReplayWedge names the pre-ALTER replay wedge (GC-44 F24)
// on a schema-race refusal exactly when the prior relation carried a row
// change in the transaction being decoded — the transaction wrote the table
// before its DDL, so every restart replays it into the same refusal — and
// leaves every other refusal as it was. The integration pins
// (schema_change_fifth_review_integration_test.go) check the marker on a
// real wedge's first delivery and its absence on a change made in its own
// transaction is pinned by TestStreamer_PGToPG_DrainedModelRecovery's
// plain-restart recovery.
func TestWithPreAlterReplayWedge(t *testing.T) {
	t.Parallel()
	base := errors.New("postgres: cdc: incompatible schema change mid-stream on public.mt (OID 1): x. hint")
	const txn = pglogrepl.LSN(0x16B3748)
	written := func(lsn pglogrepl.LSN) *relationCacheEntry {
		return &relationCacheEntry{Schema: "public", Name: "mt", writtenInTxn: lsn}
	}
	for _, tc := range []struct {
		name  string
		err   error
		prior *relationCacheEntry
		txn   pglogrepl.LSN
		wedge bool
	}{
		{"the transaction wrote the table before the change", base, written(txn), txn, true},
		{"the table was last written by an earlier transaction", base, written(txn - 8), txn, false},
		{"the table was never written in this session", base, written(0), txn, false},
		{"no prior relation (the session's first)", base, nil, txn, false},
		{"no transaction open", base, written(0), 0, false},
		{"no refusal", nil, written(txn), txn, false},
	} {
		got := withPreAlterReplayWedge(tc.err, tc.prior, tc.txn)
		if tc.err == nil {
			if got != nil {
				t.Errorf("%s: %v; want nil", tc.name, got)
			}
			continue
		}
		if !errors.Is(got, base) {
			t.Errorf("%s: the refusal no longer wraps the reader's error: %v", tc.name, got)
		}
		marked := strings.Contains(got.Error(), preAlterReplayWedgeMarker)
		if marked != tc.wedge {
			t.Errorf("%s: wedge named = %v, want %v: %v", tc.name, marked, tc.wedge, got)
		}
		if tc.wedge {
			for _, want := range []string{"public.mt", "--schema-changes=forward", "sluice slot drop", "--restart-from-scratch", "--inject-shard-column"} {
				if !strings.Contains(got.Error(), want) {
					t.Errorf("%s: the wedge note lacks %q: %v", tc.name, want, got)
				}
			}
		}
	}
	if strings.Contains(schemaRaceRecoveryHint, "--forward-schema-add-column") {
		t.Error("the reader's hint still offers the deprecated --forward-schema-add-column")
	}
}
