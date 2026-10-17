// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// brokerTailFullyApplied is the integration tests' wait condition for "the
// broker has applied the whole chain": the persisted position (the raw
// `sluice_cdc_state.source_position` column) names tail as its last FULLY
// applied incremental, with no frontier standing inside one.
//
// A substring match on the tail's id is NOT that condition: since ADR-0191
// the broker persists a frontier INSIDE the incremental it is applying,
// and that token names the incremental under in_progress.backup_id. A test
// that cancels on the substring cancels mid-tail, the broker exits loudly
// with BROKER-INCREMENTAL-PARTIAL, and the comparison then reads a target
// missing the tail's later transactions
// (SEVERED-TAIL-BROKER-SENTINEL-DIVERGES: TestSeveredTail_MySQLGTID failed
// ~1 in 4 locally on exactly this, measured with the token logged).
func brokerTailFullyApplied(pos, tail string) bool {
	if tail == "" {
		return false
	}
	tok, err := decodeBrokerPosition(ir.Position{Token: pos})
	return err == nil && tok.LastAppliedBackupID == tail && tok.InProgress == nil
}

func TestBrokerTailFullyApplied(t *testing.T) {
	const chain, parent, tail = "test://sev", "05efa6152c4ebd61", "4eae1517af2dd70b"
	frontier := encodeBrokerFrontier(chain, parent, &brokerInProgress{
		BackupID: tail, Chunks: strings.Repeat("a", 64), Through: 2004,
	}).Token
	// The precondition that made the substring wait wrong: the frontier
	// standing inside the tail carries the tail's id.
	if !strings.Contains(frontier, tail) {
		t.Fatalf("frontier token %q does not name the tail; the case below grades nothing", frontier)
	}
	cases := []struct {
		name, pos string
		want      bool
	}{
		{"fully applied through the tail", encodeBrokerPosition(chain, tail).Token, true},
		{"frontier inside the tail", frontier, false},
		{"fully applied through the parent", encodeBrokerPosition(chain, parent).Token, false},
		{"no position yet", "", false},
		{"not a broker token", `{"gtid":"` + tail + `"}`, false},
	}
	for _, c := range cases {
		if got := brokerTailFullyApplied(c.pos, tail); got != c.want {
			t.Errorf("%s: brokerTailFullyApplied(%q) = %v, want %v", c.name, c.pos, got, c.want)
		}
	}
}
