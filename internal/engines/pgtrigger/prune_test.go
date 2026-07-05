// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import "testing"

// TestAppliedLastID covers the token decode `sluice trigger prune` uses to derive
// the prune bound from the target's durable frontier, through this engine's codec
// adapter. The pure codec logic is pinned in the shared triggercdc package; this
// asserts the pgtrigger wiring (prefix + single-engine accept). The Prune DELETE
// itself needs a real PG and lives in the pipeline integration suite.
//
// The batched-prune helpers (InBatches / Bookkeeper) now live in
// internal/engines/internal/triggercdc; their pins moved there with them.
func TestAppliedLastID(t *testing.T) {
	got, err := AppliedLastID(`{"last_id":99}`)
	if err != nil {
		t.Fatalf("AppliedLastID valid token: %v", err)
	}
	if got != 99 {
		t.Errorf("AppliedLastID = %d; want 99", got)
	}

	if _, err := AppliedLastID(""); err == nil {
		t.Error("AppliedLastID(empty) returned nil; want a loud error")
	}
	if _, err := AppliedLastID("{bad"); err == nil {
		t.Error("AppliedLastID(malformed) returned nil; want a loud error")
	}
	if _, err := AppliedLastID(`{"last_id":-5}`); err == nil {
		t.Error("AppliedLastID(negative) returned nil; want a loud error")
	}
	// A FOREIGN token that unmarshals cleanly (a vanilla-PG pgoutput {slot,lsn},
	// a broker envelope) must REFUSE — not silently decode to last_id=0.
	for _, foreign := range []string{
		`{"slot":"sluice_slot","lsn":"0/16B3748"}`,
		`{"chain_id":"c1","segment":3}`,
	} {
		if _, err := AppliedLastID(foreign); err == nil {
			t.Errorf("AppliedLastID(%q) returned nil; want a loud refuse (no last_id key)", foreign)
		}
	}
}
