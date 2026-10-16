// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"strings"
	"testing"
)

// TestFanoutFilterTally_RefusesOnlyWhenEveryNamespaceWasFilteredEmpty pins
// the run-wide verdict: tables existed (the push-down was asked) and the
// filter kept none in any namespace → refuse; one namespace kept a table →
// the other namespaces' skips stand; nothing was asked (no push-down, or no
// tables anywhere) → nothing to refuse; nil (single-namespace) → never.
func TestFanoutFilterTally_RefusesOnlyWhenEveryNamespaceWasFilteredEmpty(t *testing.T) {
	var none *FanoutFilterTally
	none.Add(3, 0)
	if err := none.Refuse(); err != nil {
		t.Fatalf("nil tally refused: %v", err)
	}

	all := &FanoutFilterTally{}
	all.Add(2, 0)
	all.Add(0, 0)
	all.Add(1, 0)
	err := all.Refuse()
	if err == nil || !strings.Contains(err.Error(), "table filter excluded every source table") {
		t.Fatalf("every namespace filtered empty: want the filter refusal; got %v", err)
	}

	one := &FanoutFilterTally{}
	one.Add(2, 0)
	one.Add(1, 0)
	one.Add(0, 1)
	if err := one.Refuse(); err != nil {
		t.Fatalf("one namespace kept a table: want no refusal; got %v", err)
	}

	empty := &FanoutFilterTally{}
	empty.Add(0, 0)
	if err := empty.Refuse(); err != nil {
		t.Fatalf("nothing asked: want no refusal; got %v", err)
	}
}
