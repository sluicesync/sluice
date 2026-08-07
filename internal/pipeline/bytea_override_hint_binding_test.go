// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/translate"
)

// TestBinaryOverrideHintMatchesTheCDCRefusal binds `schema preview`'s
// advisory hint to the sync-side refusal that makes the hint conditional.
//
// The two had drifted apart in the shape this repo keeps paying for: item
// 135 / audit finding B-2 taught `sync` to REFUSE a binary `--type-override`
// (the CDC lane cannot tell an override-made-binary column from a natively
// binary one, so cold-start and CDC would store different byte counts for
// the same cell), and `schema preview` went on recommending
// `--type-override users.id=binary_uuid` with no hint that one of the two
// commands would reject it. An operator following sluice's own advice got a
// refusal from sluice.
//
// Two facts can each be pinned and still leave the ARGUMENT unpinned — so
// this does not re-assert either side. It asserts the BINDING: the caveat
// fires for exactly the aliases the preflight refuses. Break either side and
// this fails.
func TestBinaryOverrideHintMatchesTheCDCRefusal(t *testing.T) {
	aliases := translate.SuggestedOverrideAliases()
	if len(aliases) < 3 {
		t.Fatalf("the advisory-hint registry yielded only %d suggested aliases — the roster broke, and a "+
			"near-empty roster agrees with any refusal", len(aliases))
	}

	var caveated int
	for alias, caveat := range aliases {
		refused := preflightBinaryTypeOverrideOnCDC([]config.Mapping{
			{Table: "t", Column: "c", TargetType: alias},
		}) != nil
		warns := strings.Contains(caveat, "REFUSES")
		if warns {
			caveated++
		}

		switch {
		case refused && !warns:
			t.Errorf("`schema preview` recommends --type-override ...=%s with no caveat, but a sync run "+
				"REFUSES it (preflightBinaryTypeOverrideOnCDC). The operator would be following sluice's "+
				"own advice into a refusal — add the caveat, or stop suggesting the alias.", alias)
		case warns && !refused:
			t.Errorf("the hint for alias %q warns that sync refuses it, but preflightBinaryTypeOverrideOnCDC "+
				"accepts it — the warning is now false and would send operators to `migrate` for no reason.", alias)
		}
	}

	// Anti-vacuity: at least one alias must actually be on the binary side,
	// or the whole check passes by having nothing to check. `binary_uuid` is
	// that alias today.
	if caveated == 0 {
		t.Error("no suggested alias resolves to a binary target type, so this gate proved nothing — if the " +
			"binary_uuid hint was removed, remove this test with it; if the caveat wiring broke, fix it")
	}
}
