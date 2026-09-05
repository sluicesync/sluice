// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"os"
	"strings"
	"testing"
)

// TestCDCRelationScopeRoster_EverySchemaRaceIsScopeGated requires every
// schema-race refusal on the RelationMessage path to sit behind the scope
// guard.
//
// WHY (UPR-2, from the pgcopydb fork review). The four DML dispatch sites have
// always consulted `schemaInScope`; the RelationMessage path consulted
// nothing. So a DDL on a relation the reader had been told to ignore — a
// schema outside `--include-schema`, or a table removed with
// `--exclude-table` — still ran `checkSeededSchemaRace` and `checkSchemaRace`,
// either of which can end the stream. The refusal then named a table the
// operator had deliberately excluded, which is a refusal firing on a working
// configuration: the tier-2 loud-failure class, not silent loss.
//
// The reviewer proved it by EXECUTION rather than by reading — hand-encoding
// pgoutput `R` payloads and driving them through `dispatchWAL` with a scope
// predicate set — which is why this is a confirmed defect and not a
// plausible-looking one.
//
// WHY A ROSTER RATHER THAN ONE TEST. `dispatchWAL` CARRIED the V1 and V2
// protocol arms as byte-for-byte duplicated blocks — the sibling pair this
// repo keeps paying for, where a fix applied to one arm looks complete, reads
// complete, and leaves the other exactly as it was. The fix therefore
// EXTRACTED the guarded refusals into a single callee rather than inlining a
// guard into each arm: duplication removed beats duplication policed.
//
// So this grades both halves of that arrangement — the callee gates, and both
// arms still route through it. The second half is not decoration: an arm that
// quietly stopped calling the callee would leave the first half green, because
// every site the first half inspects lives inside the callee.
//
// WHAT IT REACHES, stated so the name cannot be read as broader than it is:
// calls to the two named schema-race helpers inside cdc_reader.go, plus the
// count of arms routing through the gated callee. It does NOT prove the
// PREDICATE is right — that is TestRelationInScope_SemanticsUPR2 below — and
// it cannot see a refusal added under a different helper name. The
// anti-vacuity floors catch the first of those; nothing catches the second.
func TestCDCRelationScopeRoster_EverySchemaRaceIsScopeGated(t *testing.T) {
	const file = "cdc_reader.go"
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	lines := strings.Split(string(b), "\n")

	// The two refusals that can end the stream from the relation path. They
	// now live in ONE callee (gradeRelationSchemaRace) rather than being
	// duplicated into both protocol arms — so this roster grades two things:
	// that the callee gates, and that BOTH arms still route through it.
	guarded := []string{"checkSeededSchemaRace(relations", "checkSchemaRace(relations"}

	var sites, gated int
	var ungated []string
	for i, ln := range lines {
		trimmed := strings.TrimSpace(ln)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		var isSite bool
		for _, g := range guarded {
			if strings.Contains(ln, g) {
				isSite = true
				break
			}
		}
		if !isSite {
			continue
		}
		sites++
		// Walk back a short window for the enclosing scope guard. The guard
		// wraps the pair, so it sits within a handful of lines above.
		start := max(0, i-14)
		if strings.Contains(strings.Join(lines[start:i], "\n"), "relationInScope(") {
			gated++
			continue
		}
		ungated = append(ungated, strings.TrimSpace(ln))
	}

	// Anti-vacuity. A rename, or a refactor moving the refusals out of this
	// file, makes "every site is gated" trivially true — which is the failure
	// this floor exists to convert into a red.
	if sites < 2 {
		t.Fatalf("found only %d schema-race call site(s) in %s; there are TWO (both inside "+
			"gradeRelationSchemaRace, which is the point — they used to be duplicated into each "+
			"protocol arm). The walk has broken, or the refusals moved.", sites, file)
	}

	// The other half: both protocol arms must still route through the gated
	// callee. Extraction removed the duplication that made the original
	// one-arm miss possible, and this is what stops it coming back — an arm
	// that stopped calling the helper would leave the roster above green,
	// because the sites it grades all live inside the helper.
	callers := strings.Count(string(b), "r.gradeRelationSchemaRace(relations")
	if callers < 2 {
		t.Errorf("only %d protocol arm(s) call gradeRelationSchemaRace; the V1 and V2 arms are the "+
			"sibling pair UPR-2 was missed in, so both must route through it", callers)
	}

	for _, u := range ungated {
		t.Errorf("schema-race refusal is not scope-gated:\n    %s\n\n"+
			"This refusal can end the stream. Ungated, it fires for relations the reader was told to "+
			"ignore — a DDL in an unselected schema, or on an --exclude-table'd table — and its remedy "+
			"names a table the operator deliberately removed from scope (UPR-2).", u)
	}
	if len(ungated) > 0 {
		t.Logf("%d of %d schema-race sites gated", gated, sites)
	}
}

// TestRelationInScope_SemanticsUPR2 grades the PREDICATE the roster above
// only proves is present.
//
// The two halves answer different questions and both matter: schemaInScope
// answers "is this namespace selected", scopeAllowed answers "is this table
// selected" — strictly narrower, because --exclude-table removes a table from
// a schema that IS in scope. Gating on the schema alone would have left the
// exclude-table half of UPR-2 open while looking fixed.
func TestRelationInScope_SemanticsUPR2(t *testing.T) {
	t.Run("an unfiltered reader is unchanged", func(t *testing.T) {
		r := &CDCReader{schema: "public"}
		if !r.relationInScope("public", "anything") {
			t.Error("a reader with no table filter must allow every table in its bound schema — " +
				"a nil predicate has to keep pre-UPR-2 behaviour byte-identical")
		}
		if r.relationInScope("other", "anything") {
			t.Error("single-schema reader allowed a relation outside its bound schema")
		}
	})

	t.Run("the table filter narrows within an in-scope schema", func(t *testing.T) {
		r := &CDCReader{
			schema:       "public",
			scopeAllowed: func(_, table string) bool { return table != "excluded" },
		}
		if !r.relationInScope("public", "kept") {
			t.Error("an included table in the bound schema was rejected")
		}
		if r.relationInScope("public", "excluded") {
			t.Error("an --exclude-table'd table passed the guard — this is the half a schema-only " +
				"gate would miss while appearing to fix UPR-2")
		}
	})

	t.Run("schema exclusion wins regardless of the table predicate", func(t *testing.T) {
		r := &CDCReader{
			schema:           "public",
			cdcSchemaInScope: func(s string) bool { return s == "sales" },
			scopeAllowed:     func(_, _ string) bool { return true },
		}
		if r.relationInScope("analytics", "t") {
			t.Error("a relation in an unselected schema passed because the table predicate said yes; " +
				"the schema check must gate first")
		}
		if !r.relationInScope("sales", "t") {
			t.Error("a relation in a selected schema was rejected")
		}
	})
}
