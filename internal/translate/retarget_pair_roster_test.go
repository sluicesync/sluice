// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Direction roster for the shape-compare TYPE rule tables (roadmap item
// 158). External test package so it can import the engine
// implementations (which themselves import translate) without a cycle —
// the same idiom as the index-NAME roster next door.
//
// Bug 234 closed the index-NAME axis in all six ordered directions at
// once, because the primary key is matched by ROLE and a role is
// structural. The TYPE axis has no such shortcut: every ordered pair of
// storage families needs its own rule table, derived from what the
// TARGET's catalog reads back. So "which directions are covered" is a
// question that has to be answered per pair, and this roster is the
// answer — mechanically, so that a new engine or a new family cannot
// join without someone deciding.

package translate_test

import (
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// pairStatus is what a (source family, target family) direction can be.
type pairStatus string

const (
	// pairIdentity — the two engines share a storage-shape family, so the
	// catalog reads the source's own IR back and no rule is needed.
	pairIdentity pairStatus = "identity"
	// pairRule — a rule table exists and the direction is covered.
	pairRule pairStatus = "rule"
	// pairGap — no rule table. `schema diff` reports PHANTOM type drift on
	// this direction against a target `migrate` itself created. Filed, with
	// the reason, rather than left to be rediscovered.
	pairGap pairStatus = "gap"
)

// retargetPairRoster classifies every ordered pair of storage-shape
// families. The key is "<source family representative>→<target family
// representative>", where the representative is any engine of that
// family — the rules key on the family, not the engine name, which is
// itself pinned below.
//
// # What this gate proves, and what it does not
//
// It proves a DECISION exists for every direction, and that the decision
// matches BEHAVIOUR (a "rule" direction must actually rewrite something;
// an "identity" or "gap" one must not). It does NOT prove a rule table is
// COMPLETE — completeness is a claim about a real target's catalog, and
// the evidence for that is TestSchemaDiffAfterMigrate_* in
// internal/pipeline, which migrates across a real engine pair and asks
// the target itself.
var retargetPairRoster = map[string]struct {
	status pairStatus
	reason string
}{
	// ---- Same-family: identity is faithful ----
	"mysql→mysql":       {pairIdentity, ""},
	"postgres→postgres": {pairIdentity, ""},
	"sqlite→sqlite":     {pairIdentity, ""},

	// ---- Covered ----
	"postgres→mysql": {pairRule, ""},
	"mysql→postgres": {pairRule, ""},

	// ---- Filed gaps, with the reason stated ----
	//
	// All four involve SQLite, whose reader resolves a column through
	// TYPE AFFINITY rather than a declared type (internal/translate/
	// sqlite_affinity.go), so the read-back shape is a function of the
	// declaration text and not of the family alone. Deriving these means
	// the same measured migrate-then-read-back exercise item 158 ran for
	// mysql→postgres, once per direction, against a real SQLite.
	//
	// Until then `sluice schema diff` on any of these pairs still reports
	// phantom column-TYPE drift. The index-NAME and PRIMARY-KEY axes ARE
	// closed for them (Bug 234, matched by role), so this is the residual
	// and not the whole comparison.
	"postgres→sqlite": {pairGap, "no measured SQLite read-back matrix; affinity-resolved, needs its own chunk"},
	"sqlite→postgres": {pairGap, "no measured SQLite read-back matrix; affinity-resolved, needs its own chunk"},
	"mysql→sqlite":    {pairGap, "no measured SQLite read-back matrix; affinity-resolved, needs its own chunk"},
	"sqlite→mysql":    {pairGap, "no measured SQLite read-back matrix; affinity-resolved, needs its own chunk"},
}

// retargetProbeSchema carries one column of every family EITHER rule
// table rewrites, so "did this direction rewrite anything" is a question
// the probe can actually answer regardless of which direction is asked.
func retargetProbeSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "probe",
		Columns: []*ir.Column{
			// PG-native (the postgres→mysql table's arms).
			{Name: "p_uuid", Type: ir.UUID{}},
			{Name: "p_inet", Type: ir.Inet{}},
			{Name: "p_json", Type: ir.JSON{Binary: false}},
			{Name: "p_varbit", Type: ir.Bit{Length: 8, Varying: true}},
			{Name: "p_array", Type: ir.Array{Element: ir.Integer{Width: 32}}},
			// MySQL-native (the mysql→postgres table's arms).
			{Name: "m_tinyint", Type: ir.Integer{Width: 8}},
			{Name: "m_text", Type: ir.Text{Size: ir.TextRegular}},
			{Name: "m_varbinary", Type: ir.Varbinary{Length: 64}},
			{Name: "m_set", Type: ir.Set{Values: []string{"a", "b"}}},
		},
	}}}
}

// familyLabelPreference names each storage family by the engine everyone
// calls it after, so a roster key stays readable and — the load-bearing
// part — STABLE. Clustering alone would label families by whichever
// member [engines.Names] happens to sort first, which is `d1` and
// `mariadb` today and would silently re-key the whole roster the day
// someone registers an engine that sorts earlier.
var familyLabelPreference = []string{"mysql", "postgres", "sqlite"}

// familyRepresentatives clusters every registered engine by storage-shape
// family (behaviourally, via the exported predicate) and returns one
// representative per family plus the full membership, so the roster keys
// stay short while the pin below still reaches every engine.
//
// A family with no preferred label is named after its first member and
// will fail the roster as unclassified — which is the intent: a new
// storage family is exactly the event that needs a decision.
func familyRepresentatives(t *testing.T) (reps []string, members map[string][]string) {
	t.Helper()
	names := engines.Names()
	if len(names) == 0 {
		t.Fatal("no engines registered — the blank imports above should have registered them")
	}
	var groups [][]string
	for _, name := range names {
		placed := false
		for i, group := range groups {
			if translate.SameStorageShapeFamily(name, group[0]) {
				groups[i] = append(group, name)
				placed = true
				break
			}
		}
		if !placed {
			groups = append(groups, []string{name})
		}
	}

	members = map[string][]string{}
	for _, group := range groups {
		label := group[0]
		for _, preferred := range familyLabelPreference {
			if translate.SameStorageShapeFamily(preferred, group[0]) {
				label = preferred
				break
			}
		}
		reps = append(reps, label)
		members[label] = group
	}
	return reps, members
}

// rewrittenColumns runs the real compare-lane pass and counts how many
// probe columns changed type.
func rewrittenColumns(src, tgt string) int {
	in := retargetProbeSchema()
	out := translate.RetargetForShapeCompare(retargetProbeSchema(), src, tgt)
	n := 0
	for i, col := range out.Tables[0].Columns {
		if col.Type.String() != in.Tables[0].Columns[i].Type.String() {
			n++
		}
	}
	return n
}

// TestRetargetPairRosterCoversEveryOrderedFamilyPair is the sibling-sweep
// step for the TYPE axis, made mechanical: every ordered pair of storage
// families is classified, and the classification is checked against what
// the pass actually does.
func TestRetargetPairRosterCoversEveryOrderedFamilyPair(t *testing.T) {
	reps, members := familyRepresentatives(t)

	var identities, rules, gaps int
	for _, src := range reps {
		for _, tgt := range reps {
			key := src + "→" + tgt
			entry, classified := retargetPairRoster[key]
			if !classified {
				t.Errorf("storage-family direction %q is not classified in retargetPairRoster. Decide: does the "+
					"target's catalog read this source's IR back unchanged (identity), does a rule table exist "+
					"(rule), or is this a filed gap where `schema diff` still reports phantom column-TYPE drift "+
					"(gap, with a reason)? Engines in these families: %v → %v",
					key, members[src], members[tgt])
				continue
			}

			sameFamily := translate.SameStorageShapeFamily(src, tgt)
			n := rewrittenColumns(src, tgt)

			switch entry.status {
			case pairIdentity:
				if !sameFamily {
					t.Errorf("%s is classified identity but the two engines are NOT one storage-shape family", key)
				}
				if n != 0 {
					t.Errorf("%s is classified identity but the compare-lane pass rewrote %d probe column(s); "+
						"a same-family pair must round-trip", key, n)
				}
				identities++
			case pairRule:
				if sameFamily {
					t.Errorf("%s is classified rule but the two engines share a storage-shape family; "+
						"it should be identity", key)
				}
				if n == 0 {
					t.Errorf("%s is classified rule but the compare-lane pass rewrote NOTHING on a probe "+
						"carrying every family either table has an arm for — the rule table for this direction "+
						"is gone or no longer reached", key)
				}
				rules++
			case pairGap:
				if entry.reason == "" {
					t.Errorf("%s is classified gap with no reason; a gap without a stated reason is "+
						"indistinguishable from an oversight", key)
				}
				if n != 0 {
					t.Errorf("%s is classified gap but the compare-lane pass rewrote %d probe column(s) — "+
						"a rule table landed for this direction and the roster was not updated. Promote it to "+
						"rule and give it a measured integration matrix", key, n)
				}
				gaps++
			default:
				t.Errorf("%s carries unknown status %q", key, entry.status)
			}
		}
	}

	// Anti-vacuity on all three classes: a roster that collapsed to one
	// status could not distinguish them, and the gap count is the number
	// this chunk deliberately left open — if it reaches zero the roster
	// has stopped tracking anything.
	if identities < 3 {
		t.Errorf("only %d identity directions; expected one per storage family", identities)
	}
	if rules < 2 {
		t.Errorf("only %d rule directions; postgres→mysql and mysql→postgres both have rule tables", rules)
	}
	if gaps < 4 {
		t.Errorf("only %d gap directions recorded; the four SQLite pairs are open and this roster is what "+
			"keeps that visible", gaps)
	}
}

// TestRetargetPairRosterHasNoStaleEntries is the other direction: a
// classification for a family pair no registered engine can form is a
// claim nobody checks.
func TestRetargetPairRosterHasNoStaleEntries(t *testing.T) {
	reps, _ := familyRepresentatives(t)
	live := map[string]bool{}
	for _, src := range reps {
		for _, tgt := range reps {
			live[src+"→"+tgt] = true
		}
	}
	for key := range retargetPairRoster {
		if !live[key] {
			t.Errorf("retargetPairRoster classifies %q, which no pair of registered engines can form — "+
				"either the representative name changed or the entry is stale", key)
		}
	}
}

// TestRetargetRulesKeyOnTheFamilyNotTheEngineName is the pin the roster's
// "any engine of that family" shortcut rests on. Bug 234's own commit
// message records the cost of getting this wrong the other way: a
// literal-name match silently missed the vitess flavor.
func TestRetargetRulesKeyOnTheFamilyNotTheEngineName(t *testing.T) {
	_, members := familyRepresentatives(t)
	for rep, group := range members {
		want := rewrittenColumns(rep, "postgres")
		for _, name := range group {
			if got := rewrittenColumns(name, "postgres"); got != want {
				t.Errorf("engine %q rewrote %d probe columns against a postgres target; its family "+
					"representative %q rewrote %d. The rule tables must key on the storage family, so every "+
					"flavor and trigger variant gets the same expected side", name, got, rep, want)
			}
		}
		// And the same on the target side.
		want = rewrittenColumns("postgres", rep)
		for _, name := range group {
			if got := rewrittenColumns("postgres", name); got != want {
				t.Errorf("engine %q as a TARGET of a postgres source rewrote %d probe columns; its family "+
					"representative %q rewrote %d", name, got, rep, want)
			}
		}
	}
}
