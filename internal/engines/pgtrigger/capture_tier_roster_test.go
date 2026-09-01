// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The CAPTURE-TIER ROSTER (audit 2026-08-31, the gate D-1 and A-1 share).
//
// # The class both findings belong to
//
// pgtrigger installs capture artifacts of several kinds — per-table row
// triggers, per-table TRUNCATE triggers, and (on the event-trigger tier)
// one event trigger per DDL arm. Every property that must hold for "the
// install captures what it claims to" is per-ARTIFACT, and both findings
// were an artifact that got left out of a rule everybody believed was
// universal:
//
//   - A-1: `--capture-replicated-writes` set the two per-table triggers
//     ENABLE ALWAYS and left the event trigger plain, so one flag produced
//     two disagreeing capture postures — replica-role DML captured,
//     replica-role DDL invisible.
//   - D-1: the `ddl_command_end` arm's WHEN TAG list named two DROP tags
//     that `pg_event_trigger_ddl_commands()` can never report, so the tag
//     list read as coverage the function could not deliver.
//
// # How the universe is derived (not hand-listed)
//
// From the statements [renderSetupDDL] actually emits, parsed. Every
// `CREATE TRIGGER` and `CREATE EVENT TRIGGER` in the rendered plan enters
// the roster; a trigger name this file does not classify FAILS, so adding
// a third event-trigger arm (or a fourth per-table trigger) without
// deciding its posture and its recordability is a build failure rather
// than a silent omission. The posture axis is derived the same way: the
// ENABLE ALWAYS statements are parsed out of the opt-in render and matched
// against the created set.
//
// # WHAT THIS GATE REACHES, stated so the name cannot be read as broader
// than the truth
//
// The RENDERED SQL, and only that. It proves the plan sluice emits is
// internally coherent; it says nothing about what a given database has
// installed (that is the capture-shape door, [gradeCaptureShape], graded
// against the same roster via [eventTierRoster]) and nothing about what
// PostgreSQL does with those statements. The server-behaviour half is
// deliberately elsewhere and deliberately real:
// TestCaptureDropTier_DDLCommandEndIsBlindToDrops measures that
// ddl_command_end cannot see a DROP, and
// TestCaptureReplicatedWrites_ReplicaRoleDDLIsCaptured measures that an
// 'O' event trigger does not fire under replica role while an 'A' one
// does. A green roster with those two red would be a coherent plan built
// on a false premise.
//
// The TAG-completeness arm has a narrower derivation than the trigger arm
// and says so: its universe is a DECLARED set of DDL tags sluice considers
// relevant, each of which must be rendered-and-recordable or exempt with a
// written reason. That list is hand-maintained — the gate cannot notice a
// DDL tag nobody has thought about.

package pgtrigger

import (
	"regexp"
	"sort"
	"strings"
	"testing"
)

// captureTierKind is what an emitted capture trigger is FOR. Every created
// trigger must map to one; an unmapped name fails the roster.
type captureTierKind int

const (
	tierRowDML captureTierKind = iota
	tierTruncate
	tierDDLCommandEnd
	tierSQLDrop
)

// captureTierClassification is the roster's decision table, keyed by the
// trigger name setup renders. A new trigger name must be added here — with
// its posture answer — or the roster fails.
var captureTierClassification = map[string]struct {
	kind captureTierKind
	// alwaysUnderOptIn: must the --capture-replicated-writes render set
	// this trigger ENABLE ALWAYS? TRUE for every capture trigger today —
	// one flag, one posture (A-1). A future FALSE needs a written reason
	// here, which is the point of the field.
	alwaysUnderOptIn bool
	why              string
}{
	CaptureTriggerRow: {
		kind: tierRowDML, alwaysUnderOptIn: true,
		why: "replica-role INSERT/UPDATE/DELETE is the write class the opt-in exists for (ADR-0185)",
	},
	CaptureTriggerTruncate: {
		kind: tierTruncate, alwaysUnderOptIn: true,
		why: "a subscriber applies TRUNCATE through its own executor path; leaving it plain loses replicated TRUNCATE",
	},
	CaptureTriggerDDL: {
		kind: tierDDLCommandEnd, alwaysUnderOptIn: true,
		why: "audit A-1: an 'O' event trigger does not fire under replica role, so replica-role DDL would be invisible while replica-role DML is captured",
	},
	CaptureTriggerDrop: {
		kind: tierSQLDrop, alwaysUnderOptIn: true,
		why: "audit A-1 + D-1: the drop arm is a capture tier like any other; a replica-role DROP of a captured table must refuse, not vanish",
	},
}

// ddlCommandEndRecordableTags are the command tags
// pg_event_trigger_ddl_commands() actually reports rows for, so a tag on
// the ddl_command_end arm must be one of these. Measured, not assumed —
// see renderCaptureDropFunction's doc and the real-PG premise pin.
var ddlCommandEndRecordableTags = map[string]bool{
	"ALTER TABLE":  true,
	"CREATE TABLE": true,
	"CREATE INDEX": true,
}

// relevantDDLTags is the DECLARED universe for the completeness arm: DDL
// classes sluice has decided about. Each must be either recordable on the
// rendered ddl_command_end arm, or carry an exemption below. Hand
// maintained by construction (a tag nobody has thought about cannot be
// here) — the honest scope note is in the file header.
var relevantDDLTags = []string{"ALTER TABLE", "CREATE TABLE", "CREATE INDEX", "DROP TABLE", "DROP INDEX"}

// ddlTagExemptions record why a relevant tag is NOT on the
// ddl_command_end arm. Both entries are D-1's findings.
var ddlTagExemptions = map[string]string{
	"DROP TABLE": "pg_event_trigger_ddl_commands() returns zero rows for a DROP (measured); a dropped CAPTURED table is recorded by the sql_drop arm instead, " +
		"whose predicate is the dropped-object set rather than the tag (so DROP SCHEMA … CASCADE reaches it too)",
	"DROP INDEX": "same zero-row measurement, and deliberately not moved to the sql_drop arm either: sluice never forwards index DDL over CDC, " +
		"so an index drop cannot change the row shape the applier writes and a refusal would halt a stream for nothing",
}

var (
	createTriggerRe      = regexp.MustCompile(`(?m)^CREATE TRIGGER "([^"]+)"`)
	createEventTriggerRe = regexp.MustCompile(`(?m)^CREATE EVENT TRIGGER "([^"]+)" ON (\w+)`)
	alwaysTableTriggerRe = regexp.MustCompile(`(?m)^ALTER TABLE .* ENABLE ALWAYS TRIGGER "([^"]+)"`)
	alwaysEventTriggerRe = regexp.MustCompile(`(?m)^ALTER EVENT TRIGGER "([^"]+)" ENABLE ALWAYS`)
)

// renderedTriggers parses the plan for every capture trigger it creates,
// returning name → the event it is bound to ("" for a table trigger; the
// event name for an event trigger), and the set of names the plan sets
// ENABLE ALWAYS.
func renderedTriggers(stmts []string) (created map[string]string, always map[string]bool) {
	created, always = map[string]string{}, map[string]bool{}
	for _, s := range stmts {
		if m := createTriggerRe.FindStringSubmatch(s); m != nil {
			created[m[1]] = ""
		}
		if m := createEventTriggerRe.FindStringSubmatch(s); m != nil {
			created[m[1]] = m[2]
		}
		if m := alwaysTableTriggerRe.FindStringSubmatch(s); m != nil {
			always[m[1]] = true
		}
		if m := alwaysEventTriggerRe.FindStringSubmatch(s); m != nil {
			always[m[1]] = true
		}
	}
	return created, always
}

func sortedTierNames[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestCaptureTierRoster_EveryEmittedTrigger is the gate.
func TestCaptureTierRoster_EveryEmittedTrigger(t *testing.T) {
	t.Parallel()
	specs := []tableTriggerSpec{
		{Name: "orders", PKCols: []string{"id"}},
		{Name: "line_items", PKCols: []string{"tenant_id", "order_id"}},
	}
	plain := renderSetupDDL("public", specs, true, CapturePayloadFull, false, nil)
	optIn := renderSetupDDL("public", specs, true, CapturePayloadFull, true, nil)

	plainCreated, plainAlways := renderedTriggers(plain)
	optInCreated, optInAlways := renderedTriggers(optIn)

	// Anti-vacuity floor. Two tables × {row, truncate} + the two event
	// trigger arms = 6 creates, 4 distinct names. A parser that stopped
	// matching grades nothing while reporting green.
	if len(plainCreated) < 4 {
		t.Fatalf("parsed only %d distinct capture trigger(s) %v from the plain render (floor 4: row, truncate, ddl_command_end, sql_drop) — "+
			"the roster's parser has stopped seeing what setup emits", len(plainCreated), sortedTierNames(plainCreated))
	}
	events := 0
	for _, ev := range plainCreated {
		if ev != "" {
			events++
		}
	}
	if events < 2 {
		t.Fatalf("parsed only %d event trigger(s) from the plain render (floor 2: ddl_command_end + sql_drop): %v", events, plainCreated)
	}

	t.Run("every emitted trigger is classified (fail-closed on a new tier)", func(t *testing.T) {
		for _, created := range []map[string]string{plainCreated, optInCreated} {
			for name := range created {
				if _, ok := captureTierClassification[name]; !ok {
					t.Errorf("renderSetupDDL creates capture trigger %q, which the capture-tier roster does not classify — "+
						"decide its posture under --capture-replicated-writes and its recordability, then add it to captureTierClassification "+
						"(an unclassified tier is exactly how A-1 and D-1 happened)", name)
				}
			}
		}
		// The reverse direction: a classification for a trigger the render
		// no longer emits is a stale entry, and a roster grading ghosts is
		// the vacuity this floor exists to prevent.
		for name := range captureTierClassification {
			if _, ok := optInCreated[name]; !ok {
				t.Errorf("captureTierClassification names %q, which renderSetupDDL no longer creates — stale roster entry", name)
			}
		}
	})

	t.Run("the plain render sets NOTHING ENABLE ALWAYS", func(t *testing.T) {
		if len(plainAlways) != 0 {
			t.Errorf("the default posture render sets %v ENABLE ALWAYS — the opt-in leaked into every install", sortedTierNames(plainAlways))
		}
	})

	t.Run("the opt-in render sets EVERY capture trigger ENABLE ALWAYS", func(t *testing.T) {
		// Per-table triggers are created once per table, so the ALWAYS set
		// must cover each NAME; the per-table multiplicity is checked below.
		for name, class := range captureTierClassification {
			switch {
			case class.alwaysUnderOptIn && !optInAlways[name]:
				t.Errorf("--capture-replicated-writes does not set %q ENABLE ALWAYS, but the roster says it must (%s) — "+
					"the install's capture tiers would disagree under session_replication_role=replica (A-1)", name, class.why)
			case !class.alwaysUnderOptIn && optInAlways[name]:
				t.Errorf("--capture-replicated-writes sets %q ENABLE ALWAYS while the roster exempts it (%s) — one of the two is wrong", name, class.why)
			}
		}
		// Multiplicity: the per-table pair needs one ALTER per TABLE, not
		// one per name. Counted from the statements so a render that
		// enables the first table's triggers only cannot pass.
		perTable := 0
		for _, s := range optIn {
			if alwaysTableTriggerRe.MatchString(s) {
				perTable++
			}
		}
		if want := len(specs) * 2; perTable != want {
			t.Errorf("the opt-in render emits %d per-table ENABLE ALWAYS ALTER(s), want %d (row + truncate for each of %d tables)", perTable, want, len(specs))
		}
	})

	t.Run("every event-trigger tag is recordable by its own arm", func(t *testing.T) {
		for _, s := range optIn {
			m := createEventTriggerRe.FindStringSubmatch(s)
			if m == nil {
				continue
			}
			name, event := m[1], m[2]
			tags := eventTriggerTagRe.FindStringSubmatch(s)
			switch event {
			case "ddl_command_end":
				if tags == nil {
					t.Errorf("event trigger %q on ddl_command_end has no WHEN TAG list — it would fire (and record) for every DDL in the database", name)
					continue
				}
				for _, raw := range strings.Split(tags[1], ",") {
					tag := strings.Trim(strings.TrimSpace(raw), "'")
					if !ddlCommandEndRecordableTags[tag] {
						t.Errorf("event trigger %q watches tag %q, which pg_event_trigger_ddl_commands() does not report rows for — "+
							"the TAG list reads broader than the truth (D-1). Either remove it or record it through the arm that CAN see it", name, tag)
					}
				}
			case "sql_drop":
				// Deliberately unfiltered: the dropped-object set, not the
				// tag, decides whether a captured relation died (DROP
				// SCHEMA … CASCADE reaches it). A tag filter here would
				// re-introduce D-1's shape.
				if tags != nil {
					t.Errorf("event trigger %q on sql_drop carries a WHEN TAG list (%s) — the arm is unfiltered on purpose; "+
						"a captured table can also die by DROP SCHEMA … CASCADE or DROP OWNED BY", name, tags[1])
				}
			default:
				t.Errorf("event trigger %q is bound to event %q, which the capture-tier roster does not classify — "+
					"state which context function records it before shipping it", name, event)
			}
		}
	})

	t.Run("every relevant DDL tag is rendered-recordable or exempt with a reason", func(t *testing.T) {
		rendered := map[string]bool{}
		for _, s := range optIn {
			if createEventTriggerRe.FindStringSubmatch(s) == nil {
				continue
			}
			if tags := eventTriggerTagRe.FindStringSubmatch(s); tags != nil {
				for _, raw := range strings.Split(tags[1], ",") {
					rendered[strings.Trim(strings.TrimSpace(raw), "'")] = true
				}
			}
		}
		for _, tag := range relevantDDLTags {
			if rendered[tag] {
				if why, exempt := ddlTagExemptions[tag]; exempt {
					t.Errorf("tag %q is BOTH rendered on the ddl_command_end arm and exempted (%q) — the two claims contradict", tag, why)
				}
				continue
			}
			if ddlTagExemptions[tag] == "" {
				t.Errorf("DDL tag %q is neither watched by a rendered event trigger nor carries a written exemption — "+
					"decide whether it is captured and say so where the roster can read it", tag)
			}
		}
	})
}
