// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The ADR-0185 posture is INSTALL-WIDE; setup used to write only the
// per-table half for the tables of that invocation (audit 2026-08-31 A-2).
//
// # What the pre-fix code actually did (verified, not taken from the filing)
//
//	sluice trigger setup --tables=orders --capture-replicated-writes
//	  → orders' two triggers ALTERed to ENABLE ALWAYS; meta posture TRUE
//	sluice trigger setup --tables=shipments          (flag omitted)
//	  → shipments' triggers created plain; meta posture flipped to FALSE;
//	    NOT ONE statement of the plan mentions orders, whose triggers stay 'A'
//
// The next CDC open reads posture=false, grades orders at 'A' and refuses
// with "the trigger's enablement was flipped by hand … re-run `sluice
// trigger setup`". So the shape is LOUD — but the message misattributes the
// cause to a human who flipped nothing, and the remedy it prescribes is the
// command the operator just ran, which leaves orders at 'A' forever. Two
// shipped docs stated that repair unconditionally (ADR-0185's Decision and
// docs/operator/cdc-streaming.md).
//
// The fix makes the write set cover the grade set: setup reads the capture
// triggers already installed OUTSIDE its --tables and either WIDENS them to
// the requested opt-in posture (more capture is never a loss) or REFUSES to
// narrow them implicitly, naming the tables and the --tables list that
// converges the install. The pins below hold both halves, and the roster
// pin holds the containment property itself.
package pgtrigger

import (
	"sort"
	"strings"
	"testing"
)

// enableAlwaysTargets returns the tables an ENABLE ALWAYS TRIGGER statement
// in the plan targets, sorted and deduplicated.
func enableAlwaysTargets(stmts []string) []string {
	seen := map[string]bool{}
	for _, s := range stmts {
		if !strings.Contains(s, "ENABLE ALWAYS TRIGGER") || !strings.HasPrefix(s, "ALTER TABLE ") {
			continue
		}
		rest := strings.TrimPrefix(s, "ALTER TABLE ")
		ref, _, _ := strings.Cut(rest, " ENABLE ALWAYS TRIGGER")
		_, tbl, _ := strings.Cut(ref, ".")
		seen[strings.Trim(tbl, `"`)] = true
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// enableOriginTargets is the same for the plain-posture ENABLE TRIGGER
// (tgenabled 'O') statements the convergence path emits.
func enableOriginTargets(stmts []string) []string {
	seen := map[string]bool{}
	for _, s := range stmts {
		if !strings.HasPrefix(s, "ALTER TABLE ") || !strings.Contains(s, " ENABLE TRIGGER ") {
			continue
		}
		rest := strings.TrimPrefix(s, "ALTER TABLE ")
		ref, _, _ := strings.Cut(rest, " ENABLE TRIGGER")
		_, tbl, _ := strings.Cut(ref, ".")
		seen[strings.Trim(tbl, `"`)] = true
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func specsFor(names ...string) []tableTriggerSpec {
	specs := make([]tableTriggerSpec, len(names))
	for i, n := range names {
		specs[i] = tableTriggerSpec{Name: n, PKCols: []string{"id"}}
	}
	return specs
}

// installedPair is the two capture triggers of one table at one enablement.
func installedPair(table, enabled string) []installedCaptureTrigger {
	return []installedCaptureTrigger{
		{table: table, name: CaptureTriggerRow, enabled: enabled, fn: CaptureFunctionRow, tgtype: expectedRowTgType},
		{table: table, name: CaptureTriggerTruncate, enabled: enabled, fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
	}
}

// The load-bearing containment property, stated over the END STATE rather
// than over the statement list: after a setup run, EVERY capture trigger
// the door will grade — including those on tables the run did not name —
// stands at the posture the run recorded. A run that cannot reach that
// state must refuse instead of applying. Both halves are asserted here, so
// neither "setup writes less than the door grades" nor "setup narrows
// silently" can come back.
//
// The simulation below is deliberately mechanical: DROP+CREATE yields a
// plain 'O' trigger (PostgreSQL has no ENABLE ALWAYS clause on CREATE
// TRIGGER), then each ALTER in the plan moves it.
func TestSetupPostureEndStateCoversTheDoorGradeSet(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		optIn        bool
		outsideState string
		wantRefusal  bool
		wantPosture  string
	}{
		{name: "opt-in over a plain outside table widens it", optIn: true, outsideState: "O", wantPosture: "A"},
		{name: "opt-in over an already-ALWAYS outside table needs no statement", optIn: true, outsideState: "A", wantPosture: "A"},
		{name: "plain over a plain outside table is already converged", optIn: false, outsideState: "O", wantPosture: "O"},
		{name: "plain over an ALWAYS outside table REFUSES rather than half-converting", optIn: false, outsideState: "A", wantRefusal: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			outside := installedPair("orders", tc.outsideState)
			named := []string{"shipments"}
			err := refuseImplicitPostureNarrowing(outside, named, tc.optIn)
			if tc.wantRefusal {
				if err == nil {
					t.Fatal("setup accepted a run that leaves the install half-converted")
				}
				return
			}
			if err != nil {
				t.Fatalf("setup refused a convergent run: %v", err)
			}
			stmts := renderSetupDDL("public", specsFor(named...), true, CapturePayloadFull, tc.optIn, outside)
			got := simulatePostureEndState(stmts, outside, named)
			for trigger, enabled := range got {
				if enabled != tc.wantPosture {
					t.Errorf("after the plan, %s = %q; want %q — the door grades EVERY sluice-named trigger in the schema against the ONE recorded posture, so any trigger left behind wedges the next open", trigger, enabled, tc.wantPosture)
				}
			}
			// Anti-vacuity: the simulation must have seen both the named
			// table's pair and the outside pair.
			if len(got) != 4 {
				t.Fatalf("simulated %d triggers; want 4 (two tables × row+truncate): %v", len(got), got)
			}
		})
	}
}

// simulatePostureEndState replays the plan's trigger-enablement effects
// over the pre-existing state and returns the final tgenabled per
// "table.trigger".
func simulatePostureEndState(stmts []string, outside []installedCaptureTrigger, named []string) map[string]string {
	state := map[string]string{}
	for _, it := range outside {
		state[it.table+"."+it.name] = it.enabled
	}
	for _, tbl := range named {
		// DROP + CREATE TRIGGER always yields origin-only.
		for _, trg := range []string{CaptureTriggerRow, CaptureTriggerTruncate} {
			state[tbl+"."+trg] = "O"
		}
	}
	for _, s := range stmts {
		if !strings.HasPrefix(s, "ALTER TABLE ") {
			continue
		}
		rest := strings.TrimPrefix(s, "ALTER TABLE ")
		want := "A"
		ref, trg, ok := strings.Cut(rest, " ENABLE ALWAYS TRIGGER ")
		if !ok {
			want = "O"
			ref, trg, ok = strings.Cut(rest, " ENABLE TRIGGER ")
			if !ok {
				continue
			}
		}
		_, tbl, _ := strings.Cut(ref, ".")
		state[strings.Trim(tbl, `"`)+"."+strings.Trim(trg, `"`)] = want
	}
	return state
}

func TestSetupPostureAlignment(t *testing.T) {
	t.Parallel()
	t.Run("opt-in WIDENS the tables it did not name", func(t *testing.T) {
		t.Parallel()
		stmts := renderSetupDDL("public", specsFor("shipments"), true, CapturePayloadFull, true, installedPair("orders", "O"))
		if got, want := enableAlwaysTargets(stmts), []string{"orders", "shipments"}; !equalStrings(got, want) {
			t.Errorf("ENABLE ALWAYS targets = %v; want %v", got, want)
		}
		// Widening must not re-create the outside table's triggers: setup
		// has no PK list for a table it was not asked about, and a
		// DROP+CREATE with an empty TG_ARGV would install a trigger whose
		// own body refuses every row.
		for _, s := range stmts {
			if strings.Contains(s, "CREATE TRIGGER") && strings.Contains(s, `"orders"`) {
				t.Errorf("the plan re-creates the outside table's trigger, which would re-bake an unknown PK list:\n%s", s)
			}
		}
	})

	t.Run("plain render never re-postures an outside table — that direction refuses instead", func(t *testing.T) {
		t.Parallel()
		// The narrowing direction is refused before the render runs, so
		// the plain plan must carry no posture statement at all: no
		// silent widening, and no silent narrowing either.
		for _, enabled := range []string{"O", "A", "D", "R"} {
			stmts := renderSetupDDL("public", specsFor("shipments"), true, CapturePayloadFull, false, installedPair("orders", enabled))
			if got := enableAlwaysTargets(stmts); len(got) != 0 {
				t.Errorf("outside trigger at %q: plain render emitted ENABLE ALWAYS for %v", enabled, got)
			}
			if got := enableOriginTargets(stmts); len(got) != 0 {
				t.Errorf("outside trigger at %q: plain render emitted ENABLE TRIGGER for %v", enabled, got)
			}
		}
	})

	t.Run("widening leaves a DISABLED or REPLICA-only outside trigger to the capture-shape door", func(t *testing.T) {
		t.Parallel()
		// 'D' and 'R' are not a posture disagreement, they are a broken
		// shape the door refuses on by name; silently re-arming one from a
		// run about a different table would erase that signal.
		for _, enabled := range []string{"D", "R"} {
			stmts := renderSetupDDL("public", specsFor("shipments"), true, CapturePayloadFull, true, installedPair("orders", enabled))
			if got, want := enableAlwaysTargets(stmts), []string{"shipments"}; !equalStrings(got, want) {
				t.Errorf("outside trigger at %q: ENABLE ALWAYS targets = %v; want %v", enabled, got, want)
			}
		}
	})
}

// The narrowing REFUSAL: setup must not silently take an install's other
// tables off ENABLE ALWAYS because this invocation omitted the flag — that
// is the F1 silent-capture-gap class, arriving as a side effect of naming a
// different table.
func TestSetupRefusesImplicitPostureNarrowing(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name              string
		requestOptIn      bool
		outside           []installedCaptureTrigger
		named             []string
		wantRefusal       bool
		wantMessageCarves []string
	}{
		{
			name:         "plain run, outside table at ENABLE ALWAYS",
			requestOptIn: false,
			outside:      installedPair("orders", "A"),
			named:        []string{"shipments"},
			wantRefusal:  true,
			wantMessageCarves: []string{
				"orders",
				"--tables=orders,shipments",
				"--capture-replicated-writes",
			},
		},
		{
			name:         "plain run, outside table already plain",
			requestOptIn: false,
			outside:      installedPair("orders", "O"),
			named:        []string{"shipments"},
		},
		{
			name:         "opt-in run, outside table plain — widened, never refused",
			requestOptIn: true,
			outside:      installedPair("orders", "O"),
			named:        []string{"shipments"},
		},
		{
			name:         "no outside tables at all",
			requestOptIn: false,
			outside:      nil,
			named:        []string{"shipments"},
		},
		{
			name:         "a DISABLED outside trigger is not a narrowing — the capture-shape door owns that shape",
			requestOptIn: false,
			outside:      installedPair("orders", "D"),
			named:        []string{"shipments"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := refuseImplicitPostureNarrowing(tc.outside, tc.named, tc.requestOptIn)
			if !tc.wantRefusal {
				if err != nil {
					t.Fatalf("refused a shape that is not a narrowing: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("passed; want a refusal — the install's other tables would silently lose replicated-write capture")
			}
			for _, want := range tc.wantMessageCarves {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal missing %q:\n%v", want, err)
				}
			}
		})
	}
}
