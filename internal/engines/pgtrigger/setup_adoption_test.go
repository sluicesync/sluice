// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"regexp"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/appliershared"
)

// Roadmap item 149b, the Postgres half. The refusal itself needs a real server
// (see setup_adoption_integration_test.go); what lives here are the two checks
// that do not: the pure classification behind the refusal, and the gate that
// keeps [internalTableColumnFloor] honest as the DDL moves.

// TestMissingFloorColumns pins the predicate the refusal turns on, including
// the direction that matters most — a SUPERSET must satisfy the floor, or a
// future release that adds a column would make this engine refuse every source
// an older release set up.
func TestMissingFloorColumns(t *testing.T) {
	floor := []string{"id", "txid", "op"}
	cases := []struct {
		name string
		have []string
		want []string
	}{
		{"exact match", []string{"id", "txid", "op"}, nil},
		{"superset (a newer install)", []string{"id", "txid", "op", "future_col"}, nil},
		{"different order", []string{"op", "id", "txid"}, nil},
		{"a user's table", []string{"id", "note"}, []string{"txid", "op"}},
		{"empty", nil, []string{"id", "txid", "op"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := missingFloorColumns(floor, tc.have)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("missingFloorColumns(%v, %v) = %v; want %v", floor, tc.have, got, tc.want)
			}
		})
	}
}

// TestInternalTableColumnFloorMatchesTheRenderedDDL is the forcing function
// behind [internalTableColumnFloor], and the twin of the sqlite-trigger test of
// the same name. It derives the roster from what renderSetupDDL actually emits,
// so a NEW engine-internal table with no floor entry fails the build (it would
// otherwise be created with IF NOT EXISTS and ungraded — the 149b defect), and
// so does a renamed or removed column (the floor would then refuse healthy
// installs).
//
// An ADDED column also fails it, deliberately: the person adding one has to
// decide whether existing installs are migrated before the floor may move.
//
// Scope, stated rather than implied: this grades the pgtrigger render only. The
// sqlite-trigger engine carries its own copy over its own two transports.
func TestInternalTableColumnFloorMatchesTheRenderedDDL(t *testing.T) {
	rendered := renderSetupDDL("public",
		[]tableTriggerSpec{{Name: "t", PKCols: []string{"id"}}},
		true, CapturePayloadFull, false)

	got := parseCreatedTableColumns(rendered)
	if len(got) < 3 {
		t.Fatalf("parsed %d CREATE TABLE statements out of the render; expected at least 3 "+
			"(anti-vacuity: the parser has probably stopped matching)", len(got))
	}
	if len(got) != len(internalTableColumnFloor) {
		t.Fatalf("render creates %d internal tables %v but the floor has %d entries — every table "+
			"setup creates with IF NOT EXISTS must be graded", len(got), sortedKeys(got), len(internalTableColumnFloor))
	}
	for name, cols := range got {
		floor, ok := internalTableColumnFloor[name]
		if !ok {
			t.Errorf("no floor entry for internal table %q", name)
			continue
		}
		if strings.Join(sortedCopy(cols), ",") != strings.Join(sortedCopy(floor), ",") {
			t.Errorf("table %q: rendered columns %v != floor %v — a rename/removal breaks existing "+
				"installs; an addition needs a migration decision before the floor moves",
				name, cols, floor)
		}
	}
}

// TestInternalTablesAreOnTheControlTableRoster binds the floor to the OTHER
// roster that must know these names: a table sluice creates on a source but
// that the schema readers do not exclude would be enumerated as user data.
func TestInternalTablesAreOnTheControlTableRoster(t *testing.T) {
	for name := range internalTableColumnFloor {
		if !appliershared.IsControlTable(name) {
			t.Errorf("%q is created by trigger setup but is not on the control-table roster", name)
		}
	}
}

var createTableRe = regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS "public"\."([^"]+)"\s*\((.*)`)

// parseCreatedTableColumns extracts {table: [columns]} from rendered DDL. The
// column name is the first token of each line inside the parens — the shape
// every CREATE TABLE in renderSetupDDL uses; CONSTRAINT lines are skipped.
func parseCreatedTableColumns(stmts []string) map[string][]string {
	out := map[string][]string{}
	for _, s := range stmts {
		m := createTableRe.FindStringSubmatch(s)
		if m == nil {
			continue
		}
		var cols []string
		for _, line := range strings.Split(m[2], "\n") {
			f := strings.Fields(strings.TrimSpace(line))
			if len(f) < 2 || f[0] == ")" || strings.HasPrefix(f[0], "CONSTRAINT") {
				continue
			}
			cols = append(cols, strings.Trim(f[0], `"`))
		}
		out[m[1]] = cols
	}
	return out
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func sortedKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
