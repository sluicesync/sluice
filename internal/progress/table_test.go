// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"strings"
	"testing"
)

func TestTable_StructureAndContent(t *testing.T) {
	headers := []string{"NAME", "PLUGIN", "ACTIVE"}
	rows := [][]string{
		{"sluice_main", "pgoutput", "yes"},
		{"sluice_old", "pgoutput", "no"},
	}
	got := Table("Replication slots (2)", headers, rows, 2)

	for _, want := range []string{
		"Replication slots (2)",    // title
		"NAME", "PLUGIN", "ACTIVE", // headers
		"sluice_main", "sluice_old", // row names
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Table() output missing %q\n---\n%s", want, got)
		}
	}
	// Rounded-border grid is present (box-drawing runes are in the default
	// console font — see the ADR-0155 tofu note in style.go).
	if !strings.ContainsAny(got, "╭╮╰╯│┼") {
		t.Errorf("Table() output has no border runes\n---\n%s", got)
	}
	// Title sits above the grid.
	if ti, bi := strings.Index(got, "Replication slots"), strings.IndexAny(got, "╭"); ti < 0 || bi < 0 || ti > bi {
		t.Errorf("title should render above the border (title@%d border@%d)", ti, bi)
	}
}

// activeCol out of range must not panic and must still render every row.
func TestTable_ActiveColDisabled(t *testing.T) {
	got := Table("t", []string{"A", "B"}, [][]string{{"1", "2"}}, -1)
	if !strings.Contains(got, "1") || !strings.Contains(got, "2") {
		t.Errorf("Table() with activeCol=-1 dropped cells\n---\n%s", got)
	}
}
