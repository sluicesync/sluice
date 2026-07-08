// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestApplyTableFilter_DropsSequencesOwnedByFilteredTables pins the
// audit N-4 fix: a standalone sequence whose owning table the filter
// excluded must be pruned from schema.Sequences (its `ALTER SEQUENCE …
// OWNED BY` would otherwise reference a table the filter left
// uncreated → 42P01, deterministically failing the copy-table-subset
// path), while owned-by-included and unowned sequences pass through.
// Both filter directions are exercised — include and exclude reach the
// same Allows predicate but are distinct operator surfaces.
func TestApplyTableFilter_DropsSequencesOwnedByFilteredTables(t *testing.T) {
	newSchema := func() *ir.Schema {
		return &ir.Schema{
			Tables: []*ir.Table{
				{Name: "keep_me"},
				{Name: "drop_me"},
			},
			Sequences: []*ir.Sequence{
				{Name: "owned_by_kept_seq", OwnedByTable: "keep_me", OwnedByColumn: "id"},
				{Name: "owned_by_dropped_seq", OwnedByTable: "drop_me", OwnedByColumn: "id"},
				{Name: "unowned_seq"},
			},
		}
	}
	seqNames := func(s *ir.Schema) []string {
		out := make([]string, 0, len(s.Sequences))
		for _, seq := range s.Sequences {
			out = append(out, seq.Name)
		}
		return out
	}

	for _, dir := range []struct {
		name             string
		include, exclude []string
	}{
		{name: "include direction", include: []string{"keep_me"}},
		{name: "exclude direction", exclude: []string{"drop_me"}},
	} {
		dir := dir
		t.Run(dir.name, func(t *testing.T) {
			// Capture the WARN: the drop must be loud and name the
			// sequence + its excluded owner.
			var logBuf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
			t.Cleanup(func() { slog.SetDefault(prev) })

			filter, err := NewTableFilter(dir.include, dir.exclude)
			if err != nil {
				t.Fatalf("NewTableFilter: %v", err)
			}
			schema := newSchema()
			if err := ApplyTableFilter(context.Background(), schema, filter); err != nil {
				t.Fatalf("ApplyTableFilter: %v", err)
			}

			if len(schema.Tables) != 1 || schema.Tables[0].Name != "keep_me" {
				t.Fatalf("Tables = %v; want [keep_me]", schema.Tables)
			}
			got := seqNames(schema)
			want := []string{"owned_by_kept_seq", "unowned_seq"}
			if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
				t.Errorf("Sequences = %v; want %v (owned-by-excluded dropped, others kept)", got, want)
			}
			for _, sub := range []string{"owned_by_dropped_seq", "drop_me.id"} {
				if !strings.Contains(logBuf.String(), sub) {
					t.Errorf("WARN log %q missing %q — the drop must name the sequence and its excluded owner", logBuf.String(), sub)
				}
			}
		})
	}

	t.Run("empty filter is a no-op on sequences", func(t *testing.T) {
		schema := newSchema()
		if err := ApplyTableFilter(context.Background(), schema, TableFilter{}); err != nil {
			t.Fatalf("ApplyTableFilter: %v", err)
		}
		if len(schema.Sequences) != 3 {
			t.Errorf("Sequences pruned by an empty filter: %v", seqNames(schema))
		}
	})
}
