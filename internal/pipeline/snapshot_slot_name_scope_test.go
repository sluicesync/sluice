// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// slotlessScopedEngine has the MySQL family's snapshot surface: table-scoped
// and filtered opens, no slot-aware one. It counts which open the dispatch
// took.
type slotlessScopedEngine struct {
	stubEngine
	noTable, scoped, filtered int
	tables                    []string
	filters                   map[string]string
}

func (e *slotlessScopedEngine) Name() string { return "planetscale" }

func (e *slotlessScopedEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	e.noTable++
	return nil, nil //nolint:nilnil // stub
}

func (e *slotlessScopedEngine) OpenSnapshotStreamForTables(_ context.Context, _ string, tables []string) (*ir.SnapshotStream, error) {
	e.scoped++
	e.tables = tables
	return nil, nil //nolint:nilnil // stub
}

func (e *slotlessScopedEngine) OpenSnapshotStreamForTablesFiltered(_ context.Context, _ string, tables []string, f map[string]string) (*ir.SnapshotStream, error) {
	e.filtered++
	e.tables, e.filters = tables, f
	return nil, nil //nolint:nilnil // stub
}

var (
	_ ir.TableScopedSnapshotOpener = (*slotlessScopedEngine)(nil)
	_ ir.FilteredSnapshotOpener    = (*slotlessScopedEngine)(nil)
)

// TestOpenSnapshotStreamScoped_SlotNameOnASlotlessSourceKeepsTheScope pins
// VSTREAM-SLOT-NAME-UNSCOPED-COPY: --slot-name against a source with no
// slot-aware snapshot open (PlanetScale/Vitess, MySQL) must take the same
// open as without it. Before the fix it took the no-table open, which on
// VStream is one keyspace-wide stream: the copy ignored --include-table,
// skipped the --where push-down (rows outside the filter copied), and
// dropped the deletes a multi-table catch-up replays. Matrix: slot name
// unset / set × no filter / a filter, two tables; never the no-table open.
func TestOpenSnapshotStreamScoped_SlotNameOnASlotlessSourceKeepsTheScope(t *testing.T) {
	tables := []string{"a", "b"}
	filters := map[string]string{"a": "id > 10"}
	for _, slot := range []string{"", "sluice_x"} {
		for _, f := range []map[string]string{nil, filters} {
			t.Run(fmt.Sprintf("slot=%q/filtered=%v", slot, f != nil), func(t *testing.T) {
				e := &slotlessScopedEngine{}
				_, _ = openSnapshotStreamScoped(context.Background(), e, "dsn", slot, tables, f)
				if e.noTable != 0 {
					t.Fatalf("the no-table open was taken (slot=%q): on VStream that copies the whole keyspace in one stream, unfiltered", slot)
				}
				wantFiltered, wantScoped := 0, 1
				if f != nil {
					wantFiltered, wantScoped = 1, 0
				}
				if e.filtered != wantFiltered || e.scoped != wantScoped {
					t.Fatalf("filtered=%d scoped=%d; want %d/%d", e.filtered, e.scoped, wantFiltered, wantScoped)
				}
				if !slices.Equal(e.tables, tables) {
					t.Errorf("tables forwarded = %v; want %v", e.tables, tables)
				}
				if f != nil && e.filters["a"] != "id > 10" {
					t.Errorf("predicate not forwarded: %v", e.filters)
				}
			})
		}
	}
}
