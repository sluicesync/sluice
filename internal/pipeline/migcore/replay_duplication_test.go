// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// replayProbeWriter is a target row writer answering the two probes the
// replay-duplication judge asks, from fixed maps.
type replayProbeWriter struct {
	rows     map[string]bool // table holds rows
	present  map[string]bool // table exists on the target
	keyed    map[string]bool // target table has an upsert key
	probeErr error
	probed   []string
}

func (w *replayProbeWriter) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error { return nil }

func (w *replayProbeWriter) IsTableEmpty(_ context.Context, t *ir.Table) (bool, error) {
	return !w.rows[t.Name], nil
}

func (w *replayProbeWriter) ProbeReplayKey(_ context.Context, t *ir.Table) (exists, keyed bool, err error) {
	w.probed = append(w.probed, t.Name)
	if w.probeErr != nil {
		return false, false, w.probeErr
	}
	return w.present[t.Name], w.keyed[t.Name], nil
}

// plainWriter implements neither optional probe.
type plainWriter struct{}

func (plainWriter) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error { return nil }

func replayTable(name string, pk bool, uniqueNullable *bool) *ir.Table {
	t := &ir.Table{Name: name, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	if pk {
		t.PrimaryKey = &ir.Index{Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}
	}
	if uniqueNullable != nil {
		t.Columns[0].Nullable = *uniqueNullable
		t.Indexes = []*ir.Index{{Name: name + "_uq", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}}
	}
	return t
}

// TestFindReplayKeylessTables pins every branch of the F-E1 judge: the
// recorded judgment, the target judgment, the two cases where the target
// probe is not consulted, and the emptiness filter. The case matrix is the
// class — {recorded keyed, recorded keyless} × {target absent, keyed,
// keyless} — not one representative.
func TestFindReplayKeylessTables(t *testing.T) {
	ctx := context.Background()
	no, yes := false, true
	tables := []*ir.Table{
		replayTable("pk_target_keyed", true, nil),
		replayTable("pk_target_keyless", true, nil),
		replayTable("pk_target_absent", true, nil),
		replayTable("nn_unique_target_keyed", false, &no),
		replayTable("nullable_unique", false, &yes),
		replayTable("keyless_target_keyed", false, nil),
		replayTable("keyless_target_absent", false, nil),
	}
	w := &replayProbeWriter{
		present: map[string]bool{
			"pk_target_keyed": true, "pk_target_keyless": true,
			"nn_unique_target_keyed": true, "keyless_target_keyed": true,
		},
		keyed: map[string]bool{
			"pk_target_keyed": true, "nn_unique_target_keyed": true, "keyless_target_keyed": true,
		},
	}

	got, err := FindReplayKeylessTables(ctx, w, tables, ReplayJudgeOptions{ProbeTarget: true})
	if err != nil {
		t.Fatalf("FindReplayKeylessTables: %v", err)
	}
	want := map[string]ReplayKeylessReason{
		"pk_target_keyless":     ReplayKeylessTarget,   // the target lost the source's key
		"nullable_unique":       ReplayKeylessRecorded, // a nullable UNIQUE does not collide on NULL
		"keyless_target_keyed":  ReplayKeylessRecorded, // a target-only key is still refused: remedy is on the source
		"keyless_target_absent": ReplayKeylessRecorded,
	}
	assertReplayKeyless(t, got, want)

	t.Run("target probe off judges the recorded schema only", func(t *testing.T) {
		got, err := FindReplayKeylessTables(ctx, w, tables, ReplayJudgeOptions{ProbeTarget: false})
		if err != nil {
			t.Fatal(err)
		}
		assertReplayKeyless(t, got, map[string]ReplayKeylessReason{
			"nullable_unique":       ReplayKeylessRecorded,
			"keyless_target_keyed":  ReplayKeylessRecorded,
			"keyless_target_absent": ReplayKeylessRecorded,
		})
	})

	t.Run("a writer without the probe is judged on the recorded schema", func(t *testing.T) {
		got, err := FindReplayKeylessTables(ctx, plainWriter{}, tables, ReplayJudgeOptions{ProbeTarget: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("got %v, want the three recorded-keyless tables", got)
		}
	})

	t.Run("OnlyNonEmpty skips empty tables before either judgment", func(t *testing.T) {
		w.rows = map[string]bool{"pk_target_keyless": true, "keyless_target_absent": true}
		defer func() { w.rows = nil }()
		got, err := FindReplayKeylessTables(ctx, w, tables, ReplayJudgeOptions{ProbeTarget: true, OnlyNonEmpty: true})
		if err != nil {
			t.Fatal(err)
		}
		assertReplayKeyless(t, got, map[string]ReplayKeylessReason{
			"pk_target_keyless":     ReplayKeylessTarget,
			"keyless_target_absent": ReplayKeylessRecorded,
		})
	})

	t.Run("OnlyNonEmpty on a writer that cannot report emptiness refuses nothing", func(t *testing.T) {
		got, err := FindReplayKeylessTables(ctx, plainWriter{}, tables, ReplayJudgeOptions{ProbeTarget: true, OnlyNonEmpty: true})
		if err != nil || len(got) != 0 {
			t.Fatalf("got %v, %v; want nothing (the stated residual)", got, err)
		}
	})

	t.Run("a probe error surfaces rather than reading as keyed", func(t *testing.T) {
		ew := &replayProbeWriter{probeErr: errors.New("catalog unreachable")}
		_, err := FindReplayKeylessTables(ctx, ew, []*ir.Table{replayTable("t", true, nil)}, ReplayJudgeOptions{ProbeTarget: true})
		if err == nil || !strings.Contains(err.Error(), "catalog unreachable") {
			t.Fatalf("got %v, want the probe error", err)
		}
	})
}

func assertReplayKeyless(t *testing.T, got []ReplayKeylessTable, want map[string]ReplayKeylessReason) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("got %d keyless tables %v, want %d %v", len(got), got, len(want), want)
	}
	for _, g := range got {
		r, ok := want[g.Name]
		if !ok {
			t.Errorf("%q reported keyless (%s), want it cleared", g.Name, g.Reason)
			continue
		}
		if r != g.Reason {
			t.Errorf("%q reported for %q, want %q", g.Name, g.Reason, r)
		}
	}
}
