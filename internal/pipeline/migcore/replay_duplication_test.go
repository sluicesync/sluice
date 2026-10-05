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

// probeOnlyWriter implements the key probe but not the emptiness probe —
// the shape that would isolate the OnlyNonEmpty refusal from the other.
type probeOnlyWriter struct{}

func (probeOnlyWriter) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error { return nil }

func (probeOnlyWriter) ProbeReplayKey(context.Context, *ir.Table) (exists, keyed bool, err error) {
	return true, true, nil
}

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

	t.Run("a writer without the probe is refused, not judged on the recorded schema alone", func(t *testing.T) {
		got, err := FindReplayKeylessTables(ctx, plainWriter{}, tables, ReplayJudgeOptions{ProbeTarget: true})
		if err == nil || !strings.Contains(err.Error(), "ir.ReplayKeyProber") {
			t.Fatalf("got %v, %v; want the fail-closed refusal naming ir.ReplayKeyProber", got, err)
		}
	})

	t.Run("a nil writer with the probe asked for is refused", func(t *testing.T) {
		if _, err := FindReplayKeylessTables(ctx, nil, tables, ReplayJudgeOptions{ProbeTarget: true}); err == nil {
			t.Fatal("a nil writer satisfied ProbeTarget; the target judgment was silently skipped")
		}
	})

	t.Run("no options needs no writer", func(t *testing.T) {
		got, err := FindReplayKeylessTables(ctx, nil, tables, ReplayJudgeOptions{})
		if err != nil || len(got) != 3 {
			t.Fatalf("got %v, %v; want the three recorded-keyless tables", got, err)
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

	// The emptiness check and the key probe answer about the same name and
	// can contradict each other: Postgres's probe counts relkind r/p only, so
	// a foreign table or an INSTEAD-OF view holding rows reads as "absent",
	// which the judge used to clear as "will be created with its key". Rows
	// under the name prove it is not absent; the door refuses. An absent
	// table with NO rows (pk_target_absent, empty here) still passes.
	t.Run("OnlyNonEmpty refuses rows under a name the key probe calls absent", func(t *testing.T) {
		w.rows = map[string]bool{"pk_target_absent": true}
		defer func() { w.rows = nil }()
		got, err := FindReplayKeylessTables(ctx, w, tables, ReplayJudgeOptions{ProbeTarget: true, OnlyNonEmpty: true})
		if err != nil {
			t.Fatal(err)
		}
		assertReplayKeyless(t, got, map[string]ReplayKeylessReason{
			"pk_target_absent": ReplayKeylessTargetUnjudged,
		})
		got, err = FindReplayKeylessTables(ctx, w, tables, ReplayJudgeOptions{ProbeTarget: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 4 {
			t.Errorf("without OnlyNonEmpty an absent table must still pass (it will be created): got %v", got)
		}
	})

	// The F-E1 review's HIGH 2: this used to return nothing — every table
	// treated as EMPTY — which left the restore door open on any engine
	// whose writer could not report emptiness (SQLite: 200 rows for 100).
	t.Run("OnlyNonEmpty on a writer that cannot report emptiness is refused", func(t *testing.T) {
		got, err := FindReplayKeylessTables(ctx, probeOnlyWriter{}, tables, ReplayJudgeOptions{ProbeTarget: true, OnlyNonEmpty: true})
		if err == nil || !strings.Contains(err.Error(), "ir.TableEmptyChecker") {
			t.Fatalf("got %v, %v; want the fail-closed refusal naming ir.TableEmptyChecker", got, err)
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

// TestReplayKeyFingerprint_MovesWithEveryJudgedPart pins the broker's
// clearance cache key (audit F-E1 review): every part of a recorded table
// the judge reads must move the fingerprint, or a later AlterTable delta
// that drops or replaces a cleared table's key is never re-judged. Each
// mutation below is one the recorded predicate or the target probe reads.
func TestReplayKeyFingerprint_MovesWithEveryJudgedPart(t *testing.T) {
	base := func() *ir.Table {
		return &ir.Table{
			Name: "t",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "u", Type: ir.Integer{Width: 64}},
			},
			PrimaryKey: &ir.Index{Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes:    []*ir.Index{{Name: "uq_u", Unique: true, Columns: []ir.IndexColumn{{Column: "u"}}}},
		}
	}
	ref := ReplayKeyFingerprint(base())
	if ref != ReplayKeyFingerprint(base()) {
		t.Fatal("the fingerprint is not deterministic")
	}
	mutations := map[string]func(*ir.Table){
		"primary key dropped":       func(t *ir.Table) { t.PrimaryKey = nil },
		"primary key columns moved": func(t *ir.Table) { t.PrimaryKey.Columns = []ir.IndexColumn{{Column: "u"}} },
		"column made nullable":      func(t *ir.Table) { t.Columns[1].Nullable = true },
		"column made generated":     func(t *ir.Table) { t.Columns[0].GeneratedExpr = "u + 1" },
		"column renamed":            func(t *ir.Table) { t.Columns[1].Name = "v" },
		"column added":              func(t *ir.Table) { t.Columns = append(t.Columns, &ir.Column{Name: "w"}) },
		"unique index dropped":      func(t *ir.Table) { t.Indexes = nil },
		"unique made non-unique":    func(t *ir.Table) { t.Indexes[0].Unique = false },
		"unique made partial":       func(t *ir.Table) { t.Indexes[0].Predicate = "u > 0" },
		"unique made expression":    func(t *ir.Table) { t.Indexes[0].Columns[0].Expression = "(u * 2)" },
	}
	for name, mutate := range mutations {
		tbl := base()
		mutate(tbl)
		if ReplayKeyFingerprint(tbl) == ref {
			t.Errorf("%s: the fingerprint did not move, so a cleared table would not be re-judged", name)
		}
	}
}
