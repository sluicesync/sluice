// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// TestScopeDriftDoor_NarrowingMustBeRecorded is R6 S1: a narrowing whose
// record cannot be written refuses the start
// (INCLUDE-TABLE-NARROWING-UNRECORDED) — the table would otherwise stay
// established while its changes were discarded, and a later re-widen would
// pass it. A failure that only concerns pruning dropped entries WARNs.
func TestScopeDriftDoor_NarrowingMustBeRecorded(t *testing.T) {
	ctx := context.Background()
	src := newScopeDriftEngine("kd", "nt")
	src.published = []string{"kd", "nt"}
	missingColumn := errors.New(`control table lacks column dropped_tables; to add them by hand: ALTER TABLE …`)

	app := recorded([]string{"kd", "nt"})
	app.narrowErr = missingColumn
	err := driftStreamer(t, src, nonScoperEngine{}, "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st")
	if err == nil || !strings.Contains(err.Error(), "INCLUDE-TABLE-NARROWING-UNRECORDED") || !strings.Contains(err.Error(), "nt leave") ||
		!errors.Is(err, missingColumn) {
		t.Fatalf("a narrowing that could not be recorded: %v; want the INCLUDE-TABLE-NARROWING-UNRECORDED refusal naming nt", err)
	}

	// Only pruning: nothing leaves, the dropped entry's table is gone.
	src2 := newScopeDriftEngine("kd")
	src2.published = []string{"kd"}
	app = recorded([]string{"kd"}, "gone")
	app.narrowErr = missingColumn
	requireNoDrift(t, driftStreamer(t, src2, nonScoperEngine{}, "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
}

// TestScopeDriftDoor_CorruptRecords is R6 L1 at the door: an undecodable
// established record refuses terminally naming its recovery; a malformed
// pending entry convicts its table naming --abandon, and one with no readable
// table name refuses naming the SQL; a record rewritten by a cold start over
// an undecodable dropped set (all-uncopied) refuses any table it does not
// hold, and passes the ones it does.
func TestScopeDriftDoor_CorruptRecords(t *testing.T) {
	ctx := context.Background()
	src := func(published ...string) *scopeDriftEngine {
		e := newScopeDriftEngine("kd", "nt")
		e.published = published
		return e
	}

	app := &scopeDriftApplier{recErr: &ir.UndecodableTableSetError{
		Column: "dropped_tables", RawBytes: 9,
		Recovery: "re-copy the stream with `sluice sync start --restart-from-scratch`", Err: errors.New("bad line"),
	}}
	err := driftStreamer(t, src("kd"), nonScoperEngine{}, "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st")
	requireDrift(t, err, "dropped_tables", "--restart-from-scratch")
	if refusalARestartRepeats(err) == nil {
		t.Fatal("an undecodable record must be terminal: a restart reads the same bytes")
	}

	app = recorded([]string{"kd", "nt"})
	app.pending = []ir.PendingAddedTable{{Table: "nt", Malformed: "nt\x00garbage"}}
	requireDrift(t, driftStreamer(t, src("kd", "nt"), nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, app, "st"),
		"nt (", "does not decode", "--abandon nt")

	app = recorded([]string{"kd", "nt"})
	app.pending = []ir.PendingAddedTable{{Malformed: "not a quoted line"}}
	requireDrift(t, driftStreamer(t, src("kd", "nt"), nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, app, "st"),
		"does not decode", "UPDATE sluice_cdc_state SET pending_added_tables = NULL")

	app = &scopeDriftApplier{rec: ir.EstablishedRecord{Recorded: true, Established: []string{"kd"}, DroppedAllUncopied: true}}
	requireDrift(t, driftStreamer(t, src("kd"), nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, app, "st"),
		"nt (", "could not be read at its last cold start")
	requireNoDrift(t, driftStreamer(t, src("kd"), nonScoperEngine{}, "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
}

// TestColdStartRecordOverwritesAnUndecodableRecord is R6 L1 at the cold
// start: an unreadable record is overwritten, and the dropped set becomes
// the all-uncopied marker (never a false "not dropped").
func TestColdStartRecordOverwritesAnUndecodableRecord(t *testing.T) {
	app := &scopeDriftApplier{recErr: &ir.UndecodableTableSetError{Column: "dropped_tables", Err: errors.New("bad")}}
	if err := recordEstablishedAfterCopy(context.Background(), app, "st", []string{"kd"}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(app.writes, " "); got != "dropped="+ir.DroppedAllUncopiedEntry+" established=kd" {
		t.Fatalf("writes %q", got)
	}
	// A marker already there survives the next cold start.
	app = &scopeDriftApplier{rec: ir.EstablishedRecord{Recorded: true, Established: []string{"kd"}, Dropped: []string{"x"}, DroppedAllUncopied: true}}
	if err := recordEstablishedAfterCopy(context.Background(), app, "st", []string{"kd", "x"}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(app.writes, " "); got != "dropped="+ir.DroppedAllUncopiedEntry+" established=kd,x" {
		t.Fatalf("writes %q", got)
	}
}

// addTableAbandonSource is a publication source whose membership check says
// the table is published and unshared, so --abandon would drop it.
type addTableAbandonSource struct {
	addTableUndoSource
}

func (e *addTableAbandonSource) UnpublishedTables(context.Context, string, []string) (ir.PublicationMembership, error) {
	return ir.PublicationMembership{Publication: "sluice_pub"}, nil
}

// TestAbandonRefusesOnAnUnreadableRecord is R6 S2: an undecodable record is
// not "the stream did not establish it"; --abandon refuses and leaves the
// publication alone.
func TestAbandonRefusesOnAnUnreadableRecord(t *testing.T) {
	src := &addTableAbandonSource{}
	a := &AddTable{Source: src, TableName: "nt", StreamID: "st"}
	app := &scopeDriftApplier{recErr: &ir.UndecodableTableSetError{Column: "established_tables", Err: errors.New("bad")}}
	err := a.abandonPublicationMembership(context.Background(), app)
	if err == nil || !strings.Contains(err.Error(), "nothing was changed") {
		t.Fatalf("got %v; want a refusal", err)
	}
	if len(src.dropped) != 0 {
		t.Fatalf("dropped %v from the publication on an unreadable record", src.dropped)
	}
	// The control: a readable record that does not hold nt does drop it.
	if err := a.abandonPublicationMembership(context.Background(), recorded([]string{"kd"})); err != nil || len(src.dropped) != 1 {
		t.Fatalf("readable record: %v, dropped %v; want nt dropped", err, src.dropped)
	}
}

// TestScopeAdmits pins the one scope rule (R6 A): sluice's own tables never;
// a filtered-in table; a live-added table outside an include list; but never
// a live-added table an explicit --exclude-table names.
func TestScopeAdmits(t *testing.T) {
	inc, _ := migcore.NewTableFilter([]string{"orders"}, nil)
	exc, _ := migcore.NewTableFilter(nil, []string{"users"})
	live := func(t string) bool { return t == "users" || t == "sluice_cdc_state" }
	for _, c := range []struct {
		f     migcore.TableFilter
		table string
		want  bool
	}{
		{inc, "orders", true},
		{inc, "users", true},  // added by schema add-table
		{inc, "other", false}, // neither
		{exc, "users", false}, // an explicit exclusion wins
		{exc, "other", true},
		{inc, "sluice_cdc_state", false},
	} {
		if got := scopeAdmits(c.table, c.f, live); got != c.want {
			t.Errorf("scopeAdmits(%q, include=%v exclude=%v) = %v; want %v", c.table, c.f.Include, c.f.Exclude, got, c.want)
		}
	}
}
