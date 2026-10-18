// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// scopeDriftApplier serves the records the door reads: the add-table pending
// set, the established set (nil = not recorded), and the live-added tables.
type scopeDriftApplier struct {
	ir.ChangeApplier
	pending     []string
	pendErr     error
	established []string
	recorded    bool
	rewritten   [][]string
	liveAdded   []string
}

func (a *scopeDriftApplier) RecordPendingAddedTable(context.Context, string, string) error {
	return errors.New("not used")
}

func (a *scopeDriftApplier) ClearPendingAddedTables(context.Context, string, []string) error {
	return errors.New("not used")
}

func (a *scopeDriftApplier) ReadPendingAddedTables(context.Context, string) ([]string, error) {
	return a.pending, a.pendErr
}

func (a *scopeDriftApplier) ReadEstablishedTables(context.Context, string) (tables []string, recorded bool, err error) {
	return a.established, a.recorded, nil
}

func (a *scopeDriftApplier) RecordEstablishedTables(_ context.Context, _ string, tables []string) error {
	a.rewritten = append(a.rewritten, append([]string(nil), tables...))
	return nil
}

func (a *scopeDriftApplier) AddEstablishedTable(context.Context, string, string) error {
	return errors.New("not used")
}

func (a *scopeDriftApplier) ReadLiveAddedTables(context.Context, string) ([]string, error) {
	return a.liveAdded, nil
}

// established returns an applier whose stream recorded tables as established.
func established(tables ...string) *scopeDriftApplier {
	return &scopeDriftApplier{established: tables, recorded: true}
}

// scopeDriftEngine serves a schema and, as a publication source, which
// tables its publication delivers.
type scopeDriftEngine struct {
	*ukDoorEngine
	publication string   // "" = no publication exists
	published   []string // the tables it delivers
	shared      bool     // another sluice slot exists
	pubErr      error
	asked       [][]string
}

func (e *scopeDriftEngine) UnpublishedTables(_ context.Context, _ string, tables []string) (missing []string, publication string, shared bool, err error) {
	e.asked = append(e.asked, append([]string(nil), tables...))
	if e.pubErr != nil {
		return nil, "", false, e.pubErr
	}
	if e.publication == "" {
		return nil, "", false, nil
	}
	have := map[string]bool{}
	for _, t := range e.published {
		have[t] = true
	}
	for _, t := range tables {
		if !have[t] {
			missing = append(missing, t)
		}
	}
	return missing, e.publication, e.shared, nil
}

func newScopeDriftEngine(tables ...string) *scopeDriftEngine {
	schema := &ir.Schema{}
	for _, name := range tables {
		schema.Tables = append(schema.Tables, ukDoorTable(name, ukDoorKey("pk", "id")))
	}
	return &scopeDriftEngine{
		ukDoorEngine: &ukDoorEngine{
			resumeDispatchEngine: &resumeDispatchEngine{name: "postgres", caps: ir.Capabilities{CDC: ir.CDCLogicalReplication}},
			schemas:              map[string]*ir.Schema{"src-dsn": schema},
		},
		publication: "sluice_pub",
	}
}

func scopeDriftStreamer(t *testing.T, src *scopeDriftEngine, include ...string) *Streamer {
	t.Helper()
	f, err := migcore.NewTableFilter(include, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Streamer{Source: src, Target: nonScoperEngine{}, SourceDSN: "src-dsn", Filter: f}
}

func requireDrift(t *testing.T, err error, want ...string) {
	t.Helper()
	if !errors.Is(err, ir.ErrIncludeTableScopeDrift) {
		t.Fatalf("want INCLUDE-TABLE-SCOPE-DRIFT, got %v", err)
	}
	for _, w := range want {
		if !strings.Contains(err.Error(), w) {
			t.Fatalf("the refusal does not contain %q: %v", w, err)
		}
	}
}

// TestScopeDriftDoor_EstablishedArm is the primary evidence on every source:
// only a table an --include-table entry names literally refuses; a glob or an
// unfiltered stream keeps ADR-0021's "new tables stay out" with a WARN.
func TestScopeDriftDoor_EstablishedArm(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		include []string
		refuses bool
	}{
		{name: "a literal include of an unestablished table refuses", include: []string{"kd", "nt"}, refuses: true},
		{name: "a glob admitting it does not", include: []string{"kd", "n*"}},
		{name: "an unfiltered stream does not (the L1 regression)"},
		{name: "a literal include of established tables only passes", include: []string{"kd"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newScopeDriftEngine("kd", "nt")
			src.published = []string{"kd", "nt"} // a shared publication delivering it must not matter
			err := scopeDriftStreamer(t, src, tc.include...).phaseRefuseIncludeTableScopeDrift(ctx, established("kd"), "st")
			if tc.refuses {
				requireDrift(t, err, "nt (named by --include-table and never copied by this stream")
				return
			}
			if err != nil {
				t.Fatalf("want no refusal, got %v", err)
			}
			if len(src.asked) != 0 {
				t.Fatalf("a recorded stream asked the publication: %v", src.asked)
			}
		})
	}

	t.Run("a narrowed scope passes and narrows the record", func(t *testing.T) {
		app := established("kd", "nt", "gone")
		if err := scopeDriftStreamer(t, newScopeDriftEngine("kd", "nt"), "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"); err != nil {
			t.Fatalf("want no refusal, got %v", err)
		}
		if len(app.rewritten) != 1 || strings.Join(app.rewritten[0], ",") != "kd" {
			t.Fatalf("record rewritten to %v, want [kd] (out of scope and gone from the source both leave)", app.rewritten)
		}
	})

	t.Run("an unchanged scope rewrites nothing", func(t *testing.T) {
		app := established("kd", "nt")
		if err := scopeDriftStreamer(t, newScopeDriftEngine("kd", "nt"), "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"); err != nil {
			t.Fatalf("want no refusal, got %v", err)
		}
		if len(app.rewritten) != 0 {
			t.Fatalf("rewrote an unchanged record: %v", app.rewritten)
		}
	})

	t.Run("once per process", func(t *testing.T) {
		s := scopeDriftStreamer(t, newScopeDriftEngine("kd", "nt"), "kd", "nt")
		s.scopeDriftDoorDone = true
		if err := s.phaseRefuseIncludeTableScopeDrift(ctx, established("kd"), "st"); err != nil {
			t.Fatalf("a retry in the same Run re-ran the door: %v", err)
		}
	})
}

// TestScopeDriftDoor_PendingArm: a pending add-table refuses whatever admits
// its table; a pending name the source no longer has is ignored (L2); a
// failed read is a restartable failure, never the marker.
func TestScopeDriftDoor_PendingArm(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		include   []string
		tables    []string
		pending   []string
		liveAdded []string
		refuses   bool
	}{
		{name: "pending and literally included", include: []string{"kd", "nt"}, tables: []string{"kd", "nt"}, pending: []string{"nt"}, refuses: true},
		{name: "pending under an unfiltered stream", tables: []string{"kd", "nt"}, pending: []string{"nt"}, refuses: true},
		{name: "pending and live-added", include: []string{"kd"}, tables: []string{"kd", "nt"}, pending: []string{"nt"}, liveAdded: []string{"nt"}, refuses: true},
		{name: "pending and out of scope", include: []string{"kd"}, tables: []string{"kd", "nt"}, pending: []string{"nt"}},
		{name: "pending and gone from the source", tables: []string{"kd"}, pending: []string{"nt"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := established(tc.tables...)
			app.pending, app.liveAdded = tc.pending, tc.liveAdded
			err := scopeDriftStreamer(t, newScopeDriftEngine(tc.tables...), tc.include...).phaseRefuseIncludeTableScopeDrift(ctx, app, "st")
			if tc.refuses {
				requireDrift(t, err, "nt (its `sluice schema add-table` started and did not complete", "wait for it to finish")
				return
			}
			if err != nil {
				t.Fatalf("want no refusal, got %v", err)
			}
		})
	}

	t.Run("a failed read is restartable, not the marker", func(t *testing.T) {
		app := established("kd")
		app.pendErr = errors.New("connection reset by peer")
		err := scopeDriftStreamer(t, newScopeDriftEngine("kd")).phaseRefuseIncludeTableScopeDrift(ctx, app, "st")
		var cp *connectPhaseError
		if err == nil || errors.Is(err, ir.ErrIncludeTableScopeDrift) || !errors.As(err, &cp) {
			t.Fatalf("want a restartable non-marker failure, got %T %v", err, err)
		}
	})
}

// TestScopeDriftDoor_LegacyPublicationFallback: a stream with no established
// record falls back to its publication, for literal includes, and only when
// no other sluice slot exists (S1: a shared publication is every stream's
// union and proves nothing about this one).
func TestScopeDriftDoor_LegacyPublicationFallback(t *testing.T) {
	ctx := context.Background()
	legacy := func() *scopeDriftApplier { return &scopeDriftApplier{} }

	t.Run("unshared, literal, unpublished: refuses", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt", "other")
		src.published = []string{"kd"}
		err := scopeDriftStreamer(t, src, "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, legacy(), "st")
		requireDrift(t, err, `nt (named by --include-table and not in publication "sluice_pub"`)
		if strings.Contains(err.Error(), "other") {
			t.Fatalf("an out-of-scope table was judged: %v", err)
		}
		got := append([]string(nil), src.asked[0]...)
		sort.Strings(got)
		if strings.Join(got, ",") != "kd,nt" {
			t.Fatalf("asked the publication about %v, want the in-scope [kd nt]", src.asked)
		}
	})
	t.Run("unshared, unfiltered: no refusal", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.published = []string{"kd"}
		if err := scopeDriftStreamer(t, src).phaseRefuseIncludeTableScopeDrift(ctx, legacy(), "st"); err != nil {
			t.Fatalf("want no refusal, got %v", err)
		}
	})
	t.Run("shared: no evidence, no refusal", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.published, src.shared = []string{"kd"}, true
		if err := scopeDriftStreamer(t, src, "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, legacy(), "st"); err != nil {
			t.Fatalf("want no refusal, got %v", err)
		}
	})
	t.Run("no publication: no evidence, no refusal", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.publication = ""
		if err := scopeDriftStreamer(t, src, "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, legacy(), "st"); err != nil {
			t.Fatalf("want no refusal, got %v", err)
		}
	})
	t.Run("a failed membership read is not the marker", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.pubErr = errors.New("connection refused")
		err := scopeDriftStreamer(t, src, "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, legacy(), "st")
		if err == nil || errors.Is(err, ir.ErrIncludeTableScopeDrift) {
			t.Fatalf("want a non-marker failure, got %v", err)
		}
	})
	t.Run("no record is never seeded", func(t *testing.T) {
		app := legacy()
		src := newScopeDriftEngine("kd")
		src.published = []string{"kd"}
		if err := scopeDriftStreamer(t, src, "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"); err != nil {
			t.Fatal(err)
		}
		if len(app.rewritten) != 0 {
			t.Fatalf("a legacy resume wrote an established record %v; seeding would bless never-copied tables", app.rewritten)
		}
	})
}

// TestEstablishedKeys pins the record's entry shape on both stream kinds.
func TestEstablishedKeys(t *testing.T) {
	got := establishedKeysOf([]*ir.Table{{Name: "t"}, {Schema: "app", Name: "t"}})
	if got[0] != "t" || got[1] != "app\x00t" || bareTableOfKey(got[1]) != "t" || bareTableOfKey(got[0]) != "t" {
		t.Fatalf("keys %q", got)
	}
}

// addTableUndoSource records the publication drop add-table's failure path
// asks for.
type addTableUndoSource struct {
	nonScoperEngine
	dropped [][]string
	dropErr error
	ctxErr  error
}

func (e *addTableUndoSource) DropPublicationTables(ctx context.Context, _ string, tables []string) error {
	e.ctxErr = ctx.Err()
	e.dropped = append(e.dropped, tables)
	return e.dropErr
}

// TestAddTableUndoFailedAdd pins path 2's failure path: the cause survives,
// the pending record is named, and the publication add is taken back only
// when this run made it — on a cancelled context too, the commonest way here.
func TestAddTableUndoFailedAdd(t *testing.T) {
	cause := errors.New("copy failed")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("this run's add is taken back, even after a Ctrl-C", func(t *testing.T) {
		src := &addTableUndoSource{}
		a := &AddTable{Source: src, TableName: "nt", publicationAddedByThisRun: true}
		err := a.undoFailedAdd(cancelled, &scopeDriftApplier{}, cause)
		if !errors.Is(err, cause) || !strings.Contains(err.Error(), "stays recorded as pending") ||
			!strings.Contains(err.Error(), "taken back out of the source publication") {
			t.Fatalf("got %v", err)
		}
		if len(src.dropped) != 1 || src.dropped[0][0] != "nt" || src.ctxErr != nil {
			t.Fatalf("drop calls %v (ctx err %v), want one for nt on a live context", src.dropped, src.ctxErr)
		}
	})

	t.Run("a table already in the publication is left there", func(t *testing.T) {
		src := &addTableUndoSource{}
		a := &AddTable{Source: src, TableName: "nt"}
		if err := a.undoFailedAdd(context.Background(), &scopeDriftApplier{}, cause); !errors.Is(err, cause) {
			t.Fatalf("got %v", err)
		}
		if len(src.dropped) != 0 {
			t.Fatalf("dropped %v from the publication; this run did not add it", src.dropped)
		}
	})

	t.Run("a failed take-back is named", func(t *testing.T) {
		src := &addTableUndoSource{dropErr: errors.New("permission denied")}
		a := &AddTable{Source: src, TableName: "nt", publicationAddedByThisRun: true}
		if err := a.undoFailedAdd(context.Background(), &scopeDriftApplier{}, cause); !strings.Contains(err.Error(), "FAILED (permission denied)") {
			t.Fatalf("got %v", err)
		}
	})
}
