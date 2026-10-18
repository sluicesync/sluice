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

// scopeDriftApplier serves the add-table pending record and the live-added
// tables the door reads.
type scopeDriftApplier struct {
	ir.ChangeApplier
	pending   []string
	pendErr   error
	liveAdded []string
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

func (a *scopeDriftApplier) ReadLiveAddedTables(context.Context, string) ([]string, error) {
	return a.liveAdded, nil
}

// scopeDriftEngine is a publication source: it serves a schema and reports
// which tables its publication delivers.
type scopeDriftEngine struct {
	*ukDoorEngine
	publication string   // "" = no publication exists
	published   []string // the tables it delivers
	pubErr      error
	asked       [][]string
}

func (e *scopeDriftEngine) UnpublishedTables(_ context.Context, _ string, tables []string) (missing []string, publication string, err error) {
	e.asked = append(e.asked, append([]string(nil), tables...))
	if e.pubErr != nil {
		return nil, "", e.pubErr
	}
	if e.publication == "" {
		return nil, "", nil
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
	return missing, e.publication, nil
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

func scopeDriftFilter(t *testing.T, include ...string) migcore.TableFilter {
	t.Helper()
	f, err := migcore.NewTableFilter(include, nil)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestScopeDriftDoor_PendingArm is the add-table pending record's half, which
// every source reaches: a pending table refuses only while it is in scope —
// by the filter or as a live-added table — and a failed read is a restartable
// failure, never the marker.
func TestScopeDriftDoor_PendingArm(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		include   []string
		pending   []string
		liveAdded []string
		refuses   bool
	}{
		{name: "pending and in the filter", include: []string{"kd", "nt"}, pending: []string{"nt"}, refuses: true},
		{name: "pending and out of scope", include: []string{"kd"}, pending: []string{"nt"}},
		{name: "pending and live-added", include: []string{"kd"}, pending: []string{"nt"}, liveAdded: []string{"nt"}, refuses: true},
		{name: "pending under an empty filter", pending: []string{"nt"}, refuses: true},
		{name: "nothing pending", include: []string{"kd", "nt"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Streamer{Source: nonScoperEngine{}, Target: nonScoperEngine{}, Filter: scopeDriftFilter(t, tc.include...)}
			err := s.phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{pending: tc.pending, liveAdded: tc.liveAdded}, "st")
			if !tc.refuses {
				if err != nil {
					t.Fatalf("want no refusal, got %v", err)
				}
				return
			}
			if !errors.Is(err, ir.ErrIncludeTableScopeDrift) {
				t.Fatalf("want INCLUDE-TABLE-SCOPE-DRIFT, got %v", err)
			}
			if !strings.Contains(err.Error(), "nt (its `sluice schema add-table` started and did not complete") {
				t.Fatalf("the refusal does not name nt with its reason: %v", err)
			}
		})
	}

	t.Run("a failed read is restartable, not the marker", func(t *testing.T) {
		s := &Streamer{Source: nonScoperEngine{}, Target: nonScoperEngine{}}
		err := s.phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{pendErr: errors.New("connection reset by peer")}, "st")
		if err == nil || errors.Is(err, ir.ErrIncludeTableScopeDrift) {
			t.Fatalf("want a non-marker failure, got %v", err)
		}
		var cp *connectPhaseError
		if !errors.As(err, &cp) {
			t.Fatalf("want the control-read failure marked restartable, got %T %v", err, err)
		}
	})
}

// TestScopeDriftDoor_PublicationArm is the pgoutput half: the in-scope
// source tables — the cold start's own prunes, filter and ORM skip — against
// the tables the publication delivers.
func TestScopeDriftDoor_PublicationArm(t *testing.T) {
	ctx := context.Background()

	t.Run("a widened table the publication does not deliver refuses", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt", "other")
		src.published = []string{"kd"}
		s := &Streamer{Source: src, Target: nonScoperEngine{}, SourceDSN: "src-dsn", Filter: scopeDriftFilter(t, "kd", "nt")}
		err := s.phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{}, "st")
		if !errors.Is(err, ir.ErrIncludeTableScopeDrift) || !strings.Contains(err.Error(), `nt (not in publication "sluice_pub"`) {
			t.Fatalf("want nt refused as unpublished, got %v", err)
		}
		if strings.Contains(err.Error(), "other") {
			t.Fatalf("an out-of-scope table was judged: %v", err)
		}
		if len(src.asked) != 1 || strings.Join(src.asked[0], ",") != "kd,nt" {
			t.Fatalf("asked the publication about %v, want exactly the in-scope [kd nt]", src.asked)
		}
	})

	t.Run("an unchanged scope passes", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.published = []string{"kd", "nt"}
		s := &Streamer{Source: src, Target: nonScoperEngine{}, SourceDSN: "src-dsn", Filter: scopeDriftFilter(t, "kd", "nt")}
		if err := s.phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{}, "st"); err != nil {
			t.Fatalf("want no refusal, got %v", err)
		}
	})

	t.Run("a narrowed scope passes", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.published = []string{"kd", "nt"}
		s := &Streamer{Source: src, Target: nonScoperEngine{}, SourceDSN: "src-dsn", Filter: scopeDriftFilter(t, "kd")}
		if err := s.phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{}, "st"); err != nil {
			t.Fatalf("want no refusal, got %v", err)
		}
	})

	t.Run("no publication is no evidence", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.publication = ""
		s := &Streamer{Source: src, Target: nonScoperEngine{}, SourceDSN: "src-dsn", Filter: scopeDriftFilter(t, "kd", "nt")}
		if err := s.phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{}, "st"); err != nil {
			t.Fatalf("want no refusal, got %v", err)
		}
	})

	// The ORM prune must agree with the cold start's, or every warm resume
	// of a stream with no filter would refuse the bookkeeping table the cold
	// start skipped (and never published).
	t.Run("the ORM table the stream skips is not judged", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "__diesel_schema_migrations")
		src.published = []string{"kd"}
		s := &Streamer{Source: src, Target: nonScoperEngine{}, SourceDSN: "src-dsn", SkipORMTables: true}
		if err := s.phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{}, "st"); err != nil {
			t.Fatalf("want no refusal, got %v", err)
		}
		s.SkipORMTables = false
		if err := s.phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{}, "st"); !errors.Is(err, ir.ErrIncludeTableScopeDrift) {
			t.Fatalf("with the ORM skip off the table is in scope and unpublished; want the refusal, got %v", err)
		}
	})

	t.Run("both reasons on one table are one entry", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.published = []string{"kd"}
		s := &Streamer{Source: src, Target: nonScoperEngine{}, SourceDSN: "src-dsn", Filter: scopeDriftFilter(t, "kd", "nt")}
		err := s.phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{pending: []string{"nt"}}, "st")
		if !errors.Is(err, ir.ErrIncludeTableScopeDrift) || !strings.Contains(err.Error(), "1 table(s)") ||
			!strings.Contains(err.Error(), "so the source never sends its changes; its `sluice schema add-table` started") {
			t.Fatalf("want one entry carrying both reasons, got %v", err)
		}
	})

	t.Run("a multi-database stream does not ask the publication", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.published = []string{"kd"}
		s := &Streamer{Source: src, Target: nonScoperEngine{}, SourceDSN: "src-dsn", AllDatabases: true}
		if err := s.phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{}, "st"); err != nil {
			t.Fatalf("want no refusal, got %v", err)
		}
		if len(src.asked) != 0 {
			t.Fatalf("asked the publication on a multi-database stream: %v", src.asked)
		}
	})

	t.Run("a failed membership read is not the marker", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.pubErr = errors.New("connection refused")
		s := &Streamer{Source: src, Target: nonScoperEngine{}, SourceDSN: "src-dsn", Filter: scopeDriftFilter(t, "kd", "nt")}
		err := s.phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{}, "st")
		if err == nil || errors.Is(err, ir.ErrIncludeTableScopeDrift) {
			t.Fatalf("want a non-marker failure, got %v", err)
		}
	})
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
