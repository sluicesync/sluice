// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// scopeDriftApplier is an in-memory control row: the established record, the
// pending add-tables and the live-added tables, with every write logged.
type scopeDriftApplier struct {
	ir.ChangeApplier
	pending   []ir.PendingAddedTable
	pendErr   error
	rec       ir.EstablishedRecord
	recordErr error
	writes    []string // "established=a,b" / "dropped=a" / "reset" / "clear=a"
	liveAdded []string
}

func (a *scopeDriftApplier) RecordPendingAddedTable(context.Context, string, string, time.Time) error {
	return errors.New("not used")
}

func (a *scopeDriftApplier) HeartbeatPendingAddedTable(context.Context, string, string, time.Time) error {
	return errors.New("not used")
}

func (a *scopeDriftApplier) ClearPendingAddedTables(_ context.Context, _ string, tables []string) error {
	a.writes = append(a.writes, "clear="+strings.Join(tables, ","))
	return nil
}

func (a *scopeDriftApplier) ReadPendingAddedTables(context.Context, string) ([]ir.PendingAddedTable, error) {
	return a.pending, a.pendErr
}

func (a *scopeDriftApplier) ReadEstablishedRecord(context.Context, string) (ir.EstablishedRecord, error) {
	return a.rec, nil
}

func (a *scopeDriftApplier) ResetEstablishedRecord(context.Context, string) error {
	a.writes = append(a.writes, "reset")
	a.rec = ir.EstablishedRecord{}
	return nil
}

func (a *scopeDriftApplier) RecordEstablishedTables(_ context.Context, _ string, tables []string) error {
	if a.recordErr != nil {
		return a.recordErr
	}
	a.writes = append(a.writes, "established="+strings.Join(tables, ","))
	a.rec.Recorded, a.rec.Established = true, tables
	return nil
}

func (a *scopeDriftApplier) RecordDroppedTables(_ context.Context, _ string, tables []string) error {
	a.writes = append(a.writes, "dropped="+strings.Join(tables, ","))
	a.rec.Dropped = tables
	return nil
}

func (a *scopeDriftApplier) AddEstablishedTable(context.Context, string, string) error {
	return errors.New("not used")
}

func (a *scopeDriftApplier) ReadLiveAddedTables(context.Context, string) ([]string, error) {
	return a.liveAdded, nil
}

// recorded returns an applier whose stream established est and dropped drop.
func recorded(est []string, drop ...string) *scopeDriftApplier {
	return &scopeDriftApplier{rec: ir.EstablishedRecord{Recorded: true, Established: est, Dropped: drop}}
}

// scopeDriftEngine serves a schema and, as a publication source, its
// publication's membership.
type scopeDriftEngine struct {
	*ukDoorEngine
	publication string   // "" = no publication exists
	published   []string // the tables it delivers
	allTables   bool
	shared      bool
	pubErr      error
	asked       int
}

func (e *scopeDriftEngine) UnpublishedTables(_ context.Context, _ string, tables []string) (ir.PublicationMembership, error) {
	e.asked++
	if e.pubErr != nil {
		return ir.PublicationMembership{}, e.pubErr
	}
	if e.publication == "" {
		return ir.PublicationMembership{}, nil
	}
	have := toSet(e.published)
	m := ir.PublicationMembership{Publication: e.publication, AllTables: e.allTables, Shared: e.shared}
	for _, t := range tables {
		if !have[t] {
			m.Missing = append(m.Missing, t)
		}
	}
	return m, nil
}

func driftSchema(tables ...string) *ir.Schema {
	schema := &ir.Schema{}
	for _, name := range tables {
		schema.Tables = append(schema.Tables, ukDoorTable(name, ukDoorKey("pk", "id")))
	}
	return schema
}

// newScopeDriftEngine is a pgoutput source holding tables.
func newScopeDriftEngine(tables ...string) *scopeDriftEngine {
	return &scopeDriftEngine{
		ukDoorEngine: &ukDoorEngine{
			resumeDispatchEngine: &resumeDispatchEngine{name: "postgres", caps: ir.Capabilities{CDC: ir.CDCLogicalReplication}},
			schemas:              map[string]*ir.Schema{"src-dsn": driftSchema(tables...)},
		},
		publication: "sluice_pub",
	}
}

// binlogEngine is a source with no publication (binlog/VStream/trigger
// shape), holding tables; the target engine's schema is targetTables.
func binlogPair(source, targetTables []string) (src, tgt *ukDoorEngine) {
	src = &ukDoorEngine{
		resumeDispatchEngine: &resumeDispatchEngine{name: "mysql", caps: ir.Capabilities{CDC: ir.CDCBinlog}},
		schemas:              map[string]*ir.Schema{"src-dsn": driftSchema(source...)},
	}
	tgt = &ukDoorEngine{
		resumeDispatchEngine: &resumeDispatchEngine{name: "mysql"},
		schemas:              map[string]*ir.Schema{"tgt-dsn": driftSchema(targetTables...)},
	}
	return src, tgt
}

func driftStreamer(t *testing.T, src, tgt ir.Engine, include ...string) *Streamer {
	t.Helper()
	f, err := migcore.NewTableFilter(include, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Streamer{Source: src, Target: tgt, SourceDSN: "src-dsn", TargetDSN: "tgt-dsn", Filter: f}
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

func requireNoDrift(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("want no refusal, got %v", err)
	}
}

const (
	reasonDropped        = "left this stream's scope earlier"
	reasonLiteral        = "named by --include-table and never copied by this stream"
	reasonInPublication  = `publication "sluice_pub" delivers it`
	reasonTargetPresent  = "its target table exists"
	reasonUnpublished    = `no longer in publication "sluice_pub"`
	reasonIncomplete     = "started and did not complete"
	reasonLegacyUnpublic = `named by --include-table and not in publication "sluice_pub"`
)

// TestScopeDriftDoor_EstablishedArm: the record judged per admission shape on
// a pgoutput source — literal refuses; a glob or no filter refuses only when
// the table's changes would actually be applied (round 3 (c)).
func TestScopeDriftDoor_EstablishedArm(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		include   []string
		published []string
		shared    bool
		want      string // "" = no refusal
	}{
		{name: "literal, never copied", include: []string{"kd", "nt"}, published: []string{"kd"}, want: reasonLiteral},
		{name: "glob, not in the publication: WARN only", include: []string{"kd", "n*"}, published: []string{"kd"}},
		{name: "unfiltered, not in the publication: WARN only (L1)", published: []string{"kd"}},
		{name: "unfiltered, a shared publication delivers it (S1)", published: []string{"kd", "nt"}, shared: true, want: reasonInPublication},
		{name: "glob, the publication delivers it", include: []string{"kd", "n*"}, published: []string{"kd", "nt"}, want: reasonInPublication},
		{name: "literal, established only", include: []string{"kd"}, published: []string{"kd", "nt"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newScopeDriftEngine("kd", "nt")
			src.published, src.shared = tc.published, tc.shared
			err := driftStreamer(t, src, nonScoperEngine{}, tc.include...).phaseRefuseIncludeTableScopeDrift(ctx, recorded([]string{"kd"}), "st")
			if tc.want == "" {
				requireNoDrift(t, err)
				return
			}
			requireDrift(t, err, "nt ("+"", tc.want)
		})
	}

	t.Run("a missing publication is recreated FOR ALL TABLES: refuses", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.publication = ""
		requireDrift(t, driftStreamer(t, src, nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, recorded([]string{"kd"}), "st"),
			"recreates it FOR ALL TABLES")
	})

	t.Run("once per process", func(t *testing.T) {
		s := driftStreamer(t, newScopeDriftEngine("kd", "nt"), nonScoperEngine{}, "kd", "nt")
		s.scopeDriftDoorDone = true
		requireNoDrift(t, s.phaseRefuseIncludeTableScopeDrift(ctx, recorded([]string{"kd"}), "st"))
	})
}

// TestScopeDriftDoor_DroppedArm is N1: a table that left the scope after it
// was established refuses when ANY scope re-admits it, and a narrowing moves
// tables into the dropped set (dropped written first).
func TestScopeDriftDoor_DroppedArm(t *testing.T) {
	ctx := context.Background()
	for _, include := range [][]string{nil, {"kd", "n*"}, {"kd", "nt"}} {
		src := newScopeDriftEngine("kd", "nt")
		src.published = []string{"kd", "nt"}
		requireDrift(t, driftStreamer(t, src, nonScoperEngine{}, include...).phaseRefuseIncludeTableScopeDrift(ctx, recorded([]string{"kd"}, "nt"), "st"),
			"nt (", reasonDropped)
	}

	t.Run("a narrowing moves the table to dropped, dropped first", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.published = []string{"kd", "nt"}
		app := recorded([]string{"kd", "nt"})
		requireNoDrift(t, driftStreamer(t, src, nonScoperEngine{}, "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
		if strings.Join(app.writes, " ") != "dropped=nt established=kd" {
			t.Fatalf("writes %v, want [dropped=nt established=kd]", app.writes)
		}
	})

	t.Run("gone from the source is forgotten, not dropped", func(t *testing.T) {
		src := newScopeDriftEngine("kd")
		src.published = []string{"kd"}
		app := recorded([]string{"kd", "gone"}, "alsogone")
		requireNoDrift(t, driftStreamer(t, src, nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
		if strings.Join(app.writes, " ") != "dropped= established=kd" {
			t.Fatalf("writes %v, want [dropped= established=kd]", app.writes)
		}
	})

	t.Run("an unchanged scope writes nothing", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.published = []string{"kd", "nt"}
		app := recorded([]string{"kd", "nt"})
		requireNoDrift(t, driftStreamer(t, src, nonScoperEngine{}, "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
		if len(app.writes) != 0 {
			t.Fatalf("writes %v on an unchanged scope", app.writes)
		}
	})
}

// TestScopeDriftDoor_PublicationConviction is N2: an established table a FOR
// TABLE publication no longer lists refuses, shared or not; FOR ALL TABLES
// cannot lose a member that way.
func TestScopeDriftDoor_PublicationConviction(t *testing.T) {
	ctx := context.Background()
	for _, shared := range []bool{false, true} {
		src := newScopeDriftEngine("kd", "nt")
		src.published, src.shared = []string{"kd"}, shared
		requireDrift(t, driftStreamer(t, src, nonScoperEngine{}, "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, recorded([]string{"kd", "nt"}), "st"),
			"nt (", reasonUnpublished)
	}
	src := newScopeDriftEngine("kd", "nt")
	src.published, src.allTables = []string{"kd"}, true
	requireNoDrift(t, driftStreamer(t, src, nonScoperEngine{}, "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, recorded([]string{"kd", "nt"}), "st"))
}

// TestScopeDriftDoor_BinlogSource: with no publication, a never-established
// table's changes are applied wherever its target table exists.
func TestScopeDriftDoor_BinlogSource(t *testing.T) {
	ctx := context.Background()
	src, tgt := binlogPair([]string{"kd", "nt"}, []string{"kd", "nt"})
	requireDrift(t, driftStreamer(t, src, tgt).phaseRefuseIncludeTableScopeDrift(ctx, recorded([]string{"kd"}), "st"), "nt (", reasonTargetPresent)
	src, tgt = binlogPair([]string{"kd", "nt"}, []string{"kd"})
	requireNoDrift(t, driftStreamer(t, src, tgt).phaseRefuseIncludeTableScopeDrift(ctx, recorded([]string{"kd"}), "st"))
}

// TestScopeDriftDoor_PendingArm: a pending add-table refuses whatever admits
// its table — restartably (INCLUDE-TABLE-ADD-IN-PROGRESS) while its heartbeat
// is fresh and nothing else convicts, terminally otherwise; a pending name the
// source no longer has is ignored (L2).
func TestScopeDriftDoor_PendingArm(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	fresh := ir.PendingAddedTable{Table: "nt", StartedAt: now.Add(-time.Hour), HeartbeatAt: now.Add(-10 * time.Second)}
	stale := ir.PendingAddedTable{Table: "nt", StartedAt: now.Add(-time.Hour), HeartbeatAt: now.Add(-ir.PendingAddStaleAfter - time.Second)}
	pub := func() *scopeDriftEngine {
		src := newScopeDriftEngine("kd", "nt")
		src.published = []string{"kd", "nt"}
		return src
	}
	withPending := func(p ir.PendingAddedTable, est ...string) *scopeDriftApplier {
		app := recorded(est)
		app.pending = []ir.PendingAddedTable{p}
		return app
	}

	t.Run("fresh only: restartable", func(t *testing.T) {
		err := driftStreamer(t, pub(), nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, withPending(fresh, "kd", "nt"), "st")
		if !errors.Is(err, ir.ErrIncludeTableAddInProgress) || errors.Is(err, ir.ErrIncludeTableScopeDrift) {
			t.Fatalf("want only INCLUDE-TABLE-ADD-IN-PROGRESS, got %v", err)
		}
		// F2: the fleet supervisor restarts it (it is not in the roster) …
		if refusalARestartRepeats(err) != nil {
			t.Fatal("INCLUDE-TABLE-ADD-IN-PROGRESS is in the do-not-restart roster; a leg would stay dead after the add finished")
		}
	})
	t.Run("the terminal marker is not restarted", func(t *testing.T) {
		err := driftStreamer(t, pub(), nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, withPending(stale, "kd", "nt"), "st")
		if refusalARestartRepeats(err) == nil {
			t.Fatal("INCLUDE-TABLE-SCOPE-DRIFT left the do-not-restart roster")
		}
	})
	t.Run("stale: terminal", func(t *testing.T) {
		requireDrift(t, driftStreamer(t, pub(), nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, withPending(stale, "kd", "nt"), "st"),
			"nt (", reasonIncomplete)
	})
	t.Run("fresh plus another conviction: terminal", func(t *testing.T) {
		err := driftStreamer(t, pub(), nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, withPending(fresh, "kd"), "st")
		if !errors.Is(err, ir.ErrIncludeTableScopeDrift) {
			t.Fatalf("a live add plus an unestablished delivered table must be terminal, got %v", err)
		}
	})
	t.Run("gone from the source: ignored", func(t *testing.T) {
		src := newScopeDriftEngine("kd")
		src.published = []string{"kd"}
		requireNoDrift(t, driftStreamer(t, src, nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, withPending(stale, "kd"), "st"))
	})
	t.Run("a failed read is restartable, not the marker", func(t *testing.T) {
		app := recorded([]string{"kd"})
		app.pendErr = errors.New("connection reset by peer")
		err := driftStreamer(t, pub(), nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, app, "st")
		var cp *connectPhaseError
		if err == nil || errors.Is(err, ir.ErrIncludeTableScopeDrift) || !errors.As(err, &cp) {
			t.Fatalf("want a restartable non-marker failure, got %T %v", err, err)
		}
	})
}

// TestScopeDriftDoor_LegacyFallback: no record — the publication, for literal
// includes, only when no other sluice slot exists; never seeded.
func TestScopeDriftDoor_LegacyFallback(t *testing.T) {
	ctx := context.Background()
	src := newScopeDriftEngine("kd", "nt")
	src.published = []string{"kd"}
	requireDrift(t, driftStreamer(t, src, nonScoperEngine{}, "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{}, "st"),
		"nt (", reasonLegacyUnpublic)
	src.shared = true
	requireNoDrift(t, driftStreamer(t, src, nonScoperEngine{}, "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, &scopeDriftApplier{}, "st"))
	src.shared = false
	app := &scopeDriftApplier{}
	requireNoDrift(t, driftStreamer(t, src, nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
	if len(app.writes) != 0 {
		t.Fatalf("a legacy resume wrote %v; seeding would record never-copied tables as copied", app.writes)
	}
}

// TestScopeDriftRefusal_MultiDatabaseRemedy is F3: add-table cannot record a
// table on a multi-database stream, so its refusal names the re-copy only.
func TestScopeDriftRefusal_MultiDatabaseRemedy(t *testing.T) {
	s := &Streamer{Source: nonScoperEngine{}, Target: nonScoperEngine{}, AllDatabases: true}
	err := s.includeTableScopeDriftError("st", "", []scopeDriftTable{{name: "app.nt", notEstablished: true}})
	if !strings.Contains(err.Error(), "--restart-from-scratch") || strings.Contains(err.Error(), "run `sluice schema add-table") {
		t.Fatalf("multi-database remedy: %v", err)
	}
	s.AllDatabases = false
	if err := s.includeTableScopeDriftError("st", "", []scopeDriftTable{{name: "nt", notEstablished: true}}); !strings.Contains(err.Error(), "run `sluice schema add-table") {
		t.Fatalf("single-namespace remedy: %v", err)
	}
}

// TestColdStartEstablishedRecord is N3: a cold start resets the record before
// it copies, so a failed write leaves the stream UNRECORDED rather than
// holding the older copy's record; on success, previously dropped tables the
// copy did not cover stay dropped and only the copied tables' pending
// entries are cleared.
func TestColdStartEstablishedRecord(t *testing.T) {
	ctx := context.Background()
	s := &Streamer{}

	app := recorded([]string{"kd", "nt", "old"}, "gone", "nt2")
	app.recordErr = errors.New("connection reset")
	if err := s.resetEstablishedForColdStart(ctx, app, "st"); err != nil {
		t.Fatal(err)
	}
	s.coldStartEstablished = []string{"kd"}
	s.recordColdStartEstablished(ctx, app, "st")
	if app.rec.Recorded {
		t.Fatalf("a failed record write left a record %+v; want the stream unrecorded", app.rec)
	}

	app = recorded([]string{"kd"}, "nt", "nt2")
	if err := s.resetEstablishedForColdStart(ctx, app, "st"); err != nil {
		t.Fatal(err)
	}
	s.coldStartEstablished = []string{"kd", "nt"}
	s.recordColdStartEstablished(ctx, app, "st")
	if got := strings.Join(app.writes, " "); got != "reset established=kd,nt dropped=nt2 clear=kd,nt" {
		t.Fatalf("writes %q", got)
	}
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
