// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// scopeDriftApplier is an in-memory control row: the established record, the
// pending add-tables and the live-added tables, with every write logged.
type scopeDriftApplier struct {
	ir.ChangeApplier
	pending     []ir.PendingAddedTable
	pendErr     error
	serverNow   time.Time // the control-table server's clock; zero = this host's
	rec         ir.EstablishedRecord
	recordErr   error
	beginErr    error
	touchErr    error
	completeErr error
	writes      []string // "established=a,b" / "dropped=a" / "begin" / "clear=a" / "touch=a alive" / "complete=a"
	liveAdded   []string
	narrowErr   error        // NarrowEstablishedRecord fails with it
	recErr      error        // ReadEstablishedRecord fails with it
	position    *ir.Position // ReadPosition reports it as found; nil = no position
}

func (a *scopeDriftApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	if a.position == nil {
		return ir.Position{}, false, nil
	}
	return *a.position, true, nil
}

func (a *scopeDriftApplier) now() time.Time {
	if a.serverNow.IsZero() {
		return time.Now()
	}
	return a.serverNow
}

func (a *scopeDriftApplier) RecordPendingAddedTable(context.Context, string, string, string) error {
	return errors.New("not used")
}

func (a *scopeDriftApplier) TouchPendingAddedTable(_ context.Context, _, table, _ string, alive bool) error {
	state := "stopped"
	if alive {
		state = "alive"
	}
	a.writes = append(a.writes, "touch="+table+" "+state)
	return a.touchErr
}

func (a *scopeDriftApplier) CompletePendingAddedTable(_ context.Context, _, table, _, _ string) error {
	if a.completeErr != nil {
		return a.completeErr
	}
	a.writes = append(a.writes, "complete="+table)
	return nil
}

func (a *scopeDriftApplier) ClearPendingAddedTables(_ context.Context, _ string, tables []string) error {
	a.writes = append(a.writes, "clear="+strings.Join(tables, ","))
	return nil
}

func (a *scopeDriftApplier) AbandonPendingAddedTable(_ context.Context, _, table string) error {
	a.writes = append(a.writes, "abandon="+table)
	return nil
}

func (a *scopeDriftApplier) NarrowEstablishedRecord(_ context.Context, _ string, n ir.RecordNarrowing) ([]string, error) {
	if a.narrowErr != nil {
		return nil, a.narrowErr
	}
	est, drop, leaving := appliershared.NarrowRecord(a.rec.Established, a.rec.Dropped, n.InScope, n.Gone, n.Renames)
	if len(leaving) > 0 || !sameSet(drop, a.rec.Dropped) || !sameSet(est, a.rec.Established) {
		a.writes = append(a.writes, "narrow=established:"+strings.Join(est, ",")+" dropped:"+strings.Join(drop, ","))
		a.rec.Established, a.rec.Dropped = est, drop
	}
	if len(n.IDs) > 0 {
		ids := make([]string, 0, len(n.IDs))
		for k, v := range n.IDs {
			ids = append(ids, k+":"+v)
		}
		sort.Strings(ids)
		a.writes = append(a.writes, "ids="+strings.Join(ids, ","))
	}
	return leaving, nil
}

func (a *scopeDriftApplier) ReadPendingAddedTables(context.Context, string) ([]ir.PendingAddedTable, time.Time, error) {
	return a.pending, a.now(), a.pendErr
}

func (a *scopeDriftApplier) ReadEstablishedRecord(context.Context, string) (ir.EstablishedRecord, error) {
	if a.recErr != nil {
		return ir.EstablishedRecord{}, a.recErr
	}
	return a.rec, nil
}

func (a *scopeDriftApplier) BeginColdStartRecord(context.Context, string) error {
	if a.beginErr != nil {
		return a.beginErr
	}
	a.writes = append(a.writes, "begin")
	a.rec = ir.EstablishedRecord{Recorded: true, ColdStartInProgress: true, Dropped: a.rec.Dropped}
	return nil
}

func (a *scopeDriftApplier) RecordEstablishedTables(_ context.Context, _ string, tables []string, ids map[string]string) error {
	a.rec.IDs = ids
	if a.recordErr != nil {
		return a.recordErr
	}
	a.writes = append(a.writes, "established="+strings.Join(tables, ","))
	a.rec.Recorded, a.rec.ColdStartInProgress, a.rec.Established = true, false, tables
	return nil
}

func (a *scopeDriftApplier) RecordDroppedTables(_ context.Context, _ string, tables []string) error {
	a.writes = append(a.writes, "dropped="+strings.Join(tables, ","))
	a.rec.Dropped = tables
	return nil
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

	t.Run("a narrowing moves the table to dropped, in one locked write", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt")
		src.published = []string{"kd", "nt"}
		app := recorded([]string{"kd", "nt"})
		requireNoDrift(t, driftStreamer(t, src, nonScoperEngine{}, "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
		if strings.Join(app.writes, " ") != "narrow=established:kd dropped:nt" {
			t.Fatalf("writes %v, want [narrow=established:kd dropped:nt]", app.writes)
		}
	})

	t.Run("gone from the source is forgotten, not dropped", func(t *testing.T) {
		src := newScopeDriftEngine("kd")
		src.published = []string{"kd"}
		app := recorded([]string{"kd", "gone"}, "alsogone")
		requireNoDrift(t, driftStreamer(t, src, nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
		if strings.Join(app.writes, " ") != "narrow=established:kd dropped:" {
			t.Fatalf("writes %v, want [narrow=established:kd dropped:]", app.writes)
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
	// The negative a mutation run found missing: established and still a
	// member of a FOR TABLE publication, shared or not, passes.
	for _, shared := range []bool{false, true} {
		src := newScopeDriftEngine("kd", "nt")
		src.published, src.shared = []string{"kd", "nt"}, shared
		requireNoDrift(t, driftStreamer(t, src, nonScoperEngine{}, "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, recorded([]string{"kd", "nt"}), "st"))
	}
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
	// F-A: a live add overrides the convictions of ITS table that its
	// completion cures — never established (literal or delivered) and
	// previously dropped — so a restart racing a real add-table defers.
	for name, mk := range map[string]func() (*Streamer, *scopeDriftApplier){
		"delivered, never established": func() (*Streamer, *scopeDriftApplier) {
			return driftStreamer(t, pub(), nonScoperEngine{}), withPending(fresh, "kd")
		},
		"named literally, never established": func() (*Streamer, *scopeDriftApplier) {
			return driftStreamer(t, pub(), nonScoperEngine{}, "kd", "nt"), withPending(fresh, "kd")
		},
		"previously dropped": func() (*Streamer, *scopeDriftApplier) {
			app := withPending(fresh, "kd")
			app.rec.Dropped = []string{"nt"}
			return driftStreamer(t, pub(), nonScoperEngine{}), app
		},
		"no record, literal, unpublished": func() (*Streamer, *scopeDriftApplier) {
			src := pub()
			src.published = []string{"kd"}
			return driftStreamer(t, src, nonScoperEngine{}, "kd", "nt"), &scopeDriftApplier{pending: []ir.PendingAddedTable{fresh}}
		},
	} {
		t.Run("F-A: the live add overrides its own table: "+name, func(t *testing.T) {
			s, app := mk()
			err := s.phaseRefuseIncludeTableScopeDrift(ctx, app, "st")
			if !errors.Is(err, ir.ErrIncludeTableAddInProgress) || errors.Is(err, ir.ErrIncludeTableScopeDrift) {
				t.Fatalf("want only INCLUDE-TABLE-ADD-IN-PROGRESS, got %v", err)
			}
		})
	}
	t.Run("F-A: another table's conviction still counts", func(t *testing.T) {
		src := newScopeDriftEngine("kd", "nt", "x")
		src.published = []string{"kd", "nt", "x"}
		err := driftStreamer(t, src, nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, withPending(fresh, "kd"), "st")
		requireDrift(t, err, "x (never copied by this stream", "nt (its `sluice schema add-table` is still running")
		if strings.Contains(err.Error(), "nt (never copied") {
			t.Fatalf("the live add's own table kept a conviction the add cures: %v", err)
		}
	})
	t.Run("F-A: N2 is not overridden", func(t *testing.T) {
		src := pub()
		src.published = []string{"kd"}
		requireDrift(t, driftStreamer(t, src, nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, withPending(fresh, "kd", "nt"), "st"),
			reasonUnpublished)
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

// TestScopeDriftDoor_AbandonedAdd is R4-1: an entry `--abandon` retired
// convicts terminally on any stream open that would apply changes onto the
// table, and is cleared — only — by an open that finds the table out of
// scope, gone from the source, or provably not applied.
func TestScopeDriftDoor_AbandonedAdd(t *testing.T) {
	ctx := context.Background()
	abandoned := ir.PendingAddedTable{Table: "nt", Token: ir.PendingAddAbandonedToken, StartedAt: time.Now().Add(-time.Hour), HeartbeatAt: time.Unix(0, 0)}
	withAbandoned := func(app *scopeDriftApplier) *scopeDriftApplier {
		app.pending = []ir.PendingAddedTable{abandoned}
		return app
	}
	src := func(tables []string, published ...string) *scopeDriftEngine {
		e := newScopeDriftEngine(tables...)
		e.published = published
		return e
	}
	const reasonAbandoned = "was abandoned (`--abandon`)"
	cleared := func(app *scopeDriftApplier) bool {
		for _, w := range app.writes {
			if w == "clear=nt" {
				return true
			}
		}
		return false
	}

	t.Run("established, in scope: refuses", func(t *testing.T) {
		app := withAbandoned(recorded([]string{"kd", "nt"}))
		requireDrift(t, driftStreamer(t, src([]string{"kd", "nt"}, "kd", "nt"), nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, app, "st"),
			"nt (", reasonAbandoned)
		if cleared(app) {
			t.Fatal("a refused open cleared the abandoned entry")
		}
	})
	t.Run("unfiltered, delivered: refuses", func(t *testing.T) {
		app := withAbandoned(recorded([]string{"kd"}))
		requireDrift(t, driftStreamer(t, src([]string{"kd", "nt"}, "kd", "nt"), nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, app, "st"),
			reasonAbandoned, reasonInPublication)
	})
	t.Run("literal, never established: refuses", func(t *testing.T) {
		app := withAbandoned(recorded([]string{"kd"}))
		requireDrift(t, driftStreamer(t, src([]string{"kd", "nt"}, "kd"), nonScoperEngine{}, "kd", "nt").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"),
			reasonAbandoned, reasonLiteral)
	})
	t.Run("no record, not provably undelivered: refuses", func(t *testing.T) {
		app := withAbandoned(&scopeDriftApplier{})
		requireDrift(t, driftStreamer(t, src([]string{"kd", "nt"}, "kd", "nt"), nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, app, "st"),
			reasonAbandoned)
	})
	for name, mk := range map[string]func() (*Streamer, *scopeDriftApplier){
		"out of scope": func() (*Streamer, *scopeDriftApplier) {
			return driftStreamer(t, src([]string{"kd", "nt"}, "kd", "nt"), nonScoperEngine{}, "kd"), withAbandoned(recorded([]string{"kd"}))
		},
		"gone from the source": func() (*Streamer, *scopeDriftApplier) {
			return driftStreamer(t, src([]string{"kd"}, "kd"), nonScoperEngine{}), withAbandoned(recorded([]string{"kd"}))
		},
		"unfiltered, not in the publication": func() (*Streamer, *scopeDriftApplier) {
			return driftStreamer(t, src([]string{"kd", "nt"}, "kd"), nonScoperEngine{}), withAbandoned(recorded([]string{"kd"}))
		},
		"no record, not in a FOR TABLE publication": func() (*Streamer, *scopeDriftApplier) {
			return driftStreamer(t, src([]string{"kd", "nt"}, "kd"), nonScoperEngine{}), withAbandoned(&scopeDriftApplier{})
		},
	} {
		t.Run("cleared: "+name, func(t *testing.T) {
			s, app := mk()
			requireNoDrift(t, s.phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
			if !cleared(app) {
				t.Fatalf("writes %v; want the abandoned entry cleared", app.writes)
			}
		})
	}
	t.Run("a stale entry that is not abandoned is not cleared out of scope", func(t *testing.T) {
		app := recorded([]string{"kd"})
		app.pending = []ir.PendingAddedTable{{Table: "nt", Token: "t", StartedAt: abandoned.StartedAt, HeartbeatAt: abandoned.StartedAt}}
		requireNoDrift(t, driftStreamer(t, src([]string{"kd", "nt"}, "kd", "nt"), nonScoperEngine{}, "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
		if cleared(app) {
			t.Fatal("cleared a dead add's entry; only --abandon retires it")
		}
	})
}

// TestScopeDriftDoor_ServerClock is S-C: the pending classifier compares the
// entry against the control-table server's now, never this host's. The server
// here runs three hours ahead of the host: an entry it stamped ten seconds
// ago is fresh (by the host's clock it would be from the future), and one
// stamped beyond the tolerance ahead of the server's own now is stale.
func TestScopeDriftDoor_ServerClock(t *testing.T) {
	ctx := context.Background()
	serverNow := time.Now().Add(3 * time.Hour)
	run := func(heartbeat time.Time) error {
		src := newScopeDriftEngine("kd", "nt")
		src.published = []string{"kd", "nt"}
		app := recorded([]string{"kd", "nt"})
		app.serverNow = serverNow
		app.pending = []ir.PendingAddedTable{{Table: "nt", Token: "t", StartedAt: heartbeat, HeartbeatAt: heartbeat}}
		return driftStreamer(t, src, nonScoperEngine{}).phaseRefuseIncludeTableScopeDrift(ctx, app, "st")
	}
	if err := run(serverNow.Add(-10 * time.Second)); !errors.Is(err, ir.ErrIncludeTableAddInProgress) {
		t.Fatalf("an entry the server stamped 10s ago: want INCLUDE-TABLE-ADD-IN-PROGRESS, got %v", err)
	}
	if err := run(time.Now().Add(-10 * time.Second)); !errors.Is(err, ir.ErrIncludeTableScopeDrift) {
		t.Fatalf("an entry 10s old by the HOST's clock is 3h old by the server's: want terminal, got %v", err)
	}
	if err := run(serverNow.Add(ir.PendingAddFutureTolerance + time.Minute)); !errors.Is(err, ir.ErrIncludeTableScopeDrift) {
		t.Fatalf("a heartbeat ahead of the server's clock: want stale (terminal), got %v", err)
	}
	if err := run(serverNow.Add(ir.PendingAddFutureTolerance / 2)); !errors.Is(err, ir.ErrIncludeTableAddInProgress) {
		t.Fatalf("a heartbeat within the future tolerance: want fresh, got %v", err)
	}
}

// TestPendingAddedTableClassifier pins Fresh / FromTheFuture at their
// boundaries.
func TestPendingAddedTableClassifier(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	for _, c := range []struct {
		beat          time.Duration // heartbeat relative to now
		fresh, future bool
	}{
		{0, true, false},
		{-ir.PendingAddStaleAfter + time.Millisecond, true, false},
		{-ir.PendingAddStaleAfter, false, false},
		{ir.PendingAddFutureTolerance, true, false},
		{ir.PendingAddFutureTolerance + time.Millisecond, false, true},
	} {
		p := ir.PendingAddedTable{HeartbeatAt: now.Add(c.beat)}
		if p.Fresh(now) != c.fresh || p.FromTheFuture(now) != c.future {
			t.Errorf("heartbeat %v: Fresh=%v FromTheFuture=%v; want %v %v", c.beat, p.Fresh(now), p.FromTheFuture(now), c.fresh, c.future)
		}
	}
}

// TestScopeDriftDoor_ColdStartInProgress is S-A: a record a cold start
// marked at its point of no return refuses every warm resume terminally —
// whatever the scope — until a cold start finishes.
func TestScopeDriftDoor_ColdStartInProgress(t *testing.T) {
	ctx := context.Background()
	src := newScopeDriftEngine("kd")
	src.published = []string{"kd"}
	app := &scopeDriftApplier{rec: ir.EstablishedRecord{Recorded: true, ColdStartInProgress: true}}
	err := driftStreamer(t, src, nonScoperEngine{}, "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st")
	requireDrift(t, err, "COLD-START-INCOMPLETE", "--restart-from-scratch")
	if refusalARestartRepeats(err) == nil {
		t.Fatal("COLD-START-INCOMPLETE must be terminal: a restart reads the same record")
	}
	app.rec = ir.EstablishedRecord{Recorded: true, Established: []string{"kd"}}
	requireNoDrift(t, driftStreamer(t, src, nonScoperEngine{}, "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
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

// TestColdStartEstablishedRecord is S-A: a recorded stream never returns to
// "not recorded". The point of no return marks it in progress, keeping the
// dropped set; a failed final write leaves it in progress (which refuses), not
// unrecorded (which the legacy fallback can pass); success writes dropped
// minus copied, then established, and clears only the copied tables' pending
// entries. A mark that fails stops the cold start of a recorded stream and
// only WARNs on one with no record.
func TestColdStartEstablishedRecord(t *testing.T) {
	ctx := context.Background()
	s := &Streamer{}

	app := recorded([]string{"kd", "nt", "old"}, "gone", "nt2")
	if err := beginColdStartRecord(ctx, app, "st"); err != nil {
		t.Fatal(err)
	}
	if !app.rec.Recorded || !app.rec.ColdStartInProgress || len(app.rec.Dropped) != 2 {
		t.Fatalf("after the mark: %+v; want recorded, in progress, dropped kept", app.rec)
	}
	app.recordErr = errors.New("connection reset")
	s.coldStartEstablished = []string{"kd"}
	s.recordColdStartEstablished(ctx, app, "st")
	if !app.rec.Recorded || !app.rec.ColdStartInProgress {
		t.Fatalf("a failed record write left %+v; want the in-progress mark, never unrecorded", app.rec)
	}

	app = recorded([]string{"kd"}, "nt", "nt2")
	if err := beginColdStartRecord(ctx, app, "st"); err != nil {
		t.Fatal(err)
	}
	s.coldStartEstablished = []string{"kd", "nt"}
	s.recordColdStartEstablished(ctx, app, "st")
	if got := strings.Join(app.writes, " "); got != "begin dropped=nt2 established=kd,nt clear=kd,nt" {
		t.Fatalf("writes %q", got)
	}

	app = recorded([]string{"kd"})
	app.beginErr = errors.New("permission denied")
	if err := beginColdStartRecord(ctx, app, "st"); err == nil || !strings.Contains(err.Error(), "the target was not changed") {
		t.Fatalf("a failed mark of a recorded stream: %v; want the cold start stopped", err)
	}
	app = &scopeDriftApplier{beginErr: errors.New("permission denied")}
	if err := beginColdStartRecord(ctx, app, "st"); err != nil {
		t.Fatalf("a failed mark of a stream with no record: %v; want a WARN only", err)
	}
	// No record but a persisted position (an older stream on a control table
	// this role cannot ALTER): the mark is what keeps a re-copy stopped
	// mid-copy from being warm-resumed from that position onto the cleared,
	// partly re-copied target, so failing to write it stops the cold start.
	app = &scopeDriftApplier{beginErr: errors.New("permission denied"), position: &ir.Position{Engine: "mysql", Token: "old"}}
	if err := beginColdStartRecord(ctx, app, "st"); err == nil || !strings.Contains(err.Error(), "the target was not changed") {
		t.Fatalf("a failed mark of an unrecorded stream WITH a position: %v; want the cold start stopped", err)
	}
}

// TestScopeDriftDoor_ColdStartInProgressAfterTheDoorRan: the door's other
// arms run once per process, but the in-progress mark is re-checked on every
// warm resume, so a cold start begun later in the same process (the reactive
// re-snapshot) that stopped mid-copy cannot fall back to a warm resume from
// the position it was replacing.
func TestScopeDriftDoor_ColdStartInProgressAfterTheDoorRan(t *testing.T) {
	ctx := context.Background()
	src := newScopeDriftEngine("kd")
	src.published = []string{"kd"}
	s := driftStreamer(t, src, nonScoperEngine{}, "kd")
	s.scopeDriftDoorDone = true
	app := &scopeDriftApplier{rec: ir.EstablishedRecord{Recorded: true, ColdStartInProgress: true}}
	requireDrift(t, s.phaseRefuseIncludeTableScopeDrift(ctx, app, "st"), "COLD-START-INCOMPLETE", "--restart-from-scratch")
	app.rec = ir.EstablishedRecord{Recorded: true, Established: []string{"kd"}}
	requireNoDrift(t, s.phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
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

	t.Run("S-B: an entry no longer this run's is not undone", func(t *testing.T) {
		src := &addTableUndoSource{}
		app := &scopeDriftApplier{touchErr: ir.ErrPendingAddSuperseded}
		a := &AddTable{Source: src, TableName: "nt", publicationAddedByThisRun: true}
		err := a.undoFailedAdd(context.Background(), app, cause)
		if !errors.Is(err, cause) || !errors.Is(err, ir.ErrPendingAddSuperseded) || !strings.Contains(err.Error(), "abandoned") {
			t.Fatalf("got %v", err)
		}
		if len(src.dropped) != 0 {
			t.Fatalf("dropped %v from the publication; it belongs to whoever retired the entry", src.dropped)
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

// TestAddTablePendingTokenCAS is S-B on the add-table side: a heartbeat that
// finds its entry abandoned or taken over cancels the copy with
// ErrPendingAddSuperseded as the cause, and a completion refused for the same
// reason is reported as such, recording nothing.
func TestAddTablePendingTokenCAS(t *testing.T) {
	t.Run("a superseded heartbeat stops the copy", func(t *testing.T) {
		app := &scopeDriftApplier{touchErr: fmt.Errorf("postgres: pending add-table %q: %w", "nt", ir.ErrPendingAddSuperseded)}
		a := &AddTable{TableName: "nt", pendingToken: "tok", pendingHeartbeatEvery: time.Millisecond}
		copyCtx, cancelCopy := context.WithCancelCause(context.Background())
		defer cancelCopy(nil)
		stop := a.startPendingHeartbeat(copyCtx, app, cancelCopy)
		defer stop()
		select {
		case <-copyCtx.Done():
		case <-time.After(10 * time.Second):
			t.Fatal("the copy kept running after its pending entry was superseded")
		}
		if !errors.Is(context.Cause(copyCtx), ir.ErrPendingAddSuperseded) {
			t.Fatalf("cause %v; want ErrPendingAddSuperseded", context.Cause(copyCtx))
		}
	})
	t.Run("a failed heartbeat of another kind only WARNs", func(t *testing.T) {
		app := &scopeDriftApplier{touchErr: errors.New("connection reset")}
		a := &AddTable{TableName: "nt", pendingToken: "tok", pendingHeartbeatEvery: 10 * time.Millisecond}
		copyCtx, cancelCopy := context.WithCancelCause(context.Background())
		defer cancelCopy(nil)
		stop := a.startPendingHeartbeat(copyCtx, app, cancelCopy)
		time.Sleep(50 * time.Millisecond)
		stop()
		if copyCtx.Err() != nil {
			t.Fatalf("a transient heartbeat failure cancelled the copy: %v", context.Cause(copyCtx))
		}
	})
	t.Run("a superseded completion records nothing and says so", func(t *testing.T) {
		app := &scopeDriftApplier{completeErr: ir.ErrPendingAddSuperseded}
		a := &AddTable{TableName: "nt", pendingToken: "tok"}
		err := a.recordCompletedAdd(context.Background(), app)
		if !errors.Is(err, ir.ErrPendingAddSuperseded) || !strings.Contains(err.Error(), "without recording the table as established") {
			t.Fatalf("got %v", err)
		}
		if len(app.writes) != 0 {
			t.Fatalf("writes %v", app.writes)
		}
	})
}
