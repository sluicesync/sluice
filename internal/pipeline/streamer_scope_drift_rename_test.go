// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// idSource is a pgoutput source that reports engine-stable table ids;
// rebuildUnstable makes it an engine whose rebuilds change them (InnoDB).
type idSource struct {
	*scopeDriftEngine
	ids             map[string]string
	rebuildUnstable bool
}

func (e *idSource) TableIdentitySurvivesRebuild() bool { return !e.rebuildUnstable }

func (e *idSource) TableIdentities(_ context.Context, _ string, tables []string) (map[string]string, error) {
	out := map[string]string{}
	for _, t := range tables {
		if id, ok := e.ids[t]; ok {
			out[t] = id
		}
	}
	return out, nil
}

func renameTarget(tables ...string) *ukDoorEngine {
	return &ukDoorEngine{
		resumeDispatchEngine: &resumeDispatchEngine{name: "postgres"},
		schemas:              map[string]*ir.Schema{"tgt-dsn": driftSchema(tables...)},
	}
}

// TestScopeDriftDoor_FollowsARename is Bug 321 R6's rename: t8 renamed to
// t8b on the source (same id) and on the target while the stream was
// stopped — the documented drained-model recovery — passes, and the record
// follows the rename. The target left unrenamed refuses saying so; a drop
// and re-create under the new name (a new id) refuses as never copied and
// names --accept-table-rename; the acknowledgement follows a rename no id
// proves.
func TestScopeDriftDoor_FollowsARename(t *testing.T) {
	ctx := context.Background()
	source := func(ids map[string]string) *idSource {
		e := newScopeDriftEngine("kd", "t8b")
		e.published = []string{"kd", "t8b"} // ALTER TABLE … RENAME keeps publication membership
		return &idSource{scopeDriftEngine: e, ids: ids}
	}
	record := func(ids map[string]string) *scopeDriftApplier {
		app := recorded([]string{"kd", "t8"})
		app.rec.IDs = ids
		return app
	}

	t.Run("same id, target renamed: followed", func(t *testing.T) {
		app := record(map[string]string{"kd": "1", "t8": "100"})
		requireNoDrift(t, driftStreamer(t, source(map[string]string{"kd": "1", "t8b": "100"}), renameTarget("kd", "t8b")).
			phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
		if got := strings.Join(app.writes, " "); got != "narrow=established:kd,t8b dropped:" {
			t.Fatalf("writes %q; want the record to follow t8 → t8b", got)
		}
	})
	t.Run("same id, target not renamed: refuses", func(t *testing.T) {
		app := record(map[string]string{"kd": "1", "t8": "100"})
		requireDrift(t, driftStreamer(t, source(map[string]string{"kd": "1", "t8b": "100"}), renameTarget("kd", "t8")).
			phaseRefuseIncludeTableScopeDrift(ctx, app, "st"), "t8b (", "the target was not renamed")
	})
	// R6-S2: a stale, unrelated t8b on the target beside the stream's real
	// copy under t8 is not a renamed target; following would apply onto the
	// stale table and strand the real copy.
	t.Run("same id, target holds both names: refuses", func(t *testing.T) {
		app := record(map[string]string{"kd": "1", "t8": "100"})
		requireDrift(t, driftStreamer(t, source(map[string]string{"kd": "1", "t8b": "100"}), renameTarget("kd", "t8", "t8b")).
			phaseRefuseIncludeTableScopeDrift(ctx, app, "st"), "t8b (", "the target holds both t8 and t8b", "the target was not renamed")
		if len(app.writes) != 0 {
			t.Fatalf("writes %q; a refusal must not narrow the record", app.writes)
		}
	})
	t.Run("acknowledged, target holds both names: refuses", func(t *testing.T) {
		s := driftStreamer(t, source(nil), renameTarget("kd", "t8", "t8b"))
		s.AcceptTableRenames = []string{"t8=t8b"}
		requireDrift(t, s.phaseRefuseIncludeTableScopeDrift(ctx, record(nil), "st"), "the target holds both t8 and t8b")
	})
	t.Run("drop and re-create under the new name: refuses", func(t *testing.T) {
		app := record(map[string]string{"kd": "1", "t8": "100"})
		err := driftStreamer(t, source(map[string]string{"kd": "1", "t8b": "200"}), renameTarget("kd", "t8b")).
			phaseRefuseIncludeTableScopeDrift(ctx, app, "st")
		requireDrift(t, err, "t8b (", "never copied", "--accept-table-rename <old>=t8b", "t8, gone")
	})
	t.Run("no id, acknowledged: followed", func(t *testing.T) {
		app := record(nil)
		s := driftStreamer(t, source(nil), renameTarget("kd", "t8b"))
		s.AcceptTableRenames = []string{"t8=t8b"}
		requireNoDrift(t, s.phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
		if got := strings.Join(app.writes, " "); got != "narrow=established:kd,t8b dropped:" {
			t.Fatalf("writes %q", got)
		}
	})
	t.Run("no id, not acknowledged: refuses naming the flag", func(t *testing.T) {
		requireDrift(t, driftStreamer(t, source(nil), renameTarget("kd", "t8b")).
			phaseRefuseIncludeTableScopeDrift(ctx, record(nil), "st"), "--accept-table-rename")
	})
	t.Run("a malformed acknowledgement refuses", func(t *testing.T) {
		s := driftStreamer(t, source(nil), renameTarget("kd", "t8b"))
		s.AcceptTableRenames = []string{"t8"}
		if err := s.phaseRefuseIncludeTableScopeDrift(ctx, record(nil), "st"); err == nil || !strings.Contains(err.Error(), "want <old-table>=<new-table>") {
			t.Fatalf("got %v", err)
		}
	})
}

// TestScopeDriftDoor_TableIdentity is R6-S3: an established table whose
// engine id changed under the same name. On Postgres (ids survive a
// rebuild) a swap and a drop-and-re-create refuse TABLE-IDENTITY-CHANGED,
// the swap named as one; an unchanged id (VACUUM FULL, CLUSTER) passes. On
// an engine whose rebuilds change ids (InnoDB) a bare change passes and the
// record takes the new id; a swap — proved by one name's recorded id now
// belonging to another — still refuses.
func TestScopeDriftDoor_TableIdentity(t *testing.T) {
	ctx := context.Background()
	source := func(ids map[string]string, rebuildUnstable bool) *idSource {
		e := newScopeDriftEngine("kd", "t8", "t9")
		e.published = []string{"kd", "t8", "t9"} // a FOR TABLE publication follows its members by oid through a swap
		return &idSource{scopeDriftEngine: e, ids: ids, rebuildUnstable: rebuildUnstable}
	}
	record := func() *scopeDriftApplier {
		app := recorded([]string{"kd", "t8", "t9"})
		app.rec.IDs = map[string]string{"kd": "1", "t8": "100", "t9": "200"}
		return app
	}
	swapped := map[string]string{"kd": "1", "t8": "200", "t9": "100"}
	door := func(src *idSource, app *scopeDriftApplier) error {
		return driftStreamer(t, src, renameTarget("kd", "t8", "t9")).phaseRefuseIncludeTableScopeDrift(ctx, app, "st")
	}

	t.Run("postgres: swap refuses, named", func(t *testing.T) {
		app := record()
		requireDrift(t, door(source(swapped, false), app), TableIdentityChangedMarker, "t8 (", "t9 (",
			"swapped it with another table", "now holds the table this stream recorded as t9")
		if len(app.writes) != 0 {
			t.Fatalf("writes %q on a refusal", app.writes)
		}
	})
	t.Run("postgres: drop and re-create under the same name refuses", func(t *testing.T) {
		requireDrift(t, door(source(map[string]string{"kd": "1", "t8": "300", "t9": "200"}, false), record()),
			TableIdentityChangedMarker, "t8 (", "dropped and re-created")
	})
	t.Run("postgres: unchanged ids pass", func(t *testing.T) {
		app := record()
		requireNoDrift(t, door(source(map[string]string{"kd": "1", "t8": "100", "t9": "200"}, false), app))
		if len(app.writes) != 0 {
			t.Fatalf("writes %q; nothing changed", app.writes)
		}
	})
	t.Run("innodb: a rebuilt table passes and the record takes its new id", func(t *testing.T) {
		app := record()
		requireNoDrift(t, door(source(map[string]string{"kd": "1", "t8": "300", "t9": "200"}, true), app))
		if got := strings.Join(app.writes, " "); got != "ids=t8:300" {
			t.Fatalf("writes %q; want the record to take t8's new id", got)
		}
	})
	t.Run("innodb: a swap refuses", func(t *testing.T) {
		requireDrift(t, door(source(swapped, true), record()), TableIdentityChangedMarker, "t8 (", "t9 (", "swapped it")
	})
	t.Run("innodb: a recorded id now under a new in-scope name refuses the old name", func(t *testing.T) {
		e := newScopeDriftEngine("kd", "t8", "t9", "t8b")
		e.published = []string{"kd", "t8", "t9", "t8b"}
		src := &idSource{scopeDriftEngine: e, ids: map[string]string{"kd": "1", "t8": "300", "t9": "200", "t8b": "100"}, rebuildUnstable: true}
		requireDrift(t, driftStreamer(t, src, renameTarget("kd", "t8", "t9", "t8b")).phaseRefuseIncludeTableScopeDrift(ctx, record(), "st"),
			TableIdentityChangedMarker, "t8 (", "recorded under this name is now t8b")
	})
}
