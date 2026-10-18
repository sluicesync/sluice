// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// idSource is a pgoutput source that reports engine-stable table ids.
type idSource struct {
	*scopeDriftEngine
	ids map[string]string
}

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
