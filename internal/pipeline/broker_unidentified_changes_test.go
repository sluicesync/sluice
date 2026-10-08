// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestBroker_UnidentifiedKeyedChangesWarn pins the ADR-0191 review's item 3
// qualification: an incremental that records identities can still carry a
// KEYED row change without one (a VStream COPY row, a MySQL transaction with
// no GTID or server identity, an ADD COLUMN fill), which replays without
// marks. That is not refused — it converges by key and is routine on VStream
// chains — but the exactly-once claim does not cover it, so the broker says
// so: one BROKER-UNIDENTIFIED-CHANGES WARN per such incremental, with the
// count. An incremental whose keyed rows are all identified logs nothing, and
// neither does one that records no identities at all (its whole replay is
// documented as unmarked).
func TestBroker_UnidentifiedKeyedChangesWarn(t *testing.T) {
	pos := func(lsn string) ir.Position {
		return ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`}
	}
	rows := func(identified bool) []ir.Change {
		id := func(seq uint64) ir.ApplyID {
			if !identified {
				return ir.ApplyID{}
			}
			return ir.ApplyID{TxID: "pg:1:1:0/150", Seq: seq}
		}
		return []ir.Change{
			ir.TxBegin{Position: pos("0/140")},
			ir.Insert{Position: pos("0/150"), Table: "k", Row: ir.Row{"id": int64(9)}, ApplyID: id(1)},
			ir.Insert{Position: pos("0/150"), Table: "k", Row: ir.Row{"id": int64(10)}, ApplyID: id(2)},
			ir.TxCommit{Position: pos("0/160")},
		}
	}
	for _, tc := range []struct {
		name               string
		identified, stamps bool
		wantWarn           bool
	}{
		{name: "identities recorded, keyed rows without one", identified: false, stamps: true, wantWarn: true},
		{name: "identities recorded, every keyed row identified", identified: true, stamps: true},
		{name: "no identities recorded", identified: false, stamps: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := &logcapture.Buffer{}
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(prev) })

			store, fullID := doorFixture(t, rows(tc.identified), tc.stamps)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			runCtx, runCancel := context.WithCancel(ctx)
			defer runCancel()
			app := &coveringApplier{}
			p := encodeBrokerPosition("test://fe1", fullID)
			app.resume = &p
			app.onWrite = func(ir.Position) { runCancel() }
			b := newReplayBroker(store, &app.replayApplier, true)
			b.Target = replayTargetEngine{applier: app, rw: replayKeyWriter{keyed: true}}
			if err := b.Run(runCtx); err != nil {
				t.Fatalf("Run = %v; want the keyed incremental replayed", err)
			}
			if len(app.received) == 0 {
				t.Fatal("nothing reached the applier")
			}
			got := strings.Count(buf.String(), BrokerUnidentifiedChangesMarker)
			switch {
			case tc.wantWarn && (got != 1 || !strings.Contains(buf.String(), "changes=2")):
				t.Errorf("want one %s WARN counting 2 changes; got %d:\n%s", BrokerUnidentifiedChangesMarker, got, buf.String())
			case !tc.wantWarn && got != 0:
				t.Errorf("want no %s WARN; got %d:\n%s", BrokerUnidentifiedChangesMarker, got, buf.String())
			}
		})
	}
}
