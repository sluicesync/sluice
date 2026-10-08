// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// doorRows is the incremental each door cell captures: one transaction on the
// keyless table kl (identified per idKL) and/or one on the keyed table k.
func doorRows(touchKL, touchK, idKL, zeroOne bool) []ir.Change {
	pos := func(lsn string) ir.Position {
		return ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`}
	}
	var out []ir.Change
	if touchKL {
		id := func(seq uint64) ir.ApplyID {
			if !idKL || (zeroOne && seq == 2) {
				return ir.ApplyID{}
			}
			return ir.ApplyID{TxID: "pg:1:1:0/120", Seq: seq}
		}
		out = append(
			out,
			ir.TxBegin{Position: pos("0/110")},
			ir.Insert{Position: pos("0/120"), Table: "kl", Row: ir.Row{"v": int64(3)}, ApplyID: id(1)},
			ir.Insert{Position: pos("0/120"), Table: "kl", Row: ir.Row{"v": int64(4)}, ApplyID: id(2)},
			ir.TxCommit{Position: pos("0/130")},
		)
	}
	if touchK {
		out = append(
			out,
			ir.TxBegin{Position: pos("0/140")},
			ir.Insert{Position: pos("0/150"), Table: "k", Row: ir.Row{"id": int64(9)}, ApplyID: ir.ApplyID{TxID: "pg:1:1:0/150", Seq: 1}},
			ir.TxCommit{Position: pos("0/160")},
		)
	}
	return out
}

// doorFixture is a plaintext full + one incremental over the keyless kl and
// the keyed k; stamps decides whether the capture's reader declares
// identities (the manifest's ApplyIdentity).
func doorFixture(t *testing.T, changes []ir.Change, stamps bool) (store irbackup.Store, fullID string) {
	t.Helper()
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	kl := &ir.Table{Name: "kl", Columns: []*ir.Column{{Name: "v", Type: ir.Integer{Width: 64}}}}
	k := &ir.Table{
		Name: "k", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	schema := &ir.Schema{Tables: []*ir.Table{kl, k}}
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{"kl": {{"v": int64(1)}}, "k": {{"id": int64(1)}}})
	if err := (&backup.Backup{Source: src, SourceDSN: "src", Store: store}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	full.EndPosition = ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatal(err)
	}
	cdc := &fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{schema, schema}, cdcChanges: changes, stampsIdentity: stamps}
	if err := (&IncrementalBackup{Source: cdc, SourceDSN: "src", Store: store, ParentRef: full.BackupID, Window: time.Minute, ChunkChanges: 2}).Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	return store, full.BackupID
}

// coveringApplier is a replayApplier that answers ir.ApplyMarksCoverageProber
// with a fixed reason ("" = the marks cover every table).
type coveringApplier struct {
	replayApplier
	reason string

	// required is the last RequireApplyMarks value; marksLostAtApply makes
	// the apply find the mark table unusable as it starts, as a real
	// applier's startApplyMarks would after the door's judgment.
	required         bool
	marksLostAtApply bool
}

func (a *coveringApplier) MarksCoverReason(context.Context, *ir.Table) (string, error) {
	return a.reason, nil
}

func (a *coveringApplier) RequireApplyMarks(on bool) { a.required = on }

func (a *coveringApplier) ApplyBatch(ctx context.Context, s string, ch <-chan ir.Change, n int) error {
	if a.marksLostAtApply {
		if a.required {
			return fmt.Errorf("fake: %w: dropped after the door", applymarks.ErrMarksRequired)
		}
		// the WARN path: apply anyway, without marks
	}
	return a.replayApplier.ApplyBatch(ctx, s, ch, n)
}

// proberOnlyApplier answers the coverage question ("covered") but cannot be
// held to its marks: no ir.ApplyMarksRequirer.
type proberOnlyApplier struct {
	*replayApplier
}

func (a *proberOnlyApplier) MarksCoverReason(context.Context, *ir.Table) (string, error) {
	return "", nil
}

// TestBrokerKeylessDoor_PerIncrementalMatrix is ADR-0191 §9 P12: every row of
// §3.5's table, judged on a warm resume BEFORE anything of the incremental is
// applied. The keyless table kl is replayed only when the incremental records
// identities, every change to kl in it carries one, and the target's marks
// cover kl; each other row refuses SLUICE-E-BROKER-KEYLESS-TABLE naming kl,
// with nothing applied and no position written. A keyless table the
// incremental does not touch is not its concern (the narrowing). The rows
// that turn on the TARGET (marks unavailable, a vtgate sidecar, Neki, a key on
// an unsupplied surrogate) are graded here as reasons the applier reports;
// each engine's reason for its own shapes is pinned against a real server by
// TestChangeApplier_MarksCoverReason (postgres, mysql).
//
// The §3.5 row "--at-chain-id whose asserted incremental ends severed" is not
// built (ADR-0191 §13 R1): the severed-transaction door refuses that chain first.
func TestBrokerKeylessDoor_PerIncrementalMatrix(t *testing.T) {
	cases := []struct {
		name                         string
		touchKL, touchK, stamps, idK bool
		zeroOne                      bool
		reason                       string // the target's coverage answer
		noProber, proberOnly         bool
		marksLostAtApply             bool
		wantRefused                  bool
		wantInMsg                    string
	}{
		{name: "identities, marks cover: replayed", touchKL: true, stamps: true, idK: true},
		{name: "incremental without identities", touchKL: true, stamps: false, wantRefused: true, wantInMsg: "records no change identities"},
		{name: "a change without an identity", touchKL: true, stamps: true, idK: true, zeroOne: true, wantRefused: true, wantInMsg: BrokerKeylessNoIdentityMarker},
		{name: "marks unavailable", touchKL: true, stamps: true, idK: true, reason: "APPLY-MARKS-UNAVAILABLE: no table", wantRefused: true, wantInMsg: "APPLY-MARKS-UNAVAILABLE"},
		{name: "control-keyspace sidecar (an engine reason: sync from-backup has no such flag)", touchKL: true, stamps: true, idK: true, reason: "the target keeps its control tables in the `--control-keyspace` sidecar", wantRefused: true, wantInMsg: "--control-keyspace"},
		{name: "Neki", touchKL: true, stamps: true, idK: true, reason: "the target is PlanetScale Neki", wantRefused: true, wantInMsg: "Neki"},
		{name: "target key on an unsupplied surrogate", touchKL: true, stamps: true, idK: true, reason: `its target primary key includes "sid", which the replayed rows do not carry`, wantRefused: true, wantInMsg: `"sid"`},
		{name: "applier without the coverage surface", touchKL: true, stamps: true, idK: true, noProber: true, wantRefused: true, wantInMsg: "cannot report whether apply marks cover it"},
		{name: "marks unusable by the time the apply starts", touchKL: true, stamps: true, idK: true, marksLostAtApply: true, wantRefused: true, wantInMsg: "unusable when its apply started"},
		{name: "applier that cannot be held to its marks", touchKL: true, stamps: true, idK: true, proberOnly: true, wantRefused: true, wantInMsg: "cannot be held to its apply marks"},
		{name: "keyless table untouched by the incremental", touchK: true, stamps: false},
		{name: "nothing lifted: unusable marks at apply stay a WARN", touchK: true, stamps: true, marksLostAtApply: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, fullID := doorFixture(t, doorRows(tc.touchKL, tc.touchK, tc.idK, tc.zeroOne), tc.stamps)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			runCtx, runCancel := context.WithCancel(ctx)
			defer runCancel()
			app := &coveringApplier{reason: tc.reason, marksLostAtApply: tc.marksLostAtApply}
			p := encodeBrokerPosition("test://fe1", fullID)
			app.resume = &p
			app.onWrite = func(ir.Position) { runCancel() } // the incremental fully applied: stop
			var applier ir.ChangeApplier = app
			switch {
			case tc.noProber:
				applier = &app.replayApplier
			case tc.proberOnly:
				applier = &proberOnlyApplier{replayApplier: &app.replayApplier}
			}
			b := newReplayBroker(store, &app.replayApplier, true)
			b.Target = replayTargetEngine{applier: applier, rw: replayKeyWriter{keyed: true}}
			err := b.Run(runCtx)
			if !tc.wantRefused {
				if err != nil {
					t.Fatalf("Run = %v; want the incremental replayed", err)
				}
				if len(app.received) == 0 {
					t.Fatal("nothing reached the applier: the incremental was not replayed")
				}
				return
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeBrokerKeylessTable {
				t.Fatalf("Run = %v; want %s", err, sluicecode.CodeBrokerKeylessTable)
			}
			if !strings.Contains(err.Error(), `"kl"`) || !strings.Contains(err.Error(), tc.wantInMsg) {
				t.Errorf("the refusal does not name kl with %q: %v", tc.wantInMsg, err)
			}
			if strings.Contains(err.Error(), `"k" (`) {
				t.Errorf("the refusal names the keyed table k: %v", err)
			}
			if len(app.received) != 0 || len(app.written) != 0 {
				t.Errorf("the refused incremental reached the target: %d changes, %d position writes", len(app.received), len(app.written))
			}
		})
	}
}

// resetDoorWriter counts the drops a --reset-target-data cold start makes —
// the destructive step the reset door must run before.
type resetDoorWriter struct {
	replayKeyWriter
	dropped *int
}

func (w resetDoorWriter) DropTable(context.Context, *ir.Table) error { *w.dropped++; return nil }

func (resetDoorWriter) IsTableEmpty(context.Context, *ir.Table) (bool, error) { return true, nil }

// TestBrokerKeylessDoor_ResetJudgesTheTargetBeforeTheDrop pins the
// --reset-target-data half of the door (ADR-0191 §3.5): the target is about
// to be rebuilt, so what can be known is whether it could EVER make a keyless
// replay exactly-once (its marks usable, its commit atomic). A target that
// cannot refuses while it still holds its data — before the drop; one that
// can proceeds to the drop, and each later incremental is judged by the tick.
func TestBrokerKeylessDoor_ResetJudgesTheTargetBeforeTheDrop(t *testing.T) {
	for _, tc := range []struct {
		reason      string
		wantRefused bool
	}{
		{reason: "APPLY-MARKS-UNAVAILABLE: no table", wantRefused: true},
		{reason: ""},
	} {
		t.Run("reason="+tc.reason, func(t *testing.T) {
			store, _ := doorFixture(t, doorRows(true, false, true, false), true)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			app := &coveringApplier{reason: tc.reason}
			dropped := 0
			b := newReplayBroker(store, &app.replayApplier, true)
			b.Target = replayTargetEngine{applier: app, rw: resetDoorWriter{replayKeyWriter: replayKeyWriter{keyed: true}, dropped: &dropped}}
			b.ResetTargetData = true
			err := b.Run(ctx)
			if !tc.wantRefused {
				if dropped == 0 {
					t.Fatalf("a target whose marks cover keyless replay was refused before the drop: %v", err)
				}
				return
			}
			if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBrokerKeylessTable || !strings.Contains(err.Error(), `"kl"`) {
				t.Fatalf("Run = %v; want %s naming kl", err, sluicecode.CodeBrokerKeylessTable)
			}
			if dropped != 0 {
				t.Errorf("the reset dropped %d table(s) before refusing", dropped)
			}
		})
	}
}
