// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Bug 297's read-side door, over hand-built lineages. The end-to-end pins
// (a real rotated chain, real PG and MySQL targets, zero rows written) are
// TestBug297_* in internal/pipeline; these grade the judgment itself: which
// fulls are judged, which tables in them, and with which predicate.

func r297Keyed(name string) *ir.Table {
	return &ir.Table{
		Name:       name,
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func r297Keyless(name string) *ir.Table {
	return &ir.Table{
		Name:    name,
		Columns: []*ir.Column{{Name: "a", Type: ir.Integer{Width: 64}, Nullable: true}},
	}
}

// r297PartialUnique is keyed only by a PARTIAL unique index — keyless to the
// F-E1 predicate (a replayed row outside the predicate collides with nothing).
// It is here to prove the door reuses that predicate rather than "has an
// index named unique".
func r297PartialUnique(name string) *ir.Table {
	return &ir.Table{
		Name:    name,
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		Indexes: []*ir.Index{{Name: name + "_u", Unique: true, Predicate: "id > 0", Columns: []ir.IndexColumn{{Column: "id"}}}},
	}
}

// r297Link builds one lineage link. rows lists the tables the manifest
// carries chunks for; every other schema table is recorded empty.
func r297Link(kind, id, dir string, tables []*ir.Table, rows ...string) lineage.SegmentRecord {
	withRows := map[string]bool{}
	for _, r := range rows {
		withRows[r] = true
	}
	m := &irbackup.Manifest{Kind: kind, BackupID: id, Schema: &ir.Schema{Tables: tables}}
	for _, t := range tables {
		tm := &irbackup.TableManifest{Name: t.Name}
		if withRows[t.Name] {
			tm.RowCount = 1
			tm.Chunks = []*irbackup.ChunkInfo{{File: t.Name + "-0", RowCount: 1}}
		}
		m.Tables = append(m.Tables, tm)
	}
	return lineage.SegmentRecord{
		ManifestRecord: lineage.ManifestRecord{Path: lineage.ManifestFileName, Manifest: m},
		Segment:        &lineage.Segment{Dir: dir, FullManifestPath: lineage.ManifestFileName},
	}
}

func r297Incr(id string) lineage.SegmentRecord {
	return lineage.SegmentRecord{
		ManifestRecord: lineage.ManifestRecord{Path: "manifests/" + id + ".json", Manifest: &irbackup.Manifest{Kind: irbackup.BackupKindIncremental, BackupID: id}},
	}
}

func TestIsDataOnlyFull_IsEveryFullAfterTheFirst(t *testing.T) {
	full := irbackup.BackupKindFull
	links := []lineage.SegmentRecord{
		r297Link(full, "f0", "", nil), r297Incr("i0"),
		r297Link(full, "f1", "seg-1", nil), r297Incr("i1"),
		r297Link("", "f2-legacy-kind", "seg-2", nil), // an empty Kind canonicalizes to full
	}
	want := []bool{false, false, true, false, true}
	for i := range links {
		if got := isDataOnlyFull(links, i); got != want[i] {
			t.Errorf("link %d (%s): isDataOnlyFull = %v; want %v", i, links[i].Manifest.Kind, got, want[i])
		}
	}
	// A pruned chain (its root segment retired) starts at a later
	// segment's full, which restore applies as the FIRST full — not DataOnly.
	pruned := []lineage.SegmentRecord{r297Link(full, "f3", "seg-3", nil), r297Incr("i3"), r297Link(full, "f4", "seg-4", nil)}
	if isDataOnlyFull(pruned, 0) {
		t.Error("a pruned chain's first full is judged DataOnly; restore applies it as the first segment")
	}
	if !isDataOnlyFull(pruned, 2) {
		t.Error("the full after a pruned chain's first is not judged DataOnly")
	}
}

func TestRefuseRotatedKeylessRecorded_Matrix(t *testing.T) {
	ctx := context.Background()
	full := irbackup.BackupKindFull
	kd, kl, pu := r297Keyed("kd"), r297Keyless("kl"), r297PartialUnique("pu")

	cases := []struct {
		name     string
		links    []lineage.SegmentRecord
		filter   migcore.TableFilter
		refuse   bool
		mentions []string
	}{
		{
			name: "keyless with rows in a later full refuses, naming table, full and directory",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{kd, kl}, "kd", "kl"), r297Incr("i0"),
				r297Link(full, "f1", "seg-0000000000001", []*ir.Table{kd, kl}, "kd", "kl"),
			},
			refuse:   true,
			mentions: []string{`"kl"`, "segment full f1", "seg-0000000000001", "no PRIMARY KEY", "Nothing has been written", "Bug 297"},
		},
		{
			name: "every later full carrying the table is named",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{kl}, "kl"),
				r297Link(full, "f1", "seg-1", []*ir.Table{kl}, "kl"),
				r297Link(full, "f2", "seg-2", []*ir.Table{kl}, "kl"),
			},
			refuse:   true,
			mentions: []string{"segment full f1 (directory seg-1)", "segment full f2 (directory seg-2)"},
		},
		{
			name:  "a single-segment chain is never judged (the first full is not re-applied)",
			links: []lineage.SegmentRecord{r297Link(full, "f0", "", []*ir.Table{kl}, "kl"), r297Incr("i0")},
		},
		{
			name: "a later full with NO rows for the keyless table re-writes nothing",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{kd, kl}, "kd", "kl"),
				r297Link(full, "f1", "seg-1", []*ir.Table{kd, kl}, "kd"),
			},
		},
		{
			name: "--exclude-table on the keyless table restores the rest",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{kd, kl}, "kd", "kl"),
				r297Link(full, "f1", "seg-1", []*ir.Table{kd, kl}, "kd", "kl"),
			},
			filter: migcore.TableFilter{Exclude: []string{"kl"}},
		},
		{
			name: "keyed tables only",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{kd}, "kd"),
				r297Link(full, "f1", "seg-1", []*ir.Table{kd}, "kd"),
			},
		},
		{
			name: "a partial UNIQUE is not a key (the F-E1 predicate, reused)",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{pu}, "pu"),
				r297Link(full, "f1", "seg-1", []*ir.Table{pu}, "pu"),
			},
			refuse:   true,
			mentions: []string{`"pu"`},
		},
		{
			name: "a table that LOST its key by the later full is judged on that full's definition",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{r297Keyed("t")}, "t"),
				r297Link(full, "f1", "seg-1", []*ir.Table{r297Keyless("t")}, "t"),
			},
			refuse:   true,
			mentions: []string{`"t"`, "segment full f1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := refuseRotatedKeylessRecorded(ctx, tc.links, tc.filter, "chain restore")
			if !tc.refuse {
				if err != nil {
					t.Fatalf("refused a restorable chain: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("restorable verdict for a chain whose later full re-writes a keyless table")
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeBackupRotatedKeylessTable {
				t.Fatalf("refusal is not coded %s: %v", sluicecode.CodeBackupRotatedKeylessTable, err)
			}
			for _, m := range tc.mentions {
				if !strings.Contains(err.Error(), m) {
					t.Errorf("refusal does not mention %q:\n%v", m, err)
				}
			}
			if !strings.Contains(ce.Hint, "--exclude-table") {
				t.Errorf("hint does not name the --exclude-table remedy: %q", ce.Hint)
			}
		})
	}
}

// rotatedProbeEngine is a target whose row writer answers the F-E1 target
// probe as configured, and counts how many writers the door opened.
type rotatedProbeEngine struct {
	*restoreRecorderEngine
	exists, keyed bool
	opens         int
}

func (e *rotatedProbeEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	e.opens++
	return &rotatedProbeWriter{restoreRecordingRowWriter: restoreRecordingRowWriter{engine: e.restoreRecorderEngine}, e: e}, nil
}

type rotatedProbeWriter struct {
	restoreRecordingRowWriter
	e *rotatedProbeEngine
}

func (w *rotatedProbeWriter) ProbeReplayKey(context.Context, *ir.Table) (exists, keyed bool, err error) {
	return w.e.exists, w.e.keyed, nil
}

// TestRefuseRotatedKeylessTarget_SurrogateKeyedTarget is the silent arm: the
// recorded table is keyed, so the recorded half passes; the TARGET table the
// later full would be re-written into is keyed only on a column the rows do
// not supply, so every re-written row would land again.
func TestRefuseRotatedKeylessTarget_SurrogateKeyedTarget(t *testing.T) {
	ctx := context.Background()
	full := irbackup.BackupKindFull
	rotated := []lineage.SegmentRecord{
		r297Link(full, "f0", "", []*ir.Table{r297Keyed("kd")}, "kd"),
		r297Link(full, "f1", "seg-1", []*ir.Table{r297Keyed("kd")}, "kd"),
	}

	surrogate := &rotatedProbeEngine{restoreRecorderEngine: newRestoreRecorderEngine("mysql"), exists: true, keyed: false}
	err := (&ChainRestore{Target: surrogate, TargetDSN: "t"}).refuseRotatedKeylessTarget(ctx, rotated)
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupRotatedKeylessTable {
		t.Fatalf("surrogate-keyed target: err = %v; want %s", err, sluicecode.CodeBackupRotatedKeylessTable)
	}
	if !strings.Contains(err.Error(), "target table") {
		t.Errorf("refusal does not say the TARGET judgment failed:\n%v", err)
	}

	// The target the restore will create from the recorded schema passes.
	absent := &rotatedProbeEngine{restoreRecorderEngine: newRestoreRecorderEngine("mysql"), exists: false}
	if err := (&ChainRestore{Target: absent, TargetDSN: "t"}).refuseRotatedKeylessTarget(ctx, rotated); err != nil {
		t.Errorf("absent target table (created from the recorded key): %v", err)
	}
	keyed := &rotatedProbeEngine{restoreRecorderEngine: newRestoreRecorderEngine("mysql"), exists: true, keyed: true}
	if err := (&ChainRestore{Target: keyed, TargetDSN: "t"}).refuseRotatedKeylessTarget(ctx, rotated); err != nil {
		t.Errorf("target carrying a supplied key: %v", err)
	}

	// A chain that never rotated pays nothing: no writer is opened.
	single := &rotatedProbeEngine{restoreRecorderEngine: newRestoreRecorderEngine("mysql"), exists: true, keyed: false}
	if err := (&ChainRestore{Target: single, TargetDSN: "t"}).refuseRotatedKeylessTarget(ctx, rotated[:1]); err != nil {
		t.Errorf("single-segment chain: %v", err)
	}
	if single.opens != 0 {
		t.Errorf("single-segment chain opened %d target writer(s); want 0", single.opens)
	}
}

// TestVerifyBackupScanRunsTheRotatedKeylessDoor holds verify's half of the
// wiring: restore refuses a rotated chain that re-writes a keyless table, so
// verify must not report it healthy (the Bug 217/218 doctrine).
func TestVerifyBackupScanRunsTheRotatedKeylessDoor(t *testing.T) {
	if !verifyScanReaches(t, "refuseRotatedKeylessRecorded") {
		t.Fatal("verifyBackupScan no longer reaches refuseRotatedKeylessRecorded: `backup verify` would report healthy " +
			"a rotated chain restore refuses with SLUICE-E-BACKUP-ROTATED-KEYLESS-TABLE")
	}
}
