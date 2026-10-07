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

// The Bug 297 review's HIGH, over hand-built lineages: the later full is
// written into the table the chain's OWN replay left on the target (segment
// 0's full or an AddTable delta, as altered by the recorded deltas), never
// into the shape the later full records. The end-to-end pin is
// TestBug297_KeyAddedInRotationGap_MySQL.

func r297IncrDelta(id string, deltas ...*irbackup.SchemaDeltaEntry) lineage.SegmentRecord {
	l := r297Incr(id)
	l.Manifest.SchemaDelta = deltas
	return l
}

func r297Delta(kind string, before, after *ir.Table) *irbackup.SchemaDeltaEntry {
	name := ""
	if after != nil {
		name = after.Name
	} else if before != nil {
		name = before.Name
	}
	return &irbackup.SchemaDeltaEntry{Kind: kind, Table: name, Before: before, After: after}
}

// r297NotNullKeyless is kl with a NOT NULL a, so it can be keyed later.
func r297NotNullKeyless(name string) *ir.Table {
	return &ir.Table{Name: name, Columns: []*ir.Column{{Name: "a", Type: ir.Integer{Width: 64}}}}
}

func r297PKOn(t *ir.Table) *ir.Table {
	c := *t
	c.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "a"}}}
	return &c
}

func r297UniqueOn(t *ir.Table) *ir.Table {
	c := *t
	c.Indexes = []*ir.Index{{Name: t.Name + "_a", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}}}}
	return &c
}

func TestRefuseRotatedKeylessRecorded_JudgesTheTableTheChainCreates(t *testing.T) {
	ctx := context.Background()
	full := irbackup.BackupKindFull
	kd := r297Keyed("kd")
	kl := r297NotNullKeyless("kl")
	klPK, klUnique := r297PKOn(kl), r297UniqueOn(kl)

	cases := []struct {
		name   string
		links  []lineage.SegmentRecord
		refuse bool
	}{
		{
			// The review's gap shape: no delta records the key; the later full
			// records it. The target's kl is segment 0's, keyless.
			name: "segment 0 keyless, later full records a key no delta carries",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{kd, kl}, "kd", "kl"),
				r297Incr("i0"),
				r297Link(full, "f1", "seg-1", []*ir.Table{kd, klPK}, "kd", "kl"),
			},
			refuse: true,
		},
		{
			name: "created keyless by an AddTable delta, later full records a key",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{kd}, "kd"),
				r297IncrDelta("i0", r297Delta(irbackup.SchemaDeltaAddTable, nil, kl)),
				r297Link(full, "f1", "seg-1", []*ir.Table{kd, klPK}, "kd", "kl"),
			},
			refuse: true,
		},
		{
			// Drop + re-create keyed: DropTable is not replayed and the
			// AddTable's CREATE IF NOT EXISTS is a no-op over the old table.
			name: "dropped and re-created keyed in separate windows",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{kd, kl}, "kd", "kl"),
				r297IncrDelta("i0", r297Delta(irbackup.SchemaDeltaDropTable, kl, nil)),
				r297IncrDelta("i1", r297Delta(irbackup.SchemaDeltaAddTable, nil, klPK)),
				r297Link(full, "f1", "seg-1", []*ir.Table{kd, klPK}, "kd", "kl"),
			},
			refuse: true,
		},
		{
			// The remedy that works mid-chain: a NOT NULL UNIQUE index is an
			// index-created delta the replay applies.
			name: "keyed mid-chain by a recorded NOT NULL UNIQUE index delta",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{kd, kl}, "kd", "kl"),
				r297IncrDelta("i0", r297Delta(irbackup.SchemaDeltaAlterTable, kl, klUnique)),
				r297Link(full, "f1", "seg-1", []*ir.Table{kd, klUnique}, "kd", "kl"),
			},
		},
		{
			name: "created keyed by an AddTable delta",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{kd}, "kd"),
				r297IncrDelta("i0", r297Delta(irbackup.SchemaDeltaAddTable, nil, klUnique)),
				r297Link(full, "f1", "seg-1", []*ir.Table{kd, klUnique}, "kd", "kl"),
			},
		},
		{
			// Never created by the chain: the projection has no opinion; the
			// live judgments read whatever the target holds.
			name: "a later full's table the chain never created",
			links: []lineage.SegmentRecord{
				r297Link(full, "f0", "", []*ir.Table{kd}, "kd"),
				r297Link(full, "f1", "seg-1", []*ir.Table{kd, klPK}, "kd", "kl"),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := refuseRotatedKeylessRecorded(ctx, tc.links, migcore.TableFilter{}, "chain restore")
			if !tc.refuse {
				if err != nil {
					t.Fatalf("refused a restorable chain: %v", err)
				}
				return
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeBackupRotatedKeylessTable {
				t.Fatalf("err = %v; want %s", err, sluicecode.CodeBackupRotatedKeylessTable)
			}
			if !strings.Contains(err.Error(), `"kl"`) || !strings.Contains(err.Error(), "this chain's own replay") {
				t.Errorf("refusal does not name kl and the chain-created judgment:\n%v", err)
			}
		})
	}
}

func TestRefuseUnreplayableDeltas(t *testing.T) {
	full := irbackup.BackupKindFull
	kl := r297NotNullKeyless("kl")
	pk := []lineage.SegmentRecord{
		r297Link(full, "f0", "", []*ir.Table{kl}, "kl"),
		r297IncrDelta("i0", r297Delta(irbackup.SchemaDeltaAlterTable, kl, r297PKOn(kl))),
	}
	err := refuseUnreplayableDeltas(pk, migcore.TableFilter{}, "chain restore")
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupSchemaDeltaUnsupported {
		t.Fatalf("ADD PRIMARY KEY delta: err = %v; want %s", err, sluicecode.CodeBackupSchemaDeltaUnsupported)
	}
	for _, m := range []string{"primary-key", `"kl"`, "before writing anything", "i0"} {
		if !strings.Contains(err.Error(), m) {
			t.Errorf("refusal does not mention %q:\n%v", m, err)
		}
	}
	// Filter-aware, as the replay is (Bug 244).
	if err := refuseUnreplayableDeltas(pk, migcore.TableFilter{Exclude: []string{"kl"}}, "chain restore"); err != nil {
		t.Errorf("--exclude-table=kl still refused: %v", err)
	}
	// A delta the replay applies is not refused.
	unique := []lineage.SegmentRecord{
		r297Link(full, "f0", "", []*ir.Table{kl}, "kl"),
		r297IncrDelta("i0", r297Delta(irbackup.SchemaDeltaAlterTable, kl, r297UniqueOn(kl))),
	}
	if err := refuseUnreplayableDeltas(unique, migcore.TableFilter{}, "chain restore"); err != nil {
		t.Errorf("NOT NULL UNIQUE index delta refused: %v", err)
	}
}

// TestVerifyBackupScanRunsTheDeltaPreflight: verify predicts the up-front
// delta refusal restore now makes.
func TestVerifyBackupScanRunsTheDeltaPreflight(t *testing.T) {
	if !verifyScanReaches(t, "refuseUnreplayableDeltas") {
		t.Fatal("verifyBackupScan no longer reaches refuseUnreplayableDeltas: `backup verify` would report healthy " +
			"a chain restore refuses with SLUICE-E-BACKUP-SCHEMA-DELTA-UNSUPPORTED")
	}
}

// TestApplyFullProbesTheTargetBeforeEachDataOnlyFull holds judgment 3's
// wiring: the live-target belt runs inside applyFull, for DataOnly fulls.
func TestApplyFullProbesTheTargetBeforeEachDataOnlyFull(t *testing.T) {
	body := funcCallsIn(t, "chain_restore.go", "applyFull")
	if !body["refuseDataOnlyFullOnTarget"] {
		t.Fatal("ChainRestore.applyFull no longer calls refuseDataOnlyFullOnTarget: a later full is written into the target " +
			"without the live key check that backstops the chain projection")
	}
}
