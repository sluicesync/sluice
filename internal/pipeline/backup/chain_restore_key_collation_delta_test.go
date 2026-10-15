// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// shapeFailingEngine is a target whose schema writer implements
// [ir.ShapeDeltaApplier] and fails every AlterColumnType with err — the
// target's own DDL refusing a delta (MySQL's 1062 on a key loosened over two
// values the new collation equates).
type shapeFailingEngine struct {
	*schemaDeltaRecorderEngine
	err error
}

func (e *shapeFailingEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return &shapeFailingWriter{schemaDeltaRecorderWriter: &schemaDeltaRecorderWriter{owner: e.schemaDeltaRecorderEngine}, err: e.err}, nil
}

// shapeFailingWriter: only AlterColumnType is reachable from the deltas
// these tests build; the embedded nil interface panics on anything else.
type shapeFailingWriter struct {
	*schemaDeltaRecorderWriter
	ir.ShapeDeltaApplier
	err error
}

func (w *shapeFailingWriter) AlterAddColumn(ctx context.Context, t *ir.Table, cols []*ir.Column) error {
	return w.schemaDeltaRecorderWriter.AlterAddColumn(ctx, t, cols)
}

func (w *shapeFailingWriter) AlterColumnType(context.Context, *ir.Table, *ir.Column) error {
	return w.err
}

// keyDeltaLink is an incremental whose one delta changes column k of kd from
// before to after; keyed says whether k is kd's primary key.
func keyDeltaLink(before, after ir.Type, keyed bool) *lineage.SegmentRecord {
	mk := func(t ir.Type) *ir.Table {
		tbl := &ir.Table{Name: "kd", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "k", Type: t},
		}}
		key := "id"
		if keyed {
			key = "k"
		}
		tbl.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: key}}, Unique: true}
		return tbl
	}
	return &lineage.SegmentRecord{
		Segment: &lineage.Segment{Codec: blobcodec.CodecGzip},
		ManifestRecord: lineage.ManifestRecord{
			Path: "manifests/incr-0002.json",
			Manifest: &irbackup.Manifest{
				BackupID:     "incr-0002",
				SourceEngine: "mysql",
				Kind:         irbackup.BackupKindIncremental,
				SchemaDelta: []*irbackup.SchemaDeltaEntry{{
					Kind: irbackup.SchemaDeltaAlterTable, Table: "kd",
					Before: mk(before), After: mk(after),
				}},
			},
		},
	}
}

// TestApplySchemaDeltas_KeyCollationDeltaFailureIsNamed pins
// KEY-COLLATION-ROLLOVER-SPLIT-DELTA-DUP's cheap half: the target's failure
// to apply a delta that changes a key column's collation reaches the
// operator as KEY-COLLATION-MERGE with the window remedy and the target's
// own error kept, and every other failure keeps its own name.
func TestApplySchemaDeltas_KeyCollationDeltaFailureIsNamed(t *testing.T) {
	ctx := context.Background()
	dup := errors.New("Error 1062 (23000): Duplicate entry 'A' for key 'kd.PRIMARY'")
	bin := ir.Varchar{Length: 16, Charset: "utf8mb4", Collation: "utf8mb4_bin"}
	ci := ir.Varchar{Length: 16, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}

	run := func(t *testing.T, link *lineage.SegmentRecord, cmd RestoreCommand, fail error) error {
		t.Helper()
		tgt := &shapeFailingEngine{schemaDeltaRecorderEngine: newSchemaDeltaRecorderEngine("mysql"), err: fail}
		cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: &blobcodec.LocalStore{}, Command: cmd}
		return cr.applySchemaDeltas(ctx, link)
	}

	for _, tc := range []struct {
		name   string
		before ir.Type
		after  ir.Type
	}{
		{"loosened", bin, ci},
		{"tightened", ci, bin},
	} {
		t.Run(tc.name+"/restore", func(t *testing.T) {
			err := run(t, keyDeltaLink(tc.before, tc.after, true), RestoreCommandRestore, dup)
			if !errors.Is(err, ir.ErrKeyCollationMerge) {
				t.Fatalf("err = %v; want KEY-COLLATION-MERGE", err)
			}
			if !errors.Is(err, dup) {
				t.Errorf("err = %v; want the target's own error kept", err)
			}
			for _, want := range []string{keyCollationDeltaFailedMarker, "incr-0002", `"kd"`, "Take a fresh full backup after the key change"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("err = %v; want it to contain %q", err, want)
				}
			}
		})
	}

	t.Run("broker cold start names its own command", func(t *testing.T) {
		err := run(t, keyDeltaLink(bin, ci, true), RestoreCommandBrokerColdStart, dup)
		if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), "--reset-target-data --yes") {
			t.Fatalf("err = %v; want KEY-COLLATION-MERGE with the broker remedy", err)
		}
	})

	t.Run("a non-key retype keeps the target's error unnamed", func(t *testing.T) {
		// k is not a key: the collation change is no aspect at all, so a
		// width change is the delta, and its failure is not a key's.
		err := run(t, keyDeltaLink(ir.Varchar{Length: 16, Collation: "utf8mb4_bin"},
			ir.Varchar{Length: 32, Collation: "utf8mb4_0900_ai_ci"}, false), RestoreCommandRestore, dup)
		if err == nil || errors.Is(err, ir.ErrKeyCollationMerge) || strings.Contains(err.Error(), keyCollationDeltaFailedMarker) {
			t.Fatalf("err = %v; want the raw failure, not KEY-COLLATION-MERGE", err)
		}
	})

	t.Run("a cancellation keeps its name", func(t *testing.T) {
		err := run(t, keyDeltaLink(bin, ci, true), RestoreCommandRestore, context.Canceled)
		if !errors.Is(err, context.Canceled) || errors.Is(err, ir.ErrKeyCollationMerge) {
			t.Fatalf("err = %v; want the cancellation, not KEY-COLLATION-MERGE", err)
		}
	})
}
