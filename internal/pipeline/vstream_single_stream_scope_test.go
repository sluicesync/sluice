// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// singleStreamDSN carries the vstream_copy_single_stream opt-out.
const singleStreamDSN = "u:p@tcp(127.0.0.1:1)/ks?vstream_copy_single_stream=true"

// singleStreamSchema is a source schema of n tables.
func singleStreamSchema(n int) *ir.Schema {
	s := &ir.Schema{}
	for i := range n {
		s.Tables = append(s.Tables, &ir.Table{
			Name:    fmt.Sprintf("t%d", i),
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		})
	}
	return s
}

// singleStreamSource reads a fixed schema and answers the snapshot-scope
// question with the REAL VStream check (mysql.Engine.CheckSnapshotScope),
// recording each scope it was asked about. Its table-scoped snapshot open
// is the real engine's too, so a cold start that skipped the early check
// meets the open's own door, after the apply-mark clear; every other open
// panics (stubEngine).
type singleStreamSource struct {
	stubEngine
	schema *ir.Schema
	asked  [][]string
	opened int
}

func (s *singleStreamSource) OpenSnapshotStreamForTables(ctx context.Context, dsn string, tables []string) (*ir.SnapshotStream, error) {
	s.opened++
	return mysql.Engine{Flavor: mysql.FlavorPlanetScale}.OpenSnapshotStreamForTables(ctx, dsn, tables)
}

func (s *singleStreamSource) Name() string { return "planetscale" }

func (s *singleStreamSource) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return &fixedSchemaReader{schema: s.schema}, nil
}

func (s *singleStreamSource) CheckSnapshotScope(dsn string, tables []string) error {
	s.asked = append(s.asked, slices.Clone(tables))
	return mysql.Engine{Flavor: mysql.FlavorPlanetScale}.CheckSnapshotScope(dsn, tables)
}

var _ ir.SnapshotScopeChecker = (*singleStreamSource)(nil)

// marksRecordingApplier records ClearApplyMarks calls.
type marksRecordingApplier struct {
	stubChangeApplier
	cleared int
}

func (a *marksRecordingApplier) ClearApplyMarks(context.Context, string) error {
	a.cleared++
	return nil
}

// TestColdStart_SingleStreamMultiTable_RefusesBeforeAnyWrite pins where the
// cold start refuses VSTREAM-SINGLE-STREAM-MULTI-TABLE: after the source
// read, before the stream's apply marks are cleared and before anything
// opens on the target (the target is a stubEngine, which panics on any
// open), for each cold-start reason that reaches coldStart (a first cold
// start, --restart-from-scratch, the invalid-position re-snapshot). The
// refusal carries the sentinel the fleet supervisor and runWithRetry key on.
func TestColdStart_SingleStreamMultiTable_RefusesBeforeAnyWrite(t *testing.T) {
	captureSlog(t)
	for _, n := range []int{2, 9} {
		for _, fresh := range []freshCopyReason{freshCopyNone, freshCopyOperatorRestart, freshCopyAutoResnapshot} {
			t.Run(fmt.Sprintf("tables=%d/fresh=%d", n, fresh), func(t *testing.T) {
				src := &singleStreamSource{schema: singleStreamSchema(n)}
				app := &marksRecordingApplier{}
				s := &Streamer{Source: src, Target: stubEngine{}, SourceDSN: singleStreamDSN}
				_, _, err := s.coldStart(context.Background(), app, "st", fresh)
				if !errors.Is(err, ir.ErrVStreamSingleStreamMultiTable) {
					t.Fatalf("coldStart = %v; want the VSTREAM-SINGLE-STREAM-MULTI-TABLE refusal", err)
				}
				if app.cleared != 0 || src.opened != 0 {
					t.Errorf("the refused cold start cleared the apply marks %d time(s) and opened the snapshot %d time(s); "+
						"want neither (a warm resume after the refusal needs the marks)", app.cleared, src.opened)
				}
				if len(src.asked) != 1 || len(src.asked[0]) != n {
					t.Errorf("the check was asked about %v; want once, about the %d in-scope tables", src.asked, n)
				}
			})
		}
	}
}

// TestColdStartReadSourceSchema_SingleStreamOneTable_Passes is the other
// direction: a one-table scope keeps the parameter, and the check is asked
// about exactly that table.
func TestColdStartReadSourceSchema_SingleStreamOneTable_Passes(t *testing.T) {
	captureSlog(t)
	src := &singleStreamSource{schema: singleStreamSchema(1)}
	s := &Streamer{Source: src, Target: stubEngine{}, SourceDSN: singleStreamDSN}
	_, tables, err := s.coldStartReadSourceSchema(context.Background(), false)
	if err != nil {
		t.Fatalf("coldStartReadSourceSchema = %v; a one-table single-stream scope must pass", err)
	}
	if want := []string{"t0"}; !slices.Equal(tables, want) || len(src.asked) != 1 || !slices.Equal(src.asked[0], want) {
		t.Fatalf("tables = %v, asked = %v; want both %v", tables, src.asked, want)
	}
}

// TestDryRun_SingleStreamMultiTable_Refuses pins that a `sync start
// --dry-run` plan refuses the scope the cold start refuses, and plans a
// one-table scope.
func TestDryRun_SingleStreamMultiTable_Refuses(t *testing.T) {
	captureSlog(t)
	for _, n := range []int{1, 2, 9} {
		t.Run(fmt.Sprintf("tables=%d", n), func(t *testing.T) {
			s := &Streamer{Source: &singleStreamSource{schema: singleStreamSchema(n)}, Target: stubEngine{}, SourceDSN: singleStreamDSN}
			plan, err := s.buildDryRunPlan(context.Background(), "st", ir.Position{}, false)
			if n == 1 {
				if err != nil || plan == nil || len(plan.Tables) != 1 {
					t.Fatalf("plan = %+v, err = %v; want a one-table plan", plan, err)
				}
				return
			}
			if !errors.Is(err, ir.ErrVStreamSingleStreamMultiTable) {
				t.Fatalf("buildDryRunPlan = %v; want the VSTREAM-SINGLE-STREAM-MULTI-TABLE refusal", err)
			}
		})
	}
}

// singleStreamBackupSource is a backup source with two tables, the real
// VStream scope check, and a table-scoped snapshot opener that records a
// call (the refusal must come before it, because an open error falls back
// to the non-snapshot copy instead of failing the backup).
type singleStreamBackupSource struct {
	*backupRecorderEngine
	scope        singleStreamSource
	opened       int
	rowReaderOps int
}

func (e *singleStreamBackupSource) CheckSnapshotScope(dsn string, tables []string) error {
	return e.scope.CheckSnapshotScope(dsn, tables)
}

func (e *singleStreamBackupSource) OpenBackupSnapshotForTables(context.Context, string, irbackup.SnapshotOptions, []string) (*irbackup.Snapshot, error) {
	e.opened++
	return nil, errors.New("singleStreamBackupSource: the snapshot must not be opened")
}

func (e *singleStreamBackupSource) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	e.rowReaderOps++
	return e.backupRecorderEngine.OpenRowReader(ctx, dsn)
}

// TestBackup_SingleStreamMultiTable_RefusesBeforeTheSnapshot pins the
// `backup full` door: refused before the snapshot opens (so the
// non-snapshot fallback never runs) and before any manifest is written.
func TestBackup_SingleStreamMultiTable_RefusesBeforeTheSnapshot(t *testing.T) {
	captureSlog(t)
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := singleStreamSchema(2)
	src := &singleStreamBackupSource{backupRecorderEngine: newBackupRecorderEngine("planetscale", schema, nil)}
	b := &backup.Backup{
		Source: src, SourceDSN: singleStreamDSN, Store: store,
		SluiceVersion: "test", Now: func() time.Time { return time.Unix(0, 0).UTC() },
	}
	if err := b.Run(context.Background()); !errors.Is(err, ir.ErrVStreamSingleStreamMultiTable) {
		t.Fatalf("Backup.Run = %v; want the VSTREAM-SINGLE-STREAM-MULTI-TABLE refusal", err)
	}
	if src.opened != 0 || src.rowReaderOps != 0 {
		t.Errorf("the refused backup opened the snapshot %d time(s) and a row reader %d time(s); want neither", src.opened, src.rowReaderOps)
	}
	if _, err := lineage.ReadManifest(context.Background(), store); err == nil {
		t.Error("the refused backup wrote a manifest")
	}
}
