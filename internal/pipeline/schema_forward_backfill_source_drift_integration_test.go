//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// The added-column backfill reads the SOURCE as it is when the backfill
// runs, which can be later than the boundary it serves: the stream is
// behind, and the source has moved on. These pins decide what happens when
// the source moved on in a way that touches the columns the backfill names,
// on real servers, by calling the backfill directly — deterministic, where
// arranging the race through a live stream would not be.
//
//   - A LATER change to any OTHER column cannot reach the read at all: it
//     selects only the key and the added columns (gap 35). The CI failure
//     that motivated this file was exactly that — a later DROP of an
//     unrelated column failed a whole-row SELECT with "Unknown column" and
//     refused the boundary (TestStreamer_SchemaForward_DropColumn_Cross_*).
//   - (a) The ADDED column is gone from the source by the time it is read:
//     dropped, or renamed. The two cannot be told apart without looking
//     ahead in the stream, and a rename is the dangerous one — the renamed
//     column's pre-existing target rows would keep the target's fill,
//     silently. So it REFUSES, on every engine; the read's own error reaches
//     the caller, which ends the attempt as ADD-COLUMN-BACKFILL-INCOMPLETE.
//     (Postgres could prove a drop from the column's attnum; not built.)
//   - (b) A PRIMARY KEY column renamed or dropped later on the source: the
//     key comes from the source catalog, so it names a column the stream's
//     projection does not carry, or there is no key — both refuse.

// driftCase is one source engine the drift pins run against.
type driftCase struct {
	name   string
	engine string
	schema string // the IR schema name the engine's tables carry
	start  func(t *testing.T) (dsn string, cleanup func())
	exec   func(t *testing.T, dsn, stmt string)
}

func driftCases() []driftCase {
	return []driftCase{
		{
			name: "postgres", engine: "postgres", schema: "public",
			start: func(t *testing.T) (string, func()) {
				src, _, cleanup := startPostgresLogical(t)
				return src, cleanup
			},
			exec: func(t *testing.T, dsn, stmt string) { fdExec(t, fdPG, dsn, stmt) },
		},
		{
			name: "mysql", engine: "mysql", schema: "",
			start: func(t *testing.T) (string, func()) {
				src, _, cleanup := startMySQLBinlog(t)
				return src, cleanup
			},
			exec: func(t *testing.T, dsn, stmt string) { fdExec(t, fdMySQL, dsn, stmt) },
		},
	}
}

// driftBackfill runs the backfill over table, whose shape is what the
// stream's boundary carried (not what the source holds now).
func driftBackfill(t *testing.T, dc driftCase, dsn string, table *ir.Table, added []*ir.Column) error {
	t.Helper()
	eng, ok := engines.Get(dc.engine)
	if !ok {
		t.Fatalf("%s engine not registered", dc.engine)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("open schema reader: %v", err)
	}
	defer func() { _ = closeIfErrIgnored(sr) }()
	s := &Streamer{Source: eng, SourceDSN: dsn}
	lazy := &lazyBackfillReader{open: s.openAddedColumnBackfillReader}
	defer lazy.Close()
	bf := &schemaForwardBackfill{
		reader:     lazy.reader,
		streamID:   "drift",
		batchSize:  100,
		primaryKey: sourcePrimaryKeyResolver(sr),
	}
	snap := ir.SchemaSnapshot{Position: ir.Position{Engine: dc.engine, Token: "t"}, Schema: table.Schema, Table: table.Name, IR: table}
	out := make(chan ir.Change, 1024)
	return runBackfillForAddedColumn(ctx, bf, snap, added, out)
}

func driftTable(schema string, cols ...string) *ir.Table {
	tbl := &ir.Table{Schema: schema, Name: "w"}
	for _, c := range cols {
		tbl.Columns = append(tbl.Columns, &ir.Column{Name: c, Type: ir.Text{}, Nullable: true})
	}
	tbl.Columns[0].Type = ir.Integer{Width: 64}
	tbl.Columns[0].Nullable = false
	tbl.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: cols[0]}}}
	return tbl
}

// TestBackfillSourceDrift_AnotherColumnDroppedLaterDoesNotReachTheRead is
// the CI failure's shape, decided: the boundary carried `doomed`, the source
// has since dropped it, and the backfill still fills the added column.
func TestBackfillSourceDrift_AnotherColumnDroppedLaterDoesNotReachTheRead(t *testing.T) {
	for _, dc := range driftCases() {
		t.Run(dc.name, func(t *testing.T) {
			dsn, cleanup := dc.start(t)
			defer cleanup()
			dc.exec(t, dsn, `CREATE TABLE w (id BIGINT NOT NULL PRIMARY KEY, dj VARCHAR(20))`)
			dc.exec(t, dsn, `INSERT INTO w (id, dj) VALUES (1, 'v'), (2, 'v')`)
			// The boundary saw `doomed`; the source no longer has it.
			table := driftTable(dc.schema, "id", "doomed", "dj")
			if err := driftBackfill(t, dc, dsn, table, []*ir.Column{{Name: "dj", Type: ir.Text{}}}); err != nil {
				t.Fatalf("a column dropped later on the source, which the backfill does not carry, failed it: %v", err)
			}
		})
	}
}

// TestBackfillSourceDrift_AddedColumnGoneRefuses is case (a).
func TestBackfillSourceDrift_AddedColumnGoneRefuses(t *testing.T) {
	for _, dc := range driftCases() {
		t.Run(dc.name, func(t *testing.T) {
			dsn, cleanup := dc.start(t)
			defer cleanup()
			// The boundary added `dj`; the source has since renamed it.
			dc.exec(t, dsn, `CREATE TABLE w (id BIGINT NOT NULL PRIMARY KEY, dj_renamed VARCHAR(20))`)
			dc.exec(t, dsn, `INSERT INTO w (id, dj_renamed) VALUES (1, 'v')`)
			table := driftTable(dc.schema, "id", "dj")
			err := driftBackfill(t, dc, dsn, table, []*ir.Column{{Name: "dj", Type: ir.Text{}}})
			if err == nil {
				t.Fatal("the backfill completed although the added column no longer exists on the source — a rename would leave its pre-existing target rows with the target's fill, silently")
			}
			if !strings.Contains(err.Error(), "dj") {
				t.Errorf("the refusal does not name the missing column: %v", err)
			}
		})
	}
}

// TestBackfillSourceDrift_KeyColumnRenamedOrDroppedRefuses is case (b).
func TestBackfillSourceDrift_KeyColumnRenamedOrDroppedRefuses(t *testing.T) {
	for _, dc := range driftCases() {
		t.Run(dc.name+"/renamed", func(t *testing.T) {
			dsn, cleanup := dc.start(t)
			defer cleanup()
			dc.exec(t, dsn, `CREATE TABLE w (id2 BIGINT NOT NULL PRIMARY KEY, dj VARCHAR(20))`)
			dc.exec(t, dsn, `INSERT INTO w (id2, dj) VALUES (1, 'v')`)
			table := driftTable(dc.schema, "id", "dj") // the boundary's key was `id`
			err := driftBackfill(t, dc, dsn, table, []*ir.Column{{Name: "dj", Type: ir.Text{}}})
			if err == nil || !strings.Contains(err.Error(), "id2") {
				t.Fatalf("err = %v; want a refusal naming the source's key column the stream does not carry", err)
			}
		})
		t.Run(dc.name+"/dropped", func(t *testing.T) {
			dsn, cleanup := dc.start(t)
			defer cleanup()
			dc.exec(t, dsn, `CREATE TABLE w (id BIGINT NOT NULL, dj VARCHAR(20))`)
			dc.exec(t, dsn, `INSERT INTO w (id, dj) VALUES (1, 'v')`)
			table := driftTable(dc.schema, "id", "dj")
			err := driftBackfill(t, dc, dsn, table, []*ir.Column{{Name: "dj", Type: ir.Text{}}})
			if err == nil || !strings.Contains(err.Error(), "no primary key") {
				t.Fatalf("err = %v; want the no-primary-key refusal", err)
			}
		})
	}
}
