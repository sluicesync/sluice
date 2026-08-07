// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/alecthomas/kong"
	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// This file is the wiring gate for the errno-3024 index-wall hint, and it
// deliberately does NOT test the hint registry.
//
// The registry entry was correct all along, and migcore's own hints_test.go
// pinned it green — matching the walled text under
// [migcore.PhaseIndexes] in isolation. That is exactly why the defect
// survived to a 122 GB field run: the ONLY index-build path a real
// PlanetScale/MySQL target takes is the ADR-0077/0080 overlapped
// copy+index errgroup, and that call site handed its error to
// [migcore.PhaseBulkCopy] — a phase whose entries the walled text matches
// none of. A table-level pin cannot see a mis-wired phase argument, so
// this one drives the failure THROUGH the real CLI: kong parses real
// argv, MigrateCmd.Run builds a real pipeline.Migrator, the real
// orchestrator runs the real overlapped phase, and the assertions read
// what the operator actually receives — the `hint:` line on stderr and
// the `code`/`hint` attrs logCodedError emits under --log-format json.
//
// (The Bug-180 lesson one layer over: pin the fix through the CLI layer,
// not against the table it reads from.)

// walledEngine is a fake ir.Engine used as BOTH the source and the target of
// a migrate run, registered under two names so the run is cross-engine the
// way the field capture was (PG → PlanetScale-MySQL).
//
// Only the target half's SchemaWriter is interesting: it implements
// [ir.IncrementalIndexBuilder], which is what routes the orchestrator down
// the overlapped copy+index path — the same branch every production MySQL
// and Postgres target takes.
type walledEngine struct {
	name string
	// wall, when non-nil, is returned by the index builder after it
	// receives its first table.
	wall error
}

func (e *walledEngine) Name() string                  { return e.name }
func (e *walledEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *walledEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return walledSchemaReader{}, nil
}

func (e *walledEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return &walledSchemaWriter{wall: e.wall}, nil
}

func (e *walledEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return walledRowReader{}, nil
}

func (e *walledEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return walledRowWriter{}, nil
}

func (e *walledEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("walledEngine: no CDC")
}

func (e *walledEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("walledEngine: no change applier")
}

func (e *walledEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("walledEngine: no snapshot stream")
}

// walledSchemaReader yields one PK-bearing table carrying one secondary
// index — the minimum shape that makes the index phase load-bearing.
type walledSchemaReader struct{}

func (walledSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) {
	return &ir.Schema{Tables: []*ir.Table{{
		Name:       "events",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{{
			Name:    "idx_events_id",
			Columns: []ir.IndexColumn{{Column: "id"}},
		}},
	}}}, nil
}

type walledRowReader struct{}

func (walledRowReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	out := make(chan ir.Row, 1)
	out <- ir.Row{"id": int64(1)}
	close(out)
	return out, nil
}

func (walledRowReader) Err() error { return nil }

type walledRowWriter struct{}

func (walledRowWriter) WriteRows(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	for range rows { //nolint:revive // draining is the whole job
	}
	return nil
}

// walledSchemaWriter is the target writer. Every schema phase is a no-op
// except the incremental index build, which returns the configured wall.
type walledSchemaWriter struct {
	wall error
	cb   func(table *ir.Table)
}

func (*walledSchemaWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	return nil
}
func (*walledSchemaWriter) CreateIndexes(context.Context, *ir.Schema) error         { return nil }
func (*walledSchemaWriter) CreateConstraints(context.Context, *ir.Schema) error     { return nil }
func (*walledSchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error { return nil }
func (*walledSchemaWriter) CreateViews(context.Context, *ir.Schema) error           { return nil }

func (w *walledSchemaWriter) SetTableIndexedCallback(fn func(table *ir.Table)) { w.cb = fn }

func (w *walledSchemaWriter) BuildTableIndexesFromChannel(
	ctx context.Context, _ *ir.Schema, completedTables <-chan *ir.Table,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case table, ok := <-completedTables:
			if !ok {
				return nil
			}
			if w.wall != nil {
				return w.wall
			}
			if w.cb != nil {
				w.cb(table)
			}
		}
	}
}

// Compile-time proof the fake exposes the two surfaces that select the
// overlapped path; without IncrementalIndexBuilder the orchestrator would
// take the sequential fallback and this gate would exercise the wrong
// branch entirely (and pass, since that branch was never broken).
var (
	_ ir.IncrementalIndexBuilder = (*walledSchemaWriter)(nil)
	_ ir.TableIndexedNotifier    = (*walledSchemaWriter)(nil)
)

// fieldCapturedWall reproduces the errno-3024 error the engine writer
// produces, byte-for-byte in shape: the *gomysql.MySQLError the fallback
// classifier keys on, wrapped in the MySQL writer's own
// `mysql: create indexes on %q` prefix — the ENTIRE text the 122 GB field
// run printed, with no `pipeline:` prefix, no hint, and no code.
func fieldCapturedWall() error {
	return &wallWrap{inner: &gomysql.MySQLError{
		Number:  3024,
		Message: "target: scaletest-my3.-.primary: vttablet: rpc error: code = Canceled desc = (errno 3024) (sqlstate HY000): Query execution was interrupted, maximum statement execution time exceeded, elapsed time: 15m0.000198624s, killing connection ID 2595",
	}}
}

type wallWrap struct{ inner error }

func (w *wallWrap) Error() string { return `mysql: create indexes on "events": ` + w.inner.Error() }
func (w *wallWrap) Unwrap() error { return w.inner }

func init() {
	engines.Register(&walledEngine{name: "walledsrc"})
	engines.Register(&walledEngine{name: "walledtgt", wall: fieldCapturedWall()})
}

// TestMigrateIndexWall_HintAndCodeReachTheOperator is the gate. It runs the
// real `sluice migrate` through kong and asserts the operator receives, at
// the moment a 122 GB copy has already succeeded:
//
//   - the `hint:` line naming --resume as the no-re-copy remedy, and
//   - the stable SLUICE-E-INDEX-STATEMENT-TIME-LIMIT code, on the
//     structured record logCodedError emits at the exit boundary.
//
// Neuter the phase attribution in runOverlappedCopyAndIndexPhase (hand the
// error to migcore.PhaseBulkCopy again) and BOTH assertions fail — that is
// the mutation check this gate is built to survive.
func TestMigrateIndexWall_HintAndCodeReachTheOperator(t *testing.T) {
	snapshot := captureSlog(t)

	cli := &CLI{}
	parser, err := kong.New(cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	kctx, err := parser.Parse([]string{
		"migrate",
		"--source-driver=walledsrc", "--source=fake://source",
		"--target-driver=walledtgt", "--target=fake://target",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// main() does this between Parse and Run, and the hint under test is
	// `migrate`'s per-command one (Bug 230) — without it this gate would be
	// asserting against the command-neutral text.
	recordRunningCommand(kctx)
	defer migcore.SetRunningCommand("")

	runErr := kctx.Run(&cli.Globals)
	if runErr == nil {
		t.Fatal("migrate succeeded; the fake target's index build was supposed to wall at errno 3024")
	}

	// Guard the premise: if the run failed somewhere OTHER than the index
	// build this gate would be asserting against the wrong failure.
	if !strings.Contains(runErr.Error(), "maximum statement execution time") {
		t.Fatalf("the run failed before reaching the walled index build, so this gate is vacuous: %v", runErr)
	}

	// 1. The hint line. This is the sentence worth 122 GB of re-copy.
	if !strings.Contains(runErr.Error(), "\nhint: ") {
		t.Errorf("no hint line reached the operator; the whole error was:\n%v", runErr)
	}
	if !strings.Contains(runErr.Error(), "--resume finishes just the indexes with NO re-copy") {
		t.Errorf("the hint does not tell the operator the data is already copied:\n%v", runErr)
	}

	// 2. The stable code, on the record the exit boundary actually emits.
	coded, ok := sluicecode.FromError(runErr)
	if !ok {
		t.Fatalf("the error carries no sluicecode at all, so --log-format json emits nothing:\n%v", runErr)
	}
	if coded.Code != sluicecode.CodeIndexStatementTimeLimit {
		t.Errorf("code = %s; want %s", coded.Code, sluicecode.CodeIndexStatementTimeLimit)
	}

	logCodedError(runErr)
	attrs := findRecordAttrs(t, snapshot(), "command failed")
	if attrs["code"] != string(sluicecode.CodeIndexStatementTimeLimit) {
		t.Errorf("exit-boundary code attr = %q; want %q", attrs["code"], sluicecode.CodeIndexStatementTimeLimit)
	}
	if !strings.Contains(attrs["hint"], "--resume") {
		t.Errorf("exit-boundary hint attr does not name --resume: %q", attrs["hint"])
	}
}

// captureSlog swaps the default logger for a recorder for the duration of the
// test and returns a snapshot function. These tests drive a REAL Migrator whose
// parallel copy goroutines log concurrently, so the returned closure takes a
// LOCKED copy of the records — reading the slice directly would race the
// concurrent appends the `-race` job flags. Call it after the run to assert.
func captureSlog(t *testing.T) func() []slog.Record {
	t.Helper()
	var (
		mu      sync.Mutex
		records []slog.Record
	)
	prev := slog.Default()
	slog.SetDefault(slog.New(captureHandler{mu: &mu, records: &records}))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return func() []slog.Record {
		mu.Lock()
		defer mu.Unlock()
		return append([]slog.Record(nil), records...)
	}
}

// findRecordAttrs returns the attrs of the LAST record with the given
// message, failing the test when none matched.
func findRecordAttrs(t *testing.T, records []slog.Record, msg string) map[string]string {
	t.Helper()
	for i := len(records) - 1; i >= 0; i-- {
		if records[i].Message != msg {
			continue
		}
		out := map[string]string{}
		records[i].Attrs(func(a slog.Attr) bool {
			out[a.Key] = a.Value.String()
			return true
		})
		return out
	}
	t.Fatalf("no slog record with message %q was emitted", msg)
	return nil
}
