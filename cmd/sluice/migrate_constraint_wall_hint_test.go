// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// This file is the wiring gate for the errno-3024 CONSTRAINTS-phase wall
// hint (roadmap item 109), and — like its index sibling in
// migrate_index_wall_hint_test.go — it deliberately drives the failure
// THROUGH the real CLI rather than asserting against the hint registry.
//
// The registry entry and its unit pin (migcore/hints_test.go) confirm a
// PhaseConstraints errno-3024 text matches the FK hint in isolation. That is
// exactly the shape of coverage that let the ORIGINAL index-wall bug survive
// to a field run (F6, PR #304): the table was right and the wiring dropped
// it. So this gate parses real argv with kong, builds a real
// pipeline.Migrator, runs the real orchestrator down to CreateConstraints,
// and reads what the operator actually receives — the `hint:` line on the
// returned error and the `code`/`hint` attrs logCodedError emits.
//
// The load-bearing assertion is that the remedy DIVERGES from the index
// wall's: a constraints-phase 3024 is DETERMINISTIC (--resume re-runs the
// identical validating ADD FOREIGN KEY and re-hits the same wall), so the
// hint must steer to --skip-foreign-keys, never --resume.

// constraintWalledEngine is a fake ir.Engine used as both source and target
// of a migrate run. Its target SchemaWriter no-ops every phase EXCEPT
// CreateConstraints, which returns the field-captured errno-3024 FK wall. It
// deliberately does NOT implement ir.IncrementalIndexBuilder, so the
// orchestrator takes the plain sequential path (create tables → copy →
// indexes → constraints) and the constraints phase is reached with no
// index-overlap machinery in the way.
type constraintWalledEngine struct {
	name string
	// wall, when non-nil, is returned by CreateConstraints.
	wall error
	// freshTarget makes OpenSchemaReader return an EMPTY catalog. The
	// target side of this fixture models a FRESH database — the whole
	// premise of the gate is that sluice creates the foreign key itself,
	// after the copy, and walls doing so. Handing the target reader the
	// same FK-bearing catalog as the source would model a target that
	// already carries the constraint, which roadmap item 140 now refuses
	// upfront (correctly) — long before the constraints phase this gate
	// exists to reach.
	freshTarget bool
}

func (e *constraintWalledEngine) Name() string                  { return e.name }
func (e *constraintWalledEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *constraintWalledEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return constraintWalledSchemaReader{empty: e.freshTarget}, nil
}

func (e *constraintWalledEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return &constraintWalledSchemaWriter{wall: e.wall}, nil
}

func (e *constraintWalledEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return walledRowReader{}, nil
}

func (e *constraintWalledEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return walledRowWriter{}, nil
}

func (e *constraintWalledEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("constraintWalledEngine: no CDC")
}

func (e *constraintWalledEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("constraintWalledEngine: no change applier")
}

func (e *constraintWalledEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("constraintWalledEngine: no snapshot stream")
}

// constraintWalledSchemaReader yields a child table carrying a foreign key —
// the minimum shape that makes the constraints phase load-bearing, and the
// same shape the field capture had (a child table's FK to a parent).
type constraintWalledSchemaReader struct{ empty bool }

func (r constraintWalledSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) {
	if r.empty {
		return &ir.Schema{}, nil
	}
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name:       "customers",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		},
		{
			Name: "events",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "customer_id", Type: ir.Integer{Width: 64}},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
			ForeignKeys: []*ir.ForeignKey{{
				Name:              "events_customer_fk",
				Columns:           []string{"customer_id"},
				ReferencedTable:   "customers",
				ReferencedColumns: []string{"id"},
			}},
		},
	}}, nil
}

// constraintWalledSchemaWriter no-ops every phase except CreateConstraints,
// which returns the configured wall.
type constraintWalledSchemaWriter struct {
	wall error
}

func (*constraintWalledSchemaWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	return nil
}
func (*constraintWalledSchemaWriter) CreateIndexes(context.Context, *ir.Schema) error { return nil }

func (w *constraintWalledSchemaWriter) CreateConstraints(context.Context, *ir.Schema) error {
	return w.wall
}

func (*constraintWalledSchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error {
	return nil
}
func (*constraintWalledSchemaWriter) CreateViews(context.Context, *ir.Schema) error { return nil }

// fieldCapturedFKWall reproduces the errno-3024 error the MySQL writer
// produces on the FK path, byte-for-byte in shape: the *gomysql.MySQLError
// wrapped in the writer's own `mysql: add foreign key %q on %q` prefix — the
// entire text the 153M-row field run printed, with no `pipeline:` prefix, no
// hint, and no code.
func fieldCapturedFKWall() error {
	return &fkWallWrap{inner: &gomysql.MySQLError{
		Number:  3024,
		Message: "target: scaletest-my4.-.primary: vttablet: rpc error: code = Canceled desc = (errno 3024) (sqlstate HY000): Query execution was interrupted, maximum statement execution time exceeded, elapsed time: 15m0.000198624s, killing connection ID 14228",
	}}
}

type fkWallWrap struct{ inner error }

func (w *fkWallWrap) Error() string {
	return `mysql: add foreign key "events_customer_fk" on "events": ` + w.inner.Error()
}
func (w *fkWallWrap) Unwrap() error { return w.inner }

func init() {
	engines.Register(&constraintWalledEngine{name: "cwalledsrc"})
	engines.Register(&constraintWalledEngine{name: "cwalledtgt", wall: fieldCapturedFKWall(), freshTarget: true})
}

// TestMigrateConstraintWall_HintAndCodeReachTheOperator is the gate. It runs
// the real `sluice migrate` through kong and asserts that when a 153M-row
// copy has already succeeded and the deferred ADD FOREIGN KEY walls at
// errno 3024, the operator receives:
//
//   - the `hint:` line naming --skip-foreign-keys as the CONVERGING remedy
//     (NOT --resume, which re-hits the same deterministic wall), and
//   - the stable SLUICE-E-CONSTRAINT-STATEMENT-TIME-LIMIT code on the
//     structured record logCodedError emits at the exit boundary.
//
// Neuter the phase attribution in the constraints-phase WrapWithHint call
// (hand the error to migcore.PhaseBulkCopy or PhaseIndexes instead) and BOTH
// assertions fail — that is the mutation check this gate is built to survive.
func TestMigrateConstraintWall_HintAndCodeReachTheOperator(t *testing.T) {
	snapshot := captureSlog(t)

	cli := &CLI{}
	parser, err := kong.New(cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	kctx, err := parser.Parse([]string{
		"migrate",
		"--source-driver=cwalledsrc", "--source=fake://source",
		"--target-driver=cwalledtgt", "--target=fake://target",
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
		t.Fatal("migrate succeeded; the fake target's ADD FOREIGN KEY was supposed to wall at errno 3024")
	}

	// Guard the premise: if the run failed somewhere OTHER than the FK build
	// this gate would be asserting against the wrong failure (vacuous green).
	if !strings.Contains(runErr.Error(), "maximum statement execution time") {
		t.Fatalf("the run failed before reaching the walled constraints phase, so this gate is vacuous: %v", runErr)
	}
	if !strings.Contains(runErr.Error(), "create constraints") {
		t.Fatalf("the walled error is not the constraints phase's; gate is asserting the wrong phase: %v", runErr)
	}

	// 1. The hint line — and the load-bearing divergence: --skip-foreign-keys,
	//    NOT the index wall's --resume (a constraints 3024 is deterministic).
	if !strings.Contains(runErr.Error(), "\nhint: ") {
		t.Errorf("no hint line reached the operator; the whole error was:\n%v", runErr)
	}
	if !strings.Contains(runErr.Error(), "--skip-foreign-keys") {
		t.Errorf("the hint does not steer to --skip-foreign-keys (the converging remedy):\n%v", runErr)
	}
	if strings.Contains(runErr.Error(), "--resume finishes just the indexes") {
		t.Errorf("the hint wrongly gives the INDEX-wall --resume remedy for a deterministic constraints 3024:\n%v", runErr)
	}

	// 2. The stable code, on the error and on the record the exit boundary emits.
	coded, ok := sluicecode.FromError(runErr)
	if !ok {
		t.Fatalf("the error carries no sluicecode at all, so --log-format json emits nothing:\n%v", runErr)
	}
	if coded.Code != sluicecode.CodeConstraintStatementTimeLimit {
		t.Errorf("code = %s; want %s", coded.Code, sluicecode.CodeConstraintStatementTimeLimit)
	}

	logCodedError(runErr)
	attrs := findRecordAttrs(t, snapshot(), "command failed")
	if attrs["code"] != string(sluicecode.CodeConstraintStatementTimeLimit) {
		t.Errorf("exit-boundary code attr = %q; want %q", attrs["code"], sluicecode.CodeConstraintStatementTimeLimit)
	}
	if !strings.Contains(attrs["hint"], "--skip-foreign-keys") {
		t.Errorf("exit-boundary hint attr does not name --skip-foreign-keys: %q", attrs["hint"])
	}
}
