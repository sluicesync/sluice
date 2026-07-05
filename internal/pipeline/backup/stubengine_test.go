// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// stubEngine is a placeholder ir.Engine for validation / dispatch tests where
// no Open* method should be reached. Hitting one would be a regression in the
// validate-first ordering. This mirrors pipeline root's stubEngine (the root
// carve keeps its own copy for the migrate/stream tests that stay there); the
// backup domain gets its own trivial copy so the pool/backup unit tests carried
// into this package need no cross-package test-helper import.
type stubEngine struct{}

func (stubEngine) Name() string                  { return "stub" }
func (stubEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (stubEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	panic("stubEngine.OpenSchemaReader called — Run should have failed validation first")
}

func (stubEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	panic("stubEngine.OpenSchemaWriter called")
}

func (stubEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	panic("stubEngine.OpenRowReader called")
}

func (stubEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	panic("stubEngine.OpenRowWriter called")
}

func (stubEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	panic("stubEngine.OpenCDCReader called")
}

func (stubEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	panic("stubEngine.OpenChangeApplier called")
}

func (stubEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	panic("stubEngine.OpenSnapshotStream called")
}

// recordingSchemaReader is a trivial ir.SchemaReader that hands back a fixed
// schema (or error). Mirror of the pipeline-root test copy, duplicated so the
// backup test tree does not import root's.
type recordingSchemaReader struct {
	schema *ir.Schema
	err    error
}

func (r *recordingSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.schema, nil
}

// capsEngine is a minimal ir.Engine stub declaring an explicit
// (name, Capabilities) pair for gates that consume a full engine handle. The
// embedded stubEngine panics on every Open* call, so a gate that reaches past
// Name()/Capabilities() fails the test loudly. Mirror of the pipeline-root
// test copy.
type capsEngine struct {
	stubEngine

	name string
	caps ir.Capabilities
}

func (e capsEngine) Name() string                  { return e.name }
func (e capsEngine) Capabilities() ir.Capabilities { return e.caps }
