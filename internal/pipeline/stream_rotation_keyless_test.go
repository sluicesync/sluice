// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Bug 297 at `backup stream` start: with rotation enabled, a source holding
// a keyless table is refused before the pump opens and before anything is
// written; without rotation (or with keyed tables only) the stream runs.
func TestBackupStream_RotationRefusesAKeylessTableAtStart(t *testing.T) {
	stream := func(rotate bool, schema *ir.Schema) (*BackupStream, func() int) {
		store, parent := redactedChainFixture(t, false)
		if schema == nil {
			schema = parent.Schema
		}
		b := &BackupStream{
			Source:             redactedChainSource(schema),
			SourceDSN:          "src",
			Store:              store,
			ParentRef:          parent.BackupID,
			RolloverWindow:     time.Second,
			RolloverMaxChanges: 10,
			RolloverMaxBytes:   1 << 30,
			ChunkChanges:       10,
			SluiceVersion:      "test",
		}
		if rotate {
			b.RetainRotateAtChainLength = 5
		}
		return b, func() int { return manifestCount(t, store) }
	}

	// The fixture's "users" table has no PRIMARY KEY and no unique index.
	b, count := stream(true, nil)
	err := b.Run(context.Background())
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupRotatedKeylessTable {
		t.Fatalf("rotation + keyless table: err = %v; want %s", err, sluicecode.CodeBackupRotatedKeylessTable)
	}
	if n := count(); n != 1 {
		t.Errorf("manifests = %d; want 1 (the parent full alone — the refusal is before the first window)", n)
	}

	// No rotation: the same chain extends exactly as before.
	b, count = stream(false, nil)
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("no rotation, keyless table: %v", err)
	}
	if n := count(); n != 2 {
		t.Errorf("manifests = %d; want 2 (full + incremental)", n)
	}

	// Rotation with the table keyed: not refused.
	keyed := &ir.Schema{Tables: []*ir.Table{{
		Name:       "users",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}
	b, _ = stream(true, keyed)
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("rotation, keyed table: %v", err)
	}
}

// TestPerformRotation_MarksTheSegmentFullAsRotationBorn holds the one
// construction of a rotation-born segment full to the Bug 297 door: the
// door is a Backup field whose zero value is OFF (an ordinary `backup full`
// needs no key), so a rotation full built without it would silently skip it.
// There is exactly one such construction; the walk requires it.
func TestPerformRotation_MarksTheSegmentFullAsRotationBorn(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "stream_rotation.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var literals, marked int
	ast.Inspect(f, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := cl.Type.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Backup" {
			return true
		}
		literals++
		for _, e := range cl.Elts {
			kv, ok := e.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "RotationSegment" {
				if v, ok := kv.Value.(*ast.Ident); ok && v.Name == "true" {
					marked++
				}
			}
		}
		return true
	})
	if literals == 0 {
		t.Fatal("found no backup.Backup literal in stream_rotation.go — the walk broke, or the rotation full moved; re-point this gate")
	}
	if marked != literals {
		t.Errorf("%d of %d backup.Backup literal(s) in stream_rotation.go set RotationSegment: true; a rotation-born "+
			"segment full without it skips the Bug 297 keyless door and builds a chain no restore can apply whole",
			marked, literals)
	}
}
