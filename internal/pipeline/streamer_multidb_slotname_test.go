// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Audit 2026-09-01 A2-3: `--slot-name` reached the single-namespace
// snapshot opener and the single-namespace CDC reader, and NOTHING on the
// fan-out path — a Postgres multi-schema `sync start --slot-name x`
// created and then resumed `sluice_slot`. Two such streams against one
// database therefore could not coexist, and the slot name recorded in the
// CDC state row (what `sync add-table` and the slot-health reads resolve)
// named a slot the operator never asked for.
//
// Both halves are graded here because they are load-bearing TOGETHER: a
// cold start that creates `x` paired with a warm resume that opens the
// default slot resumes from whatever position that other slot sits at,
// which is a silent position substitution rather than an ignored flag.
//
// The fallback cells matter as much as the named ones: an engine with no
// slot to name (MySQL — the binlog IS the stream) must keep the unnamed
// opener, or every multi-database MySQL sync breaks on a type assertion
// that engine was never meant to satisfy.

// slotRecordingSource records which surface the orchestrator reached for
// and with what name. withSlot toggles whether it offers the slot-aware
// surfaces; the slot-aware cells wrap it in [slotAwareSource], so one
// recorder covers the Postgres-shaped and MySQL-shaped cells.
type slotRecordingSource struct {
	stubEngine

	plainSnapshotCalls int
	namedSnapshotCalls int
	snapshotSlot       string

	plainReaderCalls int
	namedReaderCalls int
	readerSlot       string
}

func (s *slotRecordingSource) Name() string { return "slot-recorder" }

func (s *slotRecordingSource) OpenMultiDatabaseSnapshotStream(context.Context, string, []string) (*ir.SnapshotStream, error) {
	s.plainSnapshotCalls++
	return nil, errors.New("stop here: the surface choice is what this pins")
}

func (s *slotRecordingSource) OpenServerCDCReader(context.Context, string) (ir.CDCReader, error) {
	s.plainReaderCalls++
	return nil, errors.New("stop here: the surface choice is what this pins")
}

// slotAwareSource adds the two optional slot-named surfaces. It is a
// separate type so the fallback cells can use a source that genuinely
// does NOT implement them — a bool field would still satisfy the
// interface and the fallback would be untestable.
type slotAwareSource struct{ *slotRecordingSource }

func (s slotAwareSource) OpenMultiDatabaseSnapshotStreamWithSlot(_ context.Context, _ string, _ []string, slotName string) (*ir.SnapshotStream, error) {
	s.namedSnapshotCalls++
	s.snapshotSlot = slotName
	return nil, errors.New("stop here: the surface choice is what this pins")
}

func (s slotAwareSource) OpenServerCDCReaderWithSlot(_ context.Context, _, slotName string) (ir.CDCReader, error) {
	s.namedReaderCalls++
	s.readerSlot = slotName
	return nil, errors.New("stop here: the surface choice is what this pins")
}

func TestMultiDatabaseOpeners_HonourSlotName(t *testing.T) {
	type counts struct {
		plain, named int
		slot         string
	}
	for _, tc := range []struct {
		name         string
		slotAware    bool
		slotName     string
		wantSnapshot counts
		wantReader   counts
	}{
		{
			// The A2-3 shape, on a Postgres-shaped source.
			name:         "slot-aware engine, operator named a slot",
			slotAware:    true,
			slotName:     "sluice_two",
			wantSnapshot: counts{named: 1, slot: "sluice_two"},
			wantReader:   counts{named: 1, slot: "sluice_two"},
		},
		{
			// No flag: the unnamed surface, byte-identical to before.
			name:         "slot-aware engine, no slot named",
			slotAware:    true,
			slotName:     "",
			wantSnapshot: counts{plain: 1},
			wantReader:   counts{plain: 1},
		},
		{
			// MySQL's shape. A named slot on a slotless engine keeps the
			// unnamed surface (DEBUG-logged) rather than failing.
			name:         "engine without the slot surfaces falls back",
			slotAware:    false,
			slotName:     "sluice_two",
			wantSnapshot: counts{plain: 1},
			wantReader:   counts{plain: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &slotRecordingSource{}
			var src ir.Engine = rec
			var snapOpener ir.MultiDatabaseSnapshotOpener = rec
			var readOpener ir.ServerCDCReaderOpener = rec
			if tc.slotAware {
				aware := slotAwareSource{slotRecordingSource: rec}
				src, snapOpener, readOpener = aware, aware, aware
			}

			// Both helpers return the stub's sentinel error; the assertion
			// is which surface ran, not what it returned.
			_, _ = openMultiDatabaseSnapshotStreamWithOptionalSlot(
				context.Background(), src, snapOpener, "dsn", []string{"a", "b"}, tc.slotName,
			)
			_, _ = openServerCDCReaderWithOptionalSlot(
				context.Background(), src, readOpener, "dsn", tc.slotName,
			)

			got := counts{plain: rec.plainSnapshotCalls, named: rec.namedSnapshotCalls, slot: rec.snapshotSlot}
			if got != tc.wantSnapshot {
				t.Errorf("cold-start snapshot opener: got %+v, want %+v — the spanning snapshot creates the slot, so a wrong name here is the slot the stream lives on", got, tc.wantSnapshot)
			}
			got = counts{plain: rec.plainReaderCalls, named: rec.namedReaderCalls, slot: rec.readerSlot}
			if got != tc.wantReader {
				t.Errorf("warm-resume reader opener: got %+v, want %+v — a warm resume on the wrong slot resumes from that slot's position", got, tc.wantReader)
			}
		})
	}
}

// TestMultiDatabaseFanOutOpensThroughTheSlotAwareHelpers is the call-site
// half, and it exists because the behavioural pin above did NOT catch its
// own mutant: reverting `coldStartMultiDatabase` to
// `opener.OpenMultiDatabaseSnapshotStream(...)` left every cell above
// green, because those cells call the helpers directly. A gate that
// grades a helper says nothing about who reaches for it — the exact
// narrow-gate shape CLAUDE.md warns about, found the only way it can be,
// by running the mutation.
//
// So this reads the AST of the fan-out file (not its text: a
// commented-out call must not satisfy it) and requires that the UNNAMED
// engine surfaces are called nowhere except inside the two helpers that
// wrap them, and that both helpers are actually reached from elsewhere in
// the file. Adding a third fan-out entry point that opens its own stream
// fails here until it goes through the helper.
func TestMultiDatabaseFanOutOpensThroughTheSlotAwareHelpers(t *testing.T) {
	const file = "streamer_multidb.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}

	// The unnamed surface each helper is allowed to wrap.
	wrappedBy := map[string]string{
		"OpenMultiDatabaseSnapshotStream": "openMultiDatabaseSnapshotStreamWithOptionalSlot",
		"OpenServerCDCReader":             "openServerCDCReaderWithOptionalSlot",
	}
	helperReached := map[string]bool{}

	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		enclosing := fn.Name.Name
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				surface := fun.Sel.Name
				helper, guarded := wrappedBy[surface]
				if guarded && enclosing != helper {
					t.Errorf("%s calls %s directly (in %s); it must go through %s, or --slot-name is dropped on that path (audit A2-3)",
						file, surface, enclosing, helper)
				}
			case *ast.Ident:
				if _, isHelper := helperReached[fun.Name]; !isHelper {
					for _, helper := range wrappedBy {
						if fun.Name == helper && enclosing != helper {
							helperReached[helper] = true
						}
					}
				}
			}
			return true
		})
	}

	// Anti-vacuity: a file that stopped opening streams entirely, or a
	// renamed helper, must fail rather than pass by having nothing to say.
	for surface, helper := range wrappedBy {
		if !helperReached[helper] {
			t.Errorf("no call to %s outside its own body — the fan-out path no longer opens through it, so nothing in this file is bound to honour --slot-name for %s",
				helper, surface)
		}
	}
}
