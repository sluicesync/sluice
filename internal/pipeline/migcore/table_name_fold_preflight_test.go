// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// foldEngine implements [ir.TableNameFoldPreflighter] and records what it was
// asked, so the dispatcher's two jobs — pass the DSN through, wrap the refusal
// with the mode — are both observable.
type foldEngine struct {
	ir.Engine // nil-panics on any real use

	sawDSN string
	calls  int
	refuse error
}

func (foldEngine) Name() string { return "folding-target" }

func (e *foldEngine) PreflightTableNameFold(_ context.Context, dsn string, _ *ir.Schema) error {
	e.sawDSN = dsn
	e.calls++
	return e.refuse
}

// nonFoldingEngine implements ir.Engine and NOT the fold surface — the Postgres /
// SQLite / source-only shape. Nothing about it may be probed.
type nonFoldingEngine struct{ ir.Engine }

func (nonFoldingEngine) Name() string { return "non-folding-target" }

func TestPreflightTableNameFold(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "orders"},
		{Name: "Orders"},
	}}

	t.Run("an engine with the surface is asked, with the DSN it was given", func(t *testing.T) {
		e := &foldEngine{}
		if err := PreflightTableNameFold(context.Background(), e, "target-dsn", schema, "migrate"); err != nil {
			t.Fatalf("clean schema refused: %v", err)
		}
		if e.calls != 1 {
			t.Errorf("engine asked %d times; want exactly 1 — the probe is a server round-trip", e.calls)
		}
		if e.sawDSN != "target-dsn" {
			t.Errorf("engine saw DSN %q; want %q. The fold is a property of the server this run points "+
				"at, so the wrong DSN answers the wrong question", e.sawDSN, "target-dsn")
		}
	})

	t.Run("a refusal is wrapped with the mode and preserves the engine's error", func(t *testing.T) {
		sentinel := errors.New("mysql: table-name collision: …")
		e := &foldEngine{refuse: sentinel}
		err := PreflightTableNameFold(context.Background(), e, "dsn", schema, "chain restore")
		if err == nil {
			t.Fatal("the engine refused and the dispatcher swallowed it")
		}
		if !errors.Is(err, sentinel) {
			t.Errorf("refusal does not wrap the engine's error (%v); a coded refusal must survive to the "+
				"CLI's sluicecode.FromError", err)
		}
		if !strings.HasPrefix(err.Error(), "chain restore: ") {
			t.Errorf("refusal is not labelled with the mode: %v", err)
		}
	})

	t.Run("an engine WITHOUT the surface is skipped silently", func(t *testing.T) {
		// The embedded nil ir.Engine panics on any method call, so "skipped"
		// here means skipped, not "called something harmless".
		if err := PreflightTableNameFold(context.Background(), nonFoldingEngine{}, "dsn", schema, "migrate"); err != nil {
			t.Fatalf("an engine that does not fold identifiers must be a no-op: %v", err)
		}
	})

	t.Run("a nil schema is skipped", func(t *testing.T) {
		e := &foldEngine{refuse: errors.New("must not be reached")}
		if err := PreflightTableNameFold(context.Background(), e, "dsn", nil, "restore"); err != nil {
			t.Fatalf("nil schema: %v", err)
		}
		if e.calls != 0 {
			t.Error("probed the server for a nil schema")
		}
	})
}
