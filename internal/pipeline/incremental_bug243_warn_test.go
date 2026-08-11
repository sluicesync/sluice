// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestWarnIfParentChainUnrestorable pins the Bug 243 incremental-door
// signal: extending a chain whose recorded schema cannot be emitted as
// valid SQL WARNs — naming the code, the count, and a first offending
// field — and a healthy parent stays silent. It is a WARN and not a
// refusal by design (the incremental's data is valid); this pin is what
// keeps the signal from silently disappearing, which is how Bug 243
// presented in the first place.
func TestWarnIfParentChainUnrestorable(t *testing.T) {
	ctx := context.Background()

	t.Run("mangled parent schema warns with the code", func(t *testing.T) {
		buf := captureSlog(t)
		parent := &irbackup.Manifest{Schema: &ir.Schema{Tables: []*ir.Table{{
			Name:             "ck",
			CheckConstraints: []*ir.CheckConstraint{{Name: "ck_name", Expr: `name <> 'o\\'brien'`}},
		}}}}
		warnIfParentChainUnrestorable(ctx, parent, "chain/manifest.json")
		out := buf.String()
		for _, want := range []string{"SLUICE-E-BACKUP-RECORDED-SCHEMA-MALFORMED", "will NOT restore", "chain/manifest.json"} {
			if !strings.Contains(out, want) {
				t.Errorf("WARN output missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("healthy parent is silent", func(t *testing.T) {
		buf := captureSlog(t)
		parent := &irbackup.Manifest{Schema: &ir.Schema{Tables: []*ir.Table{{
			Name:             "ck",
			CheckConstraints: []*ir.CheckConstraint{{Name: "ck_name", Expr: `name <> 'o''brien'`}},
		}}}}
		warnIfParentChainUnrestorable(ctx, parent, "chain/manifest.json")
		if out := buf.String(); strings.Contains(out, "RECORDED-SCHEMA-MALFORMED") {
			t.Errorf("healthy parent produced the Bug 243 WARN:\n%s", out)
		}
	})

	t.Run("nil parent and nil schema are no-ops", func(*testing.T) {
		warnIfParentChainUnrestorable(ctx, nil, "p")
		warnIfParentChainUnrestorable(ctx, &irbackup.Manifest{}, "p")
	})
}
