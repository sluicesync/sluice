// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

// Task #8 / ADR-0048 Shape A defensive refusal pin. `sluice schema
// add-table` mid-stream on a Shape A stream is Phase 2 per DP-3; v1
// uses the drained model. The --inject-shard-column flag on
// add-table is the operator's "I know this is a Shape A stream"
// affirmation; the command must refuse loudly with an operator-
// actionable recovery hint rather than silently running the Phase 1
// path against a discriminator-aware target (which would crash or
// silently drop the discriminator on new rows).
//
// This is a CLI-level early-return refusal, so the test runs against
// a Globals stub without any database or engine wiring. The refusal
// fires BEFORE any I/O — that's the load-bearing property; an
// operator who passes the flag must NEVER reach the publication-add
// or bulk-copy phases.

func TestSchemaAddTable_RefusesShapeAFlag(t *testing.T) {
	for _, val := range []string{
		"source_shard_id=shard_a",
		"shard=A",
		"  shard_col=val_b  ", // whitespace-tolerant
	} {
		val := val
		t.Run("rejects:"+val, func(t *testing.T) {
			cmd := &SchemaAddTableCmd{
				// Minimal required fields; the refusal fires before
				// any of these are exercised, so empty DSNs are fine.
				SourceDriver:      "postgres",
				Source:            "postgres://stub",
				TargetDriver:      "postgres",
				Target:            "postgres://stub",
				Table:             "widgets_v2",
				StreamID:          "stream-1",
				InjectShardColumn: val,
				Yes:               true, // bypass typed-confirmation
			}
			err := cmd.Run(&Globals{})
			if err == nil {
				t.Fatal("expected refusal when --inject-shard-column is set; got nil")
			}
			msg := err.Error()
			// Operator-actionable message must name:
			//   (1) Shape A / ADR-0048 (so the operator can search docs)
			//   (2) The drained model (the recovery path)
			//   (3) Phase 2 / DP-3 (so the operator knows it's deferred,
			//       not a permanent restriction)
			wantSubstrings := []string{
				"Shape A",
				"ADR-0048",
				"sync stop --wait",
				"sync start --resume",
			}
			for _, want := range wantSubstrings {
				if !strings.Contains(msg, want) {
					t.Errorf("refusal message missing %q\n--- got ---\n%s", want, msg)
				}
			}
		})
	}
}

// TestSchemaAddTable_AllowsEmptyShapeAFlag pins the regression guard:
// the existing non-Shape-A workflow (no --inject-shard-column) must
// NOT trip the refusal. The test stops before the I/O phase — Run
// will fail on the engine-lookup or DSN parse later, but NOT on the
// Shape A refusal itself. We assert the error message (if any) does
// NOT mention ADR-0048 Shape A.
func TestSchemaAddTable_AllowsEmptyShapeAFlag(t *testing.T) {
	for _, val := range []string{"", "   "} {
		val := val
		t.Run("allows:"+val, func(t *testing.T) {
			cmd := &SchemaAddTableCmd{
				SourceDriver:      "postgres",
				Source:            "postgres://stub:badpw@127.0.0.1:1/db?sslmode=disable",
				TargetDriver:      "postgres",
				Target:            "postgres://stub:badpw@127.0.0.1:1/db?sslmode=disable",
				Table:             "widgets_v2",
				StreamID:          "stream-1",
				InjectShardColumn: val,
				Yes:               true,
			}
			err := cmd.Run(&Globals{})
			// We expect an error from downstream (DSN dial / engine
			// open / etc.) but it must NOT be the Shape A refusal.
			if err == nil {
				t.Fatal("expected downstream error (DSN dial failure); got nil")
			}
			if strings.Contains(err.Error(), "Shape A") || strings.Contains(err.Error(), "ADR-0048") {
				t.Errorf("non-Shape-A invocation tripped Shape A refusal: %v", err)
			}
		})
	}
}
