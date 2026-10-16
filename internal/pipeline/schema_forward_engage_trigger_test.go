// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// cdcKindEngine is a stubEngine declaring one CDC kind.
type cdcKindEngine struct {
	stubEngine
	cdc ir.CDCMethod
}

func (e cdcKindEngine) Capabilities() ir.Capabilities { return ir.Capabilities{CDC: e.cdc} }

// TestForwardSchemaEnabled_TriggerCDCSourceNeverForwards pins the premise the
// session-GUC-cast roster exempts the postgres-trigger first-touch boundary on
// (docsync sessionGUCCastEmitterExempt): a trigger-CDC source's boundaries
// never reach the forward intercept, whatever --schema-changes says, and its
// unforwarded-stream refusal neither blames that flag nor offers
// --schema-changes=forward as a way out.
func TestForwardSchemaEnabled_TriggerCDCSourceNeverForwards(t *testing.T) {
	for _, mode := range []string{"", "forward", "refuse"} {
		trig := &Streamer{Source: cdcKindEngine{cdc: ir.CDCTriggers}, SchemaChanges: mode}
		if trig.forwardSchemaEnabled() || trig.singleStreamSchemaForwardActive() {
			t.Errorf("--schema-changes=%q: a trigger-CDC source has forwarding active", mode)
		}
		if trig.unforwardedForwardRemedy() {
			t.Errorf("--schema-changes=%q: a trigger-CDC refusal offers --schema-changes=forward, which cannot help", mode)
		}
		if reason := trig.unforwardedStreamReason(); !strings.Contains(reason, "trigger-CDC") {
			t.Errorf("--schema-changes=%q: the refusal's reason %q does not name the trigger-CDC source", mode, reason)
		}

		// The control: the same modes on a replication source behave as before.
		repl := &Streamer{Source: cdcKindEngine{cdc: ir.CDCLogicalReplication}, SchemaChanges: mode}
		if want := mode != "refuse"; repl.forwardSchemaEnabled() != want {
			t.Errorf("--schema-changes=%q: a logical-replication source forwardSchemaEnabled = %v; want %v", mode, repl.forwardSchemaEnabled(), want)
		}
	}
}
