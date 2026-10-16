// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
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

// boundaryEmittingTriggerEngine is a trigger-CDC engine whose reader emits
// schema boundaries, as postgres-trigger's does.
type boundaryEmittingTriggerEngine struct{ cdcKindEngine }

func (boundaryEmittingTriggerEngine) CDCReaderEmitsSchemaBoundaries() {}

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

		// TRIGGER-REFUSAL-REMEDY: the refusal names the engine's own recovery,
		// not "apply it on the target and restart", which only reaches the
		// reader's second refusal at the DDL marker.
		hint := unforwardedBoundaryDeps{triggerSource: trig.sourceRefusesDDL()}.recoveryHint("public.t", trig.unforwardedForwardRemedy(), true)
		for _, want := range []string{"drops and recreates", "--restart-from-scratch", "sync stop --wait"} {
			if !strings.Contains(hint, want) {
				t.Errorf("--schema-changes=%q: the trigger-CDC remedy %q does not name %q", mode, hint, want)
			}
		}
		if strings.Contains(hint, "SAME --stream-id") {
			t.Errorf("--schema-changes=%q: the trigger-CDC remedy still offers the drained model's restart: %q", mode, hint)
		}
		// Under --inject-shard-column the re-copy cannot remove rows deleted
		// while stopped: the remedy must say to delete the shard's rows first,
		// and only there.
		if strings.Contains(hint, "delete this shard's rows") {
			t.Errorf("--schema-changes=%q: an unsharded stream's remedy carries the shard caveat: %q", mode, hint)
		}
		sharded := unforwardedBoundaryDeps{triggerSource: true, shardEngaged: true}.recoveryHint("public.t", false, true)
		if !strings.Contains(sharded, "delete this shard's rows") {
			t.Errorf("--schema-changes=%q: a sharded trigger stream's remedy omits deleting the shard's rows first: %q", mode, sharded)
		}
		// Fourth review: under --schema-already-applied the restart drops
		// nothing, so the remedy must not promise a drop-and-recreate and must
		// say to land the change and empty the tables; the apply-then-empty
		// form names the SOURCE table and says a rename maps it elsewhere; and
		// the "not enough" claim is scoped to the event-trigger tier.
		saa := unforwardedBoundaryDeps{triggerSource: true, schemaAlreadyApplied: true}.recoveryHint("public.t", false, true)
		for _, h := range []string{sharded, saa} {
			if strings.Contains(h, "drops and recreates the stream's tables") {
				t.Errorf("--schema-changes=%q: a no-drop restart's remedy promises a drop-and-recreate: %q", mode, h)
			}
			if !strings.Contains(h, `source table "public.t"`) || !strings.Contains(h, "rename") {
				t.Errorf("--schema-changes=%q: the apply-then-empty remedy presents the source name as the target table: %q", mode, h)
			}
		}
		if !strings.Contains(saa, "--schema-already-applied") || !strings.Contains(saa, "empty it") {
			t.Errorf("--schema-changes=%q: a --schema-already-applied remedy omits landing the change and emptying the tables: %q", mode, saa)
		}
		if !strings.Contains(hint, "event triggers installed") {
			t.Errorf("--schema-changes=%q: the \"not enough\" claim is not scoped to the event-trigger tier: %q", mode, hint)
		}

		// TRIGGER-SHAPE-A-FORWARD: live cross-shard coordination forwards
		// boundaries whatever forwardSchemaEnabled says, so it refuses a
		// trigger source before engaging; --no-coordinate-live-ddl passes.
		shard := ShardColumnSpec{Name: "shard", Value: "a"}
		emitting := boundaryEmittingTriggerEngine{cdcKindEngine{cdc: ir.CDCTriggers}}
		coord := &Streamer{Source: emitting, SchemaChanges: mode, InjectShardColumn: shard}
		if err := coord.engageShardCoordination(context.Background(), nil); err == nil || !strings.Contains(err.Error(), triggerSourceCoordinationMarker) {
			t.Errorf("--schema-changes=%q: live coordination with a boundary-emitting trigger-CDC source = %v; want %s", mode, err, triggerSourceCoordinationMarker)
		}
		// A trigger-CDC source that emits no boundary (sqlite-trigger,
		// d1-trigger) has nothing for coordination to forward and must not
		// be refused by this door (the nil applier then fails the lease-store
		// check, which is a different refusal and not this one's business).
		quiet := &Streamer{Source: cdcKindEngine{cdc: ir.CDCTriggers}, SchemaChanges: mode, InjectShardColumn: shard}
		if err := quiet.engageShardCoordination(context.Background(), nil); err != nil && strings.Contains(err.Error(), triggerSourceCoordinationMarker) {
			t.Errorf("--schema-changes=%q: live coordination with a trigger-CDC source that emits no boundaries was refused: %v", mode, err)
		}
		drained := &Streamer{Source: emitting, SchemaChanges: mode, InjectShardColumn: shard, NoCoordinateLiveDDL: true}
		if err := drained.engageShardCoordination(context.Background(), nil); err != nil {
			t.Errorf("--schema-changes=%q: the drained model with a trigger-CDC source refused: %v", mode, err)
		}

		// The control: the same modes on a replication source behave as before.
		repl := &Streamer{Source: cdcKindEngine{cdc: ir.CDCLogicalReplication}, SchemaChanges: mode}
		if want := mode != "refuse"; repl.forwardSchemaEnabled() != want {
			t.Errorf("--schema-changes=%q: a logical-replication source forwardSchemaEnabled = %v; want %v", mode, repl.forwardSchemaEnabled(), want)
		}
	}
}
