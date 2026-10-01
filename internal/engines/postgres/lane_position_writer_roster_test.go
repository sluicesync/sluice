// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/applyorder"
)

// lanePositionWriters are the functions that put the stream's position on a
// target transaction.
var lanePositionWriters = []string{"writePositionTx", "writePositionPipelined", "buildWritePositionSQL"}

// lanePositionClass classifies every laneApplierAdapter method that reaches a
// position writer: "checkpoint" (the coordinator's frontier checkpoint),
// "fold" (writes an ADR-0190 amendment D fold ticket's position), "fold-core"
// (a lane write core whose fold batches reach a "fold" writer), or
// "position-free" with the reason it never writes one.
var lanePositionClass = map[string]string{
	"laneApplierAdapter.WriteCheckpoint":      "checkpoint",
	"laneApplierAdapter.queueFold":            "fold",
	"laneApplierAdapter.execFold":             "fold",
	"laneApplierAdapter.ApplyLaneBatch":       "fold-core",
	"laneApplierAdapter.applyLaneBatchSerial": "fold-core",
	"laneApplierAdapter.ApplyBarrierChange": "position-free: applyBarrierNoPosition runs applyOneImpl with " +
		"writePosition=false (Bug 158), reached here only by bare-name matching",
}

// postCommitHook matches a call that reports a durable position to something
// outside the target — the shape ADR-0190 amendment D's D-F2 asked about (the
// pre-GC-41 slot-ack tracker's reportAppliedToken / AfterCommit). None exists
// since GC-41 (the slot ack reads the persisted position back); if one is ever
// added to the checkpoint, the gate below requires the fold to call it too.
var postCommitHook = regexp.MustCompile(`^(report|release|notify|ack)|Report|Release|Notify|AfterCommit|Ack([A-Z]|$)`)

// TestLanePositionWriterRoster holds every lane-side position write to
// amendment D's classification, by AST: each laneApplierAdapter method that
// can reach a position writer is named, so a new lane path that persists a
// position cannot appear unexamined; a checkpoint and a fold writer both
// exist (the floor); and the fold's write cores retire their marks with
// Committed, as the checkpoint does, and call every post-commit hook the
// checkpoint calls — two writers of one row must leave the same trail.
func TestLanePositionWriterRoster(t *testing.T) {
	funcs, err := applyorder.ParseFuncs(".")
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	reach := applyorder.Reaching(funcs, lanePositionWriters)
	kinds := map[string]int{}
	for key := range reach {
		if !strings.HasPrefix(key, "laneApplierAdapter.") {
			continue
		}
		class, ok := lanePositionClass[key]
		if !ok {
			t.Errorf("%s can reach a position writer and is not classified in lanePositionClass (checkpoint, fold, fold-core, "+
				"or position-free with a reason)", key)
			continue
		}
		kinds[strings.SplitN(class, ":", 2)[0]]++
	}
	for key := range lanePositionClass {
		if !reach[key] {
			t.Errorf("lanePositionClass names %s, which no longer reaches a position writer — stale", key)
		}
	}
	if kinds["checkpoint"] < 1 || kinds["fold"] < 1 || kinds["checkpoint"]+kinds["fold"] < 2 {
		t.Fatalf("found %v lane position writers; want at least one checkpoint and one fold writer — the walk is not finding them", kinds)
	}
	var hooks []string
	for callee := range funcs["laneApplierAdapter.WriteCheckpoint"].Calls {
		if postCommitHook.MatchString(callee) {
			hooks = append(hooks, callee)
		}
	}
	for key, class := range lanePositionClass {
		if class != "fold-core" && class != "checkpoint" {
			continue
		}
		calls := funcs[key].Calls
		if !calls["Committed"] {
			t.Errorf("%s (%s) never calls Committed: the marks it makes durable would not be retired", key, class)
		}
		for _, h := range hooks {
			if !calls[h] {
				t.Errorf("%s (%s) does not call %s, which the checkpoint calls after its commit — the two position "+
					"writers must leave the same post-commit trail", key, class, h)
			}
		}
	}
	slices.Sort(hooks)
	t.Logf("lane position writers %v; post-commit hooks on the checkpoint: %v", kinds, hooks)
}
