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

// laneCommitCall is the COMMIT each position-writing lane core makes; the
// fold cores must anchor their transaction only after it.
var laneCommitCall = map[string]string{
	"laneApplierAdapter.WriteCheckpoint":      "commitWithTimeout",
	"laneApplierAdapter.ApplyLaneBatch":       "flushAndCommitStep",
	"laneApplierAdapter.applyLaneBatchSerial": "commitWithTimeout",
}

// TestLanePositionWriterRoster holds every lane-side position write to
// amendment D's classification, by AST: each laneApplierAdapter method that
// can reach a position writer is named, so a new lane path that persists a
// position cannot appear unexamined; a checkpoint and a fold writer both
// exist (the floor); the fold's write cores retire their marks with
// Committed, as the checkpoint does, and call every post-commit hook the
// checkpoint calls — two writers of one row must leave the same trail; a
// fold core calls LaneFence.Anchor only AFTER its COMMIT (an Anchor before it
// would let other lanes write the transaction's marks while the position
// that must precede them could still roll back); and every position-writing
// core pins synchronous_commit on its transaction, as the checkpoint does —
// directly (forceSynchronousCommitOn) or through beginPipelinedTxOn, whose
// body is checked for the SET LOCAL itself.
//
// Reach, stated: the walk sees laneApplierAdapter methods of this package by
// bare-name calls, in source order; a hook or pin reached only through a
// function value or a differently named helper is outside it. The MySQL twin
// has no synchronous_commit check — MySQL's commit durability is server
// configuration (innodb_flush_log_at_trx_commit), the same for both writers,
// and sluice sets no session pin there.
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
		commit, ok := laneCommitCall[key]
		if !ok || !calls[commit] {
			t.Errorf("%s (%s) does not call its COMMIT %q (laneCommitCall) — the ordering checks below cannot be graded", key, class, commit)
			continue
		}
		if class == "fold-core" && !funcs[key].Before(commit, "Anchor") {
			t.Errorf("%s anchors its fold before (or without) its COMMIT %s: other lanes could write the transaction's "+
				"marks while the position that must precede them could still roll back", key, commit)
		}
		if !calls["forceSynchronousCommitOn"] && !calls["beginPipelinedTxOn"] {
			t.Errorf("%s (%s) pins synchronous_commit neither directly nor through beginPipelinedTxOn: its position "+
				"write is not as durable as the checkpoint's (F7)", key, class)
		}
	}
	if begin := funcs["ChangeApplier.beginPipelinedTxOn"]; begin == nil ||
		!slices.Contains(begin.Strings, "SET LOCAL synchronous_commit = on") {
		t.Error("beginPipelinedTxOn no longer issues SET LOCAL synchronous_commit = on: the pipelined fold core's durability pin is gone")
	}
	slices.Sort(hooks)
	t.Logf("lane position writers %v; post-commit hooks on the checkpoint: %v", kinds, hooks)
}
