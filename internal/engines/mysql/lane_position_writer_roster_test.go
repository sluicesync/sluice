// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/applyorder"
)

// lanePositionWriters are the functions that put the stream's position on a
// target transaction.
var lanePositionWriters = []string{"writePositionTx", "writePositionUpsertSQL"}

// lanePositionClass classifies every laneApplierAdapter method that reaches a
// position writer — see the Postgres twin (TestLanePositionWriterRoster there)
// for the classes.
var lanePositionClass = map[string]string{
	"laneApplierAdapter.WriteCheckpoint": "checkpoint",
	"laneApplierAdapter.writeFold":       "fold",
	"laneApplierAdapter.ApplyLaneBatch":  "fold-core",
	"laneApplierAdapter.ApplyBarrierChange": "position-free: applyBarrierNoPosition runs applyOneImpl with " +
		"writePosition=false (Bug 158), reached here only by bare-name matching",
}

// postCommitHook: see the Postgres twin.
var postCommitHook = regexp.MustCompile(`^(report|release|notify|ack)|Report|Release|Notify|AfterCommit|Ack([A-Z]|$)`)

// TestLanePositionWriterRoster is the Postgres gate's MySQL twin: every
// laneApplierAdapter method that can reach a position writer is classified,
// a checkpoint and a fold writer both exist, and the fold's write core
// leaves the checkpoint's post-commit trail (Committed, and any hook).
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
