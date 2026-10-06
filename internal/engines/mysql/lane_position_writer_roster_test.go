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
	"laneApplierAdapter.WriteCheckpoint":    "checkpoint",
	"laneApplierAdapter.writeFold":          "fold",
	"laneApplierAdapter.ApplyLaneBatch":     "fold-core",
	"laneApplierAdapter.ApplyBarrierChange": "barrier-fold",
}

// laneTxBody: see the Postgres twin. The checks grade the function that owns
// each core's transaction, never a one-line adapter.
var laneTxBody = map[string]string{
	"laneApplierAdapter.WriteCheckpoint":    "ChangeApplier.commitCheckpoint",
	"laneApplierAdapter.ApplyBarrierChange": "ChangeApplier.applyOneImpl",
}

// checkLaneTxBodies: see the Postgres twin. Each laneTxBody binding must name
// a function that exists, that its lane method reaches, and that opens the
// transaction itself (BeginTx).
func checkLaneTxBodies(t *testing.T, funcs map[string]*applyorder.Func) {
	t.Helper()
	for key, body := range laneTxBody {
		f := funcs[body]
		if f == nil {
			t.Fatalf("laneTxBody names %s as %s's transaction, and no such function exists — stale", body, key)
		}
		bare := body[strings.LastIndex(body, ".")+1:]
		if !applyorder.Reaching(funcs, []string{bare})[key] {
			t.Errorf("laneTxBody binds %s to %s, which %s no longer reaches — stale", key, body, key)
		}
		if !f.Calls["BeginTx"] {
			t.Errorf("laneTxBody binds %s to %s, which opens no transaction (no BeginTx) — the roster would grade the "+
				"wrong function", key, body)
		}
	}
}

// laneBody returns the [applyorder.Func] that owns key's transaction.
func laneBody(funcs map[string]*applyorder.Func, key string) *applyorder.Func {
	if body, ok := laneTxBody[key]; ok {
		return funcs[body]
	}
	return funcs[key]
}

// postCommitHook: see the Postgres twin.
var postCommitHook = regexp.MustCompile(`^(report|release|notify|ack)|Report|Release|Notify|AfterCommit|Ack([A-Z]|$)`)

// laneCommitCall is the COMMIT each position-writing lane core makes.
var laneCommitCall = map[string]string{
	"laneApplierAdapter.WriteCheckpoint":    "commitWithTimeout",
	"laneApplierAdapter.ApplyLaneBatch":     "commitWithTimeout",
	"laneApplierAdapter.ApplyBarrierChange": "commitWithTimeout",
}

// TestLanePositionWriterRoster is the Postgres gate's MySQL twin: every
// laneApplierAdapter method that can reach a position writer is classified;
// a checkpoint, a fold writer and the barrier fold (ADR-0190 amendment E) all
// exist; every position-writing core — graded on the function that owns its
// transaction (laneTxBody) — leaves the checkpoint's post-commit trail
// (Committed, and any hook); and a fold core anchors its fold only after its
// COMMIT. Reach as the twin states; no synchronous_commit check here —
// MySQL's commit durability is server configuration, the same for every
// writer.
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
				"barrier-fold, or position-free with a reason)", key)
			continue
		}
		kinds[strings.SplitN(class, ":", 2)[0]]++
	}
	for key := range lanePositionClass {
		if !reach[key] {
			t.Errorf("lanePositionClass names %s, which no longer reaches a position writer — stale", key)
		}
	}
	if kinds["checkpoint"] < 1 || kinds["fold"] < 1 || kinds["barrier-fold"] < 1 {
		t.Fatalf("found %v lane position writers; want at least one checkpoint, one fold writer and the barrier fold — "+
			"the walk is not finding them", kinds)
	}
	checkLaneTxBodies(t, funcs)
	var hooks []string
	for callee := range laneBody(funcs, "laneApplierAdapter.WriteCheckpoint").Calls {
		if postCommitHook.MatchString(callee) {
			hooks = append(hooks, callee)
		}
	}
	for key, class := range lanePositionClass {
		if class != "fold-core" && class != "checkpoint" && class != "barrier-fold" {
			continue
		}
		body := laneBody(funcs, key)
		calls := body.Calls
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
			t.Errorf("%s (%s) does not call its COMMIT %q (laneCommitCall)", key, class, commit)
			continue
		}
		if class == "fold-core" && !body.Before(commit, "Anchor") {
			t.Errorf("%s anchors its fold before (or without) its COMMIT %s", key, commit)
		}
	}
	slices.Sort(hooks)
	t.Logf("lane position writers %v; post-commit hooks on the checkpoint: %v", kinds, hooks)
}
