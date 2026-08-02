// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The backfill completion gate's EVIDENCE matrix.
//
// `verifyComplete` used to authorize the contract step — the leg that
// DROPs or RENAMEs the old column — on one fact: zero rows still match
// the `--where` guard. That fact is unfalsifiable in the wrong
// direction: `CountRemaining` renders the same verbatim predicate the
// chunk UPDATE does, so a `--where` that is valid SQL but selects
// nothing counts 0 before the walk, updates 0 rows, and counts 0 after.
// The gate agreed with itself and authorized a DROP over a backfill
// that never happened.
//
// The four cases below are the contract. (a) and (d) refuse; (b) and
// (c) still authorize, because breaking them would break real
// workflows — see verifyComplete's doc comment for why --verify and
// --verify-only are deliberately asymmetric.

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// scriptedBackfillExecutor answers CountRemaining from a script and
// updates a fixed number of rows per chunk — the levers the evidence
// matrix needs (a guard that matches nothing; rows that match but are
// not updated) and that the row-backed fake cannot express.
type scriptedBackfillExecutor struct {
	counts       []int64 // successive CountRemaining answers; last repeats
	countCalls   int
	chunks       int   // how many chunks the walk yields
	rowsPerChunk int64 // affected rows each chunk reports
	execCalls    int
}

func (e *scriptedBackfillExecutor) CountRemaining(context.Context, *ir.Table, string) (int64, error) {
	i := e.countCalls
	e.countCalls++
	if i >= len(e.counts) {
		i = len(e.counts) - 1
	}
	return e.counts[i], nil
}

func (e *scriptedBackfillExecutor) NextChunkUpperBound(_ context.Context, _ *ir.Table, after []any, _ int) (upper []any, ok bool, err error) {
	done := 0
	if len(after) > 0 {
		done = int(after[0].(int64))
	}
	if done >= e.chunks {
		return nil, false, nil
	}
	return []any{int64(done + 1)}, true, nil
}

func (e *scriptedBackfillExecutor) ExecBackfillChunk(context.Context, *ir.Table, []ir.BackfillSet, string, []any, []any) (int64, error) {
	e.execCalls++
	return e.rowsPerChunk, nil
}

func (e *scriptedBackfillExecutor) BackfillStatement(*ir.Table, []ir.BackfillSet, string) (string, error) {
	return "UPDATE items SET new_col = old_col + 1 WHERE (id) > (?) AND (id) <= (?) AND (new_col IS NULL)", nil
}

func (e *scriptedBackfillExecutor) Close() error { return nil }

// scriptedBackfillEngine wires the scripted executor to the shared
// in-memory migrate-state store.
type scriptedBackfillEngine struct {
	stubEngineBase
	schema *ir.Schema
	ex     *scriptedBackfillExecutor
	store  *backfillFakeStore
}

func (e *scriptedBackfillEngine) Name() string { return "fake" }

func (e *scriptedBackfillEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return backfillFakeSchemaReader{schema: e.schema}, nil
}

func (e *scriptedBackfillEngine) OpenBackfillExecutor(context.Context, string) (ir.BackfillExecutor, error) {
	return e.ex, nil
}

func (e *scriptedBackfillEngine) OpenMigrationStateStore(context.Context, string) (ir.MigrationStateStore, error) {
	return e.store, nil
}

func newScriptedBackfiller(t *testing.T, ex *scriptedBackfillExecutor, store *backfillFakeStore) (*Backfiller, *bytes.Buffer) {
	t.Helper()
	eng := &scriptedBackfillEngine{schema: backfillTestSchema(backfillIntPK()), ex: ex, store: store}
	b := newTestBackfiller(eng)
	var out bytes.Buffer
	b.Out = &out
	return b, &out
}

// (a) A wrong predicate on a FRESH spec: valid SQL, matches nothing.
// The walk runs, updates nothing, and the post-count is 0 — the shape
// that used to print "safe to run the contract step (drop/rename the
// old column)" and hand `expand-contract` its authorization to DROP.
func TestBackfillVerify_WrongGuardMatchingNothingRefuses(t *testing.T) {
	ex := &scriptedBackfillExecutor{counts: []int64{0}, chunks: 3, rowsPerChunk: 0}
	b, out := newScriptedBackfiller(t, ex, newBackfillFakeStore())
	b.Verify = true

	_, err := b.Run(context.Background())
	wantBackfillCode(t, err, sluicecode.CodeBackfillVerifyNoEvidence)
	if !strings.Contains(err.Error(), "matched 0 rows") {
		t.Errorf("err %q should say the guard matched nothing before the walk", err)
	}
	if strings.Contains(out.String(), "safe to run the contract step") {
		t.Errorf("report %q authorized the contract step over a backfill that never happened", out.String())
	}
	if ex.execCalls == 0 {
		t.Error("the walk itself must still run — the refusal is about the AUTHORIZATION, not the work")
	}
}

// (b) A genuine completion still authorizes: rows matched, the walk
// updated them, nothing remains.
func TestBackfillVerify_GenuineCompletionStillAuthorizes(t *testing.T) {
	ex := &scriptedBackfillExecutor{counts: []int64{6, 0}, chunks: 3, rowsPerChunk: 2}
	b, out := newScriptedBackfiller(t, ex, newBackfillFakeStore())
	b.Verify = true

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Verified || res.RowsUpdated != 6 {
		t.Errorf("Verified=%v RowsUpdated=%d; want true, 6", res.Verified, res.RowsUpdated)
	}
	if !strings.Contains(out.String(), "safe to run the contract step") {
		t.Errorf("report %q missing the safe-to-contract signal", out.String())
	}
	if !strings.Contains(out.String(), "after 6 row(s) updated") {
		t.Errorf("report %q should state the evidence the authorization rests on", out.String())
	}
}

// (b') The resumed variant: this invocation walks nothing new because
// the prior run already did the work, and the resumed RowsCopied is
// the evidence. Cumulative RowsUpdated is what the gate reads, so a
// resume must not be mistaken for "no work ever happened".
func TestBackfillVerify_ResumedRunCarriesPriorWorkAsEvidence(t *testing.T) {
	store := newBackfillFakeStore()
	ex := &scriptedBackfillExecutor{counts: []int64{0}, chunks: 0, rowsPerChunk: 0}
	b, _ := newScriptedBackfiller(t, ex, store)
	b.Verify = true
	id := BackfillMigrationID(b.Table, b.Sets, b.Where)
	store.headers[id] = ir.MigrationState{MigrationID: id, Phase: backfillPhaseRunning}
	store.progress[id] = map[string]ir.TableProgress{
		"items": {State: ir.TableProgressInProgress, LastPK: []any{int64(9)}, RowsCopied: 42},
	}

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Resumed || !res.Verified || res.RowsUpdated != 42 {
		t.Errorf("Resumed=%v Verified=%v RowsUpdated=%d; want true, true, 42", res.Resumed, res.Verified, res.RowsUpdated)
	}
}

// (c) --verify-only after a real completed run still authorizes. It
// runs no walk and reads no control table, so it cannot PROVE the
// backfill happened — the honest move is to report the count as a fact
// and name what it does not show, not to refuse a legitimate
// "backfill ran yesterday, gate my contract step today" workflow (which
// `expand-contract --resume-from contract` is built on).
func TestBackfillVerifyOnly_ReportsTheCountAsAFactAndStillAuthorizes(t *testing.T) {
	ex := &scriptedBackfillExecutor{counts: []int64{0}}
	b, out := newScriptedBackfiller(t, ex, newBackfillFakeStore())
	b.VerifyOnly = true

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Verified {
		t.Error("Verified = false; --verify-only must still gate scripted contract steps")
	}
	if ex.execCalls != 0 {
		t.Errorf("execCalls = %d; --verify-only walks nothing", ex.execCalls)
	}
	if !strings.Contains(out.String(), "0 rows of") {
		t.Errorf("report %q missing the count fact", out.String())
	}
	if !strings.Contains(out.String(), "does not show a backfill ever ran") {
		t.Errorf("report %q missing the no-evidence caveat", out.String())
	}
	if strings.Contains(out.String(), "safe to run the contract step") {
		t.Errorf("report %q claims an inference --verify-only cannot make", out.String())
	}
}

// (d) Work existed and none was done: rows matched the guard when the
// walk started, the walk updated none of them, and they no longer
// match — something other than this backfill changed them, so this run
// cannot vouch for the result.
func TestBackfillVerify_WorkExistedAndNoneWasDoneRefuses(t *testing.T) {
	ex := &scriptedBackfillExecutor{counts: []int64{5, 0}, chunks: 2, rowsPerChunk: 0}
	b, _ := newScriptedBackfiller(t, ex, newBackfillFakeStore())
	b.Verify = true

	_, err := b.Run(context.Background())
	wantBackfillCode(t, err, sluicecode.CodeBackfillVerifyNoEvidence)
	if !strings.Contains(err.Error(), "5 row(s)") {
		t.Errorf("err %q should name the pre-walk match count", err)
	}
}

// (e) The completed-spec no-op inherits the completed run's recorded
// work as its evidence — and refuses when that run recorded none,
// which is the wrong-guard shape one invocation removed (run 1 walks
// with a bad guard and marks the spec complete; run 2 --verify would
// otherwise authorize the DROP on run 1's word).
func TestBackfillVerify_CompletedSpecNoOpUsesTheRecordedWork(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rowsCopied int64
		wantErr    bool
	}{
		{"completed run did real work", 17, false},
		{"completed run recorded nothing", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newBackfillFakeStore()
			ex := &scriptedBackfillExecutor{counts: []int64{0}}
			b, _ := newScriptedBackfiller(t, ex, store)
			b.Verify = true
			id := BackfillMigrationID(b.Table, b.Sets, b.Where)
			store.headers[id] = ir.MigrationState{MigrationID: id, Phase: ir.MigrationPhaseComplete}
			store.progress[id] = map[string]ir.TableProgress{
				"items": {State: ir.TableProgressComplete, RowsCopied: tc.rowsCopied},
			}

			res, err := b.Run(context.Background())
			if tc.wantErr {
				wantBackfillCode(t, err, sluicecode.CodeBackfillVerifyNoEvidence)
				return
			}
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if !res.AlreadyComplete || !res.Verified || res.RowsUpdated != tc.rowsCopied {
				t.Errorf("AlreadyComplete=%v Verified=%v RowsUpdated=%d; want true, true, %d",
					res.AlreadyComplete, res.Verified, res.RowsUpdated, tc.rowsCopied)
			}
		})
	}
}

// TestBackfillVerify_RerunBoundary pins all four evidence outcomes
// TOGETHER, so the boundary between "authorize" and "refuse" is
// readable in one place rather than inferred across five tests.
//
// The first two rows are the Bug 221 regression: an operator re-runs
// the IDENTICAL `backfill --verify` command — the normal, correct
// thing to do in cron / Airflow / CI, and the third authorizing case
// the gate's own contract promises. v0.108.0 refused it with
// SLUICE-E-BACKFILL-VERIFY-NO-EVIDENCE and told the operator no run of
// the spec had ever moved a row, because the completed walk's row
// count was silently dropped by the migrate-state codec's bare-string
// form for terminal states. Each row drives a REAL first walk through
// the store and then re-invokes an identical Backfiller against it, so
// the count crosses the serialization boundary the way it does in
// production (backfillFakeStore.Read round-trips through the codec).
//
// The last three rows are the hole the evidence gate closed; they must
// stay refusals. Pinning them here alongside the re-run makes it
// impossible to "fix" the false refusal by weakening the gate.
func TestBackfillVerify_RerunBoundary(t *testing.T) {
	for _, tc := range []struct {
		name string
		// seed prepares the store as an earlier run would have left it
		// (nil = fresh spec).
		seed func(store *backfillFakeStore, id string)
		// first is the executor script for the first invocation.
		first *scriptedBackfillExecutor
		// rerun, when true, runs a SECOND identical --verify invocation
		// (walking nothing) and scores THAT one.
		rerun         bool
		wantRefusal   bool
		wantRowsMoved int64
	}{
		{
			name:          "rerun of a completed spec that moved rows authorizes",
			first:         &scriptedBackfillExecutor{counts: []int64{6, 0}, chunks: 3, rowsPerChunk: 2},
			rerun:         true,
			wantRowsMoved: 6,
		},
		{
			name: "rerun of a RESUMED spec that moved rows authorizes",
			seed: func(store *backfillFakeStore, id string) {
				store.headers[id] = ir.MigrationState{MigrationID: id, Phase: backfillPhaseRunning}
				store.progress[id] = map[string]ir.TableProgress{
					"items": {State: ir.TableProgressInProgress, LastPK: []any{int64(9)}, RowsCopied: 42},
				}
			},
			first:         &scriptedBackfillExecutor{counts: []int64{4, 0}, chunks: 11, rowsPerChunk: 2},
			rerun:         true,
			wantRowsMoved: 46,
		},
		{
			name:        "wrong guard on a fresh spec refuses",
			first:       &scriptedBackfillExecutor{counts: []int64{0}, chunks: 3, rowsPerChunk: 0},
			wantRefusal: true,
		},
		{
			name:        "work existed and none was done refuses",
			first:       &scriptedBackfillExecutor{counts: []int64{5, 0}, chunks: 2, rowsPerChunk: 0},
			wantRefusal: true,
		},
		{
			name: "truly no-work completed spec refuses on re-run",
			seed: func(store *backfillFakeStore, id string) {
				store.headers[id] = ir.MigrationState{MigrationID: id, Phase: ir.MigrationPhaseComplete}
				store.progress[id] = map[string]ir.TableProgress{
					"items": {State: ir.TableProgressComplete},
				}
			},
			first:       &scriptedBackfillExecutor{counts: []int64{0}},
			wantRefusal: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newBackfillFakeStore()
			b, out := newScriptedBackfiller(t, tc.first, store)
			b.Verify = true
			if tc.seed != nil {
				tc.seed(store, BackfillMigrationID(b.Table, b.Sets, b.Where))
			}

			res, err := b.Run(context.Background())
			if !tc.rerun {
				if tc.wantRefusal {
					wantBackfillCode(t, err, sluicecode.CodeBackfillVerifyNoEvidence)
					return
				}
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("first run: %v", err)
			}
			if res.RowsUpdated != tc.wantRowsMoved {
				t.Fatalf("first run RowsUpdated = %d; want %d (the fixture is wrong, not the gate)",
					res.RowsUpdated, tc.wantRowsMoved)
			}

			// The IDENTICAL command again. Nothing else touched the DB.
			b2, out2 := newScriptedBackfiller(t, &scriptedBackfillExecutor{counts: []int64{0}}, store)
			b2.Verify = true
			res2, err2 := b2.Run(context.Background())
			if err2 != nil {
				t.Fatalf("re-run of the identical command: %v\n(first run reported %q)", err2, out.String())
			}
			if !res2.AlreadyComplete {
				t.Error("re-run did not take the completed-spec no-op path")
			}
			if res2.RowsUpdated != tc.wantRowsMoved {
				t.Errorf("re-run RowsUpdated = %d; want the completed run's %d carried forward",
					res2.RowsUpdated, tc.wantRowsMoved)
			}
			if !res2.Verified {
				t.Error("re-run Verified = false; the authorization must still hold")
			}
			if !strings.Contains(out2.String(), "safe to run the contract step") {
				t.Errorf("re-run report %q missing the safe-to-contract signal", out2.String())
			}
		})
	}
}

// A zero-work walk WITHOUT --verify is not a refusal — no
// authorization is being issued — but it must not read as success
// either. The evidence classification is what the WARN reports.
func TestBackfillVerify_ZeroWorkWalkWithoutVerifyStillSucceeds(t *testing.T) {
	ex := &scriptedBackfillExecutor{counts: []int64{0}, chunks: 2, rowsPerChunk: 0}
	b, _ := newScriptedBackfiller(t, ex, newBackfillFakeStore())

	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run without --verify: %v", err)
	}
	if res.RowsUpdated != 0 {
		t.Errorf("RowsUpdated = %d; want 0", res.RowsUpdated)
	}
}

// TestBackfillEvidenceShapesAreExhaustive keeps the classifier's shapes
// and their String() renderings in lock-step — an unnamed shape would
// surface as "unknown" in a refusal an operator has to act on.
func TestBackfillEvidenceShapesAreExhaustive(t *testing.T) {
	for _, ev := range []backfillEvidence{
		evidenceWalkDidWork, evidenceGuardNeverMatched, evidenceWalkDidNothing,
		evidenceCompletedRunDidNothing, evidenceCountOnly,
	} {
		if ev.String() == "unknown" {
			t.Errorf("evidence %d renders as %q", ev, ev.String())
		}
	}
	// Every shape except the two that authorize must produce a coded
	// refusal, and the two that authorize must produce none.
	b := &Backfiller{Table: "items", Where: "new_col IS NULL"}
	for ev, wantRefusal := range map[backfillEvidence]bool{
		evidenceWalkDidWork:            false,
		evidenceCountOnly:              false,
		evidenceGuardNeverMatched:      true,
		evidenceWalkDidNothing:         true,
		evidenceCompletedRunDidNothing: true,
	} {
		err := b.refuseUnevidencedVerify(ev, &BackfillResult{Table: "items"})
		if wantRefusal {
			wantBackfillCode(t, err, sluicecode.CodeBackfillVerifyNoEvidence)
		} else if err != nil {
			t.Errorf("evidence %s refused: %v", ev, err)
		}
	}
}
