// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The CDCPOS-1 pins (audit 2026-08-11): `XA START` is a QueryEvent that is
// neither BEGIN nor COMMIT, so it used to fall through to the generic-DDL
// arm and fold the body group's GTID EARLY — before any of the body's rows —
// re-opening the exact mid-transaction position shape item 132 closed; and
// the rows themselves cannot be applied faithfully at all (invisible on the
// source until XA COMMIT; a coordinator rollback fabricates them on the
// target). The XA arm now refuses IN-SCOPE rows loudly and keeps
// out-of-scope XA traffic on the non-lossy one-group-late fold. These are
// the synthetic halves over the same dispatch-only harness the item-132
// staging pins use; the real-server halves live in
// cdc_reader_xa_integration_test.go.

func dispatchOne(t *testing.T, r *CDCReader, ev *replication.BinlogEvent, out chan ir.Change, what string) {
	t.Helper()
	if err := r.dispatch(context.Background(), ev, out); err != nil {
		t.Fatalf("dispatch %s: %v", what, err)
	}
}

// TestXA_InScopeRowRefusesLoudly: GTID → XA START → in-scope row must refuse
// with SLUICE-E-CDC-XA-UNSUPPORTED naming the table. RED pre-fix: the row
// dispatched normally, carrying a position that already contained the body
// group's GTID.
func TestXA_InScopeRowRefusesLoudly(t *testing.T) {
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
	out := make(chan ir.Change, 8)

	dispatchOne(t, r, mysqlGTIDEvent(t, 6), out, "GTID :6")
	dispatchOne(t, r, queryEvent("XA START 'x1'"), out, "XA START")

	err := r.dispatch(context.Background(), insertRowEvent(1), out)
	if err == nil {
		t.Fatal("an in-scope row inside an XA transaction dispatched without error — pre-fix this delivered a " +
			"row that is invisible on the source until XA COMMIT, with a mid-XA position (CDCPOS-1)")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCXAUnsupported {
		t.Fatalf("refusal should carry %s structurally; got: %v", sluicecode.CodeCDCXAUnsupported, err)
	}
	if !strings.Contains(err.Error(), "app.users") {
		t.Errorf("refusal should name the table for the operator; got: %v", err)
	}
}

// TestXA_OutOfScopeBodyFoldsLateNeverEarly: the body group's GTID must stay
// STAGED through XA START/END (pre-fix it folded at XA START — the loss
// direction, and XA END was the same bug's second door) and fold one group
// late, the documented non-lossy degradation the stageGTID comment already
// promised for unmodeled terminators.
func TestXA_OutOfScopeBodyFoldsLateNeverEarly(t *testing.T) {
	const bodyTx = stagingUUID + ":6"
	const nextTx = stagingUUID + ":7"
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
	out := make(chan ir.Change, 8)

	dispatchOne(t, r, mysqlGTIDEvent(t, 6), out, "GTID :6")
	dispatchOne(t, r, queryEvent("XA START 'x1'"), out, "XA START")

	// Mid-body: the GTID must still be STAGED, not folded. Pre-fix the
	// generic-DDL arm folded it here. (White-box on the same package's
	// state, exactly like the harness pre-fills it.)
	if r.pendingGTID == "" {
		t.Fatal("XA START folded the body group's GTID early — the item-132 loss shape re-opened (CDCPOS-1)")
	}
	if containsGTID(t, FlavorVanilla, r.gtidSet.String(), bodyTx) {
		t.Fatalf("running set %q already contains the XA body %s mid-body — a position built here skips the "+
			"body's tail on resume", r.gtidSet.String(), bodyTx)
	}

	dispatchOne(t, r, queryEvent("XA END 'x1'"), out, "XA END")
	if containsGTID(t, FlavorVanilla, r.gtidSet.String(), bodyTx) {
		t.Fatal("XA END folded the body GTID early — the same loss direction via the second door")
	}

	// Next group: an ordinary transaction. Its staging folds the XA body
	// GTID one group late (non-lossy), and the ordinary staging contract
	// holds: the row excludes ITS OWN transaction while including the
	// late-folded body.
	got := dispatchAll(
		t, r,
		mysqlGTIDEvent(t, 7),
		queryEvent("BEGIN"),
		insertRowEvent(2),
		xidEvent(),
	)
	if len(got) != 3 { // TxBegin, Insert, TxCommit
		t.Fatalf("ordinary transaction after XA emitted %d changes; want 3", len(got))
	}
	row := got[1]
	assertIncludes(t, FlavorVanilla, row, bodyTx, "post-XA row (late fold of the body)")
	assertExcludes(t, FlavorVanilla, row, nextTx, "post-XA row (own transaction)")
	assertIncludes(t, FlavorVanilla, got[2], nextTx, "post-XA TxCommit")
}

// TestXA_TerminalVerbsFoldOwnGroupAndReleaseTheWindow: XA COMMIT rides its
// own GTID group and must fold it (it IS the group's only statement), and an
// in-scope row AFTER the XA window must dispatch normally — the refusal
// window must not wedge open over innocent traffic.
func TestXA_TerminalVerbsFoldOwnGroupAndReleaseTheWindow(t *testing.T) {
	const commitTx = stagingUUID + ":8"
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
	out := make(chan ir.Change, 8)

	dispatchOne(t, r, mysqlGTIDEvent(t, 6), out, "GTID :6")
	dispatchOne(t, r, queryEvent("XA START 'x1'"), out, "XA START")
	dispatchOne(t, r, queryEvent("XA END 'x1'"), out, "XA END")
	dispatchOne(t, r, mysqlGTIDEvent(t, 8), out, "GTID :8")
	dispatchOne(t, r, queryEvent("XA COMMIT 'x1'"), out, "XA COMMIT")

	if !containsGTID(t, FlavorVanilla, r.gtidSet.String(), commitTx) {
		t.Fatalf("XA COMMIT's own group %s did not fold; running set %q — the terminal verb IS the group's "+
			"only statement and keeps the implicit-commit fold semantics", commitTx, r.gtidSet.String())
	}

	got := dispatchAll(
		t, r,
		mysqlGTIDEvent(t, 9),
		queryEvent("BEGIN"),
		insertRowEvent(3),
		xidEvent(),
	)
	if len(got) != 3 {
		t.Fatalf("post-XA ordinary transaction emitted %d changes; want 3 — the XA refusal window wedged open "+
			"over innocent traffic", len(got))
	}
}

// TestXAStatementVerb pins the recogniser: leading-keyword only, case- and
// whitespace-insensitive, xid opaque; ordinary statements can never match.
func TestXAStatementVerb(t *testing.T) {
	yes := map[string]string{
		"XA START 'x1'":              "START",
		"xa start 'x1'":              "START",
		"  XA\tBEGIN 'x1'":           "BEGIN",
		"XA END 'x1'":                "END",
		"XA COMMIT 'x1' ONE PHASE":   "COMMIT",
		"xa rollback 'x1'":           "ROLLBACK",
		"XA PREPARE 'x1'":            "PREPARE",
		"XA\nRECOVER":                "RECOVER",
		"XA COMMIT 'XA START trick'": "COMMIT",
	}
	for q, want := range yes {
		verb, isXA := xaStatementVerb(q)
		if !isXA || verb != want {
			t.Errorf("xaStatementVerb(%q) = (%q, %v); want (%q, true)", q, verb, isXA, want)
		}
	}
	no := []string{
		"BEGIN",
		"XAVIER_TABLE_UPDATE",
		"CREATE TABLE xa (id INT)",
		"UPDATE xa SET v = 'XA START'",
		"XA",
		"XA ",
		"XA 'no verb'",
	}
	for _, q := range no {
		if verb, isXA := xaStatementVerb(q); isXA {
			t.Errorf("xaStatementVerb(%q) = (%q, true); want no match — an over-match would suppress the "+
				"generic-DDL fold for a non-XA statement", q, verb)
		}
	}
}
