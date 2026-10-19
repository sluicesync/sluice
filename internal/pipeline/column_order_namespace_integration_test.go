//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Review M1 — a column-order record must answer only for its own namespace.
//
// A multi-database (MySQL) or multi-schema (Postgres) stream keeps one
// column-order record set for every namespace it streams. The lookup used
// to fall back to any record with the same TABLE name, so namespace B's
// `kd` was judged by namespace A's record: B never got a record of its own
// (the attach saw A's list and skipped it), and A's record stood in for the
// "target is its own prior" fallback. The measured consequence: B's swap,
// unapplied on B's target, was accepted because A's record already held
// the new order.
//
// The cell: both namespaces cold-start; B's records are removed (what a
// stream started before the records existed presents — B quiet since);
// A takes a swap the operator applied on A's target first, which is
// accepted and records A's new order; then B takes the same swap, NOT
// applied on B's target. It must refuse COLUMN-ORDER-CHANGED with B's
// target untouched. The independent expected value is B's target itself:
// its pre-swap rows and the absence of the post-swap row.
func rsfRunCrossNamespaceCell(t *testing.T, cell twfbCell, srcExec func(ns, sql string), nsA, nsB string, swap func(ns string) string, live bool) {
	t.Helper()
	tgt := cell.tgt
	rows := func(ns string) string { return tgt.scalar(t, fmt.Sprintf("SELECT count(*) FROM %s.kd", ns)) }
	waitRows := func(ns, want string, run *twfbRun, timeout time.Duration) bool {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if rows(ns) == want {
				return true
			}
			if run != nil && run.exited() {
				return false
			}
			time.Sleep(200 * time.Millisecond)
		}
		return false
	}
	for _, ns := range []string{nsA, nsB} {
		srcExec(ns, "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT); INSERT INTO kd VALUES (1,10,20),(2,11,21),(3,12,22);")
	}
	run := startTWFBRun(cell.streamer())
	for _, ns := range []string{nsA, nsB} {
		if !waitRows(ns, "3", run, 180*time.Second) {
			t.Fatalf("the cold start never delivered %s.kd (stream: %v)", ns, run.stop(t))
		}
	}
	cell.waitStreaming(t, run)
	// The records are written when the apply phase starts, after the anchor
	// and the cold start's record writes: wait for them, not a fixed settle
	// (a multi-database MySQL cold start ran past waitStreaming's 2s, the
	// stop cancelled the catalog read, and nothing was recorded).
	countB := fmt.Sprintf("SELECT count(*) FROM sluice_cdc_column_order WHERE schema_name = '%s'", nsB)
	recorded := func() bool {
		got := tgt.scalar(t, countB)
		return got != "0" && !strings.HasPrefix(got, "<")
	}
	for deadline := time.Now().Add(60 * time.Second); !recorded() && !run.exited() && time.Now().Before(deadline); {
		time.Sleep(250 * time.Millisecond)
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the clean stop returned %v", err)
	}

	// B as a stream started before the records existed: none for B.
	if !recorded() {
		t.Fatalf("the cold start recorded no column order for %s (count %s) — the cell would not test the lookup", nsB, tgt.scalar(t, countB))
	}
	tgt.exec(t, fmt.Sprintf("DELETE FROM sluice_cdc_column_order WHERE schema_name = '%s'", nsB))
	if got := tgt.scalar(t, countB); got != "0" {
		t.Fatalf("%s's records were not removed (count %s)", nsB, got)
	}

	// A: the operator applies the swap on A's target first; the source
	// follows, and the stream accepts it and records A's new order.
	tgt.exec(t, swap(nsA))
	if live {
		run = startTWFBRun(cell.streamer())
		cell.waitStreaming(t, run)
	}
	srcExec(nsA, swap(nsA))
	srcExec(nsA, "INSERT INTO kd (id,b,c) VALUES (100,1000,2000)")
	if !live {
		run = startTWFBRun(cell.streamer())
	}
	if !waitRows(nsA, "4", run, 90*time.Second) {
		t.Fatalf("%s's swap, applied on its target, was not accepted (stream: %v)", nsA, run.stop(t))
	}
	if !live {
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("the clean stop returned %v", err)
		}
	}

	// B: the same swap, NOT applied on B's target.
	beforeB := tgt.scalar(t, fmt.Sprintf("SELECT concat(b, ',', c) FROM %s.kd WHERE id = 1", nsB))
	srcExec(nsB, swap(nsB))
	srcExec(nsB, "INSERT INTO kd (id,b,c) VALUES (100,1000,2000)")
	if !live {
		run = startTWFBRun(cell.streamer())
	}
	deadline := time.Now().Add(60 * time.Second)
	for !run.exited() && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if !run.exited() {
		_ = run.stop(t)
		t.Fatalf("%s's unapplied swap was accepted: the stream ran on (target %s.kd rows %s, row 1 b,c = %s)",
			nsB, nsB, rows(nsB), tgt.scalar(t, fmt.Sprintf("SELECT concat(b, ',', c) FROM %s.kd WHERE id = 1", nsB)))
	}
	if run.err == nil || !strings.Contains(run.err.Error(), "COLUMN-ORDER-CHANGED") {
		t.Fatalf("%s: the stream stopped, but not on COLUMN-ORDER-CHANGED: %v", nsB, run.err)
	}
	t.Logf("%s refused: %v", nsB, run.err)
	if got := rows(nsB); got != "3" {
		t.Errorf("%s.kd holds %s rows after the refusal; want the 3 it held", nsB, got)
	}
	if got := tgt.scalar(t, fmt.Sprintf("SELECT concat(b, ',', c) FROM %s.kd WHERE id = 1", nsB)); got != beforeB {
		t.Errorf("%s.kd row 1 changed from %s to %s", nsB, beforeB, got)
	}
}

// TestColumnOrderRecords_CrossNamespace_MySQLMultiDatabase: two MySQL source
// databases into a MySQL target, the swaps live at the head (a MINIMAL
// source refuses a swap met behind the head on its own, Bug 301).
func TestColumnOrderRecords_CrossNamespace_MySQLMultiDatabase(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtHomeDSN, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()
	shopDSN := mustCreateMySQLDatabase(t, srcDSN, "shop_db")
	dsns := map[string]string{"source_db": srcDSN, "shop_db": shopDSN}
	exec := func(ns, q string) { twfbDB{"mysql", dsns[ns]}.exec(t, q) }
	cell := twfbCell{
		src: twfbDB{"mysql", serverDSN(t, srcDSN)}, tgt: twfbDB{"mysql", tgtHomeDSN},
		streamID: "rsf-ns-m2m", databases: []string{"source_db", "shop_db"},
	}
	swap := func(ns string) string {
		return fmt.Sprintf("ALTER TABLE %s.kd RENAME COLUMN b TO c, RENAME COLUMN c TO b", ns)
	}
	rsfRunCrossNamespaceCell(t, cell, exec, "source_db", "shop_db", swap, true)
}

// TestColumnOrderRecords_CrossNamespace_PostgresMultiSchema: two Postgres
// source schemas into a Postgres target, each swap made while the stream
// is stopped (a Postgres reader that forwards nothing refuses a mid-session
// rename itself; a restart's first relation reaches the pipeline's check).
func TestColumnOrderRecords_CrossNamespace_PostgresMultiSchema(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	src := twfbDB{"postgres", srcDSN}
	src.exec(t, "CREATE SCHEMA sales; CREATE SCHEMA billing;")
	exec := func(ns, q string) { src.exec(t, "SET search_path TO "+ns+"; "+q) }
	cell := twfbCell{
		src: src, tgt: twfbDB{"postgres", tgtDSN},
		streamID: "rsf-ns-p2p", slotName: "rsf_ns_p2p", databases: []string{"sales", "billing"},
	}
	swap := func(ns string) string {
		return fmt.Sprintf("BEGIN; ALTER TABLE %[1]s.kd RENAME COLUMN b TO tmp; ALTER TABLE %[1]s.kd RENAME COLUMN c TO b; "+
			"ALTER TABLE %[1]s.kd RENAME COLUMN tmp TO c; COMMIT;", ns)
	}
	rsfRunCrossNamespaceCell(t, cell, exec, "sales", "billing", swap, false)
}
