//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestResumeTableIdentity_PgoutputRecreatedWhileStoppedRefuses pins
// RESUMED-COLD-START-CERTIFIES-RECREATED-TABLE on Postgres 16. A fast-path
// cold start is stopped in its index build after both copies landed; kx is
// dropped and re-created with different rows while stopped; the re-run must
// refuse naming kx rather than resume over the old copy. Measured before
// the fix: the resume ran (COLD-START-RESUMED) under both publications;
// under FOR ALL TABLES the target ended 1:x,2:y,9:live9 against a source of
// 7,8,9 and the next start passed, and under FOR TABLE the resumed run
// applied none of the new table's changes until the next start refused.
func TestResumeTableIdentity_PgoutputRecreatedWhileStoppedRefuses(t *testing.T) {
	for _, pub := range []string{"for-table", "for-all-tables"} {
		t.Run(pub, func(t *testing.T) {
			r := pgAnchorRig(t)
			r.stopInIndexBuildAfterCopies(r.run("k*"), 2)
			r.applySrc(`DROP TABLE kx; CREATE TABLE kx (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO kx VALUES (7, 'new7'), (8, 'new8')`)
			if pub == "for-all-tables" {
				r.applySrc(`DROP PUBLICATION IF EXISTS sluice_pub; CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
			}
			logs := captureSlog(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			err := r.run("k*").Run(ctx)
			if err == nil || !strings.Contains(err.Error(), "TABLE-IDENTITY-CHANGED") || !strings.Contains(err.Error(), "kx (copied as id") {
				t.Fatalf("a resume over a table re-created while stopped: %v; want TABLE-IDENTITY-CHANGED naming kx", err)
			}
			if strings.Contains(logs.String(), coldStartResumedMarker) {
				t.Fatalf("the refused run announced %s", coldStartResumedMarker)
			}
			if got := pgQueryOne[string](t, r.tgt, "SELECT string_agg(id||':'||e, ',' ORDER BY id) FROM kx"); got != "1:x,2:y" {
				t.Fatalf("the refused run changed the target: kx = %s; want the stopped copy 1:x,2:y untouched", got)
			}
		})
	}
}

// recreateBeforeTheAnchor re-creates kx on the source from the window
// between a cold start's copy and its anchor's id re-read, once.
func recreateBeforeTheAnchor(t *testing.T, r anchorRig) {
	t.Helper()
	fired := false
	coldStartBeforeAnchorIDsForTest = func() {
		if fired {
			return
		}
		fired = true
		r.applySrc(`DROP TABLE kx; CREATE TABLE kx (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO kx VALUES (7, 'new7');
			DROP PUBLICATION IF EXISTS sluice_pub; CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
	}
	t.Cleanup(func() { coldStartBeforeAnchorIDsForTest = nil })
}

// requireRefusedNotCertified requires err to be the anchor's
// TABLE-IDENTITY-CHANGED refusal naming kx, and the record not to carry
// kx's new id.
func requireRefusedNotCertified(t *testing.T, r anchorRig, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "TABLE-IDENTITY-CHANGED") || !strings.Contains(err.Error(), "kx (copied as id") {
		t.Fatalf("a table re-created between the copy and the anchor: %v; want TABLE-IDENTITY-CHANGED naming kx", err)
	}
	now := pgQueryOne[string](t, r.src, "SELECT 'kx'::regclass::oid::text")
	if rec := r.record(); rec.IDs["kx"] == now {
		t.Fatalf("the record certified the re-created kx's id %s: %+v", now, rec)
	}
}

// TestCopyIdentity_PgoutputRecreatedBeforeTheAnchorRefuses: the fresh cold
// start re-reads its tables' ids at the anchor (FOR ALL TABLES, the shape
// that would stream the new table onto the old copy).
func TestCopyIdentity_PgoutputRecreatedBeforeTheAnchorRefuses(t *testing.T) {
	r := pgAnchorRig(t)
	recreateBeforeTheAnchor(t, r)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	requireRefusedNotCertified(t, r, r.run("k*").Run(ctx))
}

// TestCopyIdentity_PgoutputRecreatedBeforeTheResumeAnchorRefuses: the
// resume's anchor runs after its index and constraint phases, hours after
// the identity gate on a large table; a table re-created in between refuses.
func TestCopyIdentity_PgoutputRecreatedBeforeTheResumeAnchorRefuses(t *testing.T) {
	r := pgAnchorRig(t)
	r.stopInIndexBuildAfterCopies(r.run("k*"), 2)
	recreateBeforeTheAnchor(t, r)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	requireRefusedNotCertified(t, r, r.run("k*").Run(ctx))
}

// TestResumeTableIdentity_PgoutputRenameSwapWhileStoppedRefuses: kx and kd
// swapped names (through a temporary one) while stopped; each name now holds
// the other table's id, and the resume refuses naming both.
func TestResumeTableIdentity_PgoutputRenameSwapWhileStoppedRefuses(t *testing.T) {
	r := pgAnchorRig(t)
	r.stopInIndexBuildAfterCopies(r.run("k*"), 2)
	r.applySrc(`ALTER TABLE kx RENAME TO ktmp; ALTER TABLE kd RENAME TO kx; ALTER TABLE ktmp RENAME TO kd`)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err := r.run("k*").Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "TABLE-IDENTITY-CHANGED") ||
		!strings.Contains(err.Error(), "kd (copied as id") || !strings.Contains(err.Error(), "kx (copied as id") {
		t.Fatalf("a rename swap while stopped: %v; want TABLE-IDENTITY-CHANGED naming kd and kx", err)
	}
}

// TestResumeTableIdentity_PgoutputTruncateWhileStoppedConverges: a source
// TRUNCATE keeps the oid, so the resume proceeds, and the TRUNCATE itself
// is on the slot after the anchor, so the target converges to the source.
func TestResumeTableIdentity_PgoutputTruncateWhileStoppedConverges(t *testing.T) {
	r := pgAnchorRig(t)
	r.stopInIndexBuildAfterCopies(r.run("k*"), 2)
	r.applySrc(`TRUNCATE kx; INSERT INTO kx VALUES (3, 'after')`)
	logs := captureSlog(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.run("k*").Run(ctx) }()
	deadline := time.Now().Add(2 * time.Minute)
	got := ""
	for time.Now().Before(deadline) {
		if got = pgQueryOne[string](t, r.tgt, "SELECT coalesce(string_agg(id||':'||e, ',' ORDER BY id), '') FROM kx"); got == "3:after" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil && !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("the resumed run after a source TRUNCATE: %v", err)
	}
	if !strings.Contains(logs.String(), coldStartResumedMarker) {
		t.Fatal("a resume after a source TRUNCATE did not resume")
	}
	if got != "3:after" {
		t.Fatalf("after a source TRUNCATE while stopped the target kx is %q; want the source's 3:after", got)
	}
}

// TestResumeTableIdentity_PgoutputVacuumFullWhileStoppedResumes is the
// no-false-refusal half: VACUUM FULL rewrites the table (a new relfilenode)
// and keeps its oid, so the resume still proves kx is the table it copied.
func TestResumeTableIdentity_PgoutputVacuumFullWhileStoppedResumes(t *testing.T) {
	r := pgAnchorRig(t)
	r.stopInIndexBuildAfterCopies(r.run("k*"), 2)
	r.applySrc(`VACUUM FULL kx`)
	logs := captureSlog(t)
	stopAfterTheAnchor(t, r.run("k*"))
	if !strings.Contains(logs.String(), coldStartResumedMarker) {
		t.Fatalf("a resume after VACUUM FULL did not resume:\n%s", logs.String())
	}
	r.requireTheCopyWithIDs([]string{"kd", "kx"})
}

// multiSchemaAnchorStreamer is a Postgres multi-schema fan-out over sales.w
// and billing.w, the lane whose anchor first shipped recording no ids
// (v0.163.1 pre-tag review): it never captured its copy's ids, so on a
// source whose ids are proof the anchor verified nothing and recorded none.
func multiSchemaAnchorStreamer(t *testing.T) (*Streamer, anchorRig) {
	t.Helper()
	src, tgt, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)
	applyDDL(t, src, `CREATE SCHEMA sales; CREATE SCHEMA billing;
		CREATE TABLE sales.w (id INT PRIMARY KEY); INSERT INTO sales.w VALUES (1);
		CREATE TABLE billing.w (id INT PRIMARY KEY); INSERT INTO billing.w VALUES (2);`)
	r := newAnchorRig(t, "postgres", src, tgt, func(q string) { applyDDL(t, src, q) })
	return &Streamer{
		Source: r.eng, Target: r.eng, SourceDSN: src, TargetDSN: tgt, StreamID: "rs",
		DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
	}, r
}

// TestCopyIdentity_PgoutputMultiSchemaStopAtTheAnchorRecords: the fan-out's
// anchor records each namespace's tables with their copy-time ids.
func TestCopyIdentity_PgoutputMultiSchemaStopAtTheAnchorRecords(t *testing.T) {
	s, r := multiSchemaAnchorStreamer(t)
	stopAfterTheAnchor(t, s)
	r.requireTheCopyWithIDs([]string{"billing\x00w", "sales\x00w"})
}

// TestCopyIdentity_PgoutputMultiSchemaRecreatedBeforeTheAnchorRefuses: a
// table re-created between the fan-out's copy and its anchor refuses, and
// the record does not certify the new table.
func TestCopyIdentity_PgoutputMultiSchemaRecreatedBeforeTheAnchorRefuses(t *testing.T) {
	s, r := multiSchemaAnchorStreamer(t)
	coldStartBeforeAnchorIDsForTest = func() {
		r.applySrc(`DROP TABLE sales.w; CREATE TABLE sales.w (id INT PRIMARY KEY); INSERT INTO sales.w VALUES (7);`)
	}
	t.Cleanup(func() { coldStartBeforeAnchorIDsForTest = nil })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err := s.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "TABLE-IDENTITY-CHANGED") || !strings.Contains(err.Error(), "copied as id") {
		t.Fatalf("a table re-created between the fan-out's copy and its anchor: %v; want TABLE-IDENTITY-CHANGED", err)
	}
	now := pgQueryOne[string](t, r.src, "SELECT 'sales.w'::regclass::oid::text")
	if rec := r.record(); rec.IDs["sales\x00w"] == now {
		t.Fatalf("the record certified the re-created sales.w's id %s: %+v", now, rec)
	}
}
