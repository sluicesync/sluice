//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The change-log id sequence's allocation settings, against a real
// Postgres.
//
// cdc_gapfree.go's first premise is that the sequence allocates strictly
// ascending ids above zero and never re-issues one. Until 2026-08-07
// only ONE of the four settings that can break it was checked (CACHE),
// and the sharpest of the other three was asserted rather than checked:
// readChangeLogAnchor's clamp said "MIN(id) - 1 can only go negative if
// the lowest in-flight id is 0, which BIGSERIAL never allocates (it
// starts at 1)".
//
// That sentence is false of a sequence someone ALTERed, and this file
// proves it two ways. TestChangeLogAnchor_MinValueZeroStrandsTheChange
// is the DEFECT proof: it binds the two halves of the safety argument
// that were each plausible alone — Postgres really allocates id 0, and
// sluice's watermark really skips it — so neither half can quietly stop
// being true without a red test.
// TestCDCReader_RefusesSequenceConfigThatCanReissueIDs is the FIX pin,
// one cell per breaking setting.

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// changeLogSeq is the sequence `sluice trigger setup` attaches to the
// change log's id column. Named once so every cell below tampers with
// the same object the preflight reads.
const changeLogSeq = "public.sluice_change_log_id_seq"

// TestCDCReader_RefusesSequenceConfigThatCanReissueIDs pins
// [verifyChangeLogSequence] across EVERY sequence setting that can put
// an id at or below the stream's watermark — not one representative.
// Each cell tampers with exactly one setting, asserts BOTH CDC doors
// (OpenCDCReader and OpenSnapshotStream) refuse and name the offending
// value plus the remedy, then restores the setting and asserts the door
// re-opens. The restore leg is what keeps this from passing on a
// preflight that refuses everything.
func TestCDCReader_RefusesSequenceConfigThatCanReissueIDs(t *testing.T) {
	cases := []struct {
		name string
		// break/repair are ALTER SEQUENCE bodies applied to changeLogSeq.
		breakIt string
		repair  string
		// wantIn are substrings the refusal must carry: the offending
		// value, and the remedy an operator can run.
		wantIn []string
	}{
		{
			name:    "cache-hands-sessions-disjoint-ranges",
			breakIt: "CACHE 32",
			repair:  "CACHE 1",
			wantIn:  []string{"CACHE 32", "ALTER SEQUENCE"},
		},
		{
			name:    "minvalue-zero-admits-id-zero",
			breakIt: "MINVALUE 0 RESTART WITH 0",
			repair:  "MINVALUE 1 RESTART WITH 1000",
			wantIn:  []string{"MINVALUE 0", "ALTER SEQUENCE"},
		},
		{
			name:    "negative-minvalue-admits-negative-ids",
			breakIt: "MINVALUE -100 RESTART WITH -100",
			repair:  "MINVALUE 1 RESTART WITH 1000",
			wantIn:  []string{"MINVALUE -100", "ALTER SEQUENCE"},
		},
		{
			name:    "descending-increment-allocates-downward",
			breakIt: "INCREMENT BY -1",
			repair:  "INCREMENT BY 1 MINVALUE 1 RESTART WITH 1000",
			wantIn:  []string{"INCREMENT -1", "ALTER SEQUENCE"},
		},
		{
			name:    "cycle-reissues-the-whole-id-space",
			breakIt: "CYCLE",
			repair:  "NO CYCLE",
			wantIn:  []string{"CYCLE", "ALTER SEQUENCE"},
		},
	}

	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	setupCaptureTable(t, ctx, dsn, "seq_config")
	e := Engine{}

	// Positive control first: the untouched BIGSERIAL `sluice trigger
	// setup` installs must open. A preflight that refused here would
	// make every cell below vacuously green.
	assertCDCDoorOpens(t, ctx, e, dsn)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyPGSQL(t, dsn, "ALTER SEQUENCE "+changeLogSeq+" "+tc.breakIt)

			_, err := e.OpenCDCReader(ctx, dsn)
			if err == nil {
				t.Fatalf("OpenCDCReader accepted `ALTER SEQUENCE %s`; want a loud refusal — "+
					"this configuration lets a change be captured with an id at or below the "+
					"stream's watermark, where it is never emitted", tc.breakIt)
			}
			for _, want := range tc.wantIn {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal does not name %q (an operator cannot act on it): %v", want, err)
				}
			}
			if _, err := e.OpenSnapshotStream(ctx, dsn); err == nil {
				t.Errorf("OpenSnapshotStream accepted `ALTER SEQUENCE %s`; the handoff door "+
					"must refuse the same configuration the CDC door does", tc.breakIt)
			}

			applyPGSQL(t, dsn, "ALTER SEQUENCE "+changeLogSeq+" "+tc.repair)
			assertCDCDoorOpens(t, ctx, e, dsn)
		})
	}
}

// TestChangeLogAnchor_MinValueZeroStrandsTheChange is the DEFECT proof
// behind the MINVALUE arm of the preflight above, and the reason that
// arm exists rather than a comment saying BIGSERIAL starts at 1.
//
// It binds the two facts the old assertion left unbound. Fact one:
// Postgres really does allocate change-log id 0 after `ALTER SEQUENCE …
// MINVALUE 0 RESTART WITH 0` — asserted here against the real server,
// not assumed. Fact two: sluice's own watermark really does skip it —
// [readChangeLogMaxID] returns 0 for an empty change log, so the "from
// now" stream starts at 0 and [CDCReader.poll] consumes only `id > 0`.
// Either fact alone is survivable; together they are silent loss, and
// pinning them separately would have let either drift.
//
// It reaches the poll DIRECTLY rather than through OpenCDCReader,
// because the preflight now refuses that door — which is the point. If
// someone removes the MINVALUE arm, this test still passes (it is
// measuring the underlying behaviour, not the guard), so it is
// deliberately paired with the refusal cell above rather than replacing
// it.
func TestChangeLogAnchor_MinValueZeroStrandsTheChange(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	setupCaptureTable(t, ctx, dsn, "seq_zero")

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// The stream's "from now" start on an empty change log.
	start, err := readChangeLogMaxID(ctx, db, "public")
	if err != nil {
		t.Fatalf("readChangeLogMaxID: %v", err)
	}
	if start != 0 {
		t.Fatalf("readChangeLogMaxID on an empty change log = %d; want 0 (the watermark floor this test rests on)", start)
	}

	applyPGSQL(t, dsn, "ALTER SEQUENCE "+changeLogSeq+" MINVALUE 0 RESTART WITH 0")
	applyPGSQL(t, dsn, `INSERT INTO seq_zero (id, label) VALUES (1, 'stranded')`)

	// Fact one, from the server: the id really is 0.
	var gotID int64
	if err := db.QueryRowContext(ctx, `SELECT id FROM public.sluice_change_log`).Scan(&gotID); err != nil {
		t.Fatalf("read the captured change-log id: %v", err)
	}
	if gotID != 0 {
		t.Fatalf("change-log id = %d; want 0 — this test's premise is that PG allocates 0 "+
			"under MINVALUE 0, and it no longer does", gotID)
	}

	// Fact two, from sluice: the poll starting at that watermark cannot
	// see it. Give the row time to settle so a zero-event result means
	// "skipped", not "held back as not-yet-settled".
	r := &CDCReader{db: db, schema: "public", batchSize: defaultBatchSize}
	var b pollBatch
	for range 20 {
		b, err = r.poll(ctx, start)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		if len(b.events) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(b.events) != 0 {
		t.Fatalf("poll(%d) emitted %d event(s); this test's other premise is that an id at the "+
			"watermark floor is UNREACHABLE, and it no longer is — re-derive whether "+
			"verifyChangeLogSequence's MINVALUE arm is still needed", start, len(b.events))
	}
	t.Logf("PROVEN: PG allocated change-log id %d and poll(%d) emitted %d events — "+
		"the change is stranded; verifyChangeLogSequence is what stops a stream from reaching this state",
		gotID, start, len(b.events))
}

// assertCDCDoorOpens is the positive-control half of the refusal cells
// above: a well-formed change-log sequence must still open, or every
// refusal assertion is vacuously green.
func assertCDCDoorOpens(t *testing.T, ctx context.Context, e Engine, dsn string) {
	t.Helper()
	reader, err := e.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader on a well-formed change-log sequence: %v", err)
	}
	if c, ok := reader.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}
