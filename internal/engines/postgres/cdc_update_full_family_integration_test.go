//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 92 regression pin (CRITICAL silent-UPDATE-loss).
//
// Under REPLICA IDENTITY FULL the pgoutput UpdateMessage carries EVERY
// column in its OldTuple with real data. The reader's emitUpdate used to
// pass that entire decoded OldTuple through as Update.Before, and the
// applier's buildWhereClause then built the UPDATE WHERE over every
// column — including rich types (jsonb / timestamptz / bytea /
// high-precision numeric) whose decoded->rebound text does NOT `=`-match
// the value already stored on the target after the pgoutput round-trip.
// The UPDATE matched zero rows, ADR-0010's resume-idempotent
// zero-rows-ok behaviour absorbed the miss, and the new value was
// silently dropped on the target.
//
// The fix narrows Update.Before to the identity-key columns (the exact
// symmetry the DELETE path has via filterBeforeToKeyCols), so the WHERE
// is key-only and robust to round-trip representation drift in non-key
// columns.
//
// This pin is end-to-end through the REAL CDC reader (so emitUpdate's
// narrowing is exercised, not a hand-built Before) and the REAL applier
// (so buildWhereClause + the pgoutput decode->rebind round-trip are
// exercised against a target that already holds the pre-image). It
// follows the CLAUDE.md "pin the class, not the representative" mandate:
// the value-family matrix is exercised in full — int/bigint, numeric
// (high precision), text/varchar, boolean, timestamp, timestamptz,
// bytea, AND jsonb — because the bug was invisible precisely because the
// prior FULL+UPDATE tests used only int/varchar (families whose text
// representation happens to survive the round-trip unchanged).
//
// Two UPDATE shapes are pinned per family:
//
//   - "changed": the family's own column is updated; assert the new
//     value lands on the target.
//   - "unchanged rich columns ride along": every UPDATE under FULL puts
//     ALL columns (including the untouched rich ones) into the OldTuple.
//     The UPDATE that changes a cheap column (touch_count) while leaving
//     jsonb/timestamptz/bytea/numeric untouched is the exact shape that
//     silently dropped pre-fix — the rich OLD values in the OldTuple
//     poisoned the WHERE. We assert that UPDATE lands too.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// bug92SeedDDL is applied identically to source and target: a table that
// carries one column per value family, PK on id, REPLICA IDENTITY FULL.
const bug92SeedDDL = `
	CREATE TABLE ledger (
		id          BIGINT        PRIMARY KEY,
		amt_int     INTEGER       NOT NULL,
		amt_big     BIGINT        NOT NULL,
		amt_num     NUMERIC(38,12) NOT NULL,
		note        VARCHAR(255)  NOT NULL,
		body        TEXT          NOT NULL,
		flag        BOOLEAN       NOT NULL,
		ts_naive    TIMESTAMP     NOT NULL,
		ts_tz       TIMESTAMPTZ   NOT NULL,
		blob        BYTEA         NOT NULL,
		doc         JSONB         NOT NULL,
		touch_count INTEGER       NOT NULL DEFAULT 0
	);
	ALTER TABLE ledger REPLICA IDENTITY FULL;
`

// bug92SeedRows seeds two identical rows on both source and target. Rich
// columns deliberately use values whose text representation is prone to
// round-trip drift under pgoutput (high-precision numeric trailing
// zeros, timestamptz offset normalisation, jsonb key reordering /
// whitespace normalisation, bytea hex casing).
const bug92SeedRows = `
	INSERT INTO ledger
		(id, amt_int, amt_big, amt_num, note, body, flag, ts_naive, ts_tz, blob, doc, touch_count)
	VALUES
		(1, 10, 9000000000, 12345.678901234500, 'alpha', 'body-1', true,
		 '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05.123456+05:30',
		 '\xDEADBEEF'::bytea, '{"b": 2, "a": 1, "nested": {"z": [3,2,1]}}'::jsonb, 0),
		(2, 20, 8000000000, 0.000000000001, 'beta', 'body-2', false,
		 '1999-12-31 23:59:59.999999', '1999-12-31 23:59:59.999999-08:00',
		 '\x00FF00FF'::bytea, '{"k": "v", "arr": [true, null, "x"]}'::jsonb, 0);
`

func openBug92Applier(t *testing.T, ctx context.Context, dsn string) ir.ChangeApplier {
	t.Helper()
	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	return applier
}

// TestCDCReader_UpdateUnderReplicaIdentityFull_FamilyMatrix is the Bug 92
// regression pin. See the file header for the why.
func TestCDCReader_UpdateUnderReplicaIdentityFull_FamilyMatrix(t *testing.T) {
	srcDSN, srcCleanup := newSharedPGDB(t, "source_db")
	defer srcCleanup()
	tgtDSN, tgtCleanup := newSharedPGDB(t, "target_db")
	defer tgtCleanup()

	// Source and target start congruent.
	applyPGSQL(t, srcDSN, bug92SeedDDL)
	applyPGSQL(t, srcDSN, bug92SeedRows)
	applyPGApplier(t, tgtDSN, bug92SeedDDL)
	applyPGApplier(t, tgtDSN, bug92SeedRows)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	eng := Engine{}
	rdr, err := eng.OpenCDCReader(ctx, srcDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let replication start register

	// Mutations on the source. Each UPDATE under REPLICA IDENTITY FULL
	// puts every column into the OldTuple — including the untouched rich
	// ones — which is exactly the poison the fix guards against.
	//
	// Row 1: touch the WHOLE family set in one UPDATE (every rich column
	//        changes value). Lands only if the WHERE is key-only.
	// Row 2: change ONLY a cheap column (touch_count) and leave every
	//        rich column UNCHANGED. This is the exact shape that silently
	//        dropped pre-fix: the OldTuple's unchanged rich values would
	//        be in the WHERE and fail to `=`-match the target.
	const dml = `
		UPDATE ledger SET
			amt_int     = 11,
			amt_big     = 9000000001,
			amt_num     = 99999.999999999999,
			note        = 'alpha-2',
			body        = 'body-1-updated',
			flag        = false,
			ts_naive    = '2025-06-07 08:09:10.654321',
			ts_tz       = '2025-06-07 08:09:10.654321+00:00',
			blob        = '\xCAFEBABE'::bytea,
			doc         = '{"updated": true, "n": 42, "deep": {"q": [9,8,7]}}'::jsonb,
			touch_count = 1
		WHERE id = 1;

		UPDATE ledger SET touch_count = 5 WHERE id = 2;
	`
	applyPGSQL(t, srcDSN, dml)

	got := drainChanges(t, ctx, changes, 2, 60*time.Second)
	if len(got) != 2 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 2 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 2", len(got))
	}

	// Every emitted Before must be key-only (id) — the load-bearing
	// invariant of the fix. A non-key column leaking into Before is the
	// regression.
	for i, c := range got {
		upd, ok := c.(ir.Update)
		if !ok {
			t.Fatalf("change[%d] = %T; want ir.Update", i, c)
		}
		if upd.Before == nil {
			t.Fatalf("change[%d].Before is nil under REPLICA IDENTITY FULL", i)
		}
		if len(upd.Before) != 1 {
			t.Errorf("change[%d].Before must be key-only; got %#v", i, upd.Before)
		}
		if _, ok := upd.Before["id"]; !ok {
			t.Errorf("change[%d].Before missing key column id; got %#v", i, upd.Before)
		}
	}

	// Feed the REAL emitted events into the applier pointed at the target.
	applier := openBug92Applier(t, ctx, tgtDSN)
	pumpChanges(t, ctx, applier, got)

	// Assert the target reflects EVERY family's new value. If the Bug 92
	// narrowing regressed, the rich-type UPDATEs would match zero rows and
	// these reads would still show the seed values.
	assertLedgerRow1Applied(t, tgtDSN)
	assertLedgerRow2Applied(t, tgtDSN)
}

// assertLedgerRow1Applied checks the all-families-changed UPDATE landed.
func assertLedgerRow1Applied(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		amtInt     int64
		amtBig     int64
		amtNum     string
		note       string
		body       string
		flag       bool
		tsNaive    string
		tsTZ       string
		blobHex    string
		docContain bool
		touchCount int64
	)
	err = db.QueryRowContext(ctx, `
		SELECT amt_int, amt_big, amt_num::text, note, body, flag,
		       to_char(ts_naive, 'YYYY-MM-DD HH24:MI:SS.US'),
		       to_char(ts_tz AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'),
		       encode(blob, 'hex'),
		       (doc @> '{"updated": true, "n": 42}'::jsonb),
		       touch_count
		  FROM ledger WHERE id = 1
	`).Scan(&amtInt, &amtBig, &amtNum, &note, &body, &flag,
		&tsNaive, &tsTZ, &blobHex, &docContain, &touchCount)
	if err != nil {
		t.Fatalf("read row 1: %v", err)
	}

	// int / bigint
	if amtInt != 11 {
		t.Errorf("row1 amt_int = %d; want 11 (UPDATE dropped — Bug 92 regression?)", amtInt)
	}
	if amtBig != 9000000001 {
		t.Errorf("row1 amt_big = %d; want 9000000001", amtBig)
	}
	// high-precision numeric — the family that exposed Bug 74's sibling
	// silent-loss; here it's the WHERE-source family.
	if amtNum != "99999.999999999999" {
		t.Errorf("row1 amt_num = %q; want 99999.999999999999", amtNum)
	}
	// text / varchar
	if note != "alpha-2" {
		t.Errorf("row1 note = %q; want alpha-2", note)
	}
	if body != "body-1-updated" {
		t.Errorf("row1 body = %q; want body-1-updated", body)
	}
	// boolean
	if flag {
		t.Errorf("row1 flag = %v; want false", flag)
	}
	// timestamp (naive)
	if tsNaive != "2025-06-07 08:09:10.654321" {
		t.Errorf("row1 ts_naive = %q; want 2025-06-07 08:09:10.654321", tsNaive)
	}
	// timestamptz (normalised to UTC for comparison)
	if tsTZ != "2025-06-07 08:09:10.654321" {
		t.Errorf("row1 ts_tz (UTC) = %q; want 2025-06-07 08:09:10.654321", tsTZ)
	}
	// bytea
	if blobHex != "cafebabe" {
		t.Errorf("row1 blob = %q; want cafebabe", blobHex)
	}
	// jsonb
	if !docContain {
		t.Errorf("row1 doc did not contain the updated jsonb (UPDATE dropped — Bug 92 regression?)")
	}
	if touchCount != 1 {
		t.Errorf("row1 touch_count = %d; want 1", touchCount)
	}
}

// assertLedgerRow2Applied checks the cheap-column-only UPDATE landed —
// the exact shape (unchanged rich columns ride along in the OldTuple)
// that silently dropped pre-fix.
func assertLedgerRow2Applied(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		touchCount int64
		note       string
	)
	if err := db.QueryRowContext(
		ctx,
		`SELECT touch_count, note FROM ledger WHERE id = 2`,
	).Scan(&touchCount, &note); err != nil {
		t.Fatalf("read row 2: %v", err)
	}
	if touchCount != 5 {
		t.Errorf("row2 touch_count = %d; want 5 — the cheap-column UPDATE was silently dropped because the unchanged rich columns in the FULL OldTuple poisoned the WHERE (Bug 92)", touchCount)
	}
	// Sanity: the unchanged column really is unchanged (the UPDATE didn't
	// touch note, and the row was not deleted/replaced).
	if note != "beta" {
		t.Errorf("row2 note = %q; want beta (unchanged)", note)
	}
}
