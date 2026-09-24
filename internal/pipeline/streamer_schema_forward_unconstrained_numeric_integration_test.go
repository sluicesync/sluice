// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// GC-36: a Postgres `numeric` column with no precision, added mid-stream,
// was projected from pgoutput's typmod -1 as Decimal{0,0}. The forwarded
// ALTER was then NUMERIC(0,0) on a Postgres target (refused) and
// DECIMAL(0,0) on a MySQL target — where every value the stream carried
// into the column afterwards was truncated to an integer, silently. The
// pre-existing-row gate (streamer_schema_forward_preexisting_defaults_*)
// grades the column's DEFAULT; these grade the VALUES carried after the
// ALTER, which is where the loss was largest.
//
// The independent expected value is the source's own rendering of each
// row, read back after the stream has carried it (fdDecimal on both sides
// — trailing fractional zeros only, so a truncated value still differs),
// plus the target catalog's column type, which must be what a cold-start
// migrate of the same column would have created.

// fdNumfreeValues are the carried values: a routine fraction, a 65-digit
// value at both of DECIMAL(65,30)'s limits (35 integer + 30 fractional
// digits), the smallest non-zero value at scale 30, an integer with
// trailing zeros, zero, and NULL.
var fdNumfreeValues = []string{
	"1.23456789",
	"12345678901234567890123456789012345.123456789012345678901234567890",
	"-0.000000000000000000000000000001",
	"100",
	"0",
	"NULL",
}

// fdNumfreeOverScale has 31 fractional digits — one past DECIMAL(65,30).
// It is carried as the LAST row, and graded separately per lane.
const fdNumfreeOverScale = "0.1234567890123456789012345678901"

// fdNumfreeOverScaleOnMySQL is what a MySQL target holds for it, MEASURED:
// KNOWN SILENT DEFECT, not introduced by GC-36 and not fixed by it. A
// strict-mode INSERT rounds excess FRACTIONAL digits into DECIMAL(65,30)
// with a note, not an error, so the CDC applier lands the rounded value at
// exit 0 (integer-part overflow, by contrast, is Error 1264). The
// DECIMAL(65,30) policy's up-front WARN names the limit but no row is
// refused. Pinned here so a fix — a per-row refusal — turns this red.
const fdNumfreeOverScaleOnMySQL = "0.12345678901234567890123456789"

// TestStreamer_AddColumnForward_UnconstrainedNumeric_CarriesValues_PostgresToPostgres
// grades the Postgres → Postgres lane: bare NUMERIC on the target.
func TestStreamer_AddColumnForward_UnconstrainedNumeric_CarriesValues_PostgresToPostgres(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	runNumfreeCarryLane(t, fdLane{
		name: "postgres->postgres", sourceEngine: "postgres", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdPG, tgt: fdPG,
	}, "SELECT CASE WHEN numeric_precision IS NULL AND numeric_scale IS NULL THEN 'numeric' ELSE "+
		"'numeric(' || numeric_precision || ',' || numeric_scale || ')' END FROM information_schema.columns "+
		"WHERE table_name = 'w' AND column_name = 'v'", "numeric", fdNumfreeOverScale)
}

// TestStreamer_AddColumnForward_UnconstrainedNumeric_CarriesValues_PostgresToMySQL
// grades the Postgres → MySQL lane: DECIMAL(65,30), the type migrate's
// emitter gives an unconstrained numeric (catalog Bug 69).
func TestStreamer_AddColumnForward_UnconstrainedNumeric_CarriesValues_PostgresToMySQL(t *testing.T) {
	src, _, srcCleanup := startPostgresLogical(t)
	defer srcCleanup()
	_, tgt, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()
	runNumfreeCarryLane(t, fdLane{
		name: "postgres->mysql", sourceEngine: "postgres", targetEngine: "mysql",
		sourceDSN: src, targetDSN: tgt, src: fdPG, tgt: fdMySQL,
	}, "SELECT COLUMN_TYPE FROM information_schema.COLUMNS "+
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'w' AND COLUMN_NAME = 'v'", "decimal(65,30)",
		fdNumfreeOverScaleOnMySQL)
}

// runNumfreeCarryLane forwards `ADD COLUMN v NUMERIC`, carries every
// fdNumfreeValues row plus the over-scale row, and grades them; wantOverScale
// is what the target must hold for the over-scale row.
func runNumfreeCarryLane(t *testing.T, lane fdLane, typeQuery, wantType, wantOverScale string) {
	t.Helper()
	s := fdStartStream(t, lane, lane.sourceDSN, lane.targetDSN, "test-fwd-numfree")
	defer s.cancel()
	s.waitRows(t, "cold start", fdSeedRows)

	fdExec(t, lane.src, lane.sourceDSN, "ALTER TABLE w ADD COLUMN v NUMERIC")
	// An UPDATE of a row that existed before the ALTER, then one INSERT per
	// value: the stream is ordered, so the last insert landing means every
	// earlier change has.
	fdExec(t, lane.src, lane.sourceDSN, "UPDATE w SET v = 3.14159265358979323846 WHERE id = 1")
	values := append(append([]string(nil), fdNumfreeValues...), fdNumfreeOverScale)
	for i, v := range values {
		fdExec(t, lane.src, lane.sourceDSN, fmt.Sprintf("INSERT INTO w (id, name, v) VALUES (%d, 'v%d', %s)", fdSeedRows+i+1, i, v))
	}
	lastID := fdSeedRows + len(values)
	s.waitRows(t, "carried values", lastID)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sh := fdShape{col: "v", def: "NUMERIC", fam: fdDecimal}
	read := func(d fdDialect, dsn string) map[int]string {
		db, err := sql.Open(d.driver(), dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()
		vals, err := fdColumnValues(ctx, db, d, sh, lastID)
		if err != nil {
			t.Fatalf("%s: read: %v", lane.name, err)
		}
		if d == lane.tgt {
			var got string
			if err := db.QueryRowContext(ctx, typeQuery).Scan(&got); err != nil {
				t.Fatalf("%s: read target column type: %v", lane.name, err)
			}
			if got != wantType {
				t.Errorf("%s: forwarded column type = %s; want %s — what a cold-start migrate of the same column creates", lane.name, got, wantType)
			}
		}
		return vals
	}
	srcVals := read(lane.src, lane.sourceDSN)
	tgtVals := read(lane.tgt, lane.targetDSN)
	if len(srcVals) != lastID || len(tgtVals) != lastID {
		t.Fatalf("%s: want %d rows each side; source %d, target %d", lane.name, lastID, len(srcVals), len(tgtVals))
	}
	for id := 1; id < lastID; id++ {
		if srcVals[id] != tgtVals[id] {
			t.Errorf("%s: id=%d: target holds [%s], source [%s] — the carried value did not survive the forwarded column", lane.name, id, tgtVals[id], srcVals[id])
		}
	}
	if srcVals[lastID] != fdNumfreeOverScale {
		t.Fatalf("%s: source holds [%s] for the over-scale row; want [%s]", lane.name, srcVals[lastID], fdNumfreeOverScale)
	}
	if tgtVals[lastID] != wantOverScale {
		t.Errorf("%s: over-scale row: target holds [%s], recorded [%s], source [%s] — the known behaviour changed; if the target now matches the source or the row is refused, update the pin",
			lane.name, tgtVals[lastID], wantOverScale, srcVals[lastID])
	}
	// Anti-vacuity: the grade compared real, non-integer values.
	if srcVals[1] != "3.14159265358979323846" || srcVals[fdSeedRows+1] != "1.23456789" {
		t.Fatalf("%s: source rendering is not what was written (id=1 [%s], id=%d [%s]) — the grade is not measuring the carried values",
			lane.name, srcVals[1], fdSeedRows+1, srcVals[fdSeedRows+1])
	}
	s.stop(t)
}
