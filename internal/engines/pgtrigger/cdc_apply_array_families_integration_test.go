//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// RDS validation F3 (2026-07-16) — array element families through the
// REAL pgtrigger apply path.
//
// The trigger change payload is to_jsonb() JSONB decoded with UseNumber
// (ADR-0066 §4). Before the fix, array ELEMENT leaves skipped the
// decoder's normalization AND the postgres writer's convertArray only
// accepted the SQL-path canonical leaf types — so the FIRST array-
// bearing UPDATE/DELETE payload crash-looped the stream ("expected
// float64, got json.Number" / "got string" for the ±Infinity
// spellings), deterministically re-crashing on every restart at the
// same change-log row.
//
// Per the Bug-74 discipline this pin covers EVERY array element family
// the payload can carry — float8/float4 (incl. ±Infinity, NaN, and a
// denormal; float NEGATIVE ZERO is normalized to +0 by PG's to_jsonb
// capture itself — the named wart pinned separately by
// TestCDCApply_FloatNegativeZeroCaptureNormalization — so -0 appears
// here only in rows the matrix deletes), numeric (incl. NaN, ±Infinity, and
// digits beyond float64 precision), int, bool, text (incl. the LITERAL
// strings "Infinity"/"NaN", which must stay text — the ambiguity that
// forbids fixing this at the type-blind decode layer), timestamp,
// timestamptz, date, time, uuid — × the shape variants {values,
// NULL-element, empty array, 2-D, whole-column NULL} × {INSERT, UPDATE,
// DELETE}, driven through pgtrigger.Setup → OpenCDCReader →
// OpenChangeApplier.Apply against a real PG 16 pair, and ground-truthed
// src==dst per column (::text md5 + array_dims). One representative
// family is NOT enough: each family has its own convertArray arm and
// its own payload leaf shape.

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// arrFamTable is the one table both sides carry; every column is an
// array of a distinct element family.
const arrFamTable = "array_families"

const arrFamDDL = `
	CREATE TABLE ` + arrFamTable + ` (
		id    BIGINT PRIMARY KEY,
		f8s   double precision[],
		f4s   real[],
		nums  numeric[],
		ints  bigint[],
		bools boolean[],
		txts  text[],
		tss   timestamp[],
		tstzs timestamptz[],
		ds    date[],
		tods  time[],
		us    uuid[]
	);
`

// arrFamColumns mirrors arrFamDDL's column order for the per-column
// digest loop (the Bug-74 "name the offending family" oracle).
var arrFamColumns = []string{
	"id", "f8s", "f4s", "nums", "ints", "bools", "txts", "tss", "tstzs", "ds", "tods", "us",
}

// arrFamInserts is the INSERT leg of the op matrix; each row is one
// shape variant across every family column.
//
//	row 1: plain values — floats incl. ±Infinity/NaN/denormal/-0, numerics
//	       incl. NaN/±Infinity/high-precision, int extremes, and a text[]
//	       holding the LITERAL non-finite spellings (must stay strings).
//	row 2: NULL elements.
//	row 3: empty arrays.
//	row 4: 2-D with NULL elements (deleted later — 2-D DELETE WHERE).
//	row 5: whole-column NULLs (updated later — IS NULL WHERE).
//	row 6: 2-D survivor (kept to compare array_dims at the end).
const arrFamInserts = `
	INSERT INTO ` + arrFamTable + ` VALUES (
		1,
		ARRAY['1.5','-2.25','Infinity','-Infinity','NaN','5e-324','-0','4503599627370496']::float8[],
		ARRAY['1.5','Infinity','-Infinity','NaN','-0']::float4[],
		ARRAY['123456789.123456789012345678','NaN','Infinity','-Infinity','0.000000001','-42','2.500']::numeric[],
		ARRAY[1,-9223372036854775808,9223372036854775807]::int8[],
		ARRAY[true,false],
		ARRAY['Infinity','-Infinity','NaN','plain text','-0','null']::text[],
		ARRAY['2026-01-02 03:04:05.123456','2000-02-29 23:59:59']::timestamp[],
		ARRAY['2026-01-02 03:04:05.123456+00','1999-12-31 16:00:00-08']::timestamptz[],
		ARRAY['2026-07-16','1970-01-01']::date[],
		ARRAY['03:04:05.123456','23:59:59']::time[],
		ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11']::uuid[]
	);
	INSERT INTO ` + arrFamTable + ` VALUES (
		2,
		ARRAY[NULL,'1.25']::float8[],
		ARRAY[NULL,'2.5']::float4[],
		ARRAY[NULL,'9.99']::numeric[],
		ARRAY[NULL,7]::int8[],
		ARRAY[NULL,true]::boolean[],
		ARRAY[NULL,'x']::text[],
		ARRAY[NULL,'2026-01-02 03:04:05']::timestamp[],
		ARRAY[NULL,'2026-01-02 03:04:05+00']::timestamptz[],
		ARRAY[NULL,'2026-01-03']::date[],
		ARRAY[NULL,'12:00:00']::time[],
		ARRAY[NULL,'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22']::uuid[]
	);
	INSERT INTO ` + arrFamTable + ` VALUES (
		3,
		'{}'::float8[], '{}'::float4[], '{}'::numeric[], '{}'::int8[], '{}'::boolean[],
		'{}'::text[], '{}'::timestamp[], '{}'::timestamptz[], '{}'::date[], '{}'::time[], '{}'::uuid[]
	);
	INSERT INTO ` + arrFamTable + ` VALUES (
		4,
		ARRAY[['1.5','NaN'],['Infinity',NULL]]::float8[],
		ARRAY[['1.5',NULL],['-Infinity','NaN']]::float4[],
		ARRAY[['1.25','NaN'],[NULL,'123456789.987654321012345678']]::numeric[],
		ARRAY[[1,2],[3,NULL]]::int8[],
		ARRAY[[true,NULL],[false,true]]::boolean[],
		ARRAY[['a','Infinity'],[NULL,'b']]::text[],
		ARRAY[['2026-01-02 03:04:05',NULL],['2000-01-01 00:00:00','2026-01-02 03:04:05.123456']]::timestamp[],
		ARRAY[['2026-01-02 03:04:05+00',NULL],['2000-01-01 00:00:00+00','2026-01-02 03:04:05.123456+00']]::timestamptz[],
		ARRAY[['2026-07-16',NULL],['1970-01-01','2000-02-29']]::date[],
		ARRAY[['03:04:05',NULL],['00:00:00','23:59:59.999999']]::time[],
		ARRAY[['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',NULL],['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22','a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a33']]::uuid[]
	);
	INSERT INTO ` + arrFamTable + ` VALUES (
		5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
	);
	INSERT INTO ` + arrFamTable + ` VALUES (
		6,
		ARRAY[['6.5','NaN'],['-Infinity','-0']]::float8[],
		ARRAY[['6.5',NULL],['Infinity','NaN']]::float4[],
		ARRAY[['6.000000000000000000000001','NaN'],[NULL,'Infinity']]::numeric[],
		ARRAY[[60,61],[62,NULL]]::int8[],
		ARRAY[[false,NULL],[true,false]]::boolean[],
		ARRAY[['six','NaN'],[NULL,'-Infinity']]::text[],
		ARRAY[['2026-06-06 06:06:06.000006',NULL],['2026-06-07 00:00:00','2026-06-08 12:00:00']]::timestamp[],
		ARRAY[['2026-06-06 06:06:06.000006+00',NULL],['2026-06-07 00:00:00+03','2026-06-08 12:00:00-05']]::timestamptz[],
		ARRAY[['2026-06-06',NULL],['2026-06-07','2026-06-08']]::date[],
		ARRAY[['06:06:06.000006',NULL],['00:00:00','23:00:00']]::time[],
		ARRAY[['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a66',NULL],['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a77','a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a88']]::uuid[]
	);
`

// arrFamMutations is the UPDATE + DELETE leg. Every UPDATE's WHERE runs
// convertArray over the full BEFORE image (incl. non-finite floats —
// PG's btree float/numeric equality treats NaN = NaN as true, so the
// predicates match); the DELETEs exercise the non-finite (row 1) and
// 2-D (row 4) before images.
const arrFamMutations = `
	UPDATE ` + arrFamTable + ` SET
		f8s  = ARRAY['5e-324','NaN','2.75']::float8[],
		txts = ARRAY['updated','Infinity']::text[]
	WHERE id = 2;

	UPDATE ` + arrFamTable + ` SET
		f8s  = ARRAY['Infinity']::float8[],
		nums = ARRAY['3.14']::numeric[],
		ints = ARRAY[33]::int8[]
	WHERE id = 3;

	UPDATE ` + arrFamTable + ` SET
		f8s   = ARRAY[['5.5',NULL],['NaN','-Infinity']]::float8[],
		nums  = ARRAY['55.555']::numeric[],
		tstzs = ARRAY['2026-05-05 05:05:05+00']::timestamptz[]
	WHERE id = 5;

	UPDATE ` + arrFamTable + ` SET
		f8s = ARRAY[['66.5','NaN'],['Infinity','1e300']]::float8[]
	WHERE id = 6;

	DELETE FROM ` + arrFamTable + ` WHERE id = 1;
	DELETE FROM ` + arrFamTable + ` WHERE id = 4;
`

// TestCDCApply_ArrayElementFamilies is the F3 class pin. It drives the
// full op matrix above through the real trigger capture → poll →
// decode → apply path and asserts the target converges to the source
// byte-exactly per column.
func TestCDCApply_ArrayElementFamilies(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPGArrayFamiliesPair(t)
	defer cleanup()

	applyPGSQL(t, srcDSN, arrFamDDL)
	applyPGSQL(t, tgtDSN, arrFamDDL)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	if _, err := Setup(ctx, srcDSN, SetupOptions{Tables: []string{arrFamTable}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	e := Engine{}
	reader, err := e.OpenCDCReader(ctx, srcDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	out, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	applier, err := e.OpenChangeApplier(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	applyCtx, applyCancel := context.WithCancel(ctx)
	applyDone := make(chan error, 1)
	var applyWG sync.WaitGroup
	applyWG.Add(1)
	go func() {
		defer applyWG.Done()
		applyDone <- applier.Apply(applyCtx, "arrfam-stream", out)
	}()

	// Drive the op matrix: INSERTs, then the UPDATE/DELETE leg.
	applyPGSQL(t, srcDSN, arrFamInserts)
	applyPGSQL(t, srcDSN, arrFamMutations)

	// Drain: rows {2,3,5,6} present with row 2's UPDATE marker, rows
	// {1,4} deleted. Watch applyDone while polling — the pre-fix
	// failure shape is Apply RETURNING with the decode error (the
	// crash-loop), and that must fail this test immediately and
	// loudly, not as a drain timeout.
	deadline := time.Now().Add(90 * time.Second)
	for {
		select {
		case err := <-applyDone:
			t.Fatalf("applier exited mid-stream (the F3 crash-loop shape): %v", err)
		default:
		}
		if arrFamDrained(tgtDSN) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("target never converged: %s", arrFamDiag(t, tgtDSN))
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Stop the applier before comparing so the digest reads settled state.
	applyCancel()
	applyWG.Wait()
	if err := <-applyDone; err != nil && !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("applier.Apply returned non-cancel error: %v", err)
	}

	// Ground truth: per-column ::text md5, src vs tgt — a divergence
	// names the offending FAMILY. PG 16's float text output is
	// shortest-round-trip (injective), so text equality here is bit
	// equality, including the "-0" sign and "NaN"/"±Infinity".
	var mismatches []string
	for _, col := range arrFamColumns {
		q := fmt.Sprintf(
			"SELECT md5(COALESCE(string_agg(COALESCE(%s::text,'<NULL>'), E'\\n' ORDER BY id), '')) FROM %s",
			quoteIdent(col), arrFamTable,
		)
		if srcD, tgtD := arrFamScalar(t, srcDSN, q), arrFamScalar(t, tgtDSN, q); srcD != tgtD {
			mismatches = append(mismatches, fmt.Sprintf("column %q: src md5=%s tgt md5=%s", col, srcD, tgtD))
		}
	}
	if len(mismatches) > 0 {
		t.Fatalf("array-family divergence through the trigger apply path (Bug-74-class):\n  - %s",
			strings.Join(mismatches, "\n  - "))
	}

	// Dims ground truth on the 2-D survivor (the Bug 74 flatten shape):
	// every array column of row 6 must still be [1:2][1:2] on the target.
	for _, col := range arrFamColumns[1:] {
		q := fmt.Sprintf("SELECT COALESCE(array_dims(%s),'<NULL>') FROM %s WHERE id = 6", quoteIdent(col), arrFamTable)
		if got := arrFamScalar(t, tgtDSN, q); got != "[1:2][1:2]" {
			t.Errorf("target row 6 %s array_dims = %q; want [1:2][1:2] (silent flatten)", col, got)
		}
	}
}

// arrFamDrained reports whether the target reflects the ENTIRE op
// matrix: the two DELETEs, the UPDATE marker on row 2, and the row 5
// NULL→values UPDATE. Read failures return false so the poll retries.
func arrFamDrained(dsn string) bool {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var drained bool
	q := fmt.Sprintf(`
		SELECT
			NOT EXISTS (SELECT 1 FROM %[1]s WHERE id IN (1, 4))
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 2 AND txts[1] = 'updated')
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 5 AND nums IS NOT NULL)
			AND (SELECT count(*) FROM %[1]s) = 4
	`, arrFamTable)
	if err := db.QueryRowContext(ctx, q).Scan(&drained); err != nil {
		return false
	}
	return drained
}

// arrFamDiag renders the drain markers for a timeout message.
func arrFamDiag(t *testing.T, dsn string) string {
	t.Helper()
	ids := arrFamScalar(t, dsn, "SELECT COALESCE(string_agg(id::text, ',' ORDER BY id),'<empty>') FROM "+arrFamTable)
	return fmt.Sprintf("target ids=[%s] (want [2,3,5,6] with row-2 marker + row-5 update)", ids)
}

// arrFamScalar runs a single-string-scalar query.
func arrFamScalar(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var s sql.NullString
	if err := db.QueryRowContext(ctx, query).Scan(&s); err != nil {
		t.Fatalf("scalar query: %v\nquery: %s", err, query)
	}
	return s.String
}

// startPGArrayFamiliesPair boots one plain postgres:16 container (the
// trigger engine's whole point is not needing wal_level=logical) and
// returns source + target DSNs on two databases of it.
func startPGArrayFamiliesPair(t *testing.T) (src, tgt string, cleanup func()) {
	t.Helper()
	src, cleanup = startPGForTrigger(t)

	// Carve the target database out of the same container.
	applyPGSQL(t, src, "CREATE DATABASE arrfam_tgt")
	tgt, err := arrFamSwapDB(src, "arrfam_tgt")
	if err != nil {
		cleanup()
		t.Fatalf("build target DSN: %v", err)
	}
	return src, tgt, cleanup
}

// TestCDCApply_FloatNegativeZeroCaptureNormalization pins the NAMED
// WART documented in engine.go: PG's to_jsonb capture stores numbers as
// numeric, which has no signed zero, so a float -0 written during CDC
// is captured as 0 INSIDE the source-side trigger — sluice's payload
// never contains the sign, and the target lands +0 (scalar and array
// element alike). The pin asserts BOTH sides: the source still renders
// '-0' (proving it's a capture normalization, not an apply bug) and
// the target renders '0'. If the capture format ever changes to
// float8out text, this test flips and the engine.go note comes out.
func TestCDCApply_FloatNegativeZeroCaptureNormalization(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPGArrayFamiliesPair(t)
	defer cleanup()

	const ddl = `CREATE TABLE neg_zero (id BIGINT PRIMARY KEY, f8 float8, f8s float8[]);`
	applyPGSQL(t, srcDSN, ddl)
	applyPGSQL(t, tgtDSN, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if _, err := Setup(ctx, srcDSN, SetupOptions{Tables: []string{"neg_zero"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	e := Engine{}
	reader, err := e.OpenCDCReader(ctx, srcDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	out, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	applier, err := e.OpenChangeApplier(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	applyCtx, applyCancel := context.WithCancel(ctx)
	applyDone := make(chan error, 1)
	go func() { applyDone <- applier.Apply(applyCtx, "negzero-stream", out) }()
	defer func() {
		applyCancel()
		<-applyDone
	}()

	applyPGSQL(t, srcDSN, `INSERT INTO neg_zero VALUES (1, '-0'::float8, ARRAY['-0','1.5']::float8[]);`)

	deadline := time.Now().Add(60 * time.Second)
	for arrFamScalar(t, tgtDSN, "SELECT count(*)::text FROM neg_zero") != "1" {
		select {
		case err := <-applyDone:
			t.Fatalf("applier exited mid-stream: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("target never received the neg_zero row")
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Source keeps the sign (float8out is faithful)...
	if got := arrFamScalar(t, srcDSN, "SELECT f8::text || '|' || f8s::text FROM neg_zero"); got != "-0|{-0,1.5}" {
		t.Fatalf("source render = %q; want \"-0|{-0,1.5}\" (precondition)", got)
	}
	// ...the capture does not: the target must hold the DOCUMENTED
	// normalized +0. If this ever reads "-0|{-0,1.5}", the capture
	// became sign-faithful — celebrate, then remove the engine.go wart
	// note and fold -0 into the main matrix's surviving rows.
	if got := arrFamScalar(t, tgtDSN, "SELECT f8::text || '|' || f8s::text FROM neg_zero"); got != "0|{0,1.5}" {
		t.Fatalf("target render = %q; want \"0|{0,1.5}\" (the documented to_jsonb -0 capture normalization)", got)
	}
}

// arrFamSwapDB replaces the database-name path of a PG URI DSN
// (file-unique mirror of the pipeline tests' swap helpers).
func arrFamSwapDB(orig, newDB string) (string, error) {
	u, err := url.Parse(orig)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	if u.Path == "" || u.Path == "/" {
		return "", fmt.Errorf("DSN has no db-name path: %q", orig)
	}
	u.Path = "/" + strings.TrimPrefix(newDB, "/")
	return u.String(), nil
}
