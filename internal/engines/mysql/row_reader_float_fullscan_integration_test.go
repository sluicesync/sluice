//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the FLOAT full-scan display-rounding fix (ADR-0153's read
// torture matrix — the pre-existing silent-precision-loss the ADR-0153
// fidelity sweep uncovered).
//
// The bug class: MySQL does NOT round-trip single-precision FLOAT in its
// float→text conversion (a verifiably-exact stored float32 8388608 renders
// "8388610"), and every read page WITHOUT bind args travels the text
// protocol (COM_QUERY) — the arg-less full-scan ReadRows on every release,
// plus a cursor-paged read's first unbounded page. Those pages silently
// display-rounded every FLOAT column. The fix reads FLOAT columns through
// `(col * 1E0) AS col` — a version-universal DOUBLE promotion — in the
// shared projection (selectColumnExpr
// — the same seam as the Vector-A temporal CAST): the float32→double
// widening is exact and sign-preserving, and MySQL prints DOUBLE
// shortest-round-trip, so the text form is exact. DOUBLE columns need no
// detour (shortest-round-trip printing — pinned below by full-precision
// doubles surviving the text path bit-exactly).
package mysql

import (
	"context"
	"database/sql"
	"math"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// floatTortureSet is the float32 torture corpus: values chosen so any
// display-rounding or double-rounding in the read path changes the bits.
// Each entry is the float64 the IR carries for the stored float32
// (decodeFloat widens FLOAT to float64).
func floatTortureSet() []float64 {
	f32 := func(f float64) float64 { return float64(float32(f)) }
	return []float64{
		f32(8388608),  // 2^23 — the discovery value ("8388610" via text)
		f32(16777215), // 2^24-1 — last odd integer float32 holds exactly
		float64(math.MaxFloat32),
		-float64(math.MaxFloat32),
		float64(math.SmallestNonzeroFloat32), // denormal min
		1.1754943508222875e-38,               // smallest normal
		math.Copysign(0, -1),                 // −0.0 (sign bit)
		f32(0.1),                             // 0.100000001490116119…, >6 sig digits
		f32(math.Pi),
		f32(1.0 / 3.0),
		f32(-123456.789),
	}
}

// TestRowReader_FloatFullScan_ExactRoundTrip is the fix pin. Legs:
//
//  1. failing-first repro (mutation-verify): a RAW un-CAST text-protocol
//     SELECT — byte-for-byte what the pre-fix reader issued — really does
//     return a display-rounded value for 2^23 on this server;
//  2. the REAL full-scan reader (arg-less → text protocol) returns every
//     torture value bit-exactly (math.Float64bits, so −0's sign counts);
//  3. the chunked reader (bound arg → prepared/binary) agrees bit-exactly
//     with the full scan;
//  4. DOUBLE columns are bit-exact on the same paths WITHOUT any CAST
//     (MySQL prints DOUBLE shortest-round-trip — why only FLOAT needs the
//     detour), and NULLs stay NULL through the CAST.
func TestRowReader_FloatFullScan_ExactRoundTrip(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "flt_fullscan")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, `CREATE TABLE flt (
		id  BIGINT NOT NULL,
		fl  FLOAT NULL,
		dbl DOUBLE NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB;`)

	// Doubles that stress shortest-round-trip printing (full 17-digit
	// precision, extremes, denormal, −0).
	dblVals := []float64{
		0.1, math.Pi, math.MaxFloat64, -math.MaxFloat64,
		math.SmallestNonzeroFloat64, math.Copysign(0, -1), 1.0 / 3.0,
		8388608, 1e300, -1e-300, 2.2250738585072014e-308,
	}
	flVals := floatTortureSet()
	n := len(flVals)
	if len(dblVals) != n {
		t.Fatalf("corpus mismatch: %d fl vs %d dbl", n, len(dblVals))
	}

	// Seed via binary-protocol params so the stored bits are exact.
	seed, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer func() { _ = seed.Close() }()
	for i := 0; i < n; i++ {
		if _, err := seed.ExecContext(ctx, "INSERT INTO flt (id, fl, dbl) VALUES (?, ?, ?)",
			int64(i+1), flVals[i], dblVals[i]); err != nil {
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}
	// NULL leg: the CAST must pass NULL through unchanged.
	if _, err := seed.ExecContext(ctx, "INSERT INTO flt (id, fl, dbl) VALUES (?, NULL, NULL)", int64(n+1)); err != nil {
		t.Fatalf("seed null insert: %v", err)
	}

	// Leg 1 — failing-first repro: the PRE-FIX projection (bare column,
	// no args → text protocol) display-rounds 2^23. If a future MySQL
	// starts printing FLOAT round-trip, this leg fails and the CAST
	// detour can be retired — that is the desired loud signal.
	var raw sql.NullFloat64
	if err := seed.QueryRowContext(ctx, "SELECT fl FROM flt WHERE id = 1").Scan(&raw); err != nil {
		t.Fatalf("raw repro select: %v", err)
	}
	if !raw.Valid || raw.Float64 == float64(float32(8388608)) {
		t.Errorf("pre-fix repro: bare text-protocol SELECT of stored float32 8388608 returned %v — expected the server's display-rounding (the bug class this fix exists for); if MySQL now prints FLOAT round-trip, the (col * 1E0) detour in selectColumnExpr can be retired", raw.Float64)
	}

	// Leg 2 — the REAL full-scan reader (arg-less → text protocol + CAST).
	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)
	tbl := readTableIR(t, ctx, dsn, "flt")
	ch, err := rr.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	fullScan := map[int64]ir.Row{}
	for row := range ch {
		fullScan[row["id"].(int64)] = row
	}
	if err := rr.(*RowReader).Err(); err != nil {
		t.Fatalf("ReadRows stream: %v", err)
	}
	if len(fullScan) != n+1 {
		t.Fatalf("full scan read %d rows; want %d", len(fullScan), n+1)
	}
	checkRow := func(path string, row ir.Row, i int) {
		t.Helper()
		fl, ok := row["fl"].(float64)
		if !ok || math.Float64bits(fl) != math.Float64bits(flVals[i]) {
			t.Errorf("%s: row %d fl = %v (%T, bits %x); want %v (bits %x)",
				path, i+1, row["fl"], row["fl"], math.Float64bits(fl), flVals[i], math.Float64bits(flVals[i]))
		}
		dbl, ok := row["dbl"].(float64)
		if !ok || math.Float64bits(dbl) != math.Float64bits(dblVals[i]) {
			t.Errorf("%s: row %d dbl = %v (%T, bits %x); want %v (bits %x)",
				path, i+1, row["dbl"], row["dbl"], math.Float64bits(dbl), dblVals[i], math.Float64bits(dblVals[i]))
		}
	}
	for i := 0; i < n; i++ {
		checkRow("full scan (text)", fullScan[int64(i+1)], i)
	}
	if nr := fullScan[int64(n+1)]; nr["fl"] != nil || nr["dbl"] != nil {
		t.Errorf("full scan NULL leg: fl=%#v dbl=%#v; want NULLs through the CAST", nr["fl"], nr["dbl"])
	}

	// Leg 3 — the chunked reader (bound arg → prepared/binary + CAST)
	// must agree bit-exactly with the full scan.
	bch, err := rr.(*RowReader).ReadRowsBatch(ctx, tbl, []any{int64(0)}, n+2)
	if err != nil {
		t.Fatalf("ReadRowsBatch: %v", err)
	}
	chunked := map[int64]ir.Row{}
	for row := range bch {
		chunked[row["id"].(int64)] = row
	}
	if err := rr.(*RowReader).Err(); err != nil {
		t.Fatalf("ReadRowsBatch stream: %v", err)
	}
	if len(chunked) != n+1 {
		t.Fatalf("chunked read %d rows; want %d", len(chunked), n+1)
	}
	for i := 0; i < n; i++ {
		checkRow("chunked (binary)", chunked[int64(i+1)], i)
	}
	if nr := chunked[int64(n+1)]; nr["fl"] != nil || nr["dbl"] != nil {
		t.Errorf("chunked NULL leg: fl=%#v dbl=%#v; want NULLs through the CAST", nr["fl"], nr["dbl"])
	}
}
