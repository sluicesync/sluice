// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

// TestCapturedValueExpr_RealRenderRoundTripsExactly is the per-PR gate on the
// REAL arm of [CapturedValueExpr]: every float64 rendered by the capture/
// projection expression on the BUNDLED SQLite (modernc — the library behind
// every local sqlite read, the staged-D1 file, and the local trigger poller)
// must parse back to the bit-identical double. This is the gate the original
// `format('%.17g', …)` arm never had: SQLite's printf CAPS float output at 16
// significant digits unless the `!` alternate-form-2 flag lifts it (a
// longstanding printf behaviour, NOT a 3.43 change — mechanism corrected
// 2026-08-23), so `%.17g` silently clamped (0.30000000000000004 → "0.3") and
// the expression shipped lossy for every REAL on the D1 reader, D1 staging,
// and both trigger-CDC capture lanes until the 2026-08-22 adversarial-corpus
// sweep caught it.
//
// Scope (per the gate-enumeration rule): this measures the BUNDLED SQLite
// only. Real Cloudflare D1 is measured by the d1verify adversarial matrix
// (r_17 / r_third) AND probed at every trigger-CDC stream open by the
// render-fidelity door; the third-party-app-library residual is named at
// [CapturedValueExpr].
func TestCapturedValueExpr_RealRenderRoundTripsExactly(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `CREATE TABLE gate (r REAL)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	// The adversarial pool: every value class that has bitten a float
	// renderer, plus deterministic random bit patterns (the class, not a
	// representative). NaN is excluded (SQLite stores it as NULL); ±Inf are
	// asserted separately below (they render as Inf/-Inf text, parsed by
	// the client decode).
	pool := []float64{
		0.30000000000000004,   // format('%.Ng') renders "0.3" — the headline
		0.1234567890123456789, // 17-sig value, rendered 16-digit by %.17g
		19.99, 1234567.89, 0.1, 1.0 / 3.0, math.Pi,
		5e-324,                  // smallest subnormal
		2.2250738585072014e-308, // smallest normal
		1.7976931348623157e308,  // max double
		9007199254740993.0,      // 2^53 + 1 as a REAL (stored rounded — must still round-trip)
		1e16 + 1,
	}
	rng := rand.New(rand.NewSource(20260822))
	for len(pool) < 2000 {
		v := math.Float64frombits(rng.Uint64())
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		pool = append(pool, v)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	ins, err := tx.PrepareContext(ctx, `INSERT INTO gate (r) VALUES (?)`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer func() { _ = ins.Close() }()
	for _, v := range pool {
		if _, err := ins.ExecContext(ctx, v); err != nil {
			t.Fatalf("insert %v: %v", v, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Render every row through the PRODUCTION expression and re-parse with
	// the client-side decoder's parser (strconv, exactly what d1StorageValue
	// runs); require bit identity. The stored double is read back alongside
	// as the independent expected value (modernc returns the raw 8-byte
	// REAL, no text hop).
	q := "SELECT r, " + CapturedValueExpr(`"r"`) + " FROM gate"
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var n, bad int
	for rows.Next() {
		var stored float64
		var rendered string
		if err := rows.Scan(&stored, &rendered); err != nil {
			t.Fatalf("scan: %v", err)
		}
		n++
		parsed, perr := strconv.ParseFloat(rendered, 64)
		if perr != nil || math.Float64bits(parsed) != math.Float64bits(stored) {
			bad++
			if bad <= 10 {
				t.Errorf("REAL render is not round-trip exact: stored bits %016x rendered %q parsed bits %016x (err=%v)",
					math.Float64bits(stored), rendered, math.Float64bits(parsed), perr)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if bad > 0 {
		t.Fatalf("%d of %d doubles do not round-trip through CapturedValueExpr's REAL arm — every REAL on the "+
			"D1 reader, D1 staging, and both trigger-CDC lanes would be silently altered", bad, n)
	}
	if n != len(pool) {
		t.Fatalf("swept %d rows; want %d (the pool shrank — anti-vacuity)", n, len(pool))
	}

	// ±Inf arm: representable in SQLite REAL; the render must be text the
	// client parser maps back to the same infinity.
	for _, lit := range []string{"9e999", "-9e999"} {
		var rendered string
		if err := db.QueryRowContext(ctx,
			"SELECT "+CapturedValueExpr("CAST("+lit+" AS REAL)")).Scan(&rendered); err != nil {
			t.Fatalf("inf render %s: %v", lit, err)
		}
		parsed, perr := strconv.ParseFloat(rendered, 64)
		if perr != nil || !math.IsInf(parsed, int(math.Copysign(1, parsed))) ||
			math.Signbit(parsed) != strings.HasPrefix(lit, "-") {
			t.Errorf("inf render %s -> %q -> %v (err=%v); want the same signed infinity", lit, rendered, parsed, perr)
		}
	}

	// NEGATIVE CONTROL (anti-vacuity for the sweep itself): the OLD
	// expression's renderer must still FAIL the headline value on this
	// SQLite — if format('%.17g', 0.30000000000000004) starts round-tripping,
	// SQLite changed its float rendering again and both this gate and the
	// CapturedValueExpr doc must be revisited with fresh measurements.
	var oldRender string
	if err := db.QueryRowContext(ctx, `SELECT format('%.17g', 0.30000000000000004)`).Scan(&oldRender); err != nil {
		t.Fatalf("negative control: %v", err)
	}
	if p, perr := strconv.ParseFloat(oldRender, 64); perr == nil && p == 0.30000000000000004 {
		t.Fatalf("negative control: format('%%.17g', 0.30000000000000004) now renders %q, which ROUND-TRIPS — "+
			"SQLite's float rendering changed again; re-measure and update CapturedValueExpr's doc + this gate", oldRender)
	}
}

// TestRealRenderProbe_GradesTheConnectedEngine pins the CDC-open
// render-fidelity probe in BOTH directions on the bundled SQLite: the
// production probe SQL ([RealRenderProbeSQL]) renders losslessly and is
// ACCEPTED by [VerifyRealRenderProbe], and the same expression with the `!`
// flag stripped — exactly the render an engine that ignores the flag would
// produce, per the measured 16-digit clamp — is REJECTED. This is the
// simulated-old-SQLite half the door's safety argument needs: a real
// pre-`!` engine cannot be booted here, but its render shape can be
// produced exactly (precision above the cap is clamped, so `%.20g` without
// `!` is byte-what-it-would-emit).
func TestRealRenderProbe_GradesTheConnectedEngine(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	var good string
	if err := db.QueryRowContext(ctx, RealRenderProbeSQL()).Scan(&good); err != nil {
		t.Fatalf("probe render: %v", err)
	}
	if err := VerifyRealRenderProbe(good); err != nil {
		t.Fatalf("the probe must ACCEPT the bundled SQLite's lossless render %q; got: %v", good, err)
	}

	// Simulated flag-ignoring engine: strip `!` from the production probe SQL
	// and render on the same engine — precision above the 16-digit cap clamps,
	// which is exactly what `%!.20g` produces where `!` is not honoured.
	lossySQL := strings.ReplaceAll(RealRenderProbeSQL(), "%!.20g", "%.20g")
	if lossySQL == RealRenderProbeSQL() {
		t.Fatal("anti-vacuity: stripping the `!` flag did not change the probe SQL")
	}
	var lossy string
	if err := db.QueryRowContext(ctx, lossySQL).Scan(&lossy); err != nil {
		t.Fatalf("lossy render: %v", err)
	}
	if lossy == good {
		t.Fatalf("anti-vacuity: the flag-stripped render %q matches the lossless one — the simulation is not simulating", lossy)
	}
	if err := VerifyRealRenderProbe(lossy); err == nil {
		t.Fatalf("the probe ACCEPTED the flag-stripped (16-digit-clamped) render %q — the door would miss an engine that ignores `!`", lossy)
	}
}

// TestCapturedValueExpr_Shape pins the non-REAL arms' spelling so the shared
// expression cannot drift by accident (the trigger bodies embed it verbatim).
func TestCapturedValueExpr_Shape(t *testing.T) {
	got := CapturedValueExpr(`NEW."c"`)
	want := `CASE typeof(NEW."c") WHEN 'blob' THEN hex(NEW."c") WHEN 'real' THEN format('%!.20g', NEW."c") ELSE CAST(NEW."c" AS TEXT) END`
	if got != want {
		t.Fatalf("CapturedValueExpr rendering drifted:\n got %s\nwant %s", got, want)
	}
}
