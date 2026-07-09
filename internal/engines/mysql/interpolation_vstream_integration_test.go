//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0153 residual-family fidelity differential against a REAL vtgate
// (vitess/vttestserver) — the Bug-74 corollary, third codec: interpolated
// SQL text transits vtgate's OWN parser/normalizer before reaching the
// backing MySQL, an independent literal-handling layer the plain-MySQL
// matrix in interpolation_fidelity_integration_test.go cannot vouch for
// (bind-variable normalization, literal re-serialization, the `_binary`
// introducer, charset introducers). The corpus is the residual-family set
// the value-fidelity review flagged for this leg: BIT, SET, ENUM, GEOMETRY,
// JSON, DECIMAL(65,30) extremes, float negative zero, and the zero-date
// epoch substitute — written through the SAME PlanetScale-flavor batched
// writer under both statement protocols and compared cell-by-cell on the
// stored bytes read back through vtgate.
//
// Name discipline: the CI vstream job runs `-run 'TestVStream_'`
// (ci.yml Integration (vstream); enforced by check-run-filter-coverage.sh),
// so this test MUST keep the TestVStream_ prefix.

package mysql

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStream_InterpolationResidualFamilies_ByteExact writes the residual
// family corpus through vtgate under {binary, interpolation} and asserts
// byte-identical stored values, plus the absolute "-0" negative-zero pin.
func TestVStream_InterpolationResidualFamilies_ByteExact(t *testing.T) {
	mysqlDSN, _, _, cleanup := startVTTestServer(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// The harness DSN carries interpolateParams=true for its own setup
	// statements; derive the two explicit protocol variants from it.
	base := strings.Replace(mysqlDSN, "&interpolateParams=true", "", 1)
	ctlDSN := base + "&interpolateParams=false"
	itpDSN := base + "&interpolateParams=true"

	const ddlT = `CREATE TABLE %s (
		id      BIGINT NOT NULL,
		dec6530 DECIMAL(65,30) NULL,
		dbl     DOUBLE NULL,
		fl      FLOAT NULL,
		ts6     TIMESTAMP(6) NULL,
		js      JSON NULL,
		b1      BIT(1) NULL,
		b8      BIT(8) NULL,
		b64     BIT(64) NULL,
		st      SET('a','b','c d','x-y') NULL,
		en      ENUM('alpha','beta','g mma') NULL,
		geo     GEOMETRY NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
	applyDDL(t, base, fmt.Sprintf(ddlT, "vsif_ctl"))
	applyDDL(t, base, fmt.Sprintf(ddlT, "vsif_itp"))

	nullRow := func(id int64) ir.Row {
		return ir.Row{
			"id": id, "dec6530": nil, "dbl": nil, "fl": nil, "ts6": nil, "js": nil,
			"b1": nil, "b8": nil, "b64": nil, "st": nil, "en": nil, "geo": nil,
		}
	}
	with := func(id int64, over ir.Row) ir.Row {
		row := nullRow(id)
		for k, v := range over {
			row[k] = v
		}
		return row
	}
	rows := []ir.Row{
		with(1, ir.Row{"dec6530": strings.Repeat("9", 35) + "." + strings.Repeat("9", 30)}),
		with(2, ir.Row{"dec6530": "-0." + strings.Repeat("0", 29) + "1"}),
		// Float negative zero, both precisions (the '-0'-string wart's
		// vtgate leg) + the absolute pin below.
		with(3, ir.Row{"dbl": math.Copysign(0, -1), "fl": math.Copysign(0, -1)}),
		// Zero-date epoch substitute (ADR-0127 --zero-date=epoch).
		with(4, ir.Row{"ts6": zeroDateEpochValue}),
		with(5, ir.Row{"js": `{"a\\b": "c\"d\ne", "emoji": "🐘", "arr": [1, null, "x"]}`}),
		with(6, ir.Row{"b1": true, "b8": "10100101", "b64": strings.Repeat("1", 64)}),
		with(7, ir.Row{"st": []string{"a", "c d", "x-y"}, "en": "g mma"}),
		with(8, ir.Row{"st": "b,x-y", "en": "alpha"}),
		with(9, ir.Row{"geo": pointWKB(1.5, -2.25)}),
		nullRow(10),
	}

	tblCtl := readTableIR(t, ctx, base, "vsif_ctl")
	tblItp := readTableIR(t, ctx, base, "vsif_itp")
	if err := writeRowsBatched(t, ctx, ctlDSN, tblCtl, rows); err != nil {
		t.Fatalf("vtgate binary-protocol control write: %v", err)
	}
	if err := writeRowsBatched(t, ctx, itpDSN, tblItp, rows); err != nil {
		t.Fatalf("vtgate interpolation write: %v", err)
	}

	cols, ctl := snapshotTableBytes(t, base, "vsif_ctl")
	_, itp := snapshotTableBytes(t, base, "vsif_itp")
	if len(ctl) != len(rows) {
		t.Fatalf("control wrote %d rows through vtgate; want %d", len(ctl), len(rows))
	}
	compareSnapshots(t, "vtgate residual families", cols, ctl, itp)

	for _, leg := range []struct {
		name string
		snap [][]tableCell
	}{{"binary control", ctl}, {"interpolation", itp}} {
		row := findSnapRow(t, leg.snap, "3")
		assertStoredNegZero(t, "vtgate "+leg.name, cols, row, "dbl")
		assertStoredNegZero(t, "vtgate "+leg.name, cols, row, "fl")
	}

	// The FLOAT read projection (selectColumnExpr's CAST(... AS DOUBLE),
	// the full-scan display-rounding fix) must PARSE and evaluate through
	// vtgate too: a real full-scan read over the vtgate MySQL port must
	// hand back the exact float32-widened doubles, −0 sign included.
	rr, err := Engine{}.OpenRowReader(ctx, ctlDSN)
	if err != nil {
		t.Fatalf("vtgate OpenRowReader: %v", err)
	}
	defer closeIf(rr)
	ch, err := rr.ReadRows(ctx, tblCtl)
	if err != nil {
		t.Fatalf("vtgate ReadRows: %v", err)
	}
	var negZeroRow ir.Row
	for row := range ch {
		if id, _ := row["id"].(int64); id == 3 {
			negZeroRow = row
		}
	}
	if err := rr.(*RowReader).Err(); err != nil {
		t.Fatalf("vtgate ReadRows stream: %v", err)
	}
	if negZeroRow == nil {
		t.Fatal("vtgate full scan did not return row 3")
	}
	for _, col := range []string{"fl", "dbl"} {
		f, ok := negZeroRow[col].(float64)
		if !ok || f != 0 || !math.Signbit(f) {
			t.Errorf("vtgate full-scan %s = %#v (%T); want exact −0.0 through the CAST projection", col, negZeroRow[col], negZeroRow[col])
		}
	}
}
