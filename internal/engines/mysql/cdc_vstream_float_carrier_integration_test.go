//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// G4 (ADR-0153 round-2 fidelity-review residual): the VStream COPY / CDC
// path decodes FLOAT through its OWN carrier — Vitess hands sluice text
// bytes in query.Row values and decodeVStreamCell parses them with
// strconv.ParseFloat(text, 64) — entirely outside the SQL reader's
// `(col * 1E0)` DOUBLE-promotion projection that made the driver path
// bit-exact. This file ground-truths that carrier against a REAL vtgate
// (vitess/vttestserver), floatTortureSet corpus, both VStream legs.
//
// GROUND TRUTH (vttestserver mysql80, 2026-07-09):
//
//   - CDC (binlog row events): vttablet decodes the binlog float32 bits
//     and re-encodes them itself (vitess go/mysql/binlog/rbr.go:
//     strconv.AppendFloat(float64(f32), 'E', -1, 32) — the shortest
//     text that round-trips the float32). Result: NO float32-level
//     loss on any torture value, but the float64 carrier is "nearest
//     double to the shortest float32 decimal text", which differs
//     bitwise from the SQL reader's float64(float32(x)) on most
//     non-short values (e.g. 0.1f lands 0x3fb999999999999a vs the
//     reader's 0x3fb99999a0000000). Both narrow to the identical
//     float32, so the divergence is observable ONLY by a double-width
//     target — the G4 informational, resolved here as exactly that and
//     pinned at the float32-exactness bar below.
//
//   - COPY (cold-start snapshot): vttablet's rowstreamer issues a
//     bare-column SELECT over the text protocol, so the raw bytes are
//     mysqld's own FLOAT rendering — the 6-significant-digit display
//     rounding (the very bug class ADR-0153's `(col * 1E0)` projection
//     fixed on the SQL reader, un-fixable from the client side: the
//     SELECT is built inside vttablet). Result: REAL float32-level
//     loss on 7/11 torture values (8388608 lands 8388610; float32-max
//     lands a different float32; -123456.789 lands -123457; …).
//     Tracked as the VStream-COPY FLOAT display-rounding entry in
//     docs/dev/roadmap.md open-bugs; the float32-exactness pin below
//     is skip-guarded until that fix lands (un-skip = its Phase-A
//     failing pin). DOUBLE and float −0.0 are exact on this leg too
//     and ARE pinned.
//
// Name discipline: the CI vstream job runs `-run 'TestVStream_'`
// (ci.yml Integration (vstream); enforced by
// check-run-filter-coverage.sh), so this test MUST keep the
// TestVStream_ prefix.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// classifyFloatCarrier compares a landed float64 carrier against the
// expected float64(float32(x)) and returns one of:
//
//	"bit-exact"    — bits identical (the SQL-reader contract),
//	"carrier-bits" — different float64 bits but the SAME float32 (the
//	                 class only a double-width target could observe), or
//	"float32-loss" — a DIFFERENT float32: real precision loss.
func classifyFloatCarrier(got, want float64) string {
	if math.Float64bits(got) == math.Float64bits(want) {
		return "bit-exact"
	}
	if math.Float32bits(float32(got)) == math.Float32bits(float32(want)) {
		return "carrier-bits"
	}
	return "float32-loss"
}

// TestVStream_FloatCarrierParity streams the float32 torture corpus
// through the VStream COPY (cold-start snapshot) and CDC (binlog
// row-event) legs and classifies each landed FLOAT carrier against
// float64(float32(x)). See the file header for what each leg's wire
// bytes actually are and which assertions are live vs skip-guarded.
func TestVStream_FloatCarrierParity(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE flt_carrier (
		id  BIGINT NOT NULL,
		fl  FLOAT NULL,
		dbl DOUBLE NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	flVals := floatTortureSet()

	// Seed via the binary protocol (bound args, no interpolation) so the
	// stored float32 bits are exactly float32(flVals[i]) — the same
	// seeding discipline as the SQL-reader pin. The DOUBLE column gets
	// the identical float64 as the 64-bit-width control. −0.0 must be
	// seeded as the STRING "-0": vtgate re-serializes bind vars into
	// literals toward mysqld, and MySQL parses the float literal -0 to
	// +0 (the ADR-0153 '-0'-string writer wart); the string→float
	// conversion preserves the sign.
	seedDSN := strings.Replace(mysqlDSN, "&interpolateParams=true", "", 1) + "&interpolateParams=false"
	seed, err := sql.Open("mysql", seedDSN)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer func() { _ = seed.Close() }()
	seedArg := func(v float64) any {
		if v == 0 && math.Signbit(v) {
			return "-0"
		}
		return v
	}
	for i, v := range flVals {
		if _, err := seed.ExecContext(ctx, "INSERT INTO flt_carrier (id, fl, dbl) VALUES (?, ?, ?)",
			int64(i+1), seedArg(v), seedArg(v)); err != nil {
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}

	// Let vttestserver's async schema tracker see the table before the
	// VStream opens (the harness convention).
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorPlanetScale}

	fltTable := &ir.Table{
		Name: "flt_carrier",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "fl", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
			{Name: "dbl", Type: ir.Float{Precision: ir.FloatDouble}, Nullable: true},
		},
	}

	// checkDouble pins the 64-bit-width control on both legs: DOUBLE
	// transits both carriers bit-exactly (mysqld prints DOUBLE
	// shortest-round-trip; the binlog leg re-encodes at 64-bit width),
	// −0.0 sign included.
	checkDouble := func(leg string, id int64, row ir.Row, want float64) {
		t.Helper()
		dbl, ok := row["dbl"].(float64)
		if !ok || math.Float64bits(dbl) != math.Float64bits(want) {
			t.Errorf("%s: row %d DOUBLE control: got %#v (%T); want %v (bits %x) bit-exact",
				leg, id, row["dbl"], row["dbl"], want, math.Float64bits(want))
		}
	}
	// checkFloat asserts the FLOAT carrier at the float32-exactness bar
	// (no real precision loss; carrier-bit divergence tolerated per the
	// file header) and pins the −0.0 sign at float32 width.
	checkFloat := func(leg string, id int64, row ir.Row, want float64) {
		t.Helper()
		fl, ok := row["fl"].(float64)
		if !ok {
			t.Errorf("%s: row %d fl = %#v (%T); want float64", leg, id, row["fl"], row["fl"])
			return
		}
		if class := classifyFloatCarrier(fl, want); class == "float32-loss" {
			t.Errorf("%s: row %d FLOAT %s: got %v (bits %x), want float64(float32) %v (bits %x)",
				leg, id, class, fl, math.Float64bits(fl), want, math.Float64bits(want))
		}
		if want == 0 && math.Signbit(want) && !math.Signbit(fl) {
			t.Errorf("%s: row %d FLOAT lost the −0.0 sign: got %v", leg, id, fl)
		}
	}

	// ---- Leg 1: VStream COPY (cold-start snapshot). ----
	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()
	rowsCh, err := stream.Rows.ReadRows(ctx, fltTable)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	copyRows := map[int64]ir.Row{}
	for row := range rowsCh {
		copyRows[row["id"].(int64)] = row
	}
	if len(copyRows) != len(flVals) {
		t.Fatalf("COPY leg saw %d rows; want %d", len(copyRows), len(flVals))
	}
	for i, want := range flVals {
		id := int64(i + 1)
		checkDouble("COPY", id, copyRows[id], want)
		// −0.0 survives the COPY carrier (mysqld prints "-0"); pin it —
		// the FLOAT magnitude pin for this leg is skip-guarded below.
		if want == 0 && math.Signbit(want) {
			checkFloat("COPY", id, copyRows[id], want)
		}
	}

	t.Run("copy-float32-exact", func(t *testing.T) {
		t.Skipf("KNOWN-LOSSY: VStream COPY FLOAT display-rounding (mysqld 6-sig-digit text via vttablet's bare-column rowstreamer SELECT; 8388608 lands 8388610) — see the docs/dev/roadmap.md open-bugs entry (2026-07-09). Un-skip as the Phase-A failing pin when that fix lands.")
		for i, want := range flVals {
			checkFloat("COPY", int64(i+1), copyRows[int64(i+1)], want)
		}
	})

	// ---- Leg 2: VStream CDC (binlog row events). ----
	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
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
	// Settle window: vtgate's "current" stream takes a moment to
	// register; rows written too quickly land before the boundary.
	time.Sleep(2 * time.Second)

	for i, v := range flVals {
		if _, err := seed.ExecContext(ctx, "INSERT INTO flt_carrier (id, fl, dbl) VALUES (?, ?, ?)",
			int64(100+i+1), seedArg(v), seedArg(v)); err != nil {
			t.Fatalf("cdc insert %d: %v", i, err)
		}
	}
	got := drainVTTestChanges(t, ctx, changes, len(flVals), 60*time.Second)
	if len(got) != len(flVals) {
		t.Fatalf("CDC leg saw %d changes; want %d", len(got), len(flVals))
	}
	for _, c := range got {
		ins, ok := c.(ir.Insert)
		if !ok {
			t.Errorf("CDC leg: unexpected change %T", c)
			continue
		}
		id := ins.Row["id"].(int64) - 100
		if id < 1 || id > int64(len(flVals)) {
			t.Errorf("CDC leg: unexpected row id %d", ins.Row["id"])
			continue
		}
		want := flVals[id-1]
		checkFloat("CDC", id, ins.Row, want)
		checkDouble("CDC", id, ins.Row, want)
	}
}
