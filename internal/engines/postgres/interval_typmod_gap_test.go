// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The interval typmod does not survive the IR, and that is currently a
// DELIBERATE gap rather than an unnoticed one (audit A2-6, re-derived
// 2026-09-07).
//
// # What was measured, on real PostgreSQL 16
//
// A PG→PG `migrate` of a table declaring `interval(0)`, `interval(2)`,
// `interval day to second` and `interval year to month` produced FOUR bare
// `interval` columns on the target. The VALUES were byte-identical — bare
// `interval` is the widest interval type, and PG rounds on STORE, so every
// value had already been rounded by the source's own declaration before
// sluice read it. There is no data loss on the copy, and the LOW grade is
// right about that much.
//
// A2-6 was filed as "a forwarded ADD COLUMN drops the interval typmod". It is
// wider: [ir.Interval] is an empty struct, so the declaration is dropped at
// the READER ([translateType]) on every path — a plain migrate is what the
// measurement above used. The forwarded ADD COLUMN is one call site of
// several, not the defect.
//
// # The harm, named rather than implied
//
// It is a silent constraint loss on the target, and it arrives at CUTOVER
// rather than during the copy: an application that starts writing to the
// migrated target stores values the source's declaration would have rounded
// or discarded. `interval year to month` is the sharpest — on the source it
// discards days and time entirely; on the target it keeps them.
//
// # Why it is not fixed here, with the cost measured
//
// The fix is to carry the typmod on [ir.Interval]. The information is
// available and already read — `information_schema.datetime_precision`
// carries the precision (measured: 0 and 2 for the columns above), and
// `interval_type` carries the field range — so the reader is discarding what
// it holds.
//
// What stops it being a small change is [backup.ComputeSchemaHash]. That
// fingerprint folds the marshalled IR type, so adding a field to
// [ir.Interval] MOVES the fingerprint and repartitions every existing backup
// chain whose schema contains an interval column. The project already treats
// that cost as serious enough to deliberately EXCLUDE a field from the
// fingerprint and accept losing a bit of tamper detection rather than pay it
// (see `fingerprintIndex` and the `goldenSchemaHash` block). So carrying the
// typmod is a minor-version change that owes a chain-epoch story, not a
// tail-sweep patch — and the preflight cannot make it loud in the meantime,
// because [ir.SchemaWriter.PreflightColumnTypes] receives the IR schema,
// by which point the declaration is already gone.
//
// # What this test is for
//
// It pins the CURRENT behaviour so the gap is recorded where the next reader
// of this code will be, and so the day someone carries the typmod they get a
// deliberate failure pointing at the backup-chain cost instead of discovering
// it in the field.
func TestIntervalTypmodIsDroppedDeliberately(t *testing.T) {
	prec := func(p int64) *int64 { return &p }

	for _, tc := range []struct {
		name string
		meta columnMeta
	}{
		{"bare interval", columnMeta{DataType: "interval", UDTName: "interval", DTPrec: prec(6)}},
		{"interval(0)", columnMeta{DataType: "interval", UDTName: "interval", DTPrec: prec(0)}},
		{"interval(2)", columnMeta{DataType: "interval", UDTName: "interval", DTPrec: prec(2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := translateType(tc.meta)
			if err != nil {
				t.Fatalf("translateType: %v", err)
			}
			iv, ok := got.(ir.Interval)
			if !ok {
				t.Fatalf("interval translated to %T, want ir.Interval", got)
			}
			// The whole point: every declaration collapses to the same
			// zero value, so the precision the reader HELD in DTPrec is
			// unrecoverable downstream.
			if iv != (ir.Interval{}) {
				t.Fatalf("ir.Interval now carries state (%+v).\n"+
					"If you are adding the typmod: good — but read this test's doc first. "+
					"backup.ComputeSchemaHash folds the marshalled IR type, so this field MOVES the "+
					"schema fingerprint and repartitions every existing backup chain whose schema has "+
					"an interval column. That needs a chain-epoch story (or an explicit fingerprint "+
					"exclusion, as fingerprintIndex does), not just a reader change. Then update this "+
					"test to assert the carried value.", iv)
			}
		})
	}

	t.Run("and the emitter renders it bare", func(t *testing.T) {
		// The other half of the round trip: even given a carried typmod,
		// the emitter would have to learn to render it. Pinned so the two
		// halves cannot be fixed one at a time and read as done.
		got, err := emitColumnType(ir.Interval{}, emitOpts{})
		if err != nil {
			t.Fatalf("emitColumnType: %v", err)
		}
		if got != "INTERVAL" {
			t.Fatalf("emitColumnType(ir.Interval{}) = %q, want %q — if the emitter learned to render "+
				"a field range or precision, this test's doc needs updating too", got, "INTERVAL")
		}
	})
}
