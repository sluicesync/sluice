//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestCDCReader_ExcludedTableTinyint1OutOfRangeDoesNotHaltTheStream is
// audit 2026-09-15 A0915-ARCH-MEDIUM-3's failing input on a real mysqld: a sync that
// EXCLUDES a legacy table whose TINYINT(1) column holds a value outside
// {0,1}. The TINYINT(1) preflight is in-scope-only, so the excluded table
// passed it clean; before the scope gate the first row of that table then
// halted CDC with SLUICE-E-VALUE-TINYINT1-RANGE — a working configuration
// refused, under a remedy that could not name --exclude-table because the
// flag would not have worked.
//
// The reader is driven with the pipeline's own predicate shape (the
// closure [Streamer.wireCDCScopePredicate] installs matches on the
// unqualified table name), so this is the end-to-end shape short of the
// pipeline itself, which lives one package up. The independent evidence
// is the IN-SCOPE row: it must ARRIVE after the excluded row was written,
// which a dead stream cannot deliver. The control arm drives the identical
// value through an INCLUDED table and requires the refusal — so the
// excluded arm is green because of the gate, not because the value
// stopped being refused.
func TestCDCReader_ExcludedTableTinyint1OutOfRangeDoesNotHaltTheStream(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE a3_legacy_flags (
			id     BIGINT     NOT NULL,
			status TINYINT(1) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB;
		CREATE TABLE a3_orders (
			id   BIGINT      NOT NULL,
			note VARCHAR(32) NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB;
	`)
	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	open := func(t *testing.T, allowed func(schema, table string) bool) (*CDCReader, <-chan ir.Change) {
		t.Helper()
		rdr, err := eng.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		cdc := rdr.(*CDCReader)
		t.Cleanup(func() { _ = cdc.Close() })
		if allowed != nil {
			cdc.SetCDCScopePredicate(allowed)
		}
		changes, err := cdc.StreamChanges(ctx, ir.Position{})
		if err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}
		// The syncer registers asynchronously; the same settle the spine
		// test uses so the first write is not ahead of the registration.
		time.Sleep(200 * time.Millisecond)
		return cdc, changes
	}

	t.Run("excluded table: the stream lives and the in-scope row arrives", func(t *testing.T) {
		cdc, changes := open(t, func(_, table string) bool { return table != "a3_legacy_flags" })

		applyMySQL(t, dsn, `
			INSERT INTO a3_legacy_flags (id, status) VALUES (1, 2);
			INSERT INTO a3_orders (id, note) VALUES (1, 'after the excluded row');
		`)
		got := drainChanges(t, ctx, changes, 1, 30*time.Second)
		if len(got) != 1 {
			t.Fatalf("got %d changes; want 1 (the a3_orders insert). Stream error: %v — a TINYINT(1)=2 in a "+
				"table the sync EXCLUDES halted the stream (audit 2026-09-15 A0915-ARCH-MEDIUM-3)", len(got), cdc.Err())
		}
		ins, ok := got[0].(ir.Insert)
		if !ok || ins.Table != "a3_orders" {
			t.Fatalf("change[0] = %T %v; want the a3_orders Insert — the excluded table must emit nothing", got[0], got[0])
		}
		if err := cdc.Err(); err != nil {
			t.Fatalf("stream error after the in-scope row arrived: %v", err)
		}
	})

	t.Run("control — included table: the same value still refuses", func(t *testing.T) {
		cdc, changes := open(t, func(string, string) bool { return true })

		applyMySQL(t, dsn, `INSERT INTO a3_legacy_flags (id, status) VALUES (2, 3);`)
		// The pump closes the channel when it dies; drainChanges returns on
		// close or on the timeout — either way the verdict is Err().
		got := drainChanges(t, ctx, changes, 1, 30*time.Second)
		err := cdc.Err()
		ce, coded := sluicecode.FromError(err)
		if err == nil || !coded || ce.Code != sluicecode.CodeValueTinyint1Range {
			t.Fatalf("an INCLUDED table's TINYINT(1)=3 must still halt the stream with %s; got %d change(s) and "+
				"err=%v — if this passes, the excluded arm above is vacuous", sluicecode.CodeValueTinyint1Range, len(got), err)
		}
	})
}
