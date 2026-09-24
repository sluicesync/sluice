//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (i) on real servers: an ENUM/SET label holding a character outside
// the Basic Multilingual Plane is written as '?' by every catalog surface
// (information_schema, SHOW CREATE TABLE), while the binlog carries only
// the label's index / bitmask — so before this pin the CDC decode mapped a
// row holding `😀b` to the catalog's `?b` at exit 0 (MEASURED on MySQL
// 8.0.46: MySQL → PG `?b` / `{?,y}`, MySQL → MySQL 3F62 / 3F2C79).
//
// # The independent expected value
//
// The label the TEST itself inserted, as a Go string literal — never a
// catalog read, which is what loses the character. Under
// binlog_row_metadata=FULL the decoded value must equal that literal
// (which also measures the premise the recovery rests on: that the
// TABLE_MAP's optional metadata carries the true label); under the
// default MINIMAL it must refuse with ENUM-LABEL-NOT-RECOVERABLE. The
// control table (BMP-only labels, including a non-ASCII 'é') must decode
// exactly on both, and a label the catalog kept must decode on the lossy
// table too — the guard refuses only a row that USES a lost label.

package mysql

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

const enumLabelLossDDL = `
	CREATE TABLE lossy (
		id INT NOT NULL,
		e  ENUM('😀b','x','é') CHARACTER SET utf8mb4 NULL,
		s  SET('😀','y','é')   CHARACTER SET utf8mb4 NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB;
	CREATE TABLE genuine (
		id INT NOT NULL,
		e  ENUM('?z','w') CHARACTER SET utf8mb4 NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB;
	CREATE TABLE ctl (
		id INT NOT NULL,
		e  ENUM('x','é') CHARACTER SET utf8mb4 NULL,
		s  SET('y','é')  CHARACTER SET utf8mb4 NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB;`

// enumLabelLossLane streams from dsn and inserts, in order: the BMP control
// rows, a lossy-table row that uses only kept labels, a genuine-'?' row,
// then the row holding the lost labels. full says whether the server runs
// binlog_row_metadata=FULL.
func enumLabelLossLane(t *testing.T, dsn string, flavor Flavor, full bool) {
	t.Helper()
	applyMySQL(t, dsn, enumLabelLossDDL)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	rdr, err := Engine{Flavor: flavor}.OpenCDCReader(ctx, dsn)
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
	time.Sleep(300 * time.Millisecond)

	insertRow := func(stmt string) {
		t.Helper()
		applyMySQL(t, dsn, stmt)
	}
	nextInsert := func(what string) ir.Insert {
		t.Helper()
		got := drainChanges(t, ctx, changes, 1, 45*time.Second)
		if len(got) != 1 {
			t.Fatalf("%s: got no change (stream error: %v)", what, rdr.(*CDCReader).Err())
		}
		ins, ok := got[0].(ir.Insert)
		if !ok {
			t.Fatalf("%s: change = %T; want ir.Insert", what, got[0])
		}
		return ins
	}

	// Control: BMP-only labels, non-ASCII included, decode exactly.
	insertRow(`INSERT INTO ctl VALUES (1, 'é', 'y,é')`)
	ctlRow := nextInsert("control").Row
	if ctlRow["e"] != "é" || !reflect.DeepEqual(ctlRow["s"], []string{"y", "é"}) {
		t.Fatalf("control row = %v; want e=é s=[y é]", ctlRow)
	}

	// The lossy table, using only labels the catalog kept.
	insertRow(`INSERT INTO lossy VALUES (1, 'x', 'y,é')`)
	kept := nextInsert("kept labels").Row
	if kept["e"] != "x" || !reflect.DeepEqual(kept["s"], []string{"y", "é"}) {
		t.Fatalf("kept-label row = %v; want e=x s=[y é]", kept)
	}

	if full {
		// A genuine '?' label streams under FULL: the TABLE_MAP confirms it.
		insertRow(`INSERT INTO genuine VALUES (1, '?z')`)
		if g := nextInsert("genuine '?'").Row; g["e"] != "?z" {
			t.Fatalf("genuine '?' row e = %v; want ?z", g["e"])
		}
		// And the lost labels are recovered from it, exactly as inserted.
		insertRow(`INSERT INTO lossy VALUES (2, '😀b', '😀,y')`)
		rec := nextInsert("lost labels under FULL").Row
		if rec["e"] != "😀b" || !reflect.DeepEqual(rec["s"], []string{"😀", "y"}) {
			t.Fatalf("lost-label row under binlog_row_metadata=FULL = e=%q s=%q; want e=%q s=%q "+
				"(the TABLE_MAP optional metadata must carry the true label)", rec["e"], rec["s"], "😀b", []string{"😀", "y"})
		}
		return
	}

	// MINIMAL: the row using a lost label refuses loudly — no change is
	// emitted for it, and the stream ends naming the label.
	insertRow(`INSERT INTO lossy VALUES (2, '😀b', '😀,y')`)
	if got := drainChanges(t, ctx, changes, 1, 45*time.Second); len(got) != 0 {
		t.Fatalf("a row holding a lost label was emitted as %v; want a refusal", got)
	}
	streamErr := rdr.(*CDCReader).Err()
	if streamErr == nil || !strings.Contains(streamErr.Error(), enumLabelNotRecoverableMarker) {
		t.Fatalf("stream error = %v; want %s", streamErr, enumLabelNotRecoverableMarker)
	}
	if !strings.Contains(streamErr.Error(), `"?b"`) {
		t.Errorf("refusal does not name the label the catalog printed: %v", streamErr)
	}
}

func TestEnumLabelLoss_MySQL_MinimalMetadataRefuses(t *testing.T) {
	dsn, cleanup := startMySQLM2Preflight(t, "--binlog-row-metadata=MINIMAL")
	defer cleanup()
	enumLabelLossLane(t, dsn, FlavorVanilla, false)
}

func TestEnumLabelLoss_MySQL_FullMetadataRecovers(t *testing.T) {
	dsn, cleanup := startMySQLM2Preflight(t, "--binlog-row-metadata=FULL")
	defer cleanup()
	enumLabelLossLane(t, dsn, FlavorVanilla, true)
}

func TestEnumLabelLoss_MariaDB_MinimalMetadataRefuses(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image, "--binlog-row-metadata=MINIMAL")
	defer cleanup()
	enumLabelLossLane(t, dsn, FlavorMariaDB, false)
}

func TestEnumLabelLoss_MariaDB_FullMetadataRecovers(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image, "--binlog-row-metadata=FULL")
	defer cleanup()
	enumLabelLossLane(t, dsn, FlavorMariaDB, true)
}
