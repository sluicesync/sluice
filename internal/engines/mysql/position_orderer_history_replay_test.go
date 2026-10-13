// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPositionAtOrAfter_PlacesASchemaVersionBetweenTransactions binds the
// real MySQL-family orderer to the use chain restore makes of it
// (KEY-COLLATION-REVERTED-WINDOW-RESTORE, pipeline/backup historyReplay): a
// schema version, anchored at the reader's position for the DDL's QUERY
// event, is released before a transaction's TxBegin when the orderer says
// that TxBegin is at or after the anchor. So for each position form the
// reader writes, the NEXT transaction's TxBegin must be at-or-after the DDL
// anchor and the PREVIOUS transaction's must not.
//
// The shapes are the reader's: in GTID mode a DDL's anchor is taken after
// its standalone GTID is folded (cdc_reader.go, "positionFor here reflects
// the GTID set accumulated through this DDL's own GTIDEvent"), and a
// transaction's TxBegin carries the PRE-transaction executed set (item 132);
// in file/pos mode both are the event's own end offset.
func TestPositionAtOrAfter_PlacesASchemaVersionBetweenTransactions(t *testing.T) {
	cases := []struct {
		name                string
		engine              Engine
		anchor, next, prior ir.Position
	}{
		{
			name:   "MySQL GTID",
			engine: Engine{},
			// transactions 1-4, the DDL as 5, the next transaction 6
			anchor: gtidPos(t, uuidA+":1-5"),
			next:   gtidPos(t, uuidA+":1-5"),
			prior:  gtidPos(t, uuidA+":1-3"),
		},
		{
			name:   "MySQL file/pos",
			engine: Engine{},
			anchor: filePos(t, "binlog.000002", 1602, uuidA),
			next:   filePos(t, "binlog.000002", 1680, uuidA),
			prior:  filePos(t, "binlog.000002", 1400, uuidA),
		},
		{
			name:   "MySQL file/pos across a rotation",
			engine: Engine{},
			anchor: filePos(t, "binlog.000002", 1602, uuidA),
			next:   filePos(t, "binlog.000003", 4, uuidA),
			prior:  filePos(t, "binlog.000001", 9000, uuidA),
		},
		{
			name:   "MariaDB GTID",
			engine: Engine{Flavor: FlavorMariaDB},
			anchor: gtidPos(t, "0-1-5"),
			next:   gtidPos(t, "0-1-5"),
			prior:  gtidPos(t, "0-1-3"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			check := func(what string, p ir.Position, want bool) {
				got, err := c.engine.PositionAtOrAfter(p, c.anchor)
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				if got != want {
					t.Errorf("%s: PositionAtOrAfter = %v; want %v", what, got, want)
				}
			}
			check("the next transaction's TxBegin", c.next, true)
			check("the previous transaction's TxBegin", c.prior, false)
		})
	}
}
