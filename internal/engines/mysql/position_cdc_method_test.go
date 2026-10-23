// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPositionCDCMethod pins the classifier `schema add-table --no-drain`
// uses on a stream's recorded position (NO-DRAIN-VSTREAM-SOURCE): every
// token shape both codecs write, under every flavor and under the target
// engine names ListStreams stamps, plus the tokens it must not claim.
func TestPositionCDCMethod(t *testing.T) {
	mustPos := func(p ir.Position, err error) string {
		t.Helper()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		return p.Token
	}
	vstreamOne := mustPos(encodeVStreamPos([]shardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"}}))
	vstreamTwo := mustPos(encodeVStreamPos([]shardGtid{
		{Keyspace: "ks", Shard: "-80", Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"},
		{Keyspace: "ks", Shard: "80-", Gtid: "MySQL56/4e11fa47-71ca-11e1-9e33-c80aa9429562:1-7"},
	}))
	binlogGTID := mustPos(encodeBinlogPos(binlogPos{Mode: positionModeGTID, GTIDSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"}))
	binlogFile := mustPos(encodeBinlogPos(binlogPos{Mode: positionModeFilePos, File: "binlog.000003", Pos: 157}))

	cases := []struct {
		name      string
		token     string
		want      ir.CDCMethod
		wantKnown bool
	}{
		{"vstream one shard", vstreamOne, ir.CDCVStream, true},
		{"vstream two shards", vstreamTwo, ir.CDCVStream, true},
		{"binlog gtid", binlogGTID, ir.CDCBinlog, true},
		{"binlog file/pos", binlogFile, ir.CDCBinlog, true},
		{"empty", "", ir.CDCNone, false},
		{"garbage", "not json", ir.CDCNone, false},
		{"empty shard list", "[]", ir.CDCNone, false},
		{"unknown binlog mode", `{"mode":"other"}`, ir.CDCNone, false},
	}
	flavors := []Flavor{FlavorVanilla, FlavorMariaDB, FlavorPlanetScale, FlavorVitess}
	positionEngines := []string{"mysql", "postgres", "planetscale", ""}
	for _, c := range cases {
		for _, f := range flavors {
			for _, pe := range positionEngines {
				got, known := Engine{Flavor: f}.PositionCDCMethod(ir.Position{Engine: pe, Token: c.token})
				if got != c.want || known != c.wantKnown {
					t.Errorf("%s (flavor %v, position engine %q): got (%s, %v), want (%s, %v)", c.name, f, pe, got, known, c.want, c.wantKnown)
				}
			}
		}
	}
}
