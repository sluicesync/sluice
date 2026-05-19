// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"github.com/orware/sluice/internal/ir"
)

const (
	uuidA = "11111111-1111-1111-1111-111111111111"
	uuidB = "22222222-2222-2222-2222-222222222222"
)

func gtidPos(t *testing.T, set string) ir.Position {
	t.Helper()
	p, err := encodeBinlogPos(binlogPos{Mode: positionModeGTID, GTIDSet: set})
	if err != nil {
		t.Fatalf("encodeBinlogPos gtid %q: %v", set, err)
	}
	return p
}

func filePos(t *testing.T, file string, pos uint32, serverUUID string) ir.Position {
	t.Helper()
	p, err := encodeBinlogPos(binlogPos{Mode: positionModeFilePos, File: file, Pos: pos, ServerUUID: serverUUID})
	if err != nil {
		t.Fatalf("encodeBinlogPos file %q: %v", file, err)
	}
	return p
}

func TestMySQLPositionOrderer_GTID(t *testing.T) {
	e := Engine{Flavor: FlavorVanilla}
	cases := []struct {
		name      string
		p, anchor string
		want      bool
	}{
		{"superset is at-or-after", uuidA + ":1-10", uuidA + ":1-5", true},
		{"subset is NOT at-or-after", uuidA + ":1-5", uuidA + ":1-10", false},
		{"equal is at-or-after (reflexive)", uuidA + ":1-10", uuidA + ":1-10", true},
		{"disjoint sets: p not after anchor", uuidA + ":1-5", uuidB + ":1-5", false},
		{"disjoint sets: anchor not after p", uuidB + ":1-5", uuidA + ":1-5", false},
		{"multi-uuid superset", uuidA + ":1-10," + uuidB + ":1-3", uuidA + ":1-5", true},
		// An empty GTID set is not a valid mysql GTID position —
		// decodeBinlogPos rejects it loudly ("gtid mode requires
		// gtid_set"). That refusal is asserted in
		// TestMySQLPositionOrderer_MalformedAndMismatch; it cannot
		// reach gtidAtOrAfter through a legitimately-encoded position.
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := e.PositionAtOrAfter(gtidPos(t, c.p), gtidPos(t, c.anchor))
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != c.want {
				t.Errorf("PositionAtOrAfter(p=%q, anchor=%q) = %v; want %v", c.p, c.anchor, got, c.want)
			}
		})
	}
}

func TestMySQLPositionOrderer_FilePos(t *testing.T) {
	e := Engine{}
	cases := []struct {
		name string
		p    ir.Position
		a    ir.Position
		want bool
	}{
		{
			"same file higher pos is after",
			filePos(t, "mysql-bin.000003", 900, uuidA),
			filePos(t, "mysql-bin.000003", 400, uuidA),
			true,
		},
		{
			"same file lower pos is not after",
			filePos(t, "mysql-bin.000003", 100, uuidA),
			filePos(t, "mysql-bin.000003", 400, uuidA),
			false,
		},
		{
			"same file equal pos is at-or-after",
			filePos(t, "mysql-bin.000003", 400, uuidA),
			filePos(t, "mysql-bin.000003", 400, uuidA),
			true,
		},
		{
			"later file is after regardless of pos",
			filePos(t, "mysql-bin.000004", 4, uuidA),
			filePos(t, "mysql-bin.000003", 9999, uuidA),
			true,
		},
		{
			"earlier file is not after",
			filePos(t, "mysql-bin.000002", 9999, uuidA),
			filePos(t, "mysql-bin.000003", 4, uuidA),
			false,
		},
		{
			"different server_uuid is unorderable (false)",
			filePos(t, "mysql-bin.000009", 9999, uuidB),
			filePos(t, "mysql-bin.000001", 4, uuidA),
			false,
		},
		{
			"empty server_uuid degrades to within-lineage compare",
			filePos(t, "mysql-bin.000004", 4, ""),
			filePos(t, "mysql-bin.000003", 9999, ""),
			true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := e.PositionAtOrAfter(c.p, c.a)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != c.want {
				t.Errorf("got %v; want %v", got, c.want)
			}
		})
	}
}

func TestMySQLPositionOrderer_MalformedAndMismatch(t *testing.T) {
	e := Engine{}

	// Malformed position (not a valid mysql-family token) → loud error.
	if _, err := e.PositionAtOrAfter(ir.Position{Engine: "mysql", Token: "{bad json"}, gtidPos(t, uuidA+":1-1")); err == nil {
		t.Error("malformed p: want error, got nil")
	}

	// Empty/from-now sentinel is not an orderable position → loud error.
	if _, err := e.PositionAtOrAfter(ir.Position{}, gtidPos(t, uuidA+":1-1")); err == nil {
		t.Error("empty-sentinel p: want error, got nil")
	}

	// Mode mismatch between p and anchor → loud error (not silent false).
	if _, err := e.PositionAtOrAfter(gtidPos(t, uuidA+":1-1"), filePos(t, "mysql-bin.000001", 4, uuidA)); err == nil {
		t.Error("mode mismatch: want error, got nil")
	}

	// Malformed GTID set inside a well-formed envelope → loud error.
	bad := ir.Position{Engine: "mysql", Token: `{"mode":"gtid","gtid_set":"not-a-uuid:1-5"}`}
	if _, err := e.PositionAtOrAfter(bad, gtidPos(t, uuidA+":1-1")); err == nil {
		t.Error("malformed gtid set: want error, got nil")
	}

	// An empty GTID set is not a legitimate position — decodeBinlogPos
	// refuses it; the orderer must surface that loudly, never treat it
	// as the empty set silently.
	emptySet := ir.Position{Engine: "mysql", Token: `{"mode":"gtid","gtid_set":""}`}
	if _, err := e.PositionAtOrAfter(emptySet, gtidPos(t, uuidA+":1-1")); err == nil {
		t.Error("empty gtid set: want loud error, got nil")
	}
}

// Compile-time assertion that Engine satisfies ir.PositionOrderer.
var _ ir.PositionOrderer = Engine{}
