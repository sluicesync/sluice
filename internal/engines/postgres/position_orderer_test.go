// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/orware/sluice/internal/ir"
)

func pgPosToken(t *testing.T, slot, lsn string) ir.Position {
	t.Helper()
	p, err := encodePGPos(pgPos{Slot: slot, LSN: lsn})
	if err != nil {
		t.Fatalf("encodePGPos(%q,%q): %v", slot, lsn, err)
	}
	return p
}

func TestPGPositionOrderer_LSN(t *testing.T) {
	e := Engine{}
	cases := []struct {
		name string
		p    ir.Position
		a    ir.Position
		want bool
	}{
		{
			"higher lsn is at-or-after",
			pgPosToken(t, "s1", "0/2000000"),
			pgPosToken(t, "s1", "0/1000000"),
			true,
		},
		{
			"lower lsn is not at-or-after",
			pgPosToken(t, "s1", "0/1000000"),
			pgPosToken(t, "s1", "0/2000000"),
			false,
		},
		{
			"equal lsn is at-or-after (reflexive)",
			pgPosToken(t, "s1", "0/1500000"),
			pgPosToken(t, "s1", "0/1500000"),
			true,
		},
		{
			"cross-segment ordering is numeric not lexical",
			pgPosToken(t, "s1", "1/0"),
			pgPosToken(t, "s1", "0/FFFFFFFF"),
			true,
		},
		{
			"different slot is unorderable (false)",
			pgPosToken(t, "s2", "9/0"),
			pgPosToken(t, "s1", "0/1"),
			false,
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

func TestPGPositionOrderer_MalformedAndSentinel(t *testing.T) {
	e := Engine{}

	if _, err := e.PositionAtOrAfter(ir.Position{Engine: "postgres", Token: "{bad"}, pgPosToken(t, "s1", "0/1")); err == nil {
		t.Error("malformed p: want error, got nil")
	}
	if _, err := e.PositionAtOrAfter(ir.Position{}, pgPosToken(t, "s1", "0/1")); err == nil {
		t.Error("empty-sentinel p: want error, got nil")
	}
	// Wrong-engine token (a mysql position) must be rejected loudly.
	if _, err := e.PositionAtOrAfter(ir.Position{Engine: "mysql", Token: "x"}, pgPosToken(t, "s1", "0/1")); err == nil {
		t.Error("wrong-engine p: want error, got nil")
	}
}

// Compile-time assertion that Engine satisfies ir.PositionOrderer.
var _ ir.PositionOrderer = Engine{}
