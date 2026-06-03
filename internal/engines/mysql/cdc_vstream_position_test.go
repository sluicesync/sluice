// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestEncodeDecodeVStreamPos(t *testing.T) {
	cases := []struct {
		name string
		in   []shardGtid
	}{
		{
			"unsharded keyspace",
			[]shardGtid{
				{Keyspace: "main", Shard: "-", Gtid: "MySQL56/abcd-1234:1-100"},
			},
		},
		{
			"two-shard keyspace",
			[]shardGtid{
				{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/abcd:1-50"},
				{Keyspace: "main", Shard: "80-", Gtid: "MySQL56/efgh:1-200"},
			},
		},
		{
			"two keyspaces",
			[]shardGtid{
				{Keyspace: "ks_a", Shard: "-", Gtid: "MySQL56/x:1"},
				{Keyspace: "ks_b", Shard: "-", Gtid: "MySQL56/y:1"},
			},
		},
		{
			"current sentinel",
			[]shardGtid{
				{Keyspace: "main", Shard: "-", Gtid: "current"},
			},
		},
		{
			"beginning sentinel",
			[]shardGtid{
				{Keyspace: "main", Shard: "-", Gtid: ""},
			},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			encoded, err := encodeVStreamPos(c.in)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if encoded.Engine != engineNameVStream {
				t.Errorf("Engine = %q; want %q", encoded.Engine, engineNameVStream)
			}
			got, ok, err := decodeVStreamPos(encoded)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !ok {
				t.Fatalf("decode: ok=false; expected a valid position")
			}
			if !reflect.DeepEqual(got, c.in) {
				// The encode path sorts; on inputs already in
				// canonical order the round-trip is exact.
				t.Errorf("round-trip\n got:  %#v\nwant: %#v", got, c.in)
			}
		})
	}
}

// TestEncodeVStreamPosCanonicalOrder confirms encoding produces a
// stable token regardless of input slice order. Two sequential
// calls with the same logical contents should produce
// byte-identical token strings — useful for log-greps and diffing
// position rows in the control table.
func TestEncodeVStreamPosCanonicalOrder(t *testing.T) {
	a := []shardGtid{
		{Keyspace: "main", Shard: "-80", Gtid: "g1"},
		{Keyspace: "main", Shard: "80-", Gtid: "g2"},
	}
	b := []shardGtid{
		{Keyspace: "main", Shard: "80-", Gtid: "g2"},
		{Keyspace: "main", Shard: "-80", Gtid: "g1"},
	}

	encA, err := encodeVStreamPos(a)
	if err != nil {
		t.Fatalf("encode a: %v", err)
	}
	encB, err := encodeVStreamPos(b)
	if err != nil {
		t.Fatalf("encode b: %v", err)
	}
	if encA.Token != encB.Token {
		t.Errorf("tokens differ across input orderings:\n  a: %s\n  b: %s", encA.Token, encB.Token)
	}
}

// TestEncodeVStreamPosDoesNotMutateInput confirms encodeVStreamPos
// doesn't reorder the caller's slice in-place. Catches a foot-gun
// where the canonical-order sort surprised a caller mid-iteration.
func TestEncodeVStreamPosDoesNotMutateInput(t *testing.T) {
	in := []shardGtid{
		{Keyspace: "main", Shard: "80-", Gtid: "g2"},
		{Keyspace: "main", Shard: "-80", Gtid: "g1"},
	}
	original := make([]shardGtid, len(in))
	copy(original, in)
	if _, err := encodeVStreamPos(in); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !reflect.DeepEqual(in, original) {
		t.Errorf("encodeVStreamPos mutated input slice:\n got:  %#v\nwant: %#v", in, original)
	}
}

func TestEncodeVStreamPosErrors(t *testing.T) {
	cases := []struct {
		name    string
		in      []shardGtid
		wantSub string
	}{
		{"empty slice", nil, "at least one shardGtid"},
		{"missing keyspace", []shardGtid{{Shard: "-", Gtid: "g"}}, "keyspace is required"},
		{"missing shard", []shardGtid{{Keyspace: "main", Gtid: "g"}}, "shard is required"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := encodeVStreamPos(c.in)
			if err == nil {
				t.Fatalf("expected error containing %q; got nil", c.wantSub)
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Errorf("err = %q; want substring %q", err.Error(), c.wantSub)
			}
		})
	}
}

func TestDecodeVStreamPosFromNowSentinel(t *testing.T) {
	_, ok, err := decodeVStreamPos(ir.Position{})
	if err != nil {
		t.Fatalf("zero position should not error: %v", err)
	}
	if ok {
		t.Errorf("zero position should report ok=false (from-now sentinel)")
	}
}

func TestDecodeVStreamPosErrors(t *testing.T) {
	cases := []struct {
		name    string
		in      ir.Position
		wantSub string
	}{
		{
			// "mysql" is now accepted as a valid mysql-family engine
			// alias — the applier round-trips planetscale positions
			// tagged with the applier's own engine name. So this is
			// no longer an error; positive coverage of the alias case
			// lives in TestDecodeVStreamPos_AcceptsMySQLEngineAlias.
			"wrong engine — postgres is rejected",
			ir.Position{Engine: "postgres", Token: `[{"keyspace":"main","shard":"-","gtid":"g"}]`},
			"wrong engine",
		},
		{
			"empty token with non-empty engine",
			ir.Position{Engine: engineNameVStream, Token: ""},
			"empty token",
		},
		{
			"malformed json",
			ir.Position{Engine: engineNameVStream, Token: "not json"},
			"unmarshal",
		},
		{
			"empty array decodes to no shards",
			ir.Position{Engine: engineNameVStream, Token: "[]"},
			"empty shard list",
		},
		{
			"shard with missing keyspace",
			ir.Position{Engine: engineNameVStream, Token: `[{"shard":"-","gtid":"g"}]`},
			"missing keyspace",
		},
		{
			"shard with missing shard name",
			ir.Position{Engine: engineNameVStream, Token: `[{"keyspace":"main","gtid":"g"}]`},
			"missing shard",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, _, err := decodeVStreamPos(c.in)
			if err == nil {
				t.Fatalf("expected error containing %q; got nil", c.wantSub)
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Errorf("err = %q; want substring %q", err.Error(), c.wantSub)
			}
		})
	}
}

// TestDecodeVStreamPos_AcceptsMySQLEngineAlias covers Bug 2's fix:
// the [ChangeApplier].ReadPosition path stamps every recovered
// position with the applier's own engine name (always "mysql" on
// the MySQL applier), regardless of which reader produced the
// original. So a VStream-shape token written by a planetscale-
// flavor stream comes back through ReadPosition tagged "mysql".
// decodeVStreamPos must accept that alias to keep warm resume
// working — without this, every restart of a planetscale sync
// fails with "wrong engine 'mysql'; want 'planetscale'".
func TestDecodeVStreamPos_AcceptsMySQLEngineAlias(t *testing.T) {
	want := []shardGtid{
		{Keyspace: "main", Shard: "-", Gtid: "MySQL56/abcd:1-100"},
	}
	pos := ir.Position{
		Engine: "mysql", // applier's stamp, not the reader's canonical "planetscale"
		Token:  `[{"keyspace":"main","shard":"-","gtid":"MySQL56/abcd:1-100"}]`,
	}
	got, ok, err := decodeVStreamPos(pos)
	if err != nil {
		t.Fatalf("expected mysql-aliased VStream position to decode cleanly; got err: %v", err)
	}
	if !ok {
		t.Fatal("ok=false; want true (this is a real position, not the from-now sentinel)")
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("\n got:  %#v\nwant: %#v", got, want)
	}
}

// TestSentinelHelpers confirms fromNow / fromBeginning produce the
// canonical Vitess sentinel strings that VStream recognises.
func TestSentinelHelpers(t *testing.T) {
	t.Run("fromNow uses 'current'", func(t *testing.T) {
		got := fromNowVStreamPos("main", []string{"-"})
		if len(got) != 1 || got[0].Gtid != "current" {
			t.Errorf("got %#v; want one entry with Gtid=current", got)
		}
	})

	t.Run("fromBeginning uses ''", func(t *testing.T) {
		got := fromBeginningVStreamPos("main", []string{"-"})
		if len(got) != 1 || got[0].Gtid != "" {
			t.Errorf("got %#v; want one entry with Gtid=''", got)
		}
	})

	t.Run("preserves shard order", func(t *testing.T) {
		got := fromNowVStreamPos("main", []string{"-80", "80-"})
		if len(got) != 2 {
			t.Fatalf("got %d entries; want 2", len(got))
		}
		if got[0].Shard != "-80" || got[1].Shard != "80-" {
			t.Errorf("shard order = %q,%q; want -80,80-", got[0].Shard, got[1].Shard)
		}
	})
}
