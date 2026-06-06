// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
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

// --- VStream / PlanetScale position ordering ---------------------------
//
// PIN THE CLASS (Bug-74 lesson): a VStream position is a JSON ARRAY of
// per-shard shardGtid — a different token family from the binlogPos
// object the cases above exercise. v0.99.7 crashed on EVERY warm-resume
// of a PlanetScale stream because PositionAtOrAfter routed the array
// token through decodeBinlogPos (object-only) and panicked the resolve
// with "cannot unmarshal array into Go value of type mysql.binlogPos".
// The matrix below pins the full VStream family: single-shard ×
// multi-shard, each × {with table_p_ks, without table_p_ks} (ordering
// MUST be identical with/without the COPY cursor), across the GTID
// relation {p ⊋ anchor, p ⊊ anchor, p == anchor, disjoint, anchor
// empty}, plus the exact crashing regression fixture.

// vstreamPos builds a VStream ir.Position via the real encoder so the
// token shape matches production exactly. withPK adds a (fake but
// well-formed) per-table COPY cursor to every shard — ordering must be
// invariant to it.
func vstreamPos(t *testing.T, withPK bool, shards ...shardGtid) ir.Position {
	t.Helper()
	out := make([]shardGtid, len(shards))
	copy(out, shards)
	if withPK {
		for i := range out {
			out[i].TablePKs = []encodedTablePK{{
				TableName: "connections",
				// Lastpk is opaque to the orderer (TablePKs is ignored);
				// a non-empty value is enough to exercise the cursor-
				// present branch. The encoder doesn't validate it.
				Lastpk: "AAEC",
			}}
		}
	}
	p, err := encodeVStreamPos(out)
	if err != nil {
		t.Fatalf("encodeVStreamPos(%+v): %v", out, err)
	}
	return p
}

func sg(keyspace, shard, gtid string) shardGtid {
	return shardGtid{Keyspace: keyspace, Shard: shard, Gtid: gtid}
}

func TestMySQLPositionOrderer_VStream(t *testing.T) {
	e := Engine{Flavor: FlavorPlanetScale}

	const (
		ks   = "main"
		pfx  = "MySQL56/"
		shrd = "-" // unsharded PlanetScale convention
	)

	cases := []struct {
		name      string
		p, anchor []shardGtid
		want      bool
	}{
		{
			"single-shard: p superset of anchor (true)",
			[]shardGtid{sg(ks, shrd, pfx+uuidA+":1-10")},
			[]shardGtid{sg(ks, shrd, pfx+uuidA+":1-5")},
			true,
		},
		{
			"single-shard: p subset of anchor (false)",
			[]shardGtid{sg(ks, shrd, pfx+uuidA+":1-5")},
			[]shardGtid{sg(ks, shrd, pfx+uuidA+":1-10")},
			false,
		},
		{
			"single-shard: p == anchor (true, reflexive)",
			[]shardGtid{sg(ks, shrd, pfx+uuidA+":1-10")},
			[]shardGtid{sg(ks, shrd, pfx+uuidA+":1-10")},
			true,
		},
		{
			"single-shard: disjoint GTIDs (false, NOT error)",
			[]shardGtid{sg(ks, shrd, pfx+uuidA+":1-5")},
			[]shardGtid{sg(ks, shrd, pfx+uuidB+":1-5")},
			false,
		},
		{
			"single-shard: multi-uuid p superset of anchor (true)",
			[]shardGtid{sg(ks, shrd, pfx+uuidA+":1-10,"+uuidB+":1-3")},
			[]shardGtid{sg(ks, shrd, pfx+uuidA+":1-5")},
			true,
		},
		{
			"multi-shard: p superset on every shard (true)",
			[]shardGtid{sg(ks, "-80", pfx+uuidA+":1-10"), sg(ks, "80-", pfx+uuidB+":1-20")},
			[]shardGtid{sg(ks, "-80", pfx+uuidA+":1-5"), sg(ks, "80-", pfx+uuidB+":1-15")},
			true,
		},
		{
			"multi-shard: p superset on one shard, subset on the other (false)",
			[]shardGtid{sg(ks, "-80", pfx+uuidA+":1-10"), sg(ks, "80-", pfx+uuidB+":1-5")},
			[]shardGtid{sg(ks, "-80", pfx+uuidA+":1-5"), sg(ks, "80-", pfx+uuidB+":1-20")},
			false,
		},
		{
			"multi-shard: anchor shard absent from p (false, partial order)",
			[]shardGtid{sg(ks, "-80", pfx+uuidA+":1-10")},
			[]shardGtid{sg(ks, "-80", pfx+uuidA+":1-5"), sg(ks, "80-", pfx+uuidB+":1-15")},
			false,
		},
		{
			"multi-shard: p == anchor (true)",
			[]shardGtid{sg(ks, "-80", pfx+uuidA+":1-10"), sg(ks, "80-", pfx+uuidB+":1-20")},
			[]shardGtid{sg(ks, "-80", pfx+uuidA+":1-10"), sg(ks, "80-", pfx+uuidB+":1-20")},
			true,
		},
	}

	for _, c := range cases {
		c := c
		// Each case is run for {without table_p_ks, with table_p_ks};
		// the COPY cursor must NOT change the ordering result.
		for _, withPK := range []bool{false, true} {
			withPK := withPK
			label := c.name
			if withPK {
				label += " [+table_p_ks]"
			}
			t.Run(label, func(t *testing.T) {
				p := vstreamPos(t, withPK, c.p...)
				anchor := vstreamPos(t, withPK, c.anchor...)
				got, err := e.PositionAtOrAfter(p, anchor)
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if got != c.want {
					t.Errorf("PositionAtOrAfter = %v; want %v\n p=%s\n a=%s", got, c.want, p.Token, anchor.Token)
				}
			})
		}
	}
}

// TestMySQLPositionOrderer_VStream_AnchorEmptyGTID pins the "anchor
// covers no transactions" edge: an empty GTID set is a subset of every
// set, so any p is at-or-after it. The empty set is the bare flavor
// prefix ("MySQL56/" → "" after the strip), exercising the
// gtidAtOrAfter empty-anchor branch through the VStream path.
func TestMySQLPositionOrderer_VStream_AnchorEmptyGTID(t *testing.T) {
	e := Engine{Flavor: FlavorPlanetScale}
	p := vstreamPos(t, false, sg("main", "-", "MySQL56/"+uuidA+":1-10"))
	anchor := vstreamPos(t, false, sg("main", "-", "MySQL56/"))
	got, err := e.PositionAtOrAfter(p, anchor)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !got {
		t.Errorf("p at-or-after empty anchor = %v; want true", got)
	}
}

// TestMySQLPositionOrderer_VStream_RegressionFixture reconstructs the
// EXACT crashing shape from the v0.99.7 PlanetScale warm-resume repro: a
// single-shard "-" token with a multi-uuid MySQL56/ gtid and a
// `connections` table_p_ks lastpk cursor. Before the fix this panicked
// the schema-history resolve ("cannot unmarshal array into Go value of
// type mysql.binlogPos"); it must now order without erroring.
func TestMySQLPositionOrderer_VStream_RegressionFixture(t *testing.T) {
	e := Engine{Flavor: FlavorPlanetScale}
	// The persisted resume position: mid-COPY, carries the connections
	// cursor, multi-uuid GTID set.
	p := ir.Position{
		Engine: engineNameVStream,
		Token: `[{"keyspace":"main","shard":"-",` +
			`"gtid":"MySQL56/` + uuidA + `:1-100,` + uuidB + `:1-7",` +
			`"table_p_ks":[{"table_name":"connections","lastpk":"AAEC"}]}]`,
	}
	// A schema-history anchor at an earlier point in the same lineage.
	anchor := ir.Position{
		Engine: engineNameVStream,
		Token:  `[{"keyspace":"main","shard":"-","gtid":"MySQL56/` + uuidA + `:1-50"}]`,
	}
	got, err := e.PositionAtOrAfter(p, anchor)
	if err != nil {
		t.Fatalf("regression fixture must not error (was the v0.99.7 crash): %v", err)
	}
	if !got {
		t.Errorf("resume position p ⊇ anchor; want at-or-after true, got false")
	}

	// Also assert the no-cursor CDC-phase restart (the ordinary
	// PlanetScale sync restart that Phase A proved ALSO crashed) orders
	// identically — the cursor is pure bookkeeping.
	pNoPK := ir.Position{
		Engine: engineNameVStream,
		Token:  `[{"keyspace":"main","shard":"-","gtid":"MySQL56/` + uuidA + `:1-100,` + uuidB + `:1-7"}]`,
	}
	got2, err := e.PositionAtOrAfter(pNoPK, anchor)
	if err != nil {
		t.Fatalf("no-cursor restart must not error: %v", err)
	}
	if got2 != got {
		t.Errorf("ordering differs with/without table_p_ks: withPK=%v noPK=%v", got, got2)
	}
}

// TestMySQLPositionOrderer_VStream_ShapeMismatch pins that mixing a
// VStream array position with a vanilla binlogPos object is a LOUD "not
// comparable" error, never a silent false (mirrors the mode-mismatch
// floor on the binlog path).
func TestMySQLPositionOrderer_VStream_ShapeMismatch(t *testing.T) {
	e := Engine{Flavor: FlavorPlanetScale}
	vs := vstreamPos(t, false, sg("main", "-", "MySQL56/"+uuidA+":1-10"))
	bin := gtidPos(t, uuidA+":1-10")

	if _, err := e.PositionAtOrAfter(vs, bin); err == nil {
		t.Error("vstream p vs binlogPos anchor: want loud error, got nil")
	}
	if _, err := e.PositionAtOrAfter(bin, vs); err == nil {
		t.Error("binlogPos p vs vstream anchor: want loud error, got nil")
	}
}

// TestStripGTIDFlavor pins the MySQL56/ prefix handling in isolation:
// go-mysql's ParseMysqlGTIDSet rejects the prefixed form, so the orderer
// MUST strip it. Case-insensitive; no-op for already-bare sets.
func TestStripGTIDFlavor(t *testing.T) {
	cases := []struct{ in, want string }{
		{"MySQL56/" + uuidA + ":1-10", uuidA + ":1-10"},
		{"mysql56/" + uuidA + ":1-10", uuidA + ":1-10"}, // case-insensitive prefix
		{uuidA + ":1-10", uuidA + ":1-10"},              // already bare → no-op
		{"MySQL56/", ""},                                // flavor only → empty set
		{"", ""},                                        // empty → no-op
	}
	for _, c := range cases {
		if got := stripGTIDFlavor(c.in); got != c.want {
			t.Errorf("stripGTIDFlavor(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// Compile-time assertion that Engine satisfies ir.PositionOrderer.
var _ ir.PositionOrderer = Engine{}
