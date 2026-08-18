//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 254 CDC domain pin. Before this, the pgoutput path's oidToType had no
// case for a user-defined DOMAIN (its OID is dynamic, assigned at CREATE DOMAIN
// time), so a PG source table with a domain column cold-started (COPY) fine but
// wedged the continuous-sync stream on the FIRST domain DML ("unsupported column
// type OID <dyn> (typmod -1)"). Same class as the Bug 151 enum / Bug 144 array /
// Bug 147 geometry oidToType gaps, and the wire-OID sibling of the Bug 233
// ir.Domain-transparency class the audit closed for the col.Type dispatch paths.
//
// The fix resolves the runtime domain OIDs (ensureDomainBaseOIDs /
// resolveDomainBase) and UNWRAPS each domain column to its ultimate base type,
// resolving THAT — so the CDC-decoded value matches the base type the applier
// expects (the applier reads TARGET column types from information_schema, which
// unwraps domain→base). The result is never an ir.Domain wrapper.
//
// This pin is end-to-end through the REAL CDC reader and the REAL applier against
// a real target, over the family matrix the task requires: domain-over-{int,
// varchar(n) with a real typmod, uuid, enum, int[]} plus domain-over-domain. It
// drives INSERT / UPDATE / DELETE and asserts the values round-trip (src==dst) —
// i.e. the stream does NOT halt and every value is faithful. The varchar(10)
// base-typmod carriage is additionally ground-truthed against the real catalog
// (assertDomainBaseTypmodCarriage), proving the base length is carried and not
// defaulted.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// domSeedDDL is applied identically to source and target (the cold-copy
// congruent starting state). One column per domain family, PK on id, REPLICA
// IDENTITY DEFAULT (the common case — before-image narrows to the PK).
const domSeedDDL = `
	CREATE TYPE mood AS ENUM ('lo','hi','mid');
	CREATE DOMAIN dom_int   AS integer;
	CREATE DOMAIN dom_name  AS varchar(10);
	CREATE DOMAIN dom_name2 AS dom_name;   -- domain-over-domain
	CREATE DOMAIN dom_uuid  AS uuid;
	CREATE DOMAIN dom_mood  AS mood;       -- domain-over-enum
	CREATE DOMAIN dom_tags  AS int[];      -- domain-over-array
	CREATE TABLE dom_t (
		id   BIGINT PRIMARY KEY,
		di   dom_int,
		dn   dom_name,
		d2   dom_name2,
		du   dom_uuid,
		dm   dom_mood,
		dg   dom_tags,
		note text
	);
`

const domSeedRows = `
	INSERT INTO dom_t (id, di, dn, d2, du, dm, dg, note) VALUES
		(1, 100, 'alpha', 'aa', '11111111-1111-1111-1111-111111111111', 'lo', '{1,2,3}', 'r1'),
		(2, 200, 'beta',  'bb', '22222222-2222-2222-2222-222222222222', 'hi', '{4,5}',   'r2');
`

func TestCDCReader_Domain_Bug254(t *testing.T) {
	srcDSN, srcCleanup := newSharedPGDB(t, "dom_src")
	defer srcCleanup()
	tgtDSN, tgtCleanup := newSharedPGDB(t, "dom_tgt")
	defer tgtCleanup()

	// Source and target start congruent (the cold-copy equivalent).
	applyPGSQL(t, srcDSN, domSeedDDL)
	applyPGSQL(t, srcDSN, domSeedRows)
	applyPGApplier(t, tgtDSN, domSeedDDL)
	applyPGApplier(t, tgtDSN, domSeedRows)

	// Ground-truth the base-typmod carriage against the REAL catalog before
	// touching the stream: a domain-over-varchar(10) — and a domain-over-domain
	// over it — must resolve to ir.Varchar{Length:10}, using PG's OWN typtypmod
	// rather than a hardcoded convention.
	assertDomainBaseTypmodCarriage(t, srcDSN)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	eng := Engine{}
	rdr, err := eng.OpenCDCReader(ctx, srcDSN)
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
	time.Sleep(200 * time.Millisecond) // let replication start register

	// Drive one INSERT (a fresh row touching every domain family, incl. a
	// full-width 10-char varchar value), one UPDATE (mutating every domain
	// column), one DELETE. Pre-fix the FIRST of these wedged the stream with
	// "unsupported column type OID"; the three arriving proves it does not.
	const dml = `
		INSERT INTO dom_t (id, di, dn, d2, du, dm, dg, note) VALUES
			(3, 300, 'abcdefghij', 'cc', '33333333-3333-3333-3333-333333333333', 'hi', '{7,8,9}', 'r3');
		UPDATE dom_t SET di = 999, dn = 'zeta', d2 = 'zz', dm = 'mid', dg = '{10,20}' WHERE id = 1;
		DELETE FROM dom_t WHERE id = 2;
	`
	applyPGSQL(t, srcDSN, dml)

	got := drainChanges(t, ctx, changes, 3, 60*time.Second)
	if len(got) != 3 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 3 (domain must not wedge the stream — Bug 254; stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 3 (domain must not wedge the stream — Bug 254)", len(got))
	}

	// Feed the REAL emitted events into the applier pointed at the target.
	applier := openBug92Applier(t, ctx, tgtDSN)
	pumpChanges(t, ctx, applier, got)

	// src==dst: the target must now reflect the INSERT, the UPDATE, and the
	// DELETE, with every domain value faithful.
	assertDomainRow3Inserted(t, tgtDSN)
	assertDomainRow1Updated(t, tgtDSN)
	assertDomainRow2Deleted(t, tgtDSN)
}

// assertDomainBaseTypmodCarriage reads the real domain catalog entries and
// asserts the resolver unwraps them to the correct BASE ir type — pinning that
// a domain-over-varchar(10) carries the length (not defaulted to Text / length
// 0), and that a domain-over-domain flattens to the same base. Uses the exact
// (typbasetype, typtypmod) values the running PostgreSQL stored.
func assertDomainBaseTypmodCarriage(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Load every domain's immediate base (chains included) exactly as the
	// reader's ensureDomainBaseOIDs does.
	rows, err := db.QueryContext(ctx, `SELECT oid, typbasetype, typtypmod FROM pg_type WHERE typtype = 'd'`)
	if err != nil {
		t.Fatalf("read domain catalog: %v", err)
	}
	defer func() { _ = rows.Close() }()
	domainBases := map[uint32]domainBase{}
	for rows.Next() {
		var oid, baseOID uint32
		var baseTypmod int32
		if err := rows.Scan(&oid, &baseOID, &baseTypmod); err != nil {
			t.Fatalf("scan domain catalog: %v", err)
		}
		domainBases[oid] = domainBase{baseOID: baseOID, baseTypmod: baseTypmod}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate domain catalog: %v", err)
	}

	oidOf := func(typname string) uint32 {
		var oid uint32
		if err := db.QueryRowContext(ctx, `SELECT oid FROM pg_type WHERE typname = $1`, typname).Scan(&oid); err != nil {
			t.Fatalf("oid of %s: %v", typname, err)
		}
		return oid
	}

	cases := []struct {
		typname string
		want    ir.Type
	}{
		{"dom_name", ir.Varchar{Length: 10}},  // domain-over-varchar(10): length carried
		{"dom_name2", ir.Varchar{Length: 10}}, // domain-over-domain flattens to the base
	}
	for _, c := range cases {
		got, err := resolveWireColumnType(oidOf(c.typname), -1, 0, nil, domainBases)
		if err != nil {
			t.Fatalf("resolveWireColumnType(%s): %v", c.typname, err)
		}
		if got != c.want {
			t.Errorf("%s resolved to %#v; want %#v (base typmod must be carried, not defaulted)", c.typname, got, c.want)
		}
	}
}

func assertDomainRow3Inserted(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		di   int64
		dn   string
		d2   string
		du   string
		dm   string
		dg   string
		note string
	)
	err = db.QueryRowContext(ctx, `
		SELECT di, dn, d2, du::text, dm::text, dg::text, note
		  FROM dom_t WHERE id = 3
	`).Scan(&di, &dn, &d2, &du, &dm, &dg, &note)
	if err != nil {
		t.Fatalf("read row 3 (INSERT dropped? — Bug 254): %v", err)
	}
	if di != 300 {
		t.Errorf("row3 di = %d; want 300 (domain-over-int)", di)
	}
	if dn != "abcdefghij" {
		t.Errorf("row3 dn = %q; want abcdefghij (domain-over-varchar(10), full width)", dn)
	}
	if d2 != "cc" {
		t.Errorf("row3 d2 = %q; want cc (domain-over-domain)", d2)
	}
	if du != "33333333-3333-3333-3333-333333333333" {
		t.Errorf("row3 du = %q; want the seeded uuid (domain-over-uuid)", du)
	}
	if dm != "hi" {
		t.Errorf("row3 dm = %q; want hi (domain-over-enum)", dm)
	}
	if dg != "{7,8,9}" {
		t.Errorf("row3 dg = %q; want {7,8,9} (domain-over-int[])", dg)
	}
	if note != "r3" {
		t.Errorf("row3 note = %q; want r3", note)
	}
}

func assertDomainRow1Updated(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		di int64
		dn string
		d2 string
		dm string
		dg string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT di, dn, d2, dm::text, dg::text FROM dom_t WHERE id = 1
	`).Scan(&di, &dn, &d2, &dm, &dg); err != nil {
		t.Fatalf("read row 1 (UPDATE dropped? — Bug 254): %v", err)
	}
	if di != 999 {
		t.Errorf("row1 di = %d; want 999 (UPDATE on a domain column dropped)", di)
	}
	if dn != "zeta" {
		t.Errorf("row1 dn = %q; want zeta", dn)
	}
	if d2 != "zz" {
		t.Errorf("row1 d2 = %q; want zz (domain-over-domain UPDATE)", d2)
	}
	if dm != "mid" {
		t.Errorf("row1 dm = %q; want mid (domain-over-enum UPDATE)", dm)
	}
	if dg != "{10,20}" {
		t.Errorf("row1 dg = %q; want {10,20} (domain-over-int[] UPDATE)", dg)
	}
}

func assertDomainRow2Deleted(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM dom_t WHERE id = 2`).Scan(&n); err != nil {
		t.Fatalf("count row 2: %v", err)
	}
	if n != 0 {
		t.Errorf("row 2 still present after DELETE (count=%d); the DELETE on a domain-carrying table was dropped — Bug 254", n)
	}
}
