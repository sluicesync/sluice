//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestDomainColumn_WireOIDIsBaseType_PremisePin binds the load-bearing
// environmental premise the whole ir.Domain value-transparency argument rests
// on (audit backlog PREM-1, the CLAUDE.md premise-naming rule).
//
// The v0.128.0 value-fidelity review established EMPIRICALLY — but nothing in
// the suite asserted — that for a REGULAR query, PostgreSQL reports a domain
// column's BASE type OID (not the domain's own dynamic OID) in the wire
// RowDescription. That is WHY the domain population is value-safe: pgx's
// CopyFrom and value decode key on the reported OID, so they pick the base
// type's codec and the domain wrapper is erased at the wire before any codec is
// chosen — making a domain-over-X column byte-identical to a bare X column. If
// a future PostgreSQL or pgx ever reported the DOMAIN's OID here instead, the
// base-keyed codec registrations and the physical-write predicates the audit
// added (now correct BECAUSE the base codec is selected) would silently
// mis-encode. This pins the fact so that regression fails the build.
//
// Note the deliberate contrast with the CDC path: pgoutput LOGICAL REPLICATION
// carries the domain's OWN dynamic OID (that asymmetry is Bug 254, handled by
// cdc_relations.go's resolveDomainBase). This test is about the REGULAR-query
// RowDescription, which is the opposite and is what the cold-start COPY path
// relies on.
func TestDomainColumn_WireOIDIsBaseType_PremisePin(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, dsn, `
		CREATE DOMAIN d_int   AS integer   CHECK (VALUE >= 0);
		CREATE DOMAIN d_text  AS text       CHECK (char_length(VALUE) <= 32);
		CREATE DOMAIN d_num   AS numeric(10,2);
		CREATE DOMAIN d_ttz   AS timetz;
		CREATE DOMAIN d_uuid  AS uuid;
		CREATE DOMAIN d_dd    AS d_int;   -- domain over a domain
		CREATE TABLE prem (
			id   integer PRIMARY KEY,
			a_int  d_int,
			a_text d_text,
			a_num  d_num,
			a_ttz  d_ttz,
			a_uuid d_uuid,
			a_dd   d_dd
		);
	`)

	// Each domain column, and the base-type OID PostgreSQL must report for it in
	// a regular query's RowDescription. All are builtin OIDs (< 16384); the
	// domains themselves have dynamic OIDs (>= 16384) which must NOT appear here.
	want := []struct {
		col     string
		baseOID uint32
	}{
		{"a_int", pgtype.Int4OID},
		{"a_text", pgtype.TextOID},
		{"a_num", pgtype.NumericOID},
		{"a_ttz", pgtype.TimetzOID},
		{"a_uuid", pgtype.UUIDOID},
		{"a_dd", pgtype.Int4OID}, // domain-over-domain flattens to the concrete base
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	for _, w := range want {
		rows, err := conn.Query(ctx, "SELECT "+w.col+" FROM prem")
		if err != nil {
			t.Fatalf("query %s: %v", w.col, err)
		}
		fds := rows.FieldDescriptions()
		if len(fds) != 1 {
			rows.Close()
			t.Fatalf("%s: got %d field descriptions, want 1", w.col, len(fds))
		}
		got := fds[0].DataTypeOID
		rows.Close()

		// The premise, stated two ways so the pin cannot pass on a coincidence:
		// (1) the reported OID is the BASE type's, and (2) it is a builtin
		// (< 16384), i.e. NOT the domain's own dynamic OID. If PG ever reported
		// the domain OID, (2) fails even if (1)'s constant were somehow stale.
		if got != w.baseOID {
			t.Errorf("column %s: RowDescription DataTypeOID = %d, want the BASE type OID %d — "+
				"PostgreSQL is no longer reporting a domain column's base OID on the wire, which is the "+
				"load-bearing premise the ir.Domain value-transparency (audit A-1/A-2/A-3) depends on", w.col, got, w.baseOID)
		}
		if got >= 16384 {
			t.Errorf("column %s: RowDescription DataTypeOID = %d is a DYNAMIC (>=16384) OID — PostgreSQL is "+
				"reporting the DOMAIN's own OID rather than its base; the base-keyed COPY/value codecs would "+
				"mis-select. This is exactly the regression PREM-1 guards.", w.col, got)
		}
	}
}
